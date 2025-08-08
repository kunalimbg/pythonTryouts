import math
from random import randint
import time
from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd
import json
from datetime import date, datetime, timedelta
import os
from decimal import Decimal, ROUND_HALF_UP
import argparse

# SSH and DB config
SSH_HOST = 'jump-production.yourtoken.io'
SSH_PORT = 22
SSH_USER = 'azureuser'
SSH_PRIVATE_KEY = '/home/kunal/Downloads/jump-production_key.pem'
SSH_PASSWORD = None

DB_HOST = 'production-yt-replica.postgres.database.azure.com'
DB_PORT = 5432
DB_NAME = 'postgres'
DB_USER = 'postgres_ARDowsChlogR'
DB_PASSWORD = '6:?wH564}Ueh'

BATCH_INTERVAL = timedelta(minutes=10)
BASE_OUTPUT_DIR = 'new_analytics/transaction'
BASE_OUTPUT_DIR_BUNDLE = "new_analytics/bundleTxnLink"
BASE_OUTPUT_DIR_TXN_BUNDLE_PRODUCT_LINKS = "new_analytics/txnBundleProductLink"

def parse_date(date_str):
    return datetime.strptime(date_str, "%d-%m-%y").date()

def extract_shopify_store_id(data):
    from re import search
    url = data.get("order_status_url", "")
    match = search(r"https://[^/]+/(\d+)/orders/", url)
    return match.group(1) if match else ""

def extract_bundle_info(properties):
    is_bundle = False
    bundle_id = ""

    for prop in properties:
        name = prop.get("name")
        value = prop.get("value")

        if name == "_isYtCustomBundle":
            if isinstance(value, bool):
                is_bundle = value
            elif isinstance(value, str):
                is_bundle = value.lower() == "true"

        elif name == "_bundleId":
            bundle_id = str(value) if value is not None else ""

    return is_bundle, bundle_id

def round_two_places(value):
    return float(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def calculate_bundle_sales_and_subtotal(line_items):
    bundle_sales = {}
    subtotal_amount = 0.0

    for item in line_items:
        price_str = item.get("price_set", {}).get("shop_money", {}).get("amount", "0")
        price = float(price_str) if price_str else 0.0

        discount = 0.0
        for d in item.get("discount_allocations", []):
            d_amt_str = d.get("amount_set", {}).get("shop_money", {}).get("amount", "0")
            discount += float(d_amt_str) if d_amt_str else 0.0

        qty = item.get("quantity", 1)

        is_bundle, bundle_id = extract_bundle_info(item.get("properties", []))
        total_item_amount = qty * (price - discount)

        if is_bundle and bundle_id:
            bundle_sales[bundle_id] = bundle_sales.get(bundle_id, 0.0) + total_item_amount

        subtotal_amount += total_item_amount

    for bundle_id, bundle_sale in bundle_sales.items():
        bundle_sales[bundle_id] = round_two_places(bundle_sale)

    return bundle_sales, round_two_places(subtotal_amount)

def wrap_row(row):
    try:
        payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        now_utc = datetime.utcnow().isoformat() + "Z"
        subtotal = calculate_bundle_sales_and_subtotal(payload.get("line_items", []))[1]

        return {
            "id" : payload.get("id") - randint(1, 999999),
            "transaction_id": payload.get("id"),
            "name": "transaction",
            "timestamp": payload.get("created_at"),
            "data": payload,
            "shopify_store_id": extract_shopify_store_id(payload),
            "currency": payload.get("presentment_currency"),
            "transaction_amount": payload.get("current_total_price"),
            "discount_amount": payload.get("current_total_discounts"),
            "sub_total_amount": subtotal,
        }
    except Exception as e:
        print(f"⚠️ Skipping row due to error: {e}")
        return None

def process_date(target_date, cur, cur_local):
    next_day = target_date + timedelta(days=1)

    cur.execute(
        '''
        SELECT MIN("createdAt"), MAX("createdAt")
        FROM public.transaction
        WHERE "orderPayload" IS NOT NULL
        AND "createdAt" >= %s AND "createdAt" < %s
        ''',
        (target_date, next_day)
    )

    min_ts, max_ts = cur.fetchone()
    if not min_ts or not max_ts:
        print(f"❌ No transactions found for {target_date}")
        return 0, 0, 0  # Always return 3 values

    print(f"📅 Time range for {target_date}: {min_ts} to {max_ts}")
    current_start = min_ts
    batch_num = 1
    transaction_count = 0
    bundle_count = 0
    txn_bundle_product_count = 0

    while current_start < max_ts:
        current_end = current_start + BATCH_INTERVAL
        print(f"⏳ Batch {batch_num}: {current_start} → {current_end}")

        cur.execute("""
            SELECT "orderPayload", "createdAt"
            FROM public.transaction
            WHERE "orderPayload" IS NOT NULL
              AND "createdAt" >= %s AND "createdAt" < %s;
        """, (current_start, current_end))

        rows = cur.fetchall()
        batch_data = [wrap_row(row) for row in rows]
        batch_data = [row for row in batch_data if row is not None]

        # Deduplicate within the batch by transaction_id
        seen_tx_ids = set()
        deduped_data = []
        for row in batch_data:
            tx_id = row.get("transaction_id")
            if tx_id and tx_id not in seen_tx_ids:
                seen_tx_ids.add(tx_id)
                deduped_data.append(row)
        batch_data = deduped_data

        if batch_data:
            df = pd.DataFrame(batch_data)
            df["data"] = df["data"].apply(json.dumps)

            # Save transaction parquet
            dir_path = os.path.join(
                BASE_OUTPUT_DIR,
                f"year={current_start.year}",
                f"month={current_start.month:02}",
                f"day={current_start.day:02}",
                f"hour={current_start.hour:02}"
            )
            os.makedirs(dir_path, exist_ok=True)

            filename = f"order_payload_{current_start.hour:02}_{current_start.minute:02}.parquet"
            file_path = os.path.join(dir_path, filename)
            df.to_parquet(file_path, index=False)
            print(f"✅ Wrote {len(df)} rows to {file_path}")
            transaction_count += len(df)

            # Prepare bundle and tbp links
            txn_bundle_product_links = []
            bundle_records = []

            for row in batch_data:
                payload = row.get("data")
                timestamp = row.get("timestamp")
                if isinstance(payload, str):
                    payload = json.loads(payload)

                shopify_store_id = row.get("shopify_store_id", "")
                transaction_id = row.get("transaction_id")

                for item in payload.get("line_items", []):
                    qty = item.get("quantity", 1)
                    unit_price = float(item.get("price_set", {}).get("shop_money", {}).get("amount", 0))
                    discount_total = sum([
                        float(d.get("amount_set", {}).get("shop_money", {}).get("amount", 0))
                        for d in item.get("discount_allocations", [])
                    ])
                    unit_discount = discount_total / qty if qty else 0
                    net_unit_price = unit_price - unit_discount
                    is_bundle, bundle_id = extract_bundle_info(item.get("properties", []))

                    product_id = item.get("product_id", "")
                    if not product_id:
                        continue

                    product_url = ""
                    if is_bundle:
                        cur_local.execute(
                                """
                                SELECT product_url
                                FROM bundle_data
                                WHERE bundle_id = %s AND product_id = '%s'
                                """,
                            (bundle_id, product_id)
                        )
                        bundle_map = cur_local.fetchone()

                        product_url = bundle_map[0] if bundle_map else item.get("product_url", "")
                    txn_bundle_product_links.append({
                        "transaction_id": transaction_id,
                        "bundle_id": bundle_id if is_bundle else None,
                        "product_id": product_id,
                        "product_name": item.get("title"),
                        "product_url": product_url,
                        "shopify_store_id": shopify_store_id,
                        "product_unit_amount": round_two_places(unit_price) if unit_price else 0.0,
                        "product_quantity": qty,
                        "product_total_amount": round_two_places(unit_price * qty) if unit_price else 0.0,
                        "product_discount_amount": round_two_places(unit_discount * qty) if unit_discount else 0.0,
                        "product_net_amount": round_two_places(net_unit_price * qty) if net_unit_price else 0.0,
                        "timestamp": timestamp,
                    })

                # Bundle sales
                try:
                    line_items = payload.get("line_items", [])
                    bundle_sales, _ = calculate_bundle_sales_and_subtotal(line_items)
                    for bundle_id, amount in bundle_sales.items():
                        bundle_records.append({
                            "id": payload.get("id") - randint(1, 999999),
                            "transaction_id": transaction_id,
                            "currency": row.get("currency"),
                            "shopify_store_id": shopify_store_id,
                            "bundle_id": bundle_id,
                            "sales": amount,
                            "timestamp": timestamp,
                            "bundle_sale": amount
                        })
                except Exception as e:
                    print(f"⚠️ Error processing bundle sales: {e}")
                    continue

            # Save bundles
            if bundle_records:
                bundle_df = pd.DataFrame(bundle_records)
                bundle_dir = os.path.join(
                    BASE_OUTPUT_DIR_BUNDLE,
                    f"year={current_start.year}",
                    f"month={current_start.month:02}",
                    f"day={current_start.day:02}",
                    f"hour={current_start.hour:02}"
                )
                os.makedirs(bundle_dir, exist_ok=True)

                bundle_filename = f"bundle_sales_{current_start.hour:02}_{current_start.minute:02}.parquet"
                bundle_file_path = os.path.join(bundle_dir, bundle_filename)

                bundle_df.to_parquet(bundle_file_path, index=False)
                print(f"✅ Wrote {len(bundle_df)} bundle rows to {bundle_file_path}")
                bundle_count += len(bundle_df)
            else:
                print("⚠️ No bundle sales data in this batch.")

            # Save tbp links
            if txn_bundle_product_links:
                tbp_dir = os.path.join(
                    BASE_OUTPUT_DIR_TXN_BUNDLE_PRODUCT_LINKS,
                    f"year={current_start.year}",
                    f"month={current_start.month:02}",
                    f"day={current_start.day:02}",
                    f"hour={current_start.hour:02}"
                )
                os.makedirs(tbp_dir, exist_ok=True)
                tbp_filename = f"transaction_bundle_product_links_{current_start.hour:02}_{current_start.minute:02}.parquet"
                tbp_path = os.path.join(tbp_dir, tbp_filename)
                tbp_df = pd.DataFrame(txn_bundle_product_links)
                tbp_df.to_parquet(tbp_path, index=False)
                print(f"✅ Wrote {len(tbp_df)} transaction_bundle_product_links rows to {tbp_path}")
                txn_bundle_product_count += len(tbp_df)
            else:
                print("⚠️ No transaction_bundle_product_links generated in this batch.")

        current_start = current_end
        batch_num += 1

    return transaction_count, bundle_count, txn_bundle_product_count


# ----------------- Main Execution -----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", required=True, help="Start date in format DD-MM-YY")
    parser.add_argument("--end_date", required=True, help="End date in format DD-MM-YY")
    args = parser.parse_args()

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date")

    start_time = time.time()

    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASSWORD,
        ssh_pkey=SSH_PRIVATE_KEY,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=('localhost', 5434),
    ) as tunnel:
        print("✅ SSH tunnel established")

        conn = psycopg2.connect(
            host='localhost',
            port=5434,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        conn_local = psycopg2.connect(
        host='localhost',
        port=5432,
        database="postgres",
        user="postgres",
        password="postgres"
    )
        print("✅ Connected to PostgreSQL")

        cur = conn.cursor() 
        cur_local = conn_local.cursor()

        total_transaction_count = 0
        total_bundle_count = 0
        total_txn_bundle_product_count = 0

        current_date = start_date
        while current_date <= end_date:
            print(f"\n🚀 Processing date: {current_date}")
            txn_count, bundle_count, tbp_count = process_date(current_date, cur, cur_local)
            total_transaction_count += txn_count
            total_bundle_count += bundle_count
            total_txn_bundle_product_count += tbp_count
            current_date += timedelta(days=1)

        cur.close()
        conn.close()
        print("✅ PostgreSQL connection closed")

    elapsed_time = time.time() - start_time
    print(f"⏱️ Total time taken: {elapsed_time:.2f} seconds")
    print(f"📦 Total transaction record count: {total_transaction_count}")
    print(f"🎁 Total bundleTxnLink record count: {total_bundle_count}")
    print(f"🛍️ Total transaction_bundle_product_links record count: {total_txn_bundle_product_count}")

    # python3 ./new_analytics/transaction_pg_to_parquet_new_schema_transaction_working.py --start_date 09-05-25 --end_date 15-08-25
