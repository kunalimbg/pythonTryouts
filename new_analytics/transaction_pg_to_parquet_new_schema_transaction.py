from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd
import json
from datetime import date, datetime, timedelta
import os
from decimal import Decimal, ROUND_HALF_UP

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

def calculate_bundle_sales_and_subtotal(line_items, payload=None):
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
        return {
            # "id": payload.get("id"),
            "transaction_id": payload.get("id"),
            "name": "transaction",
            "timestamp": payload.get("created_at"),
            "data": payload,
            "shopify_store_id": extract_shopify_store_id(payload),
            "currency": payload.get("presentment_currency"),
            "transaction_amount": payload.get("current_total_price"),
            "discount_amount": payload.get("current_total_discounts"),
            "sub_total_amount": calculate_bundle_sales_and_subtotal(payload.get("line_items", []), payload)[1],
        }
    except Exception as e:
        print(f"⚠️ Skipping row due to error: {e}")
        return None

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
    print("✅ Connected to PostgreSQL")

    cur = conn.cursor()

    # Get min and max of createdAt
    target_date = date(2025, 8, 5)  # Change this to your desired date
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
    print(f"📅 Time range for {target_date}: {min_ts} to {max_ts}")

    current_start = min_ts
    batch_num = 1

    while current_start < max_ts:
        current_end = current_start + BATCH_INTERVAL
        print(f"⏳ Batch {batch_num}: {current_start} → {current_end}")

        cur.execute("""
            SELECT "orderPayload", "createdAt"
            FROM public.transaction
            WHERE "orderPayload" IS NOT NULL
              AND "createdAt" >= %s AND "createdAt" < %s
                    limit 1;
        """, (current_start, current_end))

        rows = cur.fetchall()
        batch_data = [wrap_row(row) for row in rows]
        batch_data = [row for row in batch_data if row is not None]

        if batch_data:
            df = pd.DataFrame(batch_data)
            df["data"] = df["data"].apply(json.dumps) # Convert dict to JSON string for storage

            # Create directory structure based on current_start timestamp
            dir_path = os.path.join(
                BASE_OUTPUT_DIR,
                f"year={current_start.year}",
                f"month={current_start.month:02}",
                f"day={current_start.day:02}",
                f"hour={current_start.hour:02}"
            )
            os.makedirs(dir_path, exist_ok=True)

            # Create file name with minute included
            filename = f"order_payload_{current_start.hour:02}_{current_start.minute:02}.parquet"
            file_path = os.path.join(dir_path, filename)

            df.to_parquet(file_path, index=False)
            print(f"✅ Wrote {len(df)} rows to {file_path}")
        else:
            print("⚠️ No data in this batch.")

        current_start = current_end
        batch_num += 1

    cur.close()
    conn.close()
    print("✅ PostgreSQL connection closed")
