import json
import os
from sshtunnel import SSHTunnelForwarder
import psycopg2


def flatten_json(y, prefix=''):
    out = {}
    for k, v in y.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten_json(v, key))
        else:
            out[key] = v
    return out


def unflatten_json(flat_dict):
    result = {}
    for compound_key, value in flat_dict.items():
        keys = compound_key.split(".")
        d = result
        for key in keys[:-1]:
            if key not in d or not isinstance(d[key], dict):
                d[key] = {}
            d = d[key]
        d[keys[-1]] = value
    return result


def fetch_and_merge_payloads_from_db(
    ssh_host,
    ssh_port,
    ssh_user,
    ssh_key_path,
    db_host,
    db_port,
    db_name,
    db_user,
    db_password,
    output_file="merged_payload.json",
    limit=1000
):
    with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=ssh_key_path,
        remote_bind_address=(db_host, db_port),
        local_bind_address=('localhost', 5434)
    ) as tunnel:
        print("✅ SSH tunnel established")

        conn = psycopg2.connect(
            host='localhost',
            port=5434,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("✅ Connected to PostgreSQL")

        cur = conn.cursor()

        query = f"""
            SELECT "orderPayload"
            FROM public.transaction
            WHERE "orderPayload" IS NOT NULL
            ORDER BY "transactionId"
            LIMIT {limit};
        """
        cur.execute(query)
        rows = cur.fetchall()

        all_keys = set()
        flat_payloads = []

        for i, row in enumerate(rows):
            try:
                payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                flat = flatten_json(payload)
                flat_payloads.append(flat)
                all_keys.update(flat.keys())
            except Exception as e:
                print(f"⚠️ Skipping row #{i} due to error: {e}")

        merged = {}

        for key in all_keys:
            for flat in flat_payloads:
                if key in flat and flat[key] is not None:
                    if key not in merged:
                        merged[key] = flat[key]
                    break
            if key not in merged:
                merged[key] = None

        nested = unflatten_json(merged)

        # Save to file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(nested, f, indent=2)
            print(f"✅ Merged payload with nested keys written to {output_file}")

        cur.close()
        conn.close()
        print("✅ PostgreSQL connection closed")
fetch_and_merge_payloads_from_db(
    ssh_host='jump-production.yourtoken.io',
    ssh_port=22,
    ssh_user='azureuser',
    ssh_key_path='/home/kunal/Downloads/jump-production_key.pem',
    db_host='production-yt-replica.postgres.database.azure.com',
    db_port=5432,
    db_name='postgres',
    db_user='postgres_ARDowsChlogR',
    db_password='6:?wH564}Ueh',
    output_file="merged_order_payload.json",  # or change filename as needed
    limit=1000  # number of payloads to read
)
