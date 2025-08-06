from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd
from datetime import date, datetime, timedelta
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq

# SSH and DB config
SSH_HOST = 'jump-preprod.yourtoken.io'
SSH_PORT = 22
SSH_USER = 'preprodazuretest'
SSH_PASSWORD = 'XRDXxD1YCezOszI'

DB_HOST = 'pg-instance-temp.postgres.database.azure.com'
DB_PORT = 5432
DB_NAME = 'yrtk-analytics'
DB_USER = 'pintoadminpg321op'
DB_PASSWORD = 'fU~e|d911@71'

BATCH_INTERVAL = timedelta(minutes=10)
BASE_OUTPUT_DIR = 'pgTransaction/bundleTxnLink'

target_date = date(2025, 8, 4)  # Change as needed
next_day = target_date + timedelta(days=1)

# Helper function to fix `data` column
def convert_data_to_json(row):
    if isinstance(row, dict):
        return json.dumps(row)
    elif isinstance(row, str):
        return row  # already JSON
    elif pd.isna(row):
        return json.dumps({})
    else:
        return json.dumps({"_raw": str(row)})

with SSHTunnelForwarder(
    (SSH_HOST, SSH_PORT),
    ssh_username=SSH_USER,
    ssh_password=SSH_PASSWORD,
    remote_bind_address=(DB_HOST, DB_PORT),
) as tunnel:

    print("✅ SSH tunnel established")

    conn = psycopg2.connect(
        host='localhost',
        port=tunnel.local_bind_port,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print("✅ Connected to PostgreSQL")

    cur = conn.cursor()

    # Get min and max timestamps
    cur.execute(
        '''
        SELECT MIN(CAST("timestamp" AS timestamp)), MAX(CAST("timestamp" AS timestamp))
        FROM public.bundle_transaction_links
        WHERE CAST("timestamp" AS timestamp) >= %s AND CAST("timestamp" AS timestamp) < %s
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
            SELECT *
            FROM public.bundle_transaction_links
            WHERE CAST("timestamp" AS timestamp) >= %s AND CAST("timestamp" AS timestamp) < %s
        """, (current_start, current_end))

        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        if rows:
            df = pd.DataFrame(rows, columns=colnames)

            # # Convert 'data' column to JSON strings
            # if 'data' in df.columns:
            #     df['data'] = df['data'].apply(convert_data_to_json)

            # Create output path
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

            # Write to Parquet using PyArrow
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)

            print(f"✅ Wrote {len(df)} rows to {file_path}")
        else:
            print("⚠️ No data in this batch.")

        current_start = current_end
        batch_num += 1

    cur.close()
    conn.close()
    print("✅ PostgreSQL connection closed")
