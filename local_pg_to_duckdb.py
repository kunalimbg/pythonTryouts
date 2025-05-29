from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd
import json
from datetime import date, datetime, timedelta
import os

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
BASE_OUTPUT_DIR = 'transaction'

def wrap_row(row):
    try:
        payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        now_utc = datetime.utcnow().isoformat() + "Z"
        return {
            "id": payload.get("id"),
            "name": "transaction",
            "timestamp": payload.get("createdAt"),
            "data": payload,
            "EventProcessedUtcTime": None,
            "PartitionId": None,
            "EventEnqueuedUtcTime": None
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
    target_date = date(2025, 5, 28)  # Change this to your desired date
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
              AND "createdAt" >= %s AND "createdAt" < %s;
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
