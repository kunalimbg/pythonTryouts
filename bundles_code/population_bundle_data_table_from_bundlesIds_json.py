import json
import time
import requests
import psycopg2
from datetime import datetime

# DB config
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres"
}

def extract_shopify_id(gid):
    """Extract numeric product ID from Shopify GID"""
    return gid.strip().split("/")[-1]

def fetch_and_insert_bundle(bundle_id, cursor, failed_bundles):
    url = f"https://products.yourtoken.io/bundles?id={bundle_id}&status=all"

    try:
        response = requests.get(url, data='')
        response.raise_for_status()
    except Exception as e:
        print(f"[API ERROR] Bundle ID {bundle_id}: {e}")
        failed_bundles.append(bundle_id)
        return

    try:
        bundles = response.json()
    except json.JSONDecodeError as e:
        print(f"[JSON ERROR] Bundle ID {bundle_id}: {e}")
        failed_bundles.append(bundle_id)
        return

    try:
        for bundle in bundles:
            bundle_id_db = bundle.get("_id")
            for product in bundle.get("products", []):
                pd = product.get("productDetails", {})
                product_id = extract_shopify_id(pd.get("id", ""))
                product_name = pd.get("title", "")
                product_url = pd.get("onlineStoreUrl", "")
                timestamp = datetime.now()

                if product_id:
                    cursor.execute(
                        """
                        INSERT INTO bundle_data (
                            bundle_id, product_id, product_name, product_url, timestamp, bundle_version
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (bundle_id, product_id, bundle_version) DO NOTHING
                        """,
                        (bundle_id_db, product_id, product_name, product_url, timestamp, "v1")
                    )
                    print(f"✅ Inserted product {product_id} from bundle {bundle_id_db}")
    except Exception as db_err:
        print(f"[DB ERROR] Bundle ID {bundle_id}: {db_err}")
        failed_bundles.append(bundle_id)

def main():
    start_time = time.time()
    failed_bundles = []

    # Load bundle IDs
    with open("BundlesIds.json") as f:
        data = json.load(f)
        bundle_ids = data.get("BundlesId's", [])

    print(f"📦 Loaded {len(bundle_ids)} bundle IDs from JSON.")

    # Connect to DB
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for bundle_id in bundle_ids:
        fetch_and_insert_bundle(bundle_id, cursor, failed_bundles)

    conn.commit()
    cursor.close()
    conn.close()

    # Save failed bundle IDs to JSON
    if failed_bundles:
        with open("FailedToInsertBundleDataId.json", "w") as out_file:
            json.dump({"failed_bundle_ids": failed_bundles}, out_file, indent=2)
        print(f"❌ Saved {len(failed_bundles)} failed bundle IDs to FailedToInsertBundleDataId.json")
    else:
        print("✅ All bundles processed successfully.")

    elapsed_time = time.time() - start_time
    print(f"⏱️ Total time taken: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()
