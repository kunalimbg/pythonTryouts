import requests
import json
import time

# Config
json_file_path = "BundlesIds.json"
failed_json_file = "FailedBundles1.json"
api_url = "http://localhost:8080/store-bundle"
batch_size = 100

def load_bundle_ids(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data.get("BundlesId's", [])

def process_batches(ids, batch_size):
    failed_ids = []

    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1}: {len(batch)} items")
        for bundle_id in batch:
            payload = {"bundleId": bundle_id}
            try:
                response = requests.post(api_url, json=payload)
                if response.status_code == 200:
                    print(f"✅ Success for ID {bundle_id}: {response.json()}")
                else:
                    print(f"❌ Error for ID {bundle_id}: {response.status_code} {response.text}")
                    failed_ids.append(bundle_id)
            except Exception as e:
                print(f"⚠️ Exception for ID {bundle_id}: {e}")
                failed_ids.append(bundle_id)
            time.sleep(0.1)  # Optional: avoid overwhelming the server

    # Write failed IDs to JSON
    if failed_ids:
        with open(failed_json_file, 'w') as f:
            json.dump({"FailedBundles": failed_ids}, f, indent=4)
        print(f"❌ Failed IDs written to {failed_json_file}")

# Main
if __name__ == "__main__":
    bundle_ids = load_bundle_ids(json_file_path)
    if not bundle_ids:
        print("No bundle IDs found in the JSON file.")
    else:
        process_batches(bundle_ids, batch_size)
