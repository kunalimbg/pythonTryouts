import os
import time
from azure.storage.blob import BlobServiceClient, ContentSettings

# Azure Blob Storage configuration
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=yrtkstorage;AccountKey=BJCHt2R7+U6H33v/YYZJ9//ntWEYpbnhW6ddPfA7u4Q6nd3wrdF9jKtAoSgl27w1D1tTD6FWjewp+AStfBM6DQ==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "mycont"
# LOCAL_DIRECTORY = "new_analytics/bundleTxnLink/year=2025/month=08/day=07"  # Base local directory
# LOCAL_DIRECTORY = "new_analytics/transaction/year=2025/month=08/day=07"  # Base local directory
LOCAL_DIRECTORY = "new_analytics/txnBundleProductLink/year=2025/month=08/day=07"  # Base local directory
# LOCAL_DIRECTORY = "transaction/year=2025/month=05/day=27"  # Base local directory


# Create the blob service client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

start_time = time.time()

# Create container if not exists
try:
    container_client.create_container()
    print(f"🪣 Created container: {CONTAINER_NAME}")
except Exception:
    print(f"📦 Using existing container: {CONTAINER_NAME}")

# Walk through all files and upload Parquet files
for root, _, files in os.walk(LOCAL_DIRECTORY):
    for file in files:
        if file.endswith(".parquet"):
            local_file_path = os.path.join(root, file)
            # Strip the "transaction/" prefix for blob path
            blob_path = local_file_path.replace("\\", "/")

            print(f"⬆️ Uploading {local_file_path} → {blob_path}")

            with open(local_file_path, "rb") as data:
                container_client.upload_blob(
                    name=blob_path,
                    data=data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type='application/octet-stream')
                )

print("✅ All Parquet files uploaded successfully.")
elapsed_time = time.time() - start_time
print(f"⏱️ Total time taken: {elapsed_time:.2f} seconds")