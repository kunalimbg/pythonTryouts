import json

# Step 1: Read the JSON file
with open('/home/kunal/learn/pythonTryouts/bundles_code/products.bundles.json', 'r') as f:
    documents = json.load(f)

# Step 2: Extract all _id values
ids = [doc["_id"]["$oid"] for doc in documents]

# Step 3: Write to BundlesIds.json
output_data = {
    "BundlesId's": ids
}

with open('BundlesIds.json', 'w') as f:
    json.dump(output_data, f, indent=4)

print("BundlesIds.json created successfully!")
