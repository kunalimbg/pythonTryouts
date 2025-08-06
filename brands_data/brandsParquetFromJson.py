import json
import pandas as pd

# Load JSON data from file
with open("brands_data/brands.json", "r") as file:
    data = json.load(file)

# Extract the list of brand entries
brands = data.get("brand", [])

# Fields to keep
required_fields = [
    "brandDomain",
    "brandName",
    "brandTokenName",
    "shopifyStoreId",
    "shopifyStoreEmail"
]

# Extract required fields from each brand
filtered_brands = [
    {field: brand.get(field) for field in required_fields}
    for brand in brands
]

# Convert to DataFrame
df = pd.DataFrame(filtered_brands)

# Save to Parquet
df.to_parquet("brands2.parquet", engine="pyarrow", index=False)
record_count = len(df)
print(f"✅ Total records to write: {record_count}")

print("✅ Parquet file 'brands.parquet' created successfully!")
