import pandas as pd

# Input files
file1 = "csv1.csv"
file2 = "cvs2.csv"
output = "non_matches.csv"

# Read CSV files
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Ensure 'order_id' column exists
if "order_id" not in df1.columns or "transaction_id" not in df2.columns:
    raise ValueError("Both CSVs must have an 'order_id' column.")

# Get rows from file1 where order_id is NOT in file2
non_matches = df1[~df1["order_id"].isin(df2["transaction_id"])]

# Save result
non_matches.to_csv(output, index=False)

print(f"✅ Non-matching rows from file1 saved to {output}")