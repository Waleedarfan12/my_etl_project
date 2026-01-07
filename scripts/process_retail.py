import pandas as pd
import os

# Paths
raw_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/raw"
processed_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed/retail_processed.parquet"
os.makedirs(processed_dir, exist_ok=True)

# Read all CSV files from raw retail folder
all_files = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith(".csv")]
df_list = [pd.read_csv(f) for f in all_files]

# Combine into one DataFrame
df = pd.concat(df_list, ignore_index=True)

# ✅ Standardize column names (adjust based on your raw schema)
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# ✅ Ensure date column is datetime
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# ✅ Handle missing values
df['gender'] = df['gender'].fillna("Unknown")
df['age'] = df['age'].fillna(df['age'].median())
df['product_category'] = df['product_category'].fillna("Unknown")
df['quantity'] = df['quantity'].fillna(0)
df['price_per_unit'] = df['price_per_unit'].fillna(0)
df['total_amount'] = df['total_amount'].fillna(df['quantity'] * df['price_per_unit'])

# ✅ Add derived fields
df['day_of_week'] = df['date'].dt.day_name()
df['month'] = df['date'].dt.month
df['season'] = df['month'].map({
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Autumn", 10: "Autumn", 11: "Autumn"
})

# ✅ Save to processed folder
output_file = os.path.join(processed_dir, "retail_processed.parquet")
df.to_parquet(output_file, index=False)

print(f"✅ Saved processed retail sales → {output_file}")
print(df.head())