import pandas as pd
import json
import os
from datetime import datetime

# Paths
raw_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/raw"
processed_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed/weather_processed.parquet"
os.makedirs(processed_dir, exist_ok=True)

# Collect all JSON files (headlines data)
all_files = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith(".json")]


weather_records = []

for f in all_files:
    with open(f) as infile:
        data = json.load(infile)

        # ✅ Extract fields from weather JSON
        record = {
            "date": pd.to_datetime(data.get("dt"), unit="s").normalize() if data.get("dt") else None,
            "city": data.get("name"),
            "temperature": data.get("main", {}).get("temp"),
            "humidity": data.get("main", {}).get("humidity"),
            "condition": data.get("weather", [{}])[0].get("description")
        }
        weather_records.append(record)

# Convert to DataFrame
df = pd.DataFrame(weather_records)

# ✅ Handle missing values
df["temperature"] = df["temperature"].fillna(df["temperature"].mean())
df["humidity"] = df["humidity"].fillna(df["humidity"].mean())
df["condition"] = df["condition"].fillna("Unknown")

# ✅ Aggregate to daily level
weather_daily = df.groupby("date").agg({
    "temperature": "mean",
    "humidity": "mean",
    "condition": lambda x: x.mode()[0] if not x.mode().empty else "Unknown"
}).reset_index()



# ✅ Save to processed folder
output_file = os.path.join(processed_dir, "weather_processed.parquet")
weather_daily.to_parquet(output_file, index=False)
print(f"✅ Saved processed weather data → {output_file}")
print(weather_daily.head())