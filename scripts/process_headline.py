import pandas as pd
import json
import os
from datetime import datetime
from textblob import TextBlob  # ✅ for sentiment analysis

# Paths
raw_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/raw/web"
processed_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed/headlines_processed.parquet"
os.makedirs(processed_dir, exist_ok=True)

# Collect all headline JSON files (filter by prefix)
all_files = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) 
             if f.startswith("news_headlines") and f.endswith(".json")]

headline_records = []

for f in all_files:
    with open(f) as infile:
        headlines = json.load(infile)

        # Extract timestamp from filename
        timestamp_str = f.split("_")[-1].replace(".json", "")
        try:
            date = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S").date()
        except:
            date = None

        # Build records
        for h in headlines:
            sentiment = TextBlob(h).sentiment.polarity
            if sentiment > 0.1:
                sentiment_label = "Positive"
            elif sentiment < -0.1:
                sentiment_label = "Negative"
            else:
                sentiment_label = "Neutral"

            headline_records.append({
                "date": date,
                "headline_text": h,
                "sentiment": sentiment_label
            })

# Convert to DataFrame
df = pd.DataFrame(headline_records)

# ✅ Handle missing values
df["headline_text"] = df["headline_text"].fillna("Unknown")
df["sentiment"] = df["sentiment"].fillna("Neutral")

# ✅ Save to processed folder
output_file = os.path.join(processed_dir, "headlines_processed.parquet")
df.to_parquet(output_file, index=False)

print(f"✅ Saved processed headlines → {output_file}")
print(df.head())