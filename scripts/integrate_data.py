import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
import os

# Ensure logs folder exists
os.makedirs("logs", exist_ok=True)

# Configure logging
log_file = os.path.join("logs", "etl.log")

logging.basicConfig(
    level=logging.INFO,  # INFO, DEBUG, WARNING, ERROR
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5),
        logging.StreamHandler()  # also print to console
    ]
)

logger = logging.getLogger("ETL")
processed_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed"

# Load processed files
retail = pd.read_parquet(os.path.join(processed_dir, "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed/retail_processed.parquet/retail_processed.parquet"))
weather = pd.read_parquet(os.path.join(processed_dir, "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed/weather_processed.parquet/weather_processed.parquet"))
headlines = pd.read_parquet(os.path.join(processed_dir, "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed/headlines_processed.parquet/headlines_processed.parquet"))

for df in [retail, weather, headlines]:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

# ✅ Sort by date (required for merge_asof)
retail = retail.sort_values("date")
weather = weather.sort_values("date")
headlines = headlines.sort_values("date")

# ✅ Drop rows with null dates (important for merge_asof)
retail = retail.dropna(subset=['date']).sort_values("date")
weather = weather.dropna(subset=['date']).sort_values("date")
headlines = headlines.dropna(subset=['date']).sort_values("date")


# ✅ Merge retail with nearest weather record
df = pd.merge_asof(
    retail,
    weather,
    on="date",
    direction="nearest"   # can be "backward", "forward", or "nearest"
)

# ✅ Merge headlines also by nearest date
df = pd.merge_asof(
    df,
    headlines,
    on="date",
    direction="nearest"
)

# Save integrated dataset
output_file = os.path.join(processed_dir, "integrated_dataset.parquet")
df.to_parquet(output_file, index=False)

logger.info(f"✅ Integrated dataset saved → {output_file}")
logger.info(df.head())

# Save integrated dataset
output_file = os.path.join(processed_dir, "integrated_dataset.parquet")
df.to_parquet(output_file, index=False)

logger.info(f"✅ Integrated dataset saved → {output_file}")
logger.info(df.head())