import requests
from bs4 import BeautifulSoup  # type: ignore
import os 
import json
from datetime import datetime

url = "https://www.cnbc.com/business/"
response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
soup = BeautifulSoup(response.text, "html.parser")

# CNBC headlines are inside divs with class 'Card-titleContainer'
headlines = [h.get_text(strip=True) for h in soup.select("div.Card-titleContainer")]

print(len(headlines))
print(headlines[:5])
# ✅ Save to raw folder
output_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/raw/web"
os.makedirs(output_dir, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"news_headlines_{timestamp}.json"
filepath = os.path.join(output_dir, filename)

# ✅ Write headlines to JSON file
with open(filepath, "w") as f:
    json.dump(headlines, f, indent=4)

print(f"✅ Saved {len(headlines)} headlines → {filepath}")
