import requests
import json
import os
from datetime import datetime

# ✅ Replace with your actual API key
API_KEY = "5b9e75b9f678d717c20a9fb28741ea2a"

# ✅ Cities with lat/lon (you can add more)
cities = {
    "lahore": {"lat": 31.5497, "lon": 74.3436},
    "karachi": {"lat": 24.8607, "lon": 67.0011},
    "islamabad": {"lat": 33.6844, "lon": 73.0479},
    "london": {"lat": 51.5072, "lon": -0.1276}
}

# ✅ Folder to save raw API data
output_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/raw"
os.makedirs(output_dir, exist_ok=True)

for city, coords in cities.items():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={coords['lat']}&lon={coords['lon']}&appid={API_KEY}&units=metric"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{city}_weather_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)

        print(f"✅ Saved weather data for {city} → {filepath}")
    else:
        print(f"❌ Failed to fetch data for {city}: {response.status_code}")