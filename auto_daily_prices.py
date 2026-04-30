import requests
import pandas as pd
from datetime import datetime

print("🚀 Starting auto_daily_prices script...")

API_URL = "https://uae-market-production.up.railway.app/api/candles/latest"

try:
    print("📡 Fetching data from API...")
    res = requests.get(API_URL)
    data = res.json()

    print(f"✅ Received {len(data.get('prices', []))} records")

    df = pd.DataFrame(data["prices"])

    print("📊 Data sample:")
    print(df.head())

    # حفظ CSV
    filename = f"daily_prices_{datetime.now().date()}.csv"
    df.to_csv(filename, index=False)

    print(f"💾 Saved CSV: {filename}")

    print("🎯 Script finished successfully")

except Exception as e:
    print("❌ ERROR OCCURRED:")
    print(str(e))
