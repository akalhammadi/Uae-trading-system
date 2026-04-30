import os
import requests
from datetime import datetime

API_BASE = os.getenv("API_BASE", "https://uae-market-production.up.railway.app")
API_URL = f"{API_BASE.rstrip('/')}/api/candles/daily-latest"

print("🚀 Starting auto_daily_prices.py")
print("🌐 API_URL =", API_URL)

try:
    print("📡 Fetching data from API...")
    response = requests.get(API_URL, timeout=60)

    print("HTTP status:", response.status_code)
    print("Response preview:", response.text[:500])

    response.raise_for_status()
    data = response.json()

    prices = data.get("prices", [])

    print(f"✅ Received {len(prices)} records")

    if not prices:
        print("⚠️ No prices received. Check /api/candles/daily-latest output.")
        raise SystemExit(0)

    print("📊 Latest prices:")
    for item in prices:
        print(
            item.get("symbol"),
            item.get("bar_time"),
            "close=",
            item.get("close"),
            "volume=",
            item.get("volume"),
        )

    print("✅ Finished successfully at", datetime.utcnow().isoformat())

except Exception as e:
    print("❌ ERROR OCCURRED:")
    print(repr(e))
    raise
