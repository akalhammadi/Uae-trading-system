import os
import json
import time
from datetime import datetime, timezone

import requests

API_BASE = os.getenv("API_BASE", "https://uae-market-production.up.railway.app").rstrip("/")
TIMEOUT = 60


def log(message):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {message}", flush=True)


def get_json(path):
    url = f"{API_BASE}{path}"
    log(f"GET {url}")
    r = requests.get(url, timeout=TIMEOUT)
    log(f"HTTP {r.status_code}")
    if r.status_code != 200:
        log(f"Response preview: {r.text[:500]}")
    r.raise_for_status()
    return r.json()


def main():
    log("🚀 Starting auto_daily_prices.py")
    log(f"API_BASE={API_BASE}")

    # Daily data check: this only reads what is already stored in the main API.
    daily = get_json("/api/candles/latest?timeframe=1D&limit=500")
    candles = daily.get("candles", [])
    log(f"✅ Daily candle rows received: {len(candles)}")

    for row in candles[:30]:
        log(
            f"{row.get('symbol')} | tf={row.get('timeframe')} | date={row.get('bar_time')} | "
            f"close={row.get('close')} | volume={row.get('volume')}"
        )

    time.sleep(2)

    # Generate signals using 1D + 1H only.
    signals = get_json("/api/ai/dual-signals")
    log(f"✅ Dual signals count: {signals.get('count')}")
    log(json.dumps(signals, ensure_ascii=False)[:1500])

    time.sleep(2)

    # Send Telegram alerts if new non-duplicate signals exist.
    alerts = get_json("/api/ai/send-alerts?dry_run=false")
    log(f"✅ Alerts result: sent={alerts.get('sent_count')} skipped={alerts.get('skipped_count')}")
    log(json.dumps(alerts, ensure_ascii=False)[:1500])

    log("✅ Finished successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log("❌ ERROR OCCURRED")
        log(repr(exc))
        raise
