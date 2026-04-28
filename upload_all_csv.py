import os
import csv
import time
import requests
from datetime import datetime

URL = "https://uae-market-production.up.railway.app/api/upload-batch"

FILES = {
    "SIB": "SIB.csv",
    "MANAZEL": "MANAZEL.csv",
    "INVICTUS": "INVICTUS.csv",
    "ESHRAQ": "ESHRAQ.csv",
    "2POINTZERO": "2POINTZERO.csv",
    "TALABAT": "TALABAT.csv",
    "PARKIN": "PARKIN.csv",
    "SPACE42": "SPACE42.csv",
    "FERTIGLB": "FERTIGLB.csv",
    "DTC": "DTC.csv",
    "APEX": "APEX.csv"
}

CHUNK_SIZE = 25
SLEEP_BETWEEN_CHUNKS = 5
SLEEP_BETWEEN_SYMBOLS = 10
TIMEOUT = 120


def clean_num(x):
    if x is None:
        return None

    x = str(x).strip().replace(",", "")

    if x in ["", "-", "nan", "None", "null"]:
        return None

    if x.endswith("M"):
        return float(x[:-1]) * 1_000_000

    if x.endswith("K"):
        return float(x[:-1]) * 1_000

    if x.endswith("B"):
        return float(x[:-1]) * 1_000_000_000

    return float(x)


def clean_date(x):
    x = str(x).strip()

    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(x, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    return x


def read_csv_rows(filename):
    rows = []

    with open(filename, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            raw_date = row.get("Date") or row.get("date")
            close = row.get("Price") or row.get("Close") or row.get("close")

            if not raw_date or not close:
                continue

            try:
                item = {
                    "date": clean_date(raw_date),
                    "open": clean_num(row.get("Open") or row.get("open")),
                    "high": clean_num(row.get("High") or row.get("high")),
                    "low": clean_num(row.get("Low") or row.get("low")),
                    "close": clean_num(close),
                    "volume": clean_num(row.get("Vol.") or row.get("Volume") or row.get("volume")),
                }

                if item["close"] is not None:
                    rows.append(item)

            except Exception as e:
                print(f"⚠️ Skipped bad row in {filename}: {e}", flush=True)

    return rows


success = []
failed = []
missing = []

for symbol, filename in FILES.items():
    if not os.path.exists(filename):
        print(f"❌ Missing file: {filename}", flush=True)
        missing.append(symbol)
        continue

    print(f"\n📄 Reading {symbol} from {filename}", flush=True)

    try:
        rows = read_csv_rows(filename)
    except Exception as e:
        print(f"❌ Failed reading {symbol}: {e}", flush=True)
        failed.append(symbol)
        continue

    print(f"📊 {symbol}: {len(rows)} rows", flush=True)

    if len(rows) == 0:
        print(f"⚠️ {symbol}: no valid rows", flush=True)
        failed.append(symbol)
        continue

    symbol_ok = True
    total_chunks = (len(rows) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk_no = i // CHUNK_SIZE + 1
        chunk = rows[i:i + CHUNK_SIZE]

        payload = {
            "symbol": symbol,
            "exchange": "HISTORICAL",
            "timeframe": "1D",
            "rows": chunk
        }

        uploaded = False

        for attempt in range(1, 4):
            try:
                response = requests.post(URL, json=payload, timeout=TIMEOUT)

                if response.status_code == 200:
                    print(f"✅ {symbol}: chunk {chunk_no}/{total_chunks} uploaded", flush=True)
                    uploaded = True
                    break

                print(
                    f"⚠️ {symbol}: chunk {chunk_no}/{total_chunks} attempt {attempt}/3 "
                    f"status {response.status_code} - {response.text[:200]}",
                    flush=True
                )

            except Exception as e:
                print(
                    f"⚠️ {symbol}: chunk {chunk_no}/{total_chunks} attempt {attempt}/3 error → {e}",
                    flush=True
                )

            time.sleep(10)

        if not uploaded:
            print(f"❌ {symbol}: failed at chunk {chunk_no}/{total_chunks}", flush=True)
            symbol_ok = False
            break

        time.sleep(SLEEP_BETWEEN_CHUNKS)

    if symbol_ok:
        success.append(symbol)
    else:
        failed.append(symbol)

    print(f"😴 Cooling after {symbol}...", flush=True)
    time.sleep(SLEEP_BETWEEN_SYMBOLS)

print("\n🔥 Upload Summary", flush=True)
print(f"✅ Success: {len(success)} → {success}", flush=True)
print(f"❌ Failed: {len(failed)} → {failed}", flush=True)
print(f"📁 Missing: {len(missing)} → {missing}", flush=True)
print("🚀 Done", flush=True)
