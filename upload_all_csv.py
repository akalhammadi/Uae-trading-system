import os
import csv
import time
import requests
from datetime import datetime

URL = "https://uae-market-production.up.railway.app/api/upload-batch"

FILES = {
    "EMAAR": "EMAAR.csv",
    "DIB": "DIB.csv",
    "SALIK": "SALIK.csv",
    "DEWA": "DEWA.csv",
    "ALDAR": "ALDAR.csv",
    "FAB": "FAB.csv",
    "ADNOCDRILL": "ADNOCDRILL.csv",
    "ADNOCGAS": "ADNOCGAS.csv",
    "ADNOCLS": "ADNOCLS.csv",
    "ADNOCDIST": "ADNOCDIST.csv",
    "PRESIGHT": "PRESIGHT.csv",
    "BOROUGE": "BOROUGE.csv",
    "TAQA": "TAQA.csv",
    "EAND": "EAND.csv",
    "DU": "DU.csv",
    "AIRARABIA": "AIRARABIA.csv",
    "DFM": "DFM.csv",
    "EMAARDEV": "EMAARDEV.csv",
    "ARAMEX": "ARAMEX.csv",
    "GFH": "GFH.csv",
    "SHUAA": "SHUAA.csv",
    "RAKPROP": "RAKPROP.csv",
    "RAKBANK": "RAKBANK.csv",
    "AJMANBANK": "AJMANBANK.csv",
    "ENBD": "ENBD.csv",
    "MASQ": "MASQ.csv",
    "NMDC": "NMDC.csv",
    "ADPORTS": "ADPORTS.csv",
    "ADIB": "ADIB.csv",
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
    "APEX": "APEX.csv",
}

CHUNK_SIZE = 200


def clean_num(x):
    if x is None:
        return None

    x = str(x).strip().replace(",", "")

    if x in ["", "-", "nan", "None"]:
        return None

    if x.endswith("M"):
        return float(x[:-1]) * 1_000_000

    if x.endswith("K"):
        return float(x[:-1]) * 1_000

    return float(x)


def clean_date(x):
    x = str(x).strip()

    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(x, fmt).strftime("%Y-%m-%d")
        except:
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

            rows.append({
                "date": clean_date(raw_date),
                "open": clean_num(row.get("Open") or row.get("open")),
                "high": clean_num(row.get("High") or row.get("high")),
                "low": clean_num(row.get("Low") or row.get("low")),
                "close": clean_num(close),
                "volume": clean_num(row.get("Vol.") or row.get("Volume") or row.get("volume")),
            })

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

    symbol_ok = True

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]

        payload = {
            "symbol": symbol,
            "exchange": "HISTORICAL",
            "timeframe": "1D",
            "rows": chunk
        }

        try:
            response = requests.post(URL, json=payload, timeout=60)

            if response.status_code == 200:
                print(f"✅ {symbol}: chunk {i//CHUNK_SIZE + 1} uploaded", flush=True)
            else:
                print(f"❌ {symbol}: {response.status_code} {response.text[:300]}", flush=True)
                symbol_ok = False
                break

        except Exception as e:
            print(f"❌ {symbol}: {e}", flush=True)
            symbol_ok = False
            break

        time.sleep(1)

    if symbol_ok:
        success.append(symbol)
    else:
        failed.append(symbol)

    time.sleep(3)

print("\n🔥 Upload Summary", flush=True)
print(f"✅ Success: {len(success)} → {success}", flush=True)
print(f"❌ Failed: {len(failed)} → {failed}", flush=True)
print(f"📁 Missing: {len(missing)} → {missing}", flush=True)
print("🚀 Done", flush=True)
