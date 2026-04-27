import os
import time
import requests

URL = "https://uae-market-production.up.railway.app/api/upload-csv"

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

success = []
failed = []
missing = []

for symbol, filename in FILES.items():
    if not os.path.exists(filename):
        print(f"❌ Missing file: {filename}", flush=True)
        missing.append(symbol)
        continue

    uploaded = False

    for attempt in range(1, 4):
        print(f"⬆️ Uploading {symbol} | attempt {attempt}/3", flush=True)

        try:
            with open(filename, "rb") as f:
                response = requests.post(
                    URL,
                    params={"symbol": symbol},
                    files={"file": f},
                    timeout=90
                )

            if response.status_code == 200:
                print(f"✅ {symbol}: {response.json()}", flush=True)
                success.append(symbol)
                uploaded = True
                break

            print(f"⚠️ {symbol}: {response.status_code} - {response.text[:300]}", flush=True)

        except Exception as e:
            print(f"⚠️ {symbol}: {e}", flush=True)

        print("⏳ waiting 20 seconds before retry...", flush=True)
        time.sleep(20)

    if not uploaded:
        failed.append(symbol)

    print("😴 cooling server 15 seconds...", flush=True)
    time.sleep(15)

print("\n🔥 Upload Summary", flush=True)
print(f"✅ Success: {len(success)} → {success}", flush=True)
print(f"❌ Failed: {len(failed)} → {failed}", flush=True)
print(f"📁 Missing: {len(missing)} → {missing}", flush=True)
print("🚀 Done", flush=True)
