import os
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

success = 0
failed = 0

for symbol, filename in FILES.items():
    if not os.path.exists(filename):
        print(f"❌ Missing file: {filename}", flush=True)
        failed += 1
        continue

    print(f"⬆️ Uploading {symbol} from {filename}...", flush=True)

    try:
        with open(filename, "rb") as f:
            response = requests.post(
                URL,
                params={"symbol": symbol},
                files={"file": f},
                timeout=20
            )

        if response.status_code == 200:
            print(f"✅ {symbol}: {response.json()}", flush=True)
            success += 1
        else:
            print(f"❌ {symbol}: {response.status_code} - {response.text[:300]}", flush=True)
            failed += 1

    except Exception as e:
        print(f"❌ {symbol}: Error → {e}", flush=True)
        failed += 1

print("\n🔥 Upload Summary", flush=True)
print(f"✅ Success: {success}", flush=True)
print(f"❌ Failed: {failed}", flush=True)
print("🚀 Done", flush=True)
