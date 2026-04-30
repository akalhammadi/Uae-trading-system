import os
import requests
import yfinance as yf
from datetime import datetime

API_BASE = os.getenv("API_BASE", "https://uae-market-production.up.railway.app")
SECRET = os.getenv("SECRET", "abc123")

SYMBOLS = {
    "EMAAR": "EMAAR.DU",
    "DEWA": "DEWA.DU",
    "DIB": "DIB.DU",
    "DFM": "DFM.DU",
    "SALIK": "SALIK.DU",
    "ALDAR": "ALDAR.AD",
    "ADIB": "ADIB.AD",
    "FAB": "FAB.AD",
    "SIB": "SIB.AD",
    "RAKBANK": "RAKBANK.AD",
    "ADNOCLS": "ADNOCLS.AD",
    "PRESIGHT": "PRESIGHT.AD",
    "SPACE42": "SPACE42.AD",
    "EAND": "EAND.AD",
    "TAQA": "TAQA.AD",
    "BOROUGE": "BOROUGE.AD",
    "ADNOCGAS": "ADNOCGAS.AD",
    "ADNOCDRILL": "ADNOCDRILL.AD",
    "ADNOCDIST": "ADNOCDIST.AD",
    "ARMX": "ARMX.AD",
    "INVICTUS": "INVICTUS.AD",
    "ESG": "ESG.AD",
    "ESHRQ": "ESHRAQ.AD",
    "MANAZEL": "MANAZEL.AD",
    "RAKPROP": "RAKPROP.AD",
    "GFH": "GFH.DU",
    "FERTIGLB": "FERTIGLB.AD",
    "NMDC": "NMDC.AD",
    "PARKIN": "PARKIN.DU",
    "TALABAT": "TALABAT.DU",
    "DU": "DU.DU",
    "DTC": "DTC.DU",
    "JULPHAR": "JULPHAR.AD",
    "MODON": "MODON.AD",
    "2POINTZERO": "2POINTZERO.AD",
    "AIRARABIA": "AIRARABIA.DU",
    "GHITHA": "GHITHA.AD",
    "AGTHIA": "AGTHIA.AD",
}

def fetch_symbol(symbol, yf_symbol):
    df = yf.download(
        yf_symbol,
        period="7d",
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    if df is None or df.empty:
        return None

    df = df.dropna()
    last = df.iloc[-1]
    bar_date = df.index[-1].strftime("%Y-%m-%d")

    return {
        "symbol": symbol,
        "exchange": "AUTO_YFINANCE",
        "timeframe": "1D",
        "rows": [
            {
                "date": bar_date,
                "open": float(last["Open"]),
                "high": float(last["High"]),
                "low": float(last["Low"]),
                "close": float(last["Close"]),
                "volume": float(last["Volume"]) if "Volume" in last else None,
            }
        ],
    }

def main():
    ok = 0
    failed = []

    for symbol, yf_symbol in SYMBOLS.items():
        try:
            payload = fetch_symbol(symbol, yf_symbol)

            if not payload:
                failed.append({"symbol": symbol, "reason": "no_data"})
                continue

            r = requests.post(
                f"{API_BASE}/api/upload-batch",
                json=payload,
                timeout=30,
            )

            if r.status_code == 200:
                ok += 1
                print("OK", symbol, payload["rows"][0])
            else:
                failed.append({
                    "symbol": symbol,
                    "status": r.status_code,
                    "text": r.text,
                })

        except Exception as e:
            failed.append({"symbol": symbol, "error": str(e)})

    try:
        requests.post(f"{API_BASE}/api/ai/train", timeout=60)
    except Exception as e:
        failed.append({"ai_train_error": str(e)})

    print({
        "finished_at": datetime.utcnow().isoformat(),
        "uploaded": ok,
        "failed": failed,
    })

if __name__ == "__main__":
    main()
