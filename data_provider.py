import yfinance as yf


# ===== SYMBOL MAP =====

DFM_SYMBOLS = {
    "EMAAR", "EMAARDEV", "DEWA", "SALIK", "TECOM", "DFM", "DU",
    "SHUAA", "GULFNAV", "AJMANBANK", "AIRARABIA", "AMLAK",
    "DTC", "TALABAT", "EMPOWER", "GFH"
}

ADX_SYMBOLS = {
    "ALDAR", "ADNOCGAS", "ADNOCDRILL", "ADNOCDIST", "ADPORTS",
    "BOROUGE", "EAND", "FAB", "ADIB", "RAKBANK", "RAKPROP",
    "NMDC", "TAQA", "JULPHAR", "ESHRAQ", "GHITHA",
    "MANAZEL", "PRESIGHT", "SIB", "UPP", "2POINTZERO",
    "INVICTUS", "MODON", "SPACE42", "PUREHEALTH", "ALEFEDT",
    "EMSTEEL", "DANA", "FERTIGLB", "APEX", "WAHA"
}


def format_symbol(symbol):
    s = str(symbol).upper().strip()

    if s in DFM_SYMBOLS:
        return f"{s}.DU"

    if s in ADX_SYMBOLS:
        return f"{s}.AD"

    return s


# ===== MAIN FUNCTION (النظام يعتمد عليها) =====

def get_data(symbol, interval="1d"):
    try:
        yf_symbol = format_symbol(symbol)

        ticker = yf.Ticker(yf_symbol)

        df = ticker.history(
            period="3mo",
            interval="1d"
        )

        if df is None or df.empty:
            return None

        df = df.reset_index()

        candles = []

        for _, row in df.iterrows():
            candles.append({
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row.get("Volume", 0) or 0)
            })

        return candles

    except Exception as e:
        print("DATA ERROR:", e)
        return None
