import yfinance as yf
import pandas as pd


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


SPECIAL_YAHOO_MAP = {
    "GFH": "GFH.DU",          # GFH في DFM
    "EMAAR": "EMAAR.DU",
    "ALDAR": "ALDAR.AD",
    "EAND": "EAND.AD",
    "2POINTZERO": "2POINTZERO.AD",
}


def normalize_symbol(symbol: str) -> str:
    return str(symbol).upper().strip().replace(" ", "").replace("-", "")


def format_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)

    if s in SPECIAL_YAHOO_MAP:
        return SPECIAL_YAHOO_MAP[s]

    if s in DFM_SYMBOLS:
        return f"{s}.DU"

    if s in ADX_SYMBOLS:
        return f"{s}.AD"

    return s


def normalize_interval(interval: str) -> str:
    i = str(interval).lower().strip()

    if i in ["60", "1h", "hour", "hourly"]:
        return "60m"

    if i in ["1d", "day", "daily", "1day"]:
        return "1d"

    return i


def default_period(interval: str) -> str:
    i = normalize_interval(interval)

    if i in ["60m", "30m", "15m"]:
        return "3mo"

    return "1y"


def fetch_data(symbol: str, interval: str = "1d", period: str | None = None):
    yf_symbol = format_symbol(symbol)
    yf_interval = normalize_interval(interval)
    yf_period = period or default_period(yf_interval)

    try:
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(
            period=yf_period,
            interval=yf_interval,
            auto_adjust=False
        )

        if df is None or df.empty:
            return None

        df = df.reset_index()

        df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            },
            inplace=True
        )

        df["symbol"] = symbol
        df["yf_symbol"] = yf_symbol

        return df

    except Exception as e:
        print(f"DATA ERROR {symbol} -> {yf_symbol}: {e}")
        return None


def fetch_candles(symbol: str, interval: str = "1d", period: str | None = None):
    df = fetch_data(symbol, interval, period)

    if df is None or df.empty:
        return []

    candles = []

    for _, row in df.iterrows():
        candles.append({
            "symbol": symbol,
            "provider_symbol": row.get("yf_symbol"),
            "datetime": str(row.iloc[0]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row.get("volume", 0) or 0),
        })

    return candles


def debug_symbol(symbol: str):
    yf_symbol = format_symbol(symbol)
    df = fetch_data(symbol)

    return {
        "symbol": symbol,
        "yf_symbol": yf_symbol,
        "has_data": df is not None and not df.empty,
        "rows": 0 if df is None else len(df),
        "last_close": None if df is None or df.empty else float(df["close"].iloc[-1]),
    }
