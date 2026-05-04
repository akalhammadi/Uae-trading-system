from data_provider import get_data


WATCHLIST = [
    "EMAAR",
    "SALIK",
    "DEWA",
    "ALDAR",
    "FAB",
    "ADIB",
    "GFH"
]


def run_scan():
    results = []

    for symbol in WATCHLIST:
        data = get_data(symbol)

        if not data:
            results.append({
                "symbol": symbol,
                "has_data": False,
                "action": "NO_DATA"
            })
            continue

        last = data[-1]["close"]
        prev = data[-2]["close"]

        action = "BUY" if last > prev else "SELL"

        results.append({
            "symbol": symbol,
            "has_data": True,
            "price": last,
            "action": action
        })

    return results
