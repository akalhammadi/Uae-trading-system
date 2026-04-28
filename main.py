from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg2
import psycopg2.extras
from datetime import datetime
import os

SECRET = os.getenv("SECRET", "abc123")
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="UAE Trading Webhook System")


def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        exchange TEXT,
        timeframe TEXT NOT NULL,
        bar_time TEXT NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        received_at TEXT NOT NULL,
        UNIQUE(symbol, timeframe, bar_time)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        exchange TEXT,
        timeframe TEXT,
        signal TEXT NOT NULL,
        price DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        bar_time TEXT,
        received_at TEXT NOT NULL,
        payload TEXT
    )
    """)

    conn.commit()
    conn.close()


init_db()


class TVPayload(BaseModel):
    secret: str
    type: str
    symbol: str
    exchange: Optional[str] = None
    timeframe: Optional[str] = None
    time: Optional[str] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    signal: Optional[str] = None
    price: Optional[float] = None


SYMBOL_MAP = {
    "ARAMEX": "ARMX",
}

def normalize_symbol(symbol: str):
    if not symbol:
        return symbol

    symbol = symbol.upper().strip()

    if ":" in symbol:
        symbol = symbol.split(":")[-1]

    return SYMBOL_MAP.get(symbol, symbol)


@app.post("/webhook/tradingview")
def tradingview_webhook(payload: TVPayload):
    if payload.secret != SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    now = datetime.utcnow().isoformat()
    symbol = normalize_symbol(payload.symbol)

    conn = db()
    cur = conn.cursor()

    if payload.type == "CANDLE_UPDATE":
        cur.execute("""
        INSERT INTO candles
        (symbol, exchange, timeframe, bar_time, open, high, low, close, volume, received_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timeframe, bar_time)
        DO UPDATE SET
            exchange = EXCLUDED.exchange,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            received_at = EXCLUDED.received_at
        """, (
            symbol,
            payload.exchange,
            payload.timeframe or "60",
            payload.time or now,
            payload.open,
            payload.high,
            payload.low,
            payload.close,
            payload.volume,
            now
        ))

        conn.commit()
        conn.close()
        return {
            "status": "ok",
            "stored": "candle",
            "original_symbol": payload.symbol,
            "symbol": symbol
        }

    if payload.type == "SIGNAL":
        cur.execute("""
        INSERT INTO signals
        (symbol, exchange, timeframe, signal, price, volume, bar_time, received_at, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol,
            payload.exchange,
            payload.timeframe,
            payload.signal or "UNKNOWN",
            payload.price or payload.close,
            payload.volume,
            payload.time,
            now,
            payload.model_dump_json()
        ))

        conn.commit()
        conn.close()
        return {
            "status": "ok",
            "stored": "signal",
            "original_symbol": payload.symbol,
            "symbol": symbol
        }

    conn.close()
    return {
        "status": "ignored",
        "reason": "unknown type",
        "original_symbol": payload.symbol,
        "symbol": symbol
    }

@app.get("/api/candles/latest")
def latest_candles(symbol: Optional[str] = None, timeframe: Optional[str] = None, limit: int = 100):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    q = "SELECT * FROM candles WHERE 1=1"
    params = []

    if symbol:
        q += " AND symbol = %s"
        params.append(symbol)

    if timeframe:
        q += " AND timeframe = %s"
        params.append(timeframe)

    q += " ORDER BY bar_time DESC LIMIT %s"
    params.append(limit)

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    return {"count": len(rows), "candles": rows}
    
@app.get("/api/signals/latest")
def latest_signals(symbol: Optional[str] = None, limit: int = 50):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    q = "SELECT * FROM signals WHERE 1=1"
    params = []

    if symbol:
        q += " AND symbol = %s"
        params.append(symbol)

    q += " ORDER BY received_at DESC LIMIT %s"
    params.append(limit)

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    return {"count": len(rows), "signals": rows}


@app.get("/api/dashboard")
def dashboard():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT DISTINCT ON (symbol, timeframe)
            symbol, exchange, timeframe, bar_time, close, volume
        FROM candles
        ORDER BY symbol, timeframe, bar_time DESC
    """)
    rows = cur.fetchall()

    cur.execute("""
        SELECT symbol, signal, price, bar_time, received_at
        FROM signals
        ORDER BY received_at DESC
        LIMIT 20
    """)
    signals = cur.fetchall()

    conn.close()

    return {
        "latest_candles": rows,
        "latest_signals": signals
    }


@app.get("/api/market-status")
def market_status():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT symbol, timeframe, bar_time, open, high, low, close, volume
        FROM candles
        WHERE close IS NOT NULL
        ORDER BY symbol, bar_time DESC
    """)
    rows = cur.fetchall()
    conn.close()

    latest = {}
    history = {}

    for r in rows:
        symbol = r["symbol"]
        history.setdefault(symbol, []).append(r)

        if symbol not in latest:
            latest[symbol] = r

    results = []

    for symbol, current in latest.items():
        data = history.get(symbol, [])

        last_close = current["close"]
        current_volume = current["volume"] or 0

        prev_close = data[1]["close"] if len(data) > 1 else None

        if prev_close is None:
            prev_close = current["open"] if current["open"] is not None else last_close

        volumes = [x["volume"] for x in data[:5] if x["volume"] is not None]
        avg_volume = sum(volumes) / len(volumes) if volumes else current_volume

        change_pct = ((last_close - prev_close) / prev_close) * 100 if prev_close else 0
        volume_ratio = current_volume / avg_volume if avg_volume else 1

        highs = [x["high"] for x in data[1:6] if x["high"] is not None]
        recent_high = max(highs) if highs else last_close
        breakout = last_close > recent_high

        score = 0

        if breakout:
            score += 40
        if change_pct > 0.3:
            score += 20
        if change_pct > 1:
            score += 20
        if volume_ratio > 1.1:
            score += 20
        if volume_ratio > 1.3:
            score += 20

        status = "Neutral"
        if score >= 60:
            status = "Strong"
        elif score >= 30:
            status = "Watch"
        elif score < 15:
            status = "Weak"

        results.append({
            "symbol": symbol,
            "close": last_close,
            "change_pct": round(change_pct, 2),
            "volume": current_volume,
            "volume_ratio": round(volume_ratio, 2),
            "breakout": breakout,
            "recent_high": recent_high,
            "score": score,
            "status": status,
            "bar_time": current["bar_time"]
        })

    results = sorted(results, key=lambda x: x["score"], reverse=True)

    strong_count = len([x for x in results if x["score"] >= 60])

    market_mode = "Defensive"
    if strong_count >= 8:
        market_mode = "Aggressive"
    elif strong_count >= 4:
        market_mode = "Neutral"

    return {
        "market_mode": market_mode,
        "strong_count": strong_count,
        "top_stocks": results[:10],
        "all_stocks": results
    }

from fastapi import UploadFile, File
import csv
import io
from datetime import datetime

@app.post("/api/upload-csv")
async def upload_csv(file: UploadFile = File(...), symbol: str = "UNKNOWN"):
    content = await file.read()
    decoded = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(decoded))

    conn = db()
    cur = conn.cursor()

    inserted = 0

    for row in reader:
        raw_date = row.get("Date") or row.get("date")
        
        try:
            date = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
        except:
            date = raw_date
        
    
        open_price = row.get("Open") or row.get("open")
        high = row.get("High") or row.get("high")
        low = row.get("Low") or row.get("low")
        close = row.get("Price") or row.get("Close") or row.get("close")
        volume = row.get("Vol.") or row.get("Volume") or row.get("volume")

        if not date or not close:
            continue

        def clean_num(x):
            if x is None:
                return None
            return str(x).replace(",", "").replace("M", "000000").replace("K", "000").strip()

        cur.execute("""
            INSERT INTO candles
            (symbol, exchange, timeframe, bar_time, open, high, low, close, volume, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timeframe, bar_time)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                received_at = EXCLUDED.received_at
        """, (
            symbol,
            "HISTORICAL",
            "1D",
            date,
            float(clean_num(open_price)) if open_price else None,
            float(clean_num(high)) if high else None,
            float(clean_num(low)) if low else None,
            float(clean_num(close)),
            float(clean_num(volume)) if volume else None,
            datetime.utcnow().isoformat()
        ))

        inserted += 1

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "symbol": symbol,
        "rows_inserted": inserted
    }

from pydantic import BaseModel
from typing import List, Optional
import psycopg2.extras
from datetime import datetime

class CandleRow(BaseModel):
    date: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float
    volume: Optional[float] = None

class BatchUpload(BaseModel):
    symbol: str
    exchange: str = "HISTORICAL"
    timeframe: str = "1D"
    rows: List[CandleRow]

@app.post("/api/upload-batch")
def upload_batch(payload: BatchUpload):
    if not payload.rows:
        return {"status": "empty", "inserted": 0}

    conn = db()
    cur = conn.cursor()

    now = datetime.utcnow().isoformat()

    values = [
        (
            payload.symbol,
            payload.exchange,
            payload.timeframe,
            r.date,
            r.open,
            r.high,
            r.low,
            r.close,
            r.volume,
            now
        )
        for r in payload.rows
    ]

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO candles
        (symbol, exchange, timeframe, bar_time, open, high, low, close, volume, received_at)
        VALUES %s
        ON CONFLICT (symbol, timeframe, bar_time)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            received_at = EXCLUDED.received_at
        """,
        values
    )

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "symbol": payload.symbol,
        "inserted": len(values)
    }

# =========================
# TRADES SYSTEM
# =========================

class TradePayload(BaseModel):
    symbol: str
    qty: float
    price: float
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    note: Optional[str] = None


def init_trades_table():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        qty REAL NOT NULL,
        entry_price REAL,
        stop_loss REAL,
        target REAL,
        sell_price REAL,
        status TEXT NOT NULL,
        opened_at TEXT,
        closed_at TEXT,
        note TEXT
    )
    """)

    conn.commit()
    conn.close()


init_trades_table()


# =========================
# BUY
# =========================
@app.post("/api/trades/buy")
def buy_trade(payload: TradePayload):
    conn = db()
    cur = conn.cursor()

    now = datetime.utcnow().isoformat()
    symbol = normalize_symbol(payload.symbol)

    cur.execute("""
    INSERT INTO trades
    (symbol, qty, entry_price, stop_loss, target, status, opened_at, note)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """, (
        symbol,
        payload.qty,
        payload.price,
        payload.stop_loss,
        payload.target,
        "OPEN",
        now,
        payload.note
    ))

    trade_id = cur.fetchone()[0]

    conn.commit()
    conn.close()

    return {
        "status": "opened",
        "trade_id": trade_id,
        "symbol": symbol,
        "entry": payload.price,
        "stop": payload.stop_loss,
        "target": payload.target
    }


# =========================
# SELL
# =========================
@app.post("/api/trades/sell/{trade_id}")
def sell_trade(trade_id: int, payload: TradePayload):
    conn = db()
    cur = conn.cursor()

    now = datetime.utcnow().isoformat()

    cur.execute("""
    SELECT entry_price, qty, symbol, stop_loss, target
    FROM trades
    WHERE id = %s AND status = 'OPEN'
    """, (trade_id,))

    row = cur.fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Trade not found")

    entry, qty, symbol, stop_loss, target = row

    pnl = (payload.price - entry) * qty
    pnl_pct = ((payload.price - entry) / entry) * 100

    cur.execute("""
    UPDATE trades
    SET sell_price = %s,
        status = 'CLOSED',
        closed_at = %s
    WHERE id = %s
    """, (
        payload.price,
        now,
        trade_id
    ))

    conn.commit()
    conn.close()

    return {
        "status": "closed",
        "symbol": symbol,
        "entry": entry,
        "exit": payload.price,
        "qty": qty,
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
        "stop_loss": stop_loss,
        "target": target
    }


# =========================
# OPEN TRADES
# =========================
@app.get("/api/trades/open")
def open_trades():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
    SELECT *,
    (target - entry_price) AS reward,
    (entry_price - stop_loss) AS risk,
    (target - entry_price) / NULLIF((entry_price - stop_loss),0) AS rr
    FROM trades
    WHERE status = 'OPEN'
    ORDER BY opened_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return {"count": len(rows), "trades": rows}


# =========================
# ALL TRADES
# =========================
@app.get("/api/trades/all")
def all_trades():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
    SELECT *,
    (sell_price - entry_price) * qty AS pnl
    FROM trades
    ORDER BY id DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return {"count": len(rows), "trades": rows}

from fastapi.responses import HTMLResponse
from datetime import timezone, timedelta


def init_daily_recommendations_table():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_recommendations (
        id SERIAL PRIMARY KEY,
        report_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        last_close DOUBLE PRECISION,
        change_pct DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        score DOUBLE PRECISION,
        recommendation TEXT,
        entry DOUBLE PRECISION,
        stop_loss DOUBLE PRECISION,
        target DOUBLE PRECISION,
        risk_reward DOUBLE PRECISION,
        market_phase TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(report_date, symbol)
    )
    """)

    conn.commit()
    conn.close()


init_daily_recommendations_table()


def get_market_phase():
    uae_now = datetime.now(timezone(timedelta(hours=4)))

    if 10 <= uae_now.hour < 15:
        market_phase = "MARKET_OPEN"
    elif uae_now.hour >= 15:
        market_phase = "AFTER_CLOSE"
    else:
        market_phase = "PRE_MARKET"

    return uae_now, market_phase


@app.get("/api/dashboard/daily")
def daily_dashboard():
    uae_now, market_phase = get_market_phase()
    report_date = uae_now.strftime("%Y-%m-%d")

    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT symbol, bar_time, open, high, low, close, volume
        FROM candles
        WHERE timeframe = '1D'
          AND close IS NOT NULL
        ORDER BY symbol, bar_time DESC
    """)

    rows = cur.fetchall()

    from collections import defaultdict
    data = defaultdict(list)

    for r in rows:
        data[r["symbol"]].append(r)

    results = []

    for symbol, candles in data.items():
        if len(candles) < 30:
            continue

        latest = candles[0]
        last_close = latest["close"]
        last_high = latest["high"]
        last_low = latest["low"]
        last_volume = latest["volume"] or 0

        prev_close = candles[1]["close"]
        change_pct = ((last_close - prev_close) / prev_close) * 100 if prev_close else 0

        highs_20 = [c["high"] for c in candles[1:21] if c["high"] is not None]
        lows_10 = [c["low"] for c in candles[1:11] if c["low"] is not None]
        volumes_20 = [c["volume"] for c in candles[1:21] if c["volume"] is not None]

        recent_high = max(highs_20) if highs_20 else last_high
        recent_low = min(lows_10) if lows_10 else last_low
        avg_volume = sum(volumes_20) / len(volumes_20) if volumes_20 else last_volume

        breakout = last_close > recent_high
        volume_confirmed = last_volume > avg_volume * 1.3 if avg_volume else False
        positive_momentum = change_pct > 0.5

        score = 0
        if breakout:
            score += 40
        if volume_confirmed:
            score += 30
        if positive_momentum:
            score += 20
        if last_close > recent_low:
            score += 10

        recommendation = "AVOID"
        if score >= 80:
            recommendation = "BUY"
        elif score >= 60:
            recommendation = "WATCH"
        elif score >= 40:
            recommendation = "HOLD"

        entry = last_close
        stop_loss = recent_low
        risk = entry - stop_loss if entry and stop_loss else None
        target = entry + (risk * 2) if risk and risk > 0 else None
        rr = 2 if target else None

        item = {
            "symbol": symbol,
            "last_close": round(last_close, 3),
            "change_pct": round(change_pct, 2),
            "volume": last_volume,
            "score": score,
            "recommendation": recommendation,
            "entry": round(entry, 3),
            "stop_loss": round(stop_loss, 3) if stop_loss else None,
            "target": round(target, 3) if target else None,
            "risk_reward": rr,
            "breakout": breakout,
            "volume_confirmed": volume_confirmed,
            "bar_time": latest["bar_time"]
        }

        results.append(item)

    results = sorted(results, key=lambda x: x["score"], reverse=True)

    buy_list = [x for x in results if x["recommendation"] == "BUY"]
    watch_list = [x for x in results if x["recommendation"] == "WATCH"]

    market_mode = "Defensive"
    if len(buy_list) >= 5:
        market_mode = "Aggressive"
    elif len(buy_list) >= 2 or len(watch_list) >= 5:
        market_mode = "Neutral"

    # نحفظ history فقط بعد إغلاق السوق
    if market_phase == "AFTER_CLOSE":
        now = datetime.utcnow().isoformat()

        for item in results:
            cur.execute("""
            INSERT INTO daily_recommendations
            (report_date, symbol, last_close, change_pct, volume, score,
             recommendation, entry, stop_loss, target, risk_reward,
             market_phase, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date, symbol)
            DO UPDATE SET
                last_close = EXCLUDED.last_close,
                change_pct = EXCLUDED.change_pct,
                volume = EXCLUDED.volume,
                score = EXCLUDED.score,
                recommendation = EXCLUDED.recommendation,
                entry = EXCLUDED.entry,
                stop_loss = EXCLUDED.stop_loss,
                target = EXCLUDED.target,
                risk_reward = EXCLUDED.risk_reward,
                market_phase = EXCLUDED.market_phase,
                created_at = EXCLUDED.created_at
            """, (
                report_date,
                item["symbol"],
                item["last_close"],
                item["change_pct"],
                item["volume"],
                item["score"],
                item["recommendation"],
                item["entry"],
                item["stop_loss"],
                item["target"],
                item["risk_reward"],
                market_phase,
                now
            ))

        conn.commit()

    conn.close()

    return {
        "report_date": report_date,
        "report_time_uae": uae_now.strftime("%Y-%m-%d %H:%M"),
        "market_phase": market_phase,
        "report_status": "FINAL_AFTER_15:00" if market_phase == "AFTER_CLOSE" else "LIVE_NOT_FINAL",
        "market_mode": market_mode,
        "buy_count": len(buy_list),
        "watch_count": len(watch_list),
        "top_recommendations": results[:10],
        "buy_list": buy_list,
        "watch_list": watch_list,
        "all_stocks": results
    }


@app.get("/api/dashboard/history")
def dashboard_history(limit: int = 200):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT *
        FROM daily_recommendations
        ORDER BY report_date DESC, score DESC
        LIMIT %s
    """, (limit,))

    rows = cur.fetchall()
    conn.close()

    return {"count": len(rows), "history": rows}

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page():
    data = daily_dashboard()

    rows_html = ""

    for item in data["top_recommendations"]:
        rec = item["recommendation"]

        if rec == "BUY":
            badge = "buy"
        elif rec == "WATCH":
            badge = "watch"
        elif rec == "HOLD":
            badge = "hold"
        else:
            badge = "avoid"

        rows_html += f"""
        <tr>
            <td>{item['symbol']}</td>
            <td>{item['last_close']}</td>
            <td>{item['change_pct']}%</td>
            <td>{item['score']}</td>
            <td><span class="badge {badge}">{rec}</span></td>
            <td>{item['entry']}</td>
            <td>{item['stop_loss']}</td>
            <td>{item['target']}</td>
            <td>{item['risk_reward']}</td>
        </tr>
        """

    all_rows_html = ""

    for item in data["all_stocks"]:
        rec = item["recommendation"]

        if rec == "BUY":
            badge = "buy"
        elif rec == "WATCH":
            badge = "watch"
        elif rec == "HOLD":
            badge = "hold"
        else:
            badge = "avoid"

        all_rows_html += f"""
        <tr>
            <td>{item['symbol']}</td>
            <td>{item['last_close']}</td>
            <td>{item['change_pct']}%</td>
            <td>{item['score']}</td>
            <td><span class="badge {badge}">{rec}</span></td>
            <td>{"YES" if item["breakout"] else "NO"}</td>
            <td>{"YES" if item["volume_confirmed"] else "NO"}</td>
            <td>{item['entry']}</td>
            <td>{item['stop_loss']}</td>
            <td>{item['target']}</td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <title>UAE Market Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #0f172a;
                color: #e5e7eb;
                padding: 20px;
            }}
            .card {{
                background: #111827;
                border-radius: 14px;
                padding: 18px;
                margin-bottom: 18px;
                box-shadow: 0 8px 24px rgba(0,0,0,0.3);
                overflow-x: auto;
            }}
            h1 {{
                margin-top: 0;
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
            }}
            .metric {{
                background: #1f2937;
                padding: 14px;
                border-radius: 12px;
            }}
            .metric small {{
                color: #9ca3af;
            }}
            .metric strong {{
                display: block;
                font-size: 24px;
                margin-top: 6px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                padding: 12px;
                border-bottom: 1px solid #374151;
                text-align: left;
                font-size: 14px;
                white-space: nowrap;
            }}
            th {{
                color: #9ca3af;
            }}
            .badge {{
                padding: 6px 10px;
                border-radius: 999px;
                font-weight: bold;
                font-size: 12px;
            }}
            .buy {{ background: #16a34a; color: white; }}
            .watch {{ background: #f59e0b; color: #111827; }}
            .hold {{ background: #3b82f6; color: white; }}
            .avoid {{ background: #6b7280; color: white; }}
        </style>
    </head>
    <body>
        <h1>UAE Market Daily Dashboard</h1>

        <div class="card grid">
            <div class="metric">
                <small>Report Time UAE</small>
                <strong>{data['report_time_uae']}</strong>
            </div>
            <div class="metric">
                <small>Market Phase</small>
                <strong>{data['market_phase']}</strong>
            </div>
            <div class="metric">
                <small>Market Mode</small>
                <strong>{data['market_mode']}</strong>
            </div>
            <div class="metric">
                <small>BUY / WATCH</small>
                <strong>{data['buy_count']} / {data['watch_count']}</strong>
            </div>
        </div>

        <div class="card">
            <h2>Top Recommendations</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Close</th>
                        <th>Change</th>
                        <th>Score</th>
                        <th>Recommendation</th>
                        <th>Entry</th>
                        <th>Stop</th>
                        <th>Target</th>
                        <th>R/R</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>All Market Positions</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Close</th>
                        <th>Change</th>
                        <th>Score</th>
                        <th>Status</th>
                        <th>Breakout</th>
                        <th>Volume</th>
                        <th>Entry</th>
                        <th>Stop</th>
                        <th>Target</th>
                    </tr>
                </thead>
                <tbody>
                    {all_rows_html}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """

    return html
