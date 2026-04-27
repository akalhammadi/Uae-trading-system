from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import sqlite3
from datetime import datetime
from pathlib import Path
import os
SECRET = os.getenv("SECRET", "abc123")

DB_PATH = Path("uae_trading.db")

app = FastAPI(title="UAE Trading Webhook System")

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        exchange TEXT,
        timeframe TEXT NOT NULL,
        bar_time TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        received_at TEXT NOT NULL,
        UNIQUE(symbol, timeframe, bar_time)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        exchange TEXT,
        timeframe TEXT,
        signal TEXT NOT NULL,
        price REAL,
        volume REAL,
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

@app.post("/webhook/tradingview")
def tradingview_webhook(payload: TVPayload):
    if payload.secret != SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    now = datetime.utcnow().isoformat()
    conn = db()
    cur = conn.cursor()

    if payload.type == "CANDLE_UPDATE":
        cur.execute("""
        INSERT OR REPLACE INTO candles
        (symbol, exchange, timeframe, bar_time, open, high, low, close, volume, received_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payload.symbol, payload.exchange, payload.timeframe or "1H",
            payload.time or now, payload.open, payload.high, payload.low,
            payload.close, payload.volume, now
        ))
        conn.commit()
        conn.close()
        return {"status": "ok", "stored": "candle", "symbol": payload.symbol}

    if payload.type == "SIGNAL":
        cur.execute("""
        INSERT INTO signals
        (symbol, exchange, timeframe, signal, price, volume, bar_time, received_at, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payload.symbol, payload.exchange, payload.timeframe,
            payload.signal or "UNKNOWN", payload.price or payload.close,
            payload.volume, payload.time, now, payload.model_dump_json()
        ))
        conn.commit()
        conn.close()
        return {"status": "ok", "stored": "signal", "symbol": payload.symbol}

    conn.close()
    return {"status": "ignored", "reason": "unknown type"}

@app.get("/api/candles/latest")
def latest_candles(symbol: Optional[str] = None, timeframe: Optional[str] = None, limit: int = 100):
    conn = db()
    q = "SELECT * FROM candles WHERE 1=1"
    params = []
    if symbol:
        q += " AND symbol = ?"
        params.append(symbol)
    if timeframe:
        q += " AND timeframe = ?"
        params.append(timeframe)
    q += " ORDER BY bar_time DESC LIMIT ?"
    params.append(limit)
    rows = [dict(r) for r in conn.execute(q, params).fetchall()]
    conn.close()
    return {"count": len(rows), "candles": rows}

@app.get("/api/signals/latest")
def latest_signals(symbol: Optional[str] = None, limit: int = 50):
    conn = db()
    q = "SELECT * FROM signals WHERE 1=1"
    params = []
    if symbol:
        q += " AND symbol = ?"
        params.append(symbol)
    q += " ORDER BY received_at DESC LIMIT ?"
    params.append(limit)
    rows = [dict(r) for r in conn.execute(q, params).fetchall()]
    conn.close()
    return {"count": len(rows), "signals": rows}

@app.get("/api/dashboard")
def dashboard():
    conn = db()
    rows = [dict(r) for r in conn.execute("""
        SELECT symbol, exchange, timeframe, bar_time, close, volume
        FROM candles
        WHERE id IN (
            SELECT MAX(id) FROM candles GROUP BY symbol, timeframe
        )
        ORDER BY symbol
    """).fetchall()]
    signals = [dict(r) for r in conn.execute("""
        SELECT symbol, signal, price, bar_time, received_at
        FROM signals
        ORDER BY received_at DESC LIMIT 20
    """).fetchall()]
    conn.close()
    return {"latest_candles": rows, "latest_signals": signals}

@app.get("/api/market-status")
def market_status():
    conn = db()

    rows = [dict(r) for r in conn.execute("""
        SELECT symbol, timeframe, bar_time, open, high, low, close, volume
        FROM candles
        WHERE close IS NOT NULL
        ORDER BY symbol, bar_time DESC
    """).fetchall()]

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

        score = 0

        if change_pct > 0.3:
            score += 20
        if change_pct > 1:
            score += 20
        if volume_ratio > 1.1:
            score += 20
        if volume_ratio > 1.3:
            score += 20
        if volume_ratio > 1.6:
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
