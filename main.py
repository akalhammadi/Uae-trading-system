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


@app.post("/webhook/tradingview")
def tradingview_webhook(payload: TVPayload):
    if payload.secret != SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    now = datetime.utcnow().isoformat()
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
            payload.symbol,
            payload.exchange,
            payload.timeframe or "1H",
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
        return {"status": "ok", "stored": "candle", "symbol": payload.symbol}

    if payload.type == "SIGNAL":
        cur.execute("""
        INSERT INTO signals
        (symbol, exchange, timeframe, signal, price, volume, bar_time, received_at, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            payload.symbol,
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
        return {"status": "ok", "stored": "signal", "symbol": payload.symbol}

    conn.close()
    return {"status": "ignored", "reason": "unknown type"}


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
