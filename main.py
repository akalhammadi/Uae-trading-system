from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg2
import psycopg2.extras
from datetime import datetime
import os
import ai_engine

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
    dead_stocks = []

    for symbol, candles in data.items():
        if len(candles) < 30:
            continue

        latest = candles[0]
        last_close = latest["close"]
        last_volume = latest["volume"] or 0
        prev_close = candles[1]["close"]

        change_pct = ((last_close - prev_close) / prev_close) * 100 if prev_close else 0

        closes = [c["close"] for c in candles[:50] if c["close"] is not None]
        highs_20 = [c["high"] for c in candles[1:21] if c["high"] is not None]
        lows_10 = [c["low"] for c in candles[1:11] if c["low"] is not None]
        volumes_20 = [c["volume"] for c in candles[1:21] if c["volume"] is not None]

        recent_high = max(highs_20) if highs_20 else latest["high"]
        recent_low = min(lows_10) if lows_10 else latest["low"]
        avg_volume = sum(volumes_20) / len(volumes_20) if volumes_20 else last_volume

        ema20 = sum(closes[:20]) / 20 if len(closes) >= 20 else last_close
        ema50 = sum(closes[:50]) / 50 if len(closes) >= 50 else ema20

        trend_up = last_close > ema20 and ema20 > ema50
        trend_down = last_close < ema20 and ema20 < ema50

        breakout = last_close > recent_high
        volume_confirmed = last_volume > avg_volume * 1.3 if avg_volume else False
        quiet_volume = last_volume < avg_volume * 0.7 if avg_volume else False

        score = 0

        if trend_up:
            score += 25
        if breakout:
            score += 30
        if volume_confirmed:
            score += 25
        if change_pct > 0.5:
            score += 10
        if change_pct > 1.5:
            score += 10

        if trend_down:
            score -= 20
        if change_pct < -1:
            score -= 10
        if quiet_volume:
            score -= 10

        score = max(score, 0)

        dead = False
        if score <= 10 and quiet_volume and not breakout:
            dead = True
        if trend_down and score <= 20:
            dead = True

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
            "avg_volume_20": round(avg_volume, 2) if avg_volume else None,
            "volume_confirmed": volume_confirmed,
            "quiet_volume": quiet_volume,
            "breakout": breakout,
            "trend": "UP" if trend_up else "DOWN" if trend_down else "SIDEWAYS",
            "recent_high_20": round(recent_high, 3),
            "recent_low_10": round(recent_low, 3),
            "score": score,
            "recommendation": recommendation,
            "entry": round(entry, 3),
            "stop_loss": round(stop_loss, 3) if stop_loss else None,
            "target": round(target, 3) if target else None,
            "risk_reward": rr,
            "bar_time": latest["bar_time"]
        }

        if dead:
            item["recommendation"] = "DEAD"
            dead_stocks.append(item)
        else:
            results.append(item)

    results = sorted(results, key=lambda x: x["score"], reverse=True)
    dead_stocks = sorted(dead_stocks, key=lambda x: x["score"])

    top_recommendations = [x for x in results if x["recommendation"] in ["BUY", "WATCH"]][:10]

    top_symbols = set(x["symbol"] for x in top_recommendations)
    dead_symbols = set(x["symbol"] for x in dead_stocks)

    other_stocks = [
        x for x in results
        if x["symbol"] not in top_symbols and x["symbol"] not in dead_symbols
    ]

    buy_list = [x for x in top_recommendations if x["recommendation"] == "BUY"]
    watch_list = [x for x in top_recommendations if x["recommendation"] == "WATCH"]

    market_mode = "Defensive"
    if len(buy_list) >= 5:
        market_mode = "Aggressive"
    elif len(buy_list) >= 2 or len(watch_list) >= 5:
        market_mode = "Neutral"

    if market_phase == "AFTER_CLOSE":
        now = datetime.utcnow().isoformat()

        for item in top_recommendations:
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
        "dead_count": len(dead_stocks),
        "top_recommendations": top_recommendations,
        "other_stocks": other_stocks,
        "dead_stocks": dead_stocks
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

    def badge_class(rec):
        if rec == "BUY":
            return "buy"
        if rec == "WATCH":
            return "watch"
        if rec == "HOLD":
            return "hold"
        if rec == "DEAD":
            return "dead"
        return "avoid"

    def change_class(change):
        return "green" if change > 0 else "red" if change < 0 else "neutral"

    def build_rows(items, include_entry=True):
        html_rows = ""

        for item in items:
            rec = item["recommendation"]
            badge = badge_class(rec)
            ch_class = change_class(item["change_pct"])

            html_rows += f"""
            <tr>
                <td>{item['symbol']}</td>
                <td>{item['last_close']}</td>
                <td class="{ch_class}">{item['change_pct']}%</td>
                <td>{item['score']}</td>
                <td><span class="badge {badge}">{rec}</span></td>
                <td>{item.get('trend', '-')}</td>
                <td>{"YES" if item.get("breakout") else "NO"}</td>
                <td>{"YES" if item.get("volume_confirmed") else "NO"}</td>
                <td>{item.get('entry', '-')}</td>
                <td>{item.get('stop_loss', '-')}</td>
                <td>{item.get('target', '-')}</td>
            </tr>
            """

        return html_rows

    top_rows = build_rows(data["top_recommendations"])
    other_rows = build_rows(data["other_stocks"])
    dead_rows = build_rows(data["dead_stocks"])

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
            h1 {{ margin-top: 0; }}
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
            .metric small {{ color: #9ca3af; }}
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
            th {{ color: #9ca3af; }}
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
            .dead {{ background: #7f1d1d; color: white; }}
            .green {{ color: #22c55e; font-weight: bold; }}
            .red {{ color: #ef4444; font-weight: bold; }}
            .neutral {{ color: #e5e7eb; }}
        </style>
    </head>
    <body>
        <h1>UAE Market Daily Dashboard</h1>

        <div class="card grid">
            <div class="metric"><small>Report Time UAE</small><strong>{data['report_time_uae']}</strong></div>
            <div class="metric"><small>Market Phase</small><strong>{data['market_phase']}</strong></div>
            <div class="metric"><small>Market Mode</small><strong>{data['market_mode']}</strong></div>
            <div class="metric"><small>BUY / WATCH / DEAD</small><strong>{data['buy_count']} / {data['watch_count']} / {data['dead_count']}</strong></div>
        </div>

        <div class="card">
            <h2>Top Recommendations</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th><th>Close</th><th>Change</th><th>Score</th>
                        <th>Status</th><th>Trend</th><th>Breakout</th><th>Volume</th>
                        <th>Entry</th><th>Stop</th><th>Target</th>
                    </tr>
                </thead>
                <tbody>{top_rows}</tbody>
            </table>
        </div>

        <div class="card">
            <h2>Other Stocks</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th><th>Close</th><th>Change</th><th>Score</th>
                        <th>Status</th><th>Trend</th><th>Breakout</th><th>Volume</th>
                        <th>Entry</th><th>Stop</th><th>Target</th>
                    </tr>
                </thead>
                <tbody>{other_rows}</tbody>
            </table>
        </div>

        <div class="card">
            <h2>Dead / Weak Stocks</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th><th>Close</th><th>Change</th><th>Score</th>
                        <th>Status</th><th>Trend</th><th>Breakout</th><th>Volume</th>
                        <th>Entry</th><th>Stop</th><th>Target</th>
                    </tr>
                </thead>
                <tbody>{dead_rows}</tbody>
            </table>
        </div>
    </body>
    </html>
    """

    return html

# =========================
# AI SYSTEM
# =========================

AUTO_TRAIN_HOURS = 12
LAST_AUTO_TRAIN_AT = None


def smart_auto_train(conn, force: bool = False):
    global LAST_AUTO_TRAIN_AT

    now = datetime.utcnow()
    status = ai_engine.ai_status(conn)

    trained_models = status.get("trained_models", {})
    all_ready = all(trained_models.values()) if trained_models else False

    should_train = False
    reason = "TRAIN_NOT_NEEDED"

    if force:
        should_train = True
        reason = "FORCED_TRAIN"
    elif not all_ready:
        should_train = True
        reason = "MODELS_NOT_READY"
    elif LAST_AUTO_TRAIN_AT is None:
        should_train = True
        reason = "FIRST_AUTO_TRAIN"
    elif (now - LAST_AUTO_TRAIN_AT).total_seconds() >= AUTO_TRAIN_HOURS * 3600:
        should_train = True
        reason = "SCHEDULED_REFRESH"

    if should_train:
        result = ai_engine.train_models(conn)
        LAST_AUTO_TRAIN_AT = now
        return {
            "trained": True,
            "reason": reason,
            "trained_at": now.isoformat(),
            "result": result
        }

    return {
        "trained": False,
        "reason": reason,
        "last_train_at": LAST_AUTO_TRAIN_AT.isoformat() if LAST_AUTO_TRAIN_AT else None
    }


@app.get("/api/ai/status")
def api_ai_status():
    conn = db()
    result = ai_engine.ai_status(conn)
    conn.close()
    return result


@app.post("/api/ai/train")
def api_ai_train():
    conn = db()
    result = smart_auto_train(conn, force=True)
    conn.close()
    return result


@app.get("/api/ai/predict")
def api_ai_predict():
    conn = db()

    train_info = smart_auto_train(conn)
    result = ai_engine.predict_latest(conn)

    conn.close()

    return {
        "auto_train": train_info,
        "count": len(result),
        "predictions": result
    }


# =========================
# AI TOP TRADES ENGINE
# =========================

@app.get("/api/ai/top-trades")
def ai_top_trades(limit: int = 5):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    train_info = smart_auto_train(conn)

    predictions = ai_engine.predict_latest(conn)
    final_trades = []

    for p in predictions:
        prob = p.get("best_probability", 0) or 0
        symbol = p.get("symbol")
        horizon = p.get("best_horizon", "5d")
        features = p.get("features", {})

        last_close = p.get("last_close", 0) or 0
        if last_close <= 0:
            continue

        cur.execute("""
            SELECT high, low
            FROM candles
            WHERE symbol = %s
              AND timeframe = '1D'
            ORDER BY bar_time DESC
            LIMIT 20
        """, (symbol,))

        rows = cur.fetchall()

        highs = [r["high"] for r in rows if r["high"] is not None]
        lows = [r["low"] for r in rows if r["low"] is not None]

        recent_high = max(highs) if highs else last_close * 1.05
        recent_low = min(lows) if lows else last_close * 0.97

        entry = last_close
        stop = recent_low
        risk = (entry - stop) / entry if entry > stop else 0

        base_target = entry * 1.05
        breakout_target = recent_high * 1.02
        target = max(base_target, breakout_target)

        profit = (target - entry) / entry
        rr = profit / risk if risk > 0 else 0

        if profit < 0.03:
            continue

        if rr < 1.2:
            continue

        volume_ratio = features.get("volume_ratio", 1) or 1
        breakout = features.get("breakout", 0)
        trend_up = features.get("trend_up", 0) or 0
        trend_down = features.get("trend_down", 0) or 0

        action = "WATCH"

        if prob >= 0.6 and breakout and volume_ratio > 1.5:
            action = "BREAKOUT ATTACK"
        elif prob >= 0.7 and profit >= 0.05 and rr >= 1.8 and trend_up:
            action = "STRONG BUY"
        elif prob >= 0.5 and profit >= 0.06 and rr >= 2:
            if trend_down:
                action = "REVERSAL BUY"
            else:
                action = "SMART BUY"
        elif prob >= 0.75 and profit >= 0.03:
            action = "SAFE BUY"
        elif prob >= 0.45 and profit >= 0.08 and rr >= 2.5:
            action = "HIGH RISK HIGH REWARD"
        elif prob >= 0.7 and trend_up:
            action = "BUY"
        elif prob >= 0.7 and trend_down:
            action = "REVERSAL WATCH"
        elif prob >= 0.7:
            action = "AI STRONG"
        elif prob >= 0.6:
            action = "AI GOOD"
        elif prob >= 0.5:
            action = "AI WEAK"

        score = prob * (profit * 100) * rr

        final_trades.append({
            "symbol": symbol,
            "action": action,
            "probability": round(prob, 2),
            "entry": round(entry, 3),
            "stop_loss": round(stop, 3),
            "target": round(target, 3),
            "expected_profit_pct": round(profit * 100, 2),
            "risk_pct": round(risk * 100, 2),
            "rr": round(rr, 2),
            "holding": horizon,
            "score": round(score, 2),
            "volume_ratio": volume_ratio,
            "breakout": breakout,
            "trend_up": trend_up,
            "trend_down": trend_down
        })

    conn.close()

    final_trades = sorted(final_trades, key=lambda x: x["score"], reverse=True)

    top = [
        x for x in final_trades
        if x["action"] in [
            "BUY",
            "AI STRONG",
            "AI GOOD",
            "SAFE BUY",
            "STRONG BUY",
            "SMART BUY",
            "BREAKOUT ATTACK",
            "REVERSAL BUY"
        ]
    ][:limit]

    aggressive = [
        x for x in final_trades
        if x["action"] == "HIGH RISK HIGH REWARD"
    ][:5]

    watch = [
        x for x in final_trades
        if x["action"] in ["WATCH", "AI WEAK", "REVERSAL WATCH"]
    ][:10]

    return {
        "auto_train": train_info,
        "top_trades": top,
        "aggressive_opportunities": aggressive,
        "watchlist": watch
    }


# =========================
# AI TRADE MONITOR
# =========================

@app.get("/api/ai/monitor")
def ai_monitor_trades():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT *
        FROM trades
        WHERE status = 'OPEN'
    """)

    trades = cur.fetchall()
    results = []

    for t in trades:
        symbol = t["symbol"]
        entry = t["entry_price"]
        stop = t["stop_loss"]
        target = t["target"]

        cur.execute("""
            SELECT close
            FROM candles
            WHERE symbol = %s
            ORDER BY bar_time DESC
            LIMIT 1
        """, (symbol,))

        row = cur.fetchone()
        if not row:
            continue

        price = row["close"]
        action = "HOLD"

        if target and price > target * 1.03:
            action = "EXTEND TARGET"
        elif target and price >= target:
            action = "TAKE PROFIT"
        elif stop and price <= stop:
            action = "EXIT NOW"
        elif stop and price <= (stop * 1.02):
            action = "STOP WARNING"
        elif target and price > entry * 1.05 and price < target:
            action = "TRAIL PROFIT"

        results.append({
            "symbol": symbol,
            "entry": entry,
            "current_price": price,
            "pnl_pct": round(((price - entry) / entry) * 100, 2),
            "action": action,
            "target": target,
            "stop": stop
        })

    conn.close()

    return {
        "count": len(results),
        "trades": results
    }
