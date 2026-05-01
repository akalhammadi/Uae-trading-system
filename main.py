from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List
import psycopg2
import psycopg2.extras
import psycopg2.errors
from datetime import datetime, timezone, timedelta
import os
import csv
import io
import json
import ai_engine
import requests
import json


SECRET = os.getenv("SECRET", "abc123")
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(
    title="UAE Trading AI System",
    version="0.2.0"
)


def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


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


def now_utc():
    return datetime.utcnow().isoformat()


def uae_now():
    return datetime.now(timezone(timedelta(hours=4)))


def clean_num(x):
    if x is None:
        return None

    s = str(x).replace(",", "").strip().upper()

    if s in ["", "-", "N/A", "NONE", "NULL"]:
        return None

    multiplier = 1

    if s.endswith("M"):
        multiplier = 1_000_000
        s = s[:-1]
    elif s.endswith("K"):
        multiplier = 1_000
        s = s[:-1]
    elif s.endswith("B"):
        multiplier = 1_000_000_000
        s = s[:-1]

    return float(s) * multiplier


def get_market_phase():
    n = uae_now()

    if 10 <= n.hour < 15:
        phase = "MARKET_OPEN"
    elif n.hour >= 15:
        phase = "AFTER_CLOSE"
    else:
        phase = "PRE_MARKET"

    return n, phase


# =========================
# DB INIT
# =========================

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        qty DOUBLE PRECISION NOT NULL,
        entry_price DOUBLE PRECISION,
        stop_loss DOUBLE PRECISION,
        target DOUBLE PRECISION,
        sell_price DOUBLE PRECISION,
        status TEXT NOT NULL,
        opened_at TEXT,
        closed_at TEXT,
        note TEXT
    )
    """)

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_settings (
        id INTEGER PRIMARY KEY,
        capital DOUBLE PRECISION NOT NULL,
        cash DOUBLE PRECISION NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_decisions (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        decision_time TEXT NOT NULL,
        source TEXT,
        action TEXT,
        probability DOUBLE PRECISION,
        score DOUBLE PRECISION,
        entry DOUBLE PRECISION,
        stop_loss DOUBLE PRECISION,
        target DOUBLE PRECISION,
        expected_profit_pct DOUBLE PRECISION,
        risk_pct DOUBLE PRECISION,
        rr DOUBLE PRECISION,
        reaction_score DOUBLE PRECISION,
        signal_score DOUBLE PRECISION,
        market_mode TEXT,
        payload TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_training_log (
        id SERIAL PRIMARY KEY,
        trained_at TEXT NOT NULL,
        reason TEXT,
        result TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS signal_outcomes (
        id SERIAL PRIMARY KEY,
        signal_id INTEGER,
        symbol TEXT NOT NULL,
        signal TEXT,
        signal_price DOUBLE PRECISION,
        signal_time TEXT,
        close_1d DOUBLE PRECISION,
        close_3d DOUBLE PRECISION,
        close_5d DOUBLE PRECISION,
        return_1d DOUBLE PRECISION,
        return_3d DOUBLE PRECISION,
        return_5d DOUBLE PRECISION,
        evaluated_at TEXT
    )
    """)

    conn.commit()
    conn.close()


init_db()


# =========================
# MODELS
# =========================

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


class PortfolioSetup(BaseModel):
    capital: float


class PortfolioBuyPayload(BaseModel):
    symbol: str
    qty: float
    price: float
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    note: Optional[str] = None


class PortfolioSellPayload(BaseModel):
    price: float
    note: Optional[str] = None


class TradePayload(BaseModel):
    symbol: str
    qty: float
    price: float
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    note: Optional[str] = None


# =========================
# PRICE HELPERS
# =========================

def get_latest_daily_candle(conn, symbol):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT symbol, bar_time, open, high, low, close, volume
        FROM candles
        WHERE symbol = %s
          AND timeframe = '1D'
          AND close IS NOT NULL
        ORDER BY bar_time DESC
        LIMIT 1
    """, (symbol,))

    return cur.fetchone()


def get_latest_daily_price(conn, symbol, fallback=None):
    row = get_latest_daily_candle(conn, symbol)
    if not row:
        return fallback, None
    return row["close"], row["bar_time"]


# =========================
# WEBHOOK
# =========================

@app.post("/webhook/tradingview")
def tradingview_webhook(payload: TVPayload):
    if payload.secret != SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    conn = db()
    cur = conn.cursor()

    symbol = normalize_symbol(payload.symbol)
    current_time = now_utc()

    if payload.type == "CANDLE_UPDATE":
        timeframe = payload.timeframe or "60"

        cur.execute("""
            INSERT INTO candles
            (symbol, exchange, timeframe, bar_time, open, high, low, close, volume, received_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            timeframe,
            payload.time or current_time,
            payload.open,
            payload.high,
            payload.low,
            payload.close,
            payload.volume,
            current_time
        ))

        conn.commit()
        conn.close()

        return {
            "status": "ok",
            "stored": "candle",
            "symbol": symbol,
            "timeframe": timeframe
        }

    if payload.type == "SIGNAL":
        cur.execute("""
            INSERT INTO signals
            (symbol, exchange, timeframe, signal, price, volume, bar_time, received_at, payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            symbol,
            payload.exchange,
            payload.timeframe,
            payload.signal or "UNKNOWN",
            payload.price or payload.close,
            payload.volume,
            payload.time,
            current_time,
            payload.model_dump_json()
        ))

        conn.commit()
        conn.close()

        return {
            "status": "ok",
            "stored": "signal",
            "symbol": symbol
        }

    conn.close()

    return {
        "status": "ignored",
        "reason": "unknown type",
        "symbol": symbol
    }


# =========================
# DATA API
# =========================

@app.get("/api/candles/latest")
def latest_candles(symbol: Optional[str] = None, timeframe: Optional[str] = None, limit: int = 100):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    q = """
        SELECT *
        FROM candles
        WHERE close IS NOT NULL
    """
    params = []

    if symbol:
        q += " AND symbol = %s"
        params.append(normalize_symbol(symbol))

    if timeframe:
        q += " AND timeframe = %s"
        params.append(timeframe)

    q += " ORDER BY bar_time DESC LIMIT %s"
    params.append(limit)

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    return {"count": len(rows), "candles": rows}


@app.get("/api/prices/daily-latest")
def daily_latest_prices():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT DISTINCT ON (symbol)
            symbol, bar_time, open, high, low, close, volume, received_at
        FROM candles
        WHERE timeframe = '1D'
          AND close IS NOT NULL
        ORDER BY symbol, bar_time DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return {
        "count": len(rows),
        "prices": rows
    }


@app.get("/api/signals/latest")
def latest_signals(symbol: Optional[str] = None, limit: int = 50):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    q = "SELECT * FROM signals WHERE 1=1"
    params = []

    if symbol:
        q += " AND symbol = %s"
        params.append(normalize_symbol(symbol))

    q += " ORDER BY received_at DESC LIMIT %s"
    params.append(limit)

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    return {"count": len(rows), "signals": rows}


@app.post("/api/upload-csv")
async def upload_csv(file: UploadFile = File(...), symbol: str = "UNKNOWN"):
    content = await file.read()
    decoded = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(decoded))

    conn = db()
    cur = conn.cursor()

    inserted = 0
    symbol = normalize_symbol(symbol)

    for row in reader:
        raw_date = row.get("Date") or row.get("date")

        try:
            date = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
        except Exception:
            date = raw_date

        open_price = row.get("Open") or row.get("open")
        high = row.get("High") or row.get("high")
        low = row.get("Low") or row.get("low")
        close = row.get("Price") or row.get("Close") or row.get("close")
        volume = row.get("Vol.") or row.get("Volume") or row.get("volume")

        if not date or not close:
            continue

        cur.execute("""
            INSERT INTO candles
            (symbol, exchange, timeframe, bar_time, open, high, low, close, volume, received_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            clean_num(open_price),
            clean_num(high),
            clean_num(low),
            clean_num(close),
            clean_num(volume),
            now_utc()
        ))

        inserted += 1

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "symbol": symbol,
        "rows_inserted": inserted
    }


@app.post("/api/upload-batch")
def upload_batch(payload: BatchUpload):
    if not payload.rows:
        return {"status": "empty", "inserted": 0}

    conn = db()
    cur = conn.cursor()

    symbol = normalize_symbol(payload.symbol)
    current_time = now_utc()

    values = [
        (
            symbol,
            payload.exchange,
            payload.timeframe,
            r.date,
            r.open,
            r.high,
            r.low,
            r.close,
            r.volume,
            current_time
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
        "symbol": symbol,
        "timeframe": payload.timeframe,
        "inserted": len(values)
    }


# =========================
# MARKET REACTION
# =========================

def get_signal_score(conn, symbol):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT signal, price, volume, bar_time, received_at
        FROM signals
        WHERE symbol = %s
        ORDER BY received_at DESC
        LIMIT 10
    """, (symbol,))

    rows = cur.fetchall()

    score = 0
    last_signal = None

    for r in rows:
        sig = (r["signal"] or "").upper()
        last_signal = sig

        if "BUY" in sig:
            score += 20
        if "BREAKOUT" in sig:
            score += 25
        if "VOLUME" in sig:
            score += 15
        if "SELL" in sig or "EXIT" in sig:
            score -= 30

    return {
        "signal_score": max(min(score, 100), -100),
        "last_signal": last_signal
    }


def get_market_reaction_score(conn, symbol):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT bar_time, open, high, low, close, volume
        FROM candles
        WHERE symbol = %s
          AND timeframe = '1D'
          AND close IS NOT NULL
        ORDER BY bar_time DESC
        LIMIT 25
    """, (symbol,))

    rows = cur.fetchall()
    signal_data = get_signal_score(conn, symbol)

    if len(rows) < 10:
        return {
            "reaction_score": 0,
            "reaction_label": "NO_DATA",
            "today_change_pct": 0,
            "intraday_strength": 0,
            "volume_ratio": 0,
            "breakout": False,
            "signal_score": signal_data["signal_score"],
            "last_signal": signal_data["last_signal"]
        }

    latest = rows[0]
    prev = rows[1]

    close = latest["close"]
    open_price = latest["open"] or prev["close"]
    prev_close = prev["close"]

    today_change_pct = ((close - prev_close) / prev_close) * 100 if prev_close else 0
    intraday_strength = ((close - open_price) / open_price) * 100 if open_price else 0

    volumes = [r["volume"] for r in rows[1:21] if r["volume"]]
    avg_volume = sum(volumes) / len(volumes) if volumes else 0
    volume_ratio = (latest["volume"] or 0) / avg_volume if avg_volume else 0

    highs_20 = [r["high"] for r in rows[1:21] if r["high"]]
    recent_high = max(highs_20) if highs_20 else close
    breakout = close > recent_high

    score = 0

    if today_change_pct > 0.5:
        score += 10
    if today_change_pct > 1:
        score += 15
    if today_change_pct > 2:
        score += 20
    if intraday_strength > 0.5:
        score += 10
    if volume_ratio > 1.2:
        score += 15
    if volume_ratio > 1.5:
        score += 20
    if breakout:
        score += 25

    score = min(score, 100)

    label = "QUIET"
    if score >= 75:
        label = "HOT_MOMENTUM"
    elif score >= 55:
        label = "ACTIVE"
    elif score >= 35:
        label = "WATCHING"

    return {
        "reaction_score": round(score, 2),
        "reaction_label": label,
        "today_change_pct": round(today_change_pct, 2),
        "intraday_strength": round(intraday_strength, 2),
        "volume_ratio": round(volume_ratio, 2),
        "breakout": breakout,
        "signal_score": signal_data["signal_score"],
        "last_signal": signal_data["last_signal"]
    }


@app.get("/api/market/reaction")
def market_reaction():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT DISTINCT symbol
        FROM candles
        WHERE timeframe = '1D'
    """)

    symbols = [r["symbol"] for r in cur.fetchall()]
    results = []

    for symbol in symbols:
        reaction = get_market_reaction_score(conn, symbol)
        reaction["symbol"] = symbol
        reaction["total_score"] = reaction["reaction_score"] + reaction["signal_score"]
        results.append(reaction)

    conn.close()

    results = sorted(results, key=lambda x: x["total_score"], reverse=True)

    return {
        "count": len(results),
        "hot_stocks": results[:10],
        "all": results
    }


# =========================
# DAILY DASHBOARD
# =========================

@app.get("/api/dashboard/daily")
def daily_dashboard():
    n, market_phase = get_market_phase()
    report_date = n.strftime("%Y-%m-%d")

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
        prev = candles[1]

        last_close = latest["close"]
        last_volume = latest["volume"] or 0
        prev_close = prev["close"]

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

        reaction = get_market_reaction_score(conn, symbol)

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

        score += reaction["reaction_score"] * 0.25
        score += reaction["signal_score"] * 0.15

        if trend_down:
            score -= 20
        if change_pct < -1:
            score -= 10
        if quiet_volume:
            score -= 10

        score = max(round(score, 2), 0)

        recommendation = "AVOID"

        if score >= 80 and not trend_down:
            recommendation = "BUY"
        elif score >= 60:
            recommendation = "WATCH"
        elif score >= 40:
            recommendation = "HOLD"

        dead = False
        if score <= 15 and quiet_volume and not breakout:
            dead = True
        if trend_down and score <= 25:
            dead = True

        entry = last_close
        stop_loss = recent_low
        risk = entry - stop_loss if entry and stop_loss else None
        target = entry + (risk * 2) if risk and risk > 0 else None
        rr = 2 if target else None

        item = {
            "symbol": symbol,
            "last_close": round(last_close, 3),
            "data_date": latest["bar_time"],
            "change_pct": round(change_pct, 2),
            "volume": last_volume,
            "avg_volume_20": round(avg_volume, 2) if avg_volume else None,
            "volume_confirmed": volume_confirmed,
            "quiet_volume": quiet_volume,
            "breakout": breakout,
            "trend": "UP" if trend_up else "DOWN" if trend_down else "SIDEWAYS",
            "recent_high_20": round(recent_high, 3),
            "recent_low_10": round(recent_low, 3),
            "reaction_score": reaction["reaction_score"],
            "reaction_label": reaction["reaction_label"],
            "signal_score": reaction["signal_score"],
            "last_signal": reaction["last_signal"],
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
        for item in top_recommendations:
            cur.execute("""
                INSERT INTO daily_recommendations
                (report_date, symbol, last_close, change_pct, volume, score,
                 recommendation, entry, stop_loss, target, risk_reward,
                 market_phase, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                now_utc()
            ))

        conn.commit()

    conn.close()

    return {
        "report_date": report_date,
        "report_time_uae": n.strftime("%Y-%m-%d %H:%M"),
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


# =========================
# PORTFOLIO
# =========================

def get_portfolio_settings(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM portfolio_settings WHERE id = 1")
    row = cur.fetchone()

    if not row:
        current_time = now_utc()
        cur.execute("""
            INSERT INTO portfolio_settings (id, capital, cash, updated_at)
            VALUES (1, 100000, 100000, %s)
        """, (current_time,))
        conn.commit()

        return {
            "capital": 100000,
            "cash": 100000,
            "updated_at": current_time
        }

    return row


@app.post("/api/portfolio/setup")
def setup_portfolio(payload: PortfolioSetup):
    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'")
    open_count = cur.fetchone()[0]

    if open_count > 0:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Cannot reset portfolio while open trades exist"
        )

    cur.execute("""
        INSERT INTO portfolio_settings (id, capital, cash, updated_at)
        VALUES (1, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            capital = EXCLUDED.capital,
            cash = EXCLUDED.cash,
            updated_at = EXCLUDED.updated_at
    """, (payload.capital, payload.capital, now_utc()))

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "capital": payload.capital,
        "cash": payload.capital
    }


@app.post("/api/portfolio/buy")
def portfolio_buy(payload: PortfolioBuyPayload):
    conn = db()
    cur = conn.cursor()

    settings = get_portfolio_settings(conn)
    cost = payload.qty * payload.price

    if cost > settings["cash"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Not enough cash")

    symbol = normalize_symbol(payload.symbol)

    cur.execute("""
        INSERT INTO trades
        (symbol, qty, entry_price, stop_loss, target, status, opened_at, note)
        VALUES (%s,%s,%s,%s,%s,'OPEN',%s,%s)
        RETURNING id
    """, (
        symbol,
        payload.qty,
        payload.price,
        payload.stop_loss,
        payload.target,
        now_utc(),
        payload.note
    ))

    trade_id = cur.fetchone()[0]

    cur.execute("""
        UPDATE portfolio_settings
        SET cash = cash - %s,
            updated_at = %s
        WHERE id = 1
    """, (cost, now_utc()))

    conn.commit()
    conn.close()

    return {
        "status": "BOUGHT",
        "trade_id": trade_id,
        "symbol": symbol,
        "qty": payload.qty,
        "price": payload.price,
        "cost": round(cost, 2)
    }


@app.post("/api/portfolio/sell/{trade_id}")
def portfolio_sell(trade_id: int, payload: PortfolioSellPayload):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT *
        FROM trades
        WHERE id = %s AND status = 'OPEN'
    """, (trade_id,))

    trade = cur.fetchone()

    if not trade:
        conn.close()
        raise HTTPException(status_code=404, detail="Open trade not found")

    entry_price = trade["entry_price"]
    qty = trade["qty"]
    symbol = trade["symbol"]

    sell_value = qty * payload.price
    pnl = (payload.price - entry_price) * qty
    pnl_pct = ((payload.price - entry_price) / entry_price) * 100 if entry_price else 0

    cur.execute("""
        UPDATE trades
        SET sell_price = %s,
            status = 'CLOSED',
            closed_at = %s,
            note = COALESCE(note, '') || %s
        WHERE id = %s
    """, (
        payload.price,
        now_utc(),
        f" | SELL NOTE: {payload.note}" if payload.note else "",
        trade_id
    ))

    cur.execute("""
        UPDATE portfolio_settings
        SET cash = cash + %s,
            updated_at = %s
        WHERE id = 1
    """, (sell_value, now_utc()))

    conn.commit()
    conn.close()

    return {
        "status": "SOLD",
        "trade_id": trade_id,
        "symbol": symbol,
        "sell_value": round(sell_value, 2),
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2)
    }


def portfolio_summary_data():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    settings = get_portfolio_settings(conn)

    cur.execute("""
        SELECT *
        FROM trades
        WHERE status = 'OPEN'
        ORDER BY opened_at DESC
    """)

    trades = cur.fetchall()

    holdings = []
    invested = 0
    market_value = 0

    for t in trades:
        symbol = t["symbol"]
        current_price, price_date = get_latest_daily_price(conn, symbol, t["entry_price"])

        cost = t["qty"] * t["entry_price"]
        value = t["qty"] * current_price
        pnl = value - cost
        pnl_pct = (pnl / cost) * 100 if cost else 0

        invested += cost
        market_value += value

        holdings.append({
            "trade_id": t["id"],
            "symbol": symbol,
            "qty": t["qty"],
            "entry": round(t["entry_price"], 3),
            "current_price": round(current_price, 3),
            "price_date": price_date,
            "cost": round(cost, 2),
            "market_value": round(value, 2),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "stop_loss": t["stop_loss"],
            "target": t["target"]
        })

    total_equity = settings["cash"] + market_value
    exposure_pct = (market_value / total_equity) * 100 if total_equity else 0

    conn.close()

    return {
        "capital": round(settings["capital"], 2),
        "cash": round(settings["cash"], 2),
        "invested_cost": round(invested, 2),
        "market_value": round(market_value, 2),
        "total_equity": round(total_equity, 2),
        "total_pnl": round(total_equity - settings["capital"], 2),
        "total_pnl_pct": round(((total_equity - settings["capital"]) / settings["capital"]) * 100, 2) if settings["capital"] else 0,
        "exposure_pct": round(exposure_pct, 2),
        "holdings": holdings
    }


@app.get("/api/portfolio/summary")
def portfolio_summary():
    return portfolio_summary_data()


@app.post("/api/trades/buy")
def buy_trade(payload: TradePayload):
    return portfolio_buy(
        PortfolioBuyPayload(
            symbol=payload.symbol,
            qty=payload.qty,
            price=payload.price,
            stop_loss=payload.stop_loss,
            target=payload.target,
            note=payload.note
        )
    )


@app.post("/api/trades/sell/{trade_id}")
def sell_trade(trade_id: int, payload: TradePayload):
    return portfolio_sell(
        trade_id,
        PortfolioSellPayload(
            price=payload.price,
            note=payload.note
        )
    )


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


# =========================
# AI
# =========================

AUTO_TRAIN_HOURS = 12
LAST_AUTO_TRAIN_AT = None


def smart_auto_train(conn, force: bool = False):
    global LAST_AUTO_TRAIN_AT

    current = datetime.utcnow()
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
    elif (current - LAST_AUTO_TRAIN_AT).total_seconds() >= AUTO_TRAIN_HOURS * 3600:
        should_train = True
        reason = "SCHEDULED_REFRESH"

    if should_train:
        result = ai_engine.train_models(conn)

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_training_log (trained_at, reason, result)
            VALUES (%s,%s,%s)
        """, (
            current.isoformat(),
            reason,
            json.dumps(result, default=str)
        ))

        conn.commit()

        LAST_AUTO_TRAIN_AT = current

        return {
            "trained": True,
            "reason": reason,
            "trained_at": current.isoformat(),
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


@app.get("/api/ai/top-trades")
def ai_top_trades(limit: int = 5):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    train_info = smart_auto_train(conn)
    predictions = ai_engine.predict_latest(conn)
    daily = daily_dashboard()
    market_mode = daily["market_mode"]

    final_trades = []

    for p in predictions:
        prob = p.get("best_probability", 0) or 0
        symbol = p.get("symbol")
        horizon = p.get("best_horizon", "5d")
        features = p.get("features", {})

        reaction = get_market_reaction_score(conn, symbol)

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
        rr = min(rr, 5)

        if profit < 0.03:
            continue

        if rr < 1.2:
            continue

        volume_ratio = features.get("volume_ratio", 1) or 1
        breakout = bool(features.get("breakout", 0))
        trend_up = bool(features.get("trend_up", 0))
        trend_down = bool(features.get("trend_down", 0))

        action = "WATCH"

        if prob >= 0.65 and reaction["breakout"] and reaction["volume_ratio"] > 1.3:
            action = "BREAKOUT ATTACK"
        elif prob >= 0.72 and profit >= 0.05 and rr >= 1.8 and trend_up:
            action = "STRONG BUY"
        elif prob >= 0.58 and profit >= 0.06 and rr >= 2 and not trend_down:
            action = "SMART BUY"
        elif prob >= 0.75 and profit >= 0.03 and not trend_down:
            action = "SAFE BUY"
        elif prob >= 0.5 and profit >= 0.08 and rr >= 2.5:
            action = "HIGH RISK HIGH REWARD"
        elif prob >= 0.7:
            action = "AI GOOD"

        if trend_down and not reaction["breakout"]:
            action = "REVERSAL WATCH"

        if market_mode == "Defensive" and not reaction["breakout"] and reaction["reaction_score"] < 60:
            action = "WATCH"

        score = (
            prob * 100
            + profit * 120
            + rr * 8
            + reaction["reaction_score"] * 1.5
            + reaction["signal_score"] * 1.2
        )

        if market_mode == "Defensive":
            score -= 20

        if reaction["reaction_label"] == "HOT_MOMENTUM":
            score += 25

        if reaction["breakout"]:
            score += 20

        decision = {
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
            "trend_down": trend_down,
            "reaction_score": reaction["reaction_score"],
            "reaction_label": reaction["reaction_label"],
            "today_change_pct": reaction["today_change_pct"],
            "intraday_strength": reaction["intraday_strength"],
            "market_volume_ratio": reaction["volume_ratio"],
            "signal_score": reaction["signal_score"],
            "last_signal": reaction["last_signal"],
            "market_mode": market_mode
        }

        final_trades.append(decision)

        cur.execute("""
            INSERT INTO ai_decisions
            (symbol, decision_time, source, action, probability, score,
             entry, stop_loss, target, expected_profit_pct, risk_pct, rr,
             reaction_score, signal_score, market_mode, payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            symbol,
            now_utc(),
            "AI_TOP_TRADES",
            action,
            decision["probability"],
            decision["score"],
            decision["entry"],
            decision["stop_loss"],
            decision["target"],
            decision["expected_profit_pct"],
            decision["risk_pct"],
            decision["rr"],
            reaction["reaction_score"],
            reaction["signal_score"],
            market_mode,
            json.dumps(decision, default=str)
        ))

    conn.commit()
    conn.close()

    final_trades = sorted(final_trades, key=lambda x: x["score"], reverse=True)

    top_actions = [
        "BUY",
        "AI GOOD",
        "SAFE BUY",
        "STRONG BUY",
        "SMART BUY",
        "BREAKOUT ATTACK"
    ]

    top = [x for x in final_trades if x["action"] in top_actions][:limit]
    aggressive = [x for x in final_trades if x["action"] == "HIGH RISK HIGH REWARD"][:5]
    watch = [x for x in final_trades if x["action"] in ["WATCH", "REVERSAL WATCH", "AI WEAK"]][:10]

    return {
        "auto_train": train_info,
        "market_mode": market_mode,
        "top_trades": top,
        "aggressive_opportunities": aggressive,
        "watchlist": watch
    }


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

        price, price_date = get_latest_daily_price(conn, symbol, entry)

        action = "HOLD"

        if target and price >= target:
            action = "TAKE PROFIT"
        elif stop and price <= stop:
            action = "EXIT NOW"
        elif stop and price <= stop * 1.02:
            action = "STOP WARNING"
        elif target and price > entry * 1.05 and price < target:
            action = "TRAIL PROFIT"

        results.append({
            "symbol": symbol,
            "entry": entry,
            "current_price": price,
            "price_date": price_date,
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


@app.get("/api/ai/risk-manager")
def ai_risk_manager():
    portfolio = portfolio_summary_data()
    daily = daily_dashboard()

    market_mode = daily["market_mode"]
    buy_count = daily["buy_count"]
    watch_count = daily["watch_count"]
    exposure = portfolio["exposure_pct"]

    max_exposure = 25

    if market_mode == "Aggressive":
        max_exposure = 80
    elif market_mode == "Neutral":
        max_exposure = 55
    elif market_mode == "Defensive":
        max_exposure = 25

    decision = "HOLD"
    advice = "Keep current positions and monitor."

    if market_mode == "Defensive" and exposure > 40:
        decision = "REDUCE RISK"
        advice = "Market is weak. Reduce exposure and avoid new buying."
    elif market_mode == "Defensive" and buy_count == 0:
        decision = "NO NEW BUY"
        advice = "No new trades. Wait for real market strength."
    elif market_mode == "Neutral" and exposure < max_exposure:
        decision = "SELECTIVE BUY"
        advice = "Only buy the strongest setups."
    elif market_mode == "Aggressive" and exposure < max_exposure:
        decision = "BUY ALLOWED"
        advice = "Buying allowed but position sizing must be controlled."

    position_actions = []

    for h in portfolio["holdings"]:
        action = "HOLD"

        if h["target"] and h["current_price"] >= h["target"]:
            action = "TAKE PROFIT"
        elif h["stop_loss"] and h["current_price"] <= h["stop_loss"]:
            action = "EXIT NOW"
        elif market_mode == "Defensive" and h["pnl_pct"] < -2:
            action = "SELL / CUT LOSS"
        elif h["pnl_pct"] > 5:
            action = "TRAIL PROFIT"

        position_actions.append({
            "trade_id": h["trade_id"],
            "symbol": h["symbol"],
            "pnl_pct": h["pnl_pct"],
            "action": action
        })

    suggested_cash = portfolio["total_equity"] * (1 - max_exposure / 100)
    suggested_invested = portfolio["total_equity"] * (max_exposure / 100)

    return {
        "market_mode": market_mode,
        "decision": decision,
        "advice": advice,
        "buy_count": buy_count,
        "watch_count": watch_count,
        "current_exposure_pct": exposure,
        "max_allowed_exposure_pct": max_exposure,
        "suggested_cash": round(suggested_cash, 2),
        "suggested_invested": round(suggested_invested, 2),
        "portfolio": portfolio,
        "position_actions": position_actions
    }


@app.get("/api/ai/learning-report")
def learning_report():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT action, COUNT(*) AS count, AVG(score) AS avg_score
        FROM ai_decisions
        GROUP BY action
        ORDER BY avg_score DESC
    """)

    stats = cur.fetchall()

    cur.execute("""
        SELECT *
        FROM ai_training_log
        ORDER BY trained_at DESC
        LIMIT 10
    """)

    logs = cur.fetchall()

    conn.close()

    return {
        "ai_action_stats": stats,
        "training_log": logs
    }


# =========================
# HTML DASHBOARD
# =========================

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page():
    data = daily_dashboard()
    portfolio = portfolio_summary_data()

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

    def build_rows(items):
        rows = ""
        for item in items:
            rows += f"""
            <tr>
                <td>{item['symbol']}</td>
                <td>{item['last_close']}</td>
                <td>{item.get('data_date', '-')}</td>
                <td class="{change_class(item['change_pct'])}">{item['change_pct']}%</td>
                <td>{item['score']}</td>
                <td><span class="badge {badge_class(item['recommendation'])}">{item['recommendation']}</span></td>
            </tr>
            """
        return rows

    def build_holdings(items):
        rows = ""
        for h in items:
            rows += f"""
            <tr>
                <td>{h['trade_id']}</td>
                <td>{h['symbol']}</td>
                <td>{h['qty']}</td>
                <td>{h['entry']}</td>
                <td>{h['current_price']}</td>
                <td>{h.get('price_date', '-')}</td>
                <td>{h['market_value']}</td>
                <td>{h['pnl']}</td>
                <td>{h['pnl_pct']}%</td>
            </tr>
            """
        return rows

    html = f"""
    <html>
    <head>
        <title>UAE Trading AI Dashboard</title>
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
                overflow-x: auto;
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
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
                white-space: nowrap;
                text-align: left;
            }}
            th {{ color: #9ca3af; }}
            .badge {{
                padding: 6px 12px;
                border-radius: 999px;
                font-weight: bold;
            }}
            .buy {{ background: #16a34a; }}
            .watch {{ background: #f59e0b; color: #111827; }}
            .hold {{ background: #3b82f6; }}
            .avoid {{ background: #6b7280; }}
            .dead {{ background: #991b1b; }}
            .green {{ color: #22c55e; font-weight: bold; }}
            .red {{ color: #ef4444; font-weight: bold; }}
            .neutral {{ color: #e5e7eb; }}
        </style>
    </head>
    <body>
        <h1>UAE Trading AI Dashboard</h1>

        <div class="card grid">
            <div class="metric"><small>Report Time UAE</small><strong>{data['report_time_uae']}</strong></div>
            <div class="metric"><small>Market Mode</small><strong>{data['market_mode']}</strong></div>
            <div class="metric"><small>BUY / WATCH / DEAD</small><strong>{data['buy_count']} / {data['watch_count']} / {data['dead_count']}</strong></div>
            <div class="metric"><small>Cash</small><strong>{portfolio['cash']}</strong></div>
            <div class="metric"><small>Market Value</small><strong>{portfolio['market_value']}</strong></div>
            <div class="metric"><small>Total Equity</small><strong>{portfolio['total_equity']}</strong></div>
            <div class="metric"><small>Total PnL</small><strong>{portfolio['total_pnl']} ({portfolio['total_pnl_pct']}%)</strong></div>
            <div class="metric"><small>Exposure</small><strong>{portfolio['exposure_pct']}%</strong></div>
        </div>

        <div class="card">
            <h2>Holdings / Owned</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th><th>Symbol</th><th>Qty</th><th>Entry</th>
                        <th>Current</th><th>Price Date</th><th>Value</th><th>PnL</th><th>PnL %</th>
                    </tr>
                </thead>
                <tbody>{build_holdings(portfolio['holdings'])}</tbody>
            </table>
        </div>

        <div class="card">
            <h2>Top Recommendations</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th><th>Close</th><th>Data Date</th><th>Change</th><th>Score</th><th>Status</th>
                    </tr>
                </thead>
                <tbody>{build_rows(data['top_recommendations'])}</tbody>
            </table>
        </div>

        <div class="card">
            <h2>Other Stocks</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th><th>Close</th><th>Data Date</th><th>Change</th><th>Score</th><th>Status</th>
                    </tr>
                </thead>
                <tbody>{build_rows(data['other_stocks'])}</tbody>
            </table>
        </div>

        <div class="card">
            <h2>Dead / Weak Stocks</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th><th>Close</th><th>Data Date</th><th>Change</th><th>Score</th><th>Status</th>
                    </tr>
                </thead>
                <tbody>{build_rows(data['dead_stocks'])}</tbody>
            </table>
        </div>
    </body>
    </html>
    """

    return html

# =========================
# DUAL AI SIGNALS + TELEGRAM - MEDIUM MODE
# =========================

import json
import requests
import psycopg2.errors

DASHBOARD_URL = os.getenv(
    "DASHBOARD_URL",
    "https://uae-market-production.up.railway.app/dashboard"
)

SYSTEM_MODE = "MEDIUM"


def ensure_alert_tables():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_alerts_log (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            trigger_key TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            price DOUBLE PRECISION,
            message TEXT,
            payload TEXT,
            UNIQUE(symbol, signal_type, trigger_key)
        )
    """)

    conn.commit()
    conn.close()


def send_telegram_message(message: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return {"ok": False, "error": "Missing Telegram variables"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    r = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        },
        timeout=15
    )

    return {
        "ok": r.ok,
        "status_code": r.status_code,
        "response": r.text[:500]
    }


def get_symbol_candles(symbol: str, timeframe: str, limit: int = 250):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT symbol, exchange, timeframe, bar_time, open, high, low, close, volume, received_at
        FROM candles
        WHERE symbol = %s
          AND timeframe = %s
          AND close IS NOT NULL
        ORDER BY bar_time DESC
        LIMIT %s
    """, (normalize_symbol(symbol), timeframe, limit))

    rows = cur.fetchall()
    conn.close()

    return list(reversed(rows))


def get_all_symbols():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT DISTINCT symbol
        FROM candles
        WHERE close IS NOT NULL
        ORDER BY symbol
    """)

    rows = cur.fetchall()
    conn.close()

    return [r["symbol"] for r in rows]


def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)
    e = values[0]

    for price in values[1:]:
        e = price * k + e * (1 - k)

    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def macd(values):
    if len(values) < 35:
        return None

    ema12 = ema(values[-60:], 12)
    ema26 = ema(values[-60:], 26)

    if ema12 is None or ema26 is None:
        return None

    line = ema12 - ema26
    return line


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        if high is None or low is None or prev_close is None:
            continue

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


def support_resistance(candles, lookback=40):
    recent = candles[-lookback:] if len(candles) >= lookback else candles

    lows = [x["low"] for x in recent if x["low"] is not None]
    highs = [x["high"] for x in recent if x["high"] is not None]

    support = min(lows) if lows else None
    resistance = max(highs) if highs else None

    return support, resistance


def volume_ratio(candles, lookback=20):
    if len(candles) < lookback + 1:
        return 1

    latest_volume = candles[-1]["volume"] or 0
    volumes = [x["volume"] for x in candles[-lookback - 1:-1] if x["volume"]]

    if not volumes:
        return 1

    avg_volume = sum(volumes) / len(volumes)
    return latest_volume / avg_volume if avg_volume else 1


def classify_strength(score):
    if score >= 80:
        return "VERY STRONG"
    if score >= 65:
        return "STRONG"
    if score >= 50:
        return "MEDIUM"
    return "WEAK"


def analyze_dual_symbol(symbol: str):
    symbol = normalize_symbol(symbol)

    daily = get_symbol_candles(symbol, "1D", 260)
    h1 = get_symbol_candles(symbol, "60", 180)
    m1 = get_symbol_candles(symbol, "1", 180)

    short_data = h1 if len(h1) >= 50 else m1
    active_tf = "60" if len(h1) >= 50 else "1"

    signals = []

    # =========================
    # SWING SYSTEM
    # Target: 7% - 15%
    # Holding: 1 week - 1 month
    # =========================

    if len(daily) >= 60:
        closes = [x["close"] for x in daily if x["close"] is not None]
        latest = daily[-1]
        price = latest["close"]

        ema50 = ema(closes, 50)
        ema200 = ema(closes, 200) if len(closes) >= 200 else None
        rsi14 = rsi(closes, 14)
        macd_line = macd(closes)

        support, resistance = support_resistance(daily, 60)
        vr = volume_ratio(daily, 20)
        day_atr = atr(daily, 14)

        trend_score = 0
        momentum_score = 0
        structure_score = 0
        volume_score = 0

        if ema50 and price > ema50:
            trend_score += 25

        if ema200 and ema50 and ema50 > ema200 * 0.97:
            trend_score += 20

        if rsi14 and 45 <= rsi14 <= 70:
            momentum_score += 15

        if macd_line and macd_line > 0:
            momentum_score += 10

        near_support = support and price <= support * 1.05
        breakout = resistance and price >= resistance * 0.995

        if near_support:
            structure_score += 20

        if breakout:
            structure_score += 25

        if vr >= 0.9:
            volume_score += 10

        if vr >= 1.3:
            volume_score += 15

        total_score = trend_score + momentum_score + structure_score + volume_score

        if total_score >= 55 and support:
            entry = price

            if day_atr:
                stop = max(support, entry - day_atr * 2)
            else:
                stop = support

            if stop >= entry:
                stop = entry * 0.94

            target1 = entry * 1.07
            target2 = entry * 1.12
            target3 = entry * 1.15

            risk_pct = ((entry - stop) / entry) * 100
            expected_pct = ((target1 - entry) / entry) * 100
            rr = expected_pct / risk_pct if risk_pct > 0 else 0

            if expected_pct >= 7 and rr >= 0.9:
                signals.append({
                    "symbol": symbol,
                    "type": "SWING",
                    "action": "SWING BUY WATCH",
                    "timeframe": "1D",
                    "price": round(entry, 3),
                    "entry_zone": [round(entry * 0.99, 3), round(entry * 1.01, 3)],
                    "stop_loss": round(stop, 3),
                    "target1": round(target1, 3),
                    "target2": round(target2, 3),
                    "target3": round(target3, 3),
                    "expected_move_pct": round(expected_pct, 2),
                    "risk_pct": round(risk_pct, 2),
                    "rr": round(rr, 2),
                    "score": round(total_score, 2),
                    "strength": classify_strength(total_score),
                    "trend": "UP" if trend_score >= 25 else "MIXED",
                    "support": round(support, 3) if support else None,
                    "resistance": round(resistance, 3) if resistance else None,
                    "rsi": round(rsi14, 2) if rsi14 else None,
                    "volume_ratio": round(vr, 2),
                    "holding": "1 week to 1 month",
                    "reason": "Daily trend + support/breakout + momentum filter",
                    "trigger_key": f"SWING-{latest['bar_time']}"
                })

    # =========================
    # SHORT SWING SYSTEM
    # Target: 3% - 5%
    # Holding: 1 - 5 days
    # =========================

    if len(short_data) >= 50:
        closes = [x["close"] for x in short_data if x["close"] is not None]
        latest = short_data[-1]
        price = latest["close"]

        ema20 = ema(closes, 20)
        ema50 = ema(closes, 50)
        rsi14 = rsi(closes, 14)
        macd_line = macd(closes)

        support, resistance = support_resistance(short_data, 40)
        short_atr = atr(short_data, 14)
        vr = volume_ratio(short_data, 20)

        trend_score = 0
        momentum_score = 0
        structure_score = 0
        volume_score = 0

        if ema20 and ema50 and ema20 >= ema50 * 0.995:
            trend_score += 25

        if ema20 and price >= ema20 * 0.995:
            trend_score += 15

        if rsi14 and 45 <= rsi14 <= 75:
            momentum_score += 15

        if macd_line and macd_line >= 0:
            momentum_score += 10

        breakout = resistance and price >= resistance * 0.995
        near_support = support and price <= support * 1.035

        if breakout:
            structure_score += 25

        if near_support:
            structure_score += 15

        if vr >= 0.8:
            volume_score += 10

        if vr >= 1.2:
            volume_score += 15

        total_score = trend_score + momentum_score + structure_score + volume_score

        if total_score >= 50:
            entry = price

            if short_atr:
                stop = entry - short_atr * 1.5
            elif support:
                stop = support
            else:
                stop = entry * 0.97

            if support and support < entry:
                stop = max(stop, support)

            if stop >= entry:
                stop = entry * 0.97

            target1 = entry * 1.03
            target2 = entry * 1.05

            risk_pct = ((entry - stop) / entry) * 100
            expected_pct = ((target1 - entry) / entry) * 100
            rr = expected_pct / risk_pct if risk_pct > 0 else 0

            if expected_pct >= 3 and rr >= 0.75:
                signals.append({
                    "symbol": symbol,
                    "type": "SHORT_SWING",
                    "action": "SHORT SWING BUY",
                    "timeframe": active_tf,
                    "price": round(entry, 3),
                    "entry_zone": [round(entry * 0.995, 3), round(entry * 1.005, 3)],
                    "stop_loss": round(stop, 3),
                    "target1": round(target1, 3),
                    "target2": round(target2, 3),
                    "expected_move_pct": round(expected_pct, 2),
                    "risk_pct": round(risk_pct, 2),
                    "rr": round(rr, 2),
                    "score": round(total_score, 2),
                    "strength": classify_strength(total_score),
                    "trend": "UP" if trend_score >= 25 else "MIXED",
                    "support": round(support, 3) if support else None,
                    "resistance": round(resistance, 3) if resistance else None,
                    "rsi": round(rsi14, 2) if rsi14 else None,
                    "volume_ratio": round(vr, 2),
                    "holding": "1 to 5 days",
                    "reason": "Short trend + breakout/support + volume/momentum",
                    "trigger_key": f"SHORT-{latest['bar_time']}"
                })

    return signals


def build_alert_message(signal):
    t2 = signal.get("target2")
    t3 = signal.get("target3")

    targets = f"""
<b>Target 1:</b> {signal['target1']}
<b>Target 2:</b> {t2}
""".strip()

    if t3:
        targets += f"\n<b>Target 3:</b> {t3}"

    return f"""
🚨 <b>UAE Trading AI Alert</b>

<b>Symbol:</b> {signal['symbol']}
<b>Type:</b> {signal['type']}
<b>Action:</b> {signal['action']}
<b>Strength:</b> {signal['strength']}
<b>Score:</b> {signal['score']}

<b>Price:</b> {signal['price']}
<b>Entry Zone:</b> {signal['entry_zone'][0]} - {signal['entry_zone'][1]}
<b>Stop Loss:</b> {signal['stop_loss']}

{targets}

<b>Expected Move:</b> +{signal['expected_move_pct']}%
<b>Risk:</b> {signal['risk_pct']}%
<b>R/R:</b> {signal['rr']}

<b>Timeframe:</b> {signal['timeframe']}
<b>Holding:</b> {signal['holding']}
<b>RSI:</b> {signal.get('rsi')}
<b>Volume Ratio:</b> {signal['volume_ratio']}

<b>Reason:</b> {signal['reason']}

Dashboard:
{DASHBOARD_URL}
""".strip()


@app.get("/api/ai/dual-signals")
def api_dual_signals(symbol: Optional[str] = None):
    ensure_alert_tables()

    if symbol:
        signals = analyze_dual_symbol(symbol)
    else:
        signals = []
        for s in get_all_symbols():
            signals.extend(analyze_dual_symbol(s))

    signals = sorted(
        signals,
        key=lambda x: (x["score"], x["expected_move_pct"], x["rr"]),
        reverse=True
    )

    return {
        "mode": SYSTEM_MODE,
        "count": len(signals),
        "signals": signals
    }


@app.get("/api/ai/test-telegram")
def api_test_telegram():
    result = send_telegram_message(
        f"✅ UAE Trading AI Telegram is connected.\n\nDashboard:\n{DASHBOARD_URL}"
    )
    return result


@app.get("/api/ai/send-alerts")
def api_send_alerts(symbol: Optional[str] = None, dry_run: bool = True):
    ensure_alert_tables()

    conn = db()
    cur = conn.cursor()

    if symbol:
        all_signals = analyze_dual_symbol(symbol)
    else:
        all_signals = []
        for s in get_all_symbols():
            all_signals.extend(analyze_dual_symbol(s))

    sent = []
    skipped = []

    for sig in all_signals:
        message = build_alert_message(sig)

        try:
            cur.execute("""
                INSERT INTO ai_alerts_log
                (symbol, signal_type, trigger_key, sent_at, price, message, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                sig["symbol"],
                sig["type"],
                sig["trigger_key"],
                datetime.utcnow().isoformat(),
                sig["price"],
                message,
                json.dumps(sig)
            ))

            if not dry_run:
                tg = send_telegram_message(message)
            else:
                tg = {"ok": True, "dry_run": True}

            sent.append({
                "symbol": sig["symbol"],
                "type": sig["type"],
                "telegram": tg
            })

            conn.commit()

        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            skipped.append({
                "symbol": sig["symbol"],
                "type": sig["type"],
                "reason": "duplicate_alert"
            })

    conn.close()

    return {
        "mode": SYSTEM_MODE,
        "dry_run": dry_run,
        "signals_found": len(all_signals),
        "sent_count": len(sent),
        "skipped_count": len(skipped),
        "sent": sent,
        "skipped": skipped
    }


@app.get("/api/ai/alerts-log")
def api_alerts_log(limit: int = 50):
    ensure_alert_tables()

    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT *
        FROM ai_alerts_log
        ORDER BY id DESC
        LIMIT %s
    """, (limit,))

    rows = cur.fetchall()
    conn.close()

    return {
        "count": len(rows),
        "alerts": rows
    }
