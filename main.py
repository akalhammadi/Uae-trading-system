import os
import json
import math
import traceback
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="UAE Market PRO AI V14 Full Stable")

DATABASE_URL = os.getenv("DATABASE_URL")
SECRET = os.getenv("SECRET", "abc123")
CRON_SECRET = os.getenv("CRON_SECRET", "cron123")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "tgsecret123")

BASE_URL = os.getenv("API_BASE", "https://uae-market-production.up.railway.app").rstrip("/")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", f"{BASE_URL}/dashboard")

AI_MODE = os.getenv("AI_MODE", "PAPER").upper().strip()
LEARNING_DAYS = int(os.getenv("LEARNING_DAYS", "14"))
CAPITAL = float(os.getenv("CAPITAL", "200000"))

HYBRID_STRONG_ALERTS = os.getenv("HYBRID_STRONG_ALERTS", "true").lower() == "true"
STRONG_ALERT_SCORE = float(os.getenv("STRONG_ALERT_SCORE", "90"))
STRONG_ALERT_MIN_RR = float(os.getenv("STRONG_ALERT_MIN_RR", "0.9"))

OBSERVATION_LEARNING = os.getenv("OBSERVATION_LEARNING", "true").lower() == "true"
OBSERVATION_TARGET_PCT = float(os.getenv("OBSERVATION_TARGET_PCT", "3.0"))
OBSERVATION_DROP_PCT = float(os.getenv("OBSERVATION_DROP_PCT", "2.0"))
TELEGRAM_TOP_N = int(os.getenv("TELEGRAM_TOP_N", "5"))

MIN_H1_CANDLES = int(os.getenv("MIN_H1_CANDLES", "20"))
MIN_D1_CANDLES = int(os.getenv("MIN_D1_CANDLES", "5"))
SCAN_MAX_ERRORS = int(os.getenv("SCAN_MAX_ERRORS", "100"))

UAE_TZ_OFFSET = timedelta(hours=4)

WATCHLIST = [
    "DTC","DU","EAND","EMSTEEL","ESHRAQ","GFH","GHITHA","GULFNAV",
    "MANAZEL","PRESIGHT","SALIK","SHUAA","SIB","UPP","TECOM","JULPHAR",
    "2POINTZERO","INVICTUS","MODON","EMPOWER","SPACE42","ADPORTS",
    "RAKPROP","ALEFEDT","TALABAT","PUREHEALTH","TAQA","NMDC",
    "RAKBANK","FAB","ADIB","ADNOCGAS","ADNOCDRILL","ADNOCLS",
    "ADNOCDIST","BURJEEL","BOROUGE","DEWA","DIB","EMAARDEV",
    "EMAAR","AIRARABIA","ESG","AGTHIA","AMR","APEX","ARMX",
    "ALDAR","FERTIGLB","DANA","DFM","AJMANBANK","DIC"
]


# ============================================================
# TIME + HELPERS
# ============================================================

def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)

def utc_now() -> str:
    return utc_now_dt().isoformat()

def uae_now_dt() -> datetime:
    return utc_now_dt() + UAE_TZ_OFFSET

def is_uae_trading_day(dt: Optional[datetime] = None) -> bool:
    d = dt or uae_now_dt()
    return d.weekday() in [0, 1, 2, 3, 4]

def is_uae_market_time(dt: Optional[datetime] = None) -> bool:
    d = dt or uae_now_dt()
    return is_uae_trading_day(d) and 10 <= d.hour < 15

def business_days_between(start: datetime, end: datetime) -> int:
    if start > end:
        return 0
    count = 0
    cur = (start + UAE_TZ_OFFSET).date()
    end_date = (end + UAE_TZ_OFFSET).date()
    while cur <= end_date:
        if cur.weekday() in [0, 1, 2, 3, 4]:
            count += 1
        cur += timedelta(days=1)
    return count

def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=10)

def normalize_symbol(symbol: str) -> str:
    s = str(symbol or "").upper().replace(" ", "").strip()
    if ":" in s:
        s = s.split(":")[-1]
    if "." in s:
        s = s.split(".")[0]
    return s

def normalize_tf(tf: str) -> str:
    t = str(tf or "").strip()
    if t in ["1h", "1H", "60m", "60M", "H1", "60"]:
        return "60"
    if t in ["D", "1d", "1D", "daily", "DAY"]:
        return "1D"
    return t

def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        x = float(str(value).replace(",", ""))
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default

def parse_bar_time(value) -> str:
    if value is None:
        return utc_now()
    s = str(value).strip()
    try:
        if s.isdigit():
            n = int(s)
            if n > 10_000_000_000:
                return datetime.fromtimestamp(n / 1000, timezone.utc).isoformat()
            return datetime.fromtimestamp(n, timezone.utc).isoformat()
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return s

def parse_dt(value: str):
    try:
        if not value:
            return None
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def esc(x) -> str:
    return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS system_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT NOT NULL)""")

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
            received_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_id ON candles(symbol, timeframe, id DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_scan_results (
            id SERIAL PRIMARY KEY,
            scan_type TEXT NOT NULL,
            mode TEXT NOT NULL,
            created_at TEXT NOT NULL,
            watchlist_count INTEGER,
            scanned_count INTEGER,
            signals_count INTEGER,
            payload TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_scan_results_type_id ON ai_scan_results(scan_type, id DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_alerts_log (
            id SERIAL PRIMARY KEY,
            alert_key TEXT UNIQUE NOT NULL,
            symbol TEXT,
            signal_type TEXT,
            created_at TEXT NOT NULL,
            payload TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_virtual_signals (
            id SERIAL PRIMARY KEY,
            signal_key TEXT UNIQUE NOT NULL,
            mode TEXT NOT NULL,
            symbol TEXT NOT NULL,
            signal_type TEXT,
            timeframe TEXT,
            action TEXT,
            price DOUBLE PRECISION,
            entry_low DOUBLE PRECISION,
            entry_high DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION,
            target1 DOUBLE PRECISION,
            target2 DOUBLE PRECISION,
            target3 DOUBLE PRECISION,
            score DOUBLE PRECISION,
            strength TEXT,
            rr DOUBLE PRECISION,
            risk_pct DOUBLE PRECISION,
            target_pct DOUBLE PRECISION,
            max_hold_days INTEGER,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            outcome TEXT,
            outcome_at TEXT,
            max_high DOUBLE PRECISION,
            min_low DOUBLE PRECISION,
            bars_checked INTEGER DEFAULT 0,
            payload TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_observations (
            id SERIAL PRIMARY KEY,
            obs_key TEXT UNIQUE NOT NULL,
            symbol TEXT NOT NULL,
            scan_type TEXT,
            timeframe TEXT,
            action TEXT,
            model_action TEXT,
            strength TEXT,
            score DOUBLE PRECISION,
            rank_score DOUBLE PRECISION,
            price DOUBLE PRECISION,
            observed_at TEXT NOT NULL,
            status TEXT NOT NULL,
            outcome TEXT,
            outcome_at TEXT,
            max_high DOUBLE PRECISION,
            min_low DOUBLE PRECISION,
            return_pct DOUBLE PRECISION,
            payload TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_learning_stats (
            symbol TEXT PRIMARY KEY,
            trades_count INTEGER DEFAULT 0,
            wins_count INTEGER DEFAULT 0,
            losses_count INTEGER DEFAULT 0,
            virtual_short_count INTEGER DEFAULT 0,
            virtual_short_wins INTEGER DEFAULT 0,
            virtual_short_losses INTEGER DEFAULT 0,
            virtual_long_count INTEGER DEFAULT 0,
            virtual_long_wins INTEGER DEFAULT 0,
            virtual_long_losses INTEGER DEFAULT 0,
            avg_return_pct DOUBLE PRECISION DEFAULT 0,
            score_adjustment DOUBLE PRECISION DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS batch_scan_state (
            key TEXT PRIMARY KEY,
            next_index INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()

def get_setting(key: str, default=None):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT value FROM system_settings WHERE key=%s", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else default

def set_setting(key: str, value: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO system_settings(key,value,updated_at)
        VALUES(%s,%s,%s)
        ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
    """, (key, value, utc_now()))
    conn.commit()
    conn.close()

def ensure_learning_start():
    if not get_setting("learning_started_at"):
        set_setting("learning_started_at", utc_now())

@app.on_event("startup")
def startup():
    init_db()
    ensure_learning_start()


# ============================================================
# MODE
# ============================================================

def get_ai_mode():
    return os.getenv("AI_MODE", get_setting("ai_mode", AI_MODE) or "PAPER").upper().strip()

def learning_age_days():
    start = parse_dt(get_setting("learning_started_at"))
    if not start:
        return 0
    return max(0, business_days_between(start, utc_now_dt()) - 1)

def learning_remaining_days():
    return max(0, LEARNING_DAYS - learning_age_days())


# ============================================================
# BASIC ROUTES
# ============================================================

@app.get("/")
def home():
    return {
        "ok": True,
        "status": "UAE PRO AI V14 Full Stable Running",
        "version": "V14_FULL_STABLE",
        "mode": get_ai_mode(),
        "uae_now": uae_now_dt().isoformat(),
        "is_trading_day": is_uae_trading_day(),
        "is_market_time": is_uae_market_time(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
    }

@app.get("/api/health")
def health():
    return {
        "ok": True,
        "version": "V14_FULL_STABLE",
        "mode": get_ai_mode(),
        "uae_now": uae_now_dt().isoformat(),
        "is_trading_day": is_uae_trading_day(),
        "is_market_time": is_uae_market_time(),
    }

@app.get("/api/healthz")
def healthz():
    return health()

@app.get("/api/watchlist")
def watchlist():
    return {"ok": True, "count": len(WATCHLIST), "stocks": WATCHLIST}

@app.get("/api/system/mode")
def api_mode():
    return {
        "ok": True,
        "mode": get_ai_mode(),
        "learning_days": LEARNING_DAYS,
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "weekend_excluded": True,
    }

@app.get("/api/system/set-mode")
def api_set_mode(mode: str):
    mode = mode.upper().strip()
    if mode not in ["LEARNING", "PAPER", "LIVE"]:
        return {"ok": False, "error": "mode must be LEARNING, PAPER, LIVE"}
    set_setting("ai_mode", mode)
    return {"ok": True, "mode": mode}


# ============================================================
# TRADINGVIEW WEBHOOK
# ============================================================

@app.post("/webhook/tradingview")
async def tradingview_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"ok": False, "error": "bad_json"}

    if data.get("secret") != SECRET:
        return {"ok": False, "error": "bad_secret"}

    symbol = normalize_symbol(data.get("symbol"))
    tf = normalize_tf(data.get("timeframe") or data.get("interval"))

    o = safe_float(data.get("open"))
    h = safe_float(data.get("high"))
    l = safe_float(data.get("low"))
    c = safe_float(data.get("close") or data.get("price"))
    v = safe_float(data.get("volume"), 0)

    if not symbol or tf not in ["60", "1D"]:
        return {"ok": False, "error": "bad_symbol_or_timeframe", "symbol": symbol, "timeframe": tf}

    if None in [o, h, l, c]:
        return {"ok": False, "error": "bad_ohlc", "payload": data}

    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO candles(symbol,exchange,timeframe,bar_time,open,high,low,close,volume,received_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        symbol, data.get("exchange"), tf,
        parse_bar_time(data.get("time") or data.get("timenow")),
        o, h, l, c, v, utc_now()
    ))
    conn.commit()
    conn.close()

    return {"ok": True, "symbol": symbol, "timeframe": tf, "close": c}

@app.post("/api/webhook/price-alert")
@app.get("/api/webhook/price-alert")
async def price_alert_webhook(request: Request, secret: Optional[str] = None):
    try:
        if secret != SECRET and secret != CRON_SECRET:
            return {"ok": False, "error": "bad_secret"}

        try:
            data = await request.json()
        except Exception:
            data = dict(request.query_params)

        symbol = normalize_symbol(
            data.get("symbol") or data.get("ticker") or data.get("syminfo.ticker")
        )

        tf = normalize_tf(
            data.get("timeframe") or data.get("interval") or data.get("tf") or "60"
        )

        o = safe_float(data.get("open") or data.get("o"))
        h = safe_float(data.get("high") or data.get("h"))
        l = safe_float(data.get("low") or data.get("l"))
        c = safe_float(data.get("close") or data.get("price") or data.get("c"))
        v = safe_float(data.get("volume") or data.get("v"), 0)

        if not symbol:
            return {"ok": False, "error": "missing_symbol", "received": data}

        if tf not in ["60", "1D"]:
            return {"ok": False, "error": "bad_timeframe", "timeframe": tf, "received": data}

        if None in [o, h, l, c]:
            return {
                "ok": False,
                "error": "missing_ohlc",
                "symbol": symbol,
                "timeframe": tf,
                "received": data
            }

        conn = db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO candles(symbol,exchange,timeframe,bar_time,open,high,low,close,volume,received_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            symbol,
            data.get("exchange") or "TRADINGVIEW",
            tf,
            parse_bar_time(data.get("time") or data.get("timenow")),
            o, h, l, c, v,
            utc_now()
        ))
        conn.commit()
        conn.close()

        return {
            "ok": True,
            "saved": True,
            "symbol": symbol,
            "timeframe": tf,
            "close": c
        }

    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "trace": traceback.format_exc()[-1500:]
        }
        
# ============================================================
# CANDLES + COVERAGE
# ============================================================

def get_candles(symbol: str, timeframe: str, limit: int = 220):
    timeframe = normalize_tf(timeframe)
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT * FROM candles
        WHERE symbol=%s AND timeframe=%s
        ORDER BY id DESC
        LIMIT %s
    """, (normalize_symbol(symbol), timeframe, limit))
    rows = list(reversed(cur.fetchall()))
    conn.close()
    return rows

@app.get("/api/candles/latest")
def latest_candles(symbol: Optional[str] = None, timeframe: Optional[str] = None, limit: int = 100):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    q = "SELECT * FROM candles WHERE 1=1"
    params = []

    if symbol:
        q += " AND symbol=%s"
        params.append(normalize_symbol(symbol))
    if timeframe:
        q += " AND timeframe=%s"
        params.append(normalize_tf(timeframe))

    q += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return {"ok": True, "count": len(rows), "candles": rows}

@app.get("/api/watchlist/coverage")
def coverage():
    out = []
    ready_count = 0

    for s in WATCHLIST:
        h1 = len(get_candles(s, "60", 25))
        d1 = len(get_candles(s, "1D", 10))
        ready = h1 >= MIN_H1_CANDLES and d1 >= MIN_D1_CANDLES

        if ready:
            ready_count += 1

        out.append({
            "symbol": s,
            "has_1h": h1 > 0,
            "has_1d": d1 > 0,
            "h1_count": h1,
            "d1_count": d1,
            "ready": ready
        })

    return {"ok": True, "count": len(out), "ready_count": ready_count, "coverage": out}

@app.get("/api/coverage")
def coverage_alias():
    return coverage()


# ============================================================
# INDICATORS
# ============================================================

def sma(values, n):
    vals = [x for x in values if x is not None]
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n

def ema(values, n):
    vals = [x for x in values if x is not None]
    if len(vals) < n:
        return None
    k = 2 / (n + 1)
    e = sum(vals[:n]) / n
    for price in vals[n:]:
        e = price * k + e * (1 - k)
    return e

def rsi(values, n=14):
    vals = [x for x in values if x is not None]
    if len(vals) < n + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(vals)):
        d = vals[i] - vals[i - 1]
        gains.append(max(d, 0))
        losses.append(abs(min(d, 0)))
    ag = sum(gains[-n:]) / n
    al = sum(losses[-n:]) / n
    if al == 0:
        return 100
    rs = ag / al
    return 100 - (100 / (1 + rs))

def atr(candles, n=14):
    if len(candles) < n + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h = safe_float(candles[i]["high"])
        l = safe_float(candles[i]["low"])
        pc = safe_float(candles[i - 1]["close"])
        if h is None or l is None or pc is None:
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < n:
        return None
    return sum(trs[-n:]) / n

def support_resistance(candles, lookback=30):
    valid = [x for x in candles if safe_float(x.get("low")) is not None and safe_float(x.get("high")) is not None]
    if not valid:
        return None, None
    recent = valid[-lookback:] if len(valid) >= lookback else valid
    return min(float(x["low"]) for x in recent), max(float(x["high"]) for x in recent)

def recent_momentum(candles, lookback=8):
    if len(candles) < lookback + 1:
        return 0
    start = safe_float(candles[-lookback]["close"])
    end = safe_float(candles[-1]["close"])
    return ((end - start) / start) * 100 if start else 0


# ============================================================
# LEARNING
# ============================================================

def get_learning_adjustment(symbol: str):
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT score_adjustment FROM ai_learning_stats WHERE symbol=%s", (normalize_symbol(symbol),))
        row = cur.fetchone()
        conn.close()
        return float(row["score_adjustment"] or 0) if row else 0
    except Exception:
        return 0

def update_learning(symbol: str, ret_pct: float, signal_type: str, is_virtual: bool):
    symbol = normalize_symbol(symbol)
    signal_type = str(signal_type or "").upper()
    ret_pct = safe_float(ret_pct, 0) or 0

    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_learning_stats WHERE symbol=%s", (symbol,))
    row = cur.fetchone()

    if row:
        trades = int(row["trades_count"] or 0)
        wins = int(row["wins_count"] or 0)
        losses = int(row["losses_count"] or 0)
        vs = int(row["virtual_short_count"] or 0)
        vsw = int(row["virtual_short_wins"] or 0)
        vsl = int(row["virtual_short_losses"] or 0)
        vl = int(row["virtual_long_count"] or 0)
        vlw = int(row["virtual_long_wins"] or 0)
        vll = int(row["virtual_long_losses"] or 0)

        if is_virtual and signal_type == "SHORT_SWING":
            vs += 1
            vsw += 1 if ret_pct > 0 else 0
            vsl += 1 if ret_pct <= 0 else 0
        elif is_virtual and signal_type == "LONG_SWING":
            vl += 1
            vlw += 1 if ret_pct > 0 else 0
            vll += 1 if ret_pct <= 0 else 0
        else:
            trades += 1
            wins += 1 if ret_pct > 0 else 0
            losses += 1 if ret_pct <= 0 else 0

        total_events = max(trades + vs + vl, 1)
        old_avg = float(row["avg_return_pct"] or 0)
        avg_return = ((old_avg * max(total_events - 1, 0)) + ret_pct) / total_events

        total_wins = wins + vsw + vlw
        total_losses = losses + vsl + vll
        closed = total_wins + total_losses
        win_rate = total_wins / closed if closed else 0
        score_adj = max(-15, min(15, (win_rate - 0.5) * 30 + avg_return))

        cur.execute("""
            UPDATE ai_learning_stats
            SET trades_count=%s,wins_count=%s,losses_count=%s,
                virtual_short_count=%s,virtual_short_wins=%s,virtual_short_losses=%s,
                virtual_long_count=%s,virtual_long_wins=%s,virtual_long_losses=%s,
                avg_return_pct=%s,score_adjustment=%s,updated_at=%s
            WHERE symbol=%s
        """, (trades,wins,losses,vs,vsw,vsl,vl,vlw,vll,avg_return,score_adj,utc_now(),symbol))
    else:
        trades=wins=losses=vs=vsw=vsl=vl=vlw=vll=0

        if is_virtual and signal_type == "SHORT_SWING":
            vs = 1
            vsw = 1 if ret_pct > 0 else 0
            vsl = 1 if ret_pct <= 0 else 0
        elif is_virtual and signal_type == "LONG_SWING":
            vl = 1
            vlw = 1 if ret_pct > 0 else 0
            vll = 1 if ret_pct <= 0 else 0
        else:
            trades = 1
            wins = 1 if ret_pct > 0 else 0
            losses = 1 if ret_pct <= 0 else 0

        score_adj = 5 if ret_pct > 0 else -5

        cur.execute("""
            INSERT INTO ai_learning_stats
            (symbol,trades_count,wins_count,losses_count,
             virtual_short_count,virtual_short_wins,virtual_short_losses,
             virtual_long_count,virtual_long_wins,virtual_long_losses,
             avg_return_pct,score_adjustment,updated_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (symbol,trades,wins,losses,vs,vsw,vsl,vl,vlw,vll,ret_pct,score_adj,utc_now()))

    conn.commit()
    conn.close()


# ============================================================
# SIGNAL ANALYSIS
# ============================================================

def smart_stop(entry, entry_low, support, atr_value, kind):
    if kind == "SHORT_SWING":
        min_stop_pct, max_stop_pct, atr_mult = 0.018, 0.045, 2.0
    else:
        min_stop_pct, max_stop_pct, atr_mult = 0.045, 0.11, 2.5

    buffer_pct = 0.006
    candidates = []

    if support and support < entry_low:
        candidates.append(support * (1 - buffer_pct))

    if atr_value:
        atr_stop = entry - (atr_value * atr_mult)
        if atr_stop < entry_low:
            candidates.append(atr_stop)

    stop = min(candidates) if candidates else entry * (1 - min_stop_pct)

    max_allowed = entry_low * (1 - min_stop_pct)
    min_allowed = entry * (1 - max_stop_pct)

    if stop >= entry_low:
        stop = max_allowed
    if stop > max_allowed:
        stop = max_allowed
    if stop < min_allowed:
        stop = min_allowed

    return stop

def position_sizing(entry, stop, kind):
    risk_pct_per_trade = 0.01 if kind == "SHORT_SWING" else 0.015
    max_position_pct = 0.18 if kind == "SHORT_SWING" else 0.25
    risk_per_share = max(entry - stop, 0)
    max_risk_aed = CAPITAL * risk_pct_per_trade

    if risk_per_share <= 0 or entry <= 0:
        return {"qty": 0, "position_value": 0, "max_risk_aed": round(max_risk_aed, 2)}

    qty_by_risk = max_risk_aed / risk_per_share
    max_position_value = CAPITAL * max_position_pct
    qty_by_cap = max_position_value / entry
    qty = max(0, min(qty_by_risk, qty_by_cap))

    return {
        "qty": round(qty, 2),
        "position_value": round(qty * entry, 2),
        "max_risk_aed": round(max_risk_aed, 2),
        "max_position_value": round(max_position_value, 2)
    }

def no_data_result(symbol, reason, h1_count=0, d1_count=0):
    return {
        "symbol": normalize_symbol(symbol),
        "has_data": False,
        "action": "NO_DATA",
        "model_action": "NO_DATA",
        "display_action": "NO_DATA",
        "reason": reason,
        "ai_comment": reason,
        "score": None,
        "rank_score": None,
        "strength": None,
        "price": None,
        "rr": None,
        "volume_ratio": None,
        "h1_count": h1_count,
        "d1_count": d1_count,
    }

def daily_trend_score(d1):
    if len(d1) < MIN_D1_CANDLES:
        return "UNKNOWN", 0

    closes = [safe_float(x["close"]) for x in d1]
    closes = [x for x in closes if x is not None]

    if len(closes) < MIN_D1_CANDLES:
        return "UNKNOWN", 0

    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50) if len(closes) >= 50 else ma20

    score = 0
    if ma20 and closes[-1] > ma20:
        score += 12
    if ma50 and closes[-1] > ma50:
        score += 12
    if ma20 and ma50 and ma20 >= ma50:
        score += 8

    trend = "UP" if score >= 20 else "MIXED" if score >= 10 else "DOWN"
    return trend, score

def ai_rank_score(sig: Dict[str, Any]) -> float:
    score = float(sig.get("score") or 0)
    rr = min(float(sig.get("rr") or 0), 5) * 4
    vol = min(float(sig.get("volume_ratio") or 0), 3) * 3
    trend_bonus = 10 if sig.get("trend") == "UP" else 3 if sig.get("trend") == "MIXED" else 0
    strength_bonus = {"VERY STRONG": 12, "STRONG": 7, "MEDIUM": 3, "WEAK": 0}.get(sig.get("strength"), 0)
    return round(score + rr + vol + trend_bonus + strength_bonus, 2)

def ai_comment(sig: Optional[Dict[str, Any]]) -> str:
    if not sig:
        return "No data yet"
    if sig.get("model_action") == "BUY":
        return "Trade setup candidate"
    if sig.get("strength") == "VERY STRONG":
        return "Strong watch; waiting for confirmation"
    if sig.get("strength") == "STRONG":
        return "Good watch; monitor next candle"
    if sig.get("strength") == "MEDIUM":
        return "Neutral watch"
    return "Weak or unclear setup"

def classify_action(sig: Optional[Dict[str, Any]]) -> str:
    if not sig:
        return "NO_DATA"
    if sig.get("model_action") == "BUY":
        return "BUY"
    if sig.get("strength") == "VERY STRONG":
        return "STRONG WATCH"
    if sig.get("strength") in ["STRONG", "MEDIUM", "WEAK"]:
        return "WATCH"
    return sig.get("action", "NO_DATA")

def is_hybrid_strong_signal(sig: Dict[str, Any]) -> bool:
    return (
        get_ai_mode() == "LEARNING"
        and HYBRID_STRONG_ALERTS
        and sig.get("strength") == "VERY STRONG"
        and float(sig.get("score") or 0) >= STRONG_ALERT_SCORE
        and float(sig.get("rr") or 0) >= STRONG_ALERT_MIN_RR
    )

def build_signal(symbol, kind, candles, d1):
    symbol = normalize_symbol(symbol)
    required = MIN_H1_CANDLES if kind == "SHORT_SWING" else MIN_D1_CANDLES

    if len(candles) < required:
        return None

    closes = [safe_float(x["close"]) for x in candles]
    volumes = [safe_float(x.get("volume"), 0) or 0 for x in candles]
    closes = [x for x in closes if x is not None]

    if len(closes) < required:
        return None

    price = closes[-1]
    if not price or price <= 0:
        return None

    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50) if len(closes) >= 50 else ma20
    ema20_v = ema(closes, 20)
    r = rsi(closes, 14)
    a = atr(candles, 14)
    support, resistance = support_resistance(candles, 30 if kind == "SHORT_SWING" else 60)
    avg_vol = sma(volumes, 20) or 1
    volume_ratio = volumes[-1] / avg_vol if avg_vol else 1
    momentum = recent_momentum(candles, 8 if kind == "SHORT_SWING" else 15)
    trend, _ = daily_trend_score(d1)

    score = 0
    reasons = []

    if ma20 and price > ma20:
        score += 16 if kind == "SHORT_SWING" else 12
        reasons.append("Price above MA20")
    if ma50 and price > ma50:
        score += 14
        reasons.append("Price above MA50")
    if ema20_v and price > ema20_v:
        score += 8
        reasons.append("Price above EMA20")
    if trend == "UP":
        score += 22 if kind == "SHORT_SWING" else 28
        reasons.append("Daily trend positive")
    elif trend == "MIXED":
        score += 8
        reasons.append("Daily trend mixed")
    if r is not None and 45 <= r <= 68:
        score += 14
        reasons.append("RSI healthy")
    elif r is not None and 68 < r <= 75:
        score += 5
        reasons.append("RSI extended")
    if volume_ratio >= 1.4:
        score += 15
        reasons.append("Strong volume")
    elif volume_ratio >= 1.15:
        score += 8
        reasons.append("Volume improving")
    if resistance and price >= resistance * 0.985:
        score += 8
        reasons.append("Near breakout zone")
    if momentum > (1.0 if kind == "SHORT_SWING" else 2.5):
        score += 8
        reasons.append("Positive momentum")

    score += get_learning_adjustment(symbol)

    entry = price
    entry_low = entry * 0.995
    entry_high = entry * 1.005
    stop = smart_stop(entry, entry_low, support, a, kind)

    if kind == "SHORT_SWING":
        target_pct = 3.0
        t1, t2, t3 = entry * 1.03, entry * 1.05, entry * 1.055
        max_hold_days, timeframe = 5, "60"
    else:
        target_pct = 7.0
        t1, t2, t3 = entry * 1.07, entry * 1.12, entry * 1.15
        max_hold_days, timeframe = 25, "1D"

    risk_pct = ((entry - stop) / entry) * 100 if entry else 0
    rr = ((t1 - entry) / (entry - stop)) if entry > stop else 0

    strength = "VERY STRONG" if score >= 85 else "STRONG" if score >= 70 else "MEDIUM" if score >= 55 else "WEAK"
    model_action = "BUY" if score >= 70 and rr >= 0.9 and (risk_pct <= (4.5 if kind == "SHORT_SWING" else 11)) else "WATCH"

    mode = get_ai_mode()
    if mode == "LEARNING":
        action = "LEARN_SIGNAL" if model_action == "BUY" else "WATCH"
    elif mode == "PAPER":
        action = "PAPER_BUY" if model_action == "BUY" else "WATCH"
    else:
        action = "BUY" if model_action == "BUY" else "WATCH"

    hybrid_alert = (
        mode == "LEARNING" and HYBRID_STRONG_ALERTS and strength == "VERY STRONG"
        and score >= STRONG_ALERT_SCORE and rr >= STRONG_ALERT_MIN_RR
    )
    if hybrid_alert:
        action = "STRONG_LEARNING_ALERT"

    result = {
        "symbol": symbol,
        "has_data": True,
        "type": kind,
        "mode": mode,
        "action": action,
        "model_action": model_action,
        "hybrid_alert": hybrid_alert,
        "timeframe": timeframe,
        "price": round(entry, 3),
        "entry_zone": [round(entry_low, 3), round(entry_high, 3)],
        "stop_loss": round(stop, 3),
        "target1": round(t1, 3),
        "target2": round(t2, 3),
        "target3": round(t3, 3),
        "target_pct": target_pct,
        "expected_move_pct": target_pct,
        "risk_pct": round(risk_pct, 2),
        "rr": round(rr, 2),
        "score": round(score, 2),
        "strength": strength,
        "trend": trend,
        "support": round(support, 3) if support else None,
        "resistance": round(resistance, 3) if resistance else None,
        "rsi": round(r, 2) if r is not None else None,
        "volume_ratio": round(volume_ratio, 2),
        "momentum_pct": round(momentum, 2),
        "max_hold_days": max_hold_days,
        "holding": "1 to 5 days" if kind == "SHORT_SWING" else "1 to 4 weeks",
        "position_sizing": position_sizing(entry, stop, kind),
        "reason": " + ".join(reasons) if reasons else "No strong setup"
    }

    result["rank_score"] = ai_rank_score(result)
    result["ai_comment"] = ai_comment(result)
    result["display_action"] = classify_action(result)
    return result

def analyze_symbol(symbol: str, scan_type: str = "ALL"):
    symbol = normalize_symbol(symbol)
    scan_type = scan_type.upper()
    if scan_type == "COMBINED":
        scan_type = "ALL"

    h1 = get_candles(symbol, "60", 220)
    d1 = get_candles(symbol, "1D", 220)
    signals = []

    if scan_type in ["ALL", "HOURLY"]:
        short_sig = build_signal(symbol, "SHORT_SWING", h1, d1)
        if short_sig:
            signals.append(short_sig)

    if scan_type in ["ALL", "DAILY"]:
        long_sig = build_signal(symbol, "LONG_SWING", d1, d1)
        if long_sig:
            signals.append(long_sig)

    return signals

@app.get("/api/ai/analyze")
def api_analyze(symbol: str):
    symbol = normalize_symbol(symbol)
    h1 = len(get_candles(symbol, "60", 25))
    d1 = len(get_candles(symbol, "1D", 10))
    sigs = analyze_symbol(symbol, "ALL")
    return {"ok": True, "symbol": symbol, "h1_count": h1, "d1_count": d1, "signals_count": len(sigs), "signals": sigs}

@app.get("/api/analyze/{symbol}")
def api_analyze_alias(symbol: str):
    return api_analyze(symbol)


# ============================================================
# SCANS
# ============================================================

def save_scan_result(scan_type: str, payload: Dict[str, Any]):
    scan_type = scan_type.upper()
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ai_scan_results(scan_type,mode,created_at,watchlist_count,scanned_count,signals_count,payload)
        VALUES(%s,%s,%s,%s,%s,%s,%s)
    """, (
        scan_type,
        get_ai_mode(),
        utc_now(),
        len(WATCHLIST),
        payload.get("scanned_count", 0),
        payload.get("signals_count", 0),
        json.dumps(payload)
    ))
    conn.commit()
    conn.close()

def latest_scan_result(scan_type: str = "COMBINED"):
    scan_type = scan_type.upper()
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_scan_results WHERE scan_type=%s ORDER BY id DESC LIMIT 1", (scan_type,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return json.loads(row["payload"])

def run_scan(scan_type: str):
    scan_type = scan_type.upper()
    if scan_type == "COMBINED":
        scan_type = "ALL"

    ranked, signals, coverage_rows, errors = [], [], [], []
    scanned = 0

    for s in WATCHLIST:
        scanned += 1
        h1_count = len(get_candles(s, "60", 25))
        d1_count = len(get_candles(s, "1D", 10))

        try:
            sigs = analyze_symbol(s, scan_type)
            best = max(sigs, key=lambda x: x.get("rank_score", 0), default=None)

            if best:
                ranked.append(best)
                if best.get("model_action") == "BUY" or is_hybrid_strong_signal(best):
                    signals.append(best)

                coverage_rows.append({
                    "symbol": s,
                    "has_data": True,
                    "action": classify_action(best),
                    "model_action": best.get("model_action"),
                    "score": best.get("score"),
                    "rank_score": best.get("rank_score"),
                    "strength": best.get("strength"),
                    "price": best.get("price"),
                    "rr": best.get("rr"),
                    "volume_ratio": best.get("volume_ratio"),
                    "ai_comment": ai_comment(best),
                    "h1_count": h1_count,
                    "d1_count": d1_count,
                })
            else:
                if h1_count < MIN_H1_CANDLES and d1_count < MIN_D1_CANDLES:
                    reason = f"need more data: h1={h1_count}/{MIN_H1_CANDLES}, d1={d1_count}/{MIN_D1_CANDLES}"
                elif h1_count < MIN_H1_CANDLES:
                    reason = f"need more 1H candles: {h1_count}/{MIN_H1_CANDLES}"
                elif d1_count < MIN_D1_CANDLES:
                    reason = f"need more 1D candles: {d1_count}/{MIN_D1_CANDLES}"
                else:
                    reason = "no setup"
                coverage_rows.append(no_data_result(s, reason, h1_count, d1_count))
        except Exception as e:
            err = str(e)
            errors.append({"symbol": s, "error": err})
            coverage_rows.append({
                "symbol": s,
                "has_data": False,
                "action": "ERROR",
                "model_action": "ERROR",
                "score": None,
                "rank_score": None,
                "strength": "ERROR",
                "price": None,
                "rr": None,
                "volume_ratio": None,
                "ai_comment": err,
                "h1_count": h1_count,
                "d1_count": d1_count
            })
            if len(errors) >= SCAN_MAX_ERRORS:
                break

    ranked = sorted(ranked, key=lambda x: x.get("rank_score", 0), reverse=True)
    signals = sorted(signals, key=lambda x: x.get("rank_score", 0), reverse=True)

    payload = {
        "ok": True,
        "version": "V14_FULL_STABLE",
        "mode": get_ai_mode(),
        "scan_type": scan_type,
        "created_at": utc_now(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
        "scanned_count": scanned,
        "signals_count": len(signals),
        "signals": signals[:20],
        "ranked_count": len(ranked),
        "ranked": ranked,
        "errors_count": len(errors),
        "errors": errors[:20],
        "coverage": sorted(coverage_rows, key=lambda x: (x.get("rank_score") or -1), reverse=True)
    }

    if OBSERVATION_LEARNING:
        try:
            record_observations(payload)
        except Exception as e:
            payload["observation_error"] = str(e)

    return payload

@app.get("/api/ai/pro-scan")
def pro_scan(scan_type: str = "COMBINED", run: bool = False):
    scan_type = scan_type.upper()
    if run:
        data = run_scan(scan_type)
        save_scan_result(scan_type if scan_type != "COMBINED" else "COMBINED", data)
        return data

    data = latest_scan_result(scan_type)
    if not data:
        return {
            "ok": True,
            "mode": get_ai_mode(),
            "message": "No saved scan yet. Use /api/ai/pro-scan?run=true or cron first.",
            "signals": [],
            "coverage": []
        }
    return data


# ============================================================
# VIRTUAL SIGNALS + OBSERVATIONS
# ============================================================

def sig_key(sig):
    return f"{sig['symbol']}-{sig['type']}-{sig['price']}-{sig['target1']}-{sig['stop_loss']}"

def record_virtual_signal(sig):
    if not sig or not sig.get("has_data"):
        return False

    key = sig_key(sig)
    conn = db()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO ai_virtual_signals
            (signal_key,mode,symbol,signal_type,timeframe,action,price,entry_low,entry_high,
             stop_loss,target1,target2,target3,score,strength,rr,risk_pct,target_pct,max_hold_days,
             created_at,status,payload)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)
        """, (
            key,
            sig["mode"],
            sig["symbol"],
            sig["type"],
            sig["timeframe"],
            sig["action"],
            sig["price"],
            sig["entry_zone"][0],
            sig["entry_zone"][1],
            sig["stop_loss"],
            sig["target1"],
            sig["target2"],
            sig["target3"],
            sig["score"],
            sig["strength"],
            sig["rr"],
            sig["risk_pct"],
            sig["target_pct"],
            sig["max_hold_days"],
            utc_now(),
            json.dumps(sig)
        ))
        conn.commit()
        ok = True
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        ok = False
    except Exception:
        conn.rollback()
        ok = False
    finally:
        conn.close()

    return ok

def evaluate_virtual_signals():
    if not is_uae_trading_day():
        return []

    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_virtual_signals WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
    rows = cur.fetchall()
    evaluated = []

    for sig in rows:
        try:
            symbol = sig["symbol"]
            tf = normalize_tf(sig["timeframe"])
            created = parse_dt(sig["created_at"])
            candles = get_candles(symbol, tf, 400)

            relevant = []
            for c in candles:
                t = parse_dt(c["bar_time"]) or parse_dt(c["received_at"])
                if created and t and t >= created:
                    relevant.append(c)

            if not relevant:
                continue

            max_high = max(float(x["high"]) for x in relevant)
            min_low = min(float(x["low"]) for x in relevant)
            latest_close = float(relevant[-1]["close"])

            price = float(sig["price"])
            target1 = float(sig["target1"])
            stop = float(sig["stop_loss"])
            max_hold_days = int(sig["max_hold_days"] or 5)

            target_hit = max_high >= target1
            stop_hit = min_low <= stop

            status, outcome = "OPEN", None
            ret_pct = ((latest_close - price) / price) * 100

            if target_hit and not stop_hit:
                status, outcome = "CLOSED", "TARGET1_HIT"
                ret_pct = ((target1 - price) / price) * 100
            elif stop_hit and not target_hit:
                status, outcome = "CLOSED", "STOP_HIT"
                ret_pct = ((stop - price) / price) * 100
            elif target_hit and stop_hit:
                status, outcome = "CLOSED", "BOTH_TOUCHED_CONSERVATIVE_STOP"
                ret_pct = ((stop - price) / price) * 100
            elif created and business_days_between(created, utc_now_dt()) > max_hold_days:
                status, outcome = "CLOSED", "TIME_EXIT"
                ret_pct = ((latest_close - price) / price) * 100

            cur.execute("""
                UPDATE ai_virtual_signals
                SET max_high=%s,min_low=%s,bars_checked=%s,status=%s,outcome=%s,outcome_at=%s
                WHERE id=%s
            """, (
                max_high,
                min_low,
                len(relevant),
                status,
                outcome,
                utc_now() if status == "CLOSED" else None,
                sig["id"]
            ))

            if status == "CLOSED":
                update_learning(symbol, ret_pct, sig["signal_type"], is_virtual=True)
                evaluated.append({
                    "id": sig["id"],
                    "symbol": symbol,
                    "type": sig["signal_type"],
                    "outcome": outcome,
                    "return_pct": round(ret_pct, 2)
                })
        except Exception:
            continue

    conn.commit()
    conn.close()
    return evaluated

def record_observations(scan_payload: Dict[str, Any]):
    conn = db()
    cur = conn.cursor()
    created = 0

    for item in scan_payload.get("ranked", []):
        if not item.get("has_data") or not item.get("price"):
            continue

        key = f"{item.get('symbol')}-{scan_payload.get('scan_type')}-{item.get('timeframe')}-{item.get('price')}-{scan_payload.get('created_at')[:13]}"

        try:
            cur.execute("""
                INSERT INTO ai_observations
                (obs_key,symbol,scan_type,timeframe,action,model_action,strength,score,rank_score,price,observed_at,status,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)
                ON CONFLICT DO NOTHING
            """, (
                key,
                item.get("symbol"),
                scan_payload.get("scan_type"),
                item.get("timeframe"),
                item.get("display_action") or item.get("action"),
                item.get("model_action"),
                item.get("strength"),
                item.get("score"),
                item.get("rank_score"),
                item.get("price"),
                scan_payload.get("created_at"),
                json.dumps(item)
            ))
            created += cur.rowcount
        except Exception:
            conn.rollback()

    conn.commit()
    conn.close()
    return created

def evaluate_observations():
    if not is_uae_trading_day():
        return []

    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_observations WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
    rows = cur.fetchall()
    out = []

    for obs in rows:
        try:
            tf = normalize_tf(obs["timeframe"] or "60")
            symbol = obs["symbol"]
            observed = parse_dt(obs["observed_at"])
            candles = get_candles(symbol, tf, 400)

            relevant = []
            for c in candles:
                t = parse_dt(c["bar_time"]) or parse_dt(c["received_at"])
                if observed and t and t >= observed:
                    relevant.append(c)

            if not relevant:
                continue

            price = float(obs["price"] or 0)
            if price <= 0:
                continue

            max_high = max(float(x["high"]) for x in relevant)
            min_low = min(float(x["low"]) for x in relevant)
            latest = float(relevant[-1]["close"])
            return_pct = ((latest - price) / price) * 100

            target_hit = ((max_high - price) / price) * 100 >= OBSERVATION_TARGET_PCT
            drop_hit = ((price - min_low) / price) * 100 >= OBSERVATION_DROP_PCT

            status, outcome = "OPEN", None
            max_days = 5 if tf == "60" else 25

            if target_hit:
                status, outcome = "CLOSED", "WATCH_RALLIED"
            elif drop_hit:
                status, outcome = "CLOSED", "WATCH_DROPPED"
            elif observed and business_days_between(observed, utc_now_dt()) > max_days:
                status, outcome = "CLOSED", "WATCH_TIME_EXIT"

            cur.execute("""
                UPDATE ai_observations
                SET max_high=%s,min_low=%s,return_pct=%s,status=%s,outcome=%s,outcome_at=%s
                WHERE id=%s
            """, (
                max_high,
                min_low,
                return_pct,
                status,
                outcome,
                utc_now() if status == "CLOSED" else None,
                obs["id"]
            ))

            if status == "CLOSED":
                update_learning(symbol, return_pct, "OBSERVATION", is_virtual=True)
                out.append({"symbol": symbol, "outcome": outcome, "return_pct": round(return_pct, 2)})
        except Exception:
            continue

    conn.commit()
    conn.close()
    return out

@app.get("/api/ai/learning-scan")
def learning_scan():
    try:
        hourly = latest_scan_result("HOURLY")
        daily = latest_scan_result("DAILY")
        signals = []

        if hourly:
            signals.extend(hourly.get("signals", []))
        if daily:
            signals.extend(daily.get("signals", []))

        created = []
        for sig in signals:
            if record_virtual_signal(sig):
                created.append({"symbol": sig["symbol"], "type": sig["type"]})

        evaluated = evaluate_virtual_signals()
        observed_evaluated = evaluate_observations()

        return {
            "ok": True,
            "mode": get_ai_mode(),
            "created_count": len(created),
            "created": created,
            "evaluated_count": len(evaluated),
            "evaluated": evaluated,
            "observations_evaluated": len(observed_evaluated),
            "weekend_excluded": True
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}

@app.get("/api/ai/observations")
def api_observations(limit: int = 100):
    evaluated = evaluate_observations()
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_observations ORDER BY id DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    conn.close()
    return {"ok": True, "evaluated_now": evaluated, "count": len(rows), "observations": rows}


# ============================================================
# CRON + BATCH
# ============================================================

def cron_ok(secret: Optional[str]):
    return secret == CRON_SECRET

def get_batch_index():
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO batch_scan_state(key,next_index,updated_at)
        VALUES('main',0,%s)
        ON CONFLICT(key) DO NOTHING
    """, (utc_now(),))
    conn.commit()
    cur.execute("SELECT next_index FROM batch_scan_state WHERE key='main'")
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0

def set_batch_index(i: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO batch_scan_state(key,next_index,updated_at)
        VALUES('main',%s,%s)
        ON CONFLICT(key) DO UPDATE SET next_index=EXCLUDED.next_index, updated_at=EXCLUDED.updated_at
    """, (i, utc_now()))
    conn.commit()
    conn.close()

@app.get("/api/cron/batch-scan")
def batch_scan(secret: Optional[str] = None, limit: int = 8, send: bool = False):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    if not is_uae_trading_day():
        return {"ok": True, "skipped": True, "reason": "UAE weekend - no trading"}

    total = len(WATCHLIST)
    start = get_batch_index()
    end = min(start + limit, total)
    batch = WATCHLIST[start:end]
    next_index = 0 if end >= total else end
    set_batch_index(next_index)

    results = []
    batch_signals = []
    ranked = []
    coverage_rows = []

    for symbol in batch:
        h1_count = len(get_candles(symbol, "60", 25))
        d1_count = len(get_candles(symbol, "1D", 10))
        try:
            sigs = analyze_symbol(symbol, "ALL")
            best = max(sigs, key=lambda x: x.get("rank_score", 0), default=None)

            if best:
                ranked.append(best)
                if best.get("model_action") == "BUY" or is_hybrid_strong_signal(best):
                    batch_signals.append(best)
                coverage_rows.append({
                    "symbol": symbol,
                    "has_data": True,
                    "action": classify_action(best),
                    "model_action": best.get("model_action"),
                    "score": best.get("score"),
                    "rank_score": best.get("rank_score"),
                    "strength": best.get("strength"),
                    "price": best.get("price"),
                    "rr": best.get("rr"),
                    "volume_ratio": best.get("volume_ratio"),
                    "ai_comment": ai_comment(best),
                    "h1_count": h1_count,
                    "d1_count": d1_count,
                })
            else:
                coverage_rows.append(no_data_result(symbol, "no setup or not enough data", h1_count, d1_count))

            results.append({"symbol": symbol, "ok": bool(best), "best": best, "signals": sigs})
        except Exception as e:
            results.append({"symbol": symbol, "ok": False, "error": str(e)})
            coverage_rows.append(no_data_result(symbol, str(e), h1_count, d1_count))

    ranked = sorted(ranked, key=lambda x: x.get("rank_score", 0), reverse=True)
    batch_signals = sorted(batch_signals, key=lambda x: x.get("rank_score", 0), reverse=True)

    payload = {
        "ok": True,
        "version": "V14_FULL_STABLE",
        "mode": get_ai_mode(),
        "scan_type": "HOURLY",
        "created_at": utc_now(),
        "watchlist_count": total,
        "scanned_count": len(batch),
        "batch_start": start,
        "batch_end": end,
        "next_index": next_index,
        "signals_count": len(batch_signals),
        "signals": batch_signals[:20],
        "ranked_count": len(ranked),
        "ranked": ranked,
        "coverage": coverage_rows,
        "results": results
    }

    save_scan_result("HOURLY", payload)
    save_combined_scan()

    created = []
    for sig in batch_signals:
        if record_virtual_signal(sig):
            created.append({"symbol": sig["symbol"], "type": sig["type"]})

    evaluated = evaluate_virtual_signals()
    observed_evaluated = evaluate_observations()

    if send and batch_signals:
        tg_main_send(format_batch_summary(batch_signals, start, end))

    payload["created_virtual"] = len(created)
    payload["evaluated"] = len(evaluated)
    payload["observations_evaluated"] = len(observed_evaluated)
    return payload

@app.get("/api/cron/safe-batch")
def safe_batch_scan(secret: Optional[str] = None, limit: int = 8):
    return batch_scan(secret=secret, limit=limit, send=False)

@app.get("/api/cron/hourly-scan")
def cron_hourly_scan(secret: Optional[str] = None, send: bool = False):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    try:
        scan = run_scan("HOURLY")
        save_scan_result("HOURLY", scan)

        created = []
        for sig in scan["signals"]:
            if record_virtual_signal(sig):
                created.append({"symbol": sig["symbol"], "type": sig["type"]})

        evaluated = evaluate_virtual_signals()
        observed_evaluated = evaluate_observations()
        save_combined_scan()

        if send and scan["signals"]:
            tg_main_send(format_scan_summary(scan, "Hourly 1H Short Swing"))

        return {
            "ok": True,
            "scan_type": "HOURLY",
            "signals_count": scan["signals_count"],
            "ranked_count": scan["ranked_count"],
            "errors_count": scan.get("errors_count", 0),
            "created_virtual": len(created),
            "evaluated": len(evaluated),
            "observations_evaluated": len(observed_evaluated)
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-2000:]}

@app.get("/api/cron/daily-scan")
def cron_daily_scan(secret: Optional[str] = None, send: bool = True):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    if not is_uae_trading_day():
        return {"ok": True, "skipped": True, "reason": "UAE weekend - no daily scan"}

    try:
        scan = run_scan("DAILY")
        save_scan_result("DAILY", scan)

        created = []
        for sig in scan["signals"]:
            if record_virtual_signal(sig):
                created.append({"symbol": sig["symbol"], "type": sig["type"]})

        evaluated = evaluate_virtual_signals()
        observed_evaluated = evaluate_observations()
        save_combined_scan()

        if send:
            tg_main_send(format_daily_report())

        return {
            "ok": True,
            "scan_type": "DAILY",
            "signals_count": scan["signals_count"],
            "ranked_count": scan["ranked_count"],
            "errors_count": scan.get("errors_count", 0),
            "created_virtual": len(created),
            "evaluated": len(evaluated),
            "observations_evaluated": len(observed_evaluated)
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-2000:]}

def save_combined_scan():
    hourly = latest_scan_result("HOURLY") or {}
    daily = latest_scan_result("DAILY") or {}

    signals = []
    signals.extend(hourly.get("signals", []))
    signals.extend(daily.get("signals", []))
    signals = sorted(signals, key=lambda x: ((x.get("score") or 0), (x.get("rr") or 0)), reverse=True)[:20]

    ranked = []
    ranked.extend(hourly.get("ranked", []))
    ranked.extend(daily.get("ranked", []))
    ranked = sorted(ranked, key=lambda x: (x.get("rank_score") or 0), reverse=True)

    combined_coverage = []
    hcov = {x["symbol"]: x for x in hourly.get("coverage", [])}
    dcov = {x["symbol"]: x for x in daily.get("coverage", [])}

    for s in WATCHLIST:
        h = hcov.get(s)
        d = dcov.get(s)
        best = h if (h and (not d or (h.get("rank_score") or -1) >= (d.get("rank_score") or -1))) else d
        combined_coverage.append(best or {"symbol": s, "has_data": False, "action": "NO_DATA", "model_action": "NO_DATA", "score": None, "strength": None})

    combined = {
        "ok": True,
        "version": "V14_FULL_STABLE",
        "mode": get_ai_mode(),
        "scan_type": "COMBINED",
        "created_at": utc_now(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
        "scanned_count": len(WATCHLIST),
        "signals_count": len(signals),
        "signals": signals,
        "ranked_count": len(ranked),
        "ranked": ranked,
        "coverage": combined_coverage
    }

    save_scan_result("COMBINED", combined)


# ============================================================
# READINESS + REPORTS
# ============================================================

@app.get("/api/ai/readiness")
def readiness_report(secret: Optional[str] = None):
    try:
        evaluate_virtual_signals()
    except Exception:
        pass

    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals")
    total = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='CLOSED'")
    evaluated = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE 'TARGET%'")
    target_hits = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE '%STOP%'")
    stop_hits = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='OPEN'")
    open_signals = int(cur.fetchone()["c"])

    cur.execute("SELECT * FROM ai_learning_stats ORDER BY (virtual_short_wins + virtual_long_wins) DESC, avg_return_pct DESC LIMIT 10")
    best = cur.fetchall()

    cur.execute("""
        SELECT avg_return_pct, (virtual_short_count + virtual_long_count + trades_count) AS cnt
        FROM ai_learning_stats
        WHERE (virtual_short_count + virtual_long_count + trades_count) > 0
    """)
    rows = cur.fetchall()
    conn.close()

    win_rate = (target_hits / evaluated * 100) if evaluated else 0
    total_weight = sum(int(x["cnt"] or 0) for x in rows)
    avg_return = 0

    if total_weight:
        avg_return = sum(float(x["avg_return_pct"] or 0) * int(x["cnt"] or 0) for x in rows) / total_weight

    status = "NEED_MORE_DATA"
    reasons = []
    age = learning_age_days()

    if age < LEARNING_DAYS:
        reasons.append(f"Learning period not completed. Remaining {round(LEARNING_DAYS-age,1)} trading days.")
    if evaluated < 20:
        reasons.append("Not enough evaluated signals. Need at least 20.")
    if win_rate < 55:
        reasons.append("Win rate below 55%.")
    if avg_return <= 0:
        reasons.append("Average return is not positive yet.")

    if age >= LEARNING_DAYS and evaluated >= 20 and win_rate >= 55 and avg_return > 0:
        status = "READY_FOR_PAPER"
        reasons.append("Ready for PAPER mode, not full live size yet.")

    progress_pct = min(100, round((age / LEARNING_DAYS) * 100, 2)) if LEARNING_DAYS else 0

    return {
        "ok": True,
        "mode": get_ai_mode(),
        "status": status,
        "learning_age_days": round(age, 2),
        "learning_days_required": LEARNING_DAYS,
        "learning_progress_pct": progress_pct,
        "weekend_excluded": True,
        "total_signals": total,
        "evaluated_signals": evaluated,
        "target_hits": target_hits,
        "stop_hits": stop_hits,
        "open_signals": open_signals,
        "win_rate": round(win_rate, 2),
        "avg_return_pct": round(avg_return, 2),
        "best_stats": best,
        "reasons": reasons
    }

def get_signal_stats():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals")
    total = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='OPEN'")
    open_count = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='CLOSED'")
    closed_count = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE 'TARGET%'")
    target_hits = int(cur.fetchone()["c"])

    cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE '%STOP%'")
    stop_hits = int(cur.fetchone()["c"])

    conn.close()

    win_rate = round((target_hits / closed_count * 100), 2) if closed_count else 0

    return {
        "total_signals": total,
        "open_signals": open_count,
        "closed_signals": closed_count,
        "target_hits": target_hits,
        "stop_hits": stop_hits,
        "win_rate": win_rate,
    }

def format_daily_report():
    rep = readiness_report()
    stats = get_signal_stats()
    scan = latest_scan_result("COMBINED") or {}
    latest_ranked = scan.get("ranked", [])[:5]

    lines = [
        "<b>UAE AI Daily Market Report</b>",
        f"Date UAE: <b>{uae_now_dt().strftime('%Y-%m-%d')}</b>",
        f"Mode: <b>{rep['mode']}</b>",
        f"Status: <b>{rep['status']}</b>",
        f"Learning Progress: <b>{rep['learning_progress_pct']}%</b>",
        f"Learning Age: <b>{rep['learning_age_days']}</b> / {rep['learning_days_required']} trading days",
        "",
        "<b>Signal Tracking</b>",
        f"Total Signals: <b>{stats['total_signals']}</b>",
        f"Open Signals: <b>{stats['open_signals']}</b>",
        f"Closed/Evaluated: <b>{stats['closed_signals']}</b>",
        f"Target Hits: <b>{stats['target_hits']}</b>",
        f"Stop Hits: <b>{stats['stop_hits']}</b>",
        f"Win Rate: <b>{stats['win_rate']}%</b>",
        "",
        "<b>Latest Scan</b>",
        f"Latest Scan Signals: <b>{scan.get('signals_count', 0)}</b>",
        f"Latest Ranked: <b>{scan.get('ranked_count', 0)}</b>",
        "",
    ]

    if scan.get("signals"):
        lines.append("<b>Top BUY Signals:</b>")
        for s in scan["signals"][:5]:
            lines.append(f"- {s.get('symbol')} | {s.get('type')} | {s.get('action')} | Score {s.get('score')} | T1 {s.get('target1')}")
    elif latest_ranked:
        lines.append("<b>Top Ranked Watches:</b>")
        for s in latest_ranked:
            lines.append(f"- {s.get('symbol')} | {s.get('type')} | {s.get('display_action')} | Score {s.get('score')} | Rank {s.get('rank_score')}")
    else:
        lines.append("No strong signals in latest scan.")

    lines.append("")
    lines.append("<b>System</b>")
    lines.append("Weekend excluded from learning calculation.")
    lines.append("")
    lines.append(DASHBOARD_URL)

    return "\n".join(lines)

def format_weekly_report():
    rep = readiness_report()
    stats = get_signal_stats()

    lines = [
        "<b>UAE AI Weekly Report</b>",
        f"Week ending UAE: <b>{uae_now_dt().strftime('%Y-%m-%d')}</b>",
        "",
        f"Mode: <b>{rep['mode']}</b>",
        f"Status: <b>{rep['status']}</b>",
        f"Learning Progress: <b>{rep['learning_progress_pct']}%</b>",
        "",
        "<b>Weekly / System Summary</b>",
        f"Total Signals: <b>{stats['total_signals']}</b>",
        f"Open Signals: <b>{stats['open_signals']}</b>",
        f"Closed/Evaluated: <b>{stats['closed_signals']}</b>",
        f"Target Hits: <b>{stats['target_hits']}</b>",
        f"Stop Hits: <b>{stats['stop_hits']}</b>",
        f"Win Rate: <b>{stats['win_rate']}%</b>",
        "",
        "Weekend excluded from learning calculation.",
        DASHBOARD_URL
    ]

    return "\n".join(lines)

@app.get("/api/reports/daily")
def daily_report(secret: Optional[str] = None, send: bool = False):
    if secret is not None and not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    text = format_daily_report()
    sent_result = None

    if send:
        sent_result = tg_main_send(text)

    return {"ok": True, "sent": send, "telegram": sent_result, "report": text}

@app.get("/api/reports/weekly")
def weekly_report(secret: Optional[str] = None, send: bool = False):
    if secret is not None and not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    text = format_weekly_report()
    sent_result = None

    if send:
        sent_result = tg_main_send(text)

    return {"ok": True, "sent": send, "telegram": sent_result, "report": text}

@app.get("/api/cron/daily-report")
def cron_daily_report(secret: Optional[str] = None):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}
    if not is_uae_trading_day():
        return {"ok": True, "skipped": True, "reason": "UAE weekend - no daily report"}
    text = format_daily_report()
    sent = tg_main_send(text)
    return {"ok": True, "sent": True, "telegram": sent}

@app.get("/api/cron/weekly-report")
def cron_weekly_report(secret: Optional[str] = None):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}
    text = format_weekly_report()
    sent = tg_main_send(text)
    return {"ok": True, "sent": True, "telegram": sent}

@app.get("/api/telegram/send-summary")
def telegram_send_summary(secret: Optional[str] = None):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}
    text = format_daily_report()
    sent = tg_main_send(text)
    return {"ok": True, "sent": True, "telegram": sent}

@app.get("/api/signals/open")
def get_open_signals():
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT symbol, signal_type, timeframe, action, price, target1, stop_loss,
                   score, strength, rr, risk_pct, created_at, status
            FROM ai_virtual_signals
            WHERE status='OPEN'
            ORDER BY id DESC
            LIMIT 50
        """)

        rows = cur.fetchall()
        conn.close()
        return {"ok": True, "count": len(rows), "signals": rows}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.get("/api/signals/completed")
def get_completed_signals():
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT symbol, signal_type, timeframe, action, price, target1, stop_loss,
                   score, strength, rr, outcome, outcome_at, max_high, min_low, created_at, status
            FROM ai_virtual_signals
            WHERE status='CLOSED'
            ORDER BY id DESC
            LIMIT 50
        """)

        rows = cur.fetchall()
        conn.close()
        return {"ok": True, "count": len(rows), "signals": rows}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


# ============================================================
# TELEGRAM
# ============================================================

def tg_api(method, payload):
    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN missing"}
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
            json=payload,
            timeout=30
        )
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status_code": r.status_code, "text": r.text[:1000]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def tg_send(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": str(text)[:4000],
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }

    if reply_markup:
        payload["reply_markup"] = reply_markup

    return tg_api("sendMessage", payload)

def tg_main_send(text, reply_markup=None):
    if not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_CHAT_ID missing"}
    return tg_send(TELEGRAM_CHAT_ID, text, reply_markup)

def signal_keyboard(symbol):
    return {
        "inline_keyboard": [
            [
                {"text": "More analysis", "callback_data": f"more:{symbol}"},
                {"text": "Ignore", "callback_data": f"ignore:{symbol}"}
            ]
        ]
    }

def format_signal(sig):
    size = sig.get("position_sizing") or {}
    entry_zone = sig.get("entry_zone") or ["-", "-"]

    return f"""
<b>{sig.get('symbol', 'UNKNOWN')} PRO AI V14</b>

Type: {sig.get('type', '-')}
Mode: {sig.get('mode', '-')}
Action: {sig.get('action', '-')}
Model: {sig.get('model_action', '-')}
Strength: {sig.get('strength', '-')}
Score: {sig.get('score', '-')}

Price: <b>{sig.get('price', '-')}</b>
Entry: <b>{entry_zone[0]} - {entry_zone[1]}</b>
Stop: <b>{sig.get('stop_loss', '-')}</b>

Target 1: <b>{sig.get('target1', '-')}</b>
Target 2: <b>{sig.get('target2', '-')}</b>
Target 3: <b>{sig.get('target3', '-')}</b>

Expected: <b>{sig.get('expected_move_pct', '-')}%</b>
Risk: <b>{sig.get('risk_pct', '-')}%</b>
RR: <b>{sig.get('rr', '-')}</b>

Position Size:
Qty: {size.get('qty', '-')}
Value: {size.get('position_value', '-')} AED
Risk: {size.get('max_risk_aed', '-')} AED

RSI: {sig.get('rsi', '-')}
Volume Ratio: {sig.get('volume_ratio', '-')}
Trend: {sig.get('trend', '-')}

{esc(sig.get('reason', '-'))}

Dashboard:
{DASHBOARD_URL}
""".strip()

def format_scan_summary(scan, title):
    lines = [
        f"<b>{title}</b>",
        f"Mode: <b>{scan.get('mode', get_ai_mode())}</b>",
        f"Signals: <b>{scan.get('signals_count', 0)}</b>",
        ""
    ]

    if scan.get("signals"):
        for s in scan["signals"][:TELEGRAM_TOP_N]:
            entry = s.get("entry_zone") or ["-", "-"]
            lines.append(
                f"- <b>{s.get('symbol')}</b> {s.get('type')} | "
                f"Score {s.get('score')} | Entry {entry[0]}-{entry[1]} | T1 {s.get('target1')}"
            )
    else:
        lines.append("No strong signals. Market is weak or setup not ready.")

    lines.append("")
    lines.append(DASHBOARD_URL)
    return "\n".join(lines)

def format_batch_summary(signals, start, end):
    lines = [
        "<b>UAE AI Batch Scan Alert</b>",
        f"Batch: {start} - {end}",
        f"Signals: <b>{len(signals)}</b>",
        ""
    ]

    for s in signals[:TELEGRAM_TOP_N]:
        lines.append(
            f"- <b>{s.get('symbol')}</b> {s.get('type')} | "
            f"{s.get('action')} | Score {s.get('score')} | T1 {s.get('target1')}"
        )

    lines.append("")
    lines.append(DASHBOARD_URL)
    return "\n".join(lines)

@app.get("/api/ai/test-telegram")
def test_telegram():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"}

    text = "â UAE AI System Telegram Test Successful"
    result = tg_main_send(text)
    return {"ok": bool(result.get("ok")), "telegram_response": result}

@app.get("/api/ai/send-alerts")
def send_alerts(secret: Optional[str] = None, force: bool = False, dry_run: bool = False):
    if secret is not None and not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    try:
        scan = latest_scan_result("COMBINED")
        if not scan:
            return {"ok": False, "message": "No saved COMBINED scan yet."}

        signals = scan.get("signals", [])
        if not signals:
            return {
                "ok": True,
                "mode": get_ai_mode(),
                "dry_run": dry_run,
                "sent_count": 0,
                "skipped_count": 0,
                "errors_count": 0,
                "message": "No signals to send.",
                "signals_count": 0
            }

        sent = []
        skipped = []
        errors = []

        conn = db()
        cur = conn.cursor()

        for sig in signals[:5]:
            try:
                symbol = sig.get("symbol", "UNKNOWN")
                sig_type = sig.get("type", "UNKNOWN")
                price = sig.get("price", "NA")
                target1 = sig.get("target1", "NA")
                mode = sig.get("mode", get_ai_mode())
                key = f"{symbol}-{sig_type}-{price}-{target1}-{mode}"

                if not force:
                    cur.execute("SELECT id FROM ai_alerts_log WHERE alert_key=%s", (key,))
                    if cur.fetchone():
                        skipped.append({"symbol": symbol, "type": sig_type, "reason": "duplicate_alert"})
                        continue

                tg_result = None
                if not dry_run:
                    tg_result = tg_main_send(format_signal(sig), signal_keyboard(symbol))

                cur.execute("""
                    INSERT INTO ai_alerts_log(alert_key,symbol,signal_type,created_at,payload)
                    VALUES(%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (key, symbol, sig_type, utc_now(), json.dumps(sig)))

                sent.append({"symbol": symbol, "type": sig_type, "telegram": tg_result})
            except Exception as e:
                errors.append({"symbol": sig.get("symbol", "UNKNOWN"), "error": str(e)})

        conn.commit()
        conn.close()

        return {
            "ok": True,
            "mode": get_ai_mode(),
            "dry_run": dry_run,
            "sent_count": len(sent),
            "skipped_count": len(skipped),
            "errors_count": len(errors),
            "sent": sent,
            "skipped": skipped,
            "errors": errors
        }

    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-2000:]}

@app.get("/api/ai/reset-alerts")
def reset_alerts():
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM ai_alerts_log")
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/api/ai/send-readiness")
def send_readiness(secret: Optional[str] = None):
    if secret is not None and not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    rep = readiness_report()
    return tg_main_send(
        f"<b>AI Readiness Report</b>\n"
        f"Mode: {rep['mode']}\n"
        f"Status: {rep['status']}\n"
        f"Learning: {rep['learning_progress_pct']}%\n"
        f"Total Signals: {rep['total_signals']}\n"
        f"Open: {rep['open_signals']}\n"
        f"Evaluated: {rep['evaluated_signals']}\n"
        f"Win Rate: {rep['win_rate']}%\n"
        f"Avg Return: {rep['avg_return_pct']}%"
    )

@app.get("/api/telegram/set-webhook")
def set_webhook():
    webhook_url = f"{BASE_URL}/api/telegram/webhook/{TELEGRAM_WEBHOOK_SECRET}"
    return tg_api("setWebhook", {"url": webhook_url})

@app.post("/api/telegram/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != TELEGRAM_WEBHOOK_SECRET:
        return {"ok": False, "error": "unauthorized"}

    data = await request.json()

    try:
        if "message" in data:
            msg = data["message"]
            chat_id = msg["chat"]["id"]
            text = msg.get("text", "").strip()
            upper = text.upper()

            if upper in ["READINESS", "Ø¬Ø§ÙØ²ÙØ©"]:
                return tg_send(chat_id, str(readiness_report()))

            if upper in ["DAILY", "ØªÙØ±ÙØ±", "ØªÙØ±ÙØ± ÙÙÙÙ"]:
                return tg_send(chat_id, format_daily_report())

            if upper in ["WEEKLY", "ØªÙØ±ÙØ± Ø£Ø³Ø¨ÙØ¹Ù", "Ø§Ø³Ø¨ÙØ¹Ù"]:
                return tg_send(chat_id, format_weekly_report())

            if upper.startswith("Ø­ÙÙ") or upper.startswith("ØªØ­ÙÙÙ") or upper.startswith("ANALYZE"):
                parts = upper.split()
                if len(parts) >= 2:
                    sigs = analyze_symbol(parts[1], "ALL")
                    if sigs:
                        best = max(sigs, key=lambda x: x["score"])
                        return tg_send(chat_id, format_signal(best), signal_keyboard(best["symbol"]))
                    return tg_send(chat_id, "No enough data yet.")

            if upper in WATCHLIST:
                sigs = analyze_symbol(upper, "ALL")
                if sigs:
                    best = max(sigs, key=lambda x: x["score"])
                    return tg_send(chat_id, format_signal(best), signal_keyboard(best["symbol"]))
                return tg_send(chat_id, "No enough data yet for this symbol.")

            return tg_send(chat_id, "Send symbol like EMAAR or: ANALYZE EMAAR, READINESS, DAILY, WEEKLY")
    except Exception as e:
        print("Telegram error:", str(e))

    return {"ok": True}

@app.get("/api/cron/send-alerts")
def cron_send_alerts(secret: Optional[str] = None, force: bool = False):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}
    return send_alerts(force=force, dry_run=False)

@app.get("/api/cron/readiness-report")
def cron_readiness_report(secret: Optional[str] = None):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}
    return send_readiness()

@app.get("/api/cron/end-of-day")
def cron_end_of_day(secret: Optional[str] = None):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}
    if not is_uae_trading_day():
        return {"ok": True, "skipped": True, "reason": "UAE weekend - no end of day report"}

    try:
        scan = run_scan("COMBINED")
        save_scan_result("COMBINED", scan)

        created = []
        for sig in scan.get("signals", []):
            if record_virtual_signal(sig):
                created.append({"symbol": sig["symbol"], "type": sig["type"]})

        evaluated = evaluate_virtual_signals()
        observed = evaluate_observations()
        sent = tg_main_send(format_daily_report())

        return {
            "ok": True,
            "scan_signals": scan.get("signals_count", 0),
            "ranked_count": scan.get("ranked_count", 0),
            "created_virtual": len(created),
            "evaluated": len(evaluated),
            "observations_evaluated": len(observed),
            "telegram": sent,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-2000:]}


# ============================================================
# DASHBOARD
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    scan = latest_scan_result("COMBINED") or {"signals": [], "coverage": [], "ranked": [], "signals_count": 0, "mode": get_ai_mode(), "created_at": "No scan yet"}
    rep = readiness_report()

    signal_rows = ""
    for s in scan.get("ranked", scan.get("signals", [])):
        if not s.get("has_data", True):
            continue
        entry_zone = s.get("entry_zone") or ["-", "-"]
        signal_rows += f"""
        <tr>
            <td>{s.get('symbol')}</td><td>{s.get('type')}</td><td>{s.get('display_action', s.get('action'))}</td><td>{s.get('strength')}</td>
            <td>{s.get('rank_score')}</td><td>{s.get('score')}</td><td>{s.get('price')}</td><td>{entry_zone[0]} - {entry_zone[1]}</td>
            <td>{s.get('stop_loss')}</td><td>{s.get('target1')} / {s.get('target2')}</td><td>{s.get('rr')}</td><td>{s.get('ai_comment')}</td>
        </tr>
        """

    coverage_rows = ""
    for x in scan.get("coverage", []):
        coverage_rows += f"""
        <tr>
            <td>{x.get('symbol')}</td><td>{'YES' if x.get('has_data') else 'NO'}</td>
            <td>{x.get('action')}</td><td>{x.get('model_action')}</td><td>{x.get('rank_score')}</td><td>{x.get('score')}</td>
            <td>{x.get('strength')}</td><td>{x.get('h1_count','-')}</td><td>{x.get('d1_count','-')}</td><td>{x.get('ai_comment')}</td>
        </tr>
        """

    return f"""
    <html>
    <head>
        <title>UAE PRO AI V14 Full Stable</title>
        <style>
            body {{ font-family: Arial; background:#111827; color:#e5e7eb; padding:20px; }}
            .card {{ background:#1f2937; padding:16px; border-radius:12px; margin-bottom:18px; }}
            table {{ width:100%; border-collapse:collapse; margin-bottom:25px; }}
            th,td {{ border-bottom:1px solid #374151; padding:10px; text-align:left; white-space:nowrap; }}
            h1,h2 {{ color:#a78bfa; }}
        </style>
    </head>
    <body>
        <h1>UAE PRO AI V14 Full Stable</h1>
        <div class="card">
            Mode: <b>{scan.get('mode')}</b><br>
            Readiness: <b>{rep['status']}</b><br>
            Learning Age: {rep['learning_age_days']} trading days / {rep['learning_days_required']} trading days<br>
            Learning Progress: {rep.get('learning_progress_pct')}%<br>
            Weekend Excluded: YES<br>
            Signals: {scan.get('signals_count', 0)}<br>
            Ranked: {scan.get('ranked_count', 0)}<br>
            Win Rate: {rep['win_rate']}% | Avg Return: {rep['avg_return_pct']}%<br>
            Last Scan: {scan.get('created_at', 'No scan yet')}
        </div>

        <h2>Top Ranked / Signals</h2>
        <table>
            <tr><th>Symbol</th><th>Type</th><th>Action</th><th>Strength</th><th>Rank</th><th>Score</th><th>Price</th><th>Entry</th><th>Stop</th><th>Targets</th><th>RR</th><th>AI Comment</th></tr>
            {signal_rows}
        </table>

        <h2>Coverage</h2>
        <table>
            <tr><th>Symbol</th><th>Data</th><th>Action</th><th>Model</th><th>Rank</th><th>Score</th><th>Strength</th><th>1H Count</th><th>1D Count</th><th>AI Comment</th></tr>
            {coverage_rows}
        </table>
    </body>
    </html>
    """
