import os
import json
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse


app = FastAPI(title="UAE Market PRO AI V3 Complete")

DATABASE_URL = os.getenv("DATABASE_URL")
SECRET = os.getenv("SECRET", "abc123")
CRON_SECRET = os.getenv("CRON_SECRET", "cron123")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "tgsecret123")

BASE_URL = os.getenv("API_BASE", "https://uae-market-production.up.railway.app")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", f"{BASE_URL}/dashboard")

AI_MODE = os.getenv("AI_MODE", "LEARNING").upper().strip()
LEARNING_DAYS = int(os.getenv("LEARNING_DAYS", "14"))
CAPITAL = float(os.getenv("CAPITAL", "200000"))

# Hybrid Learning Alerts:
# Keep AI_MODE=LEARNING but send alerts only for the strongest setups.
HYBRID_STRONG_ALERTS = os.getenv("HYBRID_STRONG_ALERTS", "true").lower() == "true"
STRONG_ALERT_SCORE = float(os.getenv("STRONG_ALERT_SCORE", "75"))
STRONG_ALERT_MIN_RR = float(os.getenv("STRONG_ALERT_MIN_RR", "0.6"))

# V4 Ranking + Observation Learning
OBSERVATION_LEARNING = os.getenv("OBSERVATION_LEARNING", "true").lower() == "true"
OBSERVATION_TARGET_PCT = float(os.getenv("OBSERVATION_TARGET_PCT", "3.0"))
OBSERVATION_DROP_PCT = float(os.getenv("OBSERVATION_DROP_PCT", "2.0"))
TELEGRAM_TOP_N = int(os.getenv("TELEGRAM_TOP_N", "10"))

# V5 Live-trading guardrails
# LIVE trading is disabled by default. It only activates with AI_MODE=LIVE and LIVE_TRADING_ENABLED=true.
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "false").lower() == "true"
LIVE_REQUIRES_CONFIRMATION = os.getenv("LIVE_REQUIRES_CONFIRMATION", "true").lower() == "true"
BROKER_WEBHOOK_URL = os.getenv("BROKER_WEBHOOK_URL", "")
BROKER_API_KEY = os.getenv("BROKER_API_KEY", "")
MAX_LIVE_ORDER_AED = float(os.getenv("MAX_LIVE_ORDER_AED", "10000"))
MAX_DAILY_LIVE_ORDERS = int(os.getenv("MAX_DAILY_LIVE_ORDERS", "3"))

# V6 Risk + Auto Paper Tracking
AUTO_PAPER_TRACKING = os.getenv("AUTO_PAPER_TRACKING", "true").lower() == "true"
MIN_AUTO_PAPER_SCORE = float(os.getenv("MIN_AUTO_PAPER_SCORE", "75"))
MIN_AUTO_PAPER_RR = float(os.getenv("MIN_AUTO_PAPER_RR", "0.6"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "20.0"))
DAILY_REPORT_ENABLED = os.getenv("DAILY_REPORT_ENABLED", "true").lower() == "true"

# V7 Batch Scanner
BATCH_SCAN_ENABLED = os.getenv("BATCH_SCAN_ENABLED", "true").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "18"))

# Learning from losing trades
LOSS_LEARNING_ENABLED = os.getenv("LOSS_LEARNING_ENABLED", "true").lower() == "true"
LOSS_SCORE_PENALTY = float(os.getenv("LOSS_SCORE_PENALTY", "6"))
WIN_SCORE_REWARD = float(os.getenv("WIN_SCORE_REWARD", "3"))

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
# HELPERS
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)

def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(DATABASE_URL)

def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").upper().replace(" ", "").strip()

def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
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
        return s
    except Exception:
        return s

def parse_dt(value: str):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT NOT NULL
        )
    """)

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
        CREATE TABLE IF NOT EXISTS telegram_sessions (
            chat_id TEXT PRIMARY KEY,
            state TEXT,
            symbol TEXT,
            amount DOUBLE PRECISION,
            entry_price DOUBLE PRECISION,
            trade_id INTEGER,
            payload TEXT,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS telegram_trades (
            id SERIAL PRIMARY KEY,
            chat_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            status TEXT NOT NULL,
            entry_price DOUBLE PRECISION NOT NULL,
            amount DOUBLE PRECISION NOT NULL,
            qty DOUBLE PRECISION NOT NULL,
            stop_loss DOUBLE PRECISION,
            target1 DOUBLE PRECISION,
            target2 DOUBLE PRECISION,
            target3 DOUBLE PRECISION,
            signal_score DOUBLE PRECISION,
            signal_strength TEXT,
            signal_type TEXT,
            signal_payload TEXT,
            opened_at TEXT NOT NULL,
            exit_price DOUBLE PRECISION,
            closed_at TEXT,
            pnl DOUBLE PRECISION,
            pnl_pct DOUBLE PRECISION,
            close_note TEXT
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
    return os.getenv("AI_MODE", get_setting("ai_mode", AI_MODE) or "LEARNING").upper().strip()

def get_batch_cursor() -> int:
    try:
        return int(get_setting("batch_cursor", "0") or 0)
    except Exception:
        return 0

def set_batch_cursor(value: int):
    set_setting("batch_cursor", str(value))

def get_batch_watchlist():
    if not BATCH_SCAN_ENABLED:
        return WATCHLIST, 0, len(WATCHLIST), True
    total = len(WATCHLIST)
    if total == 0:
        return [], 0, 0, True
    start = get_batch_cursor() % total
    size = max(1, min(BATCH_SIZE, total))
    end = min(start + size, total)
    batch = WATCHLIST[start:end]
    completed_cycle = end >= total
    next_cursor = 0 if completed_cycle else end
    set_batch_cursor(next_cursor)
    return batch, start, end, completed_cycle

def is_hybrid_strong_signal(sig: Dict[str, Any]) -> bool:
    return (
        get_ai_mode() == "LEARNING"
        and HYBRID_STRONG_ALERTS
        and sig.get("strength") == "VERY STRONG"
        and float(sig.get("score") or 0) >= STRONG_ALERT_SCORE
        and float(sig.get("rr") or 0) >= STRONG_ALERT_MIN_RR
    )

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
    return "WATCH"

def learning_age_days():
    start = parse_dt(get_setting("learning_started_at"))
    if not start:
        return 0
    return max(0, (utc_now_dt() - start).total_seconds() / 86400)

def learning_remaining_days():
    return max(0, LEARNING_DAYS - learning_age_days())


# ============================================================
# BASIC ROUTES
# ============================================================

@app.get("/")
def home():
    return {
        "status": "UAE PRO AI V3 Complete Running",
        "mode": get_ai_mode(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
        "scan_policy": "hourly + daily only",
        "capital": CAPITAL,
        "hybrid_strong_alerts": HYBRID_STRONG_ALERTS,
        "strong_alert_score": STRONG_ALERT_SCORE,
        "strong_alert_min_rr": STRONG_ALERT_MIN_RR,
    }

@app.get("/api/health")
def health():
    return {"ok": True, "mode": get_ai_mode()}

@app.get("/api/watchlist")
def watchlist():
    return {"count": len(WATCHLIST), "stocks": WATCHLIST}

@app.get("/api/system/mode")
def api_mode():
    return {
        "mode": get_ai_mode(),
        "learning_days": LEARNING_DAYS,
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
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
    data = await request.json()

    if data.get("secret") != SECRET:
        return {"ok": False, "error": "bad_secret"}

    symbol = normalize_symbol(data.get("symbol"))
    tf = str(data.get("timeframe", "")).strip()

    if tf in ["1h", "1H", "60m", "H1"]:
        tf = "60"
    elif tf in ["D", "1d", "daily"]:
        tf = "1D"

    o = safe_float(data.get("open"))
    h = safe_float(data.get("high"))
    l = safe_float(data.get("low"))
    c = safe_float(data.get("close"))
    v = safe_float(data.get("volume"), 0)

    if not symbol or tf not in ["60", "1D"]:
        return {"ok": False, "error": "bad_symbol_or_timeframe", "symbol": symbol, "timeframe": tf}

    if None in [o, h, l, c]:
        return {"ok": False, "error": "bad_ohlc"}

    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO candles(symbol,exchange,timeframe,bar_time,open,high,low,close,volume,received_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        symbol,
        data.get("exchange"),
        tf,
        parse_bar_time(data.get("time")),
        o, h, l, c, v,
        utc_now()
    ))
    conn.commit()
    conn.close()

    return {"ok": True, "symbol": symbol, "timeframe": tf, "close": c}


# ============================================================
# CANDLES
# ============================================================

def get_candles(symbol: str, timeframe: str, limit: int = 220):
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
        params.append(timeframe)

    q += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return {"count": len(rows), "candles": rows}

@app.get("/api/watchlist/coverage")
def coverage():
    out = []
    for s in WATCHLIST:
        h1 = len(get_candles(s, "60", 5))
        d1 = len(get_candles(s, "1D", 5))
        out.append({"symbol": s, "has_1h": h1 > 0, "has_1d": d1 > 0, "ready": h1 > 0 and d1 > 0})
    return {"count": len(out), "coverage": out}


# ============================================================
# INDICATORS
# ============================================================

def sma(values, n):
    if len(values) < n:
        return None
    return sum(values[-n:]) / n

def ema(values, n):
    if len(values) < n:
        return None
    k = 2 / (n + 1)
    e = sum(values[:n]) / n
    for price in values[n:]:
        e = price * k + e * (1 - k)
    return e

def rsi(values, n=14):
    if len(values) < n + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
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
        h = float(candles[i]["high"])
        l = float(candles[i]["low"])
        pc = float(candles[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-n:]) / n

def support_resistance(candles, lookback=30):
    if not candles:
        return None, None
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    return min(float(x["low"]) for x in recent), max(float(x["high"]) for x in recent)

def recent_momentum(candles, lookback=8):
    if len(candles) < lookback + 1:
        return 0
    start = float(candles[-lookback]["close"])
    end = float(candles[-1]["close"])
    return ((end - start) / start) * 100 if start else 0


# ============================================================
# LEARNING
# ============================================================

def get_learning_adjustment(symbol: str):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT score_adjustment FROM ai_learning_stats WHERE symbol=%s", (normalize_symbol(symbol),))
    row = cur.fetchone()
    conn.close()
    return float(row["score_adjustment"] or 0) if row else 0

def update_learning(symbol: str, ret_pct: float, signal_type: str, is_virtual: bool):
    symbol = normalize_symbol(symbol)
    signal_type = str(signal_type or "").upper()

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

        total_events = trades + vs + vl
        old_avg = float(row["avg_return_pct"] or 0)
        avg_return = ((old_avg * max(total_events - 1, 0)) + ret_pct) / total_events if total_events else ret_pct

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
            vs = 1; vsw = 1 if ret_pct > 0 else 0; vsl = 1 if ret_pct <= 0 else 0
        elif is_virtual and signal_type == "LONG_SWING":
            vl = 1; vlw = 1 if ret_pct > 0 else 0; vll = 1 if ret_pct <= 0 else 0
        else:
            trades = 1; wins = 1 if ret_pct > 0 else 0; losses = 1 if ret_pct <= 0 else 0

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
# POSITION SIZING + STOPS
# ============================================================

def smart_stop(entry, entry_low, support, atr_value, kind):
    if kind == "SHORT_SWING":
        min_stop_pct = 0.018
        max_stop_pct = 0.045
        atr_mult = 2.0
    else:
        min_stop_pct = 0.045
        max_stop_pct = 0.11
        atr_mult = 2.5

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

    if risk_per_share <= 0:
        return {"qty": 0, "position_value": 0, "max_risk_aed": round(max_risk_aed, 2)}

    qty_by_risk = max_risk_aed / risk_per_share
    max_position_value = CAPITAL * max_position_pct
    qty_by_cap = max_position_value / entry

    qty = max(0, min(qty_by_risk, qty_by_cap))
    position_value = qty * entry

    return {
        "qty": round(qty, 2),
        "position_value": round(position_value, 2),
        "max_risk_aed": round(max_risk_aed, 2),
        "max_position_value": round(max_position_value, 2)
    }


# ============================================================
# ANALYSIS SHORT/LONG
# ============================================================

def daily_trend_score(d1):
    if len(d1) < 30:
        return "UNKNOWN", 0
    closes = [float(x["close"]) for x in d1]
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

def build_signal(symbol, kind, candles, d1):
    symbol = normalize_symbol(symbol)
    if len(candles) < 30:
        return None

    closes = [float(x["close"]) for x in candles]
    volumes = [float(x["volume"] or 0) for x in candles]
    price = closes[-1]

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
        t1 = entry * 1.03
        t2 = entry * 1.05
        t3 = entry * 1.055
        max_hold_days = 5
        timeframe = "60"
    else:
        target_pct = 7.0
        t1 = entry * 1.07
        t2 = entry * 1.12
        t3 = entry * 1.15
        max_hold_days = 25
        timeframe = "1D"

    risk_pct = ((entry - stop) / entry) * 100 if entry else 0
    rr = ((t1 - entry) / (entry - stop)) if entry > stop else 0

    strength = "VERY STRONG" if score >= 85 else "STRONG" if score >= 70 else "MEDIUM" if score >= 55 else "WEAK"
    model_action = "BUY" if score >= 65 and rr >= 0.6 and (risk_pct <= (5.5 if kind == "SHORT_SWING" else 12)) else "WATCH"

    mode = get_ai_mode()
    if mode == "LEARNING":
        action = "LEARN_SIGNAL" if model_action == "BUY" else "WATCH"
    elif mode == "PAPER":
        action = "PAPER_BUY" if model_action == "BUY" else "WATCH"
    else:
        action = "BUY" if model_action == "BUY" else "WATCH"

    sizing = position_sizing(entry, stop, kind)

    # Hybrid Learning: alert only the strongest setups while still learning.
    hybrid_alert = (
        mode == "LEARNING"
        and HYBRID_STRONG_ALERTS
        and strength == "VERY STRONG"
        and score >= STRONG_ALERT_SCORE
        and rr >= STRONG_ALERT_MIN_RR
    )
    if hybrid_alert:
        action = "STRONG_LEARNING_ALERT"

    result = {
        "symbol": symbol,
        "type": kind,
        "mode": mode,
        "action": action,
        "model_action": model_action,
        "hybrid_alert": hybrid_alert if 'hybrid_alert' in locals() else False,
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
        "rsi": round(r, 2) if r else None,
        "volume_ratio": round(volume_ratio, 2),
        "momentum_pct": round(momentum, 2),
        "max_hold_days": max_hold_days,
        "holding": "1 to 5 days" if kind == "SHORT_SWING" else "1 to 4 weeks",
        "position_sizing": sizing,
        "reason": " + ".join(reasons) if reasons else "No strong setup"
    }
    result["rank_score"] = ai_rank_score(result)
    result["ai_comment"] = ai_comment(result)
    result["display_action"] = classify_action(result)
    return result

def analyze_symbol(symbol: str, scan_type: str = "ALL"):
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


# ============================================================
# SCAN STORAGE
# ============================================================

def save_scan_result(scan_type: str, payload: Dict[str, Any]):
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
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT * FROM ai_scan_results
        WHERE scan_type=%s
        ORDER BY id DESC
        LIMIT 1
    """, (scan_type,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return json.loads(row["payload"])

def run_scan(scan_type: str):
    scan_watchlist, batch_start, batch_end, completed_cycle = get_batch_watchlist()

    current_ranked = []
    signals = []
    batch_coverage = []

    for s in scan_watchlist:
        try:
            sigs = analyze_symbol(s, scan_type)
            best = max(sigs, key=lambda x: x.get("rank_score", 0), default=None)

            if best:
                current_ranked.append(best)
                if best.get("model_action") == "BUY" or is_hybrid_strong_signal(best):
                    signals.append(best)

            batch_coverage.append({
                "symbol": s,
                "has_data": bool(best),
                "action": classify_action(best) if best else "NO_DATA",
                "model_action": best.get("model_action") if best else "NO_DATA",
                "score": best.get("score") if best else None,
                "rank_score": best.get("rank_score") if best else None,
                "strength": best.get("strength") if best else None,
                "price": best.get("price") if best else None,
                "rr": best.get("rr") if best else None,
                "volume_ratio": best.get("volume_ratio") if best else None,
                "ai_comment": ai_comment(best) if best else "No data yet",
                "last_batch_update": utc_now()
            })
        except Exception as e:
            batch_coverage.append({
                "symbol": s, "has_data": False, "action": "ERROR", "model_action": "ERROR",
                "score": None, "rank_score": None, "strength": str(e), "price": None,
                "rr": None, "volume_ratio": None, "ai_comment": str(e),
                "last_batch_update": utc_now()
            })

    prev = latest_scan_result("COMBINED") or {}
    prev_coverage = prev.get("coverage", []) or []
    merged_by_symbol = {x.get("symbol"): x for x in prev_coverage if x.get("symbol")}
    for x in batch_coverage:
        merged_by_symbol[x.get("symbol")] = x

    coverage = []
    for sym in WATCHLIST:
        if sym in merged_by_symbol:
            coverage.append(merged_by_symbol[sym])
        else:
            coverage.append({
                "symbol": sym,
                "has_data": False,
                "action": "PENDING",
                "model_action": "PENDING",
                "score": None,
                "rank_score": None,
                "strength": None,
                "price": None,
                "rr": None,
                "volume_ratio": None,
                "ai_comment": "Waiting for batch scan"
            })

    ranked = sorted(
        [x for x in coverage if x.get("has_data")],
        key=lambda x: x.get("rank_score") or x.get("score") or 0,
        reverse=True
    )

    signals = sorted(signals, key=lambda x: x.get("rank_score", 0), reverse=True)

    payload = {
        "mode": get_ai_mode(),
        "scan_type": scan_type,
        "created_at": utc_now(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
        "batch_enabled": BATCH_SCAN_ENABLED,
        "batch_size": BATCH_SIZE,
        "batch_start": batch_start,
        "batch_end": batch_end,
        "batch_symbols": scan_watchlist,
        "completed_cycle": completed_cycle,
        "scanned_count": len(scan_watchlist),
        "total_covered_count": len([x for x in coverage if x.get("has_data")]),
        "signals_count": len(signals),
        "signals": signals[:20],
        "ranked_count": len(ranked),
        "ranked": ranked,
        "coverage": sorted(coverage, key=lambda x: (x.get("rank_score") or -1), reverse=True)
    }

    if OBSERVATION_LEARNING:
        record_observations(payload)

    auto_paper_created = 0
    for sig in signals[:TELEGRAM_TOP_N]:
        if auto_track_paper_trade(sig):
            auto_paper_created += 1
    payload["auto_paper_created"] = auto_paper_created

    return payload

@app.get("/api/ai/pro-scan")
def pro_scan(scan_type: str = "COMBINED"):
    data = latest_scan_result(scan_type.upper())
    if not data:
        return {
            "mode": get_ai_mode(),
            "message": "No saved scan yet. Run hourly/daily cron first.",
            "signals": [],
            "coverage": []
        }
    return data

@app.get("/api/ai/analyze")
def api_analyze(symbol: str):
    sigs = analyze_symbol(symbol, "ALL")
    return {"ok": bool(sigs), "symbol": normalize_symbol(symbol), "signals": sigs}


# ============================================================
# VIRTUAL LEARNING
# ============================================================

def sig_key(sig):
    return f"{sig['symbol']}-{sig['type']}-{sig['price']}-{sig['target1']}-{sig['stop_loss']}"

def record_virtual_signal(sig):
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
            key,sig["mode"],sig["symbol"],sig["type"],sig["timeframe"],sig["action"],
            sig["price"],sig["entry_zone"][0],sig["entry_zone"][1],sig["stop_loss"],
            sig["target1"],sig["target2"],sig["target3"],sig["score"],sig["strength"],
            sig["rr"],sig["risk_pct"],sig["target_pct"],sig["max_hold_days"],utc_now(),json.dumps(sig)
        ))
        conn.commit()
        ok = True
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        ok = False
    finally:
        conn.close()
    return ok

def evaluate_virtual_signals():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_virtual_signals WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
    rows = cur.fetchall()
    evaluated = []

    for sig in rows:
        symbol = sig["symbol"]
        tf = sig["timeframe"]
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

        status = "OPEN"
        outcome = None
        ret_pct = ((latest_close - price) / price) * 100

        if target_hit and not stop_hit:
            status = "CLOSED"; outcome = "TARGET1_HIT"; ret_pct = ((target1 - price) / price) * 100
        elif stop_hit and not target_hit:
            status = "CLOSED"; outcome = "STOP_HIT"; ret_pct = ((stop - price) / price) * 100
        elif target_hit and stop_hit:
            status = "CLOSED"; outcome = "BOTH_TOUCHED_CONSERVATIVE_STOP"; ret_pct = ((stop - price) / price) * 100
        elif created and (utc_now_dt() - created) > timedelta(days=max_hold_days):
            status = "CLOSED"; outcome = "TIME_EXIT"; ret_pct = ((latest_close - price) / price) * 100

        cur.execute("""
            UPDATE ai_virtual_signals
            SET max_high=%s,min_low=%s,bars_checked=%s,status=%s,outcome=%s,outcome_at=%s
            WHERE id=%s
        """, (max_high,min_low,len(relevant),status,outcome,utc_now() if status=="CLOSED" else None,sig["id"]))

        if status == "CLOSED":
            update_learning(symbol, ret_pct, sig["signal_type"], is_virtual=True)
            evaluated.append({"id": sig["id"], "symbol": symbol, "type": sig["signal_type"], "outcome": outcome, "return_pct": round(ret_pct, 2)})

    conn.commit()
    conn.close()
    return evaluated

@app.get("/api/ai/learning-scan")
def learning_scan():
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
    paper_evaluated = evaluate_open_paper_trades()
    return {"mode": get_ai_mode(), "created_count": len(created), "created": created, "evaluated_count": len(evaluated), "evaluated": evaluated}



# ============================================================
# OBSERVATION LEARNING
# ============================================================

def record_observations(scan_payload: Dict[str, Any]):
    conn = db()
    cur = conn.cursor()
    created = 0
    for item in scan_payload.get("ranked", []):
        key = f"{item.get('symbol')}-{scan_payload.get('scan_type')}-{item.get('timeframe')}-{item.get('price')}-{scan_payload.get('created_at')[:13]}"
        try:
            cur.execute("""
                INSERT INTO ai_observations
                (obs_key,symbol,scan_type,timeframe,action,model_action,strength,score,rank_score,price,observed_at,status,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)
                ON CONFLICT DO NOTHING
            """, (
                key, item.get("symbol"), scan_payload.get("scan_type"), item.get("timeframe"),
                item.get("display_action") or item.get("action"), item.get("model_action"),
                item.get("strength"), item.get("score"), item.get("rank_score"), item.get("price"),
                scan_payload.get("created_at"), json.dumps(item)
            ))
            created += cur.rowcount
        except Exception:
            conn.rollback()
    conn.commit()
    conn.close()
    return created

def evaluate_observations():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_observations WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
    rows = cur.fetchall()
    out = []

    for obs in rows:
        tf = obs["timeframe"] or "60"
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

        status = "OPEN"
        outcome = None
        max_days = 5 if tf == "60" else 25

        if target_hit:
            status = "CLOSED"; outcome = "WATCH_RALLIED"
        elif drop_hit:
            status = "CLOSED"; outcome = "WATCH_DROPPED"
        elif observed and (utc_now_dt() - observed) > timedelta(days=max_days):
            status = "CLOSED"; outcome = "WATCH_TIME_EXIT"

        cur.execute("""
            UPDATE ai_observations
            SET max_high=%s,min_low=%s,return_pct=%s,status=%s,outcome=%s,outcome_at=%s
            WHERE id=%s
        """, (max_high, min_low, return_pct, status, outcome, utc_now() if status=="CLOSED" else None, obs["id"]))

        if status == "CLOSED":
            update_learning(symbol, return_pct, "OBSERVATION", is_virtual=True)
            out.append({"symbol": symbol, "outcome": outcome, "return_pct": round(return_pct, 2)})

    conn.commit()
    conn.close()
    return out

@app.get("/api/ai/observations")
def api_observations(limit: int = 100):
    evaluated = evaluate_observations()
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM ai_observations ORDER BY id DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    conn.close()
    return {"evaluated_now": evaluated, "count": len(rows), "observations": rows}


@app.get("/api/v7/batch-status")
def v7_batch_status():
    cursor = get_batch_cursor()
    return {
        "batch_enabled": BATCH_SCAN_ENABLED,
        "batch_size": BATCH_SIZE,
        "batch_cursor": cursor,
        "watchlist_count": len(WATCHLIST),
        "next_symbols": WATCHLIST[cursor:cursor+BATCH_SIZE]
    }

@app.get("/api/v7/reset-batch")
def v7_reset_batch():
    set_batch_cursor(0)
    return {"ok": True, "batch_cursor": 0}

# ============================================================
# CRON ENDPOINTS
# ============================================================

def cron_ok(secret: Optional[str]):
    return secret == CRON_SECRET

@app.get("/api/cron/hourly-scan")
def cron_hourly_scan(secret: Optional[str] = None, send: bool = True):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    scan = run_scan("HOURLY")
    save_scan_result("HOURLY", scan)

    created = []
    for sig in scan["signals"]:
        if record_virtual_signal(sig):
            created.append({"symbol": sig["symbol"], "type": sig["type"]})

    evaluated = evaluate_virtual_signals()
    observed_evaluated = evaluate_observations()
    paper_evaluated = evaluate_open_paper_trades()

    if send:
        send_alerts(force=True, top=TELEGRAM_TOP_N)

    save_combined_scan()

    return {"ok": True, "scan_type": "HOURLY", "signals_count": scan["signals_count"], "created_virtual": len(created), "evaluated": len(evaluated), "observations_evaluated": len(observed_evaluated), "paper_evaluated": len(paper_evaluated)}

@app.get("/api/cron/daily-scan")
def cron_daily_scan(secret: Optional[str] = None, send: bool = True):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_cron_secret"}

    scan = run_scan("DAILY")
    save_scan_result("DAILY", scan)

    created = []
    for sig in scan["signals"]:
        if record_virtual_signal(sig):
            created.append({"symbol": sig["symbol"], "type": sig["type"]})

    evaluated = evaluate_virtual_signals()

    if send:
        send_alerts(force=True, top=TELEGRAM_TOP_N)

    save_combined_scan()

    return {"ok": True, "scan_type": "DAILY", "signals_count": scan["signals_count"], "created_virtual": len(created), "evaluated": len(evaluated), "observations_evaluated": len(observed_evaluated), "paper_evaluated": len(paper_evaluated)}

def save_combined_scan():
    hourly = latest_scan_result("HOURLY") or {}
    daily = latest_scan_result("DAILY") or {}

    signals = []
    signals.extend(hourly.get("signals", []))
    signals.extend(daily.get("signals", []))
    signals = sorted(signals, key=lambda x: (x["score"], x["rr"]), reverse=True)[:20]

    coverage = []
    hcov = {x["symbol"]: x for x in hourly.get("coverage", [])}
    dcov = {x["symbol"]: x for x in daily.get("coverage", [])}
    for s in WATCHLIST:
        h = hcov.get(s)
        d = dcov.get(s)
        best = h if (h and (not d or (h.get("score") or 0) >= (d.get("score") or 0))) else d
        coverage.append(best or {"symbol": s, "has_data": False, "action": "NO_DATA", "model_action": "NO_DATA", "score": None, "strength": None})

    combined = {
        "mode": get_ai_mode(),
        "scan_type": "COMBINED",
        "created_at": utc_now(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
        "scanned_count": len(WATCHLIST),
        "signals_count": len(signals),
        "signals": signals,
        "coverage": coverage
    }
    save_scan_result("COMBINED", combined)


# ============================================================
# READINESS
# ============================================================

@app.get("/api/ai/readiness")
def readiness_report():
    evaluate_virtual_signals()

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

    cur.execute("""
        SELECT * FROM ai_learning_stats
        ORDER BY (virtual_short_wins + virtual_long_wins) DESC, avg_return_pct DESC
        LIMIT 10
    """)
    best = cur.fetchall()

    cur.execute("""
        SELECT avg_return_pct,
               (virtual_short_count + virtual_long_count + trades_count) AS cnt
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
        reasons.append(f"Learning period not completed. Remaining {round(LEARNING_DAYS-age,1)} days.")
    if evaluated < 20:
        reasons.append("Not enough evaluated signals. Need at least 20.")
    if win_rate < 55:
        reasons.append("Win rate below 55%.")
    if avg_return <= 0:
        reasons.append("Average return is not positive yet.")

    if age >= LEARNING_DAYS and evaluated >= 20 and win_rate >= 55 and avg_return > 0:
        status = "READY_FOR_PAPER"
        reasons.append("Ready for PAPER mode, not full live size yet.")

    return {
        "mode": get_ai_mode(),
        "status": status,
        "learning_age_days": round(age, 2),
        "learning_days_required": LEARNING_DAYS,
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




# ============================================================
# V6 RISK ENGINE + AUTO PAPER TRACKING
# ============================================================

def calc_v6_risk_plan(sig: Dict[str, Any]) -> Dict[str, Any]:
    price = safe_float(sig.get("price"), 0) or 0
    stop = safe_float(sig.get("stop_loss"), 0) or 0
    target1 = safe_float(sig.get("target1"), 0) or 0

    if price <= 0 or stop <= 0 or stop >= price:
        return {
            "ok": False,
            "reason": "Invalid price or stop",
            "amount_aed": 0,
            "qty": 0,
            "risk_aed": 0,
            "reward_aed": 0,
        }

    max_risk_aed = CAPITAL * (RISK_PER_TRADE_PCT / 100)
    max_position_aed = CAPITAL * (MAX_POSITION_PCT / 100)

    risk_per_share = price - stop
    qty_by_risk = max_risk_aed / risk_per_share
    qty_by_position = max_position_aed / price
    qty = max(0, min(qty_by_risk, qty_by_position))

    amount = qty * price
    risk_aed = qty * risk_per_share
    reward_aed = qty * (target1 - price) if target1 > price else 0

    return {
        "ok": True,
        "amount_aed": round(amount, 2),
        "qty": round(qty, 2),
        "risk_aed": round(risk_aed, 2),
        "reward_aed": round(reward_aed, 2),
        "risk_per_trade_pct": RISK_PER_TRADE_PCT,
        "max_position_pct": MAX_POSITION_PCT,
    }

def should_auto_track_paper(sig: Dict[str, Any]) -> bool:
    if not AUTO_PAPER_TRACKING:
        return False
    return (
        get_ai_mode() in ["PAPER", "LEARNING"]
        and (sig.get("model_action") == "BUY" or sig.get("display_action") in ["BUY", "STRONG WATCH"] or sig.get("action") in ["PAPER_BUY", "BUY", "STRONG_LEARNING_ALERT"])
        and float(sig.get("score") or 0) >= MIN_AUTO_PAPER_SCORE
        and float(sig.get("rr") or 0) >= MIN_AUTO_PAPER_RR
    )

def auto_track_paper_trade(sig: Dict[str, Any]) -> bool:
    if not should_auto_track_paper(sig):
        return False

    plan = calc_v6_risk_plan(sig)
    if not plan.get("ok"):
        return False

    symbol = sig.get("symbol")
    price = safe_float(sig.get("price"), 0)
    amount = plan["amount_aed"]
    qty = plan["qty"]

    conn = db()
    cur = conn.cursor()
    key_payload = json.dumps(sig, sort_keys=True)

    # avoid duplicate paper tracking for same symbol/type/price during same day
    today = datetime.now(timezone.utc).date().isoformat()
    cur.execute("""
        SELECT id FROM telegram_trades
        WHERE symbol=%s AND status='PAPER_OPEN' AND opened_at >= %s
        LIMIT 1
    """, (symbol, today))
    exists = cur.fetchone()
    if exists:
        conn.close()
        return False

    cur.execute("""
        INSERT INTO telegram_trades
        (chat_id,symbol,status,entry_price,amount,qty,stop_loss,target1,target2,target3,
         signal_score,signal_strength,signal_type,signal_payload,opened_at)
        VALUES(%s,%s,'PAPER_OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        TELEGRAM_CHAT_ID or "paper",
        symbol,
        price,
        amount,
        qty,
        sig.get("stop_loss"),
        sig.get("target1"),
        sig.get("target2"),
        sig.get("target3"),
        sig.get("score"),
        sig.get("strength"),
        sig.get("type"),
        key_payload,
        utc_now()
    ))
    conn.commit()
    conn.close()
    return True

def evaluate_open_paper_trades():
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM telegram_trades WHERE status='PAPER_OPEN' ORDER BY id ASC LIMIT 500")
    trades = cur.fetchall()
    evaluated = []

    for tr in trades:
        symbol = tr["symbol"]
        # use 60 first, fallback 1D
        candles = get_candles(symbol, "60", 200) or get_candles(symbol, "1D", 200)
        if not candles:
            continue

        entry = float(tr["entry_price"])
        stop = safe_float(tr.get("stop_loss"))
        target1 = safe_float(tr.get("target1"))
        latest = float(candles[-1]["close"])
        max_high = max(float(x["high"]) for x in candles[-48:])
        min_low = min(float(x["low"]) for x in candles[-48:])

        close_reason = None
        exit_price = None

        if target1 and max_high >= target1:
            close_reason = "TARGET1_HIT"
            exit_price = target1
        elif stop and min_low <= stop:
            close_reason = "STOP_HIT"
            exit_price = stop
        else:
            opened = parse_dt(tr["opened_at"])
            if opened and (utc_now_dt() - opened) > timedelta(days=5):
                close_reason = "TIME_EXIT"
                exit_price = latest

        if close_reason:
            qty = float(tr["qty"])
            pnl = (exit_price - entry) * qty
            pnl_pct = ((exit_price - entry) / entry) * 100 if entry else 0

            cur.execute("""
                UPDATE telegram_trades
                SET status='PAPER_CLOSED', exit_price=%s, closed_at=%s, pnl=%s, pnl_pct=%s, close_note=%s
                WHERE id=%s
            """, (exit_price, utc_now(), pnl, pnl_pct, close_reason, tr["id"]))

            apply_loss_learning(symbol, pnl_pct, tr.get("signal_type") or "PAPER")
            evaluated.append({
                "trade_id": tr["id"],
                "symbol": symbol,
                "reason": close_reason,
                "pnl": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 2),
            })

    conn.commit()
    conn.close()
    return evaluated

@app.get("/api/v6/evaluate-paper")
def api_evaluate_paper():
    evaluated = evaluate_open_paper_trades()
    return {"ok": True, "evaluated_count": len(evaluated), "evaluated": evaluated}

@app.get("/api/v6/report")
def api_v6_report(send: bool = False):
    evaluated = evaluate_open_paper_trades()

    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) c FROM telegram_trades WHERE status='PAPER_OPEN'")
    open_count = int(cur.fetchone()["c"])
    cur.execute("SELECT COUNT(*) c FROM telegram_trades WHERE status='PAPER_CLOSED'")
    closed_count = int(cur.fetchone()["c"])
    cur.execute("SELECT COALESCE(SUM(pnl),0) pnl, COALESCE(AVG(pnl_pct),0) avg_pct FROM telegram_trades WHERE status='PAPER_CLOSED'")
    row = cur.fetchone()
    conn.close()

    msg = (
        "📊 <b>UAE PRO AI V6 Report</b>\n\n"
        f"Mode: <b>{get_ai_mode()}</b>\n"
        f"Paper Open Trades: <b>{open_count}</b>\n"
        f"Paper Closed Trades: <b>{closed_count}</b>\n"
        f"Total Paper PnL: <b>{round(float(row['pnl']), 2)} AED</b>\n"
        f"Average Paper Return: <b>{round(float(row['avg_pct']), 2)}%</b>\n"
        f"Evaluated Now: <b>{len(evaluated)}</b>\n\n"
        f"Dashboard:\n{DASHBOARD_URL}"
    )

    if send:
        tg_main_send(msg)

    return {
        "mode": get_ai_mode(),
        "open_paper_trades": open_count,
        "closed_paper_trades": closed_count,
        "total_paper_pnl": round(float(row["pnl"]), 2),
        "avg_paper_return_pct": round(float(row["avg_pct"]), 2),
        "evaluated_now": evaluated,
    }

# ============================================================
# V5 LIVE ORDER ENGINE + LOSS LEARNING
# ============================================================

def count_live_orders_today() -> int:
    conn = db()
    cur = conn.cursor()
    today = datetime.now(timezone.utc).date().isoformat()
    cur.execute("""
        SELECT COUNT(*) FROM telegram_trades
        WHERE opened_at >= %s AND status IN ('LIVE_OPEN','LIVE_SENT','OPEN')
    """, (today,))
    n = cur.fetchone()[0]
    conn.close()
    return int(n or 0)

def send_broker_order(symbol: str, side: str, amount: float, price: float, stop_loss=None, target1=None, payload=None):
    """
    Generic broker webhook integration.
    You must connect this to your licensed broker/order-management API.
    """
    if not LIVE_TRADING_ENABLED:
        return {"ok": False, "reason": "LIVE_TRADING_ENABLED=false"}
    if get_ai_mode() != "LIVE":
        return {"ok": False, "reason": "AI_MODE is not LIVE"}
    if LIVE_REQUIRES_CONFIRMATION:
        return {"ok": False, "reason": "LIVE_REQUIRES_CONFIRMATION=true"}
    if not BROKER_WEBHOOK_URL:
        return {"ok": False, "reason": "BROKER_WEBHOOK_URL missing"}
    if amount > MAX_LIVE_ORDER_AED:
        return {"ok": False, "reason": "amount exceeds MAX_LIVE_ORDER_AED"}
    if count_live_orders_today() >= MAX_DAILY_LIVE_ORDERS:
        return {"ok": False, "reason": "daily live order limit reached"}

    order = {
        "symbol": normalize_symbol(symbol),
        "side": side.upper(),
        "amount_aed": amount,
        "price_hint": price,
        "stop_loss": stop_loss,
        "target1": target1,
        "source": "UAE_PRO_AI_V5",
        "created_at": utc_now(),
        "payload": payload or {},
    }

    headers = {"Content-Type": "application/json"}
    if BROKER_API_KEY:
        headers["Authorization"] = f"Bearer {BROKER_API_KEY}"

    try:
        r = requests.post(BROKER_WEBHOOK_URL, json=order, headers=headers, timeout=20)
        try:
            body = r.json()
        except Exception:
            body = {"text": r.text}
        return {"ok": r.ok, "status_code": r.status_code, "broker_response": body, "order": order}
    except Exception as e:
        return {"ok": False, "reason": str(e), "order": order}

def apply_loss_learning(symbol: str, pnl_pct: float, signal_type: str = "TRADE"):
    if not LOSS_LEARNING_ENABLED:
        return
    update_learning(symbol, pnl_pct, signal_type, is_virtual=False)

@app.get("/api/live/status")
def live_status():
    return {
        "mode": get_ai_mode(),
        "live_trading_enabled": LIVE_TRADING_ENABLED,
        "live_requires_confirmation": LIVE_REQUIRES_CONFIRMATION,
        "broker_webhook_configured": bool(BROKER_WEBHOOK_URL),
        "max_live_order_aed": MAX_LIVE_ORDER_AED,
        "max_daily_live_orders": MAX_DAILY_LIVE_ORDERS,
        "live_orders_today": count_live_orders_today()
    }

@app.post("/api/live/order")
async def live_order(request: Request):
    data = await request.json()
    symbol = normalize_symbol(data.get("symbol"))
    amount = safe_float(data.get("amount_aed"), 0)
    price = safe_float(data.get("price"), 0)
    side = data.get("side", "BUY")
    stop_loss = safe_float(data.get("stop_loss"))
    target1 = safe_float(data.get("target1"))

    if not symbol or amount <= 0 or price <= 0:
        return {"ok": False, "error": "symbol, amount_aed and price are required"}

    result = send_broker_order(symbol, side, amount, price, stop_loss, target1, data)
    return result

@app.get("/api/trades/close")
def close_trade(trade_id: int, exit_price: float, note: str = ""):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM telegram_trades WHERE id=%s", (trade_id,))
    trade = cur.fetchone()
    if not trade:
        conn.close()
        return {"ok": False, "error": "trade not found"}

    entry = float(trade["entry_price"])
    qty = float(trade["qty"])
    pnl = (exit_price - entry) * qty
    pnl_pct = ((exit_price - entry) / entry) * 100 if entry else 0

    cur.execute("""
        UPDATE telegram_trades
        SET status='CLOSED', exit_price=%s, closed_at=%s, pnl=%s, pnl_pct=%s, close_note=%s
        WHERE id=%s
    """, (exit_price, utc_now(), pnl, pnl_pct, note, trade_id))

    conn.commit()
    conn.close()

    apply_loss_learning(trade["symbol"], pnl_pct, trade.get("signal_type") or "TRADE")

    return {"ok": True, "trade_id": trade_id, "symbol": trade["symbol"], "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2)}

# ============================================================
# TELEGRAM
# ============================================================

def tg_api(method, payload):
    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN missing"}
    r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}", json=payload, timeout=20)
    try:
        return r.json()
    except Exception:
        return {"ok": r.ok, "text": r.text}

def tg_send(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": False}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_api("sendMessage", payload)

def tg_main_send(text, reply_markup=None):
    if not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_CHAT_ID missing"}
    return tg_send(TELEGRAM_CHAT_ID, text, reply_markup)

def signal_keyboard(symbol):
    buttons = [
        [{"text": "More Analysis", "callback_data": f"more:{symbol}"}, {"text": "Ignore", "callback_data": f"ignore:{symbol}"}],
        [{"text": "I Entered This Trade", "callback_data": f"entered:{symbol}"}]
    ]
    return {"inline_keyboard": buttons}

def format_signal(sig):
    if sig.get("hybrid_alert"):
        mode_note = "\n🔥 <b>STRONG LEARNING ALERT:</b> إشارة قوية جداً أثناء التعلم. ليست دخول إلزامي، لكنها تستحق المتابعة."
    elif sig["mode"] == "LEARNING":
        mode_note = "\n⚠️ <b>LEARNING MODE:</b> ليست توصية دخول فعلية. النظام يتعلم فقط."
    else:
        mode_note = ""
    size = sig.get("position_sizing", {})
    return f"""
📊 <b>{sig['symbol']} PRO AI V3</b>

<b>Type:</b> {sig['type']}
<b>Mode:</b> {sig['mode']}
<b>Action:</b> {sig['action']}
<b>Model:</b> {sig['model_action']}
<b>Strength:</b> {sig['strength']}
<b>Score:</b> {sig['score']}

💰 Price: <b>{sig['price']}</b>
📍 Entry: <b>{sig['entry_zone'][0]} - {sig['entry_zone'][1]}</b>
🛑 Stop: <b>{sig['stop_loss']}</b>

🎯 Target 1: <b>{sig['target1']}</b>
🎯 Target 2: <b>{sig['target2']}</b>
🎯 Target 3: <b>{sig['target3']}</b>

📈 Expected: <b>{sig['expected_move_pct']}%</b>
⚖️ Risk: <b>{sig['risk_pct']}%</b>
📐 RR: <b>{sig['rr']}</b>

Position Size:
Qty: {size.get('qty')}
Value: {size.get('position_value')} AED
Risk: {size.get('max_risk_aed')} AED

RSI: {sig['rsi']}
Volume Ratio: {sig['volume_ratio']}
Trend: {sig['trend']}

📌 {esc(sig['reason'])}
{mode_note}

Dashboard:
{DASHBOARD_URL}
""".strip()

def format_scan_summary(scan, title):
    lines = [f"🧠 <b>{title}</b>", f"Mode: <b>{scan['mode']}</b>", f"Signals: <b>{scan['signals_count']}</b>", ""]
    for s in scan["signals"][:TELEGRAM_TOP_N]:
        lines.append(f"• <b>{s['symbol']}</b> {s['type']} | Score {s['score']} | Entry {s['entry_zone'][0]}-{s['entry_zone'][1]} | T1 {s['target1']}")
    lines.append("")
    lines.append(DASHBOARD_URL)
    return "\n".join(lines)

def format_readiness(rep):
    return f"""
🧠 <b>AI Readiness Report</b>

Mode: <b>{rep['mode']}</b>
Status: <b>{rep['status']}</b>

Learning Age: {rep['learning_age_days']} days
Required: {rep['learning_days_required']} days

Total Signals: {rep['total_signals']}
Evaluated: {rep['evaluated_signals']}
Open: {rep['open_signals']}

Target Hits: {rep['target_hits']}
Stop Hits: {rep['stop_hits']}
Win Rate: {rep['win_rate']}%
Avg Return: {rep['avg_return_pct']}%

Reasons:
- {"; ".join(rep['reasons']) if rep['reasons'] else "No issues"}
""".strip()

@app.get("/api/ai/send-alerts")
def send_alerts(force: bool = False, dry_run: bool = False, top: int = None):
    """
    V5 English Telegram ranked alerts.
    Sends ranked opportunities from strongest to weakest.
    If no confirmed trade exists, it still sends the ranked watchlist and states market is WEAK.
    """
    if top is None:
        top = TELEGRAM_TOP_N

    scan = latest_scan_result("COMBINED")
    if not scan:
        msg = "📉 UAE PRO AI V5\nNo saved scan yet. Run hourly scan first."
        if not dry_run:
            tg_main_send(msg)
        return {"ok": False, "message": "No saved scan yet. Run hourly scan first."}

    ranked = scan.get("ranked", []) or []
    coverage = scan.get("coverage", []) or []

    items = ranked if ranked else coverage
    items = sorted(items, key=lambda x: (x.get("rank_score") or x.get("score") or 0), reverse=True)

    if not items:
        msg = "📉 <b>UAE PRO AI V5</b>\nNo opportunities now. Market status: <b>WEAK / NO DATA</b>"
        if not dry_run:
            tg_main_send(msg)
        return {"mode": get_ai_mode(), "sent_count": 1, "message": "WEAK", "items": []}

    lines = []
    lines.append("📊 <b>UAE PRO AI V5 - Ranked Opportunities</b>")
    lines.append(f"Mode: <b>{get_ai_mode()}</b>")
    lines.append(f"Scan: <b>{scan.get('scan_type', 'COMBINED')}</b>")
    if scan.get("batch_enabled"):
        lines.append(f"Batch: <b>{scan.get('batch_start')} - {scan.get('batch_end')}</b> / {scan.get('watchlist_count')} | Covered: <b>{scan.get('total_covered_count')}</b>")
    lines.append("")
    lines.append("Ranking from strongest to weakest:")
    lines.append("")

    sent_items = []
    real_opportunities = 0

    for i, x in enumerate(items[:top], start=1):
        symbol = x.get("symbol")
        action = x.get("display_action") or x.get("action") or "WATCH"
        model_action = x.get("model_action") or "WATCH"
        strength = x.get("strength")
        score = x.get("score")
        rank_score = x.get("rank_score") or score
        price = x.get("price")
        rr = x.get("rr")
        vol = x.get("volume_ratio")
        comment = x.get("ai_comment") or ""

        if model_action == "BUY" or action in ["BUY", "PAPER_BUY", "STRONG_LEARNING_ALERT"]:
            status = "🔥 TRADE CANDIDATE"
            real_opportunities += 1
        elif strength in ["VERY STRONG", "STRONG"]:
            status = "👀 STRONG WATCH"
        elif action in ["NO_DATA", "ERROR"]:
            status = "WEAK / NO DATA"
        else:
            status = "WEAK / WATCH"

        price_txt = price if price is not None else "-"
        rr_txt = rr if rr is not None else "-"
        vol_txt = vol if vol is not None else "-"
        risk_plan = calc_v6_risk_plan(x)
        risk_line = ""
        if risk_plan.get("ok"):
            risk_line = f"\nRisk Plan: Amount {risk_plan['amount_aed']} AED | Qty {risk_plan['qty']} | Risk {risk_plan['risk_aed']} AED"

        lines.append(
            f"{i}. <b>{symbol}</b> | {status}\n"
            f"Action: <b>{action}</b> | Model: {model_action}\n"
            f"Strength: {strength} | Score: {score} | Rank: {rank_score}\n"
            f"Price: {price_txt} | RR: {rr_txt} | Vol: {vol_txt}{risk_line}\n"
            f"{comment}\n"
        )

        sent_items.append({
            "rank": i,
            "symbol": symbol,
            "action": action,
            "model_action": model_action,
            "strength": strength,
            "score": score,
            "rank_score": rank_score,
            "price": price,
            "rr": rr,
        })

    if real_opportunities == 0:
        lines.append("📌 Summary: No confirmed trade setup now. Current market list is watch/weak.")
    else:
        lines.append(f"📌 Summary: {real_opportunities} trade candidate(s) for review.")

    lines.append("")
    if get_ai_mode() == "LIVE" and LIVE_TRADING_ENABLED:
        if LIVE_REQUIRES_CONFIRMATION:
            lines.append("⚠️ Live trading is enabled, but confirmation is required before any order.")
        else:
            lines.append("⚠️ Live auto-order mode is enabled. Check broker and risk settings.")
    else:
        lines.append("Mode note: No real broker order will be placed unless LIVE_TRADING_ENABLED=true and broker webhook is configured.")

    lines.append("")
    lines.append(f"Dashboard:\n{DASHBOARD_URL}")

    msg = "\n".join(lines)

    if not dry_run:
        tg_main_send(msg)

    return {
        "mode": get_ai_mode(),
        "dry_run": dry_run,
        "sent_count": 1,
        "items_count": len(sent_items),
        "real_opportunities": real_opportunities,
        "sent": sent_items
    }

@app.get("/api/ai/reset-alerts")
def reset_alerts():
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM ai_alerts_log")
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/api/ai/send-readiness")
def send_readiness():
    rep = readiness_report()
    return tg_main_send(format_readiness(rep))

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

            if upper in ["جاهزية", "READINESS"]:
                return tg_send(chat_id, format_readiness(readiness_report()))

            if upper.startswith("حلل"):
                parts = upper.split()
                if len(parts) >= 2:
                    sigs = analyze_symbol(parts[1], "ALL")
                    if sigs:
                        best = max(sigs, key=lambda x: x["score"])
                        return tg_send(chat_id, format_signal(best), signal_keyboard(best["symbol"]))
                    return tg_send(chat_id, "لا توجد بيانات كافية.")

            if upper.startswith("ENTERED"):
                parts = text.split()
                if len(parts) >= 4:
                    symbol = normalize_symbol(parts[1])
                    price = safe_float(parts[2], 0)
                    amount = safe_float(parts[3], 0)
                    if symbol and price > 0 and amount > 0:
                        qty = amount / price
                        conn = db()
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO telegram_trades
                            (chat_id,symbol,status,entry_price,amount,qty,opened_at,signal_type)
                            VALUES(%s,%s,'OPEN',%s,%s,%s,%s,'MANUAL')
                            RETURNING id
                        """, (str(chat_id), symbol, price, amount, qty, utc_now()))
                        trade_id = cur.fetchone()[0]
                        conn.commit()
                        conn.close()
                        return tg_send(chat_id, f"✅ Trade tracked: {symbol}\nTrade ID: {trade_id}\nEntry: {price}\nAmount: {amount}\nQty: {round(qty,2)}")
                return tg_send(chat_id, "Use: ENTERED SYMBOL PRICE AMOUNT\nExample: ENTERED EMAAR 11.22 40000")

            if upper.startswith("SOLD"):
                parts = text.split()
                if len(parts) >= 3:
                    trade_id = int(parts[1])
                    exit_price = float(parts[2])
                    result = close_trade(trade_id, exit_price, "Telegram close")
                    if result.get("ok"):
                        return tg_send(chat_id, f"✅ Trade closed\n{result['symbol']}\nPnL: {result['pnl']} AED\nPnL %: {result['pnl_pct']}%\nAI learning updated.")
                    return tg_send(chat_id, f"Could not close trade: {result}")
                return tg_send(chat_id, "Use: SOLD TRADE_ID EXIT_PRICE\nExample: SOLD 12 11.40")

            if upper in WATCHLIST:
                sigs = analyze_symbol(upper, "ALL")
                if sigs:
                    best = max(sigs, key=lambda x: x.get("rank_score", x["score"]))
                    return tg_send(chat_id, format_signal(best), signal_keyboard(best["symbol"]))
                return tg_send(chat_id, "Not enough data for this symbol.")

            return tg_send(chat_id, "Send a symbol like EMAAR, or:\nANALYZE EMAAR\nREADINESS\nENTERED EMAAR 11.22 40000\nSOLD 1 11.40")

        if "callback_query" in data:
            cq = data["callback_query"]
            callback_id = cq["id"]
            chat_id = cq["message"]["chat"]["id"]
            callback_data = cq["data"]
            tg_api("answerCallbackQuery", {"callback_query_id": callback_id})

            parts = callback_data.split(":")
            action = parts[0]

            if action == "more":
                sigs = analyze_symbol(parts[1], "ALL")
                if sigs:
                    best = max(sigs, key=lambda x: x["score"])
                    return tg_send(chat_id, format_signal(best), signal_keyboard(best["symbol"]))
                return tg_send(chat_id, "لا توجد بيانات كافية.")

            if action == "entered":
                symbol = parts[1]
                return tg_send(chat_id, f"Manual trade tracking started for {symbol}. Send: ENTERED {symbol} price amount\nExample: ENTERED EMAAR 11.22 40000")

            if action == "ignore":
                return tg_send(chat_id, "Ignored.")

    except Exception as e:
        print("Telegram error:", str(e))

    return {"ok": True}


# ============================================================
# DASHBOARD
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    scan = latest_scan_result("COMBINED") or {"signals": [], "coverage": [], "signals_count": 0, "mode": get_ai_mode(), "created_at": "No scan yet"}
    rep = readiness_report()

    signal_rows = ""
    for s in scan.get("ranked", scan.get("signals", [])):
        signal_rows += f"""
        <tr>
            <td>{s['symbol']}</td><td>{s['type']}</td><td>{s.get('display_action', s['action'])}</td><td>{s['strength']}</td>
            <td>{s.get('rank_score')}</td><td>{s['score']}</td><td>{s['price']}</td><td>{s['entry_zone'][0]} - {s['entry_zone'][1]}</td>
            <td>{s['stop_loss']}</td><td>{s['target1']} / {s['target2']}</td><td>{s['rr']}</td><td>{s.get('ai_comment')}</td>
        </tr>
        """

    coverage_rows = ""
    for x in scan["coverage"]:
        coverage_rows += f"""
        <tr>
            <td>{x['symbol']}</td><td>{'YES' if x['has_data'] else 'NO'}</td>
            <td>{x['action']}</td><td>{x['model_action']}</td><td>{x.get('rank_score')}</td><td>{x['score']}</td><td>{x['strength']}</td><td>{x.get('ai_comment')}</td>
        </tr>
        """

    return f"""
    <html>
    <head>
        <title>UAE PRO AI V3</title>
        <style>
            body {{ font-family: Arial; background:#111827; color:#e5e7eb; padding:20px; }}
            .card {{ background:#1f2937; padding:16px; border-radius:12px; margin-bottom:18px; }}
            table {{ width:100%; border-collapse:collapse; margin-bottom:25px; }}
            th,td {{ border-bottom:1px solid #374151; padding:10px; text-align:left; white-space:nowrap; }}
            h1,h2 {{ color:#a78bfa; }}
        </style>
    </head>
    <body>
        <h1>UAE PRO AI V3 - Complete Program</h1>
        <div class="card">
            Mode: <b>{scan.get('mode')}</b><br>
            Readiness: <b>{rep['status']}</b><br>
            Learning Age: {rep['learning_age_days']} days / {rep['learning_days_required']} days<br>
            Signals: {scan.get('signals_count', 0)}<br>
            Win Rate: {rep['win_rate']}% | Avg Return: {rep['avg_return_pct']}%<br>
            Last Scan: {scan.get('created_at', 'No scan yet')}
        </div>

        <h2>Top Signals</h2>
        <table>
            <tr><th>Symbol</th><th>Type</th><th>Action</th><th>Strength</th><th>Rank</th><th>Score</th><th>Price</th><th>Entry</th><th>Stop</th><th>Targets</th><th>RR</th><th>AI Comment</th></tr>
            {signal_rows}
        </table>

        <h2>Coverage</h2>
        <table>
            <tr><th>Symbol</th><th>Data</th><th>Action</th><th>Model</th><th>Rank</th><th>Score</th><th>Strength</th><th>AI Comment</th></tr>
            {coverage_rows}
        </table>
    </body>
    </html>
    """
