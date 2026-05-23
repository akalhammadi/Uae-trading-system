"""
UAE Market PRO AI — V20 INTELLIGENT CORE
=========================================
التغييرات الجوهرية عن V14:
1. كشف التجميع والتصريف (Wyckoff + OBV + CMF)
2. كشف عكس المسار (Divergence + Volume Climax + Exhaustion)
3. أهداف ديناميكية مبنية على ATR الفعلي
4. نظام ثبات القرار — القرار يُقفَل ولا يتغير إلا بمبرر قوي
5. تعلم متقدم بالـ Pattern لا مجرد Win/Loss
6. تقييم ذاتي شامل مع تقارير جاهزية تفصيلية
7. إصلاح connection leaks
8. Cron jobs محسّنة
"""

import os
import json
import math
import traceback
import requests
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from threading import Thread

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

# ============================================================
# APP
# ============================================================

app = FastAPI(title="UAE Market PRO AI V20 Intelligent Core")

# ============================================================
# ENV CONFIG
# ============================================================

DATABASE_URL             = os.getenv("DATABASE_URL")
SECRET                   = os.getenv("SECRET", "abc123")
CRON_SECRET              = os.getenv("CRON_SECRET", "cron123")

TELEGRAM_BOT_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID         = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_WEBHOOK_SECRET  = os.getenv("TELEGRAM_WEBHOOK_SECRET", "tgsecret123")

BASE_URL                 = os.getenv("API_BASE", "https://uae-market-production.up.railway.app").rstrip("/")
DASHBOARD_URL            = os.getenv("DASHBOARD_URL", f"{BASE_URL}/dashboard")

AI_MODE                  = os.getenv("AI_MODE", "PAPER").upper().strip()
LEARNING_DAYS            = int(os.getenv("LEARNING_DAYS", "21"))
CAPITAL                  = float(os.getenv("CAPITAL", "200000"))

TELEGRAM_TOP_N           = int(os.getenv("TELEGRAM_TOP_N", "5"))
MIN_H1_CANDLES           = int(os.getenv("MIN_H1_CANDLES", "30"))
MIN_D1_CANDLES           = int(os.getenv("MIN_D1_CANDLES", "10"))
SCAN_MAX_ERRORS          = int(os.getenv("SCAN_MAX_ERRORS", "100"))

# Decision stability
DECISION_MIN_HOLD_DAYS   = int(os.getenv("DECISION_MIN_HOLD_DAYS", "3"))
DECISION_CHANGE_DELTA    = float(os.getenv("DECISION_CHANGE_DELTA", "18"))

# Signal quality gates
MIN_SCORE_BUY            = float(os.getenv("MIN_SCORE_BUY", "72"))
MIN_RR_BUY               = float(os.getenv("MIN_RR_BUY", "1.2"))
MIN_CONFIDENCE_LIVE      = float(os.getenv("MIN_CONFIDENCE_LIVE", "70"))

# Observation targets
OBSERVATION_TARGET_PCT   = float(os.getenv("OBSERVATION_TARGET_PCT", "3.0"))
OBSERVATION_DROP_PCT     = float(os.getenv("OBSERVATION_DROP_PCT", "2.0"))

UAE_TZ_OFFSET            = timedelta(hours=4)

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
# BACKGROUND JOBS
# ============================================================

def run_background_job(job_func, *args, **kwargs):
    def wrapper():
        try:
            job_func(*args, **kwargs)
        except Exception:
            print("BACKGROUND JOB ERROR:")
            print(traceback.format_exc())
    t = Thread(target=wrapper, daemon=True)
    t.start()

# ============================================================
# TIME HELPERS
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
    if not start or not end or start > end:
        return 0
    count = 0
    cur = (start + UAE_TZ_OFFSET).date() if start.tzinfo else start.date()
    end_date = (end + UAE_TZ_OFFSET).date() if end.tzinfo else end.date()
    while cur <= end_date:
        if cur.weekday() in [0, 1, 2, 3, 4]:
            count += 1
        cur += timedelta(days=1)
    return count

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

# ============================================================
# UTILITY HELPERS
# ============================================================

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

def esc(x) -> str:
    return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def cron_ok(secret: Optional[str]) -> bool:
    return secret == CRON_SECRET

# ============================================================
# DATABASE — context manager prevents connection leaks
# ============================================================

@contextmanager
def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=10)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def db_query(sql: str, params=None, fetchall=False, fetchone=False, dict_cursor=False):
    """Helper for simple read queries"""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor if dict_cursor else None)
        cur.execute(sql, params or ())
        if fetchall:
            return list(cur.fetchall())
        if fetchone:
            return cur.fetchone()
        return None

# ============================================================
# DATABASE INIT — V20 SCHEMA
# ============================================================

def init_db():
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY, value TEXT, updated_at TEXT NOT NULL
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS candles (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL, exchange TEXT, timeframe TEXT NOT NULL,
            bar_time TEXT NOT NULL, open DOUBLE PRECISION, high DOUBLE PRECISION,
            low DOUBLE PRECISION, close DOUBLE PRECISION, volume DOUBLE PRECISION,
            received_at TEXT NOT NULL
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_id ON candles(symbol, timeframe, id DESC)")

        # V20: pattern-based learning per symbol
        cur.execute("""CREATE TABLE IF NOT EXISTS v20_pattern_learning (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            setup_type TEXT NOT NULL,        -- SHORT_SWING / LONG_SWING
            market_phase TEXT,               -- ACCUMULATION / MARKUP / DISTRIBUTION / MARKDOWN / NEUTRAL
            rsi_bucket TEXT,                 -- LOW<40 / HEALTHY 40-65 / EXTENDED 65-75 / OVERBOUGHT>75
            volume_bucket TEXT,              -- LOW<1.0 / NORMAL 1.0-1.4 / HIGH>1.4
            trend_alignment TEXT,            -- ALIGNED / MIXED / AGAINST
            outcome TEXT,                    -- WIN / LOSS / TIME_EXIT
            return_pct DOUBLE PRECISION,
            score_at_entry DOUBLE PRECISION,
            rr_at_entry DOUBLE PRECISION,
            created_at TEXT NOT NULL
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_v20_pattern_symbol ON v20_pattern_learning(symbol, setup_type)")

        # V20: decision lock table — one active thesis per symbol
        cur.execute("""CREATE TABLE IF NOT EXISTS v20_decision_lock (
            symbol TEXT PRIMARY KEY,
            decision TEXT NOT NULL,          -- BUY / WATCH / AVOID
            confidence DOUBLE PRECISION,
            score DOUBLE PRECISION,
            entry_price DOUBLE PRECISION,
            entry_low DOUBLE PRECISION,
            entry_high DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION,
            target1 DOUBLE PRECISION,
            target2 DOUBLE PRECISION,
            target3 DOUBLE PRECISION,
            estimated_days INTEGER,
            market_phase TEXT,
            signal_type TEXT,
            lock_reason TEXT,
            locked_at TEXT NOT NULL,
            last_checked_at TEXT,
            status TEXT DEFAULT 'LOCKED',    -- LOCKED / RELEASED / HIT_TARGET / HIT_STOP
            unlock_reason TEXT,
            payload TEXT
        )""")

        # V20: reversal alerts log
        cur.execute("""CREATE TABLE IF NOT EXISTS v20_reversal_alerts (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            alert_type TEXT NOT NULL,        -- BEARISH_DIVERGENCE / VOLUME_CLIMAX / EXHAUSTION / DISTRIBUTION_DETECTED
            severity TEXT,                   -- WARNING / STRONG / CRITICAL
            price DOUBLE PRECISION,
            details TEXT,
            created_at TEXT NOT NULL,
            sent_telegram BOOLEAN DEFAULT FALSE
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS ai_scan_results (
            id SERIAL PRIMARY KEY,
            scan_type TEXT NOT NULL, mode TEXT NOT NULL,
            created_at TEXT NOT NULL, watchlist_count INTEGER,
            scanned_count INTEGER, signals_count INTEGER, payload TEXT
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_scan_results_type_id ON ai_scan_results(scan_type, id DESC)")

        cur.execute("""CREATE TABLE IF NOT EXISTS ai_alerts_log (
            id SERIAL PRIMARY KEY, alert_key TEXT UNIQUE NOT NULL,
            symbol TEXT, signal_type TEXT, created_at TEXT NOT NULL, payload TEXT
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS ai_virtual_signals (
            id SERIAL PRIMARY KEY, signal_key TEXT UNIQUE NOT NULL,
            mode TEXT NOT NULL, symbol TEXT NOT NULL, signal_type TEXT,
            timeframe TEXT, action TEXT, price DOUBLE PRECISION,
            entry_low DOUBLE PRECISION, entry_high DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION, target1 DOUBLE PRECISION,
            target2 DOUBLE PRECISION, target3 DOUBLE PRECISION,
            score DOUBLE PRECISION, strength TEXT, rr DOUBLE PRECISION,
            risk_pct DOUBLE PRECISION, target_pct DOUBLE PRECISION,
            max_hold_days INTEGER, estimated_days INTEGER,
            market_phase TEXT,
            created_at TEXT NOT NULL, status TEXT NOT NULL,
            outcome TEXT, outcome_at TEXT,
            max_high DOUBLE PRECISION, min_low DOUBLE PRECISION,
            bars_checked INTEGER DEFAULT 0, payload TEXT
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS ai_observations (
            id SERIAL PRIMARY KEY, obs_key TEXT UNIQUE NOT NULL,
            symbol TEXT NOT NULL, scan_type TEXT, timeframe TEXT,
            action TEXT, model_action TEXT, strength TEXT, score DOUBLE PRECISION,
            rank_score DOUBLE PRECISION, price DOUBLE PRECISION,
            observed_at TEXT NOT NULL, status TEXT NOT NULL,
            outcome TEXT, outcome_at TEXT, max_high DOUBLE PRECISION,
            min_low DOUBLE PRECISION, return_pct DOUBLE PRECISION, payload TEXT
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS ai_learning_stats (
            symbol TEXT PRIMARY KEY,
            trades_count INTEGER DEFAULT 0, wins_count INTEGER DEFAULT 0,
            losses_count INTEGER DEFAULT 0,
            virtual_short_count INTEGER DEFAULT 0, virtual_short_wins INTEGER DEFAULT 0,
            virtual_short_losses INTEGER DEFAULT 0,
            virtual_long_count INTEGER DEFAULT 0, virtual_long_wins INTEGER DEFAULT 0,
            virtual_long_losses INTEGER DEFAULT 0,
            avg_return_pct DOUBLE PRECISION DEFAULT 0,
            score_adjustment DOUBLE PRECISION DEFAULT 0,
            pattern_win_rate DOUBLE PRECISION DEFAULT 0,
            updated_at TEXT NOT NULL
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS ai_failure_memory (
            id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, setup_type TEXT,
            failure_reason TEXT, loss_pct DOUBLE PRECISION,
            market_state TEXT, lesson TEXT, created_at TEXT NOT NULL, payload TEXT
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS ai_self_evaluation (
            id SERIAL PRIMARY KEY, created_at TEXT NOT NULL,
            system_state TEXT, confidence_score DOUBLE PRECISION,
            ready_for_trading BOOLEAN, win_rate DOUBLE PRECISION,
            avg_return_pct DOUBLE PRECISION, rr_quality DOUBLE PRECISION,
            lessons_count INTEGER, pattern_quality DOUBLE PRECISION,
            recommendation TEXT, payload TEXT
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS portfolio_positions (
            id SERIAL PRIMARY KEY, symbol TEXT UNIQUE NOT NULL,
            qty DOUBLE PRECISION NOT NULL, entry_price DOUBLE PRECISION NOT NULL,
            position_type TEXT DEFAULT 'HOLDING', status TEXT DEFAULT 'OPEN',
            notes TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS batch_scan_state (
            key TEXT PRIMARY KEY, next_index INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS v15_active_thesis (
            id SERIAL PRIMARY KEY, symbol TEXT UNIQUE NOT NULL,
            status TEXT DEFAULT 'ACTIVE', first_created_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL, trend_phase TEXT, decision TEXT,
            confidence TEXT, score DOUBLE PRECISION, entry_price DOUBLE PRECISION,
            ideal_entry DOUBLE PRECISION, aggressive_entry DOUBLE PRECISION,
            safe_entry DOUBLE PRECISION, invalidation DOUBLE PRECISION,
            target_base DOUBLE PRECISION, target_bull DOUBLE PRECISION,
            expected_move_pct DOUBLE PRECISION, expected_horizon TEXT,
            hold_days INTEGER DEFAULT 0, min_hold_days INTEGER DEFAULT 7,
            thesis_strength DOUBLE PRECISION DEFAULT 0, last_price DOUBLE PRECISION,
            notes TEXT, payload TEXT
        )""")

# ============================================================
# SETTINGS
# ============================================================

def get_setting(key: str, default=None):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT value FROM system_settings WHERE key=%s", (key,))
        row = cur.fetchone()
    return row["value"] if row else default

def set_setting(key: str, value: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO system_settings(key,value,updated_at) VALUES(%s,%s,%s)
            ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
        """, (key, value, utc_now()))

def ensure_learning_start():
    if not get_setting("learning_started_at"):
        set_setting("learning_started_at", utc_now())

def get_ai_mode():
    return os.getenv("AI_MODE", get_setting("ai_mode", AI_MODE) or "PAPER").upper().strip()

def learning_age_days():
    start = parse_dt(get_setting("learning_started_at"))
    if not start:
        return 0
    return max(0, business_days_between(start, utc_now_dt()) - 1)

def learning_remaining_days():
    return max(0, LEARNING_DAYS - learning_age_days())

@app.on_event("startup")
def startup():
    init_db()
    ensure_learning_start()

# ============================================================
# CANDLES
# ============================================================

def get_candles(symbol: str, timeframe: str, limit: int = 220) -> List[Dict]:
    timeframe = normalize_tf(timeframe)
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM candles WHERE symbol=%s AND timeframe=%s
            ORDER BY id DESC LIMIT %s
        """, (normalize_symbol(symbol), timeframe, limit))
        rows = list(reversed(cur.fetchall()))
    return rows

def get_all_candles_for_scan(limit_per_symbol_tf: int = 220) -> Dict:
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY symbol, timeframe ORDER BY id DESC
                ) AS rn FROM candles
                WHERE symbol = ANY(%s) AND timeframe IN ('60', '1D')
            ) x WHERE rn <= %s ORDER BY symbol, timeframe, id ASC
        """, (WATCHLIST, limit_per_symbol_tf))
        rows = cur.fetchall()

    data = {}
    for r in rows:
        s = normalize_symbol(r["symbol"])
        tf = normalize_tf(r["timeframe"])
        data.setdefault(s, {"60": [], "1D": []})
        data[s][tf].append(r)
    return data

# ============================================================
# TECHNICAL INDICATORS
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
    return 100 - (100 / (1 + ag / al))

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
# V20: ON-BALANCE VOLUME (OBV)
# ============================================================

def compute_obv(candles) -> List[float]:
    obv = 0.0
    obv_series = []
    for i, c in enumerate(candles):
        vol = safe_float(c.get("volume"), 0) or 0
        if i == 0:
            obv_series.append(obv)
            continue
        prev_close = safe_float(candles[i - 1]["close"])
        curr_close = safe_float(c["close"])
        if curr_close is None or prev_close is None:
            obv_series.append(obv)
            continue
        if curr_close > prev_close:
            obv += vol
        elif curr_close < prev_close:
            obv -= vol
        obv_series.append(obv)
    return obv_series

# ============================================================
# V20: CHAIKIN MONEY FLOW (CMF)
# ============================================================

def compute_cmf(candles, period=14) -> float:
    if len(candles) < period:
        return 0.0
    recent = candles[-period:]
    mf_vol_sum = 0.0
    vol_sum = 0.0
    for c in recent:
        h = safe_float(c.get("high"))
        l = safe_float(c.get("low"))
        cl = safe_float(c.get("close"))
        v = safe_float(c.get("volume"), 0) or 0
        if h is None or l is None or cl is None:
            continue
        if (h - l) == 0:
            continue
        mfm = ((cl - l) - (h - cl)) / (h - l)
        mf_vol_sum += mfm * v
        vol_sum += v
    return mf_vol_sum / vol_sum if vol_sum > 0 else 0.0

# ============================================================
# V20: MARKET PHASE DETECTION (Wyckoff-inspired)
# ============================================================

def detect_market_phase(candles) -> Tuple[str, float, Dict]:
    """
    يكشف مرحلة السوق:
    ACCUMULATION  — مؤسسات تشتري بهدوء
    MARKUP        — صعود مدعوم بحجم
    DISTRIBUTION  — مؤسسات تبيع في القمة
    MARKDOWN      — هبوط مدعوم
    NEUTRAL       — لا اتجاه واضح
    """
    if len(candles) < 30:
        return "NEUTRAL", 0.0, {}

    closes  = [safe_float(c["close"]) for c in candles if safe_float(c["close"])]
    highs   = [safe_float(c["high"])  for c in candles if safe_float(c["high"])]
    lows    = [safe_float(c["low"])   for c in candles if safe_float(c["low"])]
    volumes = [safe_float(c.get("volume"), 0) or 0 for c in candles]

    if len(closes) < 20:
        return "NEUTRAL", 0.0, {}

    price = closes[-1]

    # Range tightness (last 20 candles)
    recent_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    recent_low  = min(lows[-20:])  if len(lows) >= 20 else min(lows)
    price_range_pct = ((recent_high - recent_low) / recent_low * 100) if recent_low else 10

    # OBV trend
    obv_series = compute_obv(candles)
    obv_rising = len(obv_series) >= 10 and obv_series[-1] > obv_series[-10]
    obv_falling = len(obv_series) >= 10 and obv_series[-1] < obv_series[-10]

    # CMF
    cmf = compute_cmf(candles, 14)

    # Volume expansion vs contraction
    avg_vol_20 = sma(volumes, 20) or 1
    avg_vol_10 = sma(volumes, 10) or 1
    vol_expanding = avg_vol_10 > avg_vol_20 * 1.1
    vol_contracting = avg_vol_10 < avg_vol_20 * 0.9

    # Price direction
    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50) if len(closes) >= 50 else ma20
    price_above_ma20 = ma20 and price > ma20
    price_above_ma50 = ma50 and price > ma50
    ma20_above_ma50  = ma20 and ma50 and ma20 >= ma50

    # Higher highs / higher lows structure
    mid = len(highs) // 2
    recent_hh  = max(highs[mid:])  > max(highs[:mid])  if mid > 0 else False
    recent_hl  = min(lows[mid:])   > min(lows[:mid])   if mid > 0 else False

    # Lower lows / lower highs (downtrend structure)
    recent_ll  = min(lows[mid:])   < min(lows[:mid])   if mid > 0 else False
    recent_lh  = max(highs[mid:])  < max(highs[:mid])  if mid > 0 else False

    details = {
        "price_range_pct": round(price_range_pct, 2),
        "cmf": round(cmf, 3),
        "obv_rising": obv_rising,
        "obv_falling": obv_falling,
        "vol_expanding": vol_expanding,
        "vol_contracting": vol_contracting,
        "higher_highs": recent_hh,
        "higher_lows": recent_hl,
        "lower_lows": recent_ll,
        "lower_highs": recent_lh,
        "price_above_ma20": price_above_ma20,
        "price_above_ma50": price_above_ma50,
        "ma20_above_ma50": ma20_above_ma50,
    }

    # Phase scoring
    acc_score  = 0
    mkup_score = 0
    dist_score = 0
    mkdn_score = 0

    # ACCUMULATION: tight range + money flowing in quietly
    if price_range_pct < 8 and cmf > 0.05:  acc_score += 3
    if obv_rising and vol_contracting:        acc_score += 2
    if price_above_ma20 and not price_above_ma50: acc_score += 1

    # MARKUP: price breaking up with volume
    if recent_hh and recent_hl:               mkup_score += 3
    if price_above_ma20 and price_above_ma50: mkup_score += 2
    if ma20_above_ma50 and obv_rising:        mkup_score += 2
    if vol_expanding and cmf > 0.1:           mkup_score += 2

    # DISTRIBUTION: high price + money flowing out
    if price_above_ma20 and cmf < -0.05:     dist_score += 3
    if obv_falling and vol_expanding:         dist_score += 2
    if recent_hh and not recent_hl:           dist_score += 2  # new highs without higher lows = weak

    # MARKDOWN: falling price + volume
    if recent_ll and recent_lh:               mkdn_score += 3
    if not price_above_ma20 and not price_above_ma50: mkdn_score += 2
    if obv_falling and cmf < -0.1:           mkdn_score += 2

    scores = {
        "ACCUMULATION": acc_score,
        "MARKUP": mkup_score,
        "DISTRIBUTION": dist_score,
        "MARKDOWN": mkdn_score,
    }

    best_phase = max(scores, key=scores.get)
    best_score = scores[best_phase]

    if best_score < 3:
        return "NEUTRAL", cmf, details

    return best_phase, cmf, details

# ============================================================
# V20: REVERSAL SIGNAL DETECTION
# ============================================================

def detect_reversal_signals(candles) -> List[Dict]:
    """
    يكشف إشارات عكس المسار:
    - Bearish Divergence: سعر يصنع highs جديدة لكن RSI ينزل
    - Volume Climax: حجم ضخم عند القمة مع candle ذيل طويل
    - Exhaustion: شمعة doji بعد صعود طويل
    - Distribution: CMF سلبي رغم السعر العالي
    """
    signals = []
    if len(candles) < 20:
        return signals

    closes  = [safe_float(c["close"]) for c in candles if safe_float(c["close"])]
    volumes = [safe_float(c.get("volume"), 0) or 0 for c in candles]

    if len(closes) < 15:
        return signals

    # RSI values for last 10 candles
    rsi_recent = []
    for i in range(max(0, len(closes) - 10), len(closes)):
        r = rsi(closes[:i + 1], 14)
        if r is not None:
            rsi_recent.append(r)

    # Bearish Divergence: price makes higher high but RSI makes lower high
    if len(rsi_recent) >= 6 and len(closes) >= 6:
        price_higher_high = closes[-1] > closes[-6]
        rsi_lower_high    = len(rsi_recent) >= 2 and rsi_recent[-1] < rsi_recent[-3]
        if price_higher_high and rsi_lower_high and closes[-1] > 0:
            signals.append({
                "type": "BEARISH_DIVERGENCE",
                "severity": "STRONG",
                "score_penalty": -20,
                "message": "سعر يصنع highs جديدة لكن RSI يضعف — خطر انعكاس"
            })

    # Volume Climax: حجم أكبر بـ 3x من المتوسط مع شمعة ضعيفة الإغلاق
    avg_vol = sma(volumes, 20) or 1
    last_vol = volumes[-1]
    if last_vol > avg_vol * 2.5:
        last_c = candles[-1]
        body = abs(safe_float(last_c["close"]) - safe_float(last_c["open"]) or 0)
        full_range = (safe_float(last_c["high"]) or 0) - (safe_float(last_c["low"]) or 0)
        if full_range > 0 and body / full_range < 0.35:
            signals.append({
                "type": "VOLUME_CLIMAX",
                "severity": "CRITICAL",
                "score_penalty": -25,
                "message": "حجم ضخم مع شمعة ضعيفة — علامة توزيع أو إرهاق"
            })

    # Exhaustion Doji after long run
    if len(closes) >= 10:
        run_pct = ((closes[-1] - closes[-10]) / closes[-10]) * 100 if closes[-10] else 0
        last_c = candles[-1]
        body = abs(safe_float(last_c["close"]) - safe_float(last_c["open"]) or 0)
        full_range = (safe_float(last_c["high"]) or 0) - (safe_float(last_c["low"]) or 0)
        if run_pct > 8 and full_range > 0 and body / full_range < 0.25:
            signals.append({
                "type": "EXHAUSTION_DOJI",
                "severity": "WARNING",
                "score_penalty": -10,
                "message": "شمعة تردد بعد صعود — مراقبة دخول جديد"
            })

    # CMF Distribution Signal
    cmf = compute_cmf(candles, 14)
    if cmf < -0.15:
        signals.append({
            "type": "DISTRIBUTION_DETECTED",
            "severity": "STRONG",
            "score_penalty": -15,
            "message": f"تدفق مال سلبي (CMF={round(cmf, 3)}) — مؤسسات تبيع"
        })

    return signals

# ============================================================
# V20: DYNAMIC TARGETS (ATR-Based)
# ============================================================

def dynamic_targets_atr(entry: float, atr_value: float, kind: str, market_phase: str) -> Dict:
    """
    الأهداف مبنية على ATR الفعلي لكل سهم
    وليست أرقاماً ثابتة
    """
    if not atr_value or atr_value <= 0:
        atr_value = entry * 0.015

    atr_pct = (atr_value / entry) * 100

    # Phase multipliers — في مرحلة MARKUP الأهداف أبعد
    phase_mult = {
        "MARKUP": 1.3,
        "ACCUMULATION": 1.1,
        "NEUTRAL": 1.0,
        "DISTRIBUTION": 0.7,
        "MARKDOWN": 0.5,
    }.get(market_phase, 1.0)

    if kind == "SHORT_SWING":
        t1 = entry + (atr_value * 1.5 * phase_mult)
        t2 = entry + (atr_value * 2.5 * phase_mult)
        t3 = entry + (atr_value * 3.5 * phase_mult)
        stop = entry - (atr_value * 1.0)
        # تقدير الأيام: كم يوم يحتاج ATR للوصول للهدف
        target_pct = ((t1 - entry) / entry) * 100
        est_days = max(2, min(8, round(target_pct / max(atr_pct, 0.1))))
        max_hold  = 7

    else:  # LONG_SWING
        t1 = entry + (atr_value * 3.0 * phase_mult)
        t2 = entry + (atr_value * 5.0 * phase_mult)
        t3 = entry + (atr_value * 7.0 * phase_mult)
        stop = entry - (atr_value * 1.8)
        target_pct = ((t1 - entry) / entry) * 100
        est_days = max(5, min(30, round(target_pct / max(atr_pct * 0.3, 0.1))))
        max_hold  = 28

    risk_pct = ((entry - stop) / entry) * 100 if entry else 0
    rr = ((t1 - entry) / (entry - stop)) if entry > stop else 0

    return {
        "entry_low":  round(entry * 0.995, 3),
        "entry_high": round(entry * 1.005, 3),
        "stop_loss":  round(stop, 3),
        "target1":    round(t1, 3),
        "target2":    round(t2, 3),
        "target3":    round(t3, 3),
        "target_pct": round(target_pct, 2),
        "risk_pct":   round(risk_pct, 2),
        "rr":         round(rr, 2),
        "atr_value":  round(atr_value, 4),
        "atr_pct":    round(atr_pct, 2),
        "estimated_days": est_days,
        "max_hold_days":  max_hold,
    }

# ============================================================
# V20: POSITION SIZING
# ============================================================

def position_sizing(entry, stop, kind):
    risk_pct_per_trade = 0.01 if kind == "SHORT_SWING" else 0.015
    max_position_pct   = 0.18 if kind == "SHORT_SWING" else 0.25
    risk_per_share     = max(entry - stop, 0)
    max_risk_aed       = CAPITAL * risk_pct_per_trade
    if risk_per_share <= 0 or entry <= 0:
        return {"qty": 0, "position_value": 0, "max_risk_aed": round(max_risk_aed, 2)}
    qty_by_risk = max_risk_aed / risk_per_share
    max_pos_val = CAPITAL * max_position_pct
    qty         = max(0, min(qty_by_risk, max_pos_val / entry))
    return {
        "qty": round(qty, 2),
        "position_value": round(qty * entry, 2),
        "max_risk_aed": round(max_risk_aed, 2),
        "max_position_value": round(max_pos_val, 2)
    }

# ============================================================
# V20: PATTERN LEARNING ADJUSTMENTS
# ============================================================

def get_rsi_bucket(r: Optional[float]) -> str:
    if r is None: return "UNKNOWN"
    if r < 40:    return "LOW"
    if r < 65:    return "HEALTHY"
    if r < 75:    return "EXTENDED"
    return "OVERBOUGHT"

def get_volume_bucket(vol_ratio: float) -> str:
    if vol_ratio < 1.0: return "LOW"
    if vol_ratio < 1.4: return "NORMAL"
    return "HIGH"

def get_pattern_adjustment(symbol: str, setup_type: str, market_phase: str,
                           rsi_bucket: str, volume_bucket: str) -> float:
    """
    يجلب التعديل المبني على الـ patterns التاريخية لهذا السهم
    إذا هذا النمط أعطى خسائر متكررة — ينزل الـ score
    إذا هذا النمط نجح تاريخياً — يرفع الـ score
    """
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT outcome, COUNT(*) as cnt,
                       AVG(return_pct) as avg_ret
                FROM v20_pattern_learning
                WHERE symbol=%s AND setup_type=%s
                  AND market_phase=%s AND rsi_bucket=%s
                  AND created_at > NOW() - INTERVAL '60 days'
                GROUP BY outcome
            """, (symbol, setup_type, market_phase, rsi_bucket))
            rows = cur.fetchall()

        if not rows:
            return 0.0

        wins   = sum(int(r["cnt"]) for r in rows if r["outcome"] == "WIN")
        losses = sum(int(r["cnt"]) for r in rows if r["outcome"] == "LOSS")
        total  = wins + losses
        if total < 5:
            return 0.0

        win_rate = wins / total
        avg_ret  = sum(float(r["avg_ret"] or 0) * int(r["cnt"]) for r in rows) / total

        # تعديل ما بين -20 و+15
        adj = (win_rate - 0.5) * 25 + max(-5, min(5, avg_ret))
        return max(-20, min(15, adj))
    except Exception:
        return 0.0

def record_pattern_outcome(symbol: str, setup_type: str, market_phase: str,
                           rsi_bucket: str, volume_bucket: str, trend_alignment: str,
                           outcome: str, return_pct: float, score: float, rr: float):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO v20_pattern_learning
                (symbol, setup_type, market_phase, rsi_bucket, volume_bucket,
                 trend_alignment, outcome, return_pct, score_at_entry, rr_at_entry, created_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (normalize_symbol(symbol), setup_type, market_phase, rsi_bucket,
                  volume_bucket, trend_alignment, outcome, return_pct, score, rr, utc_now()))
    except Exception as e:
        print(f"Pattern record error: {e}")

# ============================================================
# V20: CLASSIC LEARNING (fallback)
# ============================================================

def get_learning_adjustment(symbol: str) -> float:
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT score_adjustment FROM ai_learning_stats WHERE symbol=%s",
                        (normalize_symbol(symbol),))
            row = cur.fetchone()
        return float(row["score_adjustment"] or 0) if row else 0.0
    except Exception:
        return 0.0

def failure_penalty(symbol: str) -> float:
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT failure_reason, COUNT(*) AS c FROM ai_failure_memory
                WHERE symbol=%s AND created_at >= %s GROUP BY failure_reason
            """, (normalize_symbol(symbol),
                  (utc_now_dt() - timedelta(days=45)).isoformat()))
            rows = cur.fetchall()

        penalty = 0.0
        for r in rows:
            c = int(r["c"] or 0)
            if c >= 3:
                reason = r["failure_reason"]
                if reason in ["LOW_VOLUME_REVERSAL", "STRUCTURE_FAILED", "DISTRIBUTION_ENTRY"]:
                    penalty -= 10
                elif reason in ["POOR_RISK_REWARD", "LATE_ENTRY_OVERBOUGHT"]:
                    penalty -= 6
                else:
                    penalty -= 3
        return max(-25, penalty)
    except Exception:
        return 0.0

def update_learning(symbol: str, ret_pct: float, signal_type: str, is_virtual: bool):
    symbol = normalize_symbol(symbol)
    ret_pct = safe_float(ret_pct, 0) or 0

    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM ai_learning_stats WHERE symbol=%s", (symbol,))
            row = cur.fetchone()

            if row:
                t  = int(row["trades_count"] or 0)
                w  = int(row["wins_count"] or 0)
                l  = int(row["losses_count"] or 0)
                vs = int(row["virtual_short_count"] or 0)
                vsw= int(row["virtual_short_wins"] or 0)
                vsl= int(row["virtual_short_losses"] or 0)
                vl = int(row["virtual_long_count"] or 0)
                vlw= int(row["virtual_long_wins"] or 0)
                vll= int(row["virtual_long_losses"] or 0)

                st = str(signal_type or "").upper()
                if is_virtual and "SHORT" in st:
                    vs+=1; vsw+=(1 if ret_pct>0 else 0); vsl+=(1 if ret_pct<=0 else 0)
                elif is_virtual and "LONG" in st:
                    vl+=1; vlw+=(1 if ret_pct>0 else 0); vll+=(1 if ret_pct<=0 else 0)
                else:
                    t+=1; w+=(1 if ret_pct>0 else 0); l+=(1 if ret_pct<=0 else 0)

                total_ev = max(t + vs + vl, 1)
                old_avg  = float(row["avg_return_pct"] or 0)
                avg_ret  = ((old_avg * max(total_ev - 1, 0)) + ret_pct) / total_ev

                total_w  = w + vsw + vlw
                total_l  = l + vsl + vll
                closed   = total_w + total_l
                wr       = total_w / closed if closed else 0
                adj      = max(-20, min(15, (wr - 0.5) * 30 + avg_ret))

                cur.execute("""
                    UPDATE ai_learning_stats SET
                        trades_count=%s,wins_count=%s,losses_count=%s,
                        virtual_short_count=%s,virtual_short_wins=%s,virtual_short_losses=%s,
                        virtual_long_count=%s,virtual_long_wins=%s,virtual_long_losses=%s,
                        avg_return_pct=%s,score_adjustment=%s,updated_at=%s
                    WHERE symbol=%s
                """, (t,w,l,vs,vsw,vsl,vl,vlw,vll,avg_ret,adj,utc_now(),symbol))
            else:
                adj = 5 if ret_pct > 0 else -5
                cur.execute("""
                    INSERT INTO ai_learning_stats
                    (symbol,trades_count,wins_count,losses_count,
                     virtual_short_count,virtual_short_wins,virtual_short_losses,
                     virtual_long_count,virtual_long_wins,virtual_long_losses,
                     avg_return_pct,score_adjustment,updated_at)
                    VALUES(%s,1,%s,%s,0,0,0,0,0,0,%s,%s,%s)
                """, (symbol, 1 if ret_pct>0 else 0, 1 if ret_pct<=0 else 0,
                      ret_pct, adj, utc_now()))
    except Exception as e:
        print(f"update_learning error: {e}")

# ============================================================
# V20: DAILY TREND SCORE
# ============================================================

def daily_trend_score(d1):
    closes = [safe_float(x["close"]) for x in d1 if safe_float(x["close"])]
    if len(closes) < MIN_D1_CANDLES:
        return "UNKNOWN", 0

    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50) if len(closes) >= 50 else ma20
    score = 0
    if ma20 and closes[-1] > ma20: score += 12
    if ma50 and closes[-1] > ma50: score += 12
    if ma20 and ma50 and ma20 >= ma50: score += 8
    trend = "UP" if score >= 20 else "MIXED" if score >= 10 else "DOWN"
    return trend, score

# ============================================================
# V20: CORE SIGNAL BUILDER
# ============================================================

def build_signal_v20(symbol: str, kind: str, candles: List, d1: List) -> Optional[Dict]:
    symbol   = normalize_symbol(symbol)
    required = MIN_H1_CANDLES if kind == "SHORT_SWING" else MIN_D1_CANDLES

    if len(candles) < required:
        return None

    closes  = [safe_float(x["close"]) for x in candles if safe_float(x["close"])]
    volumes = [safe_float(x.get("volume"), 0) or 0 for x in candles]

    if len(closes) < required:
        return None

    price = closes[-1]
    if not price or price <= 0:
        return None

    # --- Indicators ---
    ma20     = sma(closes, 20)
    ma50     = sma(closes, 50) if len(closes) >= 50 else ma20
    ema20_v  = ema(closes, 20)
    r        = rsi(closes, 14)
    a        = atr(candles, 14)
    support, resistance = support_resistance(candles, 30 if kind == "SHORT_SWING" else 60)
    avg_vol  = sma(volumes, 20) or 1
    volume_ratio = volumes[-1] / avg_vol if avg_vol else 1
    momentum = recent_momentum(candles, 8 if kind == "SHORT_SWING" else 15)
    trend, _ = daily_trend_score(d1)

    # --- V20: Market Phase & Reversal ---
    market_phase, cmf, phase_details = detect_market_phase(candles)
    reversal_signals = detect_reversal_signals(candles)
    reversal_penalty = sum(s["score_penalty"] for s in reversal_signals)

    # --- Base Scoring ---
    score   = 0
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
        reasons.append("Daily trend UP")
    elif trend == "MIXED":
        score += 8
        reasons.append("Daily trend mixed")

    if r is not None and 40 <= r <= 65:
        score += 16
        reasons.append(f"RSI healthy ({round(r, 1)})")
    elif r is not None and 65 < r <= 73:
        score += 5
        reasons.append(f"RSI extended ({round(r, 1)})")
    elif r is not None and r > 73:
        score -= 10
        reasons.append(f"RSI overbought ({round(r, 1)}) — risky entry")

    if volume_ratio >= 1.5:
        score += 18
        reasons.append(f"Strong volume ({round(volume_ratio, 2)}x)")
    elif volume_ratio >= 1.2:
        score += 10
        reasons.append(f"Volume improving ({round(volume_ratio, 2)}x)")
    elif volume_ratio < 0.8:
        score -= 8
        reasons.append("Weak volume")

    if resistance and price >= resistance * 0.985:
        score += 10
        reasons.append("Near breakout zone")

    if momentum > (1.0 if kind == "SHORT_SWING" else 2.5):
        score += 8
        reasons.append("Positive momentum")

    # --- V20: Phase bonuses ---
    if market_phase == "ACCUMULATION":
        score += 15
        reasons.append("ACCUMULATION phase — تجميع")
    elif market_phase == "MARKUP":
        score += 20
        reasons.append("MARKUP phase — صعود مدعوم")
    elif market_phase == "DISTRIBUTION":
        score -= 25
        reasons.append("DISTRIBUTION phase — تصريف! ابتعد")
    elif market_phase == "MARKDOWN":
        score -= 30
        reasons.append("MARKDOWN phase — هبوط! ابتعد")

    # --- V20: Reversal penalty ---
    if reversal_penalty:
        score += reversal_penalty
        for rs_sig in reversal_signals:
            reasons.append(f"⚠️ {rs_sig['type']}")

    # --- Learning adjustments ---
    rsi_bucket  = get_rsi_bucket(r)
    vol_bucket  = get_volume_bucket(volume_ratio)
    trend_align = "ALIGNED" if trend == "UP" else "MIXED" if trend == "MIXED" else "AGAINST"

    pattern_adj  = get_pattern_adjustment(symbol, kind, market_phase, rsi_bucket, vol_bucket)
    learning_adj = get_learning_adjustment(symbol)
    pen          = failure_penalty(symbol)

    score += pattern_adj + learning_adj + pen

    # --- V20: Dynamic targets ---
    targets = dynamic_targets_atr(price, a or (price * 0.015), kind, market_phase)

    # --- Strength & Decision ---
    strength = (
        "VERY STRONG" if score >= 88 else
        "STRONG"      if score >= 72 else
        "MEDIUM"      if score >= 55 else
        "WEAK"
    )

    # Strict quality gates — لا BUY إلا بشروط صارمة
    bad_phase    = market_phase in ["DISTRIBUTION", "MARKDOWN"]
    has_reversal = any(s["severity"] in ["STRONG", "CRITICAL"] for s in reversal_signals)

    model_action = (
        "BUY" if (
            score >= MIN_SCORE_BUY
            and targets["rr"] >= MIN_RR_BUY
            and targets["risk_pct"] <= (5.5 if kind == "SHORT_SWING" else 12)
            and not bad_phase
            and not has_reversal
        ) else "WATCH"
    )

    mode   = get_ai_mode()
    action = {
        "LEARNING": "LEARN_SIGNAL" if model_action == "BUY" else "WATCH",
        "PAPER":    "PAPER_BUY"    if model_action == "BUY" else "WATCH",
        "LIVE":     "BUY"          if model_action == "BUY" else "WATCH",
    }.get(mode, "WATCH")

    timeframe = "60" if kind == "SHORT_SWING" else "1D"

    result = {
        "symbol":          symbol,
        "has_data":        True,
        "type":            kind,
        "mode":            mode,
        "action":          action,
        "model_action":    model_action,
        "timeframe":       timeframe,
        "price":           round(price, 3),
        "entry_zone":      [targets["entry_low"], targets["entry_high"]],
        "stop_loss":       targets["stop_loss"],
        "target1":         targets["target1"],
        "target2":         targets["target2"],
        "target3":         targets["target3"],
        "target_pct":      targets["target_pct"],
        "risk_pct":        targets["risk_pct"],
        "rr":              targets["rr"],
        "estimated_days":  targets["estimated_days"],
        "max_hold_days":   targets["max_hold_days"],
        "atr_value":       targets["atr_value"],
        "atr_pct":         targets["atr_pct"],
        "score":           round(score, 2),
        "strength":        strength,
        "trend":           trend,
        "market_phase":    market_phase,
        "cmf":             round(cmf, 3),
        "reversal_signals":reversal_signals,
        "support":         round(support, 3) if support else None,
        "resistance":      round(resistance, 3) if resistance else None,
        "rsi":             round(r, 2) if r is not None else None,
        "rsi_bucket":      rsi_bucket,
        "volume_ratio":    round(volume_ratio, 2),
        "volume_bucket":   vol_bucket,
        "trend_alignment": trend_align,
        "momentum_pct":    round(momentum, 2),
        "pattern_adj":     round(pattern_adj, 2),
        "learning_adj":    round(learning_adj, 2),
        "failure_pen":     round(pen, 2),
        "position_sizing": position_sizing(price, targets["stop_loss"], kind),
        "reason":          " + ".join(reasons) if reasons else "No strong setup",
        "phase_details":   phase_details,
    }

    result["rank_score"]    = ai_rank_score(result)
    result["display_action"]= classify_action(result)
    result["ai_comment"]    = ai_comment_v20(result)
    return result

def ai_rank_score(sig: Dict) -> float:
    score    = float(sig.get("score") or 0)
    rr       = min(float(sig.get("rr") or 0), 5) * 5
    vol      = min(float(sig.get("volume_ratio") or 0), 3) * 3
    phase_b  = {"MARKUP": 15, "ACCUMULATION": 10, "NEUTRAL": 3, "DISTRIBUTION": -20, "MARKDOWN": -25}.get(
                sig.get("market_phase"), 0)
    str_b    = {"VERY STRONG": 12, "STRONG": 7, "MEDIUM": 3, "WEAK": 0}.get(sig.get("strength"), 0)
    trend_b  = 10 if sig.get("trend") == "UP" else 3 if sig.get("trend") == "MIXED" else 0
    rev_pen  = sum(s.get("score_penalty", 0) for s in (sig.get("reversal_signals") or []))
    return round(score + rr + vol + phase_b + str_b + trend_b + rev_pen, 2)

def classify_action(sig: Optional[Dict]) -> str:
    if not sig: return "NO_DATA"
    if sig.get("model_action") == "BUY":
        phase = sig.get("market_phase", "")
        if phase == "ACCUMULATION": return "BUY — تجميع"
        if phase == "MARKUP":       return "BUY — صعود"
        return "BUY"
    if sig.get("strength") == "VERY STRONG": return "STRONG WATCH"
    if sig.get("strength") in ["STRONG", "MEDIUM"]: return "WATCH"
    return "WEAK WATCH"

def ai_comment_v20(sig: Optional[Dict]) -> str:
    if not sig: return "No data"
    phase = sig.get("market_phase", "NEUTRAL")
    rev   = sig.get("reversal_signals") or []
    if phase == "DISTRIBUTION": return "⚠️ تصريف — مؤسسات تبيع، تجنب الشراء"
    if phase == "MARKDOWN":     return "❌ هبوط — ابتعد تماماً"
    if any(s["severity"] == "CRITICAL" for s in rev): return "⚠️ إشارة انعكاس قوية — انتظر"
    if sig.get("model_action") == "BUY":
        return f"✅ فرصة {phase} — دخول متحكم به"
    if sig.get("strength") == "VERY STRONG":
        return f"👀 مراقبة قوية — {phase}"
    return f"مراقبة — {phase}"

def no_data_result(symbol, reason, h1_count=0, d1_count=0):
    return {
        "symbol": normalize_symbol(symbol), "has_data": False,
        "action": "NO_DATA", "model_action": "NO_DATA",
        "display_action": "NO_DATA", "reason": reason,
        "ai_comment": reason, "score": None, "rank_score": None,
        "strength": None, "price": None, "rr": None,
        "market_phase": None, "h1_count": h1_count, "d1_count": d1_count,
    }

def analyze_symbol(symbol: str, scan_type: str = "ALL") -> List[Dict]:
    symbol    = normalize_symbol(symbol)
    scan_type = scan_type.upper()
    if scan_type == "COMBINED": scan_type = "ALL"

    h1 = get_candles(symbol, "60", 100)
    d1 = get_candles(symbol, "1D", 100)
    signals = []

    if scan_type in ["ALL", "HOURLY"]:
        sig = build_signal_v20(symbol, "SHORT_SWING", h1, d1)
        if sig: signals.append(sig)

    if scan_type in ["ALL", "DAILY"]:
        sig = build_signal_v20(symbol, "LONG_SWING", d1, d1)
        if sig: signals.append(sig)

    return signals

def analyze_symbol_from_cache(symbol: str, scan_type: str, candle_cache: Dict) -> List[Dict]:
    symbol    = normalize_symbol(symbol)
    scan_type = scan_type.upper()
    if scan_type == "COMBINED": scan_type = "ALL"

    h1 = candle_cache.get(symbol, {}).get("60", [])
    d1 = candle_cache.get(symbol, {}).get("1D", [])
    signals = []

    if scan_type in ["ALL", "HOURLY"]:
        sig = build_signal_v20(symbol, "SHORT_SWING", h1, d1)
        if sig: signals.append(sig)

    if scan_type in ["ALL", "DAILY"]:
        sig = build_signal_v20(symbol, "LONG_SWING", d1, d1)
        if sig: signals.append(sig)

    return signals

# ============================================================
# V20: DECISION LOCK SYSTEM
# ============================================================

def get_locked_decision(symbol: str) -> Optional[Dict]:
    """يجلب القرار المقفل الحالي للسهم"""
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT * FROM v20_decision_lock
                WHERE symbol=%s AND status='LOCKED'
            """, (normalize_symbol(symbol),))
            return cur.fetchone()
    except Exception:
        return None

def should_override_decision(existing: Dict, new_score: float,
                              new_phase: str, has_critical_reversal: bool) -> Tuple[bool, str]:
    """
    هل نغير القرار؟
    القواعد:
    1. إذا في انعكاس حرج (Volume Climax / Bearish Divergence CRITICAL) — نغير فوراً
    2. إذا تغير المرحلة من MARKUP إلى DISTRIBUTION — نغير فوراً
    3. خلال أول 3 أيام — لا تغيير إلا بفارق 20+ نقطة
    4. غير ذلك — فارق 18+ نقطة مطلوب
    """
    old_score   = float(existing.get("score") or 0)
    locked_at   = parse_dt(str(existing.get("locked_at") or ""))
    hold_days   = business_days_between(locked_at, utc_now_dt()) if locked_at else 99
    old_phase   = existing.get("market_phase", "NEUTRAL")
    score_delta = abs(new_score - old_score)

    # الأولوية الأولى: انعكاس حرج
    if has_critical_reversal:
        return True, "critical_reversal_detected"

    # الأولوية الثانية: تحول من مرحلة إيجابية لسلبية
    bad_phases = ["DISTRIBUTION", "MARKDOWN"]
    if new_phase in bad_phases and old_phase not in bad_phases:
        return True, "phase_turned_negative"

    # خلال فترة القفل الأساسية
    if hold_days < DECISION_MIN_HOLD_DAYS:
        if score_delta >= 20:
            return True, "major_score_shift"
        return False, f"locked_{hold_days}_days_delta_{round(score_delta, 1)}"

    # بعد فترة القفل
    if score_delta >= DECISION_CHANGE_DELTA:
        return True, "score_delta_exceeded"

    return False, f"stable_hold_delta_{round(score_delta, 1)}"

def update_decision_lock(symbol: str, sig: Optional[Dict]):
    """يحدث أو يُنشئ decision lock للسهم"""
    if not sig or not sig.get("has_data"):
        return

    symbol    = normalize_symbol(symbol)
    new_score = float(sig.get("score") or 0)
    new_phase = sig.get("market_phase", "NEUTRAL")
    has_crit  = any(
        s.get("severity") in ["CRITICAL", "STRONG"]
        for s in (sig.get("reversal_signals") or [])
    )

    existing = get_locked_decision(symbol)

    if existing:
        override, reason = should_override_decision(existing, new_score, new_phase, has_crit)
        if not override:
            # فقط نحدث السعر الحالي وآخر فحص
            try:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute("""
                        UPDATE v20_decision_lock SET
                            last_checked_at=%s,
                            entry_price=(entry_price * 0.7 + %s * 0.3)
                        WHERE symbol=%s AND status='LOCKED'
                    """, (utc_now(), sig.get("price"), symbol))
            except Exception:
                pass
            return

        lock_reason = reason
    else:
        lock_reason = "new_signal"

    # نسجل القرار الجديد
    decision = sig.get("model_action", "WATCH")
    if new_phase in ["DISTRIBUTION", "MARKDOWN"]:
        decision = "AVOID"

    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO v20_decision_lock (
                    symbol, decision, confidence, score, entry_price,
                    entry_low, entry_high, stop_loss, target1, target2, target3,
                    estimated_days, market_phase, signal_type, lock_reason,
                    locked_at, last_checked_at, status, payload
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LOCKED',%s)
                ON CONFLICT(symbol) DO UPDATE SET
                    decision=EXCLUDED.decision, confidence=EXCLUDED.confidence,
                    score=EXCLUDED.score, entry_price=EXCLUDED.entry_price,
                    entry_low=EXCLUDED.entry_low, entry_high=EXCLUDED.entry_high,
                    stop_loss=EXCLUDED.stop_loss, target1=EXCLUDED.target1,
                    target2=EXCLUDED.target2, target3=EXCLUDED.target3,
                    estimated_days=EXCLUDED.estimated_days,
                    market_phase=EXCLUDED.market_phase, signal_type=EXCLUDED.signal_type,
                    lock_reason=EXCLUDED.lock_reason, locked_at=EXCLUDED.locked_at,
                    last_checked_at=EXCLUDED.last_checked_at, status='LOCKED',
                    unlock_reason=NULL, payload=EXCLUDED.payload
            """, (
                symbol, decision, sig.get("score"), sig.get("score"),
                sig.get("price"),
                sig.get("entry_zone", [None, None])[0],
                sig.get("entry_zone", [None, None])[1],
                sig.get("stop_loss"), sig.get("target1"),
                sig.get("target2"), sig.get("target3"),
                sig.get("estimated_days"), new_phase, sig.get("type"),
                lock_reason, utc_now(), utc_now(),
                json.dumps(sig)
            ))
    except Exception as e:
        print(f"Decision lock error: {e}")

def check_decision_exits():
    """يفحص هل أي قرار مقفل وصل للهدف أو للـ stop"""
    released = []
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM v20_decision_lock WHERE status='LOCKED'")
            locks = cur.fetchall()

        for lock in locks:
            symbol = lock["symbol"]
            candles = get_candles(symbol, "60", 10)
            if not candles:
                continue
            last_price = safe_float(candles[-1]["close"])
            if not last_price:
                continue

            target1   = safe_float(lock.get("target1"))
            stop_loss = safe_float(lock.get("stop_loss"))
            status    = "LOCKED"
            reason    = None

            if target1 and last_price >= target1:
                status = "HIT_TARGET"
                reason = f"Price {last_price} reached T1 {target1}"
            elif stop_loss and last_price <= stop_loss:
                status = "HIT_STOP"
                reason = f"Price {last_price} hit stop {stop_loss}"

            if status != "LOCKED":
                with get_db() as conn2:
                    cur2 = conn2.cursor()
                    cur2.execute("""
                        UPDATE v20_decision_lock
                        SET status=%s, unlock_reason=%s, last_checked_at=%s
                        WHERE symbol=%s
                    """, (status, reason, utc_now(), symbol))
                released.append({"symbol": symbol, "status": status, "reason": reason})
    except Exception as e:
        print(f"check_decision_exits error: {e}")
    return released

# ============================================================
# SCAN ENGINE
# ============================================================

def save_scan_result(scan_type: str, payload: Dict):
    scan_type = scan_type.upper()
    mode = get_ai_mode()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_scan_results(scan_type,mode,created_at,watchlist_count,scanned_count,signals_count,payload)
            VALUES(%s,%s,%s,%s,%s,%s,%s)
        """, (
            scan_type, mode, utc_now(),
            len(WATCHLIST),
            payload.get("scanned_count", 0),
            payload.get("signals_count", 0),
            json.dumps(payload)
        ))

def latest_scan_result(scan_type: str = "COMBINED") -> Optional[Dict]:
    scan_type = scan_type.upper()
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM ai_scan_results WHERE scan_type=%s ORDER BY id DESC LIMIT 1", (scan_type,))
        row = cur.fetchone()
    if not row:
        return None
    return json.loads(row["payload"])

def run_scan(scan_type: str) -> Dict:
    scan_type = scan_type.upper()
    if scan_type == "COMBINED": scan_type = "ALL"

    ranked, signals, coverage_rows, errors = [], [], [], []
    scanned = 0

    candle_cache = get_all_candles_for_scan(220)

    for s in WATCHLIST:
        scanned += 1
        h1 = candle_cache.get(s, {}).get("60", [])
        d1 = candle_cache.get(s, {}).get("1D", [])
        h1_count = len(h1[-30:])
        d1_count = len(d1[-10:])

        try:
            sigs = analyze_symbol_from_cache(s, scan_type, candle_cache)
            best = max(sigs, key=lambda x: x.get("rank_score", 0), default=None)

            if best:
                ranked.append(best)
                if best.get("model_action") == "BUY":
                    signals.append(best)
                # Update decision lock
                update_decision_lock(s, best)
                coverage_rows.append({
                    "symbol": s, "has_data": True,
                    "action": classify_action(best),
                    "model_action": best.get("model_action"),
                    "score": best.get("score"), "rank_score": best.get("rank_score"),
                    "strength": best.get("strength"), "price": best.get("price"),
                    "rr": best.get("rr"), "market_phase": best.get("market_phase"),
                    "estimated_days": best.get("estimated_days"),
                    "volume_ratio": best.get("volume_ratio"),
                    "ai_comment": ai_comment_v20(best),
                    "h1_count": h1_count, "d1_count": d1_count,
                })
            else:
                coverage_rows.append(no_data_result(
                    s, f"insufficient data h1={h1_count} d1={d1_count}", h1_count, d1_count
                ))
        except Exception as e:
            errors.append({"symbol": s, "error": str(e)})
            coverage_rows.append(no_data_result(s, str(e), h1_count, d1_count))
            if len(errors) >= SCAN_MAX_ERRORS:
                break

    ranked  = sorted(ranked,  key=lambda x: x.get("rank_score", 0), reverse=True)
    signals = sorted(signals, key=lambda x: x.get("rank_score", 0), reverse=True)

    # Record observations
    payload = {
        "ok": True,
        "version": "V20_INTELLIGENT_CORE",
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
        "errors": errors[:10],
        "coverage": sorted(coverage_rows, key=lambda x: (x.get("rank_score") or -1), reverse=True)
    }

    try:
        record_observations(payload)
    except Exception as e:
        payload["observation_error"] = str(e)

    return payload

def save_combined_scan():
    hourly = latest_scan_result("HOURLY") or {}
    daily  = latest_scan_result("DAILY") or {}

    all_signals = sorted(
        hourly.get("signals", []) + daily.get("signals", []),
        key=lambda x: (x.get("score") or 0, x.get("rr") or 0),
        reverse=True
    )[:20]

    all_ranked = sorted(
        hourly.get("ranked", []) + daily.get("ranked", []),
        key=lambda x: x.get("rank_score") or 0,
        reverse=True
    )

    hcov = {x["symbol"]: x for x in hourly.get("coverage", [])}
    dcov = {x["symbol"]: x for x in daily.get("coverage",  [])}
    combined_coverage = []
    for s in WATCHLIST:
        h = hcov.get(s)
        d = dcov.get(s)
        best = h if (h and (not d or (h.get("rank_score") or -1) >= (d.get("rank_score") or -1))) else d
        combined_coverage.append(best or no_data_result(s, "no_scan"))

    combined = {
        "ok": True, "version": "V20_INTELLIGENT_CORE",
        "mode": get_ai_mode(), "scan_type": "COMBINED",
        "created_at": utc_now(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
        "scanned_count": len(WATCHLIST),
        "signals_count": len(all_signals),
        "signals": all_signals, "ranked_count": len(all_ranked),
        "ranked": all_ranked, "coverage": combined_coverage
    }
    save_scan_result("COMBINED", combined)

# ============================================================
# VIRTUAL SIGNALS + OBSERVATIONS
# ============================================================

def sig_key(sig):
    return f"{sig['symbol']}-{sig['type']}-{sig['price']}-{sig['target1']}-{sig['stop_loss']}"

def record_virtual_signal(sig) -> bool:
    if not sig or not sig.get("has_data"):
        return False
    key = sig_key(sig)
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO ai_virtual_signals
                (signal_key,mode,symbol,signal_type,timeframe,action,price,
                 entry_low,entry_high,stop_loss,target1,target2,target3,
                 score,strength,rr,risk_pct,target_pct,max_hold_days,
                 estimated_days,market_phase,created_at,status,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)
            """, (
                key, sig["mode"], sig["symbol"], sig["type"], sig["timeframe"],
                sig["action"], sig["price"],
                sig.get("entry_zone", [None, None])[0],
                sig.get("entry_zone", [None, None])[1],
                sig["stop_loss"], sig["target1"], sig["target2"], sig["target3"],
                sig["score"], sig["strength"], sig["rr"], sig["risk_pct"],
                sig["target_pct"], sig["max_hold_days"],
                sig.get("estimated_days"), sig.get("market_phase"),
                utc_now(), json.dumps(sig)
            ))
        return True
    except psycopg2.errors.UniqueViolation:
        return False
    except Exception as e:
        print(f"record_virtual_signal error: {e}")
        return False

def evaluate_virtual_signals() -> List[Dict]:
    if not is_uae_trading_day():
        return []

    evaluated = []
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM ai_virtual_signals WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
            rows = cur.fetchall()

        for sig in rows:
            try:
                symbol  = sig["symbol"]
                tf      = normalize_tf(sig["timeframe"])
                created = parse_dt(sig["created_at"])
                candles = get_candles(symbol, tf, 400)

                relevant = [
                    c for c in candles
                    if (parse_dt(c["bar_time"]) or parse_dt(c["received_at"])) and
                       created and
                       (parse_dt(c["bar_time"]) or parse_dt(c["received_at"])) >= created
                ]
                if not relevant:
                    continue

                max_high    = max(float(x["high"]) for x in relevant)
                min_low     = min(float(x["low"])  for x in relevant)
                latest_close= float(relevant[-1]["close"])
                price       = float(sig["price"])
                target1     = float(sig["target1"])
                stop        = float(sig["stop_loss"])
                max_hold    = int(sig["max_hold_days"] or 7)

                target_hit  = max_high >= target1
                stop_hit    = min_low <= stop

                status, outcome = "OPEN", None
                ret_pct = ((latest_close - price) / price) * 100

                if target_hit and not stop_hit:
                    status, outcome = "CLOSED", "TARGET1_HIT"
                    ret_pct = ((target1 - price) / price) * 100
                elif stop_hit and not target_hit:
                    status, outcome = "CLOSED", "STOP_HIT"
                    ret_pct = ((stop - price) / price) * 100
                elif target_hit and stop_hit:
                    status, outcome = "CLOSED", "BOTH_TOUCHED"
                    ret_pct = ((stop - price) / price) * 100
                elif created and business_days_between(created, utc_now_dt()) > max_hold:
                    status, outcome = "CLOSED", "TIME_EXIT"
                    ret_pct = ((latest_close - price) / price) * 100

                with get_db() as conn2:
                    cur2 = conn2.cursor()
                    cur2.execute("""
                        UPDATE ai_virtual_signals SET
                            max_high=%s, min_low=%s, bars_checked=%s,
                            status=%s, outcome=%s, outcome_at=%s
                        WHERE id=%s
                    """, (
                        max_high, min_low, len(relevant), status, outcome,
                        utc_now() if status == "CLOSED" else None, sig["id"]
                    ))

                if status == "CLOSED":
                    update_learning(symbol, ret_pct, sig["signal_type"], is_virtual=True)

                    # V20: Record pattern outcome
                    payload_data = json.loads(sig["payload"]) if sig.get("payload") else {}
                    outcome_label = "WIN" if ret_pct > 0 else "LOSS"
                    record_pattern_outcome(
                        symbol=symbol,
                        setup_type=sig.get("signal_type", ""),
                        market_phase=sig.get("market_phase") or payload_data.get("market_phase", "NEUTRAL"),
                        rsi_bucket=payload_data.get("rsi_bucket", "UNKNOWN"),
                        volume_bucket=payload_data.get("volume_bucket", "UNKNOWN"),
                        trend_alignment=payload_data.get("trend_alignment", "UNKNOWN"),
                        outcome=outcome_label,
                        return_pct=round(ret_pct, 2),
                        score=float(sig.get("score") or 0),
                        rr=float(sig.get("rr") or 0),
                    )

                    if ret_pct < 0:
                        _classify_and_record_failure(symbol, sig, payload_data, ret_pct)

                    evaluated.append({
                        "id": sig["id"], "symbol": symbol,
                        "type": sig["signal_type"], "outcome": outcome,
                        "return_pct": round(ret_pct, 2)
                    })
            except Exception as e:
                print(f"evaluate signal {sig.get('id')} error: {e}")
                continue
    except Exception as e:
        print(f"evaluate_virtual_signals error: {e}")

    return evaluated

def _classify_and_record_failure(symbol, sig, payload, ret_pct):
    vol   = float(payload.get("volume_ratio") or sig.get("volume_ratio") or 0)
    r_val = float(payload.get("rsi") or 0)
    rr    = float(sig.get("rr") or 0)
    phase = payload.get("market_phase") or sig.get("market_phase") or "UNKNOWN"
    strength = payload.get("strength") or sig.get("strength") or ""

    if phase in ["DISTRIBUTION", "MARKDOWN"]:
        reason = "DISTRIBUTION_ENTRY"
        lesson = "لا تشتري في مرحلة التصريف أو الهبوط"
    elif vol < 0.9:
        reason = "LOW_VOLUME_REVERSAL"
        lesson = "تجنب الدخول بحجم ضعيف"
    elif r_val > 73:
        reason = "LATE_ENTRY_OVERBOUGHT"
        lesson = "تجنب الدخول بعد RSI مرتفع جداً"
    elif rr < 1.0:
        reason = "POOR_RISK_REWARD"
        lesson = "اشترط RR >= 1.2 على الأقل"
    elif strength in ["WEAK", "MEDIUM"]:
        reason = "LOW_QUALITY_SETUP"
        lesson = "اشترط قوة STRONG كحد أدنى"
    elif ret_pct <= -5:
        reason = "STRUCTURE_FAILED"
        lesson = "الهيكل فشل — قلل الثقة بالسهم مؤقتاً"
    else:
        reason = "NORMAL_LOSS"
        lesson = "خسارة طبيعية — لا تغيير"

    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO ai_failure_memory
                (symbol,setup_type,failure_reason,loss_pct,market_state,lesson,created_at,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            """, (normalize_symbol(symbol), sig.get("signal_type",""),
                  reason, round(ret_pct, 2), phase, lesson, utc_now(), json.dumps(payload)))
    except Exception:
        pass

def record_observations(scan_payload: Dict) -> int:
    created = 0
    try:
        with get_db() as conn:
            cur = conn.cursor()
            for item in scan_payload.get("ranked", []):
                if not item.get("has_data") or not item.get("price"):
                    continue
                key = (f"{item.get('symbol')}-{scan_payload.get('scan_type')}"
                       f"-{item.get('timeframe')}-{item.get('price')}"
                       f"-{scan_payload.get('created_at','')[:13]}")
                try:
                    cur.execute("""
                        INSERT INTO ai_observations
                        (obs_key,symbol,scan_type,timeframe,action,model_action,
                         strength,score,rank_score,price,observed_at,status,payload)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)
                        ON CONFLICT DO NOTHING
                    """, (
                        key, item.get("symbol"), scan_payload.get("scan_type"),
                        item.get("timeframe"),
                        item.get("display_action") or item.get("action"),
                        item.get("model_action"), item.get("strength"),
                        item.get("score"), item.get("rank_score"), item.get("price"),
                        scan_payload.get("created_at"), json.dumps(item)
                    ))
                    created += cur.rowcount
                except Exception:
                    conn.rollback()
    except Exception as e:
        print(f"record_observations error: {e}")
    return created

def evaluate_observations() -> List[Dict]:
    if not is_uae_trading_day():
        return []
    out = []
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM ai_observations WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
            rows = cur.fetchall()

        for obs in rows:
            try:
                tf       = normalize_tf(obs["timeframe"] or "60")
                symbol   = obs["symbol"]
                observed = parse_dt(obs["observed_at"])
                candles  = get_candles(symbol, tf, 400)
                relevant = [
                    c for c in candles
                    if observed and (parse_dt(c["bar_time"]) or parse_dt(c["received_at"])) and
                       (parse_dt(c["bar_time"]) or parse_dt(c["received_at"])) >= observed
                ]
                if not relevant: continue

                price    = float(obs["price"] or 0)
                if price <= 0: continue

                max_high = max(float(x["high"]) for x in relevant)
                min_low  = min(float(x["low"])  for x in relevant)
                latest   = float(relevant[-1]["close"])
                ret_pct  = ((latest - price) / price) * 100
                max_days = 5 if tf == "60" else 25

                target_hit = ((max_high - price) / price) * 100 >= OBSERVATION_TARGET_PCT
                drop_hit   = ((price - min_low) / price) * 100 >= OBSERVATION_DROP_PCT

                status, outcome = "OPEN", None
                if target_hit:
                    status, outcome = "CLOSED", "WATCH_RALLIED"
                elif drop_hit:
                    status, outcome = "CLOSED", "WATCH_DROPPED"
                elif observed and business_days_between(observed, utc_now_dt()) > max_days:
                    status, outcome = "CLOSED", "WATCH_TIME_EXIT"

                with get_db() as conn2:
                    cur2 = conn2.cursor()
                    cur2.execute("""
                        UPDATE ai_observations SET
                            max_high=%s,min_low=%s,return_pct=%s,
                            status=%s,outcome=%s,outcome_at=%s
                        WHERE id=%s
                    """, (max_high, min_low, ret_pct, status, outcome,
                          utc_now() if status == "CLOSED" else None, obs["id"]))

                if status == "CLOSED":
                    update_learning(symbol, ret_pct, "OBSERVATION", is_virtual=True)
                    out.append({"symbol": symbol, "outcome": outcome, "return_pct": round(ret_pct, 2)})
            except Exception:
                continue
    except Exception as e:
        print(f"evaluate_observations error: {e}")
    return out

# ============================================================
# SELF EVALUATION V20 — تقييم ذاتي شامل
# ============================================================

def run_self_evaluation() -> Dict:
    """
    التقييم الذاتي الكامل:
    - Win Rate
    - Pattern Quality
    - Phase Detection Accuracy
    - Decision Stability Score
    - Readiness Grade
    """
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals")
            total = int(cur.fetchone()["c"])

            cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='CLOSED'")
            evaluated = int(cur.fetchone()["c"])

            cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE 'TARGET%'")
            target_hits = int(cur.fetchone()["c"])

            cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE '%STOP%'")
            stop_hits = int(cur.fetchone()["c"])

            cur.execute("""
                SELECT AVG(rr) avg_rr, AVG(score) avg_score
                FROM ai_virtual_signals WHERE status='CLOSED'
            """)
            rr_row = cur.fetchone()

            cur.execute("SELECT COUNT(*) c FROM ai_failure_memory")
            lessons_count = int(cur.fetchone()["c"])

            # Pattern win rates
            cur.execute("""
                SELECT market_phase,
                       SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
                       COUNT(*) as total
                FROM v20_pattern_learning
                WHERE created_at > NOW() - INTERVAL '30 days'
                GROUP BY market_phase
            """)
            phase_stats = cur.fetchall()

            # Decision lock stats
            cur.execute("SELECT COUNT(*) c FROM v20_decision_lock WHERE status='LOCKED'")
            locked_count = int(cur.fetchone()["c"])
            cur.execute("SELECT COUNT(*) c FROM v20_decision_lock WHERE status='HIT_TARGET'")
            lock_hits = int(cur.fetchone()["c"])
            cur.execute("SELECT COUNT(*) c FROM v20_decision_lock WHERE status='HIT_STOP'")
            lock_stops = int(cur.fetchone()["c"])

            cur.execute("""
                SELECT avg_return_pct, (virtual_short_count+virtual_long_count+trades_count) AS cnt
                FROM ai_learning_stats
                WHERE (virtual_short_count+virtual_long_count+trades_count) > 0
            """)
            learn_rows = cur.fetchall()

        win_rate    = (target_hits / evaluated * 100) if evaluated else 0
        total_w     = sum(int(r["cnt"] or 0) for r in learn_rows)
        avg_return  = (
            sum(float(r["avg_return_pct"] or 0) * int(r["cnt"] or 0) for r in learn_rows) / total_w
            if total_w else 0
        )
        rr_quality  = float(rr_row["avg_rr"] or 0) if rr_row else 0
        avg_score   = float(rr_row["avg_score"] or 0) if rr_row else 0

        # Phase analysis
        phase_report = {}
        for ps in phase_stats:
            ph   = ps["market_phase"]
            w    = int(ps["wins"] or 0)
            tot  = int(ps["total"] or 0)
            wr   = round(w / tot * 100, 1) if tot else 0
            phase_report[ph] = {"wins": w, "total": tot, "win_rate_pct": wr}

        # Confidence scoring
        confidence = 0
        conf_reasons = []

        if evaluated >= 10:   confidence += 10; conf_reasons.append("+10: 10+ signals evaluated")
        if evaluated >= 30:   confidence += 10; conf_reasons.append("+10: 30+ signals")
        if evaluated >= 60:   confidence += 10; conf_reasons.append("+10: 60+ signals")
        if win_rate >= 50:    confidence += 10; conf_reasons.append("+10: win rate ≥50%")
        if win_rate >= 60:    confidence += 10; conf_reasons.append("+10: win rate ≥60%")
        if win_rate >= 68:    confidence += 10; conf_reasons.append("+10: win rate ≥68%")
        if avg_return > 0:    confidence += 10; conf_reasons.append("+10: avg return positive")
        if avg_return > 1.5:  confidence += 5;  conf_reasons.append("+5: avg return >1.5%")
        if rr_quality >= 1.2: confidence += 10; conf_reasons.append("+10: avg RR ≥1.2")
        if rr_quality >= 1.8: confidence += 5;  conf_reasons.append("+5: avg RR ≥1.8")

        # MARKUP phase working well?
        mkup = phase_report.get("MARKUP", {})
        if mkup.get("win_rate_pct", 0) >= 60 and mkup.get("total", 0) >= 5:
            confidence += 5; conf_reasons.append("+5: MARKUP phase proven")

        # Penalize low performance
        if win_rate < 40 and evaluated >= 15:
            confidence -= 15; conf_reasons.append("-15: win rate below 40%")
        if avg_return < -1:
            confidence -= 10; conf_reasons.append("-10: avg return negative")

        confidence = max(0, min(100, confidence))
        age = learning_age_days()

        # State & Readiness
        if confidence < 35:
            state = "LEARNING"
            ready = False
            recommendation = "🔴 وضع التعلم — لا تداول بأموال حقيقية"
            grade = "F"
        elif confidence < 50:
            state = "DEVELOPING"
            ready = False
            recommendation = "🟠 النظام يتطور — مراقبة فقط، لا تداول"
            grade = "D"
        elif confidence < 65:
            state = "STABILIZING"
            ready = False
            recommendation = "🟡 يستقر — يمكن تداول تجريبي بمبلغ صغير جداً"
            grade = "C"
        elif confidence < 80:
            state = "STABLE"
            ready = True
            recommendation = "🟢 مستقر — تداول ورقي كامل، رأس مال حقيقي صغير ممكن"
            grade = "B"
        else:
            state = "HIGH_CONFIDENCE"
            ready = True
            recommendation = "✅ ثقة عالية — جاهز مع إدارة مخاطر صارمة"
            grade = "A"

        # Additional conditions regardless of confidence
        if age < LEARNING_DAYS:
            state = min(state, "STABILIZING")
            recommendation += f" | ⏳ لا تزال {round(LEARNING_DAYS - age, 1)} يوم تداول باقية للتعلم"
            ready = False

        result = {
            "ok": True,
            "version": "V20",
            "created_at": utc_now(),
            "system_state": state,
            "readiness_grade": grade,
            "confidence_score": confidence,
            "ready_for_trading": ready,
            "recommendation": recommendation,
            "confidence_breakdown": conf_reasons,

            "learning_age_days": round(age, 2),
            "learning_days_required": LEARNING_DAYS,
            "learning_progress_pct": round(min(100, age / LEARNING_DAYS * 100), 1),

            "signals": {
                "total": total,
                "evaluated": evaluated,
                "target_hits": target_hits,
                "stop_hits": stop_hits,
                "win_rate_pct": round(win_rate, 2),
                "avg_return_pct": round(avg_return, 2),
                "avg_rr": round(rr_quality, 2),
                "avg_score": round(avg_score, 2),
            },
            "phase_performance": phase_report,
            "decision_locks": {
                "active": locked_count,
                "hit_target": lock_hits,
                "hit_stop": lock_stops,
            },
            "lessons_learned": lessons_count,
        }

        # Save to DB
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO ai_self_evaluation
                (created_at,system_state,confidence_score,ready_for_trading,
                 win_rate,avg_return_pct,rr_quality,lessons_count,
                 pattern_quality,recommendation,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                utc_now(), state, confidence, ready,
                win_rate, avg_return, rr_quality, lessons_count,
                float(mkup.get("win_rate_pct", 0)), recommendation,
                json.dumps(result)
            ))

        return result
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}

# ============================================================
# READINESS REPORT (backward compat)
# ============================================================

def readiness_report() -> Dict:
    ev = run_self_evaluation()
    sigs = ev.get("signals", {})
    return {
        "ok": True,
        "mode": get_ai_mode(),
        "status": ev.get("system_state", "LEARNING"),
        "readiness_grade": ev.get("readiness_grade", "F"),
        "confidence_score": ev.get("confidence_score", 0),
        "ready_for_trading": ev.get("ready_for_trading", False),
        "recommendation": ev.get("recommendation", ""),
        "learning_age_days": ev.get("learning_age_days", 0),
        "learning_days_required": LEARNING_DAYS,
        "learning_progress_pct": ev.get("learning_progress_pct", 0),
        "total_signals": sigs.get("total", 0),
        "evaluated_signals": sigs.get("evaluated", 0),
        "target_hits": sigs.get("target_hits", 0),
        "stop_hits": sigs.get("stop_hits", 0),
        "open_signals": sigs.get("total", 0) - sigs.get("evaluated", 0),
        "win_rate": sigs.get("win_rate_pct", 0),
        "avg_return_pct": sigs.get("avg_return_pct", 0),
    }

def get_signal_stats() -> Dict:
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals");                   total    = int(cur.fetchone()["c"])
        cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='OPEN'"); open_c = int(cur.fetchone()["c"])
        cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='CLOSED'"); closed = int(cur.fetchone()["c"])
        cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE 'TARGET%'"); hits = int(cur.fetchone()["c"])
        cur.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE '%STOP%'");  stops= int(cur.fetchone()["c"])
    return {
        "total_signals": total, "open_signals": open_c, "closed_signals": closed,
        "target_hits": hits, "stop_hits": stops,
        "win_rate": round(hits / closed * 100, 2) if closed else 0,
    }

# ============================================================
# TELEGRAM
# ============================================================

def tg_api(method, payload):
    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN missing"}
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
            json=payload, timeout=30
        )
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status_code": r.status_code, "text": r.text[:500]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def tg_send(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id, "text": str(text)[:4000],
        "parse_mode": "HTML", "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_api("sendMessage", payload)

def tg_main_send(text, reply_markup=None):
    if not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_CHAT_ID missing"}
    return tg_send(TELEGRAM_CHAT_ID, text, reply_markup)

def signal_keyboard(symbol):
    return {"inline_keyboard": [[
        {"text": "تحليل أعمق", "callback_data": f"more:{symbol}"},
        {"text": "تجاهل", "callback_data": f"ignore:{symbol}"}
    ]]}

# ============================================================
# TELEGRAM FORMATTERS V20
# ============================================================

def format_signal_v20(sig: Dict) -> str:
    size        = sig.get("position_sizing") or {}
    entry_zone  = sig.get("entry_zone") or ["-", "-"]
    phase       = sig.get("market_phase", "-")
    rev_sigs    = sig.get("reversal_signals") or []
    rev_text    = "\n".join(f"⚠️ {s['type']}: {s['message']}" for s in rev_sigs) if rev_sigs else "لا توجد"
    est_days    = sig.get("estimated_days", "-")

    phase_emoji = {
        "ACCUMULATION": "🔵", "MARKUP": "🟢",
        "DISTRIBUTION": "🔴", "MARKDOWN": "⛔", "NEUTRAL": "⚪"
    }.get(phase, "⚪")

    return f"""
<b>{sig.get('symbol', 'UNKNOWN')} — UAE AI V20</b>

{phase_emoji} المرحلة: <b>{phase}</b>
نوع الإشارة: <b>{sig.get('type', '-')}</b>
القرار: <b>{sig.get('display_action', '-')}</b>
القوة: <b>{sig.get('strength', '-')}</b>
النقاط: <b>{sig.get('score', '-')}</b>
RR: <b>{sig.get('rr', '-')}</b>
CMF: <b>{sig.get('cmf', '-')}</b>

💰 السعر الحالي: <b>{sig.get('price', '-')}</b>
منطقة الدخول: <b>{entry_zone[0]} — {entry_zone[1]}</b>
وقف الخسارة: <b>{sig.get('stop_loss', '-')}</b>

🎯 الهدف الأول:  <b>{sig.get('target1', '-')}</b>
🎯 الهدف الثاني: <b>{sig.get('target2', '-')}</b>
🎯 الهدف الثالث: <b>{sig.get('target3', '-')}</b>

⏱ تقدير الوصول: <b>{est_days} يوم تداول</b>
المخاطرة: <b>{sig.get('risk_pct', '-')}%</b>
الهدف: <b>{sig.get('target_pct', '-')}%</b>

📊 RSI: {sig.get('rsi', '-')} | حجم: {sig.get('volume_ratio', '-')}x | Momentum: {sig.get('momentum_pct', '-')}%

📦 الحجم المقترح: {size.get('qty', '-')} سهم
قيمة المركز: {size.get('position_value', '-')} AED
الحد الأقصى للمخاطرة: {size.get('max_risk_aed', '-')} AED

⚠️ إشارات الانعكاس:
{rev_text}

السبب: {esc(sig.get('reason', '-'))}

{DASHBOARD_URL}
""".strip()

def format_daily_report_v20() -> str:
    ev    = run_self_evaluation()
    stats = get_signal_stats()
    scan  = latest_scan_result("COMBINED") or {}

    grade   = ev.get("readiness_grade", "?")
    state   = ev.get("system_state", "?")
    conf    = ev.get("confidence_score", 0)
    ready   = "✅ جاهز" if ev.get("ready_for_trading") else "🔴 غير جاهز"
    rec     = ev.get("recommendation", "")
    ph_perf = ev.get("phase_performance", {})

    phase_lines = []
    for ph, pd in ph_perf.items():
        phase_lines.append(f"  {ph}: {pd.get('win_rate_pct',0)}% ({pd.get('total',0)} إشارة)")

    top_signals = scan.get("signals", [])[:5]
    sig_lines = []
    for s in top_signals:
        phase_e = {"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔"}.get(s.get("market_phase",""),"⚪")
        sig_lines.append(
            f"{phase_e} <b>{s.get('symbol')}</b> | {s.get('type')} | "
            f"Score:{s.get('score')} | T1:{s.get('target1')} | "
            f"~{s.get('estimated_days','?')} يوم"
        )

    locks  = ev.get("decision_locks", {})

    lines = [
        "<b>🤖 UAE AI V20 — التقرير اليومي</b>",
        f"📅 {uae_now_dt().strftime('%Y-%m-%d')} | {uae_now_dt().strftime('%H:%M')} UAE",
        "",
        "<b>حالة النظام</b>",
        f"الدرجة: <b>{grade}</b> | الحالة: <b>{state}</b>",
        f"الثقة: <b>{conf}%</b> | الجاهزية: <b>{ready}</b>",
        f"التوصية: {rec}",
        "",
        f"<b>تقدم التعلم</b>",
        f"أيام التعلم: {ev.get('learning_age_days',0)} / {LEARNING_DAYS}",
        f"التقدم: {ev.get('learning_progress_pct',0)}%",
        "",
        "<b>الإشارات</b>",
        f"إجمالي: {stats['total_signals']} | مفتوحة: {stats['open_signals']} | مغلقة: {stats['closed_signals']}",
        f"أصابت الهدف: {stats['target_hits']} | ضربت الـstop: {stats['stop_hits']}",
        f"Win Rate: <b>{stats['win_rate']}%</b>",
        f"متوسط العائد: {ev.get('signals',{}).get('avg_return_pct',0)}%",
        f"متوسط RR: {ev.get('signals',{}).get('avg_rr',0)}",
        "",
        "<b>أداء المراحل</b>",
    ] + phase_lines + [
        "",
        "<b>قفل القرارات</b>",
        f"مقفلة: {locks.get('active',0)} | أصابت: {locks.get('hit_target',0)} | وقف: {locks.get('hit_stop',0)}",
        "",
    ]

    if sig_lines:
        lines.append("<b>أفضل الإشارات الحالية</b>")
        lines.extend(sig_lines)
    else:
        lines.append("لا توجد إشارات شراء قوية الآن")

    lines += ["", DASHBOARD_URL]
    return "\n".join(lines)

def format_weekly_report_v20() -> str:
    ev    = run_self_evaluation()
    stats = get_signal_stats()

    lines = [
        "<b>🤖 UAE AI V20 — التقرير الأسبوعي</b>",
        f"📅 أسبوع {uae_now_dt().strftime('%Y-%m-%d')}",
        "",
        f"الثقة: <b>{ev.get('confidence_score',0)}%</b> | الدرجة: <b>{ev.get('readiness_grade','?')}</b>",
        f"الحالة: <b>{ev.get('system_state','?')}</b>",
        f"التوصية: {ev.get('recommendation','')}",
        "",
        f"Win Rate: <b>{stats['win_rate']}%</b>",
        f"إجمالي الإشارات: {stats['total_signals']}",
        f"مغلقة: {stats['closed_signals']}",
        f"الدروس المتعلمة: {ev.get('lessons_learned',0)}",
        "",
        "<b>أداء المراحل هذا الأسبوع</b>",
    ]
    for ph, pd in ev.get("phase_performance", {}).items():
        lines.append(f"  {ph}: {pd.get('win_rate_pct',0)}% من {pd.get('total',0)} إشارة")

    lines += ["", DASHBOARD_URL]
    return "\n".join(lines)

def format_readiness_alert(ev: Dict) -> str:
    grade = ev.get("readiness_grade", "?")
    emoji = {"A":"✅","B":"🟢","C":"🟡","D":"🟠","F":"🔴"}.get(grade, "⚪")
    return (
        f"{emoji} <b>تقييم جاهزية النظام V20</b>\n"
        f"الدرجة: <b>{grade}</b> | الثقة: <b>{ev.get('confidence_score',0)}%</b>\n"
        f"الحالة: <b>{ev.get('system_state','?')}</b>\n"
        f"Win Rate: <b>{ev.get('signals',{}).get('win_rate_pct',0)}%</b>\n"
        f"متوسط RR: <b>{ev.get('signals',{}).get('avg_rr',0)}</b>\n"
        f"{ev.get('recommendation','')}"
    )

# ============================================================
# SCAN JOBS
# ============================================================

def hourly_scan_job(send: bool = False):
    scan = run_scan("HOURLY")
    save_scan_result("HOURLY", scan)
    for sig in scan.get("signals", []):
        record_virtual_signal(sig)
    evaluate_virtual_signals()
    evaluate_observations()
    check_decision_exits()
    save_combined_scan()
    if send and scan.get("signals"):
        tg_main_send(_format_scan_summary(scan, "🕐 Hourly Scan V20"))

def daily_scan_job(send: bool = True):
    scan = run_scan("DAILY")
    save_scan_result("DAILY", scan)
    for sig in scan.get("signals", []):
        record_virtual_signal(sig)
    evaluate_virtual_signals()
    evaluate_observations()
    check_decision_exits()
    save_combined_scan()
    if send:
        tg_main_send(format_daily_report_v20())

def learning_scan_job():
    hourly = latest_scan_result("HOURLY")
    daily  = latest_scan_result("DAILY")
    signals = []
    if hourly: signals.extend(hourly.get("signals", []))
    if daily:  signals.extend(daily.get("signals",  []))
    for sig in signals:
        record_virtual_signal(sig)
    evaluate_virtual_signals()
    evaluate_observations()

def _format_scan_summary(scan: Dict, title: str) -> str:
    lines = [
        f"<b>{title}</b>",
        f"Mode: {scan.get('mode', get_ai_mode())}",
        f"إشارات: <b>{scan.get('signals_count', 0)}</b>",
        "",
    ]
    for s in scan.get("signals", [])[:TELEGRAM_TOP_N]:
        entry = s.get("entry_zone") or ["-", "-"]
        ph    = s.get("market_phase", "")
        emoji = {"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔"}.get(ph,"⚪")
        lines.append(
            f"{emoji} <b>{s.get('symbol')}</b> {s.get('type')} | "
            f"Score:{s.get('score')} | "
            f"Entry:{entry[0]}-{entry[1]} | T1:{s.get('target1')} | "
            f"~{s.get('estimated_days','?')} يوم"
        )
    if not scan.get("signals"):
        lines.append("لا توجد إشارات شراء قوية — السوق يحتاج وقتاً")
    lines.extend(["", DASHBOARD_URL])
    return "\n".join(lines)

# ============================================================
# BATCH SCAN
# ============================================================

def get_batch_index() -> int:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO batch_scan_state(key,next_index,updated_at)
            VALUES('main',0,%s) ON CONFLICT(key) DO NOTHING
        """, (utc_now(),))
        cur.execute("SELECT next_index FROM batch_scan_state WHERE key='main'")
        row = cur.fetchone()
    return int(row[0]) if row else 0

def set_batch_index(i: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO batch_scan_state(key,next_index,updated_at)
            VALUES('main',%s,%s)
            ON CONFLICT(key) DO UPDATE SET next_index=EXCLUDED.next_index, updated_at=EXCLUDED.updated_at
        """, (i, utc_now()))

# ============================================================
# FAILURE MEMORY (fallback classify)
# ============================================================

def record_failure_memory(symbol, setup_type, failure_reason, loss_pct, lesson, payload=None):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_failure_memory
            (symbol,setup_type,failure_reason,loss_pct,market_state,lesson,created_at,payload)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
        """, (normalize_symbol(symbol), setup_type, failure_reason, loss_pct,
              "UNKNOWN", lesson, utc_now(), json.dumps(payload or {})))

# ============================================================
# API ROUTES
# ============================================================

@app.get("/")
def home():
    return {
        "ok": True, "version": "V20_INTELLIGENT_CORE",
        "mode": get_ai_mode(), "uae_now": uae_now_dt().isoformat(),
        "is_trading_day": is_uae_trading_day(), "is_market_time": is_uae_market_time(),
        "learning_age_days": round(learning_age_days(), 2),
        "learning_remaining_days": round(learning_remaining_days(), 2),
        "watchlist_count": len(WATCHLIST),
    }

@app.get("/api/health")
@app.get("/api/healthz")
def health():
    return {"ok": True, "version": "V20_INTELLIGENT_CORE", "mode": get_ai_mode()}

@app.get("/api/watchlist")
def watchlist_api():
    return {"ok": True, "count": len(WATCHLIST), "stocks": WATCHLIST}

@app.get("/api/system/mode")
def api_mode():
    return {"ok": True, "mode": get_ai_mode(),
            "learning_age_days": round(learning_age_days(), 2),
            "learning_remaining_days": round(learning_remaining_days(), 2)}

@app.get("/api/system/set-mode")
def api_set_mode(mode: str, secret: Optional[str] = None):
    if not cron_ok(secret):
        return {"ok": False, "error": "bad_secret"}
    mode = mode.upper().strip()
    if mode not in ["LEARNING", "PAPER", "LIVE"]:
        return {"ok": False, "error": "mode must be LEARNING, PAPER, LIVE"}
    set_setting("ai_mode", mode)
    return {"ok": True, "mode": mode}

# Webhooks
@app.post("/webhook/tradingview")
@app.post("/api/webhook/price-alert")
@app.get("/api/webhook/price-alert")
async def price_webhook(request: Request, secret: Optional[str] = None):
    try:
        sec = secret
        try:
            data = await request.json()
            if not sec: sec = data.get("secret")
        except Exception:
            data = dict(request.query_params)

        if sec != SECRET and sec != CRON_SECRET:
            return {"ok": False, "error": "bad_secret"}

        symbol = normalize_symbol(data.get("symbol") or data.get("ticker") or "")
        tf     = normalize_tf(data.get("timeframe") or data.get("interval") or "60")
        o = safe_float(data.get("open")   or data.get("o"))
        h = safe_float(data.get("high")   or data.get("h"))
        l = safe_float(data.get("low")    or data.get("l"))
        c = safe_float(data.get("close")  or data.get("price") or data.get("c"))
        v = safe_float(data.get("volume") or data.get("v"), 0)

        if not symbol:       return {"ok": False, "error": "missing_symbol"}
        if tf not in ["60", "1D"]: return {"ok": False, "error": "bad_timeframe", "tf": tf}
        if None in [o, h, l, c]:  return {"ok": False, "error": "missing_ohlc"}

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO candles(symbol,exchange,timeframe,bar_time,open,high,low,close,volume,received_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (symbol, data.get("exchange","TRADINGVIEW"), tf,
                  parse_bar_time(data.get("time") or data.get("timenow")),
                  o, h, l, c, v, utc_now()))
        return {"ok": True, "symbol": symbol, "timeframe": tf, "close": c}
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}

# Candle endpoints
@app.get("/api/candles/latest")
def latest_candles(symbol: Optional[str] = None, timeframe: Optional[str] = None, limit: int = 100):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q, params = "SELECT * FROM candles WHERE 1=1", []
        if symbol:    q += " AND symbol=%s";    params.append(normalize_symbol(symbol))
        if timeframe: q += " AND timeframe=%s"; params.append(normalize_tf(timeframe))
        q += " ORDER BY id DESC LIMIT %s"; params.append(limit)
        cur.execute(q, tuple(params))
        rows = cur.fetchall()
    return {"ok": True, "count": len(rows), "candles": rows}

@app.get("/api/watchlist/coverage")
@app.get("/api/coverage")
def coverage():
    out, ready_count = [], 0
    for s in WATCHLIST:
        h1 = len(get_candles(s, "60", 35))
        d1 = len(get_candles(s, "1D", 15))
        ready = h1 >= MIN_H1_CANDLES and d1 >= MIN_D1_CANDLES
        if ready: ready_count += 1
        out.append({"symbol": s, "h1_count": h1, "d1_count": d1, "ready": ready})
    return {"ok": True, "count": len(out), "ready_count": ready_count, "coverage": out}

# Analysis
@app.get("/api/ai/analyze")
@app.get("/api/analyze/{symbol}")
def api_analyze(symbol: str):
    symbol = normalize_symbol(symbol)
    sigs   = analyze_symbol(symbol, "ALL")
    locked = get_locked_decision(symbol)
    return {"ok": True, "symbol": symbol, "signals": sigs, "locked_decision": locked}

# Scans
@app.get("/api/ai/pro-scan")
def pro_scan(scan_type: str = "COMBINED", run: bool = False):
    scan_type = scan_type.upper()
    if run:
        data = run_scan(scan_type)
        save_scan_result(scan_type, data)
        return data
    data = latest_scan_result(scan_type)
    if not data:
        return {"ok": True, "message": "No scan yet. Use ?run=true", "signals": [], "coverage": []}
    return data

# Self evaluation
@app.get("/api/ai/self-evaluation")
def api_self_evaluation(secret: Optional[str] = None):
    return run_self_evaluation()

@app.get("/api/ai/readiness")
def api_readiness(secret: Optional[str] = None):
    return readiness_report()

# Decision locks
@app.get("/api/ai/decision-locks")
def api_decision_locks():
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM v20_decision_lock ORDER BY locked_at DESC")
        rows = cur.fetchall()
    return {"ok": True, "count": len(rows), "locks": rows}

@app.get("/api/ai/decision/{symbol}")
def api_decision(symbol: str):
    lock = get_locked_decision(normalize_symbol(symbol))
    return {"ok": True, "symbol": symbol, "decision": lock}

# Pattern stats
@app.get("/api/ai/pattern-stats")
def api_pattern_stats(symbol: Optional[str] = None):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q = """
            SELECT symbol, market_phase, setup_type, rsi_bucket,
                   COUNT(*) total,
                   SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) wins,
                   AVG(return_pct) avg_return
            FROM v20_pattern_learning
            WHERE 1=1
        """
        params = []
        if symbol:
            q += " AND symbol=%s"; params.append(normalize_symbol(symbol))
        q += " GROUP BY symbol,market_phase,setup_type,rsi_bucket ORDER BY total DESC LIMIT 100"
        cur.execute(q, params)
        rows = cur.fetchall()
    return {"ok": True, "count": len(rows), "patterns": rows}

# Signals
@app.get("/api/signals/open")
def get_open_signals():
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT symbol,signal_type,timeframe,action,price,target1,stop_loss,
                   score,strength,rr,risk_pct,market_phase,estimated_days,created_at,status
            FROM ai_virtual_signals WHERE status='OPEN' ORDER BY id DESC LIMIT 100
        """)
        rows = cur.fetchall()
    return {"ok": True, "count": len(rows), "signals": rows}

@app.get("/api/signals/completed")
def get_completed_signals():
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT symbol,signal_type,timeframe,action,price,target1,stop_loss,
                   score,strength,rr,outcome,outcome_at,max_high,min_low,
                   market_phase,estimated_days,created_at
            FROM ai_virtual_signals WHERE status='CLOSED' ORDER BY id DESC LIMIT 100
        """)
        rows = cur.fetchall()
    return {"ok": True, "count": len(rows), "signals": rows}

# Portfolio
@app.get("/api/portfolio")
def portfolio_list():
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM portfolio_positions WHERE status='OPEN' ORDER BY symbol")
        rows = cur.fetchall()
    return {"ok": True, "count": len(rows), "positions": rows}

@app.get("/api/portfolio/add")
def portfolio_add(secret: Optional[str]=None, symbol: str="", qty: float=0,
                  entry: float=0, position_type: str="HOLDING", notes: str=""):
    if not cron_ok(secret): return {"ok": False, "error": "bad_secret"}
    symbol = normalize_symbol(symbol)
    if not symbol or qty <= 0 or entry <= 0:
        return {"ok": False, "error": "symbol, qty, entry required"}
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO portfolio_positions(symbol,qty,entry_price,position_type,status,notes,created_at,updated_at)
            VALUES(%s,%s,%s,%s,'OPEN',%s,%s,%s)
            ON CONFLICT(symbol) DO UPDATE SET qty=EXCLUDED.qty, entry_price=EXCLUDED.entry_price,
                position_type=EXCLUDED.position_type, notes=EXCLUDED.notes, status='OPEN', updated_at=EXCLUDED.updated_at
        """, (symbol, qty, entry, position_type, notes, utc_now(), utc_now()))
    return {"ok": True, "symbol": symbol, "qty": qty, "entry": entry}

@app.get("/api/portfolio/monitor")
def portfolio_monitor(secret: Optional[str] = None):
    if secret is not None and not cron_ok(secret):
        return {"ok": False, "error": "bad_secret"}
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM portfolio_positions WHERE status='OPEN'")
        positions = cur.fetchall()

    out = []
    for p in positions:
        symbol = p["symbol"]; entry = float(p["entry_price"]); qty = float(p["qty"])
        candles = get_candles(symbol, "60", 5)
        last_price = float(candles[-1]["close"]) if candles else None
        sigs = analyze_symbol(symbol, "ALL")
        best = max(sigs, key=lambda x: x.get("rank_score",0), default=None)
        lock = get_locked_decision(symbol)

        pnl_pct = ((last_price - entry) / entry * 100) if last_price else None
        pnl_aed = ((last_price - entry) * qty) if last_price else None

        action, reason = "HOLD", "قرار مستقر"
        if lock and lock.get("decision") == "AVOID":
            action, reason = "EXIT_ALERT", "المرحلة تحولت لتصريف — اخرج"
        elif best and best.get("market_phase") in ["DISTRIBUTION", "MARKDOWN"]:
            action, reason = "EXIT_REVIEW", "مرحلة تصريف/هبوط"
        elif pnl_pct and pnl_pct <= -8:
            action, reason = "RISK_REVIEW", f"خسارة {round(pnl_pct,1)}% تجاوزت الحد"

        out.append({
            "symbol": symbol, "qty": qty, "entry_price": entry,
            "last_price": last_price,
            "pnl_pct": round(pnl_pct, 2) if pnl_pct else None,
            "pnl_aed": round(pnl_aed, 2) if pnl_aed else None,
            "action": action, "reason": reason,
            "market_phase": best.get("market_phase") if best else None,
            "signal_strength": best.get("strength") if best else None,
            "score": best.get("score") if best else None,
            "locked_decision": lock.get("decision") if lock else None,
        })
    return {"ok": True, "count": len(out), "positions": out}

# Reports
@app.get("/api/reports/daily")
def daily_report_api(secret: Optional[str]=None, send: bool=False):
    text = format_daily_report_v20()
    sent = tg_main_send(text) if send else None
    return {"ok": True, "sent": send, "telegram": sent, "report": text}

@app.get("/api/reports/weekly")
def weekly_report_api(secret: Optional[str]=None, send: bool=False):
    text = format_weekly_report_v20()
    sent = tg_main_send(text) if send else None
    return {"ok": True, "sent": send, "telegram": sent, "report": text}

# Observations
@app.get("/api/ai/observations")
def api_observations(limit: int = 100):
    evaluated = evaluate_observations()
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM ai_observations ORDER BY id DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
    return {"ok": True, "evaluated_now": evaluated, "count": len(rows), "observations": rows}

@app.get("/api/ai/learning-scan")
def api_learning_scan():
    hourly = latest_scan_result("HOURLY")
    daily  = latest_scan_result("DAILY")
    signals = []
    if hourly: signals.extend(hourly.get("signals", []))
    if daily:  signals.extend(daily.get("signals",  []))
    created, evaluated = [], []
    for sig in signals:
        if record_virtual_signal(sig):
            created.append({"symbol": sig["symbol"], "type": sig["type"]})
    evaluated = evaluate_virtual_signals()
    obs_ev    = evaluate_observations()
    return {
        "ok": True, "mode": get_ai_mode(),
        "created_count": len(created), "evaluated_count": len(evaluated),
        "observations_evaluated": len(obs_ev)
    }

# Telegram
@app.get("/api/ai/test-telegram")
def test_telegram():
    return tg_main_send("✅ UAE AI V20 — اختبار Telegram ناجح!")

@app.get("/api/telegram/set-webhook")
def set_webhook():
    url = f"{BASE_URL}/api/telegram/webhook/{TELEGRAM_WEBHOOK_SECRET}"
    return tg_api("setWebhook", {"url": url})

@app.post("/api/telegram/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != TELEGRAM_WEBHOOK_SECRET:
        return {"ok": False, "error": "unauthorized"}
    data = await request.json()
    try:
        if "message" in data:
            msg    = data["message"]
            chat_id= msg["chat"]["id"]
            text   = msg.get("text", "").strip()
            upper  = text.upper()

            if upper in ["READINESS", "جاهزية", "STATUS"]:
                ev = run_self_evaluation()
                return tg_send(chat_id, format_readiness_alert(ev))

            if upper in ["DAILY", "تقرير", "تقرير يومي"]:
                return tg_send(chat_id, format_daily_report_v20())

            if upper in ["WEEKLY", "أسبوعي", "اسبوعي"]:
                return tg_send(chat_id, format_weekly_report_v20())

            if upper.startswith("تحليل") or upper.startswith("ANALYZE"):
                parts = upper.split()
                if len(parts) >= 2:
                    sym  = normalize_symbol(parts[1])
                    sigs = analyze_symbol(sym, "ALL")
                    if sigs:
                        best = max(sigs, key=lambda x: x["score"])
                        return tg_send(chat_id, format_signal_v20(best), signal_keyboard(sym))
                return tg_send(chat_id, "لا توجد بيانات كافية بعد")

            sym = normalize_symbol(upper.split()[0] if upper.split() else "")
            if sym in WATCHLIST:
                sigs = analyze_symbol(sym, "ALL")
                if sigs:
                    best = max(sigs, key=lambda x: x["score"])
                    lock = get_locked_decision(sym)
                    reply = format_signal_v20(best)
                    if lock:
                        reply += f"\n\n🔒 القرار المقفل: {lock.get('decision')} | {lock.get('market_phase')}"
                    return tg_send(chat_id, reply, signal_keyboard(sym))
                return tg_send(chat_id, "لا توجد بيانات كافية لهذا السهم")

            return tg_send(chat_id,
                "أرسل اسم السهم (مثال: EMAAR) أو:\n"
                "READINESS — جاهزية النظام\n"
                "DAILY — التقرير اليومي\n"
                "WEEKLY — التقرير الأسبوعي\n"
                "ANALYZE EMAAR — تحليل سهم")
    except Exception as e:
        print(f"Telegram webhook error: {e}")
    return {"ok": True}

@app.get("/api/ai/send-alerts")
def send_alerts(secret: Optional[str] = None, force: bool = False):
    if secret is not None and not cron_ok(secret):
        return {"ok": False, "error": "bad_secret"}
    scan = latest_scan_result("COMBINED")
    if not scan: return {"ok": False, "message": "No scan yet"}
    signals = scan.get("signals", [])
    sent, skipped, errors = [], [], []

    with get_db() as conn:
        cur = conn.cursor()
        for sig in signals[:5]:
            try:
                symbol = sig.get("symbol","")
                key = f"{symbol}-{sig.get('type')}-{sig.get('price')}-{sig.get('target1')}"
                if not force:
                    cur.execute("SELECT id FROM ai_alerts_log WHERE alert_key=%s", (key,))
                    if cur.fetchone():
                        skipped.append(symbol); continue
                tg_main_send(format_signal_v20(sig), signal_keyboard(symbol))
                cur.execute("""
                    INSERT INTO ai_alerts_log(alert_key,symbol,signal_type,created_at,payload)
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                """, (key, symbol, sig.get("type"), utc_now(), json.dumps(sig)))
                sent.append(symbol)
            except Exception as e:
                errors.append({"symbol": sig.get("symbol",""), "error": str(e)})

    return {"ok": True, "sent": sent, "skipped": skipped, "errors": errors}

# ============================================================
# CRON ENDPOINTS — جدول التشغيل المقترح
# ============================================================
# كل ساعة (أثناء ساعات السوق):  /api/cron/hourly-scan
# مرة يومياً (08:30 UAE):        /api/cron/daily-scan
# مرة يومياً (15:30 UAE):        /api/cron/end-of-day
# كل يومين (أي وقت):            /api/cron/weekly-report
# كل يوم تداول (07:00 UAE):      /api/cron/self-evaluation
# كل يوم تداول (16:00 UAE):      /api/cron/learning-scan

@app.get("/api/cron/hourly-scan")
def cron_hourly_scan(secret: Optional[str] = None, send: bool = True):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    run_background_job(hourly_scan_job, send)
    return {"ok": True, "started": True, "job": "HOURLY_SCAN_V20"}

@app.get("/api/cron/daily-scan")
def cron_daily_scan(secret: Optional[str] = None, send: bool = True):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    if not is_uae_trading_day():
        return {"ok": True, "skipped": True, "reason": "UAE weekend"}
    run_background_job(daily_scan_job, send)
    return {"ok": True, "started": True, "job": "DAILY_SCAN_V20"}

@app.get("/api/cron/learning-scan")
def cron_learning_scan(secret: Optional[str] = None):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    run_background_job(learning_scan_job)
    return {"ok": True, "started": True, "job": "LEARNING_SCAN_V20"}

@app.get("/api/cron/self-evaluation")
def cron_self_evaluation(secret: Optional[str] = None, send: bool = True):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    ev = run_self_evaluation()
    if send:
        tg_main_send(format_readiness_alert(ev))
    return {"ok": True, "evaluation": ev}

@app.get("/api/cron/end-of-day")
def cron_end_of_day(secret: Optional[str] = None):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    if not is_uae_trading_day():
        return {"ok": True, "skipped": True, "reason": "UAE weekend"}
    try:
        scan = run_scan("COMBINED")
        save_scan_result("COMBINED", scan)
        for sig in scan.get("signals", []):
            record_virtual_signal(sig)
        ev = evaluate_virtual_signals()
        obs = evaluate_observations()
        exits = check_decision_exits()
        self_ev = run_self_evaluation()
        tg_main_send(format_daily_report_v20())
        return {
            "ok": True, "scan_signals": scan.get("signals_count", 0),
            "evaluated": len(ev), "observations": len(obs),
            "decision_exits": len(exits),
            "confidence": self_ev.get("confidence_score", 0),
            "state": self_ev.get("system_state"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}

@app.get("/api/cron/daily-report")
def cron_daily_report(secret: Optional[str] = None):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    if not is_uae_trading_day():
        return {"ok": True, "skipped": True, "reason": "UAE weekend"}
    text = format_daily_report_v20()
    sent = tg_main_send(text)
    return {"ok": True, "sent": True, "telegram": sent}

@app.get("/api/cron/weekly-report")
def cron_weekly_report(secret: Optional[str] = None):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    text = format_weekly_report_v20()
    sent = tg_main_send(text)
    return {"ok": True, "sent": True, "telegram": sent}

@app.get("/api/cron/portfolio-monitor")
def cron_portfolio_monitor(secret: Optional[str] = None):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    data = portfolio_monitor(secret)
    positions = data.get("positions", [])
    if not positions: return {"ok": True, "message": "No positions"}
    lines = ["<b>📊 Portfolio Monitor V20</b>", ""]
    for p in positions:
        lines.append(
            f"{p['symbol']} | {p['action']} | "
            f"PnL {p['pnl_pct']}% | المرحلة: {p.get('market_phase','?')} | {p['reason']}"
        )
    tg_main_send("\n".join(lines))
    return {"ok": True, "count": len(positions)}

@app.get("/api/cron/batch-scan")
def batch_scan(secret: Optional[str] = None, limit: int = 10, send: bool = False):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    if not is_uae_trading_day(): return {"ok": True, "skipped": True}

    total  = len(WATCHLIST)
    start  = get_batch_index()
    end    = min(start + limit, total)
    batch  = WATCHLIST[start:end]
    set_batch_index(0 if end >= total else end)

    ranked, batch_signals, coverage_rows = [], [], []
    candle_cache = get_all_candles_for_scan(220)

    for symbol in batch:
        try:
            sigs = analyze_symbol_from_cache(symbol, "ALL", candle_cache)
            best = max(sigs, key=lambda x: x.get("rank_score", 0), default=None)
            if best:
                ranked.append(best)
                if best.get("model_action") == "BUY":
                    batch_signals.append(best)
                update_decision_lock(symbol, best)
                coverage_rows.append({
                    "symbol": symbol, "action": classify_action(best),
                    "score": best.get("score"), "market_phase": best.get("market_phase"),
                    "estimated_days": best.get("estimated_days"),
                })
        except Exception as e:
            coverage_rows.append({"symbol": symbol, "error": str(e)})

    payload = {
        "ok": True, "version": "V20", "scan_type": "BATCH",
        "created_at": utc_now(), "batch_start": start, "batch_end": end,
        "signals_count": len(batch_signals), "signals": batch_signals[:10],
        "coverage": coverage_rows
    }
    save_scan_result("HOURLY", payload)
    save_combined_scan()

    for sig in batch_signals:
        record_virtual_signal(sig)
    evaluate_virtual_signals()
    evaluate_observations()

    if send and batch_signals:
        tg_main_send(_format_scan_summary({"signals": batch_signals, "signals_count": len(batch_signals), "mode": get_ai_mode()}, "📦 Batch Scan"))

    return payload

@app.get("/api/cron/send-alerts")
def cron_send_alerts(secret: Optional[str] = None, force: bool = False):
    if not cron_ok(secret): return {"ok": False, "error": "bad_cron_secret"}
    return send_alerts(secret=secret, force=force)

@app.get("/api/ai/reset-alerts")
def reset_alerts(secret: Optional[str] = None):
    if not cron_ok(secret): return {"ok": False, "error": "bad_secret"}
    with get_db() as conn:
        conn.cursor().execute("DELETE FROM ai_alerts_log")
    return {"ok": True}

# ============================================================
# DASHBOARD
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    scan = latest_scan_result("COMBINED") or {"signals": [], "coverage": [], "ranked": [], "signals_count": 0}
    ev   = run_self_evaluation()
    sigs = ev.get("signals", {})

    grade_color = {"A": "#22c55e", "B": "#86efac", "C": "#fbbf24", "D": "#f97316", "F": "#ef4444"}.get(
        ev.get("readiness_grade", "F"), "#9ca3af"
    )

    phase_colors = {
        "ACCUMULATION": "#3b82f6", "MARKUP": "#22c55e",
        "DISTRIBUTION": "#ef4444", "MARKDOWN": "#7f1d1d", "NEUTRAL": "#6b7280"
    }

    signal_rows = ""
    for s in scan.get("ranked", [])[:30]:
        ph      = s.get("market_phase", "NEUTRAL")
        ph_col  = phase_colors.get(ph, "#6b7280")
        rev     = s.get("reversal_signals") or []
        rev_icon= "⚠️" if rev else ""
        signal_rows += f"""
        <tr>
            <td><b>{esc(s.get('symbol',''))}</b></td>
            <td>{esc(s.get('type',''))}</td>
            <td style="color:{ph_col}">{esc(ph)}</td>
            <td><b>{esc(s.get('display_action',''))}</b></td>
            <td>{esc(s.get('strength',''))}</td>
            <td>{esc(s.get('score',''))}</td>
            <td>{esc(s.get('price',''))}</td>
            <td>{esc(s.get('stop_loss',''))}</td>
            <td>{esc(s.get('target1',''))}</td>
            <td>{esc(s.get('estimated_days',''))} يوم</td>
            <td>{esc(s.get('rr',''))}</td>
            <td>{esc(s.get('cmf',''))}</td>
            <td>{rev_icon} {esc(s.get('ai_comment',''))}</td>
        </tr>"""

    lock_rows = ""
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM v20_decision_lock WHERE status='LOCKED' ORDER BY locked_at DESC")
            locks = cur.fetchall()
        for lk in locks:
            ph    = lk.get("market_phase","")
            ph_col= phase_colors.get(ph, "#6b7280")
            lock_rows += f"""
            <tr>
                <td><b>{esc(lk.get('symbol',''))}</b></td>
                <td>{esc(lk.get('decision',''))}</td>
                <td style="color:{ph_col}">{esc(ph)}</td>
                <td>{esc(lk.get('score',''))}</td>
                <td>{esc(lk.get('entry_price',''))}</td>
                <td>{esc(lk.get('stop_loss',''))}</td>
                <td>{esc(lk.get('target1',''))}</td>
                <td>{esc(lk.get('estimated_days',''))} يوم</td>
                <td>{esc(lk.get('lock_reason',''))}</td>
                <td>{esc(lk.get('locked_at','')[:16])}</td>
            </tr>"""
    except Exception:
        pass

    return f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>UAE PRO AI V20</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background:#0f172a; color:#e2e8f0; padding:20px; margin:0; }}
        .card {{ background:#1e293b; padding:20px; border-radius:16px; margin-bottom:20px; border:1px solid #334155; }}
        .grade {{ font-size:48px; font-weight:bold; color:{grade_color}; }}
        .stat {{ display:inline-block; margin:8px 16px; text-align:center; }}
        .stat-val {{ font-size:24px; font-weight:bold; color:#a78bfa; }}
        .stat-lbl {{ font-size:12px; color:#94a3b8; }}
        table {{ width:100%; border-collapse:collapse; margin:10px 0; font-size:13px; }}
        th {{ background:#334155; padding:10px 8px; text-align:right; color:#94a3b8; white-space:nowrap; }}
        td {{ border-bottom:1px solid #1e293b; padding:8px; white-space:nowrap; }}
        tr:hover td {{ background:#1e293b; }}
        h1 {{ color:#a78bfa; margin:0 0 4px; }}
        h2 {{ color:#60a5fa; margin:20px 0 10px; font-size:16px; }}
        .badge {{ display:inline-block; padding:2px 8px; border-radius:9999px; font-size:11px; font-weight:bold; }}
        .badge-green {{ background:#166534; color:#86efac; }}
        .badge-red   {{ background:#7f1d1d; color:#fca5a5; }}
        .badge-blue  {{ background:#1e3a5f; color:#93c5fd; }}
        .rec {{ background:#1e3a5f; border-left:4px solid #3b82f6; padding:12px; border-radius:8px; margin:10px 0; }}
    </style>
</head>
<body>
<h1>🤖 UAE PRO AI V20 — Intelligent Core</h1>
<div class="card">
    <div class="grade">{ev.get('readiness_grade','?')}</div>
    <div class="rec">{ev.get('recommendation','')}</div>
    <div>
        <div class="stat"><div class="stat-val">{ev.get('confidence_score',0)}%</div><div class="stat-lbl">الثقة</div></div>
        <div class="stat"><div class="stat-val">{sigs.get('win_rate_pct',0)}%</div><div class="stat-lbl">Win Rate</div></div>
        <div class="stat"><div class="stat-val">{sigs.get('avg_rr',0)}</div><div class="stat-lbl">Avg RR</div></div>
        <div class="stat"><div class="stat-val">{sigs.get('avg_return_pct',0)}%</div><div class="stat-lbl">Avg Return</div></div>
        <div class="stat"><div class="stat-val">{sigs.get('evaluated',0)}</div><div class="stat-lbl">Evaluated</div></div>
        <div class="stat"><div class="stat-val">{ev.get('learning_progress_pct',0)}%</div><div class="stat-lbl">Learning Progress</div></div>
        <div class="stat"><div class="stat-val">{scan.get('signals_count',0)}</div><div class="stat-lbl">BUY Signals</div></div>
        <div class="stat"><div class="stat-val">{scan.get('created_at','')[:16]}</div><div class="stat-lbl">Last Scan</div></div>
    </div>
</div>

<h2>🔒 القرارات المقفلة (Decision Locks)</h2>
<div class="card">
<table>
    <tr><th>السهم</th><th>القرار</th><th>المرحلة</th><th>Score</th><th>الدخول</th><th>Stop</th><th>T1</th><th>الأيام</th><th>السبب</th><th>تاريخ القفل</th></tr>
    {lock_rows if lock_rows else "<tr><td colspan='10' style='text-align:center'>لا توجد قرارات مقفلة</td></tr>"}
</table>
</div>

<h2>📊 ترتيب الأسهم</h2>
<div class="card">
<table>
    <tr><th>السهم</th><th>النوع</th><th>المرحلة</th><th>القرار</th><th>القوة</th><th>Score</th><th>السعر</th><th>Stop</th><th>T1</th><th>الأيام</th><th>RR</th><th>CMF</th><th>التعليق</th></tr>
    {signal_rows if signal_rows else "<tr><td colspan='13' style='text-align:center'>لا توجد إشارات</td></tr>"}
</table>
</div>
</body>
</html>"""
