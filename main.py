"""
UAE Market PRO AI — V20 INTELLIGENT CORE
V20.1 FIXES:
- get_latest_price(): أحدث سعر من H1 أو 1D أيهما أحدث
- portfolio_monitor: يستخدم get_latest_price
- dashboard: أسعار صحيحة + قسم بورتفوليو + auto-refresh
- build_signal_v20: لا BUY إذا البيانات قديمة
- check_decision_exits: يستخدم get_latest_price
"""

import os, json, math, traceback, requests, psycopg2, psycopg2.extras
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from threading import Thread
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="UAE Market PRO AI V20")

DATABASE_URL            = os.getenv("DATABASE_URL")
SECRET                  = os.getenv("SECRET", "abc123")
CRON_SECRET             = os.getenv("CRON_SECRET", "cron123")
TELEGRAM_BOT_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID        = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "tgsecret123")
BASE_URL                = os.getenv("API_BASE", "https://uae-market-production.up.railway.app").rstrip("/")
DASHBOARD_URL           = os.getenv("DASHBOARD_URL", f"{BASE_URL}/dashboard")
AI_MODE                 = os.getenv("AI_MODE", "PAPER").upper().strip()
LEARNING_DAYS           = int(os.getenv("LEARNING_DAYS", "21"))
CAPITAL                 = float(os.getenv("CAPITAL", "200000"))
TELEGRAM_TOP_N          = int(os.getenv("TELEGRAM_TOP_N", "5"))
MIN_H1_CANDLES          = int(os.getenv("MIN_H1_CANDLES", "30"))
MIN_D1_CANDLES          = int(os.getenv("MIN_D1_CANDLES", "10"))
SCAN_MAX_ERRORS         = int(os.getenv("SCAN_MAX_ERRORS", "100"))
DECISION_MIN_HOLD_DAYS  = int(os.getenv("DECISION_MIN_HOLD_DAYS", "3"))
DECISION_CHANGE_DELTA   = float(os.getenv("DECISION_CHANGE_DELTA", "18"))
MIN_SCORE_BUY           = float(os.getenv("MIN_SCORE_BUY", "72"))
MIN_RR_BUY              = float(os.getenv("MIN_RR_BUY", "1.2"))
MIN_CONFIDENCE_LIVE     = float(os.getenv("MIN_CONFIDENCE_LIVE", "70"))
OBSERVATION_TARGET_PCT  = float(os.getenv("OBSERVATION_TARGET_PCT", "3.0"))
OBSERVATION_DROP_PCT    = float(os.getenv("OBSERVATION_DROP_PCT", "2.0"))
UAE_TZ_OFFSET           = timedelta(hours=4)
MAX_CANDLE_AGE_HOURS    = int(os.getenv("MAX_CANDLE_AGE_HOURS", "72"))

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

def run_background_job(f, *a, **kw):
    def w():
        try: f(*a, **kw)
        except: print(traceback.format_exc())
    Thread(target=w, daemon=True).start()

def utc_now_dt(): return datetime.now(timezone.utc)
def utc_now(): return utc_now_dt().isoformat()
def uae_now_dt(): return utc_now_dt() + UAE_TZ_OFFSET

def is_uae_trading_day(dt=None):
    return (dt or uae_now_dt()).weekday() in [0,1,2,3,4]

def is_uae_market_time(dt=None):
    d = dt or uae_now_dt()
    return is_uae_trading_day(d) and 10 <= d.hour < 15

def business_days_between(start, end):
    if not start or not end or start > end: return 0
    count = 0
    cur = (start+UAE_TZ_OFFSET).date() if start.tzinfo else start.date()
    ed  = (end+UAE_TZ_OFFSET).date()   if end.tzinfo   else end.date()
    while cur <= ed:
        if cur.weekday() in [0,1,2,3,4]: count += 1
        cur += timedelta(days=1)
    return count

def parse_dt(value):
    try:
        if not value: return None
        dt = datetime.fromisoformat(str(value).replace("Z","+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except: return None

def parse_bar_time(value):
    if value is None: return utc_now()
    s = str(value).strip()
    try:
        if s.isdigit():
            n = int(s)
            return datetime.fromtimestamp(n/1000 if n>10_000_000_000 else n, timezone.utc).isoformat()
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).isoformat()
    except: return s

def candle_age_hours(candle):
    bt = parse_dt(str(candle.get("bar_time") or candle.get("received_at") or ""))
    if not bt: return 9999
    return (utc_now_dt() - bt).total_seconds() / 3600

def normalize_symbol(s):
    s = str(s or "").upper().replace(" ","").strip()
    if ":" in s: s = s.split(":")[-1]
    if "." in s: s = s.split(".")[0]
    return s

def normalize_tf(tf):
    t = str(tf or "").strip().upper()
    if t in ["1H","60M","H1","60"]: return "60"
    if t in ["D","1D","DAILY","DAY","1440","1DAY","W","1W","WEEK","1"]: return "1D"
    return t

def is_daily_exchange(exchange):
    return any(x in str(exchange or "").upper() for x in ["DLY","DAILY","DAY"])

def safe_float(v, default=None):
    try:
        if v is None or v == "": return default
        x = float(str(v).replace(",",""))
        return default if (math.isnan(x) or math.isinf(x)) else x
    except: return default

def esc(x): return str(x).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def cron_ok(s): return s == CRON_SECRET

@contextmanager
def get_db():
    if not DATABASE_URL: raise RuntimeError("DATABASE_URL missing")
    conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=10)
    try: yield conn; conn.commit()
    except: conn.rollback(); raise
    finally: conn.close()

def init_db():
    with get_db() as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS system_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT NOT NULL)")
        c.execute("""CREATE TABLE IF NOT EXISTS candles (
            id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, exchange TEXT, timeframe TEXT NOT NULL,
            bar_time TEXT NOT NULL, open DOUBLE PRECISION, high DOUBLE PRECISION,
            low DOUBLE PRECISION, close DOUBLE PRECISION, volume DOUBLE PRECISION, received_at TEXT NOT NULL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_id ON candles(symbol,timeframe,id DESC)")
        c.execute("""CREATE TABLE IF NOT EXISTS v20_pattern_learning (
            id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, setup_type TEXT NOT NULL,
            market_phase TEXT, rsi_bucket TEXT, volume_bucket TEXT, trend_alignment TEXT,
            outcome TEXT, return_pct DOUBLE PRECISION, score_at_entry DOUBLE PRECISION,
            rr_at_entry DOUBLE PRECISION, created_at TEXT NOT NULL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_v20_pattern_symbol ON v20_pattern_learning(symbol,setup_type)")
        c.execute("""CREATE TABLE IF NOT EXISTS v20_decision_lock (
            symbol TEXT PRIMARY KEY, decision TEXT NOT NULL, confidence DOUBLE PRECISION,
            score DOUBLE PRECISION, entry_price DOUBLE PRECISION, entry_low DOUBLE PRECISION,
            entry_high DOUBLE PRECISION, stop_loss DOUBLE PRECISION, target1 DOUBLE PRECISION,
            target2 DOUBLE PRECISION, target3 DOUBLE PRECISION, estimated_days INTEGER,
            market_phase TEXT, signal_type TEXT, lock_reason TEXT, locked_at TEXT NOT NULL,
            last_checked_at TEXT, status TEXT DEFAULT 'LOCKED', unlock_reason TEXT, payload TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS v20_reversal_alerts (
            id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, alert_type TEXT NOT NULL,
            severity TEXT, price DOUBLE PRECISION, details TEXT, created_at TEXT NOT NULL,
            sent_telegram BOOLEAN DEFAULT FALSE)""")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_scan_results (
            id SERIAL PRIMARY KEY, scan_type TEXT NOT NULL, mode TEXT NOT NULL,
            created_at TEXT NOT NULL, watchlist_count INTEGER, scanned_count INTEGER,
            signals_count INTEGER, payload TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ai_scan_results_type_id ON ai_scan_results(scan_type,id DESC)")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_alerts_log (
            id SERIAL PRIMARY KEY, alert_key TEXT UNIQUE NOT NULL,
            symbol TEXT, signal_type TEXT, created_at TEXT NOT NULL, payload TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_virtual_signals (
            id SERIAL PRIMARY KEY, signal_key TEXT UNIQUE NOT NULL, mode TEXT NOT NULL,
            symbol TEXT NOT NULL, signal_type TEXT, timeframe TEXT, action TEXT,
            price DOUBLE PRECISION, entry_low DOUBLE PRECISION, entry_high DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION, target1 DOUBLE PRECISION, target2 DOUBLE PRECISION,
            target3 DOUBLE PRECISION, score DOUBLE PRECISION, strength TEXT, rr DOUBLE PRECISION,
            risk_pct DOUBLE PRECISION, target_pct DOUBLE PRECISION, max_hold_days INTEGER,
            estimated_days INTEGER, market_phase TEXT, created_at TEXT NOT NULL,
            status TEXT NOT NULL, outcome TEXT, outcome_at TEXT, max_high DOUBLE PRECISION,
            min_low DOUBLE PRECISION, bars_checked INTEGER DEFAULT 0, payload TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_observations (
            id SERIAL PRIMARY KEY, obs_key TEXT UNIQUE NOT NULL, symbol TEXT NOT NULL,
            scan_type TEXT, timeframe TEXT, action TEXT, model_action TEXT, strength TEXT,
            score DOUBLE PRECISION, rank_score DOUBLE PRECISION, price DOUBLE PRECISION,
            observed_at TEXT NOT NULL, status TEXT NOT NULL, outcome TEXT, outcome_at TEXT,
            max_high DOUBLE PRECISION, min_low DOUBLE PRECISION, return_pct DOUBLE PRECISION, payload TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_learning_stats (
            symbol TEXT PRIMARY KEY, trades_count INTEGER DEFAULT 0, wins_count INTEGER DEFAULT 0,
            losses_count INTEGER DEFAULT 0, virtual_short_count INTEGER DEFAULT 0,
            virtual_short_wins INTEGER DEFAULT 0, virtual_short_losses INTEGER DEFAULT 0,
            virtual_long_count INTEGER DEFAULT 0, virtual_long_wins INTEGER DEFAULT 0,
            virtual_long_losses INTEGER DEFAULT 0, avg_return_pct DOUBLE PRECISION DEFAULT 0,
            score_adjustment DOUBLE PRECISION DEFAULT 0, pattern_win_rate DOUBLE PRECISION DEFAULT 0,
            updated_at TEXT NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_failure_memory (
            id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, setup_type TEXT,
            failure_reason TEXT, loss_pct DOUBLE PRECISION, market_state TEXT,
            lesson TEXT, created_at TEXT NOT NULL, payload TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_self_evaluation (
            id SERIAL PRIMARY KEY, created_at TEXT NOT NULL, system_state TEXT,
            confidence_score DOUBLE PRECISION, ready_for_trading BOOLEAN, win_rate DOUBLE PRECISION,
            avg_return_pct DOUBLE PRECISION, rr_quality DOUBLE PRECISION, lessons_count INTEGER,
            pattern_quality DOUBLE PRECISION, recommendation TEXT, payload TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS portfolio_positions (
            id SERIAL PRIMARY KEY, symbol TEXT UNIQUE NOT NULL, qty DOUBLE PRECISION NOT NULL,
            entry_price DOUBLE PRECISION NOT NULL, position_type TEXT DEFAULT 'HOLDING',
            status TEXT DEFAULT 'OPEN', notes TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS batch_scan_state (
            key TEXT PRIMARY KEY, next_index INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS portfolio_trades (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            qty DOUBLE PRECISION NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            pnl_pct DOUBLE PRECISION,
            pnl_aed DOUBLE PRECISION,
            entry_price DOUBLE PRECISION,
            notes TEXT,
            created_at TEXT NOT NULL)""")

        c.execute("""CREATE TABLE IF NOT EXISTS v15_active_thesis (
            id SERIAL PRIMARY KEY, symbol TEXT UNIQUE NOT NULL, status TEXT DEFAULT 'ACTIVE',
            first_created_at TEXT NOT NULL, last_updated_at TEXT NOT NULL, trend_phase TEXT,
            decision TEXT, confidence TEXT, score DOUBLE PRECISION, entry_price DOUBLE PRECISION,
            ideal_entry DOUBLE PRECISION, aggressive_entry DOUBLE PRECISION,
            safe_entry DOUBLE PRECISION, invalidation DOUBLE PRECISION,
            target_base DOUBLE PRECISION, target_bull DOUBLE PRECISION,
            expected_move_pct DOUBLE PRECISION, expected_horizon TEXT,
            hold_days INTEGER DEFAULT 0, min_hold_days INTEGER DEFAULT 7,
            thesis_strength DOUBLE PRECISION DEFAULT 0, last_price DOUBLE PRECISION,
            notes TEXT, payload TEXT)""")

def get_setting(key, default=None):
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT value FROM system_settings WHERE key=%s",(key,))
        r = c.fetchone()
    return r["value"] if r else default

def set_setting(key, value):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO system_settings(key,value,updated_at) VALUES(%s,%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at",(key,value,utc_now()))

def ensure_learning_start():
    if not get_setting("learning_started_at"): set_setting("learning_started_at",utc_now())

def get_ai_mode():
    return os.getenv("AI_MODE", get_setting("ai_mode", AI_MODE) or "PAPER").upper().strip()

def learning_age_days():
    s = parse_dt(get_setting("learning_started_at"))
    return max(0, business_days_between(s, utc_now_dt())-1) if s else 0

def learning_remaining_days():
    return max(0, LEARNING_DAYS - learning_age_days())

@app.on_event("startup")
def startup(): init_db(); ensure_learning_start()

# ── CANDLES ──────────────────────────────────────────────────

def get_candles(symbol, timeframe, limit=220):
    tf = normalize_tf(timeframe)
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM candles WHERE symbol=%s AND timeframe=%s ORDER BY id DESC LIMIT %s",(normalize_symbol(symbol),tf,limit))
        return list(reversed(c.fetchall()))

def get_all_candles_for_scan(limit=220):
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("""SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY symbol,timeframe ORDER BY id DESC) AS rn
            FROM candles WHERE symbol=ANY(%s) AND timeframe IN ('60','1D')) x
            WHERE rn<=%s ORDER BY symbol,timeframe,id ASC""",(WATCHLIST,limit))
        rows = c.fetchall()
    data = {}
    for r in rows:
        s=normalize_symbol(r["symbol"]); tf=normalize_tf(r["timeframe"])
        data.setdefault(s,{"60":[],"1D":[]}); data[s][tf].append(r)
    return data

# ── FIX V20.1: LATEST PRICE ──────────────────────────────────

def get_latest_price(symbol):
    """أحدث سعر من H1 أو 1D — أيهما أحدث. Returns (price, tf, bar_time)"""
    symbol = normalize_symbol(symbol)
    h1 = get_candles(symbol,"60",3); d1 = get_candles(symbol,"1D",3)
    h = h1[-1] if h1 else None; d = d1[-1] if d1 else None
    if not h and not d: return None,None,None
    if h and d:
        ht = parse_dt(str(h.get("bar_time") or h.get("received_at") or ""))
        dt_ = parse_dt(str(d.get("bar_time") or d.get("received_at") or ""))
        if ht and dt_:
            if ht >= dt_: return safe_float(h["close"]),"H1",str(h.get("bar_time",""))
            else:         return safe_float(d["close"]),"1D",str(d.get("bar_time",""))
        if ht: return safe_float(h["close"]),"H1",str(h.get("bar_time",""))
        return safe_float(d["close"]),"1D",str(d.get("bar_time",""))
    if h: return safe_float(h["close"]),"H1",str(h.get("bar_time",""))
    return safe_float(d["close"]),"1D",str(d.get("bar_time",""))

def get_latest_price_from_cache(symbol, cache):
    symbol = normalize_symbol(symbol)
    h1 = cache.get(symbol,{}).get("60",[]); d1 = cache.get(symbol,{}).get("1D",[])
    h = h1[-1] if h1 else None; d = d1[-1] if d1 else None
    if not h and not d: return None,None,None
    if h and d:
        ht = parse_dt(str(h.get("bar_time") or h.get("received_at") or ""))
        dt_ = parse_dt(str(d.get("bar_time") or d.get("received_at") or ""))
        if ht and dt_:
            if ht >= dt_: return safe_float(h["close"]),"H1",str(h.get("bar_time",""))
            else:         return safe_float(d["close"]),"1D",str(d.get("bar_time",""))
        if ht: return safe_float(h["close"]),"H1",str(h.get("bar_time",""))
        return safe_float(d["close"]),"1D",str(d.get("bar_time",""))
    if h: return safe_float(h["close"]),"H1",str(h.get("bar_time",""))
    return safe_float(d["close"]),"1D",str(d.get("bar_time",""))

# ── INDICATORS ───────────────────────────────────────────────

def sma(values,n):
    v=[x for x in values if x is not None]
    return sum(v[-n:])/n if len(v)>=n else None

def ema(values,n):
    v=[x for x in values if x is not None]
    if len(v)<n: return None
    k=2/(n+1); e=sum(v[:n])/n
    for p in v[n:]: e=p*k+e*(1-k)
    return e

def rsi(values,n=14):
    v=[x for x in values if x is not None]
    if len(v)<n+1: return None
    g,l=[],[]
    for i in range(1,len(v)):
        d=v[i]-v[i-1]; g.append(max(d,0)); l.append(abs(min(d,0)))
    ag=sum(g[-n:])/n; al=sum(l[-n:])/n
    return 100 if al==0 else 100-(100/(1+ag/al))

def atr(candles,n=14):
    if len(candles)<n+1: return None
    trs=[]
    for i in range(1,len(candles)):
        h=safe_float(candles[i]["high"]); l=safe_float(candles[i]["low"]); pc=safe_float(candles[i-1]["close"])
        if None in [h,l,pc]: continue
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(trs[-n:])/n if len(trs)>=n else None

def support_resistance(candles,lookback=30):
    v=[x for x in candles if safe_float(x.get("low")) and safe_float(x.get("high"))]
    if not v: return None,None
    r=v[-lookback:] if len(v)>=lookback else v
    return min(float(x["low"]) for x in r),max(float(x["high"]) for x in r)

def recent_momentum(candles,lookback=8):
    if len(candles)<lookback+1: return 0
    s=safe_float(candles[-lookback]["close"]); e=safe_float(candles[-1]["close"])
    return ((e-s)/s)*100 if s else 0

def compute_obv(candles):
    obv=0.0; series=[]
    for i,c in enumerate(candles):
        vol=safe_float(c.get("volume"),0) or 0
        if i==0: series.append(obv); continue
        pc=safe_float(candles[i-1]["close"]); cc=safe_float(c["close"])
        if cc and pc:
            if cc>pc: obv+=vol
            elif cc<pc: obv-=vol
        series.append(obv)
    return series

def compute_cmf(candles,period=14):
    if len(candles)<period: return 0.0
    mfv=vol=0.0
    for c in candles[-period:]:
        h=safe_float(c.get("high")); l=safe_float(c.get("low")); cl=safe_float(c.get("close")); v=safe_float(c.get("volume"),0) or 0
        if None in [h,l,cl] or (h-l)==0: continue
        mfv+=((cl-l)-(h-cl))/(h-l)*v; vol+=v
    return mfv/vol if vol>0 else 0.0

def detect_market_phase(candles):
    if len(candles)<30: return "NEUTRAL",0.0,{}
    closes=[safe_float(c["close"]) for c in candles if safe_float(c["close"])]
    highs=[safe_float(c["high"]) for c in candles if safe_float(c["high"])]
    lows=[safe_float(c["low"]) for c in candles if safe_float(c["low"])]
    volumes=[safe_float(c.get("volume"),0) or 0 for c in candles]
    if len(closes)<20: return "NEUTRAL",0.0,{}
    price=closes[-1]
    rh=max(highs[-20:]); rl=min(lows[-20:])
    prange=((rh-rl)/rl*100) if rl else 10
    obv=compute_obv(candles)
    obv_r=len(obv)>=10 and obv[-1]>obv[-10]; obv_f=len(obv)>=10 and obv[-1]<obv[-10]
    cmf=compute_cmf(candles,14)
    av20=sma(volumes,20) or 1; av10=sma(volumes,10) or 1
    vexp=av10>av20*1.1; vcon=av10<av20*0.9
    ma20=sma(closes,20); ma50=sma(closes,50) if len(closes)>=50 else ma20
    pma20=ma20 and price>ma20; pma50=ma50 and price>ma50; m2050=ma20 and ma50 and ma20>=ma50
    mid=len(highs)//2
    hh=max(highs[mid:])>max(highs[:mid]) if mid>0 else False
    hl=min(lows[mid:])>min(lows[:mid]) if mid>0 else False
    ll=min(lows[mid:])<min(lows[:mid]) if mid>0 else False
    lh=max(highs[mid:])<max(highs[:mid]) if mid>0 else False
    details={"price_range_pct":round(prange,2),"cmf":round(cmf,3),"obv_rising":obv_r,"obv_falling":obv_f,
             "vol_expanding":vexp,"vol_contracting":vcon,"higher_highs":hh,"higher_lows":hl,
             "lower_lows":ll,"lower_highs":lh,"price_above_ma20":pma20,"price_above_ma50":pma50,"ma20_above_ma50":m2050}
    a=mk=di=md=0
    if prange<8 and cmf>0.05: a+=3
    if obv_r and vcon: a+=2
    if pma20 and not pma50: a+=1
    if hh and hl: mk+=3
    if pma20 and pma50: mk+=2
    if m2050 and obv_r: mk+=2
    if vexp and cmf>0.1: mk+=2
    if pma20 and cmf<-0.05: di+=3
    if obv_f and vexp: di+=2
    if hh and not hl: di+=2
    if ll and lh: md+=3
    if not pma20 and not pma50: md+=2
    if obv_f and cmf<-0.1: md+=2
    scores={"ACCUMULATION":a,"MARKUP":mk,"DISTRIBUTION":di,"MARKDOWN":md}
    best=max(scores,key=scores.get)
    return ("NEUTRAL",cmf,details) if scores[best]<3 else (best,cmf,details)

def detect_reversal_signals(candles):
    sigs=[]
    if len(candles)<20: return sigs
    closes=[safe_float(c["close"]) for c in candles if safe_float(c["close"])]
    volumes=[safe_float(c.get("volume"),0) or 0 for c in candles]
    if len(closes)<15: return sigs
    rsi_r=[]
    for i in range(max(0,len(closes)-10),len(closes)):
        r=rsi(closes[:i+1],14)
        if r is not None: rsi_r.append(r)
    if len(rsi_r)>=6 and len(closes)>=6 and closes[-1]>closes[-6] and len(rsi_r)>=2 and rsi_r[-1]<rsi_r[-3] and closes[-1]>0:
        sigs.append({"type":"BEARISH_DIVERGENCE","severity":"STRONG","score_penalty":-20,"message":"سعر يصنع highs جديدة لكن RSI يضعف"})
    avg_vol=sma(volumes,20) or 1
    if volumes[-1]>avg_vol*2.5:
        lc=candles[-1]; body=abs(safe_float(lc["close"])-safe_float(lc["open"]) or 0)
        fr=(safe_float(lc["high"]) or 0)-(safe_float(lc["low"]) or 0)
        if fr>0 and body/fr<0.35:
            sigs.append({"type":"VOLUME_CLIMAX","severity":"CRITICAL","score_penalty":-25,"message":"حجم ضخم مع شمعة ضعيفة"})
    if len(closes)>=10:
        run=((closes[-1]-closes[-10])/closes[-10])*100 if closes[-10] else 0
        lc=candles[-1]; body=abs(safe_float(lc["close"])-safe_float(lc["open"]) or 0)
        fr=(safe_float(lc["high"]) or 0)-(safe_float(lc["low"]) or 0)
        if run>8 and fr>0 and body/fr<0.25:
            sigs.append({"type":"EXHAUSTION_DOJI","severity":"WARNING","score_penalty":-10,"message":"شمعة تردد بعد صعود"})
    cmf=compute_cmf(candles,14)
    if cmf<-0.15:
        sigs.append({"type":"DISTRIBUTION_DETECTED","severity":"STRONG","score_penalty":-15,"message":f"CMF={round(cmf,3)} — مؤسسات تبيع"})
    return sigs

def dynamic_targets_atr(entry,atr_v,kind,phase):
    if not atr_v or atr_v<=0: atr_v=entry*0.015
    atr_pct=(atr_v/entry)*100
    pm={"MARKUP":1.3,"ACCUMULATION":1.1,"NEUTRAL":1.0,"DISTRIBUTION":0.7,"MARKDOWN":0.5}.get(phase,1.0)
    if kind=="SHORT_SWING":
        t1=entry+atr_v*1.5*pm; t2=entry+atr_v*2.5*pm; t3=entry+atr_v*3.5*pm; stop=entry-atr_v
        tpct=((t1-entry)/entry)*100; ed=max(2,min(8,round(tpct/max(atr_pct,0.1)))); mh=7
    else:
        t1=entry+atr_v*3.0*pm; t2=entry+atr_v*5.0*pm; t3=entry+atr_v*7.0*pm; stop=entry-atr_v*1.8
        tpct=((t1-entry)/entry)*100; ed=max(5,min(30,round(tpct/max(atr_pct*0.3,0.1)))); mh=28
    rpct=((entry-stop)/entry)*100 if entry else 0
    rr=((t1-entry)/(entry-stop)) if entry>stop else 0
    return {"entry_low":round(entry*0.995,3),"entry_high":round(entry*1.005,3),
            "stop_loss":round(stop,3),"target1":round(t1,3),"target2":round(t2,3),"target3":round(t3,3),
            "target_pct":round(tpct,2),"risk_pct":round(rpct,2),"rr":round(rr,2),
            "atr_value":round(atr_v,4),"atr_pct":round(atr_pct,2),"estimated_days":ed,"max_hold_days":mh}

def position_sizing(entry,stop,kind):
    rp=0.01 if kind=="SHORT_SWING" else 0.015; mp=0.18 if kind=="SHORT_SWING" else 0.25
    rs=max(entry-stop,0); mra=CAPITAL*rp
    if rs<=0 or entry<=0: return {"qty":0,"position_value":0,"max_risk_aed":round(mra,2)}
    q=max(0,min(mra/rs,(CAPITAL*mp)/entry))
    return {"qty":round(q,2),"position_value":round(q*entry,2),"max_risk_aed":round(mra,2),"max_position_value":round(CAPITAL*mp,2)}

def get_rsi_bucket(r):
    if r is None: return "UNKNOWN"
    if r<40: return "LOW"
    if r<65: return "HEALTHY"
    if r<75: return "EXTENDED"
    return "OVERBOUGHT"

def get_volume_bucket(vr):
    if vr<1.0: return "LOW"
    if vr<1.4: return "NORMAL"
    return "HIGH"

def get_pattern_adjustment(symbol,setup_type,phase,rsi_bucket,vol_bucket):
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""SELECT outcome,COUNT(*) cnt,AVG(return_pct) avg_ret FROM v20_pattern_learning
                WHERE symbol=%s AND setup_type=%s AND market_phase=%s AND rsi_bucket=%s
                AND created_at::timestamp>NOW()-INTERVAL '60 days' GROUP BY outcome""",
                (symbol,setup_type,phase,rsi_bucket))
            rows=c.fetchall()
        if not rows: return 0.0
        wins=sum(int(r["cnt"]) for r in rows if r["outcome"]=="WIN")
        losses=sum(int(r["cnt"]) for r in rows if r["outcome"]=="LOSS")
        total=wins+losses
        if total<5: return 0.0
        wr=wins/total; ar=sum(float(r["avg_ret"] or 0)*int(r["cnt"]) for r in rows)/total
        return max(-20,min(15,(wr-0.5)*25+max(-5,min(5,ar))))
    except: return 0.0

def record_pattern_outcome(symbol,setup_type,phase,rsi_bucket,vol_bucket,trend_align,outcome,ret_pct,score,rr):
    try:
        with get_db() as conn:
            c=conn.cursor()
            c.execute("""INSERT INTO v20_pattern_learning
                (symbol,setup_type,market_phase,rsi_bucket,volume_bucket,trend_alignment,outcome,return_pct,score_at_entry,rr_at_entry,created_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (normalize_symbol(symbol),setup_type,phase,rsi_bucket,vol_bucket,trend_align,outcome,ret_pct,score,rr,utc_now()))
    except Exception as e: print(f"pattern error:{e}")

def get_learning_adjustment(symbol):
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT score_adjustment FROM ai_learning_stats WHERE symbol=%s",(normalize_symbol(symbol),))
            r=c.fetchone()
        return float(r["score_adjustment"] or 0) if r else 0.0
    except: return 0.0

def failure_penalty(symbol):
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""SELECT failure_reason,COUNT(*) c FROM ai_failure_memory
                WHERE symbol=%s AND created_at::timestamp>=%s GROUP BY failure_reason""",
                (normalize_symbol(symbol),(utc_now_dt()-timedelta(days=45)).isoformat()))
            rows=c.fetchall()
        pen=0.0
        for r in rows:
            cnt=int(r["c"] or 0)
            if cnt>=3:
                if r["failure_reason"] in ["LOW_VOLUME_REVERSAL","STRUCTURE_FAILED","DISTRIBUTION_ENTRY"]: pen-=10
                elif r["failure_reason"] in ["POOR_RISK_REWARD","LATE_ENTRY_OVERBOUGHT"]: pen-=6
                else: pen-=3
        return max(-25,pen)
    except: return 0.0

def update_learning(symbol,ret_pct,signal_type,is_virtual):
    symbol=normalize_symbol(symbol); ret_pct=safe_float(ret_pct,0) or 0
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM ai_learning_stats WHERE symbol=%s",(symbol,))
            row=c.fetchone()
            if row:
                t=int(row["trades_count"] or 0); w=int(row["wins_count"] or 0); l=int(row["losses_count"] or 0)
                vs=int(row["virtual_short_count"] or 0); vsw=int(row["virtual_short_wins"] or 0); vsl=int(row["virtual_short_losses"] or 0)
                vl=int(row["virtual_long_count"] or 0); vlw=int(row["virtual_long_wins"] or 0); vll=int(row["virtual_long_losses"] or 0)
                st=str(signal_type or "").upper()
                if is_virtual and "SHORT" in st: vs+=1; vsw+=(1 if ret_pct>0 else 0); vsl+=(1 if ret_pct<=0 else 0)
                elif is_virtual and "LONG" in st: vl+=1; vlw+=(1 if ret_pct>0 else 0); vll+=(1 if ret_pct<=0 else 0)
                else: t+=1; w+=(1 if ret_pct>0 else 0); l+=(1 if ret_pct<=0 else 0)
                tev=max(t+vs+vl,1); oa=float(row["avg_return_pct"] or 0)
                ar=((oa*max(tev-1,0))+ret_pct)/tev
                tw=w+vsw+vlw; tl=l+vsl+vll; cl=tw+tl
                wr=tw/cl if cl else 0; adj=max(-20,min(15,(wr-0.5)*30+ar))
                c.execute("""UPDATE ai_learning_stats SET trades_count=%s,wins_count=%s,losses_count=%s,
                    virtual_short_count=%s,virtual_short_wins=%s,virtual_short_losses=%s,
                    virtual_long_count=%s,virtual_long_wins=%s,virtual_long_losses=%s,
                    avg_return_pct=%s,score_adjustment=%s,updated_at=%s WHERE symbol=%s""",
                    (t,w,l,vs,vsw,vsl,vl,vlw,vll,ar,adj,utc_now(),symbol))
            else:
                adj=5 if ret_pct>0 else -5
                c.execute("""INSERT INTO ai_learning_stats
                    (symbol,trades_count,wins_count,losses_count,virtual_short_count,virtual_short_wins,
                    virtual_short_losses,virtual_long_count,virtual_long_wins,virtual_long_losses,
                    avg_return_pct,score_adjustment,updated_at) VALUES(%s,1,%s,%s,0,0,0,0,0,0,%s,%s,%s)""",
                    (symbol,1 if ret_pct>0 else 0,1 if ret_pct<=0 else 0,ret_pct,adj,utc_now()))
    except Exception as e: print(f"update_learning:{e}")

def daily_trend_score(d1):
    closes=[safe_float(x["close"]) for x in d1 if safe_float(x["close"])]
    if len(closes)<MIN_D1_CANDLES: return "UNKNOWN",0
    ma20=sma(closes,20); ma50=sma(closes,50) if len(closes)>=50 else ma20
    score=0
    if ma20 and closes[-1]>ma20: score+=12
    if ma50 and closes[-1]>ma50: score+=12
    if ma20 and ma50 and ma20>=ma50: score+=8
    return ("UP" if score>=20 else "MIXED" if score>=10 else "DOWN"),score

# ── BUILD SIGNAL V20.1 ───────────────────────────────────────

def build_signal_v20(symbol,kind,candles,d1):
    symbol=normalize_symbol(symbol); req=MIN_H1_CANDLES if kind=="SHORT_SWING" else MIN_D1_CANDLES
    if len(candles)<req: return None
    closes=[safe_float(x["close"]) for x in candles if safe_float(x["close"])]
    volumes=[safe_float(x.get("volume"),0) or 0 for x in candles]
    if len(closes)<req: return None
    price=closes[-1]
    if not price or price<=0: return None

    # FIX: عمر الكاندل
    last_c=candles[-1]; c_age=candle_age_hours(last_c); stale=c_age>MAX_CANDLE_AGE_HOURS

    ma20=sma(closes,20); ma50=sma(closes,50) if len(closes)>=50 else ma20
    ema20v=ema(closes,20); r=rsi(closes,14); a=atr(candles,14)
    sup,res=support_resistance(candles,30 if kind=="SHORT_SWING" else 60)
    avg_vol=sma(volumes,20) or 1; vr=volumes[-1]/avg_vol if avg_vol else 1
    mom=recent_momentum(candles,8 if kind=="SHORT_SWING" else 15)
    trend,_=daily_trend_score(d1)
    phase,cmf,phase_d=detect_market_phase(candles)
    rev_sigs=detect_reversal_signals(candles); rev_pen=sum(s["score_penalty"] for s in rev_sigs)

    score=0; reasons=[]
    if stale: reasons.append(f"⚠️ بيانات قديمة ({round(c_age,1)}h)")
    if ma20 and price>ma20: score+=16 if kind=="SHORT_SWING" else 12; reasons.append("Price above MA20")
    if ma50 and price>ma50: score+=14; reasons.append("Price above MA50")
    if ema20v and price>ema20v: score+=8; reasons.append("Price above EMA20")
    if trend=="UP": score+=22 if kind=="SHORT_SWING" else 28; reasons.append("Daily trend UP")
    elif trend=="MIXED": score+=8; reasons.append("Daily trend mixed")
    if r is not None:
        if 40<=r<=65: score+=16; reasons.append(f"RSI healthy ({round(r,1)})")
        elif 65<r<=73: score+=5; reasons.append(f"RSI extended ({round(r,1)})")
        elif r>73: score-=10; reasons.append(f"RSI overbought ({round(r,1)})")
    if vr>=1.5: score+=18; reasons.append(f"Strong volume ({round(vr,2)}x)")
    elif vr>=1.2: score+=10; reasons.append(f"Volume improving ({round(vr,2)}x)")
    elif vr<0.8: score-=8; reasons.append("Weak volume")
    if res and price>=res*0.985: score+=10; reasons.append("Near breakout zone")
    if mom>(1.0 if kind=="SHORT_SWING" else 2.5): score+=8; reasons.append("Positive momentum")
    if phase=="ACCUMULATION": score+=15; reasons.append("ACCUMULATION — تجميع")
    elif phase=="MARKUP": score+=20; reasons.append("MARKUP — صعود مدعوم")
    elif phase=="DISTRIBUTION": score-=25; reasons.append("DISTRIBUTION — تصريف!")
    elif phase=="MARKDOWN": score-=30; reasons.append("MARKDOWN — هبوط!")
    if rev_pen: score+=rev_pen; [reasons.append(f"⚠️ {s['type']}") for s in rev_sigs]

    rb=get_rsi_bucket(r); vb=get_volume_bucket(vr)
    ta="ALIGNED" if trend=="UP" else "MIXED" if trend=="MIXED" else "AGAINST"
    score+=get_pattern_adjustment(symbol,kind,phase,rb,vb)+get_learning_adjustment(symbol)+failure_penalty(symbol)

    tg=dynamic_targets_atr(price,a or price*0.015,kind,phase)
    strength=("VERY STRONG" if score>=88 else "STRONG" if score>=72 else "MEDIUM" if score>=55 else "WEAK")
    bad=phase in ["DISTRIBUTION","MARKDOWN"]
    has_rev=any(s["severity"] in ["STRONG","CRITICAL"] for s in rev_sigs)

    # FIX: لا BUY إذا البيانات قديمة
    ma=("BUY" if score>=MIN_SCORE_BUY and tg["rr"]>=MIN_RR_BUY
        and tg["risk_pct"]<=(5.5 if kind=="SHORT_SWING" else 12)
        and not bad and not has_rev and not stale else "WATCH")

    mode=get_ai_mode()
    action={"LEARNING":"LEARN_SIGNAL" if ma=="BUY" else "WATCH",
            "PAPER":"PAPER_BUY" if ma=="BUY" else "WATCH",
            "LIVE":"BUY" if ma=="BUY" else "WATCH"}.get(mode,"WATCH")
    tf="60" if kind=="SHORT_SWING" else "1D"

    sig={
        "symbol":symbol,"has_data":True,"type":kind,"mode":mode,"action":action,
        "model_action":ma,"timeframe":tf,"price":round(price,3),
        "entry_zone":[tg["entry_low"],tg["entry_high"]],"stop_loss":tg["stop_loss"],
        "target1":tg["target1"],"target2":tg["target2"],"target3":tg["target3"],
        "target_pct":tg["target_pct"],"risk_pct":tg["risk_pct"],"rr":tg["rr"],
        "estimated_days":tg["estimated_days"],"max_hold_days":tg["max_hold_days"],
        "atr_value":tg["atr_value"],"atr_pct":tg["atr_pct"],
        "score":round(score,2),"strength":strength,"trend":trend,"market_phase":phase,
        "cmf":round(cmf,3),"reversal_signals":rev_sigs,
        "support":round(sup,3) if sup else None,"resistance":round(res,3) if res else None,
        "rsi":round(r,2) if r is not None else None,"rsi_bucket":rb,
        "volume_ratio":round(vr,2),"volume_bucket":vb,"trend_alignment":ta,
        "momentum_pct":round(mom,2),"pattern_adj":0.0,"learning_adj":0.0,"failure_pen":0.0,
        "position_sizing":position_sizing(price,tg["stop_loss"],kind),
        "reason":" + ".join(reasons) if reasons else "No strong setup",
        "phase_details":phase_d,
        "data_is_stale":stale,
        "candle_age_hours":round(c_age,1),
        "last_bar_time":str(last_c.get("bar_time","")),
    }
    sig["rank_score"]=ai_rank_score(sig)
    sig["display_action"]=classify_action(sig)
    sig["ai_comment"]=ai_comment_v20(sig)
    return sig

def ai_rank_score(sig):
    s=float(sig.get("score") or 0); rr=min(float(sig.get("rr") or 0),5)*5
    vol=min(float(sig.get("volume_ratio") or 0),3)*3
    pb={"MARKUP":15,"ACCUMULATION":10,"NEUTRAL":3,"DISTRIBUTION":-20,"MARKDOWN":-25}.get(sig.get("market_phase"),0)
    sb={"VERY STRONG":12,"STRONG":7,"MEDIUM":3,"WEAK":0}.get(sig.get("strength"),0)
    tb=10 if sig.get("trend")=="UP" else 3 if sig.get("trend")=="MIXED" else 0
    rp=sum(x.get("score_penalty",0) for x in (sig.get("reversal_signals") or []))
    return round(s+rr+vol+pb+sb+tb+rp,2)

def classify_action(sig):
    if not sig: return "NO_DATA"
    if sig.get("data_is_stale"): return "STALE_DATA"
    if sig.get("model_action")=="BUY":
        p=sig.get("market_phase","")
        if p=="ACCUMULATION": return "BUY — تجميع"
        if p=="MARKUP": return "BUY — صعود"
        return "BUY"
    if sig.get("strength")=="VERY STRONG": return "STRONG WATCH"
    if sig.get("strength") in ["STRONG","MEDIUM"]: return "WATCH"
    return "WEAK WATCH"

def ai_comment_v20(sig):
    if not sig: return "No data"
    if sig.get("data_is_stale"): return f"⚠️ بيانات قديمة ({sig.get('candle_age_hours',0)}h) — انتظر تحديث"
    p=sig.get("market_phase","NEUTRAL"); rev=sig.get("reversal_signals") or []
    if p=="DISTRIBUTION": return "⚠️ تصريف — مؤسسات تبيع"
    if p=="MARKDOWN": return "❌ هبوط — ابتعد"
    if any(s["severity"]=="CRITICAL" for s in rev): return "⚠️ انعكاس قوي — انتظر"
    if sig.get("model_action")=="BUY": return f"✅ فرصة {p}"
    if sig.get("strength")=="VERY STRONG": return f"👀 مراقبة قوية — {p}"
    return f"مراقبة — {p}"

def no_data_result(symbol,reason,h1=0,d1=0):
    return {"symbol":normalize_symbol(symbol),"has_data":False,"action":"NO_DATA",
            "model_action":"NO_DATA","display_action":"NO_DATA","reason":reason,
            "ai_comment":reason,"score":None,"rank_score":None,"strength":None,
            "price":None,"rr":None,"market_phase":None,"h1_count":h1,"d1_count":d1}

def analyze_symbol(symbol,scan_type="ALL"):
    symbol=normalize_symbol(symbol); st=scan_type.upper()
    if st=="COMBINED": st="ALL"
    h1=get_candles(symbol,"60",100); d1=get_candles(symbol,"1D",100); sigs=[]
    if st in ["ALL","HOURLY"]:
        s=build_signal_v20(symbol,"SHORT_SWING",h1,d1)
        if s: sigs.append(s)
    if st in ["ALL","DAILY"]:
        s=build_signal_v20(symbol,"LONG_SWING",d1,d1)
        if s: sigs.append(s)
    return sigs

def analyze_symbol_from_cache(symbol,scan_type,cache):
    symbol=normalize_symbol(symbol); st=scan_type.upper()
    if st=="COMBINED": st="ALL"
    h1=cache.get(symbol,{}).get("60",[]); d1=cache.get(symbol,{}).get("1D",[]); sigs=[]
    if st in ["ALL","HOURLY"]:
        s=build_signal_v20(symbol,"SHORT_SWING",h1,d1)
        if s: sigs.append(s)
    if st in ["ALL","DAILY"]:
        s=build_signal_v20(symbol,"LONG_SWING",d1,d1)
        if s: sigs.append(s)
    return sigs
# ── DECISION LOCK ─────────────────────────────────────────────

def get_locked_decision(symbol):
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM v20_decision_lock WHERE symbol=%s AND status='LOCKED'",(normalize_symbol(symbol),))
            return c.fetchone()
    except: return None

def should_override_decision(existing,new_score,new_phase,has_crit):
    old=float(existing.get("score") or 0)
    la=parse_dt(str(existing.get("locked_at") or ""))
    hd=business_days_between(la,utc_now_dt()) if la else 99
    op=existing.get("market_phase","NEUTRAL"); delta=abs(new_score-old)
    if has_crit: return True,"critical_reversal_detected"
    if new_phase in ["DISTRIBUTION","MARKDOWN"] and op not in ["DISTRIBUTION","MARKDOWN"]: return True,"phase_turned_negative"
    if hd<DECISION_MIN_HOLD_DAYS:
        return (True,"major_score_shift") if delta>=20 else (False,f"locked_{hd}d")
    return (True,"score_delta_exceeded") if delta>=DECISION_CHANGE_DELTA else (False,f"stable_hold")

def update_decision_lock(symbol,sig):
    if not sig or not sig.get("has_data"): return
    symbol=normalize_symbol(symbol); ns=float(sig.get("score") or 0)
    np=sig.get("market_phase","NEUTRAL")
    hc=any(s.get("severity") in ["CRITICAL","STRONG"] for s in (sig.get("reversal_signals") or []))
    ex=get_locked_decision(symbol)
    if ex:
        ov,reason=should_override_decision(ex,ns,np,hc)
        if not ov:
            try:
                with get_db() as conn:
                    conn.cursor().execute("UPDATE v20_decision_lock SET last_checked_at=%s,entry_price=(entry_price*0.7+%s*0.3) WHERE symbol=%s AND status='LOCKED'",(utc_now(),sig.get("price"),symbol))
            except: pass
            return
        lr=reason
    else: lr="new_signal"
    dec=sig.get("model_action","WATCH")
    if np in ["DISTRIBUTION","MARKDOWN"]: dec="AVOID"
    try:
        with get_db() as conn:
            conn.cursor().execute("""INSERT INTO v20_decision_lock
                (symbol,decision,confidence,score,entry_price,entry_low,entry_high,stop_loss,
                target1,target2,target3,estimated_days,market_phase,signal_type,lock_reason,
                locked_at,last_checked_at,status,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LOCKED',%s)
                ON CONFLICT(symbol) DO UPDATE SET
                decision=EXCLUDED.decision,confidence=EXCLUDED.confidence,score=EXCLUDED.score,
                entry_price=EXCLUDED.entry_price,entry_low=EXCLUDED.entry_low,entry_high=EXCLUDED.entry_high,
                stop_loss=EXCLUDED.stop_loss,target1=EXCLUDED.target1,target2=EXCLUDED.target2,
                target3=EXCLUDED.target3,estimated_days=EXCLUDED.estimated_days,
                market_phase=EXCLUDED.market_phase,signal_type=EXCLUDED.signal_type,
                lock_reason=EXCLUDED.lock_reason,locked_at=EXCLUDED.locked_at,
                last_checked_at=EXCLUDED.last_checked_at,status='LOCKED',unlock_reason=NULL,payload=EXCLUDED.payload""",
                (symbol,dec,sig.get("score"),sig.get("score"),sig.get("price"),
                sig.get("entry_zone",[None,None])[0],sig.get("entry_zone",[None,None])[1],
                sig.get("stop_loss"),sig.get("target1"),sig.get("target2"),sig.get("target3"),
                sig.get("estimated_days"),np,sig.get("type"),lr,utc_now(),utc_now(),json.dumps(sig)))
    except Exception as e: print(f"lock error:{e}")

def check_decision_exits():
    released=[]
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM v20_decision_lock WHERE status='LOCKED'")
            locks=c.fetchall()
        for lock in locks:
            sym=lock["symbol"]
            # FIX: استخدام get_latest_price
            last_price,_,_=get_latest_price(sym)
            if not last_price: continue
            t1=safe_float(lock.get("target1")); sl=safe_float(lock.get("stop_loss"))
            status="LOCKED"; reason=None
            if t1 and last_price>=t1: status="HIT_TARGET"; reason=f"Price {last_price} hit T1 {t1}"
            elif sl and last_price<=sl: status="HIT_STOP"; reason=f"Price {last_price} hit stop {sl}"
            if status!="LOCKED":
                with get_db() as conn2:
                    conn2.cursor().execute("UPDATE v20_decision_lock SET status=%s,unlock_reason=%s,last_checked_at=%s WHERE symbol=%s",(status,reason,utc_now(),sym))
                released.append({"symbol":sym,"status":status,"reason":reason})
    except Exception as e: print(f"exits error:{e}")
    return released

# ── SCAN ENGINE ───────────────────────────────────────────────

def save_scan_result(scan_type,payload):
    mode=get_ai_mode()
    with get_db() as conn:
        conn.cursor().execute("INSERT INTO ai_scan_results(scan_type,mode,created_at,watchlist_count,scanned_count,signals_count,payload) VALUES(%s,%s,%s,%s,%s,%s,%s)",
            (scan_type.upper(),mode,utc_now(),len(WATCHLIST),payload.get("scanned_count",0),payload.get("signals_count",0),json.dumps(payload)))

def latest_scan_result(scan_type="COMBINED"):
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM ai_scan_results WHERE scan_type=%s ORDER BY id DESC LIMIT 1",(scan_type.upper(),))
        r=c.fetchone()
    return json.loads(r["payload"]) if r else None

def run_scan(scan_type):
    st=scan_type.upper()
    if st=="COMBINED": st="ALL"
    ranked,signals,coverage,errors=[],[],[],[]
    cache=get_all_candles_for_scan(220)
    for s in WATCHLIST:
        h1=cache.get(s,{}).get("60",[]); d1=cache.get(s,{}).get("1D",[])
        h1c=len(h1[-30:]); d1c=len(d1[-10:])
        try:
            sigs=analyze_symbol_from_cache(s,st,cache)
            best=max(sigs,key=lambda x:x.get("rank_score",0),default=None)
            if best:
                ranked.append(best)
                if best.get("model_action")=="BUY": signals.append(best)
                update_decision_lock(s,best)
                # FIX: أحدث سعر من cache
                lp,ltf,lbt=get_latest_price_from_cache(s,cache)
                coverage.append({"symbol":s,"has_data":True,"action":classify_action(best),
                    "model_action":best.get("model_action"),"score":best.get("score"),
                    "rank_score":best.get("rank_score"),"strength":best.get("strength"),
                    "price":lp,"price_tf":ltf,"price_time":lbt,
                    "rr":best.get("rr"),"market_phase":best.get("market_phase"),
                    "estimated_days":best.get("estimated_days"),"volume_ratio":best.get("volume_ratio"),
                    "ai_comment":ai_comment_v20(best),"data_is_stale":best.get("data_is_stale",False),
                    "candle_age_hours":best.get("candle_age_hours"),"h1_count":h1c,"d1_count":d1c})
            else:
                coverage.append(no_data_result(s,f"insufficient data h1={h1c} d1={d1c}",h1c,d1c))
        except Exception as e:
            errors.append({"symbol":s,"error":str(e)}); coverage.append(no_data_result(s,str(e),h1c,d1c))
            if len(errors)>=SCAN_MAX_ERRORS: break
    ranked=sorted(ranked,key=lambda x:x.get("rank_score",0),reverse=True)
    signals=sorted(signals,key=lambda x:x.get("rank_score",0),reverse=True)
    payload={"ok":True,"version":"V20.1","mode":get_ai_mode(),"scan_type":st,
        "created_at":utc_now(),"learning_age_days":round(learning_age_days(),2),
        "learning_remaining_days":round(learning_remaining_days(),2),
        "watchlist_count":len(WATCHLIST),"scanned_count":len(WATCHLIST),
        "signals_count":len(signals),"signals":signals[:20],
        "ranked_count":len(ranked),"ranked":ranked,
        "errors_count":len(errors),"errors":errors[:10],
        "coverage":sorted(coverage,key=lambda x:(x.get("rank_score") or -1),reverse=True)}
    try: record_observations(payload)
    except Exception as e: payload["observation_error"]=str(e)
    return payload

def save_combined_scan():
    hourly=latest_scan_result("HOURLY") or {}; daily=latest_scan_result("DAILY") or {}
    all_sigs=sorted(hourly.get("signals",[])+daily.get("signals",[]),key=lambda x:(x.get("score") or 0,x.get("rr") or 0),reverse=True)[:20]
    all_ranked=sorted(hourly.get("ranked",[])+daily.get("ranked",[]),key=lambda x:x.get("rank_score") or 0,reverse=True)
    hcov={x["symbol"]:x for x in hourly.get("coverage",[])}; dcov={x["symbol"]:x for x in daily.get("coverage",[])}
    cov=[]
    for s in WATCHLIST:
        h=hcov.get(s); d=dcov.get(s)
        best=h if (h and (not d or (h.get("rank_score") or -1)>=(d.get("rank_score") or -1))) else d
        cov.append(best or no_data_result(s,"no_scan"))
    save_scan_result("COMBINED",{"ok":True,"version":"V20.1","mode":get_ai_mode(),"scan_type":"COMBINED",
        "created_at":utc_now(),"learning_age_days":round(learning_age_days(),2),
        "learning_remaining_days":round(learning_remaining_days(),2),
        "watchlist_count":len(WATCHLIST),"scanned_count":len(WATCHLIST),
        "signals_count":len(all_sigs),"signals":all_sigs,"ranked_count":len(all_ranked),
        "ranked":all_ranked,"coverage":cov})

# ── VIRTUAL SIGNALS ───────────────────────────────────────────

def sig_key(sig):
    # FIX: أضف التاريخ للـ key عشان كل يوم تداول يُسجل إشارة جديدة
    today = utc_now_dt().strftime("%Y-%m-%d")
    return f"{sig['symbol']}-{sig['type']}-{today}-{sig['price']}-{sig['target1']}"

def record_virtual_signal(sig):
    if not sig or not sig.get("has_data"): return False
    key=sig_key(sig)
    try:
        with get_db() as conn:
            conn.cursor().execute("""INSERT INTO ai_virtual_signals
                (signal_key,mode,symbol,signal_type,timeframe,action,price,entry_low,entry_high,
                stop_loss,target1,target2,target3,score,strength,rr,risk_pct,target_pct,
                max_hold_days,estimated_days,market_phase,created_at,status,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)""",
                (key,sig["mode"],sig["symbol"],sig["type"],sig["timeframe"],sig["action"],sig["price"],
                sig.get("entry_zone",[None,None])[0],sig.get("entry_zone",[None,None])[1],
                sig["stop_loss"],sig["target1"],sig["target2"],sig["target3"],
                sig["score"],sig["strength"],sig["rr"],sig["risk_pct"],sig["target_pct"],
                sig["max_hold_days"],sig.get("estimated_days"),sig.get("market_phase"),
                utc_now(),json.dumps(sig)))
        return True
    except psycopg2.errors.UniqueViolation: return False
    except Exception as e: print(f"virtual signal error:{e}"); return False

def evaluate_virtual_signals():
    if not is_uae_trading_day(): return []
    evaluated=[]
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM ai_virtual_signals WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
            rows=c.fetchall()
        for sig in rows:
            try:
                sym=sig["symbol"]; tf=normalize_tf(sig["timeframe"]); created=parse_dt(sig["created_at"])
                candles=get_candles(sym,tf,400)
                rel=[c for c in candles if created and (parse_dt(c["bar_time"]) or parse_dt(c["received_at"])) and (parse_dt(c["bar_time"]) or parse_dt(c["received_at"]))>=created]
                if not rel: continue
                # FIX: لا تقيّم إشارة عمرها أقل من يوم تداول — تعطيها فرصة
                age_days=business_days_between(created,utc_now_dt())
                if age_days < 1 and len(rel) < 4: continue
                mh=max(float(x["high"]) for x in rel); ml=min(float(x["low"]) for x in rel)
                lc=float(rel[-1]["close"]); price=float(sig["price"]); t1=float(sig["target1"]); stop=float(sig["stop_loss"])
                mxh=int(sig["max_hold_days"] or 7)
                th=mh>=t1; sh=ml<=stop; status="OPEN"; outcome=None
                ret=((lc-price)/price)*100
                if th and not sh: status,outcome,ret="CLOSED","TARGET1_HIT",((t1-price)/price)*100
                elif sh and not th: status,outcome,ret="CLOSED","STOP_HIT",((stop-price)/price)*100
                elif th and sh: status,outcome,ret="CLOSED","BOTH_TOUCHED",((stop-price)/price)*100
                elif created and age_days>mxh: status,outcome="CLOSED","TIME_EXIT"
                with get_db() as conn2:
                    conn2.cursor().execute("UPDATE ai_virtual_signals SET max_high=%s,min_low=%s,bars_checked=%s,status=%s,outcome=%s,outcome_at=%s WHERE id=%s",
                        (mh,ml,len(rel),status,outcome,utc_now() if status=="CLOSED" else None,sig["id"]))
                if status=="CLOSED":
                    update_learning(sym,ret,sig["signal_type"],is_virtual=True)
                    pd=json.loads(sig["payload"]) if sig.get("payload") else {}
                    record_pattern_outcome(sym,sig.get("signal_type",""),sig.get("market_phase") or pd.get("market_phase","NEUTRAL"),
                        pd.get("rsi_bucket","UNKNOWN"),pd.get("volume_bucket","UNKNOWN"),pd.get("trend_alignment","UNKNOWN"),
                        "WIN" if ret>0 else "LOSS",round(ret,2),float(sig.get("score") or 0),float(sig.get("rr") or 0))
                    if ret<0: _classify_and_record_failure(sym,sig,pd,ret)
                    evaluated.append({"id":sig["id"],"symbol":sym,"type":sig["signal_type"],"outcome":outcome,"return_pct":round(ret,2)})
            except Exception as e: print(f"eval signal error:{e}"); continue
    except Exception as e: print(f"eval signals error:{e}")
    return evaluated

def _classify_and_record_failure(symbol,sig,payload,ret_pct):
    vol=float(payload.get("volume_ratio") or sig.get("volume_ratio") or 0)
    rv=float(payload.get("rsi") or 0); rr=float(sig.get("rr") or 0)
    phase=payload.get("market_phase") or sig.get("market_phase") or "UNKNOWN"
    strength=payload.get("strength") or sig.get("strength") or ""
    if phase in ["DISTRIBUTION","MARKDOWN"]: reason,lesson="DISTRIBUTION_ENTRY","لا تشتري في التصريف"
    elif vol<0.9: reason,lesson="LOW_VOLUME_REVERSAL","تجنب الدخول بحجم ضعيف"
    elif rv>73: reason,lesson="LATE_ENTRY_OVERBOUGHT","تجنب RSI مرتفع"
    elif rr<1.0: reason,lesson="POOR_RISK_REWARD","اشترط RR>=1.2"
    elif strength in ["WEAK","MEDIUM"]: reason,lesson="LOW_QUALITY_SETUP","اشترط STRONG"
    elif ret_pct<=-5: reason,lesson="STRUCTURE_FAILED","الهيكل فشل"
    else: reason,lesson="NORMAL_LOSS","خسارة طبيعية"
    try:
        with get_db() as conn:
            conn.cursor().execute("INSERT INTO ai_failure_memory (symbol,setup_type,failure_reason,loss_pct,market_state,lesson,created_at,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                (normalize_symbol(symbol),sig.get("signal_type",""),reason,round(ret_pct,2),phase,lesson,utc_now(),json.dumps(payload)))
    except: pass

def record_observations(scan_payload):
    created=0
    try:
        with get_db() as conn:
            c=conn.cursor()
            for item in scan_payload.get("ranked",[]):
                if not item.get("has_data") or not item.get("price"): continue
                key=f"{item.get('symbol')}-{scan_payload.get('scan_type')}-{item.get('timeframe')}-{item.get('price')}-{scan_payload.get('created_at','')[:13]}"
                try:
                    c.execute("""INSERT INTO ai_observations
                        (obs_key,symbol,scan_type,timeframe,action,model_action,strength,score,rank_score,price,observed_at,status,payload)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s) ON CONFLICT DO NOTHING""",
                        (key,item.get("symbol"),scan_payload.get("scan_type"),item.get("timeframe"),
                        item.get("display_action") or item.get("action"),item.get("model_action"),
                        item.get("strength"),item.get("score"),item.get("rank_score"),item.get("price"),
                        scan_payload.get("created_at"),json.dumps(item)))
                    created+=c.rowcount
                except: conn.rollback()
    except Exception as e: print(f"obs error:{e}")
    return created

def evaluate_observations():
    if not is_uae_trading_day(): return []
    out=[]
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM ai_observations WHERE status='OPEN' ORDER BY id ASC LIMIT 500")
            rows=c.fetchall()
        for obs in rows:
            try:
                tf=normalize_tf(obs["timeframe"] or "60"); sym=obs["symbol"]; observed=parse_dt(obs["observed_at"])
                candles=get_candles(sym,tf,400)
                rel=[c for c in candles if observed and (parse_dt(c["bar_time"]) or parse_dt(c["received_at"])) and (parse_dt(c["bar_time"]) or parse_dt(c["received_at"]))>=observed]
                if not rel: continue
                price=float(obs["price"] or 0)
                if price<=0: continue
                mh=max(float(x["high"]) for x in rel); ml=min(float(x["low"]) for x in rel)
                lat=float(rel[-1]["close"]); ret=((lat-price)/price)*100; mxd=5 if tf=="60" else 25
                th=((mh-price)/price)*100>=OBSERVATION_TARGET_PCT; dh=((price-ml)/price)*100>=OBSERVATION_DROP_PCT
                status,outcome="OPEN",None
                if th: status,outcome="CLOSED","WATCH_RALLIED"
                elif dh: status,outcome="CLOSED","WATCH_DROPPED"
                elif observed and business_days_between(observed,utc_now_dt())>mxd: status,outcome="CLOSED","WATCH_TIME_EXIT"
                with get_db() as conn2:
                    conn2.cursor().execute("UPDATE ai_observations SET max_high=%s,min_low=%s,return_pct=%s,status=%s,outcome=%s,outcome_at=%s WHERE id=%s",
                        (mh,ml,ret,status,outcome,utc_now() if status=="CLOSED" else None,obs["id"]))
                if status=="CLOSED": update_learning(sym,ret,"OBSERVATION",is_virtual=True); out.append({"symbol":sym,"outcome":outcome,"return_pct":round(ret,2)})
            except: continue
    except Exception as e: print(f"obs eval error:{e}")
    return out

# ── SELF EVALUATION ───────────────────────────────────────────

def run_self_evaluation():
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT COUNT(*) c FROM ai_virtual_signals"); total=int(c.fetchone()["c"])
            c.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='CLOSED'"); evaluated=int(c.fetchone()["c"])
            c.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE 'TARGET%'"); th=int(c.fetchone()["c"])
            c.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE '%STOP%'"); sh=int(c.fetchone()["c"])
            c.execute("SELECT AVG(rr) avg_rr,AVG(score) avg_score FROM ai_virtual_signals WHERE status='CLOSED'"); rr_row=c.fetchone()
            c.execute("SELECT COUNT(*) c FROM ai_failure_memory"); lc=int(c.fetchone()["c"])
            c.execute("""SELECT market_phase,SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) wins,COUNT(*) total
                FROM v20_pattern_learning WHERE created_at::timestamp>NOW()-INTERVAL '30 days' GROUP BY market_phase""")
            ps=c.fetchall()
            c.execute("SELECT COUNT(*) c FROM v20_decision_lock WHERE status='LOCKED'"); locked=int(c.fetchone()["c"])
            c.execute("SELECT COUNT(*) c FROM v20_decision_lock WHERE status='HIT_TARGET'"); lh=int(c.fetchone()["c"])
            c.execute("SELECT COUNT(*) c FROM v20_decision_lock WHERE status='HIT_STOP'"); ls=int(c.fetchone()["c"])
            c.execute("SELECT avg_return_pct,(virtual_short_count+virtual_long_count+trades_count) cnt FROM ai_learning_stats WHERE (virtual_short_count+virtual_long_count+trades_count)>0")
            lr=c.fetchall()
        wr=(th/evaluated*100) if evaluated else 0
        tw=sum(int(r["cnt"] or 0) for r in lr)
        ar=sum(float(r["avg_return_pct"] or 0)*int(r["cnt"] or 0) for r in lr)/tw if tw else 0
        rq=float(rr_row["avg_rr"] or 0) if rr_row else 0
        aq=float(rr_row["avg_score"] or 0) if rr_row else 0
        phase_report={}
        for p in ps:
            ph=p["market_phase"]; w=int(p["wins"] or 0); t=int(p["total"] or 0)
            phase_report[ph]={"wins":w,"total":t,"win_rate_pct":round(w/t*100,1) if t else 0}
        conf=0; cr=[]
        if evaluated>=10: conf+=10; cr.append("+10: 10+ evaluated")
        if evaluated>=30: conf+=10; cr.append("+10: 30+ signals")
        if evaluated>=60: conf+=10; cr.append("+10: 60+ signals")
        if wr>=50: conf+=10; cr.append("+10: WR>=50%")
        if wr>=60: conf+=10; cr.append("+10: WR>=60%")
        if wr>=68: conf+=10; cr.append("+10: WR>=68%")
        if ar>0: conf+=10; cr.append("+10: avg return positive")
        if ar>1.5: conf+=5; cr.append("+5: avg return>1.5%")
        if rq>=1.2: conf+=10; cr.append("+10: avg RR>=1.2")
        if rq>=1.8: conf+=5; cr.append("+5: avg RR>=1.8")
        mkup=phase_report.get("MARKUP",{})
        if mkup.get("win_rate_pct",0)>=60 and mkup.get("total",0)>=5: conf+=5; cr.append("+5: MARKUP proven")
        if wr<40 and evaluated>=15: conf-=15; cr.append("-15: WR<40%")
        if ar<-1: conf-=10; cr.append("-10: avg return negative")
        conf=max(0,min(100,conf)); age=learning_age_days()
        if conf<35: state,ready,rec,grade="LEARNING",False,"🔴 وضع التعلم — لا تداول","F"
        elif conf<50: state,ready,rec,grade="DEVELOPING",False,"🟠 يتطور — مراقبة فقط","D"
        elif conf<65: state,ready,rec,grade="STABILIZING",False,"🟡 يستقر — تجريبي فقط","C"
        elif conf<80: state,ready,rec,grade="STABLE",True,"🟢 مستقر — ورقي كامل","B"
        else: state,ready,rec,grade="HIGH_CONFIDENCE",True,"✅ ثقة عالية — جاهز","A"
        if age<LEARNING_DAYS: rec+=f" | ⏳ {round(LEARNING_DAYS-age,1)} يوم باقية"; ready=False
        result={"ok":True,"version":"V20.1","created_at":utc_now(),"system_state":state,
            "readiness_grade":grade,"confidence_score":conf,"ready_for_trading":ready,
            "recommendation":rec,"confidence_breakdown":cr,
            "learning_age_days":round(age,2),"learning_days_required":LEARNING_DAYS,
            "learning_progress_pct":round(min(100,age/LEARNING_DAYS*100),1),
            "signals":{"total":total,"evaluated":evaluated,"target_hits":th,"stop_hits":sh,
                "win_rate_pct":round(wr,2),"avg_return_pct":round(ar,2),"avg_rr":round(rq,2),"avg_score":round(aq,2)},
            "phase_performance":phase_report,
            "decision_locks":{"active":locked,"hit_target":lh,"hit_stop":ls},
            "lessons_learned":lc}
        with get_db() as conn:
            c=conn.cursor()
            # FIX: أضف العمود تلقائياً إذا ما كان موجود
            try: c.execute("ALTER TABLE ai_self_evaluation ADD COLUMN IF NOT EXISTS pattern_quality DOUBLE PRECISION")
            except: pass
            c.execute("INSERT INTO ai_self_evaluation (created_at,system_state,confidence_score,ready_for_trading,win_rate,avg_return_pct,rr_quality,lessons_count,pattern_quality,recommendation,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (utc_now(),state,conf,ready,wr,ar,rq,lc,float(mkup.get("win_rate_pct",0)),rec,json.dumps(result)))
        return result
    except Exception as e: return {"ok":False,"error":str(e),"trace":traceback.format_exc()[-1000:]}

def readiness_report():
    ev=run_self_evaluation(); sigs=ev.get("signals",{})
    return {"ok":True,"mode":get_ai_mode(),"status":ev.get("system_state","LEARNING"),
        "readiness_grade":ev.get("readiness_grade","F"),"confidence_score":ev.get("confidence_score",0),
        "ready_for_trading":ev.get("ready_for_trading",False),"recommendation":ev.get("recommendation",""),
        "learning_age_days":ev.get("learning_age_days",0),"learning_days_required":LEARNING_DAYS,
        "learning_progress_pct":ev.get("learning_progress_pct",0),
        "total_signals":sigs.get("total",0),"evaluated_signals":sigs.get("evaluated",0),
        "target_hits":sigs.get("target_hits",0),"stop_hits":sigs.get("stop_hits",0),
        "open_signals":sigs.get("total",0)-sigs.get("evaluated",0),
        "win_rate":sigs.get("win_rate_pct",0),"avg_return_pct":sigs.get("avg_return_pct",0)}

def get_signal_stats():
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT COUNT(*) c FROM ai_virtual_signals"); total=int(c.fetchone()["c"])
        c.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='OPEN'"); op=int(c.fetchone()["c"])
        c.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE status='CLOSED'"); cl=int(c.fetchone()["c"])
        c.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE 'TARGET%'"); hits=int(c.fetchone()["c"])
        c.execute("SELECT COUNT(*) c FROM ai_virtual_signals WHERE outcome LIKE '%STOP%'"); stops=int(c.fetchone()["c"])
    return {"total_signals":total,"open_signals":op,"closed_signals":cl,"target_hits":hits,"stop_hits":stops,
            "win_rate":round(hits/cl*100,2) if cl else 0}

# ── TELEGRAM ─────────────────────────────────────────────────

def tg_api(method,payload):
    if not TELEGRAM_BOT_TOKEN: return {"ok":False,"error":"no token"}
    try:
        r=requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",json=payload,timeout=30)
        try: return r.json()
        except: return {"ok":False,"text":r.text[:500]}
    except Exception as e: return {"ok":False,"error":str(e)}

def tg_send(chat_id,text,reply_markup=None):
    p={"chat_id":chat_id,"text":str(text)[:4000],"parse_mode":"HTML","disable_web_page_preview":True}
    if reply_markup: p["reply_markup"]=reply_markup
    return tg_api("sendMessage",p)

def tg_main_send(text,reply_markup=None):
    if not TELEGRAM_CHAT_ID: return {"ok":False,"error":"no chat_id"}
    return tg_send(TELEGRAM_CHAT_ID,text,reply_markup)

def signal_keyboard(symbol):
    return {"inline_keyboard":[[{"text":"تحليل أعمق","callback_data":f"more:{symbol}"},{"text":"تجاهل","callback_data":f"ignore:{symbol}"}]]}

def format_signal_v20(sig):
    sz=sig.get("position_sizing") or {}; ez=sig.get("entry_zone") or ["-","-"]
    phase=sig.get("market_phase","-"); rev=sig.get("reversal_signals") or []
    rt="\n".join(f"⚠️ {s['type']}: {s['message']}" for s in rev) if rev else "لا توجد"
    stale=f"\n⚠️ بيانات قديمة ({sig.get('candle_age_hours',0)}h)" if sig.get("data_is_stale") else ""
    pe={"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔","NEUTRAL":"⚪"}.get(phase,"⚪")
    return f"""<b>{sig.get('symbol','?')} — UAE AI V20.1</b>{stale}

{pe} المرحلة: <b>{phase}</b>
القرار: <b>{sig.get('display_action','-')}</b> | القوة: <b>{sig.get('strength','-')}</b>
Score: <b>{sig.get('score','-')}</b> | RR: <b>{sig.get('rr','-')}</b> | CMF: <b>{sig.get('cmf','-')}</b>

💰 السعر: <b>{sig.get('price','-')}</b>
دخول: <b>{ez[0]} — {ez[1]}</b> | Stop: <b>{sig.get('stop_loss','-')}</b>

🎯 T1: <b>{sig.get('target1','-')}</b>
🎯 T2: <b>{sig.get('target2','-')}</b>
🎯 T3: <b>{sig.get('target3','-')}</b>

⏱ ~{sig.get('estimated_days','-')} يوم | خطر: {sig.get('risk_pct','-')}% | هدف: {sig.get('target_pct','-')}%
📊 RSI:{sig.get('rsi','-')} | حجم:{sig.get('volume_ratio','-')}x

📦 الكمية: {sz.get('qty','-')} | القيمة: {sz.get('position_value','-')} AED

⚠️ انعكاس: {rt}
{esc(sig.get('reason','-'))}

{DASHBOARD_URL}""".strip()

def format_daily_report_v20():
    ev=run_self_evaluation(); stats=get_signal_stats(); scan=latest_scan_result("COMBINED") or {}
    grade=ev.get("readiness_grade","?"); conf=ev.get("confidence_score",0)
    ready="✅ جاهز" if ev.get("ready_for_trading") else "🔴 غير جاهز"
    ph_lines=[f"  {ph}: {pd.get('win_rate_pct',0)}% ({pd.get('total',0)} إشارة)" for ph,pd in ev.get("phase_performance",{}).items()]
    sig_lines=[]
    for s in scan.get("signals",[])[:5]:
        pe={"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔"}.get(s.get("market_phase",""),"⚪")
        sig_lines.append(f"{pe} <b>{s.get('symbol')}</b> | Score:{s.get('score')} | T1:{s.get('target1')} | ~{s.get('estimated_days','?')} يوم")
    locks=ev.get("decision_locks",{})
    lines=["<b>🤖 UAE AI V20.1 — التقرير اليومي</b>",
        f"📅 {uae_now_dt().strftime('%Y-%m-%d %H:%M')} UAE","",
        f"الدرجة: <b>{grade}</b> | الثقة: <b>{conf}%</b> | {ready}",
        f"{ev.get('recommendation','')}","",
        f"أيام التعلم: {ev.get('learning_age_days',0)}/{LEARNING_DAYS} ({ev.get('learning_progress_pct',0)}%)","",
        f"إشارات: {stats['total_signals']} | مغلقة: {stats['closed_signals']} | Win Rate: <b>{stats['win_rate']}%</b>",
        f"متوسط العائد: {ev.get('signals',{}).get('avg_return_pct',0)}% | RR: {ev.get('signals',{}).get('avg_rr',0)}","",
        "<b>أداء المراحل</b>"]+ph_lines+["",
        f"Locks: {locks.get('active',0)} مقفل | {locks.get('hit_target',0)} هدف | {locks.get('hit_stop',0)} stop",""]
    lines+=sig_lines if sig_lines else ["لا توجد إشارات شراء الآن"]
    lines+=[f"",DASHBOARD_URL]
    return "\n".join(lines)

def format_weekly_report_v20():
    ev=run_self_evaluation(); stats=get_signal_stats()
    lines=["<b>🤖 UAE AI V20.1 — التقرير الأسبوعي</b>",f"📅 {uae_now_dt().strftime('%Y-%m-%d')}","",
        f"الثقة: <b>{ev.get('confidence_score',0)}%</b> | الدرجة: <b>{ev.get('readiness_grade','?')}</b>",
        f"{ev.get('recommendation','')}","",
        f"Win Rate: <b>{stats['win_rate']}%</b> | إشارات: {stats['total_signals']}",
        f"الدروس: {ev.get('lessons_learned',0)}","","<b>أداء المراحل</b>"]
    for ph,pd in ev.get("phase_performance",{}).items():
        lines.append(f"  {ph}: {pd.get('win_rate_pct',0)}% من {pd.get('total',0)}")
    lines+=[f"",DASHBOARD_URL]
    return "\n".join(lines)

def format_readiness_alert(ev):
    grade=ev.get("readiness_grade","?"); emoji={"A":"✅","B":"🟢","C":"🟡","D":"🟠","F":"🔴"}.get(grade,"⚪")
    return (f"{emoji} <b>جاهزية V20.1</b>\n"
            f"الدرجة: <b>{grade}</b> | الثقة: <b>{ev.get('confidence_score',0)}%</b>\n"
            f"Win Rate: <b>{ev.get('signals',{}).get('win_rate_pct',0)}%</b>\n"
            f"{ev.get('recommendation','')}")

# ── SCAN JOBS ─────────────────────────────────────────────────

def _scan_summary(scan,title):
    lines=[f"<b>{title}</b>",f"Mode:{scan.get('mode',get_ai_mode())} | إشارات:<b>{scan.get('signals_count',0)}</b>",""]
    for s in scan.get("signals",[])[:TELEGRAM_TOP_N]:
        ez=s.get("entry_zone") or ["-","-"]; ph=s.get("market_phase","")
        pe={"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔"}.get(ph,"⚪")
        lines.append(f"{pe} <b>{s.get('symbol')}</b> {s.get('type')} | Score:{s.get('score')} | Entry:{ez[0]}-{ez[1]} | T1:{s.get('target1')} | ~{s.get('estimated_days','?')}يوم")
    if not scan.get("signals"): lines.append("لا توجد إشارات — السوق يحتاج وقتاً")
    lines+=[f"",DASHBOARD_URL]; return "\n".join(lines)

def hourly_scan_job(send=False):
    scan=run_scan("HOURLY"); save_scan_result("HOURLY",scan)
    for s in scan.get("signals",[]): record_virtual_signal(s)
    evaluate_virtual_signals(); evaluate_observations(); check_decision_exits(); save_combined_scan()
    # FIX: الـ hourly لا يرسل تلقائياً — الإرسال فقط من send_alerts
    # if send and scan.get("signals"): tg_main_send(_scan_summary(scan,"🕐 Hourly Scan V20.1"))

def daily_scan_job(send=True):
    scan=run_scan("DAILY"); save_scan_result("DAILY",scan)
    for s in scan.get("signals",[]): record_virtual_signal(s)
    evaluate_virtual_signals(); evaluate_observations(); check_decision_exits(); save_combined_scan()
    if send: tg_main_send(format_daily_report_v20())

def learning_scan_job():
    for scan in [latest_scan_result("HOURLY"),latest_scan_result("DAILY")]:
        if scan:
            for s in scan.get("signals",[]): record_virtual_signal(s)
    evaluate_virtual_signals(); evaluate_observations()

def get_batch_index():
    with get_db() as conn:
        c=conn.cursor()
        c.execute("INSERT INTO batch_scan_state(key,next_index,updated_at) VALUES('main',0,%s) ON CONFLICT(key) DO NOTHING",(utc_now(),))
        c.execute("SELECT next_index FROM batch_scan_state WHERE key='main'")
        r=c.fetchone()
    return int(r[0]) if r else 0

def set_batch_index(i):
    with get_db() as conn:
        conn.cursor().execute("INSERT INTO batch_scan_state(key,next_index,updated_at) VALUES('main',%s,%s) ON CONFLICT(key) DO UPDATE SET next_index=EXCLUDED.next_index,updated_at=EXCLUDED.updated_at",(i,utc_now()))

def record_failure_memory(symbol,setup_type,failure_reason,loss_pct,lesson,payload=None):
    with get_db() as conn:
        conn.cursor().execute("INSERT INTO ai_failure_memory (symbol,setup_type,failure_reason,loss_pct,market_state,lesson,created_at,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
            (normalize_symbol(symbol),setup_type,failure_reason,loss_pct,"UNKNOWN",lesson,utc_now(),json.dumps(payload or {})))

# ── PORTFOLIO TRADE FUNCTIONS ────────────────────────────────

def portfolio_buy(symbol, qty, price, notes=""):
    """تسجيل صفقة شراء"""
    symbol = normalize_symbol(symbol)
    qty = float(qty); price = float(price)
    with get_db() as conn:
        c = conn.cursor()
        # أضف أو حدّث المركز
        c.execute("""INSERT INTO portfolio_positions(symbol,qty,entry_price,position_type,status,notes,created_at,updated_at)
            VALUES(%s,%s,%s,'HOLDING','OPEN',%s,%s,%s)
            ON CONFLICT(symbol) DO UPDATE SET
            qty=EXCLUDED.qty, entry_price=EXCLUDED.entry_price,
            notes=EXCLUDED.notes, status='OPEN', updated_at=EXCLUDED.updated_at""",
            (symbol, qty, price, notes, utc_now(), utc_now()))
        # سجّل في تاريخ الصفقات
        c.execute("""INSERT INTO portfolio_trades(symbol,action,qty,price,entry_price,notes,created_at)
            VALUES(%s,'BUY',%s,%s,%s,%s,%s)""",
            (symbol, qty, price, price, notes, utc_now()))
    return {"ok": True, "symbol": symbol, "action": "BUY", "qty": qty, "price": price}

def portfolio_sell(symbol, qty, price, notes=""):
    """تسجيل صفقة بيع وحساب الربح"""
    symbol = normalize_symbol(symbol)
    qty = float(qty); price = float(price)
    # جلب سعر الدخول
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM portfolio_positions WHERE symbol=%s AND status='OPEN'", (symbol,))
        pos = c.fetchone()
    if not pos:
        return {"ok": False, "error": f"{symbol} مو موجود في المحفظة"}
    entry = float(pos["entry_price"]); pos_qty = float(pos["qty"])
    pnl_pct = ((price - entry) / entry) * 100
    pnl_aed = (price - entry) * qty
    with get_db() as conn:
        c = conn.cursor()
        # سجّل البيع
        c.execute("""INSERT INTO portfolio_trades(symbol,action,qty,price,entry_price,pnl_pct,pnl_aed,notes,created_at)
            VALUES(%s,'SELL',%s,%s,%s,%s,%s,%s,%s)""",
            (symbol, qty, price, entry, round(pnl_pct,2), round(pnl_aed,2), notes, utc_now()))
        # إذا باع كل الكمية أغلق المركز
        if qty >= pos_qty:
            c.execute("UPDATE portfolio_positions SET status='CLOSED', updated_at=%s WHERE symbol=%s", (utc_now(), symbol))
        else:
            new_qty = pos_qty - qty
            c.execute("UPDATE portfolio_positions SET qty=%s, updated_at=%s WHERE symbol=%s", (new_qty, utc_now(), symbol))
    return {"ok": True, "symbol": symbol, "action": "SELL", "qty": qty,
            "price": price, "entry": entry,
            "pnl_pct": round(pnl_pct, 2), "pnl_aed": round(pnl_aed, 2)}

def format_portfolio_tg():
    """تنسيق المحفظة لـ Telegram"""
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN' ORDER BY symbol")
        positions = c.fetchall()
    if not positions:
        return "📭 المحفظة فارغة"
    lines = ["<b>💼 محفظتي</b>", ""]
    total_pnl = 0
    for p in positions:
        sym = p["symbol"]; entry = float(p["entry_price"]); qty = float(p["qty"])
        lp, ltf, _ = get_latest_price(sym)
        pnl_pct = ((lp - entry) / entry * 100) if lp else None
        pnl_aed = ((lp - entry) * qty) if lp else None
        if pnl_aed: total_pnl += pnl_aed
        icon = "🟢" if pnl_pct and pnl_pct >= 0 else "🔴"
        pnl_txt = f"{round(pnl_pct,2):+.2f}% ({round(pnl_aed,0):+.0f} AED)" if pnl_pct is not None else "لا سعر"
        lines.append(f"{icon} <b>{sym}</b> | {qty:.0f} سهم @ {entry}")
        lines.append(f"   السعر: {lp or '-'} ({ltf}) | P&L: {pnl_txt}")
    lines.append("")
    lines.append(f"<b>إجمالي P&L: {round(total_pnl,0):+.0f} AED</b>")
    return "\n".join(lines)

def format_trade_history_tg(symbol=None):
    """تاريخ الصفقات"""
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if symbol:
            c.execute("SELECT * FROM portfolio_trades WHERE symbol=%s ORDER BY id DESC LIMIT 10", (normalize_symbol(symbol),))
        else:
            c.execute("SELECT * FROM portfolio_trades ORDER BY id DESC LIMIT 15")
        rows = c.fetchall()
    if not rows:
        return "لا توجد صفقات مسجلة"
    lines = ["<b>📋 تاريخ الصفقات</b>", ""]
    for r in rows:
        icon = "🟢 شراء" if r["action"] == "BUY" else "🔴 بيع"
        pnl = f" | P&L: {r['pnl_pct']:+.2f}%" if r.get("pnl_pct") is not None else ""
        lines.append(f"{icon} <b>{r['symbol']}</b> {r['qty']:.0f}@ {r['price']}{pnl}")
        lines.append(f"   {str(r['created_at'])[:16]}")
    return "\n".join(lines)

def format_analysis_tg(symbol):
    """تحليل سهم للبوت"""
    symbol = normalize_symbol(symbol)
    if symbol not in WATCHLIST:
        return f"⚠️ {symbol} مو في الـ Watchlist"
    sigs = analyze_symbol(symbol, "ALL")
    if not sigs:
        return f"لا توجد بيانات كافية لـ {symbol}"
    best = max(sigs, key=lambda x: x.get("score", 0))
    lock = get_locked_decision(symbol)
    lp, ltf, lbt = get_latest_price(symbol)
    stale = ""
    if lbt:
        pt = parse_dt(lbt)
        if pt:
            age = (utc_now_dt() - pt).total_seconds() / 3600
            if age > MAX_CANDLE_AGE_HOURS:
                stale = f"\n⚠️ بيانات قديمة ({round(age,0):.0f}h)"
    phase = best.get("market_phase", "-")
    pe = {"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔","NEUTRAL":"⚪"}.get(phase,"⚪")
    lines = [
        f"<b>📊 {symbol}</b>{stale}",
        f"{pe} المرحلة: <b>{phase}</b>",
        f"Score: <b>{best.get('score','-')}</b> | القوة: <b>{best.get('strength','-')}</b>",
        f"السعر: <b>{lp or best.get('price','-')}</b> ({ltf or '-'})",
        f"RSI: {best.get('rsi','-')} | حجم: {best.get('volume_ratio','-')}x",
        f"",
        f"القرار: <b>{best.get('display_action','-')}</b>",
        f"دخول: {best.get('entry_zone',['?','?'])[0]} — {best.get('entry_zone',['?','?'])[1]}",
        f"Stop: {best.get('stop_loss','-')} | T1: {best.get('target1','-')}",
        f"RR: {best.get('rr','-')} | خطر: {best.get('risk_pct','-')}%",
    ]
    if lock:
        lines.append(f"\n🔒 القرار المقفل: <b>{lock.get('decision')}</b> | {lock.get('market_phase')}")
    lines.append(f"\n{best.get('ai_comment','')}")
    return "\n".join(lines)

HELP_MSG = """<b>🤖 UAE AI Bot — الأوامر</b>

<b>📊 تحليل الأسهم:</b>
EMAAR — تحليل سهم
تحليل EMAAR — نفس الشيء

<b>💼 المحفظة:</b>
محفظة — عرض مراكزي
صفقاتي — تاريخ الصفقات
صفقات EMAAR — صفقات سهم معين

<b>💰 الشراء والبيع:</b>
شراء EMAAR 1000 11.56
بيع EMAAR 500 12.00

<b>📈 التقارير:</b>
تقرير — التقرير اليومي
أسبوعي — التقرير الأسبوعي
جاهزية — حالة النظام"""

# ============================================================
# V20.2 — SMART POSITION ANALYSIS & EXIT SYSTEM
# ============================================================

# ── STRUCTURE ANALYSIS ───────────────────────────────────────

def analyze_price_structure(candles):
    """
    يحلل هيكل السعر:
    - هل في Higher Highs / Higher Lows (uptrend سليم)
    - هل في Lower Lows / Lower Highs (downtrend)
    - هل الهيكل مكسور
    """
    if len(candles) < 20:
        return {"structure": "UNKNOWN", "broken": False, "details": "بيانات غير كافية"}

    highs  = [safe_float(c["high"])  for c in candles if safe_float(c["high"])]
    lows   = [safe_float(c["low"])   for c in candles if safe_float(c["low"])]
    closes = [safe_float(c["close"]) for c in candles if safe_float(c["close"])]

    if len(highs) < 15:
        return {"structure": "UNKNOWN", "broken": False, "details": "بيانات غير كافية"}

    # نقسم إلى ثلاثة أثلاث
    n = len(highs)
    t1_h = highs[:n//3]; t2_h = highs[n//3:2*n//3]; t3_h = highs[2*n//3:]
    t1_l = lows[:n//3];  t2_l = lows[n//3:2*n//3];  t3_l = lows[2*n//3:]

    # أحدث قاع وقمة
    recent_high = max(t3_h); recent_low = min(t3_l)
    mid_high    = max(t2_h); mid_low    = min(t2_l)
    old_high    = max(t1_h); old_low    = min(t1_l)

    # Higher Highs + Higher Lows = uptrend
    hh = recent_high > mid_high > old_high
    hl = recent_low  > mid_low  > old_low

    # Lower Lows + Lower Highs = downtrend
    ll = recent_low  < mid_low  < old_low
    lh = recent_high < mid_high < old_high

    # كسر الهيكل: آخر low كسر قاع سابق مهم
    key_support = min(lows[-20:-5]) if len(lows) >= 20 else min(lows[:-3])
    structure_broken = closes[-1] < key_support

    if hh and hl:
        structure = "UPTREND"
        desc = "Higher Highs + Higher Lows — اتجاه صاعد سليم"
    elif ll and lh:
        structure = "DOWNTREND"
        desc = "Lower Lows + Lower Highs — اتجاه هابط"
    elif structure_broken:
        structure = "BROKEN"
        desc = f"كسر دعم رئيسي عند {round(key_support,3)}"
    else:
        structure = "RANGING"
        desc = "تذبذب — لا اتجاه واضح"

    return {
        "structure":        structure,
        "broken":           structure_broken,
        "key_support":      round(key_support, 3),
        "recent_high":      round(recent_high, 3),
        "recent_low":       round(recent_low, 3),
        "higher_highs":     hh,
        "higher_lows":      hl,
        "lower_lows":       ll,
        "lower_highs":      lh,
        "description":      desc,
    }

def analyze_volume_behavior(candles):
    """
    يحلل سلوك الحجم:
    - هل الحجم يرتفع في الهبوط (بيع حقيقي)
    - هل الحجم خفيف في الهبوط (تصحيح طبيعي)
    - هل في تجميع هادئ
    """
    if len(candles) < 15:
        return {"verdict": "UNKNOWN", "details": "بيانات غير كافية"}

    volumes = [safe_float(c.get("volume"), 0) or 0 for c in candles]
    closes  = [safe_float(c["close"]) for c in candles if safe_float(c["close"])]

    avg_vol = sma(volumes, 20) or 1

    # كاندلز هابطة وصاعدة
    down_candles = [candles[i] for i in range(1, len(candles))
                    if safe_float(candles[i]["close"]) and safe_float(candles[i-1]["close"])
                    and safe_float(candles[i]["close"]) < safe_float(candles[i-1]["close"])]
    up_candles   = [candles[i] for i in range(1, len(candles))
                    if safe_float(candles[i]["close"]) and safe_float(candles[i-1]["close"])
                    and safe_float(candles[i]["close"]) > safe_float(candles[i-1]["close"])]

    avg_down_vol = (sum(safe_float(c.get("volume"),0) or 0 for c in down_candles[-10:]) /
                    max(len(down_candles[-10:]), 1))
    avg_up_vol   = (sum(safe_float(c.get("volume"),0) or 0 for c in up_candles[-10:]) /
                    max(len(up_candles[-10:]), 1))

    obv = compute_obv(candles)
    obv_trend = "RISING" if len(obv)>=10 and obv[-1]>obv[-10] else "FALLING"
    cmf_val   = compute_cmf(candles, 14)

    # بيع حقيقي: حجم الهبوط أكبر من حجم الصعود
    real_selling    = avg_down_vol > avg_up_vol * 1.3
    # تجميع هادئ: OBV صاعد مع حجم معتدل
    quiet_accum     = obv_trend == "RISING" and avg_up_vol > avg_down_vol
    # تصحيح طبيعي: هبوط بحجم خفيف
    healthy_pullback= avg_down_vol < avg_vol * 0.8 and obv_trend == "RISING"

    if real_selling and obv_trend == "FALLING":
        verdict = "REAL_SELLING"
        desc    = "بيع حقيقي — مؤسسات تخرج"
    elif quiet_accum:
        verdict = "ACCUMULATION"
        desc    = "تجميع هادئ — مؤسسات تشتري"
    elif healthy_pullback:
        verdict = "HEALTHY_PULLBACK"
        desc    = "تصحيح طبيعي — حجم خفيف، الاتجاه سليم"
    elif obv_trend == "FALLING":
        verdict = "DISTRIBUTION"
        desc    = "توزيع — ضغط بيع مستمر"
    else:
        verdict = "NEUTRAL"
        desc    = "حجم محايد"

    return {
        "verdict":          verdict,
        "obv_trend":        obv_trend,
        "cmf":              round(cmf_val, 3),
        "real_selling":     real_selling,
        "quiet_accum":      quiet_accum,
        "healthy_pullback": healthy_pullback,
        "avg_down_vol":     round(avg_down_vol, 0),
        "avg_up_vol":       round(avg_up_vol, 0),
        "description":      desc,
    }

def find_key_support_levels(candles, n=3):
    """يجد أهم مستويات الدعم القريبة"""
    if len(candles) < 10:
        return []
    lows = [(i, safe_float(c["low"])) for i,c in enumerate(candles) if safe_float(c["low"])]
    # نجد القيعان المحلية
    supports = []
    for i in range(2, len(lows)-2):
        idx, val = lows[i]
        if val and val < lows[i-1][1] and val < lows[i-2][1] and val < lows[i+1][1] and val < lows[i+2][1]:
            supports.append(round(val, 3))
    # نرتبها ونأخذ الأقرب للسعر الحالي
    current = safe_float(candles[-1]["close"]) or 0
    supports = sorted(set(supports), key=lambda x: abs(x - current))
    return supports[:n]

def calculate_averaging_down(entry_price, entry_qty, current_price, target_exit=None):
    """
    يحسب خطة Averaging Down:
    - كم تشتري عشان تعدل السعر
    - ما هو سعر التعادل الجديد
    - هل يستحق؟
    """
    if not entry_price or not current_price or current_price >= entry_price:
        return None

    loss_pct = ((current_price - entry_price) / entry_price) * 100
    current_value = entry_price * entry_qty

    # خيارات Averaging Down
    options = []
    for multiplier in [0.5, 1.0, 1.5, 2.0]:
        add_qty = entry_qty * multiplier
        add_value = current_price * add_qty
        total_qty = entry_qty + add_qty
        avg_price = (current_value + add_value) / total_qty
        breakeven_move = ((avg_price - current_price) / current_price) * 100

        options.append({
            "add_qty":        round(add_qty, 0),
            "add_value_aed":  round(add_value, 0),
            "new_avg_price":  round(avg_price, 3),
            "breakeven_pct":  round(breakeven_move, 2),
            "total_qty":      round(total_qty, 0),
            "label":          f"أضف {multiplier}x"
        })

    return {
        "entry_price":  entry_price,
        "entry_qty":    entry_qty,
        "current_price":current_price,
        "loss_pct":     round(loss_pct, 2),
        "options":      options
    }

# ── SMART POSITION DECISION ───────────────────────────────────

def smart_position_analysis(symbol, entry_price, entry_qty):
    """
    التحليل الذكي الكامل للمركز:
    يقرر: خروج / انتظار / averaging down
    """
    symbol = normalize_symbol(symbol)
    entry_price = float(entry_price)
    entry_qty   = float(entry_qty)

    # جلب البيانات
    h1 = get_candles(symbol, "60", 100)
    d1 = get_candles(symbol, "1D", 60)

    if not h1 and not d1:
        return {"ok": False, "error": "لا توجد بيانات"}

    # استخدم D1 إذا متاح، وإلا H1
    candles = d1 if len(d1) >= 15 else h1

    # السعر الحالي
    current_price, price_tf, _ = get_latest_price(symbol)
    if not current_price:
        return {"ok": False, "error": "لا يوجد سعر حالي"}

    pnl_pct = ((current_price - entry_price) / entry_price) * 100
    pnl_aed = (current_price - entry_price) * entry_qty

    # التحليلات
    structure = analyze_price_structure(candles)
    volume    = analyze_volume_behavior(candles)
    phase, cmf_val, _ = detect_market_phase(candles)
    rev_sigs  = detect_reversal_signals(candles)
    supports  = find_key_support_levels(candles)

    r = rsi([safe_float(c["close"]) for c in candles if safe_float(c["close"])], 14)
    avg_calc  = calculate_averaging_down(entry_price, entry_qty, current_price)

    # ── منطق القرار ──────────────────────────────────────────

    exit_signals   = []
    hold_signals   = []
    avg_signals    = []
    decision       = "HOLD"
    confidence     = "MEDIUM"
    avg_down_ok    = False

    # إشارات الخروج
    if structure["structure"] == "DOWNTREND" and structure["broken"]:
        exit_signals.append("🔴 هيكل مكسور + اتجاه هابط")
    elif structure["structure"] == "DOWNTREND":
        exit_signals.append("🔴 اتجاه هابط — Lower Lows متتالية")

    if volume["verdict"] == "REAL_SELLING":
        exit_signals.append("🔴 بيع حقيقي مؤسسي")

    if phase in ["MARKDOWN", "DISTRIBUTION"]:
        exit_signals.append(f"🔴 المرحلة {phase} — ابتعد")

    if any(s["severity"] == "CRITICAL" for s in rev_sigs):
        exit_signals.append("⚠️ إشارة انعكاس حرجة")

    if r and r > 70 and pnl_pct > 15:
        exit_signals.append(f"⚠️ RSI مرتفع ({round(r,1)}) مع ربح جيد — فكر بالخروج")

    # إشارات الانتظار
    if structure["structure"] == "UPTREND":
        hold_signals.append("✅ هيكل سليم — Higher Highs + Higher Lows")

    if volume["verdict"] in ["HEALTHY_PULLBACK", "ACCUMULATION"]:
        hold_signals.append(f"✅ {volume['description']}")

    if phase in ["ACCUMULATION", "MARKUP"]:
        hold_signals.append(f"✅ مرحلة {phase} — إيجابية")

    if supports and current_price > supports[0] * 0.98:
        hold_signals.append(f"✅ قريب من دعم {supports[0]} — قد يرتد")

    # إشارات Averaging Down
    if (structure["structure"] in ["UPTREND", "RANGING"] and
        volume["verdict"] in ["HEALTHY_PULLBACK", "ACCUMULATION"] and
        phase not in ["MARKDOWN", "DISTRIBUTION"] and
        pnl_pct < -5):
        avg_signals.append("✅ الهيكل سليم — Averaging Down ممكن")
        avg_down_ok = True

    if structure["broken"] or volume["verdict"] == "REAL_SELLING":
        avg_down_ok = False
        avg_signals = ["❌ لا تضيف على خسارة — الهيكل مكسور أو بيع حقيقي"]

    # القرار النهائي
    exit_score = len(exit_signals) * 2
    hold_score = len(hold_signals)

    if exit_score >= 4:
        decision = "EXIT_NOW"
        confidence = "HIGH"
    elif exit_score >= 2 and hold_score == 0:
        decision = "EXIT_SOON"
        confidence = "MEDIUM"
    elif hold_score >= 2 and exit_score == 0:
        decision = "HOLD"
        confidence = "HIGH"
    elif avg_down_ok and exit_score == 0:
        decision = "CONSIDER_AVERAGING"
        confidence = "MEDIUM"
    else:
        decision = "WATCH"
        confidence = "LOW"

    return {
        "ok":            True,
        "symbol":        symbol,
        "entry_price":   entry_price,
        "entry_qty":     entry_qty,
        "current_price": current_price,
        "price_source":  price_tf,
        "pnl_pct":       round(pnl_pct, 2),
        "pnl_aed":       round(pnl_aed, 2),
        "decision":      decision,
        "confidence":    confidence,
        "exit_signals":  exit_signals,
        "hold_signals":  hold_signals,
        "avg_signals":   avg_signals,
        "avg_down_ok":   avg_down_ok,
        "avg_calc":      avg_calc,
        "structure":     structure,
        "volume":        volume,
        "phase":         phase,
        "rsi":           round(r, 1) if r else None,
        "key_supports":  supports,
    }

def format_position_analysis_tg(analysis):
    """تنسيق تحليل المركز لـ Telegram"""
    if not analysis.get("ok"):
        return f"❌ {analysis.get('error','خطأ')}"

    sym     = analysis["symbol"]
    dec     = analysis["decision"]
    conf    = analysis["confidence"]
    pnl_pct = analysis["pnl_pct"]
    pnl_aed = analysis["pnl_aed"]
    phase   = analysis["phase"]
    struct  = analysis["structure"]["structure"]
    vol_v   = analysis["volume"]["verdict"]

    # أيقونات
    dec_icons = {
        "EXIT_NOW":           "🚨 اخرج الآن",
        "EXIT_SOON":          "🔴 خروج قريب",
        "HOLD":               "✅ احتفظ",
        "CONSIDER_AVERAGING": "🔵 فكر في Averaging Down",
        "WATCH":              "👀 راقب",
    }
    conf_icons = {"HIGH":"عالية","MEDIUM":"متوسطة","LOW":"منخفضة"}
    pnl_icon = "🟢" if pnl_pct >= 0 else "🔴"
    phase_icons = {"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔","NEUTRAL":"⚪"}

    lines = [
        f"<b>🎯 تحليل مركز {sym}</b>",
        "",
        f"دخول: <b>{analysis['entry_price']}</b> | الكمية: {analysis['entry_qty']:.0f}",
        f"السعر الحالي: <b>{analysis['current_price']}</b> ({analysis['price_source']})",
        f"{pnl_icon} P&L: <b>{pnl_pct:+.2f}%</b> ({pnl_aed:+,.0f} AED)",
        "",
        f"<b>━━━ القرار ━━━</b>",
        f"{dec_icons.get(dec, dec)} | ثقة: {conf_icons.get(conf,conf)}",
        "",
        f"<b>━━━ التحليل ━━━</b>",
        f"الهيكل: <b>{struct}</b> — {analysis['structure']['description']}",
        f"الحجم: <b>{vol_v}</b> — {analysis['volume']['description']}",
        f"{phase_icons.get(phase,'⚪')} المرحلة: <b>{phase}</b>",
        f"RSI: {analysis['rsi'] or '-'}",
    ]

    if analysis["key_supports"]:
        lines.append(f"دعم رئيسي: {' | '.join(str(s) for s in analysis['key_supports'])}")

    if analysis["exit_signals"]:
        lines.append("")
        lines.append("<b>إشارات الخروج:</b>")
        lines.extend(analysis["exit_signals"])

    if analysis["hold_signals"]:
        lines.append("")
        lines.append("<b>إشارات الانتظار:</b>")
        lines.extend(analysis["hold_signals"])

    # Averaging Down
    if analysis["avg_down_ok"] and analysis["avg_calc"]:
        ac = analysis["avg_calc"]
        lines.append("")
        lines.append("<b>━━━ Averaging Down ━━━</b>")
        lines.append(f"{analysis['avg_signals'][0] if analysis['avg_signals'] else ''}")
        lines.append("")
        for opt in ac["options"][:3]:
            lines.append(
                f"• {opt['label']}: أضف {opt['add_qty']:.0f} سهم بـ {opt['add_value_aed']:,.0f} AED"
                f" → سعر تعادل جديد: <b>{opt['new_avg_price']}</b>"
                f" (تحتاج ارتفاع {opt['breakeven_pct']}%)"
            )
    elif analysis["avg_signals"]:
        lines.append("")
        lines.extend(analysis["avg_signals"])

    return "\n".join(lines)

def format_full_portfolio_analysis_tg():
    """تحليل كامل لكل المحفظة"""
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN' ORDER BY symbol")
        positions = c.fetchall()

    if not positions:
        return "📭 المحفظة فارغة"

    lines = ["<b>🎯 تحليل المحفظة الكامل</b>", ""]
    total_pnl = 0
    exits = []; holds = []; avgs = []

    for p in positions:
        a = smart_position_analysis(p["symbol"], p["entry_price"], p["qty"])
        if not a.get("ok"): continue
        total_pnl += a["pnl_aed"]
        icon = {"EXIT_NOW":"🚨","EXIT_SOON":"🔴","HOLD":"✅","CONSIDER_AVERAGING":"🔵","WATCH":"👀"}.get(a["decision"],"⚪")
        line = f"{icon} <b>{a['symbol']}</b> | {a['pnl_pct']:+.2f}% | {a['decision']}"
        if a["decision"] in ["EXIT_NOW","EXIT_SOON"]: exits.append(line)
        elif a["decision"] == "CONSIDER_AVERAGING": avgs.append(line)
        else: holds.append(line)

    if exits:
        lines.append("🚨 <b>اخرج الآن:</b>")
        lines.extend(exits); lines.append("")
    if avgs:
        lines.append("🔵 <b>Averaging Down ممكن:</b>")
        lines.extend(avgs); lines.append("")
    if holds:
        lines.append("✅ <b>احتفظ:</b>")
        lines.extend(holds); lines.append("")

    pnl_icon = "🟢" if total_pnl >= 0 else "🔴"
    lines.append(f"{pnl_icon} <b>إجمالي P&L: {total_pnl:+,.0f} AED</b>")
    return "\n".join(lines)




# ============================================================
# V20.2 — MORNING & EOD REPORTS
# ============================================================

def format_morning_report():
    """
    تقرير الصباح 07:00 UAE
    - وضع المحفظة مع قرار لكل مركز
    - أفضل الفرص اليوم
    - تنبيهات مهمة
    """
    lines = [
        f"☀️ <b>تقرير الصباح — {uae_now_dt().strftime('%Y-%m-%d')}</b>",
        f"🕖 {uae_now_dt().strftime('%H:%M')} UAE",
        "",
    ]

    # ── قسم المحفظة ──────────────────────────────────────────
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN' ORDER BY symbol")
            positions = c.fetchall()

        if positions:
            lines.append("💼 <b>محفظتك اليوم:</b>")
            total_pnl = 0
            urgent = []

            for p in positions:
                sym   = p["symbol"]
                entry = float(p["entry_price"])
                qty   = float(p["qty"])
                a     = smart_position_analysis(sym, entry, qty)
                if not a.get("ok"): continue

                pnl   = a["pnl_pct"]
                dec   = a["decision"]
                price = a["current_price"]
                total_pnl += a["pnl_aed"]

                dec_map = {
                    "EXIT_NOW":           "🚨 اخرج اليوم",
                    "EXIT_SOON":          "🔴 فكر بالخروج",
                    "HOLD":               "✅ احتفظ",
                    "CONSIDER_AVERAGING": "🔵 ممكن تضيف",
                    "WATCH":              "👀 راقب",
                }
                icon = dec_map.get(dec, dec)

                lines.append(
                    f"{icon} | <b>{sym}</b> {pnl:+.2f}% "
                    f"(دخول:{entry} → حالي:{price})"
                )

                # إذا قرار عاجل أضفه للتنبيهات
                if dec in ["EXIT_NOW", "EXIT_SOON"]:
                    reason = a["exit_signals"][0] if a["exit_signals"] else ""
                    urgent.append(f"⚠️ {sym}: {reason}")

            pnl_icon = "🟢" if total_pnl >= 0 else "🔴"
            lines.append(f"{pnl_icon} <b>إجمالي P&L: {total_pnl:+,.0f} AED</b>")

            if urgent:
                lines.append("")
                lines.append("🚨 <b>تنبيهات عاجلة:</b>")
                lines.extend(urgent)
        else:
            lines.append("💼 المحفظة فارغة")

    except Exception as e:
        lines.append(f"⚠️ خطأ في تحليل المحفظة: {e}")

    # ── قسم الفرص ────────────────────────────────────────────
    lines.append("")
    lines.append("🎯 <b>أفضل الفرص اليوم:</b>")
    try:
        scan = latest_scan_result("COMBINED") or {}
        ranked = scan.get("ranked", [])

        # فلتر الفرص الجيدة فقط
        opportunities = [
            s for s in ranked
            if s.get("score", 0) >= 80
            and s.get("market_phase") in ["ACCUMULATION", "MARKUP"]
            and not s.get("data_is_stale", False)
        ][:5]

        if opportunities:
            for s in opportunities:
                phase = s.get("market_phase", "")
                pe = {"ACCUMULATION": "🔵", "MARKUP": "🟢"}.get(phase, "⚪")
                lines.append(
                    f"{pe} <b>{s.get('symbol')}</b> | "
                    f"Score:{s.get('score')} | "
                    f"T1:{s.get('target1')} | "
                    f"RR:{s.get('rr')} | "
                    f"~{s.get('estimated_days','?')} يوم"
                )
        else:
            lines.append("لا توجد فرص قوية الآن — السوق يحتاج وقتاً")

    except Exception as e:
        lines.append(f"⚠️ خطأ في جلب الفرص: {e}")

    # ── ملاحظات ──────────────────────────────────────────────
    lines.append("")
    lines.append(f"<i>للتحليل المفصل: اكتب اسم السهم في البوت</i>")
    lines.append(DASHBOARD_URL)

    return "\n".join(lines)


def format_eod_report():
    """
    تقرير ما بعد الإغلاق 15:30 UAE
    - حركة المحفظة اليوم
    - نسبة التعلم والجاهزية
    - الإشارات الجديدة
    """
    lines = [
        f"📊 <b>تقرير ما بعد الإغلاق — {uae_now_dt().strftime('%Y-%m-%d')}</b>",
        f"🕒 {uae_now_dt().strftime('%H:%M')} UAE",
        "",
    ]

    # ── حالة التعلم والجاهزية ────────────────────────────────
    try:
        ev = {}
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT payload FROM ai_self_evaluation ORDER BY id DESC LIMIT 1")
            r = c.fetchone()
            if r and r["payload"]: ev = json.loads(r["payload"])

        if ev:
            grade = ev.get("readiness_grade", "?")
            conf  = ev.get("confidence_score", 0)
            sigs  = ev.get("signals", {})
            wr    = sigs.get("win_rate_pct", 0)
            age   = ev.get("learning_age_days", 0)
            prog  = ev.get("learning_progress_pct", 0)
            grade_emoji = {"A":"✅","B":"🟢","C":"🟡","D":"🟠","F":"🔴"}.get(grade,"⚪")

            lines.append("🧠 <b>حالة النظام:</b>")
            lines.append(f"{grade_emoji} الدرجة: <b>{grade}</b> | الثقة: <b>{conf}%</b>")
            lines.append(f"Win Rate: <b>{wr}%</b> | التعلم: {age:.0f}/{ev.get('learning_days_required',21)} يوم ({prog}%)")
            lines.append(f"إشارات: {sigs.get('total',0)} | مقيّمة: {sigs.get('evaluated',0)}")
            lines.append(f"RR متوسط: {sigs.get('avg_rr',0)} | عائد متوسط: {sigs.get('avg_return_pct',0)}%")
        else:
            lines.append("🧠 <b>التعلم:</b> لا يوجد تقييم بعد")

    except Exception as e:
        lines.append(f"⚠️ خطأ في التقييم: {e}")

    # ── ملخص المحفظة بعد الإغلاق ─────────────────────────────
    lines.append("")
    lines.append("💼 <b>محفظتك عند الإغلاق:</b>")
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN' ORDER BY symbol")
            positions = c.fetchall()

        total_pnl = 0
        for p in positions:
            lp, ltf, _ = get_latest_price(p["symbol"])
            entry = float(p["entry_price"]); qty = float(p["qty"])
            pnl_pct = ((lp - entry) / entry * 100) if lp else None
            pnl_aed = ((lp - entry) * qty) if lp else None
            if pnl_aed: total_pnl += pnl_aed
            icon = "🟢" if pnl_pct and pnl_pct >= 0 else "🔴"
            lines.append(
                f"{icon} <b>{p['symbol']}</b> {pnl_pct:+.2f}% "
                f"({pnl_aed:+,.0f} AED) | {lp or '-'} ({ltf or '-'})"
                if pnl_pct is not None else
                f"⚪ <b>{p['symbol']}</b> — لا سعر"
            )
        pnl_icon = "🟢" if total_pnl >= 0 else "🔴"
        lines.append(f"{pnl_icon} <b>إجمالي اليوم: {total_pnl:+,.0f} AED</b>")

    except Exception as e:
        lines.append(f"⚠️ خطأ: {e}")

    # ── إشارات اليوم ─────────────────────────────────────────
    lines.append("")
    lines.append("📡 <b>إشارات اليوم:</b>")
    try:
        scan = latest_scan_result("COMBINED") or {}
        signals = scan.get("signals", [])
        if signals:
            for s in signals[:5]:
                pe = {"ACCUMULATION":"🔵","MARKUP":"🟢","DISTRIBUTION":"🔴","MARKDOWN":"⛔"}.get(
                    s.get("market_phase",""),"⚪")
                lines.append(
                    f"{pe} <b>{s.get('symbol')}</b> | "
                    f"Score:{s.get('score')} | T1:{s.get('target1')}"
                )
        else:
            lines.append("لا توجد إشارات شراء قوية اليوم")

    except Exception as e:
        lines.append(f"⚠️ خطأ: {e}")

    lines.append("")
    lines.append(DASHBOARD_URL)
    return "\n".join(lines)


# ── API ROUTES ────────────────────────────────────────────────

@app.get("/")
def home():
    return {"ok":True,"version":"V20.1","mode":get_ai_mode(),"uae_now":uae_now_dt().isoformat(),
        "is_trading_day":is_uae_trading_day(),"is_market_time":is_uae_market_time(),
        "learning_age_days":round(learning_age_days(),2),"learning_remaining_days":round(learning_remaining_days(),2),
        "watchlist_count":len(WATCHLIST)}

@app.get("/api/health")
@app.get("/api/healthz")
def health(): return {"ok":True,"version":"V20.1","mode":get_ai_mode()}

@app.get("/api/watchlist")
def watchlist_api(): return {"ok":True,"count":len(WATCHLIST),"stocks":WATCHLIST}

@app.get("/api/system/mode")
def api_mode(): return {"ok":True,"mode":get_ai_mode(),"learning_age_days":round(learning_age_days(),2),"learning_remaining_days":round(learning_remaining_days(),2)}

@app.get("/api/system/set-mode")
def api_set_mode(mode:str,secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    mode=mode.upper().strip()
    if mode not in ["LEARNING","PAPER","LIVE"]: return {"ok":False,"error":"invalid mode"}
    set_setting("ai_mode",mode); return {"ok":True,"mode":mode}

@app.post("/webhook/tradingview")
@app.post("/api/webhook/price-alert")
@app.get("/api/webhook/price-alert")
async def price_webhook(request:Request,secret:Optional[str]=None):
    try:
        sec=secret
        try:
            data=await request.json()
            if not sec: sec=data.get("secret")
        except: data=dict(request.query_params)
        if sec!=SECRET and sec!=CRON_SECRET: return {"ok":False,"error":"bad_secret"}
        symbol=normalize_symbol(data.get("symbol") or data.get("ticker") or "")
        exchange=str(data.get("exchange") or data.get("source") or "TRADINGVIEW").upper()
        tf_raw=str(data.get("timeframe") or data.get("interval") or "60")
        if tf_raw.strip() in ["1D","1440","D","DAY","DAILY"]: tf="1D"
        elif tf_raw.strip() in ["60","1H","H1","1h"]: tf="60"
        elif is_daily_exchange(exchange) and tf_raw.strip() not in ["60","1H","H1"]: tf="1D"
        else: tf=normalize_tf(tf_raw)
        o=safe_float(data.get("open") or data.get("o")); h=safe_float(data.get("high") or data.get("h"))
        l=safe_float(data.get("low") or data.get("l")); cl=safe_float(data.get("close") or data.get("price") or data.get("c"))
        v=safe_float(data.get("volume") or data.get("v"),0)
        if not symbol: return {"ok":False,"error":"missing_symbol"}
        if tf not in ["60","1D"]: tf="60"
        if None in [o,h,l,cl]: return {"ok":False,"error":"missing_ohlc"}
        with get_db() as conn:
            conn.cursor().execute("INSERT INTO candles(symbol,exchange,timeframe,bar_time,open,high,low,close,volume,received_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (symbol,exchange,tf,parse_bar_time(data.get("time") or data.get("timenow")),o,h,l,cl,v,utc_now()))
        return {"ok":True,"symbol":symbol,"timeframe":tf,"exchange":exchange,"close":cl}
    except Exception as e: return {"ok":False,"error":str(e),"trace":traceback.format_exc()[-500:]}

@app.get("/api/candles/latest")
def latest_candles(symbol:Optional[str]=None,timeframe:Optional[str]=None,limit:int=100):
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q,params="SELECT * FROM candles WHERE 1=1",[]
        if symbol: q+=" AND symbol=%s"; params.append(normalize_symbol(symbol))
        if timeframe: q+=" AND timeframe=%s"; params.append(normalize_tf(timeframe))
        q+=" ORDER BY id DESC LIMIT %s"; params.append(limit)
        c.execute(q,tuple(params)); rows=c.fetchall()
    return {"ok":True,"count":len(rows),"candles":rows}

@app.get("/api/admin/candle-stats")
def candle_stats():
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT timeframe,exchange,COUNT(*) count,MIN(bar_time) oldest,MAX(bar_time) newest FROM candles GROUP BY timeframe,exchange ORDER BY count DESC")
        rows=c.fetchall()
        c.execute("SELECT COUNT(DISTINCT symbol) FROM candles WHERE timeframe='1D'"); d1=c.fetchone()["count"]
        c.execute("SELECT COUNT(DISTINCT symbol) FROM candles WHERE timeframe='60'"); h1=c.fetchone()["count"]
    return {"ok":True,"h1_symbols":h1,"d1_symbols":d1,"breakdown":rows}

@app.get("/api/admin/fix-daily-candles")
def fix_daily_candles(secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    with get_db() as conn:
        c=conn.cursor()
        c.execute("UPDATE candles SET timeframe='1D' WHERE timeframe IN ('60','1','D','DAILY','DAY','1440') AND (exchange ILIKE '%DLY%' OR exchange ILIKE '%DAILY%' OR exchange ILIKE '%DAY%')")
        u=c.rowcount
    return {"ok":True,"updated_rows":u}

@app.get("/api/admin/fix-dfm-hourly")
def fix_dfm_hourly(secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    with get_db() as conn:
        c=conn.cursor()
        c.execute("UPDATE candles SET timeframe='60' WHERE timeframe='1D' AND exchange='DFM_DLY' AND (bar_time NOT LIKE '%T10:00:00%' AND bar_time NOT LIKE '%10:00:00+00:00%')")
        f=c.rowcount
    return {"ok":True,"fixed_rows":f}

@app.get("/api/admin/reset-learning-start")
def reset_learning_start(secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT MIN(created_at) oldest FROM ai_virtual_signals"); r=c.fetchone()
    oldest=r["oldest"] if r and r["oldest"] else utc_now()
    set_setting("learning_started_at",str(oldest)); return {"ok":True,"learning_started_at":str(oldest)}

@app.get("/api/watchlist/coverage")
@app.get("/api/coverage")
def coverage():
    out,ready_count=[],0
    for s in WATCHLIST:
        h1=len(get_candles(s,"60",35)); d1=len(get_candles(s,"1D",15))
        ready=h1>=MIN_H1_CANDLES and d1>=MIN_D1_CANDLES
        if ready: ready_count+=1
        out.append({"symbol":s,"h1_count":h1,"d1_count":d1,"ready":ready})
    return {"ok":True,"count":len(out),"ready_count":ready_count,"coverage":out}

@app.get("/api/ai/analyze")
@app.get("/api/analyze/{symbol}")
def api_analyze(symbol:str):
    symbol=normalize_symbol(symbol); sigs=analyze_symbol(symbol,"ALL"); locked=get_locked_decision(symbol)
    lp,ltf,lbt=get_latest_price(symbol)
    return {"ok":True,"symbol":symbol,"latest_price":lp,"price_timeframe":ltf,"price_bar_time":lbt,"signals":sigs,"locked_decision":locked}

@app.get("/api/ai/pro-scan")
def pro_scan(scan_type:str="COMBINED",run:bool=False):
    if run:
        data=run_scan(scan_type.upper()); save_scan_result(scan_type.upper(),data); return data
    data=latest_scan_result(scan_type.upper())
    return data or {"ok":True,"message":"No scan yet. Use ?run=true","signals":[],"coverage":[]}

@app.get("/api/ai/self-evaluation")
def api_self_evaluation(secret:Optional[str]=None): return run_self_evaluation()

@app.get("/api/ai/readiness")
def api_readiness(secret:Optional[str]=None): return readiness_report()

@app.get("/api/ai/decision-locks")
def api_decision_locks():
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM v20_decision_lock ORDER BY locked_at DESC"); rows=c.fetchall()
    return {"ok":True,"count":len(rows),"locks":rows}

@app.get("/api/ai/decision/{symbol}")
def api_decision(symbol:str): return {"ok":True,"symbol":symbol,"decision":get_locked_decision(normalize_symbol(symbol))}

@app.get("/api/ai/pattern-stats")
def api_pattern_stats(symbol:Optional[str]=None):
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q="SELECT symbol,market_phase,setup_type,rsi_bucket,COUNT(*) total,SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) wins,AVG(return_pct) avg_return FROM v20_pattern_learning WHERE 1=1"
        params=[]
        if symbol: q+=" AND symbol=%s"; params.append(normalize_symbol(symbol))
        q+=" GROUP BY symbol,market_phase,setup_type,rsi_bucket ORDER BY total DESC LIMIT 100"
        c.execute(q,params); rows=c.fetchall()
    return {"ok":True,"count":len(rows),"patterns":rows}

@app.get("/api/signals/open")
def get_open_signals():
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT symbol,signal_type,timeframe,action,price,target1,stop_loss,score,strength,rr,risk_pct,market_phase,estimated_days,created_at,status FROM ai_virtual_signals WHERE status='OPEN' ORDER BY id DESC LIMIT 100")
        rows=c.fetchall()
    return {"ok":True,"count":len(rows),"signals":rows}

@app.get("/api/signals/completed")
def get_completed_signals():
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT symbol,signal_type,timeframe,action,price,target1,stop_loss,score,strength,rr,outcome,outcome_at,max_high,min_low,market_phase,estimated_days,created_at FROM ai_virtual_signals WHERE status='CLOSED' ORDER BY id DESC LIMIT 100")
        rows=c.fetchall()
    return {"ok":True,"count":len(rows),"signals":rows}

@app.get("/api/portfolio")
def portfolio_list():
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN' ORDER BY symbol"); rows=c.fetchall()
    return {"ok":True,"count":len(rows),"positions":rows}

@app.get("/api/portfolio/add")
def portfolio_add(secret:Optional[str]=None,symbol:str="",qty:float=0,entry:float=0,position_type:str="HOLDING",notes:str=""):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    symbol=normalize_symbol(symbol)
    if not symbol or qty<=0 or entry<=0: return {"ok":False,"error":"symbol,qty,entry required"}
    with get_db() as conn:
        conn.cursor().execute("INSERT INTO portfolio_positions(symbol,qty,entry_price,position_type,status,notes,created_at,updated_at) VALUES(%s,%s,%s,%s,'OPEN',%s,%s,%s) ON CONFLICT(symbol) DO UPDATE SET qty=EXCLUDED.qty,entry_price=EXCLUDED.entry_price,position_type=EXCLUDED.position_type,notes=EXCLUDED.notes,status='OPEN',updated_at=EXCLUDED.updated_at",
            (symbol,qty,entry,position_type,notes,utc_now(),utc_now()))
    return {"ok":True,"symbol":symbol,"qty":qty,"entry":entry}

@app.get("/api/portfolio/buy")
def api_portfolio_buy(secret:Optional[str]=None,symbol:str="",qty:float=0,price:float=0,notes:str=""):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    if not symbol or qty<=0 or price<=0: return {"ok":False,"error":"symbol, qty, price required"}
    return portfolio_buy(symbol,qty,price,notes)

@app.get("/api/portfolio/sell")
def api_portfolio_sell(secret:Optional[str]=None,symbol:str="",qty:float=0,price:float=0,notes:str=""):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    if not symbol or qty<=0 or price<=0: return {"ok":False,"error":"symbol, qty, price required"}
    return portfolio_sell(symbol,qty,price,notes)

@app.get("/api/portfolio/trades")
def api_portfolio_trades(symbol:Optional[str]=None,limit:int=50):
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if symbol:
            c.execute("SELECT * FROM portfolio_trades WHERE symbol=%s ORDER BY id DESC LIMIT %s",(normalize_symbol(symbol),limit))
        else:
            c.execute("SELECT * FROM portfolio_trades ORDER BY id DESC LIMIT %s",(limit,))
        rows=c.fetchall()
    return {"ok":True,"count":len(rows),"trades":rows}

@app.get("/api/portfolio/analyze/{symbol}")
def api_position_analysis(symbol:str, entry:float=0, qty:float=0):
    if entry<=0 or qty<=0:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM portfolio_positions WHERE symbol=%s AND status='OPEN'",(normalize_symbol(symbol),))
            p=c.fetchone()
        if not p: return {"ok":False,"error":"أدخل entry و qty أو أضف السهم للمحفظة"}
        entry=float(p["entry_price"]); qty=float(p["qty"])
    return smart_position_analysis(symbol,entry,qty)

@app.get("/api/portfolio/analyze-all")
def api_portfolio_analyze_all():
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN'")
        positions=c.fetchall()
    results=[]
    for p in positions:
        a=smart_position_analysis(p["symbol"],p["entry_price"],p["qty"])
        results.append(a)
    return {"ok":True,"count":len(results),"analyses":results}

@app.get("/api/portfolio/monitor")
def portfolio_monitor(secret:Optional[str]=None):
    if secret is not None and not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN'"); positions=c.fetchall()
    out=[]
    for p in positions:
        sym=p["symbol"]; entry=float(p["entry_price"]); qty=float(p["qty"])
        # FIX: أحدث سعر من H1 أو 1D
        last_price,price_tf,price_time=get_latest_price(sym)
        sigs=analyze_symbol(sym,"ALL"); best=max(sigs,key=lambda x:x.get("rank_score",0),default=None)
        lock=get_locked_decision(sym)
        pnl_pct=((last_price-entry)/entry*100) if last_price else None
        pnl_aed=((last_price-entry)*qty) if last_price else None
        price_age=None; is_stale=False
        if price_time:
            pt=parse_dt(price_time)
            if pt:
                price_age=round((utc_now_dt()-pt).total_seconds()/3600,1)
                is_stale=price_age>MAX_CANDLE_AGE_HOURS
        action,reason="HOLD","قرار مستقر"
        if is_stale: action,reason="STALE_DATA",f"⚠️ بيانات قديمة ({price_age}h)"
        elif lock and lock.get("decision")=="AVOID": action,reason="EXIT_ALERT","المرحلة تحولت لتصريف"
        elif best and best.get("market_phase") in ["DISTRIBUTION","MARKDOWN"]: action,reason="EXIT_REVIEW","مرحلة تصريف/هبوط"
        elif pnl_pct and pnl_pct<=-8: action,reason="RISK_REVIEW",f"خسارة {round(pnl_pct,1)}%"
        out.append({"symbol":sym,"qty":qty,"entry_price":entry,"last_price":last_price,
            "price_source":price_tf,"price_time":price_time,"price_age_hours":price_age,
            "data_is_stale":is_stale,"pnl_pct":round(pnl_pct,2) if pnl_pct is not None else None,
            "pnl_aed":round(pnl_aed,2) if pnl_aed is not None else None,
            "action":action,"reason":reason,"market_phase":best.get("market_phase") if best else None,
            "signal_strength":best.get("strength") if best else None,"score":best.get("score") if best else None,
            "locked_decision":lock.get("decision") if lock else None})
    return {"ok":True,"count":len(out),"positions":out}

@app.get("/api/reports/daily")
def daily_report_api(secret:Optional[str]=None,send:bool=False):
    text=format_daily_report_v20(); sent=tg_main_send(text) if send else None
    return {"ok":True,"sent":send,"telegram":sent,"report":text}

@app.get("/api/reports/weekly")
def weekly_report_api(secret:Optional[str]=None,send:bool=False):
    text=format_weekly_report_v20(); sent=tg_main_send(text) if send else None
    return {"ok":True,"sent":send,"telegram":sent,"report":text}

@app.get("/api/ai/observations")
def api_observations(limit:int=100):
    ev=evaluate_observations()
    with get_db() as conn:
        c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM ai_observations ORDER BY id DESC LIMIT %s",(limit,)); rows=c.fetchall()
    return {"ok":True,"evaluated_now":ev,"count":len(rows),"observations":rows}

@app.get("/api/ai/learning-scan")
def api_learning_scan():
    hourly=latest_scan_result("HOURLY"); daily=latest_scan_result("DAILY"); signals=[]
    if hourly: signals.extend(hourly.get("signals",[]))
    if daily: signals.extend(daily.get("signals",[]))
    created=[{"symbol":s["symbol"],"type":s["type"]} for s in signals if record_virtual_signal(s)]
    ev=evaluate_virtual_signals(); ob=evaluate_observations()
    return {"ok":True,"mode":get_ai_mode(),"created_count":len(created),"evaluated_count":len(ev),"observations_evaluated":len(ob)}

@app.get("/api/ai/test-telegram")
def test_telegram(): return tg_main_send("✅ UAE AI V20.1 — اختبار Telegram ناجح!")

@app.get("/api/telegram/set-webhook")
def set_webhook():
    return tg_api("setWebhook",{"url":f"{BASE_URL}/api/telegram/webhook/{TELEGRAM_WEBHOOK_SECRET}"})

@app.post("/api/telegram/webhook/{secret}")
async def telegram_webhook(secret:str,request:Request):
    if secret!=TELEGRAM_WEBHOOK_SECRET: return {"ok":False,"error":"unauthorized"}
    data=await request.json()
    try:
        if "message" in data:
            msg=data["message"]; chat_id=msg["chat"]["id"]
            text=msg.get("text","").strip(); upper=text.upper()
            parts=text.split(); uparts=upper.split()

            # ── تقارير وجاهزية ──────────────────────────────────
            if upper in ["READINESS","جاهزية","STATUS","حالة"]:
                return tg_send(chat_id,format_readiness_alert(run_self_evaluation()))

            if upper in ["DAILY","تقرير","تقرير يومي"]:
                return tg_send(chat_id,format_daily_report_v20())

            if upper in ["WEEKLY","أسبوعي","اسبوعي","أسبوعي"]:
                return tg_send(chat_id,format_weekly_report_v20())

            if upper in ["HELP","مساعدة","اوامر","أوامر"]:
                return tg_send(chat_id,HELP_MSG)

            # ── المحفظة ──────────────────────────────────────────
            if upper in ["محفظة","PORTFOLIO","بورتفوليو"]:
                return tg_send(chat_id,format_portfolio_tg())

            # تحليل المحفظة الكامل
            if upper in ["تحليل محفظتي","تحليل المحفظة","محفظة تحليل","ANALYZE PORTFOLIO"]:
                return tg_send(chat_id,format_full_portfolio_analysis_tg())

            # تحليل مركز معين — مركز EMAAR أو تحليل مركز EMAAR
            if (upper.startswith("مركز ") or upper.startswith("تحليل مركز ")) and len(uparts)>=2:
                sym=normalize_symbol(uparts[-1])
                with get_db() as conn:
                    c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    c.execute("SELECT * FROM portfolio_positions WHERE symbol=%s AND status='OPEN'",(sym,))
                    p=c.fetchone()
                if p:
                    a=smart_position_analysis(sym,p["entry_price"],p["qty"])
                    return tg_send(chat_id,format_position_analysis_tg(a))
                return tg_send(chat_id,f"⚠️ {sym} مو موجود في محفظتك")

            if upper in ["صفقاتي","TRADES","HISTORY"]:
                return tg_send(chat_id,format_trade_history_tg())

            # صفقات EMAAR
            if upper.startswith("صفقات ") and len(uparts)>=2:
                return tg_send(chat_id,format_trade_history_tg(uparts[1]))

            # ── شراء ─────────────────────────────────────────────
            # شراء EMAAR 1000 11.56
            if (upper.startswith("شراء ") or upper.startswith("BUY ")) and len(parts)>=4:
                try:
                    sym=normalize_symbol(parts[1]); qty=float(parts[2]); price=float(parts[3])
                    notes=" ".join(parts[4:]) if len(parts)>4 else ""
                    r=portfolio_buy(sym,qty,price,notes)
                    return tg_send(chat_id,
                        f"✅ <b>تم تسجيل الشراء</b>\n"
                        f"السهم: <b>{sym}</b>\n"
                        f"الكمية: {qty:.0f} سهم\n"
                        f"السعر: {price}\n"
                        f"القيمة: {round(qty*price,2):,.0f} AED")
                except Exception as e:
                    return tg_send(chat_id,f"❌ خطأ: {e}\nالصيغة: شراء EMAAR 1000 11.56")

            # ── بيع ──────────────────────────────────────────────
            # بيع EMAAR 1000 12.00
            if (upper.startswith("بيع ") or upper.startswith("SELL ")) and len(parts)>=4:
                try:
                    sym=normalize_symbol(parts[1]); qty=float(parts[2]); price=float(parts[3])
                    notes=" ".join(parts[4:]) if len(parts)>4 else ""
                    r=portfolio_sell(sym,qty,price,notes)
                    if not r.get("ok"): return tg_send(chat_id,f"❌ {r.get('error')}")
                    icon="🟢" if r['pnl_pct']>=0 else "🔴"
                    return tg_send(chat_id,
                        f"{icon} <b>تم تسجيل البيع</b>\n"
                        f"السهم: <b>{sym}</b>\n"
                        f"الكمية: {qty:.0f} سهم\n"
                        f"البيع: {price} | الدخول: {r['entry']}\n"
                        f"P&L: <b>{r['pnl_pct']:+.2f}% ({r['pnl_aed']:+,.0f} AED)</b>")
                except Exception as e:
                    return tg_send(chat_id,f"❌ خطأ: {e}\nالصيغة: بيع EMAAR 1000 12.00")

            # ── تحليل سهم ────────────────────────────────────────
            if (upper.startswith("تحليل ") or upper.startswith("ANALYZE ")) and len(uparts)>=2:
                sym=normalize_symbol(uparts[1])
                return tg_send(chat_id,format_analysis_tg(sym),signal_keyboard(sym))

            # اسم السهم مباشرة
            sym=normalize_symbol(uparts[0]) if uparts else ""
            if sym in WATCHLIST:
                return tg_send(chat_id,format_analysis_tg(sym),signal_keyboard(sym))

            # callback من أزرار الكيبورد
            return tg_send(chat_id,HELP_MSG)

        # callback query (أزرار inline)
        if "callback_query" in data:
            cb=data["callback_query"]; chat_id=cb["message"]["chat"]["id"]
            cb_data=cb.get("data",""); cb_id=cb["id"]
            tg_api("answerCallbackQuery",{"callback_query_id":cb_id})
            if cb_data.startswith("more:"):
                sym=cb_data.split(":")[1]
                return tg_send(chat_id,format_analysis_tg(sym))
            if cb_data.startswith("ignore:"):
                return tg_send(chat_id,"تم التجاهل ✓")

    except Exception as e: print(f"tg webhook error:{e}\n{traceback.format_exc()}")
    return {"ok":True}

@app.get("/api/ai/send-alerts")
def send_alerts(secret:Optional[str]=None,force:bool=False):
    if secret is not None and not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    scan=latest_scan_result("COMBINED")
    if not scan: return {"ok":False,"message":"No scan yet"}
    sent,skipped,errors=[],[],[]
    with get_db() as conn:
        c=conn.cursor()
        for sig in scan.get("signals",[])[:5]:
            try:
                sym=sig.get("symbol",""); lock=get_locked_decision(sym)
                if not lock: skipped.append(f"{sym}:no_lock"); continue
                if lock.get("decision")=="AVOID": skipped.append(f"{sym}:avoid"); continue
                key=f"{sym}-{lock.get('decision')}-{str(lock.get('locked_at',''))[:10]}"
                if not force:
                    c.execute("SELECT id FROM ai_alerts_log WHERE alert_key=%s",(key,))
                    if c.fetchone(): skipped.append(f"{sym}:already_sent"); continue
                tg_main_send(format_signal_v20(sig),signal_keyboard(sym))
                c.execute("INSERT INTO ai_alerts_log(alert_key,symbol,signal_type,created_at,payload) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (key,sym,sig.get("type"),utc_now(),json.dumps(sig)))
                sent.append(sym)
            except Exception as e: errors.append({"symbol":sig.get("symbol",""),"error":str(e)})
    return {"ok":True,"sent":sent,"skipped":skipped,"errors":errors}

@app.get("/api/ai/reset-alerts")
def reset_alerts(secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_secret"}
    with get_db() as conn: conn.cursor().execute("DELETE FROM ai_alerts_log")
    return {"ok":True}

# ── CRON ENDPOINTS ────────────────────────────────────────────

@app.get("/api/cron/hourly-scan")
def cron_hourly_scan(secret:Optional[str]=None,send:bool=True):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    run_background_job(hourly_scan_job,send); return {"ok":True,"started":True,"job":"HOURLY_SCAN"}

@app.get("/api/cron/daily-scan")
def cron_daily_scan(secret:Optional[str]=None,send:bool=True):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    if not is_uae_trading_day(): return {"ok":True,"skipped":True,"reason":"UAE weekend"}
    run_background_job(daily_scan_job,send); return {"ok":True,"started":True,"job":"DAILY_SCAN"}

@app.get("/api/cron/learning-scan")
def cron_learning_scan(secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    run_background_job(learning_scan_job); return {"ok":True,"started":True,"job":"LEARNING_SCAN"}

@app.get("/api/cron/self-evaluation")
def cron_self_evaluation(secret:Optional[str]=None,send:bool=True):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    ev=run_self_evaluation()
    if send: tg_main_send(format_readiness_alert(ev))
    return {"ok":True,"evaluation":ev}

@app.get("/api/cron/end-of-day")
def cron_end_of_day(secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    if not is_uae_trading_day(): return {"ok":True,"skipped":True,"reason":"UAE weekend"}
    try:
        scan=run_scan("COMBINED"); save_scan_result("COMBINED",scan)
        for s in scan.get("signals",[]): record_virtual_signal(s)
        ev=evaluate_virtual_signals(); obs=evaluate_observations(); exits=check_decision_exits()
        self_ev=run_self_evaluation()
        # FIX: بعد الإغلاق نرسل EOD report بدل daily report
        tg_main_send(format_eod_report())
        return {"ok":True,"scan_signals":scan.get("signals_count",0),"evaluated":len(ev),
            "observations":len(obs),"decision_exits":len(exits),"confidence":self_ev.get("confidence_score",0),"state":self_ev.get("system_state")}
    except Exception as e: return {"ok":False,"error":str(e),"trace":traceback.format_exc()[-1500:]}

@app.get("/api/cron/weekly-report")
def cron_weekly_report(secret:Optional[str]=None):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    sent=tg_main_send(format_weekly_report_v20()); return {"ok":True,"sent":True,"telegram":sent}

@app.get("/api/cron/morning-report")
def cron_morning_report(secret:Optional[str]=None,send:bool=True):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    if not is_uae_trading_day(): return {"ok":True,"skipped":True,"reason":"UAE weekend"}
    text=format_morning_report()
    if send: tg_main_send(text)
    return {"ok":True,"sent":send,"report":text}

@app.get("/api/cron/eod-report")
def cron_eod_report(secret:Optional[str]=None,send:bool=True):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    if not is_uae_trading_day(): return {"ok":True,"skipped":True,"reason":"UAE weekend"}
    # شغّل self evaluation أولاً عشان الأرقام محدثة
    run_self_evaluation()
    text=format_eod_report()
    if send: tg_main_send(text)
    return {"ok":True,"sent":send,"report":text}

@app.get("/api/cron/portfolio-monitor")
def cron_portfolio_monitor(secret:Optional[str]=None):
    # مدمج في تقرير الصباح الآن — هذا الـ endpoint للاستخدام اليدوي فقط
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    data=portfolio_monitor(secret)
    return {"ok":True,"message":"Use /api/cron/morning-report for scheduled reports","data":data}

@app.get("/api/cron/send-alerts")
def cron_send_alerts(secret:Optional[str]=None,force:bool=False):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    return send_alerts(secret=secret,force=force)

@app.get("/api/cron/batch-scan")
def batch_scan(secret:Optional[str]=None,limit:int=10,send:bool=False):
    if not cron_ok(secret): return {"ok":False,"error":"bad_cron_secret"}
    if not is_uae_trading_day(): return {"ok":True,"skipped":True}
    total=len(WATCHLIST); start=get_batch_index(); end=min(start+limit,total)
    batch=WATCHLIST[start:end]; set_batch_index(0 if end>=total else end)
    ranked,batch_sigs,cov=[],[],[]
    cache=get_all_candles_for_scan(220)
    for sym in batch:
        try:
            sigs=analyze_symbol_from_cache(sym,"ALL",cache)
            best=max(sigs,key=lambda x:x.get("rank_score",0),default=None)
            if best:
                ranked.append(best)
                if best.get("model_action")=="BUY": batch_sigs.append(best)
                update_decision_lock(sym,best)
                cov.append({"symbol":sym,"action":classify_action(best),"score":best.get("score"),"market_phase":best.get("market_phase")})
        except Exception as e: cov.append({"symbol":sym,"error":str(e)})
    payload={"ok":True,"version":"V20.1","scan_type":"BATCH","created_at":utc_now(),
        "batch_start":start,"batch_end":end,"signals_count":len(batch_sigs),"signals":batch_sigs[:10],"coverage":cov}
    save_scan_result("HOURLY",payload); save_combined_scan()
    for s in batch_sigs: record_virtual_signal(s)
    evaluate_virtual_signals(); evaluate_observations()
    if send and batch_sigs: tg_main_send(_scan_summary({"signals":batch_sigs,"signals_count":len(batch_sigs),"mode":get_ai_mode()},"📦 Batch Scan"))
    return payload

# ── DASHBOARD ─────────────────────────────────────────────────

@app.get("/dashboard",response_class=HTMLResponse)
def dashboard():
    # FIX: كل البيانات من DB في query واحدة لكل section — لا live queries لكل سهم
    scan=latest_scan_result("COMBINED") or {"signals":[],"coverage":[],"ranked":[],"signals_count":0}
    pc={"ACCUMULATION":"#3b82f6","MARKUP":"#22c55e","DISTRIBUTION":"#ef4444","MARKDOWN":"#7f1d1d","NEUTRAL":"#6b7280"}

    # self evaluation — من آخر نتيجة محفوظة، وإذا ما في نشغل مرة واحدة
    ev={}
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT payload FROM ai_self_evaluation ORDER BY id DESC LIMIT 1")
            r=c.fetchone()
            if r and r["payload"]: ev=json.loads(r["payload"])
    except: pass
    if not ev:
        try: ev=run_self_evaluation()
        except: ev={}
    if not ev: ev={"readiness_grade":"F","recommendation":"لا يوجد تقييم بعد","confidence_score":0,"signals":{},"learning_progress_pct":0,"decision_locks":{}}
    sigs=ev.get("signals",{})
    gc={"A":"#22c55e","B":"#86efac","C":"#fbbf24","D":"#f97316","F":"#ef4444"}.get(ev.get("readiness_grade","F"),"#9ca3af")

    # جلب أسعار كل الأسهم في query واحدة — تشمل portfolio + ranked + locks
    price_map={}
    try:
        # جمع كل الأسهم المطلوبة
        syms_set=set()
        for s in scan.get("ranked",[])[:30]: syms_set.add(s.get("symbol",""))
        # أضف أسهم البورتفوليو والقرارات
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT symbol FROM portfolio_positions WHERE status='OPEN'")
            for r in c.fetchall(): syms_set.add(r["symbol"])
            c.execute("SELECT symbol FROM v20_decision_lock WHERE status='LOCKED'")
            for r in c.fetchall(): syms_set.add(r["symbol"])

        symbols_needed=[s for s in syms_set if s]
        if symbols_needed:
            with get_db() as conn:
                c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                c.execute("""
                    SELECT DISTINCT ON (symbol, timeframe) symbol, timeframe, close, bar_time
                    FROM candles WHERE symbol=ANY(%s) AND timeframe IN ('60','1D')
                    ORDER BY symbol, timeframe, id DESC
                """, (symbols_needed,))
                rows=c.fetchall()
            tmp={}
            for r in rows:
                sym=r["symbol"]; tf=r["timeframe"]
                # تقريب السعر لـ 4 أرقام بعد الفاصلة
                price=round(float(r["close"]),4) if r["close"] else None
                bt=parse_dt(str(r.get("bar_time") or ""))
                existing=tmp.get(sym)
                if not existing:
                    tmp[sym]=(price, "H1" if tf=="60" else "1D", str(r.get("bar_time","")))
                else:
                    ebt=parse_dt(str(existing[2]))
                    if bt and ebt and bt>ebt:
                        tmp[sym]=(price, "H1" if tf=="60" else "1D", str(r.get("bar_time","")))
            price_map=tmp
    except: pass

    # بورتفوليو — query واحدة
    port_rows=""
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM portfolio_positions WHERE status='OPEN' ORDER BY symbol")
            pp=c.fetchall()
        for p in pp:
            sym=p["symbol"]; entry=float(p["entry_price"]); qty=float(p["qty"])
            # السعر من price_map أو None
            pm=price_map.get(sym)
            lp=pm[0] if pm else None; ltf=pm[1] if pm else None; lbt=pm[2] if pm else None
            pnl_pct=((lp-entry)/entry*100) if lp else None
            pnl_aed=((lp-entry)*qty) if lp else None
            stale_warn=""; pst=""
            if lbt:
                pt=parse_dt(lbt)
                if pt:
                    age=(utc_now_dt()-pt).total_seconds()/3600
                    if age>MAX_CANDLE_AGE_HOURS: stale_warn=f"⚠️{round(age,0):.0f}h"; pst="color:#f97316"
            pc_=("#22c55e" if pnl_pct and pnl_pct>=0 else "#ef4444") if pnl_pct is not None else "#94a3b8"
            port_rows+=f"""<tr>
            <td><b>{esc(sym)}</b></td><td>{esc(qty)}</td><td>{esc(entry)}</td>
            <td style="{pst}"><b>{esc(lp or '-')}</b> <small>✓{esc(ltf or '')}</small> {stale_warn}</td>
            <td style="color:{pc_}"><b>{round(pnl_pct,2) if pnl_pct is not None else '-'}%</b></td>
            <td style="color:{pc_}">{round(pnl_aed,0) if pnl_aed is not None else '-'} AED</td></tr>"""
    except: pass

    # قرارات مقفلة — query واحدة
    lock_rows=""
    try:
        with get_db() as conn:
            c=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM v20_decision_lock WHERE status='LOCKED' ORDER BY locked_at DESC")
            locks=c.fetchall()
        for lk in locks:
            ph=lk.get("market_phase",""); phc=pc.get(ph,"#6b7280")
            sym=lk.get("symbol","")
            pm=price_map.get(sym); lp=pm[0] if pm else None; ltf=pm[1] if pm else None
            ep=safe_float(lk.get("entry_price"))
            diff_txt=""
            if lp and ep:
                d=((lp-ep)/ep)*100; dc="#22c55e" if d>=0 else "#ef4444"
                diff_txt=f'<span style="color:{dc}"> ({round(d,1):+.1f}%)</span>'
            lock_rows+=f"""<tr>
            <td><b>{esc(sym)}</b></td><td>{esc(lk.get('decision',''))}</td>
            <td style="color:{phc}">{esc(ph)}</td><td>{esc(lk.get('score',''))}</td>
            <td>{esc(lp or lk.get('entry_price',''))} <small>✓{esc(ltf or '')}</small>{diff_txt}</td>
            <td>{esc(lk.get('stop_loss',''))}</td><td>{esc(lk.get('target1',''))}</td>
            <td>{esc(lk.get('estimated_days',''))} يوم</td>
            <td>{esc(lk.get('lock_reason',''))}</td><td>{esc(str(lk.get('locked_at',''))[:16])}</td></tr>"""
    except: pass

    # جدول الأسهم — السعر من price_map (بدون query لكل سهم)
    sig_rows=""
    for s in scan.get("ranked",[])[:30]:
        ph=s.get("market_phase","NEUTRAL"); phc=pc.get(ph,"#6b7280")
        rev=s.get("reversal_signals") or []; ri="⚠️" if rev else ""
        sym=s.get("symbol","")
        pm=price_map.get(sym); lp=pm[0] if pm else None; ltf=pm[1] if pm else None; lbt=pm[2] if pm else None
        dp=lp if lp else s.get("price","")
        pan=""; pst=""
        if lbt:
            pt=parse_dt(lbt)
            if pt:
                age=(utc_now_dt()-pt).total_seconds()/3600
                if age>MAX_CANDLE_AGE_HOURS: pan=f"⚠️{round(age,0):.0f}h"; pst="color:#f97316"
                else: pan=f"✓{ltf}"
        sig_rows+=f"""<tr>
        <td><b>{esc(sym)}</b></td><td>{esc(s.get('type',''))}</td>
        <td style="color:{phc}">{esc(ph)}</td><td><b>{esc(s.get('display_action',''))}</b></td>
        <td>{esc(s.get('strength',''))}</td><td>{esc(s.get('score',''))}</td>
        <td style="{pst}"><b>{esc(dp)}</b> <small>{pan}</small></td>
        <td>{esc(s.get('stop_loss',''))}</td><td>{esc(s.get('target1',''))}</td>
        <td>{esc(s.get('estimated_days',''))} يوم</td><td>{esc(s.get('rr',''))}</td>
        <td>{esc(s.get('cmf',''))}</td><td>{ri} {esc(s.get('ai_comment',''))}</td></tr>"""

    return f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="300">
<title>UAE PRO AI V20.1</title>
<style>
body{{font-family:'Segoe UI',Arial,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px;margin:0}}
.card{{background:#1e293b;padding:20px;border-radius:16px;margin-bottom:20px;border:1px solid #334155}}
.grade{{font-size:48px;font-weight:bold;color:{gc}}}
.stat{{display:inline-block;margin:8px 16px;text-align:center}}
.stat-val{{font-size:24px;font-weight:bold;color:#a78bfa}}
.stat-lbl{{font-size:12px;color:#94a3b8}}
table{{width:100%;border-collapse:collapse;margin:10px 0;font-size:13px}}
th{{background:#334155;padding:10px 8px;text-align:right;color:#94a3b8;white-space:nowrap}}
td{{border-bottom:1px solid #1e293b;padding:8px;white-space:nowrap}}
tr:hover td{{background:#263548}}
h1{{color:#a78bfa;margin:0 0 4px}}
h2{{color:#60a5fa;margin:20px 0 10px;font-size:16px}}
.rec{{background:#1e3a5f;border-left:4px solid #3b82f6;padding:12px;border-radius:8px;margin:10px 0}}
.info{{background:#1e3a5f;border:1px solid #3b82f6;padding:8px 12px;border-radius:8px;margin-bottom:16px;font-size:13px}}
</style>
</head>
<body>
<h1>🤖 UAE PRO AI V20.1</h1>
<p style="color:#94a3b8;font-size:12px">🕐 {uae_now_dt().strftime('%Y-%m-%d %H:%M')} UAE | تحديث تلقائي كل 5 دقائق</p>
<div class="info">ℹ️ الأسعار: أحدث سعر من H1 أو 1D — أيهما أحدث | ✓H1=ساعي | ✓1D=يومي | ⚠️Xh=قديم X ساعة</div>

<div class="card">
<div class="grade">{ev.get('readiness_grade','?')}</div>
<div class="rec">{ev.get('recommendation','')}</div>
<div>
<div class="stat"><div class="stat-val">{ev.get('confidence_score',0)}%</div><div class="stat-lbl">الثقة</div></div>
<div class="stat"><div class="stat-val">{sigs.get('win_rate_pct',0)}%</div><div class="stat-lbl">Win Rate</div></div>
<div class="stat"><div class="stat-val">{sigs.get('avg_rr',0)}</div><div class="stat-lbl">Avg RR</div></div>
<div class="stat"><div class="stat-val">{sigs.get('avg_return_pct',0)}%</div><div class="stat-lbl">Avg Return</div></div>
<div class="stat"><div class="stat-val">{sigs.get('evaluated',0)}</div><div class="stat-lbl">Evaluated</div></div>
<div class="stat"><div class="stat-val">{ev.get('learning_progress_pct',0)}%</div><div class="stat-lbl">Learning</div></div>
<div class="stat"><div class="stat-val">{scan.get('signals_count',0)}</div><div class="stat-lbl">BUY Signals</div></div>
<div class="stat"><div class="stat-val">{scan.get('created_at','')[:16]}</div><div class="stat-lbl">Last Scan</div></div>
</div>
</div>

<h2>💼 البورتفوليو</h2>
<div class="card">
<table>
<tr><th>السهم</th><th>الكمية</th><th>سعر الدخول</th><th>السعر الحالي</th><th>P&L %</th><th>P&L AED</th></tr>
{port_rows if port_rows else "<tr><td colspan='6' style='text-align:center;color:#94a3b8'>لا توجد مراكز مفتوحة</td></tr>"}
</table>
</div>

<h2>🔒 القرارات المقفلة</h2>
<div class="card">
<table>
<tr><th>السهم</th><th>القرار</th><th>المرحلة</th><th>Score</th><th>السعر الحالي</th><th>Stop</th><th>T1</th><th>الأيام</th><th>السبب</th><th>تاريخ القفل</th></tr>
{lock_rows if lock_rows else "<tr><td colspan='10' style='text-align:center;color:#94a3b8'>لا توجد قرارات مقفلة</td></tr>"}
</table>
</div>

<h2>📊 ترتيب الأسهم</h2>
<div class="card">
<table>
<tr><th>السهم</th><th>النوع</th><th>المرحلة</th><th>القرار</th><th>القوة</th><th>Score</th><th>السعر</th><th>Stop</th><th>T1</th><th>الأيام</th><th>RR</th><th>CMF</th><th>التعليق</th></tr>
{sig_rows if sig_rows else "<tr><td colspan='13' style='text-align:center;color:#94a3b8'>لا توجد إشارات</td></tr>"}
</table>
</div>
</body>
</html>"""
