import os
from datetime import datetime
from collections import defaultdict

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score


MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)

HORIZONS = [5, 10, 15, 20]


def _safe_float(x):
    if x is None:
        return np.nan
    try:
        return float(x)
    except Exception:
        return np.nan


def load_daily_candles(conn):
    q = """
        SELECT symbol, bar_time, open, high, low, close, volume
        FROM candles
        WHERE timeframe = '1D'
          AND close IS NOT NULL
        ORDER BY symbol, bar_time ASC
    """
    return pd.read_sql(q, conn)


def build_features_for_symbol(df):
    df = df.copy()

    df["close"] = df["close"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["volume"] = df["volume"].fillna(0).astype(float)

    df["return_1d"] = df["close"].pct_change()
    df["return_5d"] = df["close"].pct_change(5)
    df["ema20"] = df["close"].rolling(20).mean()
    df["ema50"] = df["close"].rolling(50).mean()
    df["vol20"] = df["volume"].rolling(20).mean()
    df["high20"] = df["high"].rolling(20).max()
    df["low20"] = df["low"].rolling(20).min()
    df["low50"] = df["low"].rolling(50).min()

    df["volume_ratio"] = df["volume"] / df["vol20"]
    df["distance_from_low50"] = (df["close"] - df["low50"]) / df["low50"]
    df["distance_from_high20"] = (df["high20"] - df["close"]) / df["high20"]

    df["trend_up"] = ((df["close"] > df["ema20"]) & (df["ema20"] > df["ema50"])).astype(int)
    df["trend_down"] = ((df["close"] < df["ema20"]) & (df["ema20"] < df["ema50"])).astype(int)
    df["breakout"] = (df["close"] > df["high20"].shift(1)).astype(int)

    df["bounce_from_low"] = ((df["close"] - df["low50"]) / df["low50"]) * 100
    df["pullback_from_high"] = ((df["high20"] - df["close"]) / df["high20"]) * 100

    df["wave1_reversal"] = (
        (df["trend_down"].shift(5) == 1) &
        (df["bounce_from_low"] >= 8) &
        (df["close"] > df["ema20"])
    ).astype(int)

    df["wave2_pullback"] = (
        (df["wave1_reversal"] == 1) &
        (df["pullback_from_high"] >= 3) &
        (df["pullback_from_high"] <= 12) &
        (df["close"] > df["low50"] * 1.03)
    ).astype(int)

    df["accumulation"] = (
        (df["trend_down"] == 1) &
        (df["close"] > df["low50"] * 1.03) &
        (df["return_1d"] >= 0)
    ).astype(int)

    return df


FEATURE_COLUMNS = [
    "return_1d",
    "return_5d",
    "volume_ratio",
    "distance_from_low50",
    "distance_from_high20",
    "trend_up",
    "trend_down",
    "breakout",
    "bounce_from_low",
    "pullback_from_high",
    "wave1_reversal",
    "wave2_pullback",
    "accumulation",
]


def build_training_dataset(conn):
    raw = load_daily_candles(conn)

    if raw.empty:
        return pd.DataFrame()

    frames = []

    for symbol, sdf in raw.groupby("symbol"):
        sdf = build_features_for_symbol(sdf)
        sdf["symbol"] = symbol

        for h in HORIZONS:
            future_close = sdf["close"].shift(-h)
            future_return = (future_close - sdf["close"]) / sdf["close"]

            sdf[f"future_return_{h}d"] = future_return

            # WIN = حقق 5% أو أكثر خلال الفترة
            sdf[f"label_{h}d"] = (future_return >= 0.05).astype(int)

        frames.append(sdf)

    data = pd.concat(frames, ignore_index=True)
    data = data.dropna(subset=FEATURE_COLUMNS)

    return data


def train_models(conn):
    data = build_training_dataset(conn)

    if data.empty or len(data) < 200:
        return {
            "status": "NOT_READY",
            "reason": "Not enough training data",
            "rows": int(len(data))
        }

    results = {}

    for h in HORIZONS:
        label_col = f"label_{h}d"

        train_data = data.dropna(subset=[label_col])
        if len(train_data) < 200:
            results[f"{h}d"] = {
                "status": "SKIPPED",
                "reason": "Not enough rows"
            }
            continue

        X = train_data[FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan).fillna(0)
        y = train_data[label_col].astype(int)

        split = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split], X.iloc[split:]
        y_train, y_test = y.iloc[:split], y.iloc[split:]

        model = RandomForestClassifier(
            n_estimators=300,
            max_depth=6,
            min_samples_leaf=8,
            random_state=42,
            class_weight="balanced"
        )

        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        acc = accuracy_score(y_test, preds) if len(y_test) else None

        model_path = os.path.join(MODEL_DIR, f"model_{h}d.joblib")
        joblib.dump(model, model_path)

        results[f"{h}d"] = {
            "status": "TRAINED",
            "rows": int(len(train_data)),
            "accuracy": round(float(acc), 3) if acc is not None else None,
            "model_path": model_path
        }

    return {
        "status": "TRAINED",
        "trained_at": datetime.utcnow().isoformat(),
        "rows": int(len(data)),
        "features": FEATURE_COLUMNS,
        "models": results
    }


def ai_status(conn):
    data = build_training_dataset(conn)

    rows = len(data)
    status = "TRAINING"

    if rows >= 1000:
        status = "PAPER_TRADING"
    if rows >= 3000:
        status = "READY_CANDIDATE"

    trained_models = {}
    for h in HORIZONS:
        path = os.path.join(MODEL_DIR, f"model_{h}d.joblib")
        trained_models[f"{h}d"] = os.path.exists(path)

    return {
        "ai_status": status,
        "training_rows": int(rows),
        "required_min_rows": 1000,
        "ready_candidate_rows": 3000,
        "trained_models": trained_models,
        "horizons": HORIZONS
    }


def predict_latest(conn):
    raw = load_daily_candles(conn)

    if raw.empty:
        return []

    predictions = []

    for symbol, sdf in raw.groupby("symbol"):
        if len(sdf) < 60:
            continue

        sdf = build_features_for_symbol(sdf)
        latest = sdf.iloc[-1]

        X = latest[FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan).fillna(0)
        X = pd.DataFrame([X], columns=FEATURE_COLUMNS)

        horizon_probs = {}

        for h in HORIZONS:
            model_path = os.path.join(MODEL_DIR, f"model_{h}d.joblib")

            if not os.path.exists(model_path):
                horizon_probs[f"{h}d"] = None
                continue

            model = joblib.load(model_path)
            prob = model.predict_proba(X)[0][1]
            horizon_probs[f"{h}d"] = round(float(prob), 3)

        valid_probs = [p for p in horizon_probs.values() if p is not None]

        if valid_probs:
            best_prob = max(valid_probs)
            best_horizon = max(horizon_probs, key=lambda k: horizon_probs[k] if horizon_probs[k] is not None else -1)
        else:
            best_prob = None
            best_horizon = None

        if best_prob is None:
            recommendation = "AI_TRAINING"
        elif best_prob >= 0.70:
            recommendation = "AI_BUY"
        elif best_prob >= 0.58:
            recommendation = "AI_WATCH"
        else:
            recommendation = "AI_AVOID"

        predictions.append({
            "symbol": symbol,
            "last_close": round(float(latest["close"]), 3),
            "ai_recommendation": recommendation,
            "best_horizon": best_horizon,
            "best_probability": best_prob,
            "probabilities": horizon_probs,
            "features": {
                "trend_up": int(latest["trend_up"]),
                "trend_down": int(latest["trend_down"]),
                "breakout": int(latest["breakout"]),
                "wave1_reversal": int(latest["wave1_reversal"]),
                "wave2_pullback": int(latest["wave2_pullback"]),
                "accumulation": int(latest["accumulation"]),
                "volume_ratio": round(float(latest["volume_ratio"]), 3) if pd.notna(latest["volume_ratio"]) else None
            }
        })

    return sorted(
        predictions,
        key=lambda x: x["best_probability"] if x["best_probability"] is not None else 0,
        reverse=True
    )
