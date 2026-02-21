"""
ML Model Trainer for Stock Recommendation.
Handles data fetching, feature engineering, labeling, and training LightGBM models.
"""

import logging
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, Any

import lightgbm as lgb
import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import TimeSeriesSplit

from ..db import get_db_cursor
from ..config import config

logger = logging.getLogger(__name__)

MODELS_DIR = Path("models")
MODELS_DIR.mkdir(exist_ok=True)

# Suppress LightGBM verbose logging
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="lightgbm")


def fetch_extended_prices(ticker: str) -> int:
    """
    Fetch maximum available historical prices from yfinance and upsert into market_prices.
    Uses period="max" to get all data since IPO for ML training.

    Returns:
        Number of records upserted.
    """
    logger.info(f"Fetching max available history for {ticker} from yfinance...")

    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="max", interval="1d")
        if df.empty:
            logger.warning(f"No extended price data returned for {ticker}")
            return 0

        count = 0
        with get_db_cursor() as cur:
            for date_idx, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO market_prices
                        (ticker, date, open, high, low, close, volume)
                    VALUES
                        (%(ticker)s, %(date)s, %(open)s, %(high)s,
                         %(low)s, %(close)s, %(volume)s)
                    ON CONFLICT (ticker, date)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                    """,
                    {
                        "ticker": ticker,
                        "date": date_idx.date(),
                        "open": round(float(row["Open"]), 4),
                        "high": round(float(row["High"]), 4),
                        "low": round(float(row["Low"]), 4),
                        "close": round(float(row["Close"]), 4),
                        "volume": int(row["Volume"]),
                    },
                )
                count += 1

        logger.info(f"Upserted {count} extended price records for {ticker}")
        return count

    except Exception as e:
        logger.error(f"Failed to fetch extended prices for {ticker}: {e}")
        return 0


def fetch_training_data(ticker: str) -> pd.DataFrame:
    """
    Fetch and merge market price, sentiment, and fundamental data.
    Returns a DataFrame with Date index and raw features.
    """
    # 1. Fetch Market Prices (Daily)
    query_prices = """
        SELECT date, open, high, low, close, volume
        FROM market_prices
        WHERE ticker = %(ticker)s
        ORDER BY date ASC
    """
    with get_db_cursor() as cur:
        cur.execute(query_prices, {"ticker": ticker})
        prices = cur.fetchall()

    if not prices:
        logger.warning(f"No price data found for {ticker}")
        return pd.DataFrame()

    df = pd.DataFrame(prices)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df = df.sort_index()
    
    # Ensure numeric types (convert from Decimal)
    cols = ["open", "high", "low", "close", "volume"]
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(float)

    # 2. Fetch Sentiment (Aggregated Daily)
    query_sentiment = """
        SELECT date::date as day, AVG(impact) as sentiment_score, COUNT(*) as news_count
        FROM news_sentiment
        WHERE ticker = %(ticker)s
        GROUP BY day
        ORDER BY day ASC
    """
    with get_db_cursor() as cur:
        cur.execute(query_sentiment, {"ticker": ticker})
        sentiment_data = cur.fetchall()

    if sentiment_data:
        sent_df = pd.DataFrame(sentiment_data)
        sent_df["day"] = pd.to_datetime(sent_df["day"])
        sent_df.set_index("day", inplace=True)
        # Verify columns exist before merge
        if "sentiment_score" not in sent_df.columns:
            sent_df["sentiment_score"] = 0.0
    else:
        sent_df = pd.DataFrame()

    # Merge Sentiment (Left Join)
    if not sent_df.empty:
        df = df.join(sent_df, how="left")

    # Ensure columns exist even if no sentiment data was available
    if "sentiment_score" not in df.columns:
        df["sentiment_score"] = 0.0
    if "news_count" not in df.columns:
        df["news_count"] = 0

    df["sentiment_score"] = df["sentiment_score"].fillna(0.0).astype(float)
    df["news_count"] = df["news_count"].fillna(0).astype(int)

    # 3. Fetch Financial Scores (Quarterly -> Daily ffill)
    query_scores = """
        SELECT created_at::date as day, score
        FROM scores_financial
        WHERE ticker = %(ticker)s
        ORDER BY created_at ASC
    """
    with get_db_cursor() as cur:
        cur.execute(query_scores, {"ticker": ticker})
        scores_data = cur.fetchall()

    if scores_data:
        score_df = pd.DataFrame(scores_data)
        score_df["day"] = pd.to_datetime(score_df["day"])
        score_df = score_df.set_index("day").sort_index()
        # Group duplicates by taking the latest
        score_df = score_df[~score_df.index.duplicated(keep='last')]
    else:
        score_df = pd.DataFrame()

    # Merge Scores (ffill)
    if not score_df.empty:
        df = df.join(score_df, how="left")

    if "score" not in df.columns:
        df["score"] = 50.0

    df["score"] = df["score"].ffill().fillna(50.0).astype(float)  # Default neutral score

    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate technical indicators, lag features, and targets (Manual Implementation).
    """
    if df.empty:
        return df

    # 1. Momentum: RSI (14) — Wilder's smoothing (EWM)
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    # Wilder's smoothing: EWM with alpha=1/period
    avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()

    rs = avg_gain / (avg_loss + 1e-9)
    df["RSI_14"] = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    exp12 = df["close"].ewm(span=12, adjust=False).mean()
    exp26 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD_12_26_9"] = exp12 - exp26
    df["MACDs_12_26_9"] = df["MACD_12_26_9"].ewm(span=9, adjust=False).mean()
    df["MACDh_12_26_9"] = df["MACD_12_26_9"] - df["MACDs_12_26_9"]

    # 2. Volatility: ATR (14) & BBands (20, 2)
    # ATR
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["ATR_14"] = tr.rolling(window=14, min_periods=14).mean()
    
    # BBands
    sma20 = df["close"].rolling(window=20).mean()
    std20 = df["close"].rolling(window=20).std()
    df["BBU_20_2.0"] = sma20 + (2 * std20)
    df["BBL_20_2.0"] = sma20 - (2 * std20)
    
    # 3. Trend: ADX is complex, skipping for manual impl.
    # Use SMA alignment instead.
    df["SMA_50"] = df["close"].rolling(window=50).mean()
    df["SMA_200"] = df["close"].rolling(window=200).mean()
    df["Trend_Alignment"] = (df["SMA_50"] > df["SMA_200"]).astype(int)
    
    # 4. Volume
    df["VOL_SMA_20"] = df["volume"].rolling(20).mean()
    df["VOL_REL"] = df["volume"] / (df["VOL_SMA_20"] + 1e-9)

    # 5. Returns & Logs
    df["RET_1d"] = df["close"].pct_change(1)
    df["RET_5d"] = df["close"].pct_change(5)
    df["RET_20d"] = df["close"].pct_change(20)
    
    df["LOG_RET_1d"] = np.log(df["close"] / df["close"].shift(1))
    
    # 6. Sentiment Moving Averages
    df["SENT_MA_7"] = df["sentiment_score"].rolling(7).mean().fillna(0)
    df["SENT_MA_30"] = df["sentiment_score"].rolling(30).mean().fillna(0)

    # 7. Interaction Features
    df["RSI_x_SENT"] = df["RSI_14"] * df["sentiment_score"]

    return df


def create_labels(df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
    """
    Create classification labels based on future returns.
    Target: Return over next 'horizon' days.
    """
    # Future Close
    df["Future_Close"] = df["close"].shift(-horizon)
    df["Future_Return"] = (df["Future_Close"] / df["close"]) - 1.0
    
    # Labeling Logic
    # Strong Buy: > 5%
    # Buy: 2% to 5%
    # Hold: -2% to 2%
    # Sell: -5% to -2%
    # Strong Sell: < -5%
    
    def label_logic(ret):
        if pd.isna(ret):
            return np.nan
        if ret > 0.05:
            return 4  # Strong Buy
        if ret > 0.02:
            return 3  # Buy
        if ret > -0.02:
            return 2  # Hold
        if ret > -0.05:
            return 1  # Sell
        return 0  # Strong Sell

    # Using integer map: 
    # 0: Strong Sell
    # 1: Sell
    # 2: Hold
    # 3: Buy
    # 4: Strong Buy

    df["Target"] = df["Future_Return"].apply(label_logic)
    
    return df


def _validate_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Data sanity checks on OHLCV data.
    Logs warnings and drops anomalous rows.
    """
    n_before = len(df)

    # high must be >= max(open, close); low must be <= min(open, close)
    bad_high = df["high"] < df[["open", "close"]].max(axis=1)
    bad_low = df["low"] > df[["open", "close"]].min(axis=1)
    bad_vol = df["volume"] < 0

    bad_mask = bad_high | bad_low | bad_vol
    n_bad = bad_mask.sum()

    if n_bad > 0:
        logger.warning(
            f"OHLCV sanity: {n_bad}/{n_before} rows have anomalous data "
            f"(bad_high={bad_high.sum()}, bad_low={bad_low.sum()}, "
            f"bad_vol={bad_vol.sum()}). Dropping them."
        )
        df = df[~bad_mask].copy()

    return df


def train_model(ticker: str) -> dict:
    """
    Fetch data, engineer features, and train LightGBM model.
    Auto-fetches extended historical data if DB has insufficient rows.
    Saves model to disk.
    """
    # Set seed for reproducibility
    SEED = 42
    np.random.seed(SEED)

    logger.info(f"Starting model training for {ticker}")

    # 0. Ensure enough historical data exists
    with get_db_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) as c FROM market_prices WHERE ticker = %(t)s",
            {"t": ticker},
        )
        row_count = cur.fetchone()["c"]

    if row_count < 200:
        logger.info(
            f"Only {row_count} price rows in DB for {ticker}, "
            f"fetching 2y extended history..."
        )
        fetched = fetch_extended_prices(ticker)
        logger.info(f"Extended fetch complete: {fetched} records upserted")

    # 1. Prepare Data
    raw_df = fetch_training_data(ticker)
    if len(raw_df) < 60:
        logger.warning("Insufficient data for training (need > 60 days)")
        return {"status": "failed", "reason": "Insufficient data"}

    # Data sanity checks
    raw_df = _validate_ohlcv(raw_df)

    df = engineer_features(raw_df)
    df = create_labels(df, horizon=5)

    # Drop rows with NaN targets (last 5 days) or NaN features
    train_df = df.dropna().copy()

    if len(train_df) < 50:
        logger.warning(f"Not enough training samples after cleaning: {len(train_df)}")
        return {"status": "failed", "reason": "Insufficient samples"}

    # Features (Exclude Target and auxiliary columns)
    exclude_cols = [
        "open", "high", "low", "close", "volume",
        "Future_Close", "Future_Return", "Target",
        "date", "day",
    ]
    feature_cols = [c for c in train_df.columns if c not in exclude_cols]

    # Leakage guard — fail hard if future-looking columns leaked into features
    leakage_cols = [c for c in feature_cols if "Future_" in c or c == "Target"]
    assert not leakage_cols, (
        f"DATA LEAKAGE DETECTED! These columns must not be features: {leakage_cols}"
    )

    X = train_df[feature_cols]
    y = train_df["Target"].astype(int)

    # Log class distribution
    class_dist = y.value_counts().sort_index()
    logger.info(f"Class distribution:\n{class_dist.to_string()}")
    
    # 2. Train with TimeSeriesSplit
    tscv = TimeSeriesSplit(n_splits=3)
    
    params = {
        'objective': 'multiclass',
        'num_class': 5,
        'metric': 'multi_logloss',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.9,
        'verbose': -1,
        'seed': SEED,
        'bagging_seed': SEED,
        'feature_fraction_seed': SEED,
    }
    
    fold_accuracies = []
    fold_f1_scores = []
    models = []
    
    logger.info(f"Training on {len(X)} samples with {len(feature_cols)} features...")
    
    for train_index, val_index in tscv.split(X):
        X_train, X_val = X.iloc[train_index], X.iloc[val_index]
        y_train, y_val = y.iloc[train_index], y.iloc[val_index]
        
        train_data = lgb.Dataset(X_train, label=y_train)
        val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
        
        bst = lgb.train(
            params,
            train_data,
            num_boost_round=100,
            valid_sets=[val_data],
            callbacks=[
                lgb.early_stopping(stopping_rounds=10, verbose=False),
                lgb.log_evaluation(period=0) # Suppress log
            ] 
        )
        
        preds = bst.predict(X_val)
        pred_labels = np.argmax(preds, axis=1)
        acc = accuracy_score(y_val, pred_labels)
        f1 = f1_score(y_val, pred_labels, average='macro', zero_division=0)
        fold_accuracies.append(acc)
        fold_f1_scores.append(f1)
        models.append(bst)

    avg_acc = np.mean(fold_accuracies)
    avg_f1 = np.mean(fold_f1_scores)
    logger.info(f"Average CV Accuracy: {avg_acc:.2%} | Macro-F1: {avg_f1:.4f}")
    
    # 3. Final Training on Full Data
    full_train_data = lgb.Dataset(X, label=y)
    final_model = lgb.train(params, full_train_data, num_boost_round=100)
    
    # Feature Importance
    importance = final_model.feature_importance(importance_type='gain')
    feat_imp = pd.DataFrame({'Feature': feature_cols, 'Gain': importance}).sort_values(by='Gain', ascending=False)
    top_features = feat_imp.head(10).to_dict(orient='records')

    # Save Model
    save_path = MODELS_DIR / f"{ticker}_lgbm.pkl"
    with open(save_path, "wb") as f:
        pickle.dump({
            "model": final_model,
            "features": feature_cols,
            "timestamp": datetime.now().isoformat(),
            "metrics": {"cv_accuracy": avg_acc, "cv_macro_f1": avg_f1},
            "class_distribution": class_dist.to_dict(),
        }, f)
        
    logger.info(f"Model saved to {save_path}")
    
    return {
        "status": "success",
        "path": str(save_path),
        "accuracy": avg_acc,
        "macro_f1": avg_f1,
        "top_features": top_features,
        "class_distribution": class_dist.to_dict(),
    }

if __name__ == "__main__":
    # Test run
    # logging.basicConfig(level=logging.INFO)
    # train_model("BBCA.JK")
    pass
