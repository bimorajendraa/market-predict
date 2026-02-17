"""
ML Model Predictor.
Loads trained models and generates buy/sell signals for the latest data.
Includes Risk Management logic (ATR-based Stop Loss).
"""

import logging
import pickle
from pathlib import Path
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd

from .model_trainer import fetch_training_data, engineer_features

logger = logging.getLogger(__name__)

MODELS_DIR = Path("models")

LABEL_MAP = {
    0: "Strong Sell",
    1: "Sell",
    2: "Hold",
    3: "Buy",
    4: "Strong Buy"
}

def load_model(ticker: str) -> Optional[dict]:
    """Load trained model artifact."""
    model_path = MODELS_DIR / f"{ticker}_lgbm.pkl"
    if not model_path.exists():
        logger.warning(f"No trained model found for {ticker} at {model_path}")
        return None
        
    try:
        with open(model_path, "rb") as f:
            artifact = pickle.load(f)
        return artifact
    except Exception as e:
        logger.error(f"Failed to load model for {ticker}: {e}")
        return None


def predict_latest(ticker: str) -> Dict[str, Any]:
    """
    Generate prediction for the latest available date.
    Returns:
        {
            "signal": "Buy",
            "confidence": 0.75,
            "stop_loss": 12500,
            "atr": 250,
            "features": {...}
        }
    """
    # 1. Load Model
    artifact = load_model(ticker)
    if not artifact:
        return {"signal": "Unknown", "confidence": 0.0, "reason": "No Model"}
        
    model = artifact["model"]
    feature_cols = artifact["features"]
    
    # 2. Fetch & Engineer Data
    raw_df = fetch_training_data(ticker)
    if raw_df.empty:
        return {"signal": "Unknown", "confidence": 0.0, "reason": "No Data"}
        
    df = engineer_features(raw_df)
    
    # Get latest row
    latest_row = df.iloc[[-1]].copy()
    latest_date = latest_row.index[0]
    
    # Check if data is stale (warning only)
    # 
    
    # 3. Prepare Feature Vector — fill missing features with 0
    for col in feature_cols:
        if col not in latest_row.columns:
            logger.warning(f"Missing feature '{col}' in prediction data — filling with 0")
            latest_row[col] = 0.0

    try:
        X_latest = latest_row[feature_cols].fillna(0)
    except KeyError as e:
        logger.error(f"Feature mismatch: {e}")
        return {"signal": "Error", "confidence": 0.0, "reason": "Feature Mismatch"}
        
    # 4. Predict
    # LightGBM returns probabilities for multiclass
    probs = model.predict(X_latest)[0]
    pred_class = np.argmax(probs)
    confidence = float(np.max(probs))
    
    signal = LABEL_MAP.get(pred_class, "Hold")
    
    # 5. Risk Management (ATR Based)
    # Stop Loss suggestion: 2 * ATR below close (for Buy) or above (for Sell)
    close_price = latest_row["close"].values[0]

    atr_val = None
    for atr_col in ["ATRr_14", "ATR_14", "atr_14"]:
        if atr_col in latest_row.columns:
            val = latest_row[atr_col].values[0]
            if val is not None and not np.isnan(val):
                atr_val = val
                break
    
    stop_loss = None
    if atr_val and not np.isnan(atr_val):
        if pred_class >= 3: # Buy/Strong Buy
            stop_loss = close_price - (2.0 * atr_val)
        elif pred_class <= 1: # Sell/Strong Sell
            stop_loss = close_price + (2.0 * atr_val)
    
    return {
        "ticker": ticker,
        "date": latest_date.isoformat(),
        "signal": signal,
        "confidence": round(confidence, 2),
        "stop_loss": round(stop_loss, 2) if stop_loss else None,
        "atr": round(atr_val, 2) if atr_val and not np.isnan(atr_val) else None,
        "features": {k: float(v) for k, v in X_latest.iloc[0].to_dict().items() if k in ["RSI_14", "MACD_12_26_9", "VOL_REL"]}
    }

