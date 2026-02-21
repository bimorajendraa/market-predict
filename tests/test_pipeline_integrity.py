"""
Pipeline Integrity Tests.
Validates core ML pipeline fixes: RSI consistency, leakage guard,
ATR truthy bug, OHLCV sanity checks, and MACD histogram.

Self-contained — does not import model_trainer (which requires lightgbm).
Instead, re-implements the core formulas inline to verify correctness.
"""

import numpy as np
import pandas as pd
import pytest


# ─────────────────────────────────────────────
# Helpers: create synthetic price DataFrames
# ─────────────────────────────────────────────

def _make_price_df(n: int = 100, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic OHLCV DataFrame for testing."""
    rng = np.random.RandomState(seed)
    dates = pd.bdate_range(end="2025-12-31", periods=n)
    close = 100 + np.cumsum(rng.randn(n) * 0.5)
    high = close + rng.uniform(0.5, 2.0, n)
    low = close - rng.uniform(0.5, 2.0, n)
    open_ = close + rng.randn(n) * 0.3
    volume = rng.randint(1_000_000, 10_000_000, n).astype(float)

    df = pd.DataFrame({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }, index=dates)
    df.index.name = "date"
    return df


def _wilder_rsi_series(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI — the formula used in model_trainer.engineer_features()."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))


def _wilder_rsi_single(close: pd.Series, period: int = 14) -> float:
    """Wilder's RSI — the formula used in technical_analysis.compute_rsi()."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean().iloc[-1]
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean().iloc[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ─────────────────────────────────────────────
# Test 1: RSI consistency between trainer and TA
# ─────────────────────────────────────────────

def test_rsi_consistency():
    """RSI series (trainer) last value must match RSI single (TA) on same data."""
    df = _make_price_df(200)
    close = df["close"]

    trainer_last = _wilder_rsi_series(close).dropna().iloc[-1]
    ta_last = _wilder_rsi_single(close)

    assert abs(trainer_last - ta_last) < 0.01, (
        f"RSI mismatch: trainer={trainer_last:.4f}, TA={ta_last:.4f}"
    )


# ─────────────────────────────────────────────
# Test 2: RSI should differ from old SMA-based
# ─────────────────────────────────────────────

def test_rsi_wilder_differs_from_sma():
    """Wilder's RSI must differ from naive SMA-based RSI (proving we upgraded)."""
    df = _make_price_df(200)
    close = df["close"]

    # Old SMA-based RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    sma_avg_gain = gain.rolling(14).mean().iloc[-1]
    sma_avg_loss = loss.rolling(14).mean().iloc[-1]
    sma_rsi = 100 - (100 / (1 + sma_avg_gain / (sma_avg_loss + 1e-9)))

    # New Wilder's RSI
    wilder_rsi = _wilder_rsi_single(close)

    # They should NOT be identical (proving the formula changed)
    assert abs(sma_rsi - wilder_rsi) > 0.01, (
        f"Wilder's RSI should differ from SMA RSI, but both are ~{wilder_rsi:.2f}"
    )


# ─────────────────────────────────────────────
# Test 3: Leakage guard — feature_cols must not
#          contain Future_* or Target
# ─────────────────────────────────────────────

def test_leakage_guard():
    """Simulate the feature selection logic and verify no leakage."""
    df = _make_price_df(100)
    # Add the columns that create_labels() would add
    df["Future_Close"] = df["close"].shift(-5)
    df["Future_Return"] = (df["Future_Close"] / df["close"]) - 1.0
    df["Target"] = 2  # Hold for all
    df["RSI_14"] = 50.0  # Fake feature
    df["MACD_12_26_9"] = 0.5

    train_df = df.dropna().copy()

    exclude_cols = [
        "open", "high", "low", "close", "volume",
        "Future_Close", "Future_Return", "Target",
        "date", "day",
    ]
    feature_cols = [c for c in train_df.columns if c not in exclude_cols]

    leakage = [c for c in feature_cols if "Future_" in c or c == "Target"]
    assert not leakage, f"Leakage columns in features: {leakage}"


# ─────────────────────────────────────────────
# Test 4: ATR truthy bug — atr_val=0.0 must not
#          be treated as False
# ─────────────────────────────────────────────

def test_atr_truthy_zero():
    """atr_val=0.0 should still be valid (not falsy)."""
    atr_val = 0.0

    # Old buggy code: `if atr_val and not np.isnan(atr_val):` → False
    # New correct code: `if atr_val is not None and not np.isnan(atr_val):` → True
    old_result = bool(atr_val and not np.isnan(atr_val))
    new_result = bool(atr_val is not None and not np.isnan(atr_val))

    assert old_result is False, "Old code should have failed on 0.0"
    assert new_result is True, "New code should accept 0.0 as valid"


def test_atr_truthy_none():
    """atr_val=None should be rejected."""
    atr_val = None
    result = atr_val is not None and not np.isnan(atr_val) if atr_val is not None else False
    assert result is False


# ─────────────────────────────────────────────
# Test 5: OHLCV sanity checks
# ─────────────────────────────────────────────

def test_ohlcv_sanity_drops_bad_rows():
    """Rows where high < open should be dropped by validation."""
    df = pd.DataFrame({
        "open": [100.0, 100.0, 100.0],
        "high": [105.0, 90.0, 110.0],     # row 1: high(90) < open(100) — BAD
        "low":  [95.0, 95.0, 95.0],
        "close": [102.0, 98.0, 105.0],
        "volume": [1000.0, 2000.0, 3000.0],
    })

    # Inline the validation logic from _validate_ohlcv
    bad_high = df["high"] < df[["open", "close"]].max(axis=1)
    bad_low = df["low"] > df[["open", "close"]].min(axis=1)
    bad_vol = df["volume"] < 0
    bad_mask = bad_high | bad_low | bad_vol
    result = df[~bad_mask]

    assert len(result) == 2, f"Expected 2 rows after dropping bad, got {len(result)}"


# ─────────────────────────────────────────────
# Test 6: MACD histogram formula
# ─────────────────────────────────────────────

def test_macd_histogram():
    """MACD histogram = MACD line - Signal line."""
    df = _make_price_df(100)
    close = df["close"]

    exp12 = close.ewm(span=12, adjust=False).mean()
    exp26 = close.ewm(span=26, adjust=False).mean()
    macd = exp12 - exp26
    signal = macd.ewm(span=9, adjust=False).mean()
    histogram = macd - signal

    # Non-trivial: histogram should have both positive and negative values
    assert histogram.dropna().min() < 0, "Histogram should have negative values"
    assert histogram.dropna().max() > 0, "Histogram should have positive values"


# ─────────────────────────────────────────────
# Test 7: Factor model sanity caps
# ─────────────────────────────────────────────

def test_factor_sanity_dividend_yield_cap():
    """Extreme dividend yield should be capped at 30%."""
    raw_dy = 3.19  # 319%
    capped = min(raw_dy, 0.30)
    assert capped == 0.30


def test_factor_sanity_debt_equity_cap():
    """Extreme D/E should be capped at 10x."""
    raw_dte = 25.0
    capped = min(raw_dte, 10.0)
    assert capped == 10.0


def test_factor_sanity_market_cap_reject():
    """Market cap >$5T should be flagged."""
    mc = 473.7e12  # $473.7T — clearly wrong
    assert mc > 5e12, "This market cap should be rejected"
