"""
Technical Analysis module for Finance Analytics.
Computes actionable buy/sell price levels using technical indicators.

Includes:
- Support & Resistance from swing highs/lows
- Fibonacci Retracement levels
- Classic Pivot Points (PP, S1-S3, R1-R3)
- Buy/Sell Zone recommendations based on confluence
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from ..db import get_db_cursor

logger = logging.getLogger(__name__)


def _fetch_prices_df(ticker: str, days: int = 250) -> pd.DataFrame:
    """Fetch price data from DB and return as DataFrame."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM market_prices
            WHERE ticker = %(ticker)s
            ORDER BY date ASC
            """,
            {"ticker": ticker},
        )
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = df[c].astype(float)
    return df.tail(days)


def compute_support_resistance(
    df: pd.DataFrame, window: int = 5, num_levels: int = 3
) -> dict:
    """
    Find support and resistance levels from swing highs/lows.

    Uses a rolling window to detect local maxima (resistance) and
    local minima (support) in the price history.
    """
    if len(df) < window * 2:
        return {"support": [], "resistance": []}

    highs = df["high"].values
    lows = df["low"].values

    swing_highs = []
    swing_lows = []

    for i in range(window, len(highs) - window):
        # Swing High: highest in window
        if highs[i] == max(highs[i - window : i + window + 1]):
            swing_highs.append(float(highs[i]))
        # Swing Low: lowest in window
        if lows[i] == min(lows[i - window : i + window + 1]):
            swing_lows.append(float(lows[i]))

    # Cluster nearby levels (within 1.5% of each other)
    support = _cluster_levels(swing_lows, pct=0.015)
    resistance = _cluster_levels(swing_highs, pct=0.015)

    current_price = float(df["close"].iloc[-1])

    # Filter: support below price, resistance above
    support = sorted([s for s in support if s < current_price], reverse=True)[
        :num_levels
    ]
    resistance = sorted([r for r in resistance if r > current_price])[:num_levels]

    return {"support": support, "resistance": resistance}


def _cluster_levels(levels: list[float], pct: float = 0.015) -> list[float]:
    """Cluster price levels that are within pct of each other."""
    if not levels:
        return []

    sorted_levels = sorted(levels)
    clusters = []
    current_cluster = [sorted_levels[0]]

    for level in sorted_levels[1:]:
        if (level - current_cluster[0]) / current_cluster[0] <= pct:
            current_cluster.append(level)
        else:
            clusters.append(np.mean(current_cluster))
            current_cluster = [level]
    clusters.append(np.mean(current_cluster))

    return [round(c, 2) for c in clusters]


def compute_fibonacci(df: pd.DataFrame, lookback: int = 60) -> dict:
    """
    Compute Fibonacci retracement levels based on the high/low range
    over the lookback period.
    """
    recent = df.tail(lookback)
    if len(recent) < 10:
        return {}

    high = float(recent["high"].max())
    low = float(recent["low"].min())
    diff = high - low

    if diff < 0.01:
        return {}

    levels = {
        "high": round(high, 2),
        "low": round(low, 2),
        "fib_236": round(high - diff * 0.236, 2),
        "fib_382": round(high - diff * 0.382, 2),
        "fib_500": round(high - diff * 0.500, 2),
        "fib_618": round(high - diff * 0.618, 2),
        "fib_786": round(high - diff * 0.786, 2),
    }

    return levels


def compute_pivot_points(df: pd.DataFrame) -> dict:
    """
    Compute Classic Pivot Points from the most recent trading day.

    PP = (High + Low + Close) / 3
    S1 = 2*PP - High, S2 = PP - (High - Low), S3 = Low - 2*(High - PP)
    R1 = 2*PP - Low,  R2 = PP + (High - Low), R3 = High + 2*(PP - Low)
    """
    if len(df) < 1:
        return {}

    last = df.iloc[-1]
    h = float(last["high"])
    l = float(last["low"])
    c = float(last["close"])

    pp = (h + l + c) / 3

    return {
        "PP": round(pp, 2),
        "R1": round(2 * pp - l, 2),
        "R2": round(pp + (h - l), 2),
        "R3": round(h + 2 * (pp - l), 2),
        "S1": round(2 * pp - h, 2),
        "S2": round(pp - (h - l), 2),
        "S3": round(l - 2 * (h - pp), 2),
    }


def compute_buy_sell_zones(
    current_price: float,
    support: list[float],
    resistance: list[float],
    fibonacci: dict,
    pivot: dict,
) -> dict:
    """
    Compute recommended buy/sell zones based on confluence of technical levels.

    Buy zone: cluster of support + Fibonacci + pivot support levels
    Sell zone: cluster of resistance + Fibonacci + pivot resistance levels
    """
    buy_levels = []
    sell_levels = []

    # Collect all support-like levels
    buy_levels.extend(support)
    for key in ["fib_618", "fib_786"]:
        if key in fibonacci and fibonacci[key] < current_price:
            buy_levels.append(fibonacci[key])
    for key in ["S1", "S2"]:
        if key in pivot and pivot[key] < current_price:
            buy_levels.append(pivot[key])

    # Collect all resistance-like levels
    sell_levels.extend(resistance)
    for key in ["fib_236", "fib_382"]:
        if key in fibonacci and fibonacci[key] > current_price:
            sell_levels.append(fibonacci[key])
    for key in ["R1", "R2"]:
        if key in pivot and pivot[key] > current_price:
            sell_levels.append(pivot[key])

    # Determine zones
    buy_zone = None
    sell_zone = None

    if buy_levels:
        buy_sorted = sorted(buy_levels, reverse=True)
        # Best buy zone = highest support cluster (closest to price)
        buy_zone = {
            "ideal": round(buy_sorted[0], 2),
            "range_low": round(buy_sorted[-1], 2) if len(buy_sorted) > 1 else round(buy_sorted[0] * 0.98, 2),
            "range_high": round(buy_sorted[0], 2),
        }

    if sell_levels:
        sell_sorted = sorted(sell_levels)
        # Best sell zone = lowest resistance cluster (closest to price)
        sell_zone = {
            "ideal": round(sell_sorted[0], 2),
            "range_low": round(sell_sorted[0], 2),
            "range_high": round(sell_sorted[-1], 2) if len(sell_sorted) > 1 else round(sell_sorted[0] * 1.02, 2),
        }

    return {"buy_zone": buy_zone, "sell_zone": sell_zone}


# ============================================
# Trend, Volatility, & Alert Indicators
# ============================================

def compute_moving_averages(df: pd.DataFrame) -> dict:
    """Compute MA20, MA50, and trend direction from crossover."""
    if len(df) < 50:
        return {}

    close = df["close"]
    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1])

    if ma20 > ma50:
        trend = "Bullish"
        cross = "Golden Cross (MA20 > MA50)"
    else:
        trend = "Bearish"
        cross = "Death Cross (MA20 < MA50)"

    return {
        "ma20": round(ma20, 2),
        "ma50": round(ma50, 2),
        "trend": trend,
        "cross_description": cross,
    }


def compute_rsi(df: pd.DataFrame, period: int = 14) -> dict:
    """Compute RSI(14) and classify overbought/oversold/neutral."""
    if len(df) < period + 1:
        return {}

    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.rolling(period).mean().iloc[-1]
    avg_loss = loss.rolling(period).mean().iloc[-1]

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    if rsi > 70:
        zone = "Overbought"
    elif rsi > 55:
        zone = "Strong"
    elif rsi > 45:
        zone = "Neutral"
    elif rsi > 30:
        zone = "Weak"
    else:
        zone = "Oversold"

    return {"rsi": round(rsi, 1), "rsi_zone": zone}


def compute_atr_regime(df: pd.DataFrame, atr_period: int = 14, lookback: int = 90) -> dict:
    """
    Compute current ATR and its percentile rank over the lookback window.
    Classifies volatility regime as High / Normal / Low.
    """
    if len(df) < atr_period + lookback:
        trimmed = df.copy()
    else:
        trimmed = df.tail(atr_period + lookback).copy()

    if len(trimmed) < atr_period + 1:
        return {}

    high = trimmed["high"]
    low = trimmed["low"]
    close = trimmed["close"]
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    atr_series = tr.rolling(atr_period).mean().dropna()

    if atr_series.empty:
        return {}

    current_atr = float(atr_series.iloc[-1])
    percentile = float((atr_series < current_atr).mean() * 100)

    if percentile >= 75:
        regime = "High"
        description = "Market is volatile — wider stops recommended"
    elif percentile <= 25:
        regime = "Low"
        description = "Market is calm — tighter stops possible"
    else:
        regime = "Normal"
        description = "Average volatility"

    return {
        "atr": round(current_atr, 2),
        "atr_percentile": round(percentile, 1),
        "volatility_regime": regime,
        "volatility_description": description,
    }


def generate_alerts(
    current_price: float,
    buy_zone: dict | None,
    sell_zone: dict | None,
    support: list[float],
    resistance: list[float],
    rsi_data: dict,
    ma_data: dict,
) -> list[str]:
    """Generate actionable alert strings based on current conditions."""
    alerts: list[str] = []

    # Buy Zone Alert
    if buy_zone:
        lo = buy_zone.get("range_low", 0)
        hi = buy_zone.get("range_high", 0)
        if lo <= current_price <= hi:
            alerts.append("🟢 Price in Buy Zone")

    # Sell Zone Alert
    if sell_zone:
        lo = sell_zone.get("range_low", 0)
        hi = sell_zone.get("range_high", 0)
        if lo <= current_price <= hi:
            alerts.append("🔴 Price in Sell Zone")

    # Resistance Break
    if resistance and current_price > resistance[0]:
        alerts.append(f"🚀 Broke Resistance {resistance[0]}")

    # Support Break
    if support and current_price < support[0]:
        alerts.append(f"⚠️ Broke Support {support[0]}")

    # RSI Alerts
    zone = rsi_data.get("rsi_zone", "")
    if zone == "Oversold":
        alerts.append(f"📉 RSI Oversold ({rsi_data['rsi']})")
    elif zone == "Overbought":
        alerts.append(f"📈 RSI Overbought ({rsi_data['rsi']})")

    # MA Cross
    trend = ma_data.get("trend", "")
    if trend == "Bullish":
        alerts.append("📊 Bullish Trend (MA20 > MA50)")
    elif trend == "Bearish":
        alerts.append("📊 Bearish Trend (MA20 < MA50)")

    return alerts


def run_technical_analysis(ticker: str) -> dict:
    """
    Run full technical analysis for a ticker.

    Returns dict with:
    - current_price
    - support, resistance
    - fibonacci levels
    - pivot_points
    - buy_zone, sell_zone
    - trend (MA20/MA50)
    - rsi
    - volatility_regime (ATR percentile)
    - alerts
    """
    df = _fetch_prices_df(ticker, days=250)

    if df.empty or len(df) < 20:
        logger.warning(f"Insufficient price data for technical analysis of {ticker}")
        return {
            "ticker": ticker,
            "status": "insufficient_data",
            "current_price": None,
        }

    current_price = float(df["close"].iloc[-1])

    # Core computations
    sr = compute_support_resistance(df, window=5, num_levels=3)
    fib = compute_fibonacci(df, lookback=60)
    pivot = compute_pivot_points(df)
    zones = compute_buy_sell_zones(
        current_price, sr["support"], sr["resistance"], fib, pivot
    )

    # Trend & Momentum
    ma_data = compute_moving_averages(df)
    rsi_data = compute_rsi(df, period=14)
    atr_data = compute_atr_regime(df, atr_period=14, lookback=90)

    # Alert Rules
    alerts = generate_alerts(
        current_price,
        zones["buy_zone"],
        zones["sell_zone"],
        sr["support"],
        sr["resistance"],
        rsi_data,
        ma_data,
    )

    return {
        "ticker": ticker,
        "status": "ok",
        "current_price": round(current_price, 2),
        "support": sr["support"],
        "resistance": sr["resistance"],
        "fibonacci": fib,
        "pivot_points": pivot,
        "buy_zone": zones["buy_zone"],
        "sell_zone": zones["sell_zone"],
        "trend": ma_data,
        "rsi": rsi_data,
        "volatility": atr_data,
        "alerts": alerts,
    }
