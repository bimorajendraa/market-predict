"""
Currency & Unit Utilities for multi-market financial data.
Handles IDR/USD detection, display formatting, and sanity checks.
"""

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


# ── Currency Detection ──

IDR_SUFFIXES = {".JK", ".JKT"}
# Approximate IDR/USD rate — used only for sanity checks, not conversion
IDRRUSD_APPROX = 15_500


def detect_currency(ticker: str) -> str:
    """
    Detect display currency based on ticker suffix.
    Returns 'IDR' for Jakarta-listed tickers, 'USD' otherwise.
    """
    upper = ticker.upper()
    for suffix in IDR_SUFFIXES:
        if upper.endswith(suffix):
            return "IDR"
    return "USD"


# ── Display Formatting ──

_UNITS = [
    (1e12, "T"),
    (1e9, "B"),
    (1e6, "M"),
    (1e3, "K"),
]

_CURRENCY_SYMBOLS = {
    "IDR": "Rp",
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "SGD": "S$",
}

SCALE_MAP = {
    "1": 1.0,
    "K": 1e3,
    "M": 1e6,
    "B": 1e9,
    "T": 1e12,
}


def format_financial(
    value: Optional[float],
    currency: str = "USD",
    unit: str = "auto",
    decimals: int = 1,
) -> str:
    """
    Format a financial value with correct currency symbol and magnitude.

    Args:
        value: The numeric value (in base units, e.g. raw IDR or USD)
        currency: Currency code ('IDR', 'USD', etc.)
        unit: 'auto' to pick best unit, or 'T'/'B'/'M'/'K'/''
        decimals: Number of decimal places
    
    Returns:
        Formatted string like 'Rp 478.3 T' or '$32.5 B'
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"

    symbol = _CURRENCY_SYMBOLS.get(currency, currency)

    if unit == "auto":
        abs_val = abs(value)
        for threshold, label in _UNITS:
            if abs_val >= threshold:
                return f"{symbol} {value / threshold:,.{decimals}f} {label}"
        # Small value — no unit suffix
        return f"{symbol} {value:,.{decimals}f}"
    else:
        unit_map = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3, "": 1}
        divisor = unit_map.get(unit, 1)
        return f"{symbol} {value / divisor:,.{decimals}f} {unit}".rstrip()


def format_price(
    value: Optional[float],
    currency: str = "USD",
) -> str:
    """Format a share price with currency symbol."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    symbol = _CURRENCY_SYMBOLS.get(currency, currency)
    # IDR prices typically shown as integers
    if currency == "IDR":
        return f"{symbol} {value:,.0f}"
    return f"{symbol}{value:,.2f}"


def format_percent(value: Optional[float], decimals: int = 1) -> str:
    """Format as percentage."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    return f"{value * 100:.{decimals}f}%" if abs(value) < 1 else f"{value:.{decimals}f}%"


def normalize_with_scale(
    value: Optional[float],
    scale: str = "1",
) -> Optional[float]:
    """Normalize value into base unit using declared scale metadata."""
    if value is None:
        return None
    factor = SCALE_MAP.get((scale or "1").upper(), 1.0)
    return float(value) * factor


def infer_scale_for_value(value: Optional[float], currency: str = "USD") -> str:
    """Infer compact scale label for metadata storage and display."""
    if value is None:
        return "1"
    v = abs(float(value))
    if currency == "IDR":
        if v >= 1e12:
            return "T"
        if v >= 1e9:
            return "B"
        if v >= 1e6:
            return "M"
        return "1"
    if v >= 1e12:
        return "T"
    if v >= 1e9:
        return "B"
    if v >= 1e6:
        return "M"
    return "1"


def format_idr_trillion(value: Optional[float], decimals: int = 2) -> str:
    """Standard formatter for Indonesia memo normalization: IDR trillion."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    return f"Rp {float(value)/1e12:,.{decimals}f} T"


# ── Sanity Checks ──

# Maximum reasonable market caps by currency (in base units)
_MAX_MARKET_CAP = {
    "IDR": 5_000e12,   # Rp 5,000 T ≈ $322B (reasonable for BCA/BMRI)
    "USD": 5e12,       # $5T
}

_MAX_MARKET_CAP_ID = {
    "banking": 6_500e12,
    "tower_infra": 1_500e12,
    "consumer": 2_500e12,
    "commodities": 2_500e12,
    "general": 5_000e12,
}

# Maximum reasonable dividend yields
_MAX_DIVIDEND_YIELD = 0.30  # 30%

# Maximum reasonable D/E
_MAX_DEBT_TO_EQUITY = 10.0


def market_cap_sanity(
    value: float,
    currency: str,
    ticker: str = "",
    sector: str = "general",
) -> tuple[Optional[float], bool]:
    """
    Check if market cap is reasonable for the given currency.

    Returns:
        (value_or_None, is_suspect)
        - If suspect: returns (None, True)
        - If OK: returns (value, False)
    """
    if value is None:
        return None, False

    if currency == "IDR":
        max_cap = _MAX_MARKET_CAP_ID.get(sector, _MAX_MARKET_CAP_ID["general"])
    else:
        max_cap = _MAX_MARKET_CAP.get(currency, _MAX_MARKET_CAP["USD"])

    if value <= 0 or value > max_cap:
        logger.warning(
            f"Suspect market_cap={value:,.0f} {currency} for {ticker} "
            f"(max={max_cap:,.0f}) — dropping"
        )
        return None, True

    return value, False


def sanitize_metrics(
    metrics: dict,
    currency: str,
    ticker: str = "",
    sector: str = "general",
) -> dict:
    """
    Apply currency-aware sanity checks to financial metrics.
    Modifies in-place and returns the dict.
    """
    # Market cap
    mc = metrics.get("market_cap")
    if mc is not None:
        mc_clean, suspect = market_cap_sanity(mc, currency, ticker, sector=sector)
        if suspect:
            metrics.pop("market_cap", None)
        else:
            metrics["market_cap"] = mc_clean

    # Dividend yield
    dy = metrics.get("dividend_yield")
    if dy is not None and dy > _MAX_DIVIDEND_YIELD:
        logger.warning(
            f"Suspect dividend_yield={dy:.2%} for {ticker} — capping at {_MAX_DIVIDEND_YIELD:.0%}"
        )
        metrics["dividend_yield"] = _MAX_DIVIDEND_YIELD

    # Debt-to-equity
    dte = metrics.get("debt_to_equity")
    if dte is not None and dte > _MAX_DEBT_TO_EQUITY:
        logger.warning(
            f"Extreme debt_to_equity={dte:.2f} for {ticker} — capping at {_MAX_DEBT_TO_EQUITY}"
        )
        metrics["debt_to_equity"] = _MAX_DEBT_TO_EQUITY

    # Store currency for downstream
    metrics["_currency"] = currency

    return metrics
