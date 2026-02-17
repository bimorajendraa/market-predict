"""
Metric Mapper module for Finance Analytics.
Maps account names from financial reports to standardized metric names.
Handles bilingual (EN/ID) account names and unit normalization.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ============================================
# Standard metric names
# ============================================
STANDARD_METRICS = [
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "operating_cash_flow",
    "capex",
    "total_assets",
    "total_liabilities",
    "total_equity",
    "total_debt",
]

# ============================================
# Account name → metric mapping (EN + ID)
# Keys are lowercased for matching
# ============================================
ACCOUNT_MAP: dict[str, str] = {
    # Revenue
    "total revenue": "revenue",
    "revenue": "revenue",
    "net revenue": "revenue",
    "net sales": "revenue",
    "sales": "revenue",
    "total sales": "revenue",
    "pendapatan": "revenue",
    "pendapatan bersih": "revenue",
    "pendapatan usaha": "revenue",
    "penjualan bersih": "revenue",
    "penjualan": "revenue",
    # Gross Profit
    "gross profit": "gross_profit",
    "gross income": "gross_profit",
    "laba kotor": "gross_profit",
    "laba bruto": "gross_profit",
    # Operating Income
    "operating income": "operating_income",
    "operating profit": "operating_income",
    "income from operations": "operating_income",
    "operating earnings": "operating_income",
    "laba usaha": "operating_income",
    "laba operasi": "operating_income",
    "pendapatan operasional": "operating_income",
    # Net Income
    "net income": "net_income",
    "net profit": "net_income",
    "net earnings": "net_income",
    "profit for the period": "net_income",
    "profit for the year": "net_income",
    "net income attributable": "net_income",
    "laba bersih": "net_income",
    "laba tahun berjalan": "net_income",
    "laba periode berjalan": "net_income",
    # Operating Cash Flow
    "operating cash flow": "operating_cash_flow",
    "cash from operations": "operating_cash_flow",
    "net cash from operating": "operating_cash_flow",
    "net cash provided by operating": "operating_cash_flow",
    "cash flows from operating activities": "operating_cash_flow",
    "arus kas dari aktivitas operasi": "operating_cash_flow",
    "kas dari operasi": "operating_cash_flow",
    # Capital Expenditures
    "capital expenditures": "capex",
    "capital expenditure": "capex",
    "capex": "capex",
    "purchase of property": "capex",
    "purchases of property, plant and equipment": "capex",
    "belanja modal": "capex",
    "pengeluaran modal": "capex",
    # Total Assets
    "total assets": "total_assets",
    "total aset": "total_assets",
    "jumlah aset": "total_assets",
    "total aktiva": "total_assets",
    # Total Liabilities
    "total liabilities": "total_liabilities",
    "total liabilitas": "total_liabilities",
    "jumlah liabilitas": "total_liabilities",
    "total kewajiban": "total_liabilities",
    "jumlah kewajiban": "total_liabilities",
    # Total Equity
    "total equity": "total_equity",
    "total stockholders equity": "total_equity",
    "total shareholders equity": "total_equity",
    "stockholders equity": "total_equity",
    "total ekuitas": "total_equity",
    "jumlah ekuitas": "total_equity",
    # Total Debt
    "total debt": "total_debt",
    "total borrowings": "total_debt",
    "long-term debt": "total_debt",
    "total utang": "total_debt",
    "jumlah utang": "total_debt",
    "total pinjaman": "total_debt",
}

# ============================================
# Unit multipliers
# ============================================
UNIT_MULTIPLIERS: dict[str, float] = {
    # English
    "in thousands": 1_000,
    "in millions": 1_000_000,
    "in billions": 1_000_000_000,
    "thousands": 1_000,
    "millions": 1_000_000,
    "billions": 1_000_000_000,
    # Indonesian
    "dalam ribuan": 1_000,
    "dalam jutaan": 1_000_000,
    "dalam miliar": 1_000_000_000,
    "dalam miliaran": 1_000_000_000,
    "ribuan": 1_000,
    "jutaan": 1_000_000,
    "miliar": 1_000_000_000,
    "miliaran": 1_000_000_000,
}

# ============================================
# Currency detection patterns
# ============================================
CURRENCY_PATTERNS: dict[str, str] = {
    r"\bUSD\b": "USD",
    r"\$": "USD",
    r"\bIDR\b": "IDR",
    r"\bRp\.?\b": "IDR",
    r"\bRupiah\b": "IDR",
    r"\bEUR\b": "EUR",
    r"€": "EUR",
    r"\bGBP\b": "GBP",
    r"£": "GBP",
    r"\bJPY\b": "JPY",
    r"¥": "JPY",
}


def map_account_to_metric(account_name: str) -> Optional[str]:
    """
    Map a financial account name to a standardized metric.

    Args:
        account_name: Raw account name from report (EN or ID)

    Returns:
        Standardized metric name (e.g., 'revenue'), or None if not recognized.
    """
    cleaned = account_name.strip().lower()
    # Remove common noise
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"[:\-–—]$", "", cleaned).strip()

    # Direct match
    if cleaned in ACCOUNT_MAP:
        return ACCOUNT_MAP[cleaned]

    # Partial / contains match (for longer account names)
    for pattern, metric in ACCOUNT_MAP.items():
        if pattern in cleaned:
            return metric

    logger.debug(f"Unrecognized account: '{account_name}'")
    return None


def detect_unit_multiplier(text: str) -> float:
    """
    Detect unit multiplier from context text (e.g., 'in millions').

    Args:
        text: Context text containing unit info (e.g., table header or footnote)

    Returns:
        Multiplier value (default 1.0 if not detected)
    """
    text_lower = text.lower()
    for pattern, multiplier in UNIT_MULTIPLIERS.items():
        if pattern in text_lower:
            logger.debug(f"Detected unit multiplier: {pattern} = {multiplier}")
            return multiplier
    return 1.0


def detect_currency(text: str) -> str:
    """
    Detect currency from text using regex patterns.

    Args:
        text: Text to search for currency indicators

    Returns:
        Currency code (default 'USD')
    """
    for pattern, currency in CURRENCY_PATTERNS.items():
        if re.search(pattern, text):
            return currency
    return "USD"


def normalize_value(raw_value: str, multiplier: float = 1.0) -> Optional[float]:
    """
    Parse and normalize a financial value string.

    Args:
        raw_value: Raw string value from report (e.g., '1,234.56', '(500)', '-200')
        multiplier: Unit multiplier to apply

    Returns:
        Normalized float value, or None if parsing fails
    """
    if not raw_value or not raw_value.strip():
        return None

    cleaned = raw_value.strip()

    # Handle parentheses (negative numbers)
    is_negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        is_negative = True
        cleaned = cleaned[1:-1]

    # Handle explicit negative
    if cleaned.startswith("-"):
        is_negative = True
        cleaned = cleaned[1:]

    # Remove currency symbols and whitespace
    cleaned = re.sub(r"[^\d.,\-]", "", cleaned)

    if not cleaned:
        return None

    try:
        # Handle different number formats
        # If both , and . exist, determine which is decimal separator
        if "," in cleaned and "." in cleaned:
            # Format: 1,234.56 (English) or 1.234,56 (European)
            if cleaned.rindex(".") > cleaned.rindex(","):
                # English format: 1,234.56
                cleaned = cleaned.replace(",", "")
            else:
                # European format: 1.234,56
                cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "," in cleaned:
            # Could be thousands separator (1,234) or decimal (1,5)
            parts = cleaned.split(",")
            if len(parts[-1]) == 3:
                # Thousands separator
                cleaned = cleaned.replace(",", "")
            else:
                # Decimal separator
                cleaned = cleaned.replace(",", ".")

        value = float(cleaned) * multiplier
        return -value if is_negative else value

    except (ValueError, IndexError):
        logger.warning(f"Could not parse value: '{raw_value}'")
        return None


def normalize_unit(value: float, multiplier: float) -> tuple[float, str]:
    """
    Return the normalized value and its unit label.

    Args:
        value: Raw numeric value
        multiplier: Detected unit multiplier

    Returns:
        Tuple of (normalized_value, unit_label)
    """
    normalized = value * multiplier
    if multiplier >= 1_000_000_000:
        unit_label = "billions"
    elif multiplier >= 1_000_000:
        unit_label = "millions"
    elif multiplier >= 1_000:
        unit_label = "thousands"
    else:
        unit_label = "units"
    return normalized, unit_label
