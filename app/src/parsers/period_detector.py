"""
Period Detector module for Finance Analytics.
Detects reporting periods (e.g., 'Q3-2025', 'FY-2024') from financial report text.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Month → quarter mapping
MONTH_TO_QUARTER = {
    "january": 1, "february": 1, "march": 1,
    "april": 2, "may": 2, "june": 2,
    "july": 3, "august": 3, "september": 3,
    "october": 4, "november": 4, "december": 4,
    "jan": 1, "feb": 1, "mar": 1,
    "apr": 2, "jun": 2,
    "jul": 3, "aug": 3, "sep": 3, "sept": 3,
    "oct": 4, "nov": 4, "dec": 4,
}

# Fiscal quarter end months (3-month periods ending in these months)
QUARTER_END_MONTHS = {
    "march": 1, "mar": 1,
    "june": 2, "jun": 2,
    "september": 3, "sep": 3, "sept": 3,
    "december": 4, "dec": 4,
}


def detect_period(text: str, fallback: Optional[str] = None) -> Optional[str]:
    """
    Detect the reporting period from document text.

    Looks for patterns like:
    - "Q3 2025", "Q3-2025", "Q3 FY2025"
    - "Three Months Ended September 30, 2025"
    - "Fiscal Year 2024", "FY2024", "FY 2024"
    - "For the quarter ended June 30, 2025"
    - "10-K 2025", "10-Q Q1 2026"

    Args:
        text: Document text to analyze
        fallback: Fallback period if detection fails

    Returns:
        Period string like 'Q3-2025' or 'FY-2024', or fallback if not detected.
    """
    if not text:
        return fallback

    # Normalize whitespace
    text_clean = re.sub(r"\s+", " ", text[:10000])  # Only scan first ~10k chars

    # --- Pattern 0: URL-style fiscal quarter ---
    # "fy2025-q2", "FY25_Q2", "FY2026-Q1", "fy26-q1"
    m = re.search(
        r"\bFY[\-_]?(\d{2,4})[\-_]Q([1-4])\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        year_str, quarter = m.group(1), m.group(2)
        year = int(year_str)
        if year < 100:
            year += 2000
        return f"Q{quarter}-{year}"

    # --- Pattern 1: Explicit quarter notation ---
    # "Q3 2025", "Q3-2025", "Q1 FY2025", "Q3 FY 2025", "1Q2025", "1Q 2025"
    m = re.search(
        r"\bQ([1-4])[\s\-]*(?:FY[\s\-]?)?(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        quarter, year = m.group(1), m.group(2)
        return f"Q{quarter}-{year}"

    # "1Q2025", "1Q 2025", "3Q FY2025"
    m = re.search(
        r"\b([1-4])Q[\s\-]*(?:FY[\s\-]?)?(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        quarter, year = m.group(1), m.group(2)
        return f"Q{quarter}-{year}"

    # --- Pattern 2: SEC filing type with quarter ---
    # "10-Q Q1 2026", "10Q Q3 2025"
    m = re.search(
        r"\b10[\-]?Q\b.*?\bQ([1-4])[\s\-]?(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        quarter, year = m.group(1), m.group(2)
        return f"Q{quarter}-{year}"

    # --- Pattern 3: "Three/Six/Nine Months Ended <Month> <Day>, <Year>" ---
    m = re.search(
        r"\b(?:three|six|nine|3|6|9)\s+months?\s+ended\s+"
        r"(\w+)\s+\d{1,2},?\s+(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        month_str = m.group(1).lower()
        year = m.group(2)
        quarter = QUARTER_END_MONTHS.get(month_str)
        if quarter:
            return f"Q{quarter}-{year}"
        # Try general month → quarter mapping
        quarter = _month_to_quarter(month_str)
        if quarter:
            return f"Q{quarter}-{year}"

    # --- Pattern 4: "Quarter ended <Month> <Day>, <Year>" ---
    m = re.search(
        r"\bquarter\s+ended\s+(\w+)\s+\d{1,2},?\s+(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        month_str = m.group(1).lower()
        year = m.group(2)
        quarter = QUARTER_END_MONTHS.get(month_str)
        if quarter:
            return f"Q{quarter}-{year}"
        quarter = _month_to_quarter(month_str)
        if quarter:
            return f"Q{quarter}-{year}"

    # --- Pattern 5: "Year Ended <Month> <Day>, <Year>" or "Fiscal Year 2024" ---
    m = re.search(
        r"\b(?:fiscal\s+)?year\s+ended\s+\w+\s+\d{1,2},?\s+(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        year = m.group(1)
        return f"FY-{year}"

    # "Fiscal Year 2024", "FY2024", "FY 2024", "FY-2024"
    m = re.search(
        r"\b(?:fiscal\s+year|FY)[\s\-]?(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        year = m.group(1)
        return f"FY-{year}"

    # --- Pattern 6: 10-K with year (annual report) ---
    m = re.search(
        r"\b10[\-]?K\b.*?(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        year = m.group(1)
        if 2000 <= int(year) <= 2099:
            return f"FY-{year}"

    # --- Pattern 7: "Annual Report <Year>" ---
    m = re.search(
        r"\bannual\s+report\s+(\d{4})\b",
        text_clean, re.IGNORECASE,
    )
    if m:
        year = m.group(1)
        return f"FY-{year}"

    logger.debug("Could not auto-detect period from text")
    return fallback


def _month_to_quarter(month_str: str) -> Optional[int]:
    """Convert month name to quarter number."""
    return MONTH_TO_QUARTER.get(month_str.lower())
