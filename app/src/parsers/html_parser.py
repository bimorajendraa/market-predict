"""
HTML Parser module for Finance Analytics.
Parses HTML financial reports to extract financial metrics.
"""

import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

from .metric_mapper import (
    map_account_to_metric,
    detect_unit_multiplier,
    detect_currency,
    normalize_value,
)
from .period_detector import detect_period

logger = logging.getLogger(__name__)


def parse_html_report(
    html_content: str,
    ticker: str,
    period: str,
    source_url: Optional[str] = None,
) -> list[dict]:
    """
    Parse an HTML financial report and extract standardized metrics.

    Args:
        html_content: Raw HTML content of the report
        ticker: Stock ticker symbol
        period: Reporting period (e.g., 'Q3-2025', 'FY-2024')
        source_url: URL where the report was fetched from

    Returns:
        List of dicts with keys: ticker, period, metric, value, unit, currency, source_url
    """
    soup = BeautifulSoup(html_content, "lxml")
    results = []

    # Detect unit multiplier from page context
    page_text = soup.get_text(" ", strip=True)
    multiplier = detect_unit_multiplier(page_text)
    currency = detect_currency(page_text)

    # Auto-detect period from page text if not provided
    if not period or period == "UNKNOWN":
        detected = detect_period(page_text, fallback=period)
        if detected and detected != period:
            logger.info(f"Auto-detected period: {detected}")
            period = detected

    # Final fallback to prevent null period
    if not period:
        period = "UNKNOWN"

    logger.info(
        f"Parsing HTML report for {ticker} ({period}), "
        f"multiplier={multiplier}, currency={currency}"
    )

    # Find all tables
    tables = soup.find_all("table")
    if not tables:
        logger.warning("No tables found in HTML report")
        return results

    for table_idx, table in enumerate(tables):
        table_results = _parse_table(
            table, ticker, period, multiplier, currency, source_url
        )
        results.extend(table_results)

    # Deduplicate: keep first occurrence of each metric
    seen_metrics = set()
    deduped = []
    for r in results:
        if r["metric"] not in seen_metrics:
            seen_metrics.add(r["metric"])
            deduped.append(r)

    logger.info(f"Extracted {len(deduped)} unique metrics from HTML report")
    return deduped


def _parse_table(
    table,
    ticker: str,
    period: str,
    multiplier: float,
    currency: str,
    source_url: Optional[str],
) -> list[dict]:
    """Parse a single HTML table for financial data."""
    results = []

    # Check if table has a caption or header with unit info
    caption = table.find("caption")
    if caption:
        table_text = caption.get_text()
        table_multiplier = detect_unit_multiplier(table_text)
        if table_multiplier != 1.0:
            multiplier = table_multiplier
        table_currency = detect_currency(table_text)
        if table_currency != "USD":
            currency = table_currency

    # Also check thead for unit info
    thead = table.find("thead")
    if thead:
        thead_text = thead.get_text()
        thead_multiplier = detect_unit_multiplier(thead_text)
        if thead_multiplier != 1.0:
            multiplier = thead_multiplier

    rows = table.find_all("tr")

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        # First cell is typically the account name
        account_name = cells[0].get_text(strip=True)
        if not account_name:
            continue

        # Try to map to standard metric
        metric = map_account_to_metric(account_name)
        if metric is None:
            continue

        # Try to extract value from remaining cells
        # Usually the most recent period value is in the second column
        for cell in cells[1:]:
            raw_value = cell.get_text(strip=True)
            if not raw_value or raw_value == "-" or raw_value == "—":
                continue

            value = normalize_value(raw_value, multiplier)
            if value is not None:
                results.append({
                    "ticker": ticker,
                    "period": period,
                    "metric": metric,
                    "value": value,
                    "unit": _multiplier_to_unit(multiplier),
                    "currency": currency,
                    "source_url": source_url,
                })
                break  # Take first valid value for this metric

    return results


def _multiplier_to_unit(multiplier: float) -> str:
    """Convert multiplier to human-readable unit label."""
    if multiplier >= 1_000_000_000:
        return "billions"
    elif multiplier >= 1_000_000:
        return "millions"
    elif multiplier >= 1_000:
        return "thousands"
    return "units"


def extract_tables_text(html_content: str) -> list[list[list[str]]]:
    """
    Extract all tables from HTML as lists of rows of cell texts.
    Useful for debugging / inspection.

    Returns:
        List of tables, each table is a list of rows,
        each row is a list of cell text strings.
    """
    soup = BeautifulSoup(html_content, "lxml")
    tables = []

    for table in soup.find_all("table"):
        rows = []
        for row in table.find_all("tr"):
            cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
            if cells:
                rows.append(cells)
        if rows:
            tables.append(rows)

    return tables
