"""
PDF Parser module for Finance Analytics.
Parses PDF financial reports to extract financial metrics using pdfplumber.
"""

import logging
from typing import Optional

import pdfplumber

from .metric_mapper import (
    map_account_to_metric,
    detect_unit_multiplier,
    detect_currency,
    normalize_value,
)
from .period_detector import detect_period

logger = logging.getLogger(__name__)


def parse_pdf_report(
    pdf_path: str,
    ticker: str,
    period: str,
    source_url: Optional[str] = None,
) -> list[dict]:
    """
    Parse a PDF financial report and extract standardized metrics.

    Args:
        pdf_path: Path to the PDF file
        ticker: Stock ticker symbol
        period: Reporting period (e.g., 'Q3-2025', 'FY-2024')
        source_url: URL where the report was fetched from

    Returns:
        List of dicts with keys: ticker, period, metric, value, unit, currency, source_url
    """
    results = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # First pass: detect unit multiplier and currency from all pages
            full_text = ""
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                full_text += page_text + "\n"

            multiplier = detect_unit_multiplier(full_text)
            currency = detect_currency(full_text)

            # Auto-detect period from document text if not provided
            if not period or period == "UNKNOWN":
                detected = detect_period(full_text, fallback=period)
                if detected and detected != period:
                    logger.info(f"Auto-detected period: {detected}")
                    period = detected

            # Final fallback to prevent null period
            if not period:
                period = "UNKNOWN"

            logger.info(
                f"Parsing PDF report for {ticker} ({period}), "
                f"{len(pdf.pages)} pages, multiplier={multiplier}, currency={currency}"
            )

            # Second pass: extract tables from each page
            for page_num, page in enumerate(pdf.pages, 1):
                tables = page.extract_tables()
                if not tables:
                    continue

                for table_idx, table in enumerate(tables):
                    table_results = _parse_pdf_table(
                        table, ticker, period, multiplier, currency, source_url
                    )
                    results.extend(table_results)
                    logger.debug(
                        f"Page {page_num}, table {table_idx}: "
                        f"extracted {len(table_results)} metrics"
                    )

            # Fallback: if no tables found, try text-based extraction
            if not results:
                logger.info("No tables found, trying text-based extraction")
                results = _extract_from_text(
                    full_text, ticker, period, multiplier, currency, source_url
                )

    except Exception as e:
        logger.error(f"Failed to parse PDF: {e}")
        raise

    # Deduplicate: keep first occurrence of each metric
    seen_metrics = set()
    deduped = []
    for r in results:
        if r["metric"] not in seen_metrics:
            seen_metrics.add(r["metric"])
            deduped.append(r)

    logger.info(f"Extracted {len(deduped)} unique metrics from PDF report")
    return deduped


def _parse_pdf_table(
    table: list[list],
    ticker: str,
    period: str,
    multiplier: float,
    currency: str,
    source_url: Optional[str],
) -> list[dict]:
    """Parse a single table extracted from PDF."""
    results = []

    if not table or len(table) < 2:
        return results

    # Check header row for unit/currency info
    header_text = " ".join(str(cell or "") for cell in table[0])
    header_multiplier = detect_unit_multiplier(header_text)
    if header_multiplier != 1.0:
        multiplier = header_multiplier

    header_currency = detect_currency(header_text)
    if header_currency != "USD":
        currency = header_currency

    for row in table[1:]:  # Skip header row
        if not row or len(row) < 2:
            continue

        # First cell is account name
        account_name = str(row[0] or "").strip()
        if not account_name:
            continue

        metric = map_account_to_metric(account_name)
        if metric is None:
            continue

        # Try remaining cells for values
        for cell in row[1:]:
            raw_value = str(cell or "").strip()
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
                break

    return results


def _extract_from_text(
    text: str,
    ticker: str,
    period: str,
    multiplier: float,
    currency: str,
    source_url: Optional[str],
) -> list[dict]:
    """
    Fallback: extract financial data from plain text when tables are not available.
    Looks for patterns like 'Account Name ... 1,234,567'
    """
    import re

    results = []
    lines = text.split("\n")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Try to find account name followed by numbers
        # Pattern: "Account Name    1,234.56   2,345.67"
        parts = re.split(r"\s{2,}", line)
        if len(parts) < 2:
            continue

        account_name = parts[0].strip()
        metric = map_account_to_metric(account_name)
        if metric is None:
            continue

        # Try to get the first numeric value
        for part in parts[1:]:
            value = normalize_value(part.strip(), multiplier)
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
                break

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


def parse_pdf_bytes(
    pdf_bytes: bytes,
    ticker: str,
    period: str,
    source_url: Optional[str] = None,
) -> list[dict]:
    """
    Parse PDF from bytes (e.g., downloaded from MinIO).

    Args:
        pdf_bytes: Raw PDF bytes
        ticker: Stock ticker symbol
        period: Reporting period
        source_url: Source URL
    """
    import io
    import tempfile
    import os

    # Write bytes to temp file for pdfplumber
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    try:
        return parse_pdf_report(tmp_path, ticker, period, source_url)
    finally:
        os.unlink(tmp_path)
