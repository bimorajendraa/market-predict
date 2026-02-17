"""
Parsers package for Finance Analytics.
Provides HTML and PDF financial report parsing with metric standardization.
"""

from .html_parser import parse_html_report, extract_tables_text
from .pdf_parser import parse_pdf_report, parse_pdf_bytes
from .metric_mapper import (
    map_account_to_metric,
    detect_unit_multiplier,
    detect_currency,
    normalize_value,
)

__all__ = [
    "parse_html_report",
    "extract_tables_text",
    "parse_pdf_report",
    "parse_pdf_bytes",
    "map_account_to_metric",
    "detect_unit_multiplier",
    "detect_currency",
    "normalize_value",
]
