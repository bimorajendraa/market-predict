"""
Coverage Contracts — minimum metrics required per sector.
If contract fails, memo is still generated but rating is locked + confidence penalized.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================
# Contract Definitions
# ============================================

SECTOR_CONTRACTS: dict[str, dict] = {
    "tech": {
        "required": [
            "revenue_growth", "operating_margin", "net_margin",
            "fcf_margin", "debt_to_equity",
        ],
        "recommended": [
            "eps_growth", "revenue_qoq", "share_count",
            "roic", "capex_intensity",
        ],
        "min_required": 4,
    },
    "banking": {
        "required": [
            "roe", "net_interest_margin", "non_performing_loan",
            "capital_adequacy_ratio", "cost_to_income",
        ],
        "recommended": [
            "casa_ratio", "loan_growth", "deposit_growth",
            "loan_to_deposit_ratio", "roa",
        ],
        "min_required": 3,
    },
    "consumer": {
        "required": [
            "revenue_growth", "operating_margin", "roe",
            "current_ratio", "debt_to_equity",
        ],
        "recommended": [
            "gross_margin", "dividend_yield", "inventory_turnover",
        ],
        "min_required": 4,
    },
    "telecom": {
        "required": [
            "revenue_growth", "operating_margin",
            "debt_to_equity", "dividend_yield", "fcf_margin",
        ],
        "recommended": [
            "ebitda_margin", "arpu", "subscriber_count",
        ],
        "min_required": 3,
    },
    "commodities": {
        "required": [
            "revenue_growth", "operating_margin",
            "debt_to_equity", "current_ratio", "fcf_margin",
        ],
        "recommended": [
            "dividend_yield", "reserve_life",
        ],
        "min_required": 3,
    },
    "real_estate": {
        "required": [
            "roe", "debt_to_equity", "current_ratio",
            "dividend_yield",
        ],
        "recommended": [
            "nav_discount", "occupancy_rate", "rental_yield",
        ],
        "min_required": 3,
    },
    "general": {
        "required": [
            "revenue_growth", "operating_margin", "roe",
            "debt_to_equity", "current_ratio",
        ],
        "recommended": [
            "eps_growth", "fcf_margin", "dividend_yield",
        ],
        "min_required": 3,
    },
}


def check_coverage(
    ticker: str,
    available_metrics: set[str],
    sector: Optional[str] = None,
) -> dict:
    """
    Check if available metrics satisfy the sector coverage contract.

    Args:
        ticker: Stock ticker
        available_metrics: Set of metric names available for this ticker
        sector: Override sector (auto-detected if None)

    Returns:
        Dict with passed, missing_required, missing_recommended, coverage_pct,
        confidence_penalty, rating_locked.
    """
    if sector is None:
        from .sector_scoring import detect_sector
        sector = detect_sector(ticker)

    contract = SECTOR_CONTRACTS.get(sector, SECTOR_CONTRACTS["general"])

    required = set(contract["required"])
    recommended = set(contract["recommended"])
    min_required = contract["min_required"]

    # Normalize metric names for comparison (lowercase, underscored)
    available_normalized = {m.lower().replace(" ", "_") for m in available_metrics}

    # Check required
    found_required = required & available_normalized
    missing_required = sorted(required - available_normalized)

    # Check recommended
    found_recommended = recommended & available_normalized
    missing_recommended = sorted(recommended - available_normalized)

    # Pass = enough required metrics present
    passed = len(found_required) >= min_required

    # Calculate coverage percentage
    total_contract = len(required) + len(recommended)
    total_found = len(found_required) + len(found_recommended)
    coverage_pct = round(total_found / total_contract, 2) if total_contract > 0 else 0

    # Confidence penalty
    if passed:
        # Mild penalty for missing recommended items
        confidence_penalty = len(missing_recommended) * 0.02
    else:
        # Strong penalty for failing contract
        required_deficit = max(0, min_required - len(found_required))
        confidence_penalty = 0.10 + required_deficit * 0.05

    # Rating lock: if coverage is below threshold, lock rating to Hold
    rating_locked = not passed

    logger.info(
        f"Coverage contract for {ticker} ({sector}): "
        f"{'PASS' if passed else 'FAIL'} — "
        f"{len(found_required)}/{len(required)} required, "
        f"{len(found_recommended)}/{len(recommended)} recommended"
    )

    return {
        "ticker": ticker,
        "sector": sector,
        "passed": passed,
        "min_required": min_required,
        "required_found": len(found_required),
        "required_total": len(required),
        "missing_required": missing_required,
        "missing_recommended": missing_recommended,
        "coverage_pct": coverage_pct,
        "confidence_penalty": round(confidence_penalty, 3),
        "rating_locked": rating_locked,
        "rating_lock_reason": (
            f"Coverage contract failed: {len(found_required)}/{min_required} "
            f"required metrics. Rating locked to Hold."
        ) if rating_locked else None,
    }


def format_coverage_report(result: dict) -> str:
    """Format coverage check result into human-readable report."""
    lines = [
        f"Coverage Contract: {result['sector'].upper()} ({result['ticker']})",
        f"Status: {'✅ PASS' if result['passed'] else '❌ FAIL'}",
        f"Required: {result['required_found']}/{result['required_total']} (min: {result['min_required']})",
        f"Coverage: {result['coverage_pct']:.0%}",
    ]

    if result["missing_required"]:
        lines.append(f"Missing Required: {', '.join(result['missing_required'])}")

    if result["missing_recommended"]:
        lines.append(f"Missing Recommended: {', '.join(result['missing_recommended'])}")

    if result["rating_locked"]:
        lines.append(f"⚠ {result['rating_lock_reason']}")

    return "\n".join(lines)
