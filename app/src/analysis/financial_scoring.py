"""
Financial Scoring module for Finance Analytics.
Computes financial metrics, generates a 0-100 score with explainable drivers.
Each metric has detailed score ranges and human-readable explanations.
"""

import json
import logging
from typing import Optional

from ..config import config
from ..db import get_db_cursor

logger = logging.getLogger(__name__)

# ============================================
# Metric descriptions with score ranges
# ============================================
METRIC_DESCRIPTIONS = {
    "revenue_growth": {
        "name": "Revenue Growth (YoY)",
        "description": "Year-over-year revenue change. Shows business expansion rate.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "Revenue growing >20% YoY -- strong expansion"),
            (60, 80, "Good", "Revenue growing 10-20% YoY -- healthy growth"),
            (40, 60, "Fair", "Revenue growing 0-10% YoY -- moderate growth"),
            (20, 40, "Weak", "Revenue declining 0-10% YoY -- slowing down"),
            (0, 20, "Poor", "Revenue declining >10% YoY -- contraction"),
        ],
    },
    "revenue_qoq": {
        "name": "Revenue Growth (QoQ)",
        "description": "Quarter-over-quarter revenue change. Shows short-term momentum.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "Revenue growing >15% QoQ -- strong momentum"),
            (60, 80, "Good", "Revenue growing 5-15% QoQ -- solid quarter"),
            (40, 60, "Fair", "Revenue stable to +5% QoQ -- flat performance"),
            (20, 40, "Weak", "Revenue declining 0-10% QoQ -- losing momentum"),
            (0, 20, "Poor", "Revenue declining >10% QoQ -- sharp drop"),
        ],
    },
    "net_margin": {
        "name": "Net Profit Margin",
        "description": "Net income as % of revenue. Shows overall profitability.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "Net margin >20% -- highly profitable"),
            (60, 80, "Good", "Net margin 10-20% -- strong profitability"),
            (40, 60, "Fair", "Net margin 5-10% -- moderate profitability"),
            (20, 40, "Weak", "Net margin 0-5% -- thin margins"),
            (0, 20, "Poor", "Net margin negative -- company losing money"),
        ],
    },
    "op_margin": {
        "name": "Operating Margin",
        "description": "Operating income as % of revenue. Shows core business efficiency.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "Op margin >20% -- very efficient operations"),
            (60, 80, "Good", "Op margin 10-20% -- efficient business"),
            (40, 60, "Fair", "Op margin 5-10% -- average efficiency"),
            (20, 40, "Weak", "Op margin 0-5% -- tight operations"),
            (0, 20, "Poor", "Op margin negative -- operational losses"),
        ],
    },
    "roe": {
        "name": "Return on Equity (ROE)",
        "description": "Net income / shareholders' equity. Shows return to shareholders.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "ROE >25% -- exceptional return on equity"),
            (60, 80, "Good", "ROE 15-25% -- strong shareholder returns"),
            (40, 60, "Fair", "ROE 8-15% -- adequate returns"),
            (20, 40, "Weak", "ROE 0-8% -- below average returns"),
            (0, 20, "Poor", "ROE negative -- destroying shareholder value"),
        ],
    },
    "ocf": {
        "name": "Operating Cash Flow",
        "description": "Cash generated from core business operations.",
        "format": "binary",
        "ranges": [
            (80, 100, "Excellent", "Strong positive operating cash flow"),
            (50, 80, "Good", "Positive operating cash flow"),
            (0, 50, "Poor", "Negative operating cash flow -- cash burn"),
        ],
    },
    "fcf": {
        "name": "Free Cash Flow (FCF)",
        "description": "Cash available after capital expenditures (OCF - CAPEX).",
        "format": "binary",
        "ranges": [
            (80, 100, "Excellent", "Strong positive FCF -- company generates surplus cash"),
            (50, 80, "Good", "Positive FCF -- covers capex with room to spare"),
            (0, 50, "Poor", "Negative FCF -- spending more than generating"),
        ],
    },
    "debt_to_equity": {
        "name": "Debt-to-Equity Ratio",
        "description": "Total debt / total equity. Shows financial leverage.",
        "format": "ratio",
        "ranges": [
            (80, 100, "Excellent", "D/E < 0.5x -- very low leverage, conservative"),
            (60, 80, "Good", "D/E 0.5-1.0x -- moderate, healthy leverage"),
            (40, 60, "Fair", "D/E 1.0-2.0x -- significant leverage"),
            (20, 40, "Weak", "D/E 2.0-3.0x -- high leverage, risky"),
            (0, 20, "Poor", "D/E > 3.0x -- extremely leveraged"),
        ],
    },
    "current_ratio": {
        "name": "Current Ratio",
        "description": "Current assets / current liabilities. Shows short-term liquidity.",
        "format": "ratio",
        "ranges": [
            (80, 100, "Excellent", "Ratio > 2.0x -- very strong liquidity"),
            (60, 80, "Good", "Ratio 1.5-2.0x -- healthy liquidity"),
            (40, 60, "Fair", "Ratio 1.0-1.5x -- adequate liquidity"),
            (20, 40, "Weak", "Ratio 0.5-1.0x -- tight liquidity"),
            (0, 20, "Poor", "Ratio < 0.5x -- severe liquidity risk"),
        ],
    },
    "eps_growth": {
        "name": "EPS Growth (YoY)",
        "description": "Year-over-year earnings per share growth.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "EPS growing >25% YoY -- accelerating earnings"),
            (60, 80, "Good", "EPS growing 10-25% YoY -- solid earnings growth"),
            (40, 60, "Fair", "EPS growing 0-10% YoY -- moderate growth"),
            (20, 40, "Weak", "EPS declining 0-15% YoY -- earnings contraction"),
            (0, 20, "Poor", "EPS declining >15% YoY -- significant deterioration"),
        ],
    },
    # ── Bank-specific metrics (only used when ticker is a bank) ──
    "net_interest_margin": {
        "name": "Net Interest Margin (NIM)",
        "description": "Interest income minus interest expense as % of avg earning assets.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "NIM >5% -- very high interest spread"),
            (60, 80, "Good", "NIM 3-5% -- healthy interest income"),
            (40, 60, "Fair", "NIM 2-3% -- adequate spread"),
            (20, 40, "Weak", "NIM 1-2% -- thin spread"),
            (0, 20, "Poor", "NIM <1% -- severely compressed margins"),
        ],
    },
    "non_performing_loan": {
        "name": "Non-Performing Loan Ratio (NPL)",
        "description": "NPL as % of total loans. Lower is better.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "NPL <1% -- very clean loan book"),
            (60, 80, "Good", "NPL 1-2% -- well managed credit risk"),
            (40, 60, "Fair", "NPL 2-3% -- moderate credit risk"),
            (20, 40, "Weak", "NPL 3-5% -- elevated credit risk"),
            (0, 20, "Poor", "NPL >5% -- significant credit problems"),
        ],
    },
    "capital_adequacy_ratio": {
        "name": "Capital Adequacy Ratio (CAR)",
        "description": "Bank capital as % of risk-weighted assets. Regulatory minimum ~8%.",
        "format": "percent",
        "ranges": [
            (80, 100, "Excellent", "CAR >20% -- very well capitalized"),
            (60, 80, "Good", "CAR 15-20% -- well capitalized"),
            (40, 60, "Fair", "CAR 12-15% -- adequately capitalized"),
            (20, 40, "Weak", "CAR 8-12% -- near regulatory minimum"),
            (0, 20, "Poor", "CAR <8% -- under-capitalized, regulatory risk"),
        ],
    },
}


# Score thresholds for each metric
# Maps metric -> (min_bad, max_good) for percentile normalization
METRIC_THRESHOLDS = {
    "revenue_growth": (-0.20, 0.30),    # -20% to +30% YoY
    "revenue_qoq": (-0.15, 0.20),       # -15% to +20% QoQ
    "net_margin": (-0.05, 0.25),         # -5% to +25%
    "op_margin": (-0.05, 0.30),          # -5% to +30%
    "roe": (-0.05, 0.30),               # -5% to +30%
    "ocf": (0, 1),                       # normalized 0-1 (positive = good)
    "fcf": (0, 1),                       # normalized 0-1 (positive = good)
    "debt_to_equity": (3.0, 0.0),        # INVERTED: lower is better
    "current_ratio": (0.0, 2.5),         # 0.0x to 2.5x
    "eps_growth": (-0.25, 0.30),         # -25% to +30% YoY
    # Bank metrics
    "net_interest_margin": (0.0, 0.05),  # 0% to 5%
    "non_performing_loan": (0.05, 0.0),  # INVERTED: 5% bad, 0% ideal
    "capital_adequacy_ratio": (0.08, 0.20),  # 8% min to 20% excellent
}


# ============================================
# Bank ticker detection
# ============================================
BANK_TICKERS = {
    "BBCA", "BBRI", "BMRI", "BBNI", "BRIS", "BTPN", "BDMN", "BNGA",
    "MEGA", "BBTN", "BNII", "PNBN", "NISP", "BJTM", "BJBR",
    # US banks
    "JPM", "BAC", "WFC", "C", "GS", "MS", "USB", "PNC", "SCHW",
}

# Bank-specific scoring weights (replaces general weights for bank tickers)
BANK_SCORING_WEIGHTS = {
    "revenue_growth": 0.08,
    "revenue_qoq": 0.05,
    "net_margin": 0.10,
    "op_margin": 0.05,
    "roe": 0.12,
    "fcf": 0.05,
    "ocf": 0.05,
    "debt_to_equity": 0.03,
    "current_ratio": 0.02,
    "eps_growth": 0.08,
    # Bank-specific (total = 0.37)
    "net_interest_margin": 0.15,
    "non_performing_loan": 0.12,
    "capital_adequacy_ratio": 0.10,
}


def is_bank_ticker(ticker: str) -> bool:
    """Check if a ticker belongs to a bank."""
    base = ticker.split(".")[0].upper()
    return base in BANK_TICKERS


def compute_financial_features(ticker: str, period: str) -> dict[str, Optional[float]]:
    """
    Compute financial features from financial_facts table.

    Args:
        ticker: Stock ticker
        period: Current reporting period (e.g., 'Q3-2025')

    Returns:
        Dict of metric_name -> computed value
    """
    facts = _get_facts_for_ticker(ticker)

    if not facts:
        logger.warning(f"No financial facts found for {ticker}")
        return {}

    # Group facts by period
    by_period: dict[str, dict] = {}
    for fact in facts:
        p = fact["period"]
        if p not in by_period:
            by_period[p] = {}
        by_period[p][fact["metric"]] = fact["value"]

    current = by_period.get(period, {})
    if not current:
        # Fall back to the most recent period available
        available_periods = sorted(by_period.keys(), reverse=True)
        if available_periods:
            fallback_period = available_periods[0]
            logger.warning(
                f"No data for period {period}, using most recent available: {fallback_period}"
            )
            current = by_period[fallback_period]
            period = fallback_period
        else:
            logger.warning(f"No data for current period {period}")
            return {}

    features: dict[str, Optional[float]] = {}

    # ── Revenue YoY ──
    revenue = current.get("revenue")
    prior_year_period = _get_prior_year_period(period)
    prior_year = by_period.get(prior_year_period, {})
    prior_revenue = prior_year.get("revenue")

    if revenue and prior_revenue and prior_revenue != 0:
        features["revenue_growth"] = (revenue - prior_revenue) / abs(prior_revenue)
    else:
        features["revenue_growth"] = None

    # ── Revenue QoQ ──
    prior_q_period = _get_prior_quarter_period(period)
    prior_q = by_period.get(prior_q_period, {})
    prior_q_revenue = prior_q.get("revenue")

    if revenue and prior_q_revenue and prior_q_revenue != 0:
        features["revenue_qoq"] = (revenue - prior_q_revenue) / abs(prior_q_revenue)
    else:
        features["revenue_qoq"] = None

    # ── Net Margin ──
    net_income = current.get("net_income")
    if revenue and net_income and revenue != 0:
        features["net_margin"] = net_income / revenue
    else:
        features["net_margin"] = None

    # ── Operating Margin ──
    op_income = current.get("operating_income")
    if revenue and op_income and revenue != 0:
        features["op_margin"] = op_income / revenue
    else:
        features["op_margin"] = None

    # ── Return on Equity ──
    total_equity = current.get("total_equity")
    if net_income is not None and total_equity and total_equity != 0:
        features["roe"] = net_income / abs(total_equity)
    else:
        features["roe"] = None

    # ── Operating Cash Flow (normalized as positive/negative signal) ──
    ocf = current.get("operating_cash_flow")
    if ocf is not None:
        features["ocf"] = 1.0 if ocf > 0 else 0.0
    else:
        features["ocf"] = None

    # ── Free Cash Flow = OCF - CAPEX ──
    capex = current.get("capex")
    if ocf is not None and capex is not None:
        fcf = ocf - abs(capex)  # capex is often reported as positive
        features["fcf"] = 1.0 if fcf > 0 else 0.0
    else:
        features["fcf"] = None

    # ── Debt to Equity ──
    total_debt = current.get("total_debt")
    if total_debt is not None and total_equity and total_equity != 0:
        features["debt_to_equity"] = total_debt / abs(total_equity)
    else:
        features["debt_to_equity"] = None

    # ── Current Ratio ──
    current_assets = current.get("current_assets")
    current_liabilities = current.get("current_liabilities")
    if current_assets is not None and current_liabilities and current_liabilities != 0:
        features["current_ratio"] = current_assets / abs(current_liabilities)
    else:
        features["current_ratio"] = None

    # ── EPS Growth YoY ──
    eps = current.get("eps") or current.get("earnings_per_share")
    prior_eps = prior_year.get("eps") or prior_year.get("earnings_per_share")
    if eps is not None and prior_eps and prior_eps != 0:
        features["eps_growth"] = (eps - prior_eps) / abs(prior_eps)
    else:
        features["eps_growth"] = None

    # ── Bank Metrics: NIM, NPL, CAR ──
    nim = current.get("net_interest_margin") or current.get("nim")
    if nim is not None:
        # NIM is often stored as a percentage (e.g., 5.2 for 5.2%), normalize to decimal
        features["net_interest_margin"] = nim if nim < 1 else nim / 100.0
    else:
        features["net_interest_margin"] = None

    npl = current.get("non_performing_loan") or current.get("npl")
    if npl is not None:
        features["non_performing_loan"] = npl if npl < 1 else npl / 100.0
    else:
        features["non_performing_loan"] = None

    car = current.get("capital_adequacy_ratio") or current.get("car")
    if car is not None:
        features["capital_adequacy_ratio"] = car if car < 1 else car / 100.0
    else:
        features["capital_adequacy_ratio"] = None

    return features


def compute_score(
    features: dict[str, Optional[float]],
    ticker: str = "",
) -> tuple[float, list[dict], float]:
    """
    Compute weighted financial score (0-100) from features.

    For bank tickers, uses bank-specific weights that include NIM/NPL/CAR.
    Also computes coverage_factor (0-1) indicating how much of the total
    weight was actually covered by available data.

    Args:
        features: Dict of metric_name -> value
        ticker: Stock ticker (used for bank detection)

    Returns:
        Tuple of (score, drivers, coverage_factor)
    """
    # Choose weights: bank-specific if applicable
    if is_bank_ticker(ticker):
        weights = dict(BANK_SCORING_WEIGHTS)
        logger.info(f"Using bank-specific scoring weights for {ticker}")
    else:
        weights = dict(config.SCORING_WEIGHTS)

    drivers = []
    total_score = 0.0
    total_weight = 0.0
    total_possible_weight = sum(weights.values())

    for metric, weight in weights.items():
        value = features.get(metric)
        desc = METRIC_DESCRIPTIONS.get(metric, {})

        if value is None:
            drivers.append({
                "metric": metric,
                "name": desc.get("name", metric.replace("_", " ").title()),
                "description": desc.get("description", ""),
                "value": None,
                "sub_score": 0,
                "sub_score_pct": 0,
                "weight": weight,
                "contribution": 0,
                "rating_label": "N/A",
                "rating_detail": "Insufficient data for this metric",
                "status": "no_data",
            })
            continue

        sub_score = _normalize_to_score(metric, value)
        sub_score_pct = round(sub_score * 100, 1)
        contribution = sub_score * weight

        # Find rating label from ranges
        rating_label, rating_detail = _get_rating_for_score(metric, sub_score_pct)

        total_score += contribution
        total_weight += weight

        drivers.append({
            "metric": metric,
            "name": desc.get("name", metric.replace("_", " ").title()),
            "description": desc.get("description", ""),
            "value": round(value, 4),
            "sub_score": round(sub_score, 2),
            "sub_score_pct": sub_score_pct,
            "weight": weight,
            "contribution": round(contribution, 2),
            "rating_label": rating_label,
            "rating_detail": rating_detail,
            "status": "computed",
        })

    # Normalize by actual weights used (in case some metrics are missing)
    if total_weight > 0:
        final_score = (total_score / total_weight) * 100
    else:
        final_score = 0.0

    final_score = max(0, min(100, final_score))

    # Coverage factor: what fraction of total weight do we actually have data for
    coverage_factor = round(total_weight / total_possible_weight, 2) if total_possible_weight > 0 else 0.0

    if coverage_factor < 0.50:
        logger.warning(
            f"Low data coverage ({coverage_factor:.0%}). "
            f"Confidence will be penalized."
        )

    # Sort drivers by contribution (descending)
    drivers.sort(key=lambda d: d["contribution"], reverse=True)

    return round(final_score, 2), drivers, coverage_factor


def _get_rating_for_score(metric: str, score_pct: float) -> tuple[str, str]:
    """Get rating label and detail for a metric's sub-score percentage."""
    desc = METRIC_DESCRIPTIONS.get(metric, {})
    ranges = desc.get("ranges", [])

    for range_min, range_max, label, detail in ranges:
        if range_min <= score_pct <= range_max:
            return label, detail

    # Fallback
    if score_pct >= 60:
        return "Good", "Above average"
    elif score_pct >= 40:
        return "Fair", "Average"
    else:
        return "Weak", "Below average"


def explain_score(drivers: list[dict]) -> str:
    """
    Generate a detailed human-readable explanation of the financial score.

    Args:
        drivers: List of driver dicts from compute_score()

    Returns:
        Multi-line string with detailed explanations
    """
    lines = []

    computed = [d for d in drivers if d["status"] == "computed"]
    no_data = [d for d in drivers if d["status"] == "no_data"]

    if computed:
        lines.append("FINANCIAL SCORE BREAKDOWN:")
        lines.append("")

        for i, d in enumerate(computed, 1):
            name = d["name"]
            value = d["value"]
            sub_pct = d["sub_score_pct"]
            weight_pct = d["weight"] * 100
            label = d["rating_label"]
            detail = d["rating_detail"]
            fmt = METRIC_DESCRIPTIONS.get(d["metric"], {}).get("format", "number")

            # Format value
            if fmt == "percent" and value is not None:
                val_str = f"{value:.1%}"
            elif fmt == "ratio" and value is not None:
                val_str = f"{value:.2f}x"
            elif fmt == "binary" and value is not None:
                val_str = "Positive" if value > 0.5 else "Negative"
            else:
                val_str = f"{value}" if value is not None else "N/A"

            lines.append(
                f"  {i}. {name}: {val_str}"
            )
            lines.append(
                f"     Score: {sub_pct:.0f}/100 [{label}] (weight: {weight_pct:.0f}%)"
            )
            lines.append(
                f"     {detail}"
            )
            lines.append("")

    if no_data:
        lines.append("  Metrics with insufficient data:")
        for d in no_data:
            lines.append(f"  - {d['name']} (weight: {d['weight']*100:.0f}%)")

    return "\n".join(lines)


def _normalize_to_score(metric: str, value: float) -> float:
    """
    Normalize a metric value to a 0-1 sub-score using thresholds.

    For most metrics: higher is better.
    For debt_to_equity: lower is better (inverted).
    """
    if metric not in METRIC_THRESHOLDS:
        return 0.5  # Unknown metric -> neutral

    min_val, max_val = METRIC_THRESHOLDS[metric]

    # Inverted metrics: lower is better
    inverted = metric in ("debt_to_equity", "non_performing_loan")

    if inverted:
        if value <= max_val:  # max_val is 0 / ideal
            return 1.0
        elif value >= min_val:  # min_val is worst case
            return 0.0
        else:
            return 1.0 - (value - max_val) / (min_val - max_val)
    else:
        # Normal: higher is better
        if value >= max_val:
            return 1.0
        elif value <= min_val:
            return 0.0
        else:
            return (value - min_val) / (max_val - min_val)


def run_financial_scoring(ticker: str, period: str) -> dict:
    """
    Run the complete financial scoring pipeline for a ticker/period.

    Args:
        ticker: Stock ticker
        period: Reporting period

    Returns:
        Dict with score, drivers, explanation, coverage_factor, and metadata
    """
    logger.info(f"Running financial scoring for {ticker} ({period})")

    # Compute features
    features = compute_financial_features(ticker, period)
    if not features:
        logger.warning(f"No features computed for {ticker} ({period})")
        return {
            "ticker": ticker,
            "period": period,
            "score": 0,
            "drivers": [],
            "explanation": "No financial data available for scoring.",
            "coverage_factor": 0.0,
        }

    # Compute score (with bank detection)
    score, drivers, coverage_factor = compute_score(features, ticker=ticker)

    # Generate explanation
    explanation = explain_score(drivers)

    logger.info(
        f"Financial score for {ticker} ({period}): {score} "
        f"(coverage: {coverage_factor:.0%})"
    )

    return {
        "ticker": ticker,
        "period": period,
        "score": score,
        "drivers": drivers,
        "explanation": explanation,
        "coverage_factor": coverage_factor,
    }


def _get_facts_for_ticker(ticker: str) -> list[dict]:
    """Get all financial facts for a ticker."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT ticker, period, metric, value, unit, currency
            FROM financial_facts
            WHERE ticker = %(ticker)s
            ORDER BY period DESC
            """,
            {"ticker": ticker},
        )
        rows = cursor.fetchall()
        # Convert Decimal values to float for arithmetic compatibility
        for row in rows:
            if row.get("value") is not None:
                row["value"] = float(row["value"])
        return rows


def _get_prior_year_period(period: str) -> str:
    """
    Derive prior year period string.
    E.g., 'Q3-2025' -> 'Q3-2024', 'FY-2024' -> 'FY-2023'
    """
    try:
        parts = period.split("-")
        if len(parts) == 2:
            prefix = parts[0]
            year = int(parts[1])
            return f"{prefix}-{year - 1}"
    except (ValueError, IndexError):
        pass
    return f"{period}-prior-year"


def _get_prior_quarter_period(period: str) -> str:
    """
    Derive prior quarter period string.
    E.g., 'Q3-2025' -> 'Q2-2025', 'Q1-2025' -> 'Q4-2024'
    """
    try:
        parts = period.split("-")
        if len(parts) == 2 and parts[0].startswith("Q"):
            quarter = int(parts[0][1:])
            year = int(parts[1])
            if quarter == 1:
                return f"Q4-{year - 1}"
            else:
                return f"Q{quarter - 1}-{year}"
    except (ValueError, IndexError):
        pass
    return f"{period}-prior-quarter"
