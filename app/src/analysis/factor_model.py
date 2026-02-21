"""
Institutional Factor Model.
Computes 5 factor scores (0-100) for analyst-grade quality assessment.
Factors: quality, growth, balance_sheet, cashflow, shareholder.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================
# Factor Definitions & Sub-Metrics
# ============================================

FACTOR_METRICS = {
    "quality": {
        "description": "Profitability & stability",
        "metrics": {
            "operating_margin": {"weight": 0.25, "ideal_range": (0.15, 0.40), "higher_better": True},
            "net_margin": {"weight": 0.20, "ideal_range": (0.10, 0.30), "higher_better": True},
            "roe": {"weight": 0.25, "ideal_range": (0.12, 0.30), "higher_better": True},
            "roic": {"weight": 0.20, "ideal_range": (0.10, 0.25), "higher_better": True},
            "gross_margin": {"weight": 0.10, "ideal_range": (0.30, 0.70), "higher_better": True},
        },
    },
    "growth": {
        "description": "Revenue & earnings momentum",
        "metrics": {
            "revenue_growth": {"weight": 0.30, "ideal_range": (0.05, 0.30), "higher_better": True},
            "earnings_growth": {"weight": 0.25, "ideal_range": (0.05, 0.40), "higher_better": True},
            "eps_growth": {"weight": 0.25, "ideal_range": (0.05, 0.35), "higher_better": True},
            "revenue_qoq": {"weight": 0.20, "ideal_range": (0.0, 0.15), "higher_better": True},
        },
    },
    "balance_sheet": {
        "description": "Leverage, liquidity, solvency",
        "metrics": {
            "debt_to_equity": {"weight": 0.30, "ideal_range": (0.0, 1.0), "higher_better": False},
            "current_ratio": {"weight": 0.25, "ideal_range": (1.2, 3.0), "higher_better": True},
            "interest_coverage": {"weight": 0.25, "ideal_range": (3.0, 20.0), "higher_better": True},
            "net_debt_to_ebitda": {"weight": 0.20, "ideal_range": (0.0, 2.5), "higher_better": False},
        },
    },
    "cashflow": {
        "description": "Cash generation & efficiency",
        "metrics": {
            "fcf_margin": {"weight": 0.30, "ideal_range": (0.05, 0.25), "higher_better": True},
            "cfo_to_net_income": {"weight": 0.25, "ideal_range": (0.8, 2.0), "higher_better": True},
            "capex_intensity": {"weight": 0.20, "ideal_range": (0.02, 0.15), "higher_better": False},
            "fcf_yield": {"weight": 0.25, "ideal_range": (0.03, 0.10), "higher_better": True},
        },
    },
    "shareholder": {
        "description": "Capital allocation & returns",
        "metrics": {
            "dividend_yield": {"weight": 0.30, "ideal_range": (0.01, 0.05), "higher_better": True},
            "payout_ratio": {"weight": 0.25, "ideal_range": (0.20, 0.60), "higher_better": None},  # mid is best
            "buyback_yield": {"weight": 0.25, "ideal_range": (0.0, 0.05), "higher_better": True},
            "dilution": {"weight": 0.20, "ideal_range": (-0.02, 0.01), "higher_better": False},
        },
    },
}

# Sector weights for composite score
SECTOR_FACTOR_WEIGHTS = {
    "tech": {"quality": 0.25, "growth": 0.30, "balance_sheet": 0.10, "cashflow": 0.25, "shareholder": 0.10},
    "banking": {"quality": 0.30, "growth": 0.15, "balance_sheet": 0.30, "cashflow": 0.10, "shareholder": 0.15},
    "consumer": {"quality": 0.25, "growth": 0.20, "balance_sheet": 0.15, "cashflow": 0.20, "shareholder": 0.20},
    "telecom": {"quality": 0.20, "growth": 0.15, "balance_sheet": 0.20, "cashflow": 0.25, "shareholder": 0.20},
    "commodities": {"quality": 0.20, "growth": 0.15, "balance_sheet": 0.25, "cashflow": 0.25, "shareholder": 0.15},
    "real_estate": {"quality": 0.15, "growth": 0.15, "balance_sheet": 0.30, "cashflow": 0.20, "shareholder": 0.20},
    "general": {"quality": 0.25, "growth": 0.20, "balance_sheet": 0.20, "cashflow": 0.20, "shareholder": 0.15},
}


def _score_metric(value: float, ideal_range: tuple, higher_better: Optional[bool]) -> float:
    """Score a single metric 0-100 based on its ideal range."""
    low, high = ideal_range

    if higher_better is None:
        # Mid-range is best (e.g., payout ratio)
        mid = (low + high) / 2
        dist = abs(value - mid)
        max_dist = max(abs(high - low) / 2, 0.01)
        ratio = max(0, 1 - dist / max_dist)
        return round(ratio * 100, 1)

    if higher_better:
        if value >= high:
            return 100.0
        elif value <= low:
            return max(0, (value / low) * 30) if low > 0 else 0.0
        else:
            return round(30 + 70 * (value - low) / (high - low), 1)
    else:
        # Lower is better (e.g., debt_to_equity)
        if value <= low:
            return 100.0
        elif value >= high:
            return max(0, 100 - 70 * (value - high) / max(high, 0.01))
        else:
            return round(100 - 70 * (value - low) / (high - low), 1)


def compute_factor_score(
    factor_name: str,
    metrics_data: dict[str, float],
) -> dict:
    """
    Compute a single factor score.

    Args:
        factor_name: One of quality/growth/balance_sheet/cashflow/shareholder
        metrics_data: Dict mapping metric names to their values

    Returns:
        Dict with score (0-100), components, and coverage info.
    """
    factor_def = FACTOR_METRICS.get(factor_name)
    if not factor_def:
        return {"score": 0, "error": f"Unknown factor: {factor_name}"}

    components = []
    total_weight = 0.0
    weighted_sum = 0.0

    for metric_name, config in factor_def["metrics"].items():
        value = metrics_data.get(metric_name)
        if value is None:
            components.append({
                "metric": metric_name, "value": None,
                "score": None, "weight": config["weight"], "status": "missing"
            })
            continue

        try:
            value = float(value)
        except (TypeError, ValueError):
            continue

        metric_score = _score_metric(value, config["ideal_range"], config["higher_better"])
        weight = config["weight"]
        weighted_sum += metric_score * weight
        total_weight += weight

        components.append({
            "metric": metric_name, "value": round(value, 4),
            "score": metric_score, "weight": weight, "status": "computed"
        })

    # Normalize score
    if total_weight > 0:
        factor_score = round(weighted_sum / total_weight, 1)
    else:
        factor_score = 0.0

    computed_count = sum(1 for c in components if c["status"] == "computed")
    total_count = len(components)

    return {
        "factor": factor_name,
        "description": factor_def["description"],
        "score": factor_score,
        "coverage": f"{computed_count}/{total_count}",
        "coverage_pct": round(computed_count / total_count, 2) if total_count > 0 else 0,
        "components": components,
    }


def compute_all_factors(
    ticker: str,
    metrics_data: dict[str, float],
    sector: Optional[str] = None,
) -> dict:
    """
    Compute all 5 factor scores and composite.

    Args:
        ticker: Stock ticker
        metrics_data: All available financial metrics
        sector: Override sector (auto-detected if None)

    Returns:
        Dict with factor_scores, composite_score, sector, coverage_summary.
    """
    if sector is None:
        from .sector_scoring import detect_sector
        sector = detect_sector(ticker)

    factor_results = {}
    for factor_name in FACTOR_METRICS:
        factor_results[factor_name] = compute_factor_score(factor_name, metrics_data)

    # Compute composite
    weights = SECTOR_FACTOR_WEIGHTS.get(sector, SECTOR_FACTOR_WEIGHTS["general"])
    composite = 0.0
    total_weight = 0.0

    for factor_name, weight in weights.items():
        factor_data = factor_results.get(factor_name, {})
        if factor_data.get("coverage_pct", 0) > 0:
            composite += factor_data["score"] * weight
            total_weight += weight

    composite_score = round(composite / total_weight, 1) if total_weight > 0 else 0.0

    # Coverage summary
    total_metrics = sum(
        len(f.get("components", [])) for f in factor_results.values()
    )
    computed_metrics = sum(
        sum(1 for c in f.get("components", []) if c["status"] == "computed")
        for f in factor_results.values()
    )

    logger.info(
        f"Factor model for {ticker} ({sector}): composite={composite_score}, "
        f"coverage={computed_metrics}/{total_metrics}"
    )

    return {
        "ticker": ticker,
        "sector": sector,
        "composite_score": composite_score,
        "factor_scores": factor_results,
        "sector_weights": weights,
        "coverage_summary": {
            "total_metrics": total_metrics,
            "computed_metrics": computed_metrics,
            "coverage_pct": round(computed_metrics / total_metrics, 2) if total_metrics > 0 else 0,
        },
    }


def get_metrics_from_yfinance(ticker: str) -> dict[str, float]:
    """
    Fetch metric values from yfinance for factor model input.
    Maps yfinance fields to our standard metric names.
    """
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info
        financials = stock.financials
        cashflow = stock.cashflow
        balance = stock.balance_sheet

        metrics = {}

        # Quality metrics
        metrics["operating_margin"] = info.get("operatingMargins")
        metrics["net_margin"] = info.get("profitMargins")
        metrics["roe"] = info.get("returnOnEquity")
        metrics["gross_margin"] = info.get("grossMargins")
        roic = info.get("returnOnAssets")  # Approximate ROIC
        if roic:
            metrics["roic"] = roic

        # Growth metrics
        metrics["revenue_growth"] = info.get("revenueGrowth")
        metrics["earnings_growth"] = info.get("earningsGrowth")
        eps_growth = info.get("earningsQuarterlyGrowth")
        if eps_growth is not None:
            metrics["eps_growth"] = eps_growth

        # Balance sheet
        metrics["debt_to_equity"] = info.get("debtToEquity")
        if metrics.get("debt_to_equity"):
            metrics["debt_to_equity"] = metrics["debt_to_equity"] / 100  # yfinance gives as percentage
        metrics["current_ratio"] = info.get("currentRatio")

        # EBITDA-based leverage
        ebitda = info.get("ebitda")
        total_debt = info.get("totalDebt")
        total_cash = info.get("totalCash")
        if ebitda and total_debt and ebitda > 0:
            net_debt = total_debt - (total_cash or 0)
            metrics["net_debt_to_ebitda"] = net_debt / ebitda

        # Cashflow
        fcf = info.get("freeCashflow")
        revenue = info.get("totalRevenue")
        net_income = info.get("netIncomeToCommon")
        operating_cf = info.get("operatingCashflow")

        if fcf and revenue and revenue > 0:
            metrics["fcf_margin"] = fcf / revenue
        if operating_cf and net_income and net_income != 0:
            metrics["cfo_to_net_income"] = operating_cf / net_income
        if fcf:
            market_cap = info.get("marketCap")
            if market_cap and market_cap > 0:
                metrics["fcf_yield"] = fcf / market_cap

        # Capex intensity
        if revenue and revenue > 0:
            try:
                if cashflow is not None and not cashflow.empty:
                    capex_row = cashflow.loc["Capital Expenditure"] if "Capital Expenditure" in cashflow.index else None
                    if capex_row is not None and len(capex_row) > 0:
                        capex = abs(float(capex_row.iloc[0]))
                        metrics["capex_intensity"] = capex / revenue
            except Exception:
                pass

        # Shareholder
        metrics["dividend_yield"] = info.get("dividendYield")
        metrics["payout_ratio"] = info.get("payoutRatio")

        # Buyback yield approximation
        try:
            if cashflow is not None and not cashflow.empty:
                buyback_row = None
                for label in ["Repurchase Of Capital Stock", "Common Stock Repurchased"]:
                    if label in cashflow.index:
                        buyback_row = cashflow.loc[label]
                        break
                if buyback_row is not None and len(buyback_row) > 0:
                    buyback = abs(float(buyback_row.iloc[0]))
                    market_cap = info.get("marketCap")
                    if market_cap and market_cap > 0:
                        metrics["buyback_yield"] = buyback / market_cap
        except Exception:
            pass

        # Share count change (dilution)
        try:
            shares = info.get("sharesOutstanding")
            if balance is not None and not balance.empty:
                for label in ["Ordinary Shares Number", "Share Issued"]:
                    if label in balance.index:
                        shares_row = balance.loc[label]
                        if len(shares_row) >= 2:
                            current = float(shares_row.iloc[0])
                            previous = float(shares_row.iloc[1])
                            if previous > 0:
                                metrics["dilution"] = (current - previous) / previous
                        break
        except Exception:
            pass

        # Filter None values
        cleaned = {k: v for k, v in metrics.items() if v is not None}

        # ── Currency-aware sanity checks ──
        from .currency_utils import detect_currency, sanitize_metrics
        from .sector_scoring import detect_sector
        currency = detect_currency(ticker)
        cleaned = sanitize_metrics(cleaned, currency, ticker, sector=detect_sector(ticker))

        return cleaned

    except Exception as e:
        logger.error(f"Failed to fetch yfinance metrics for factor model: {e}")
        return {}
