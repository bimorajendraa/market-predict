"""
Investment Thesis Tracker.
Manages bull/base/bear thesis, KPIs, and triggers for each ticker.
Checks current data against trigger conditions.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================
# Sector-Specific Thesis Templates
# ============================================

THESIS_TEMPLATES = {
    "tech": {
        "kpis": [
            {"name": "Cloud revenue growth (%)", "metric": "cloud_revenue_growth", "target": "> 20%"},
            {"name": "Operating margin expansion", "metric": "operating_margin", "target": "expanding QoQ"},
            {"name": "Free cash flow margin", "metric": "fcf_margin", "target": "> 15%"},
            {"name": "R&D as % of revenue", "metric": "rd_to_revenue", "target": "10-20%"},
            {"name": "Revenue growth (YoY)", "metric": "revenue_growth", "target": "> 8%"},
        ],
        "triggers": [
            {"condition": "revenue_growth < 0 for 2 consecutive quarters", "severity": "critical", "action": "Downgrade to SELL"},
            {"condition": "fcf turns negative for 2 consecutive quarters", "severity": "critical", "action": "Downgrade thesis"},
            {"condition": "operating_margin declines > 500bps YoY", "severity": "warning", "action": "Review thesis"},
            {"condition": "major customer loss (>10% revenue)", "severity": "critical", "action": "Reassess moat"},
        ],
        "bull_template": "Sustained cloud transition driving double-digit revenue growth with margin expansion.",
        "bear_template": "Cloud growth stalls, legacy business declines faster than expected, margin compression.",
    },
    "banking": {
        "kpis": [
            {"name": "Net Interest Margin", "metric": "net_interest_margin", "target": "> 3%"},
            {"name": "Non-Performing Loans", "metric": "non_performing_loan", "target": "< 3%"},
            {"name": "Capital Adequacy Ratio", "metric": "capital_adequacy_ratio", "target": "> 12%"},
            {"name": "Cost-to-Income ratio", "metric": "cost_to_income", "target": "< 45%"},
            {"name": "Loan growth (YoY)", "metric": "loan_growth", "target": "8-15%"},
        ],
        "triggers": [
            {"condition": "NPL ratio > 5%", "severity": "critical", "action": "Downgrade to SELL"},
            {"condition": "CAR below regulatory minimum", "severity": "critical", "action": "Immediately reassess"},
            {"condition": "NIM compression > 50bps in single quarter", "severity": "warning", "action": "Monitor closely"},
            {"condition": "cost_to_income > 55%", "severity": "warning", "action": "Review efficiency"},
        ],
        "bull_template": "Strong NIM expansion with controlled credit costs and digital banking gains.",
        "bear_template": "Rising NPLs, NIM compression from rate cuts, fintech competition eroding deposits.",
    },
    "consumer": {
        "kpis": [
            {"name": "Revenue growth (YoY)", "metric": "revenue_growth", "target": "> 5%"},
            {"name": "Gross margin", "metric": "gross_margin", "target": "> 30%"},
            {"name": "Same-store sales growth", "metric": "sssg", "target": "> 2%"},
            {"name": "Market share trend", "metric": "market_share", "target": "stable or growing"},
            {"name": "Inventory turnover", "metric": "inventory_turnover", "target": "improving"},
        ],
        "triggers": [
            {"condition": "revenue decline 2 consecutive quarters", "severity": "critical", "action": "Review thesis"},
            {"condition": "gross margin decline > 300bps", "severity": "warning", "action": "Check pricing power"},
            {"condition": "major competitor entry or market disruption", "severity": "warning", "action": "Reassess moat"},
        ],
        "bull_template": "Premiumization strategy succeeding with expanding distribution and pricing power.",
        "bear_template": "Consumer spending weakness, private label pressure, input cost inflation.",
    },
    "general": {
        "kpis": [
            {"name": "Revenue growth (YoY)", "metric": "revenue_growth", "target": "> 5%"},
            {"name": "Operating margin", "metric": "operating_margin", "target": "> 10%"},
            {"name": "ROE", "metric": "roe", "target": "> 12%"},
            {"name": "Debt-to-equity", "metric": "debt_to_equity", "target": "< 1.0"},
            {"name": "Free cash flow", "metric": "fcf_margin", "target": "> 5%"},
        ],
        "triggers": [
            {"condition": "revenue decline 2 consecutive quarters", "severity": "critical", "action": "Downgrade thesis"},
            {"condition": "negative FCF for 2 quarters", "severity": "warning", "action": "Review cash position"},
            {"condition": "debt_to_equity > 2.0", "severity": "warning", "action": "Monitor leverage"},
        ],
        "bull_template": "Solid fundamentals with consistent execution and market position.",
        "bear_template": "Growth deceleration, margin pressure, and rising competitive threats.",
    },
}


def init_thesis(
    ticker: str,
    sector: Optional[str] = None,
    base_thesis: Optional[str] = None,
    bull_case: Optional[str] = None,
    bear_case: Optional[str] = None,
) -> dict:
    """
    Initialize a thesis for a ticker using sector templates.

    Returns:
        Dict with thesis data ready for DB insert.
    """
    if sector is None:
        from .sector_scoring import detect_sector
        sector = detect_sector(ticker)

    template = THESIS_TEMPLATES.get(sector, THESIS_TEMPLATES["general"])

    thesis_data = {
        "ticker": ticker,
        "sector": sector,
        "base_thesis": base_thesis or f"Quality {sector} company with sustainable competitive advantages.",
        "bull_case": bull_case or template["bull_template"],
        "bear_case": bear_case or template["bear_template"],
        "kpis": template["kpis"],
        "triggers": template["triggers"],
    }

    # Persist to DB
    try:
        from ..db import insert_thesis
        thesis_id = insert_thesis(
            ticker=ticker,
            base_thesis=thesis_data["base_thesis"],
            sector=sector,
            bull_case=thesis_data["bull_case"],
            bear_case=thesis_data["bear_case"],
            kpis_json=thesis_data["kpis"],
            triggers_json=thesis_data["triggers"],
        )
        thesis_data["id"] = str(thesis_id)
        thesis_data["status"] = "on_track"
        logger.info(f"Initialized thesis for {ticker} ({sector})")
    except Exception as e:
        logger.warning(f"Could not persist thesis to DB: {e}")
        thesis_data["id"] = None
        thesis_data["status"] = "on_track"

    return thesis_data


def check_thesis(
    ticker: str,
    current_metrics: Optional[dict] = None,
) -> dict:
    """
    Check a ticker's thesis against current data.

    Returns:
        Dict with status (on_track/at_risk/broken), kpi_results, triggered_alerts.
    """
    # Fetch existing thesis
    try:
        from ..db import get_thesis, update_thesis_status
        thesis_row = get_thesis(ticker)
    except Exception:
        thesis_row = None

    if not thesis_row:
        return {
            "ticker": ticker,
            "status": "no_thesis",
            "message": f"No thesis found for {ticker}. Use run-thesis --ticker {ticker} to initialize.",
        }

    kpis = thesis_row.get("kpis_json", [])
    if isinstance(kpis, str):
        kpis = json.loads(kpis)
    triggers = thesis_row.get("triggers_json", [])
    if isinstance(triggers, str):
        triggers = json.loads(triggers)

    # Get current metrics if not provided
    if current_metrics is None:
        current_metrics = _fetch_latest_metrics(ticker)

    # Evaluate KPIs
    kpi_results = []
    for kpi in kpis:
        metric_name = kpi.get("metric", "")
        current_value = current_metrics.get(metric_name)
        kpi_results.append({
            "name": kpi.get("name", metric_name),
            "metric": metric_name,
            "target": kpi.get("target", "N/A"),
            "current_value": current_value,
            "status": "measured" if current_value is not None else "no_data",
        })

    # Simple status determination
    measured = sum(1 for k in kpi_results if k["status"] == "measured")
    no_data = sum(1 for k in kpi_results if k["status"] == "no_data")

    if no_data > len(kpi_results) / 2:
        status = "insufficient_data"
    else:
        prev_status = thesis_row.get("status", "on_track")
        # Recover automatically once sufficient KPI data is available,
        # but keep explicit risk states if they were set externally.
        if prev_status in {"broken", "at_risk"}:
            status = prev_status
        else:
            status = "on_track"

    # Update DB
    try:
        update_thesis_status(ticker, status)
    except Exception:
        pass

    return {
        "ticker": ticker,
        "status": status,
        "base_thesis": thesis_row.get("base_thesis", ""),
        "bull_case": thesis_row.get("bull_case", ""),
        "bear_case": thesis_row.get("bear_case", ""),
        "kpi_results": kpi_results,
        "triggers": triggers,
        "sector": thesis_row.get("sector", ""),
        "last_updated": str(thesis_row.get("updated_at", "")),
    }


def _fetch_latest_metrics(ticker: str) -> dict:
    """Fetch latest metrics for thesis checking.

    Priority:
    1) Computed features from financial_scoring (DB-backed, includes bank metrics)
    2) Factor model/yfinance metrics as fallback
    """
    metrics: dict = {}

    # 1) DB-backed financial features (preferred)
    try:
        from .financial_scoring import compute_financial_features

        period = _get_latest_period_hint(ticker)
        if period:
            metrics.update({k: v for k, v in compute_financial_features(ticker, period).items() if v is not None})
    except Exception as e:
        logger.debug(f"financial_scoring feature fetch failed for thesis check ({ticker}): {e}")

    # 1b) Direct bank_metrics fallback (for Indonesian banks with sparse aliases)
    try:
        bank = _fetch_latest_bank_metrics(ticker)
        if bank:
            bank_map = {
                "net_interest_margin": bank.get("nim"),
                "non_performing_loan": bank.get("npl"),
                "capital_adequacy_ratio": bank.get("car_kpmm"),
                "loan_to_deposit_ratio": bank.get("ldr"),
                "casa_ratio": bank.get("casa"),
                "cost_to_income": bank.get("bopo"),
                "cost_of_credit": bank.get("cost_of_credit"),
            }
            for key, value in bank_map.items():
                if value is not None and key not in metrics:
                    metrics[key] = float(value)
    except Exception as e:
        logger.debug(f"bank_metrics fallback failed for thesis check ({ticker}): {e}")

    # 2) yfinance/factor fallback
    try:
        from .factor_model import get_metrics_from_yfinance

        yf_metrics = get_metrics_from_yfinance(ticker)
        for key, value in yf_metrics.items():
            if key not in metrics and value is not None:
                metrics[key] = value
    except Exception as e:
        logger.warning(f"Could not fetch fallback metrics for thesis check ({ticker}): {e}")

    # Metric aliases to align thesis KPI naming with available metrics
    if "operating_margin" not in metrics and "op_margin" in metrics:
        metrics["operating_margin"] = metrics["op_margin"]
    if "fcf_margin" not in metrics and "fcf" in metrics:
        # fcf in scoring is binary signal; avoid using as margin proxy unless nothing else exists
        metrics["fcf_margin"] = metrics["fcf"]
    if "loan_growth" not in metrics and "revenue_growth" in metrics:
        # conservative proxy when loan book growth is unavailable
        metrics["loan_growth"] = metrics["revenue_growth"]

    return metrics


def _get_latest_period_hint(ticker: str) -> Optional[str]:
    """Get latest known reporting period for a ticker from DB."""
    try:
        from ..db import get_db_cursor

        with get_db_cursor() as cursor:
            cursor.execute(
                """
                SELECT period FROM bank_metrics
                WHERE ticker = %(ticker)s
                ORDER BY period DESC
                LIMIT 1
                """,
                {"ticker": ticker},
            )
            row = cursor.fetchone()
            if row and row.get("period"):
                return row["period"]

            cursor.execute(
                """
                SELECT period FROM fundamentals_quarterly
                WHERE ticker = %(ticker)s
                ORDER BY period DESC
                LIMIT 1
                """,
                {"ticker": ticker},
            )
            row = cursor.fetchone()
            if row and row.get("period"):
                return row["period"]

            cursor.execute(
                """
                SELECT period FROM financial_facts
                WHERE ticker = %(ticker)s
                ORDER BY period DESC
                LIMIT 1
                """,
                {"ticker": ticker},
            )
            row = cursor.fetchone()
            if row and row.get("period"):
                return row["period"]
    except Exception as e:
        logger.debug(f"Could not determine latest period for thesis check ({ticker}): {e}")

    return None


def _fetch_latest_bank_metrics(ticker: str) -> dict:
    """Fetch latest row from bank_metrics for direct KPI fallback."""
    from ..db import get_db_cursor

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT period, nim, npl, car_kpmm, ldr, casa, bopo, cost_of_credit
            FROM bank_metrics
            WHERE ticker = %(ticker)s
            ORDER BY period DESC
            LIMIT 1
            """,
            {"ticker": ticker},
        )
        row = cursor.fetchone()
        return row or {}


def format_thesis_report(result: dict) -> str:
    """Format thesis check result into readable report."""
    if result.get("status") == "no_thesis":
        return result["message"]

    lines = [
        f"═══════════════════════════════════════",
        f"THESIS TRACKER: {result['ticker']}",
        f"Sector: {result.get('sector', 'N/A')}",
        f"Status: {result['status'].upper().replace('_', ' ')}",
        f"═══════════════════════════════════════",
        "",
        f"BASE THESIS: {result.get('base_thesis', 'N/A')}",
        f"BULL CASE: {result.get('bull_case', 'N/A')}",
        f"BEAR CASE: {result.get('bear_case', 'N/A')}",
        "",
        "KPI DASHBOARD:",
    ]

    for kpi in result.get("kpi_results", []):
        value = kpi.get("current_value")
        value_str = f"{value:.2%}" if isinstance(value, float) and abs(value) < 10 else str(value) if value else "—"
        status_icon = "✅" if kpi["status"] == "measured" else "⬜"
        lines.append(f"  {status_icon} {kpi['name']}: {value_str}  (target: {kpi['target']})")

    lines.append("")
    lines.append("TRIGGERS:")
    for trigger in result.get("triggers", []):
        sev = trigger.get("severity", "info").upper()
        lines.append(f"  [{sev}] {trigger.get('condition', '')} → {trigger.get('action', '')}")

    return "\n".join(lines)
