"""
Investment Memo Generator.
Produces decision-grade 12-section investment memos.

Sections:
 1. Company Snapshot — business, segments, geography, competitors
 2. Investment Thesis — bull/base/bear + catalysts
 3. Variant View — Base/Bull/Bear with explicit numeric targets
 4. Key Questions — 5 questions where answer change = rating change
 5. Financial Quality — factor scores + key metrics table
 6. Moat & Execution — competitive position signals
 7. Risks — idiosyncratic + macro
 8. Valuation — multiples + DCF-lite + comps + historical percentile
 9. Catalyst Calendar — event timeline 3-6 months
10. Positioning — style fit + sizing guideline
11. Tracking Dashboard — thesis KPIs + what would change our mind
12. Evidence Appendix — links, filings, tables, timestamps
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Any

logger = logging.getLogger(__name__)

TZ_WIB = timezone(timedelta(hours=7))


def run_memo_generation(
    ticker: str,
    period: str,
    pipeline_results: Optional[dict] = None,
    technical_levels: Optional[dict] = None,
    output_dir: Optional[str] = None,
) -> dict:
    """
    Generate a full investment memo.

    Args:
        ticker: Stock ticker
        period: Reporting period (e.g., Q4-2025)
        pipeline_results: Results from run-pipeline steps
        technical_levels: Technical analysis data
        output_dir: Directory to save memo file

    Returns:
        Dict with memo_path, memo_text, and structured data.
    """
    pr = pipeline_results or {}
    now_wib = datetime.now(TZ_WIB)

    # ── Gather all data ──
    company_info = _get_company_info(ticker)
    factor_data = _compute_factors(ticker)
    coverage_data = _check_coverage(ticker, factor_data)
    thesis_data = _get_or_init_thesis(ticker)
    valuation_data = pr.get("valuation", {})
    sector_data = pr.get("sector_scoring", {})
    news_data = _get_news_catalysts(ticker)
    audit_data = pr.get("audit", {})

    # Get existing summary data for confidence/rating
    from .generator import run_summary_generation
    summary = run_summary_generation(ticker, period, pipeline_results, technical_levels)

    # Gather style/positioning data
    positioning_data = _compute_positioning(
        ticker, factor_data, thesis_data, sector_data, coverage_data
    )

    # ── Build 12 memo sections ──
    memo_lines = []

    # Header
    memo_lines.extend(_build_header(ticker, period, now_wib, summary))
    memo_lines.append("")

    # Section 1: Company Snapshot
    memo_lines.extend(_build_section_snapshot(company_info, ticker))
    memo_lines.append("")

    # Section 2: Investment Thesis
    memo_lines.extend(_build_section_thesis(thesis_data, news_data))
    memo_lines.append("")

    # Section 3: Variant View (Base / Bull / Bear with numbers)
    memo_lines.extend(_build_section_variant_view(
        ticker, thesis_data, valuation_data, factor_data, company_info
    ))
    memo_lines.append("")

    # Section 4: Key Questions
    memo_lines.extend(_build_section_key_questions(
        factor_data.get("sector", "general")
    ))
    memo_lines.append("")

    # Section 5: Financial Quality
    memo_lines.extend(_build_section_financial_quality(factor_data, coverage_data))
    memo_lines.append("")

    # Section 6: Moat & Execution
    memo_lines.extend(_build_section_moat(company_info, factor_data))
    memo_lines.append("")

    # Section 7: Risks
    memo_lines.extend(_build_section_risks(sector_data, company_info))
    memo_lines.append("")

    # Section 8: Valuation
    memo_lines.extend(_build_section_valuation(valuation_data, ticker))
    memo_lines.append("")

    # Section 9: Catalyst Calendar
    memo_lines.extend(_build_section_catalyst_calendar(ticker, news_data))
    memo_lines.append("")

    # Section 10: Positioning
    memo_lines.extend(_build_section_positioning(positioning_data))
    memo_lines.append("")

    # Section 11: Tracking Dashboard
    memo_lines.extend(_build_section_tracking(thesis_data, coverage_data))
    memo_lines.append("")

    # Section 12: Evidence Appendix
    memo_lines.extend(_build_section_evidence(summary, audit_data, now_wib))

    memo_text = "\n".join(memo_lines)

    # ── Save to file ──
    if output_dir is None:
        output_dir = str(Path.cwd() / "output")
    os.makedirs(output_dir, exist_ok=True)

    filename = f"memo_{ticker}_{period}.md"
    memo_path = os.path.join(output_dir, filename)

    with open(memo_path, "w", encoding="utf-8") as f:
        f.write(memo_text)

    logger.info(f"Investment memo saved to {memo_path}")

    return {
        "memo_path": memo_path,
        "memo_text": memo_text,
        "ticker": ticker,
        "period": period,
        "rating": summary.get("rating", "N/A"),
        "confidence": summary.get("confidence", 0),
        "coverage_passed": coverage_data.get("passed", False),
        "thesis_status": thesis_data.get("status", "unknown"),
        "sections": 12,
    }


# ============================================
# Section Builders
# ============================================

def _build_header(ticker: str, period: str, now_wib: datetime,
                  summary: dict) -> list[str]:
    L = [
        "═" * 60,
        f"  INVESTMENT MEMO: {ticker}",
        f"  Period: {period}",
        f"  Date: {now_wib.strftime('%Y-%m-%d %H:%M WIB')}",
        "═" * 60,
        "",
        f"Rating: {summary.get('rating', 'N/A')} | "
        f"Confidence: {summary.get('confidence', 0):.0%} | "
        f"Integrity: {summary.get('integrity_flag', 'N/A')}",
        "─" * 60,
    ]
    return L


def _build_section_snapshot(info: dict, ticker: str) -> list[str]:
    L = [
        "1. COMPANY SNAPSHOT",
        "─" * 40,
    ]

    name = info.get("name", ticker)
    sector = info.get("sector", "N/A")
    industry = info.get("industry", "N/A")
    country = info.get("country", "N/A")
    employees = info.get("employees")
    market_cap = info.get("market_cap")
    website = info.get("website", "")

    L.append(f"  Company: {name}")
    L.append(f"  Sector: {sector} — {industry}")
    L.append(f"  Country: {country}")

    if employees:
        L.append(f"  Employees: {employees:,}")
    if market_cap:
        if market_cap >= 1e12:
            L.append(f"  Market Cap: ${market_cap/1e12:.1f}T")
        elif market_cap >= 1e9:
            L.append(f"  Market Cap: ${market_cap/1e9:.1f}B")
        else:
            L.append(f"  Market Cap: ${market_cap/1e6:.0f}M")
    if website:
        L.append(f"  Website: {website}")

    # Business description (truncated)
    desc = info.get("description", "")
    if desc:
        # First 2 sentences
        sentences = desc.split(". ")
        short_desc = ". ".join(sentences[:3])
        if len(short_desc) > 300:
            short_desc = short_desc[:300] + "..."
        L.append("")
        L.append(f"  {short_desc}")

    # Competitors
    competitors = info.get("competitors", [])
    if competitors:
        L.append(f"  Competitors: {', '.join(competitors[:5])}")

    return L


def _build_section_thesis(thesis: dict, news: dict) -> list[str]:
    L = [
        "2. INVESTMENT THESIS",
        "─" * 40,
    ]

    status = thesis.get("status", "unknown").upper().replace("_", " ")
    L.append(f"  Status: {status}")
    L.append("")
    L.append(f"  BASE: {thesis.get('base_thesis', 'Not defined')}")
    L.append(f"  BULL: {thesis.get('bull_case', 'Not defined')}")
    L.append(f"  BEAR: {thesis.get('bear_case', 'Not defined')}")

    # Catalysts from news
    catalysts = news.get("top_catalysts", [])
    if catalysts:
        L.append("")
        L.append("  UPCOMING CATALYSTS:")
        for cat in catalysts[:5]:
            L.append(f"    • {cat.get('event', 'N/A')} — {cat.get('date_str', 'TBD')}")

    changes = news.get("changes_since_last", [])
    if changes:
        L.append("")
        L.append("  WHAT CHANGED:")
        for change in changes[:5]:
            L.append(f"    • {change}")

    return L


def _build_section_financial_quality(factors: dict, coverage: dict) -> list[str]:
    L = [
        "5. FINANCIAL QUALITY",
        "─" * 40,
    ]

    # Factor scores table
    composite = factors.get("composite_score", 0)
    L.append(f"  Composite Score: {composite}/100 (sector: {factors.get('sector', 'N/A')})")
    L.append("")

    factor_scores = factors.get("factor_scores", {})
    L.append(f"  {'Factor':<20} {'Score':>6} {'Coverage':>10}")
    L.append(f"  {'─'*20} {'─'*6} {'─'*10}")

    for factor_name in ["quality", "growth", "balance_sheet", "cashflow", "shareholder"]:
        fdata = factor_scores.get(factor_name, {})
        score = fdata.get("score", 0)
        cov = fdata.get("coverage", "0/0")
        label = factor_name.replace("_", " ").title()
        L.append(f"  {label:<20} {score:>5.1f} {cov:>10}")

    # Key metrics detail for strongest/weakest
    L.append("")
    L.append("  KEY METRICS:")
    for factor_name, fdata in factor_scores.items():
        for comp in fdata.get("components", []):
            if comp.get("status") == "computed":
                val = comp.get("value", 0)
                sc = comp.get("score", 0)
                icon = "▲" if sc >= 70 else "▼" if sc < 30 else "─"
                metric_label = comp["metric"].replace("_", " ").title()
                if isinstance(val, float) and abs(val) < 10:
                    val_str = f"{val:.2%}"
                else:
                    val_str = f"{val:.2f}" if isinstance(val, float) else str(val)
                L.append(f"    {icon} {metric_label}: {val_str} (score: {sc:.0f})")

    # Coverage contract
    if not coverage.get("passed", True):
        L.append("")
        L.append(f"  ⚠ COVERAGE CONTRACT FAILED")
        missing = coverage.get("missing_required", [])
        if missing:
            L.append(f"    Missing: {', '.join(missing)}")
        L.append(f"    Rating locked: {coverage.get('rating_lock_reason', '')}")

    return L


def _build_section_moat(info: dict, factors: dict) -> list[str]:
    L = [
        "6. MOAT & EXECUTION",
        "─" * 40,
    ]

    # Derive moat signals from factor data
    quality = factors.get("factor_scores", {}).get("quality", {})
    quality_score = quality.get("score", 0)

    if quality_score >= 75:
        L.append("  Competitive Position: STRONG")
        L.append("  High margins and returns suggest pricing power and durable advantages.")
    elif quality_score >= 50:
        L.append("  Competitive Position: MODERATE")
        L.append("  Adequate returns but limited pricing power visibility.")
    else:
        L.append("  Competitive Position: WEAK")
        L.append("  Below-average margins suggest limited competitive advantages.")

    # Growth trajectory
    growth = factors.get("factor_scores", {}).get("growth", {})
    growth_score = growth.get("score", 0)
    L.append("")
    if growth_score >= 70:
        L.append("  Growth Trajectory: ACCELERATING — strong momentum across metrics")
    elif growth_score >= 45:
        L.append("  Growth Trajectory: STEADY — moderate growth in line with sector")
    else:
        L.append("  Growth Trajectory: DECELERATING — growth below sector norms")

    # Cash generation
    cf = factors.get("factor_scores", {}).get("cashflow", {})
    cf_score = cf.get("score", 0)
    L.append("")
    if cf_score >= 70:
        L.append("  Cash Conversion: EXCELLENT — strong FCF with efficient capital allocation")
    elif cf_score >= 45:
        L.append("  Cash Conversion: ADEQUATE — positive FCF but room for improvement")
    else:
        L.append("  Cash Conversion: POOR — weak cash generation requires monitoring")

    return L


def _build_section_risks(sector_data: dict, info: dict) -> list[str]:
    L = [
        "7. RISKS",
        "─" * 40,
    ]

    # Idiosyncratic risks from sector scoring
    risk_flags = sector_data.get("risk_flags", [])
    if risk_flags:
        L.append("  IDIOSYNCRATIC RISKS:")
        for flag in risk_flags:
            sev = flag.get("severity", "info").upper()
            L.append(f"    [{sev}] {flag.get('message', '')}")
    else:
        L.append("  IDIOSYNCRATIC RISKS: No significant flags triggered.")

    # Macro risks (generic by sector)
    sector = info.get("sector", "")
    L.append("")
    L.append("  MACRO RISKS:")
    macro_risks = _get_macro_risks(sector)
    for risk in macro_risks:
        L.append(f"    • {risk}")

    return L


def _build_section_valuation(val_data: dict, ticker: str) -> list[str]:
    L = [
        "8. VALUATION",
        chr(9472) * 40,
    ]

    if val_data.get("status") != "success":
        L.append("  Valuation data not available.")
        return L

    verdict = val_data.get("verdict", "N/A")
    L.append(f"  Verdict: {verdict.upper()}")
    L.append("")

    # Multiples (filter out market_cap which is not a multiple)
    multiples = val_data.get("multiples", {})
    display_multiples = {
        k: v for k, v in multiples.items()
        if v is not None and k not in ("market_cap",)
    }
    if display_multiples:
        L.append("  CURRENT MULTIPLES:")
        for metric, value in display_multiples.items():
            label = metric.upper().replace("_", "/")
            if isinstance(value, (int, float)):
                L.append(f"    {label}: {value:.1f}x")
            else:
                L.append(f"    {label}: {value}")

    # 3-Statement Mini Model
    model = val_data.get("mini_3statement", {})
    if model.get("status") == "success":
        L.append("")
        cur = model.get("current", {})
        L.append(f"  3-STATEMENT MODEL (current: Rev ${cur.get('revenue_b', '?')}B, "
                 f"OpM {cur.get('op_margin_pct', '?')}%, "
                 f"NetM {cur.get('net_margin_pct', '?')}%):")
        sep = chr(9472)
        L.append(f"  {'Year':<6} {'Revenue':>10} {'OpIncome':>10} {'OpM%':>6} "
                 f"{'NetIncome':>10} {'FCF':>10} {'Growth':>7}")
        L.append(f"  {sep*6} {sep*10} {sep*10} {sep*6} "
                 f"{sep*10} {sep*10} {sep*7}")
        for p in model.get("projections", []):
            L.append(f"  Y{p['year']:<5} ${p['revenue']:>8.1f}B ${p['op_income']:>8.1f}B "
                     f"{p['op_margin']:>5.1f}% ${p['net_income']:>8.1f}B "
                     f"${p['fcf']:>8.1f}B {p['growth_rate']:>5.1f}%")
        assumptions = model.get("assumptions", {})
        L.append(f"  Assumptions: growth={assumptions.get('base_growth', '?')}, "
                 f"tax={assumptions.get('tax_rate', '?')}, "
                 f"target_margin={assumptions.get('target_op_margin', '?')}")

    # DCF-lite
    dcf = val_data.get("dcf_lite", {})
    if dcf:
        L.append("")
        dcf_status = dcf.get("status", "")
        if dcf_status == "success":
            L.append("  DCF INTRINSIC VALUE:")
            for scenario in ["bear", "base", "bull"]:
                val = dcf.get(scenario)
                upside = dcf.get(f"{scenario}_upside")
                assumptions = dcf.get(f"{scenario}_assumptions", {})
                if val:
                    L.append(f"    {scenario.upper()}: ${val:.2f} ({upside:+.1f}%) "
                             f"[WACC={assumptions.get('wacc', '?')}, "
                             f"g={assumptions.get('growth', '?')}]")
            cp = dcf.get("current_price")
            if cp:
                L.append(f"    Current Price: ${cp}")
        elif dcf_status == "negative_fcf":
            L.append(f"  DCF: {dcf.get('message', 'Negative FCF')}")
            cp = dcf.get("current_price")
            if cp:
                L.append(f"    Current Price: ${cp}, FCF/share: ${dcf.get('fcf_per_share', '?')}")

    # Sensitivity Table
    sens = val_data.get("sensitivity_table", {})
    if sens.get("status") == "success":
        L.append("")
        L.append(f"  SENSITIVITY TABLE (current: ${sens.get('current_price', '?')}, "
                 f"base FCF/sh: ${sens.get('base_fcf_ps', '?')}):")
        tg_labels = sens.get("tg_range", [])
        sep = chr(9472)
        L.append(f"  {'WACC':<6} " + " ".join(f"{'g=' + tg:>10}" for tg in tg_labels))
        L.append(f"  {sep*6} " + " ".join(sep * 10 for _ in tg_labels))
        for row in sens.get("grid", []):
            vals = []
            for tg in tg_labels:
                key = f"tg_{tg}"
                v = row.get(key, "?")
                vals.append(f"${v:>8.2f}" if isinstance(v, (int, float)) else f"{v:>9}")
            L.append(f"  {row.get('wacc', '?'):<6} " + " ".join(vals))

    # Historical percentile
    hist = val_data.get("historical_percentile", {})
    if hist:
        L.append("")
        L.append("  HISTORICAL CONTEXT (5Y):")
        for metric, pctiles in hist.items():
            if isinstance(pctiles, dict):
                current = pctiles.get("current", "N/A")
                p50 = pctiles.get("p50", "N/A")
                L.append(f"    {metric}: current={current}, median={p50}")

    return L


def _build_section_tracking(thesis: dict, coverage: dict) -> list[str]:
    L = [
        "11. TRACKING DASHBOARD",
        "─" * 40,
    ]

    L.append(f"  Thesis Status: {thesis.get('status', 'N/A').upper()}")
    L.append("")

    # KPIs
    kpi_results = thesis.get("kpi_results", [])
    kpis = thesis.get("kpis", [])
    display_kpis = kpi_results if kpi_results else kpis

    if display_kpis:
        L.append("  KPIs TO MONITOR:")
        for kpi in display_kpis:
            name = kpi.get("name", kpi.get("metric", "N/A"))
            target = kpi.get("target", "N/A")
            current = kpi.get("current_value")
            if current is not None:
                val_str = f"{current:.2%}" if isinstance(current, float) and abs(current) < 10 else str(current)
                L.append(f"    • {name}: {val_str} (target: {target})")
            else:
                L.append(f"    • {name}: — (target: {target})")

    # Triggers
    triggers = thesis.get("triggers", [])
    if triggers:
        L.append("")
        L.append("  WHAT WOULD CHANGE OUR MIND:")
        for trigger in triggers:
            action = trigger.get("action", "Review")
            condition = trigger.get("condition", "N/A")
            L.append(f"    [{trigger.get('severity', 'info').upper()}] If {condition} → {action}")

    # Coverage gaps
    missing_req = coverage.get("missing_required", [])
    missing_rec = coverage.get("missing_recommended", [])
    if missing_req or missing_rec:
        L.append("")
        L.append("  DATA GAPS:")
        if missing_req:
            L.append(f"    Required: {', '.join(missing_req)}")
        if missing_rec:
            L.append(f"    Recommended: {', '.join(missing_rec)}")

    return L


def _build_section_evidence(summary: dict, audit_data: dict,
                            now: datetime) -> list[str]:
    L = [
        "12. EVIDENCE APPENDIX",
        "─" * 40,
    ]

    # Evidence items from summary with source tier
    evidence = summary.get("evidence_items", [])
    if evidence:
        # Group by tier
        try:
            from ..analysis.news_sentiment import get_source_weight
            tier_a, tier_b, tier_c = [], [], []
            for ev in evidence:
                url = ev.get("source_url", "")
                weight = get_source_weight(url)
                tier_label = "A" if weight >= 0.90 else "B" if weight >= 0.70 else "C"
                ev["tier"] = tier_label
                if tier_label == "A":
                    tier_a.append(ev)
                elif tier_label == "B":
                    tier_b.append(ev)
                else:
                    tier_c.append(ev)
            L.append(f"  Sources ({len(evidence)}): "
                     f"Tier A: {len(tier_a)} | Tier B: {len(tier_b)} | Tier C: {len(tier_c)}")
        except Exception:
            L.append(f"  Sources ({len(evidence)}):")

        for ev in evidence:
            src_type = ev.get("type", "unknown")
            url = ev.get("source_url", "N/A")
            tier = ev.get("tier", "?")
            L.append(f"    [Tier {tier}][{src_type}] {url}")
    else:
        L.append("  No evidence items collected.")

    # Audit info
    if audit_data:
        L.append("")
        L.append("  AUDIT TRAIL:")
        L.append(f"    Run ID: {audit_data.get('run_id', 'N/A')}")
        L.append(f"    Sources used: {audit_data.get('sources_count', 0)}")
        row_counts = audit_data.get("row_counts", {})
        if row_counts:
            L.append("    Row counts:")
            for table, count in row_counts.items():
                L.append(f"      {table}: {count}")

    L.append("")
    L.append(f"  Generated: {now.strftime('%Y-%m-%d %H:%M:%S WIB')}")
    L.append("  Disclaimer: This memo is auto-generated. Verify all data before investment decisions.")
    L.append("═" * 60)

    return L


# ============================================
# NEW Section Builders (Sections 3, 4, 9, 10)
# ============================================


def _build_section_variant_view(
    ticker: str, thesis: dict, valuation: dict,
    factors: dict, info: dict,
) -> list[str]:
    """Section 3: Base / Bull / Bear with explicit numeric targets."""
    L = [
        "3. VARIANT VIEW",
        "─" * 40,
    ]

    # Get current price
    current_price = None
    dcf = valuation.get("dcf_lite", {})
    current_price = dcf.get("current_price")
    if not current_price:
        try:
            import yfinance as yf
            current_price = yf.Ticker(ticker).info.get("currentPrice")
        except Exception:
            pass

    market_cap = info.get("market_cap", 0) or 0
    composite = factors.get("composite_score", 50)

    # Revenue growth and margin from factor components
    rev_growth = None
    op_margin = None
    fcf_margin = None
    for fdata in factors.get("factor_scores", {}).values():
        for comp in fdata.get("components", []):
            if comp.get("status") == "computed":
                m = comp["metric"]
                v = comp.get("value")
                if m == "revenue_growth" and v is not None:
                    rev_growth = v
                elif m == "operating_margin" and v is not None:
                    op_margin = v
                elif m == "fcf_margin" and v is not None:
                    fcf_margin = v

    price_str = f"${current_price:.2f}" if current_price else "N/A"
    L.append(f"  Current Price: {price_str}")
    L.append("")

    # Base case
    L.append("  BASE CASE (60% probability):")
    if rev_growth is not None:
        base_growth = rev_growth
        L.append(f"    Revenue growth: {base_growth:.1%} (continues current trajectory)")
    if op_margin is not None:
        L.append(f"    Op margin: {op_margin:.1%} (stable)")
    if current_price:
        base_target = current_price * 1.10  # 10% upside
        L.append(f"    Price target: ${base_target:.2f} (+10%)")
    L.append(f"    Thesis: {thesis.get('base_thesis', 'N/A')}")
    L.append("")

    # Bull case
    L.append("  BULL CASE (25% probability):")
    if rev_growth is not None:
        bull_growth = rev_growth * 1.5 if rev_growth > 0 else rev_growth + 0.10
        L.append(f"    Revenue growth: {bull_growth:.1%} (accelerates)")
    if op_margin is not None:
        L.append(f"    Op margin: {op_margin + 0.03:.1%} (+300bp expansion)")
    if current_price:
        bull_target = current_price * 1.30  # 30% upside
        L.append(f"    Price target: ${bull_target:.2f} (+30%)")
    L.append(f"    Thesis: {thesis.get('bull_case', 'N/A')}")
    L.append("")

    # Bear case
    L.append("  BEAR CASE (15% probability):")
    if rev_growth is not None:
        bear_growth = max(rev_growth * 0.3, rev_growth - 0.10)
        L.append(f"    Revenue growth: {bear_growth:.1%} (decelerates materially)")
    if op_margin is not None:
        L.append(f"    Op margin: {max(op_margin - 0.05, 0):.1%} (-500bp compression)")
    if current_price:
        bear_target = current_price * 0.75  # -25% downside
        L.append(f"    Price target: ${bear_target:.2f} (-25%)")
    L.append(f"    Thesis: {thesis.get('bear_case', 'N/A')}")

    # Expected value
    if current_price:
        ev = 0.60 * (current_price * 1.10) + 0.25 * (current_price * 1.30) + 0.15 * (current_price * 0.75)
        ev_upside = (ev - current_price) / current_price
        L.append("")
        L.append(f"  PROBABILITY-WEIGHTED TARGET: ${ev:.2f} ({ev_upside:+.1%})")

    return L


def _build_section_key_questions(sector: str) -> list[str]:
    """Section 4: 5 key questions where answer change = rating change."""
    L = [
        "4. KEY QUESTIONS",
        "─" * 40,
        "  If any answer changes, revisit the rating.",
        "",
    ]

    try:
        from ..analysis.sector_questions import get_sector_questions
        questions = get_sector_questions(sector)
    except Exception:
        questions = []

    if questions:
        for i, q in enumerate(questions, 1):
            L.append(f"  Q{i}. {q['question']}")
            L.append(f"      Trigger: {q['trigger']}")
            L.append("")
    else:
        L.append("  No sector-specific questions configured.")

    return L


def _build_section_catalyst_calendar(ticker: str, news_data: dict) -> list[str]:
    """Section 9: Event timeline for next 3-6 months."""
    L = [
        "9. CATALYST CALENDAR",
        "─" * 40,
    ]

    events = []

    # Corporate events from yfinance
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        cal = stock.calendar
        if cal and isinstance(cal, dict):
            # Earnings dates
            earnings = cal.get("Earnings Date")
            if isinstance(earnings, list):
                for ed in earnings:
                    events.append({
                        "date": str(ed),
                        "event": "Earnings Release",
                        "type": "earnings",
                        "impact": "HIGH",
                    })
            elif earnings:
                events.append({
                    "date": str(earnings),
                    "event": "Earnings Release",
                    "type": "earnings",
                    "impact": "HIGH",
                })

            # Dividend dates
            ex_div = cal.get("Ex-Dividend Date")
            if ex_div:
                events.append({
                    "date": str(ex_div),
                    "event": "Ex-Dividend Date",
                    "type": "dividend",
                    "impact": "MEDIUM",
                })

            div_date = cal.get("Dividend Date")
            if div_date:
                events.append({
                    "date": str(div_date),
                    "event": "Dividend Payment",
                    "type": "dividend",
                    "impact": "LOW",
                })

        # Revenue/earnings estimates for guidance context
        try:
            info = stock.info
            rev_est = info.get("revenueEstimate")
            if rev_est:
                events.append({
                    "date": "Next Quarter",
                    "event": f"Revenue est: ${rev_est/1e9:.1f}B" if isinstance(rev_est, (int, float)) and rev_est > 1e6 else f"Revenue est: {rev_est}",
                    "type": "guidance",
                    "impact": "MEDIUM",
                })
        except Exception:
            pass

    except Exception:
        pass

    # News-based catalysts
    for cat in news_data.get("top_catalysts", []):
        if cat.get("event") not in [e.get("event") for e in events]:
            events.append({
                "date": cat.get("date_str", "TBD"),
                "event": cat.get("event", "Unknown"),
                "type": "news",
                "impact": "MEDIUM",
            })

    if events:
        L.append(f"  {'Date':<20} {'Event':<35} {'Impact':<8}")
        L.append(f"  {'─'*20} {'─'*35} {'─'*8}")
        for ev in events:
            L.append(f"  {ev['date']:<20} {ev['event']:<35} {ev['impact']:<8}")
    else:
        L.append("  No upcoming catalysts identified.")

    # Guidance window note
    L.append("")
    L.append("  NOTE: Verify dates—corporate calendars update after each earnings release.")

    return L


def _build_section_positioning(positioning: dict) -> list[str]:
    """Section 10: Style fit + sizing guideline."""
    L = [
        "10. POSITIONING",
        "─" * 40,
    ]

    # Investment style
    styles = positioning.get("styles", ["general"])
    style_descs = positioning.get("style_descriptions", {})
    L.append("  INVESTMENT STYLE:")
    for style in styles:
        desc = style_descs.get(style, "")
        L.append(f"    {style.upper()}: {desc}")

    # Sizing
    sizing = positioning.get("sizing", {})
    L.append("")
    L.append("  POSITION SIZING:")
    L.append(f"    Tier: {sizing.get('size_tier', 'N/A').upper().replace('_', ' ')}")
    L.append(f"    Suggested weight: {sizing.get('weight', 'N/A')}")
    L.append(f"    Rationale: {sizing.get('criteria', 'N/A')}")

    # Suitable for
    L.append("")
    L.append("  SUITABLE FOR:")
    if "growth" in styles:
        L.append("    ✓ Growth-oriented portfolios")
    if "value" in styles:
        L.append("    ✓ Value/income portfolios")
    if "quality" in styles:
        L.append("    ✓ Quality compounders portfolios")
    if "turnaround" in styles:
        L.append("    ✓ Special situations / turnaround portfolios")
    if not styles or styles == ["general"]:
        L.append("    ✓ Diversified / core holdings")

    return L


# ============================================
# Data Gathering Helpers
# ============================================


def _compute_positioning(
    ticker: str, factor_data: dict, thesis_data: dict,
    sector_data: dict, coverage_data: dict,
) -> dict:
    """Compute style classification and sizing recommendation."""
    try:
        from ..analysis.sector_questions import (
            classify_style, get_sizing_recommendation, STYLE_CRITERIA,
        )

        # Build metrics dict for style classification
        metrics = {}
        for fdata in factor_data.get("factor_scores", {}).values():
            for comp in fdata.get("components", []):
                if comp.get("status") == "computed" and comp.get("value") is not None:
                    metrics[comp["metric"]] = comp["value"]

        # Also pull from yfinance for P/E
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            pe_fwd = info.get("forwardPE")
            div_y = info.get("dividendYield")
            if pe_fwd:
                metrics["pe_forward"] = pe_fwd
            if div_y:
                metrics["dividend_yield"] = div_y
        except Exception:
            pass

        styles = classify_style(metrics)
        style_descriptions = {s: STYLE_CRITERIA[s]["description"]
                              for s in styles if s in STYLE_CRITERIA}

        sizing = get_sizing_recommendation(
            composite_score=factor_data.get("composite_score", 50),
            thesis_status=thesis_data.get("status", "unknown"),
            risk_flags=sector_data.get("risk_flags", []),
            coverage_passed=coverage_data.get("passed", True),
        )

        return {
            "styles": styles,
            "style_descriptions": style_descriptions,
            "sizing": sizing,
        }
    except Exception as e:
        logger.warning(f"Positioning computation failed: {e}")
        return {
            "styles": ["general"],
            "style_descriptions": {},
            "sizing": {"size_tier": "standard", "weight": "2-4%", "criteria": "Default"},
        }


def _get_company_info(ticker: str) -> dict:
    """Fetch company information from yfinance."""
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info

        return {
            "name": info.get("longName") or info.get("shortName", ticker),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "country": info.get("country", "N/A"),
            "employees": info.get("fullTimeEmployees"),
            "market_cap": info.get("marketCap"),
            "website": info.get("website", ""),
            "description": info.get("longBusinessSummary", ""),
            "competitors": _infer_competitors(info.get("industry", ""), ticker),
        }
    except Exception as e:
        logger.warning(f"Could not fetch company info: {e}")
        return {"name": ticker}


def _compute_factors(ticker: str) -> dict:
    """Compute factor model scores."""
    try:
        from ..analysis.factor_model import compute_all_factors, get_metrics_from_yfinance
        metrics = get_metrics_from_yfinance(ticker)
        return compute_all_factors(ticker, metrics)
    except Exception as e:
        logger.warning(f"Factor computation failed: {e}")
        return {"composite_score": 0, "factor_scores": {}, "sector": "general"}


def _check_coverage(ticker: str, factor_data: dict) -> dict:
    """Check coverage contract."""
    try:
        from ..analysis.coverage_contracts import check_coverage
        # Build available metrics set from factor data
        available = set()
        for fdata in factor_data.get("factor_scores", {}).values():
            for comp in fdata.get("components", []):
                if comp.get("status") == "computed":
                    available.add(comp["metric"])
        return check_coverage(ticker, available, factor_data.get("sector"))
    except Exception as e:
        logger.warning(f"Coverage check failed: {e}")
        return {"passed": True, "missing_required": [], "missing_recommended": []}


def _get_or_init_thesis(ticker: str) -> dict:
    """Get existing thesis or auto-initialize."""
    try:
        from ..analysis.thesis_tracker import check_thesis, init_thesis

        result = check_thesis(ticker)
        if result.get("status") == "no_thesis":
            # Auto-initialize
            thesis_data = init_thesis(ticker)
            return thesis_data
        return result
    except Exception as e:
        logger.warning(f"Thesis retrieval failed: {e}")
        return {"status": "unknown", "base_thesis": "Not available"}


def _get_news_catalysts(ticker: str) -> dict:
    """Get news-based catalysts and changes."""
    catalysts = []

    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        cal = stock.calendar
        if cal is not None:
            if isinstance(cal, dict):
                earnings_date = cal.get("Earnings Date")
                if earnings_date:
                    if isinstance(earnings_date, list) and len(earnings_date) > 0:
                        catalysts.append({
                            "event": "Earnings Release",
                            "date_str": str(earnings_date[0]),
                        })
                    else:
                        catalysts.append({
                            "event": "Earnings Release",
                            "date_str": str(earnings_date),
                        })

                ex_div = cal.get("Ex-Dividend Date")
                if ex_div:
                    catalysts.append({
                        "event": "Ex-Dividend Date",
                        "date_str": str(ex_div),
                    })
    except Exception:
        pass

    return {
        "top_catalysts": catalysts,
        "changes_since_last": [],
    }


def _infer_competitors(industry: str, ticker: str) -> list[str]:
    """Infer competitors based on industry."""
    INDUSTRY_PEERS = {
        "Software—Infrastructure": ["MSFT", "ORCL", "IBM", "SAP", "CRM"],
        "Semiconductors": ["NVDA", "AMD", "INTC", "TSM", "AVGO"],
        "Internet Content & Information": ["GOOG", "META", "SNAP", "PINS"],
        "Consumer Electronics": ["AAPL", "SONY", "SAMSUNG"],
        "Banks—Regional": ["BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK"],
        "Banks—Diversified": ["JPM", "BAC", "WFC", "C", "GS"],
    }
    peers = INDUSTRY_PEERS.get(industry, [])
    return [p for p in peers if p != ticker][:5]


def _get_macro_risks(sector: str) -> list[str]:
    """Get generic macro risks by sector."""
    MACRO_RISKS = {
        "Technology": [
            "Rising interest rates reducing growth stock multiples",
            "Regulatory pressure (antitrust, data privacy, AI governance)",
            "Geopolitical tensions affecting supply chains and market access",
        ],
        "Financial Services": [
            "Interest rate direction impacting NIM and bond portfolios",
            "Credit cycle deterioration leading to rising provisions",
            "Regulatory capital requirement changes",
        ],
        "Consumer Cyclical": [
            "Consumer spending weakness from inflation/unemployment",
            "Currency fluctuations affecting margins",
            "Input cost inflation compressing margins",
        ],
    }
    # Match sector to closest key
    for key, risks in MACRO_RISKS.items():
        if key.lower() in sector.lower() or sector.lower() in key.lower():
            return risks
    return [
        "Global macro uncertainty (rates, inflation, geopolitics)",
        "Sector-specific regulatory changes",
        "Currency and commodity price volatility",
    ]
