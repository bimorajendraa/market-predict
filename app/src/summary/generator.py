"""
Audit-Friendly Summary Generator (v2).
Provides evidence-driven analysis with strict integrity and confidence rules.

Main Features:
1. Integrity Checks: Verify used news URLs exist in raw collection.
2. Primary Source Check: Detect if reports were downloaded or fallback to yfinance.
3. Action Rating Logic: Force "Hold/Watch" if confidence < 0.60.
4. Bank Metrics Check: Flag missing key banking ratios.
5. Evidence Pack: Top 8 item citations.
6. Structured JSON Output.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Any

from ..db import get_db_cursor
from ..market.price_fetcher import get_returns
from ..analysis.financial_scoring import METRIC_DESCRIPTIONS
from ..analysis.news_sentiment import _resolve_company_names

logger = logging.getLogger(__name__)

# Jakarta Timezone
TZ_WIB = timezone(timedelta(hours=7))

# Base Rating Thresholds
RATING_THRESHOLDS = [
    (80, "Strong Buy"),
    (65, "Buy"),
    (45, "Hold"),
    (30, "Sell"),
    (0, "Strong Sell"),
]

# Known Bank Tickers (Indonesia)
BANK_TICKERS = {
    "BBCA", "BBRI", "BMRI", "BBNI", "BBTN", "BRIS", "BDMN", "BNGA", "PNBN", "MEGA"
}


def get_base_rating(score: float) -> str:
    """Map numeric score to base rating label."""
    for threshold, label in RATING_THRESHOLDS:
        if score >= threshold:
            return label
    return "Strong Sell"


def run_summary_generation(
    ticker: str,
    period: str,
    pipeline_results: Optional[dict] = None,
    technical_levels: Optional[dict] = None,
) -> dict:
    """
    Generate strict audit-friendly company summary.
    """
    logger.info(f"Generating audit summary for {ticker} ({period})")
    now_wib = datetime.now(TZ_WIB)
    pr = pipeline_results or {}

    # 1. Gather Data
    score_data = _get_latest_score(ticker, period)
    score = float(score_data.get("score", 0)) if score_data else 0.0
    drivers = score_data.get("drivers_json", []) if score_data else []
    if isinstance(drivers, str):
        drivers = json.loads(drivers)

    computed_drivers = sorted(
        [d for d in drivers if d.get("status") == "computed"],
        key=lambda d: d.get("contribution", 0),
        reverse=True,
    )
    no_data_drivers = [d for d in drivers if d.get("status") == "no_data"]

    # Sentiment Items
    all_sentiment = _get_all_sentiment(ticker)
    
    # Coverage Stats
    coverage = _get_coverage_stats(ticker, pr)

    # Market Returns
    return_7d = get_returns(ticker, days=7)
    return_30d = get_returns(ticker, days=30)
    return_90d = get_returns(ticker, days=90)
    price_as_of = _get_price_as_of(ticker)

    # 2. Audit Checks
    # Integrity Check: verify URL existence
    integrity_flag, missing_urls = _check_integrity(ticker, all_sentiment)
    
    # Primary Source Check
    primary_flag = "OK"
    if coverage.get("reports_downloaded", 0) == 0:
        primary_flag = "MISSING"

    # Bank Metrics Check
    bank_check = _check_bank_metrics(ticker, computed_drivers)

    # 3. Confidence Calculation
    scoring_result = pr.get("scoring", {})
    coverage_factor = scoring_result.get("coverage_factor", 1.0)

    confidence = _compute_strict_confidence(
        score, coverage, computed_drivers, integrity_flag, primary_flag,
        coverage_factor=coverage_factor,
        ticker=ticker,
    )

    # 4. Rating Logic
    base_rating = get_base_rating(score)
    if confidence < 0.60 or coverage_factor < 0.50:
        action_rating = "Hold/Watch"
    else:
        action_rating = base_rating

    # 5. Evidence Pack
    evidence_items = _build_evidence_pack(computed_drivers, all_sentiment, ticker)

    # 6. Build Data Gaps List
    data_gaps = _identify_data_gaps(coverage, no_data_drivers, bank_check, primary_flag)

    # 7. Construct Narrative
    narrative = _build_narrative(
        ticker=ticker,
        period=period,
        now_wib=now_wib,
        score=score,
        base_rating=base_rating,
        action_rating=action_rating,
        confidence=confidence,
        coverage=coverage,
        price_as_of=price_as_of,
        integrity_flag=integrity_flag,
        missing_urls=missing_urls,
        primary_flag=primary_flag,
        bank_metrics_missing=bank_check,
        evidence_items=evidence_items,
        return_7d=return_7d,
        return_30d=return_30d,
        return_90d=return_90d,
        computed_drivers=computed_drivers,
        ml_prediction=pr.get("ml_prediction", {}),
        technical_levels=technical_levels or {},
        valuation=pr.get("valuation", {}),
        sector_scoring=pr.get("sector_scoring", {}),
    )

    # 8. Final JSON Output
    evidence_urls = [ev["source_url"] for ev in evidence_items if ev.get("source_url")]

    summary_json = {
        "ticker": ticker,
        "integrity_flag": integrity_flag,
        "primary_flag": primary_flag,
        "base_rating": base_rating,
        "action_rating": action_rating,
        "confidence": round(confidence, 2),
        "data_gaps": data_gaps,
        "evidence_urls": evidence_urls,
    }

    # Append JSON block to narrative
    narrative += json.dumps(summary_json, indent=2)

    result = {
        "ticker": ticker,
        "period": period,
        "rating": action_rating,  # Use action rating as main rating
        "confidence": round(confidence, 2),
        "narrative": narrative,
        "evidence_json": summary_json,
    }

    return result


# ============================================
# Logic Helpers
# ============================================

def _compute_strict_confidence(
    score: float,
    coverage: dict,
    computed_drivers: list,
    integrity_flag: str,
    primary_flag: str,
    coverage_factor: float = 1.0,
    ticker: str = "",
) -> float:
    """Compute confidence score [0.0, 0.95] with adaptive caps.
    
    The primary source cap is now context-aware:
    - Filing-required sectors (banks) with sparse data: hard 0.60 cap
    - Strong yfinance data (30+ facts) + good news (20+ items): 0.80 cap
    - Moderate coverage: 0.70 cap
    - Sparse data everywhere: 0.60 cap
    """
    conf = 0.85
    
    # Coverage penalty
    if not computed_drivers:
        conf -= 0.3
    
    # News penalty
    news_count = coverage.get("sentiment_items", 0)
    if news_count == 0:
        conf -= 0.3
    elif news_count < 10:
        conf -= 0.15
        
    # Integrity penalty
    if integrity_flag == "FAIL":
        conf -= 0.15
        
    # Primary Source — Adaptive Cap
    if primary_flag == "MISSING":
        financial_facts = coverage.get("financial_facts", 0)
        is_filing_required = _is_filing_required_sector(ticker)
        
        if is_filing_required and financial_facts < 30:
            # Banks/regulated sectors with sparse data → hard cap
            conf = min(conf, 0.60)
        elif financial_facts >= 30 and news_count >= 20:
            # Strong alternative coverage → mild penalty
            conf = min(conf, 0.80)
            conf -= 0.05
        elif financial_facts >= 10:
            # Moderate coverage → moderate cap
            conf = min(conf, 0.70)
        else:
            # No alternatives → hard cap
            conf = min(conf, 0.60)

    # Coverage factor penalty: scale confidence by how much data we have
    conf *= coverage_factor
        
    # Absolute bounds (max 0.95 to reflect uncertainty)
    return round(max(0.0, min(0.95, conf)), 2)


def _is_filing_required_sector(ticker: str) -> bool:
    """Check if ticker is in a sector that really requires primary filings.
    
    Banks and regulated financial institutions in Indonesia need
    primary filings (OJK data, annual reports) for reliable scoring.
    yfinance data alone is insufficient for NIM, NPL, CAR metrics.
    """
    base_ticker = ticker.split(".")[0].upper() if ticker else ""
    filing_required_tickers = {
        "BBCA", "BBRI", "BMRI", "BBNI", "BBTN", "BRIS",
        "BDMN", "BNGA", "PNBN", "MEGA",
    }
    return base_ticker in filing_required_tickers


def _check_integrity(ticker: str, sentiment_items: list[dict]) -> tuple[str, list[str]]:
    """Verify sentiment URLs exist in news_items table."""
    used_urls = set()
    for item in sentiment_items:
        sources = _parse_sources(item)
        for s in sources:
            if s.get("url"):
                used_urls.add(s["url"])
    
    if not used_urls:
        return "PASS", []

    # Check existence via batch query
    found_urls = set()
    url_list = list(used_urls)
    
    with get_db_cursor() as cur:
        # Batch checks in chunks of 50 to avoid huge query params if many items
        chunk_size = 50
        for i in range(0, len(url_list), chunk_size):
            chunk = url_list[i:i+chunk_size]
            cur.execute(
                "SELECT url FROM news_items WHERE url = ANY(%(urls)s)", 
                {"urls": chunk}
            )
            for row in cur.fetchall():
                found_urls.add(row["url"])
                
    missing = used_urls - found_urls
    if missing:
        return "FAIL", list(missing)[:5]
    return "PASS", []


def _check_bank_metrics(ticker: str, drivers: list[dict]) -> list[str]:
    """Check for missing bank metrics if ticker looks like a bank."""
    is_bank = any(ticker.startswith(b) for b in BANK_TICKERS)
    if not is_bank:
        return []
        
    driver_names = {d.get("metric") for d in drivers}
    required = ["net_interest_margin", "non_performing_loan", "capital_adequacy_ratio"]
    missing = [m for m in required if m not in driver_names and m not in driver_names] # check exact map later
    
    # Since we use yfinance standard metrics primarily, these are likely missing unless mapped
    # We just report them as missing if not present.
    return missing


def _identify_data_gaps(
    coverage: dict, 
    no_data_drivers: list, 
    bank_missing: list,
    primary_flag: str
) -> list[str]:
    gaps = []
    if primary_flag == "MISSING":
        gaps.append("Primary filings (prospectus/report) missing -- fetch from IR/IDX.")
    
    if bank_missing:
        gaps.append(f"Bank metrics missing: {', '.join(bank_missing)}")
        
    for d in no_data_drivers[:3]:
        name = d.get("metric", "?")
        gaps.append(f"Missing core metric: {name}")
        
    if coverage.get("sentiment_items", 0) < 5:
        gaps.append("Low news coverage (<5 items).")
        
    return gaps


def _build_evidence_pack(
    drivers: list[dict], 
    sentiment_items: list[dict], 
    ticker: str
) -> list[dict]:
    items = []
    
    # Top 3 High-Impact News (Sorted by impact)
    sorted_news = sorted(
        sentiment_items, 
        key=lambda x: abs(float(x.get("impact", 0))), 
        reverse=True
    )
    
    for news in sorted_news[:5]:
        sources = _parse_sources(news)
        url = sources[0].get("url") if sources else "N/A"
        date = sources[0].get("date") if sources else str(news.get("date", ""))
        
        items.append({
            "title": news.get("headline", "")[:100],
            "source_url": url,
            "publish_date": date,
            "fetched_at": str(news.get("created_at") or datetime.now()),
            "why_it_matters": f"Sentiment {news.get('sentiment')} (Impact {news.get('impact')})",
        })
        
    # Top 3 Financial Drivers
    for d in drivers[:3]:
        val = d.get("value")
        # Format val depending on metric? Just str for now
        items.append({
            "title": f"Financial Metric: {d.get('name', d.get('metric'))}",
            "source_url": f"yfinance:{ticker}",
            "publish_date": None,
            "fetched_at": datetime.now(TZ_WIB).isoformat(),
            "why_it_matters": f"Rated {d.get('rating_label')} (Score contribution: {d.get('contribution', 0):.1f})",
        })
        
    return items


# ============================================
# Narrative Builder
# ============================================

def _build_narrative(
    *, ticker, period, now_wib, score, base_rating, action_rating, confidence,
    coverage, price_as_of, integrity_flag, missing_urls, primary_flag,
    bank_metrics_missing, evidence_items, return_7d, return_30d, return_90d,
    computed_drivers, ml_prediction, technical_levels,
    valuation=None, sector_scoring=None,
) -> str:
    valuation = valuation or {}
    sector_scoring = sector_scoring or {}
    L = []
    
    # Header
    L.append(f"AS OF: {now_wib.strftime('%Y-%m-%d %H:%M WIB')}")
    L.append(f"TICKER: {ticker}")
    L.append(f"PERIOD: {period}")
    if price_as_of:
        L.append(f"PRICE DATE: {price_as_of.strftime('%Y-%m-%d')}")
    L.append("-" * 40)
    
    # 1. Integrity Checks
    L.append("1. INTEGRITY & AUDIT")
    if integrity_flag == "FAIL":
        L.append(f"  [!] INTEGRITY FAIL: Found sentiment items without raw source in DB.")
        for url in missing_urls:
            L.append(f"      Missing: {url}")
    else:
        L.append("  [OK] Integrity Pass: All sentiment sources verified in raw DB.")
        
    if primary_flag == "MISSING":
        financial_facts = coverage.get("financial_facts", 0)
        L.append("  [!] PRIMARY SOURCE MISSING: reports_downloaded=0.")
        if financial_facts >= 30:
            L.append(f"      Confidence adjusted (yfinance has {financial_facts} facts).")
        else:
            L.append("      Confidence penalized — limited alternative data.")
    else:
        L.append("  [OK] Primary Source: Reports downloaded.")

    # 2. Financial & Data Coverage
    L.append(f"  Coverage: {coverage.get('financial_facts', 0)} facts, {coverage.get('sentiment_items', 0)} news items.")
    L.append("")
    
    # 3. Verdict
    L.append("2. VERDICT")
    L.append(f"  Base Rating   : {base_rating} (Score {score:.1f})")
    L.append(f"  Action Rating : {action_rating}")
    L.append(f"  Confidence    : {confidence:.2f}")
    if confidence < 0.60:
        L.append(f"  (Reason: Confidence < 0.60 forces Hold/Watch)")
    
    # AI Signal Integration
    if ml_prediction.get("signal"):
        L.append("")
        L.append("  [AI MODEL SIGNAL]")
        L.append(f"  Signal     : {ml_prediction['signal']}")
        L.append(f"  Confidence : {ml_prediction.get('confidence', 0):.2f}")
        if ml_prediction.get("stop_loss"):
            L.append(f"  Stop Loss  : {ml_prediction['stop_loss']} (Risk Manage)")
    L.append("")
    
    # 4. Evidence Pack
    L.append("3. EVIDENCE PACK (Top Items)")
    if not evidence_items:
        L.append("  (No significant evidence items found)")
    else:
        for i, item in enumerate(evidence_items, 1):
            L.append(f"  {i}. {item['title']}")
            L.append(f"     URL: {item.get('source_url')}")
            L.append(f"     Date: {item.get('publish_date')} | Fetched: {item.get('fetched_at')}")
            L.append(f"     Why: {item['why_it_matters']}")
            L.append("")
        
    # 5. Bank Specific
    if bank_metrics_missing:
        L.append("4. BANK-SPECIFIC METRICS")
        L.append(f"  [!] Missing Critical Metrics: {', '.join(bank_metrics_missing)}")
        L.append("      Requesting additional data fetch for: NIM, NPL, CAR.")
        L.append("")
        
    # 6. Market Context
    L.append("5. MARKET CONTEXT")
    def fmt_ret(r): return f"{r:+.1%}" if r is not None else "N/A"
    L.append(f"  Returns: 7d {fmt_ret(return_7d)} | 30d {fmt_ret(return_30d)} | 90d {fmt_ret(return_90d)}")
    if return_30d is not None:
        if return_30d < -0.10: L.append("  Momentum: Oversight/Correction (Bearish)")
        elif return_30d > 0.10: L.append("  Momentum: Rally (Bullish)")
        else: L.append("  Momentum: Neutral/Range")
    L.append("")

    # 7. Technical Levels
    if technical_levels and technical_levels.get("status") == "ok":
        L.append("6. TECHNICAL LEVELS")
        L.append(f"  Current Price: {technical_levels['current_price']}")
        L.append("")

        bz = technical_levels.get("buy_zone")
        if bz:
            L.append(f"  BUY ZONE:  {bz['range_low']} - {bz['range_high']}  (Ideal: {bz['ideal']})")
        sz = technical_levels.get("sell_zone")
        if sz:
            L.append(f"  SELL ZONE: {sz['range_low']} - {sz['range_high']}  (Ideal: {sz['ideal']})")
        L.append("")

        # Trend & RSI
        trend = technical_levels.get("trend", {})
        if trend:
            L.append(f"  Trend:   {trend.get('trend', 'N/A')} -- {trend.get('cross_description', '')}")
            L.append(f"  MA20={trend.get('ma20')}  MA50={trend.get('ma50')}")

        rsi_data = technical_levels.get("rsi", {})
        if rsi_data:
            L.append(f"  RSI(14): {rsi_data.get('rsi', 'N/A')} ({rsi_data.get('rsi_zone', 'N/A')})")

        # Volatility Regime
        vol = technical_levels.get("volatility", {})
        if vol:
            L.append(f"  Volatility: {vol.get('volatility_regime', 'N/A')} (ATR={vol.get('atr')}, P{vol.get('atr_percentile')})")
            L.append(f"    {vol.get('volatility_description', '')}")
        L.append("")

        # Alerts
        alerts = technical_levels.get("alerts", [])
        if alerts:
            L.append("  ALERTS:")
            for alert in alerts:
                L.append(f"    {alert}")
            L.append("")

        sup = technical_levels.get("support", [])
        res = technical_levels.get("resistance", [])
        if sup:
            L.append(f"  Support:    {', '.join(str(s) for s in sup)}")
        if res:
            L.append(f"  Resistance: {', '.join(str(r) for r in res)}")

        fib = technical_levels.get("fibonacci", {})
        if fib:
            L.append("")
            L.append(f"  Fibonacci (60d range {fib.get('low')}-{fib.get('high')}):")
            for key, label in [("fib_236", "23.6%"), ("fib_382", "38.2%"), ("fib_500", "50.0%"), ("fib_618", "61.8%"), ("fib_786", "78.6%")]:
                if key in fib:
                    L.append(f"    {label}: {fib[key]}")

        pivot = technical_levels.get("pivot_points", {})
        if pivot:
            L.append("")
            L.append(f"  Pivot Points:  PP={pivot.get('PP')}")
            L.append(f"    R1={pivot.get('R1')}  R2={pivot.get('R2')}  R3={pivot.get('R3')}")
            L.append(f"    S1={pivot.get('S1')}  S2={pivot.get('S2')}  S3={pivot.get('S3')}")
    L.append("")

    # 7. Valuation
    if valuation.get("status") == "success":
        L.append("7. VALUATION")
        verdict = valuation.get("verdict", "N/A")
        L.append(f"  Verdict: {verdict.upper()}")
        # Show comparisons
        comparisons = valuation.get("comparisons", {})
        if isinstance(comparisons, dict):  # might be nested from run_valuation_analysis
            pass  # comparisons are in valuation_result but not directly here
        L.append("")

    # 8. Sector & Risk Flags
    if sector_scoring.get("status") == "success":
        L.append("8. SECTOR & RISK")
        L.append(f"  Sector: {sector_scoring.get('sector', 'N/A')}")
        L.append(f"  Score: {sector_scoring.get('sector_adjusted_score', 'N/A')} (risk penalty: -{sector_scoring.get('risk_penalty', 0)})")
        risk_flags = sector_scoring.get("risk_flags", [])
        if risk_flags:
            L.append("  Risk Flags:")
            for flag in risk_flags:
                L.append(f"    [{flag.get('severity', '?').upper()}] {flag.get('message', '')}")
        else:
            L.append("  No risk flags triggered.")
        L.append("")

    return "\n".join(L)


# ============================================
# Helpers
# ============================================

def _get_latest_score(ticker: str, period: str) -> Optional[dict]:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT score, drivers_json FROM scores_financial WHERE ticker=%(t)s AND period=%(p)s ORDER BY created_at DESC LIMIT 1",
            {"t": ticker, "p": period}
        )
        return cursor.fetchone()

def _get_all_sentiment(ticker: str) -> list[dict]:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT headline, sentiment, impact, sources_json, created_at, date FROM news_sentiment WHERE ticker=%(t)s ORDER BY impact DESC",
            {"t": ticker}
        )
        return cursor.fetchall()

def _get_coverage_stats(ticker: str, pr: dict) -> dict:
    stats = {
        "reports_downloaded": pr.get("reports", {}).get("jobs", 0),
        "news_collected": pr.get("news", {}).get("items", 0),
    }
    with get_db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as c FROM news_sentiment WHERE ticker=%(t)s", {"t": ticker})
        stats["sentiment_items"] = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM financial_facts WHERE ticker=%(t)s", {"t": ticker})
        stats["financial_facts"] = cur.fetchone()["c"]
    return stats

def _get_price_as_of(ticker: str) -> Optional[datetime]:
    with get_db_cursor() as cur:
        cur.execute("SELECT MAX(date) as d FROM market_prices WHERE ticker=%(t)s", {"t": ticker})
        row = cur.fetchone()
        return row["d"] if row else None

def _parse_sources(item: dict) -> list[dict]:
    raw = item.get("sources_json", [])
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except: 
            return []
    return raw if isinstance(raw, list) else []

# Check imports
if __name__ == "__main__":
    pass
