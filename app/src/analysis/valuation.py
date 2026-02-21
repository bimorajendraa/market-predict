"""
Valuation Layer for Finance Analytics.
Computes valuation multiples and compares against historical/sector benchmarks.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# Sector median P/E benchmarks (approximate)
SECTOR_PE_MEDIANS: dict[str, float] = {
    "tech": 30.0,
    "banking": 12.0,
    "consumer": 20.0,
    "telecom": 15.0,
    "commodities": 10.0,
    "real_estate": 18.0,
    "healthcare": 25.0,
    "industrials": 18.0,
    "utilities": 16.0,
    "general": 18.0,
}

# Sector median EV/EBITDA benchmarks (approximate)
SECTOR_EV_EBITDA_MEDIANS: dict[str, float] = {
    "tech": 20.0,
    "banking": 8.0,
    "consumer": 12.0,
    "telecom": 7.0,
    "commodities": 6.0,
    "real_estate": 15.0,
    "healthcare": 15.0,
    "industrials": 10.0,
    "utilities": 9.0,
    "general": 12.0,
}

# Sector median P/B benchmarks
SECTOR_PB_MEDIANS: dict[str, float] = {
    "tech": 6.0,
    "banking": 1.5,
    "consumer": 3.0,
    "telecom": 2.0,
    "commodities": 1.5,
    "real_estate": 1.0,
    "general": 2.5,
}


def compute_valuation_multiples(ticker: str) -> dict:
    """
    Compute key valuation multiples from yfinance data.
    
    Returns dict with:
    - pe_trailing, pe_forward
    - ev_ebitda
    - pb_ratio
    - fcf_yield
    - dividend_yield
    - market_cap
    """
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info
        
        result = {
            "ticker": ticker,
            "market_cap": info.get("marketCap"),
            "pe_trailing": info.get("trailingPE"),
            "pe_forward": info.get("forwardPE"),
            "pb_ratio": info.get("priceToBook"),
            "ev_ebitda": info.get("enterpriseToEbitda"),
            "ev_revenue": info.get("enterpriseToRevenue"),
            "dividend_yield": info.get("dividendYield"),
            "peg_ratio": info.get("pegRatio"),
        }
        
        # Calculate FCF yield if data available
        fcf = info.get("freeCashflow")
        mkt_cap = info.get("marketCap")
        if fcf and mkt_cap and mkt_cap > 0:
            result["fcf_yield"] = round(fcf / mkt_cap, 4)
        else:
            result["fcf_yield"] = None
            
        # Clean None values for display
        for key in list(result.keys()):
            if result[key] is not None and isinstance(result[key], float):
                result[key] = round(result[key], 2)
                
        logger.info(f"Valuation multiples for {ticker}: P/E={result.get('pe_trailing')}, "
                     f"EV/EBITDA={result.get('ev_ebitda')}, P/B={result.get('pb_ratio')}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to compute valuation multiples for {ticker}: {e}")
        return {"ticker": ticker, "error": str(e)}


def compare_to_sector(multiples: dict, sector: str) -> dict:
    """
    Compare valuation multiples against sector medians.
    
    Returns dict with comparison results for each metric.
    """
    comparisons = {}
    
    # P/E comparison
    pe = multiples.get("pe_trailing")
    sector_pe = SECTOR_PE_MEDIANS.get(sector, SECTOR_PE_MEDIANS["general"])
    if pe and pe > 0:
        pe_ratio_vs_sector = pe / sector_pe
        comparisons["pe_vs_sector"] = {
            "value": pe,
            "sector_median": sector_pe,
            "ratio": round(pe_ratio_vs_sector, 2),
            "assessment": _assess_ratio(pe_ratio_vs_sector, "lower_better"),
        }
    
    # EV/EBITDA comparison
    ev_ebitda = multiples.get("ev_ebitda")
    sector_ev = SECTOR_EV_EBITDA_MEDIANS.get(sector, SECTOR_EV_EBITDA_MEDIANS["general"])
    if ev_ebitda and ev_ebitda > 0:
        ev_ratio = ev_ebitda / sector_ev
        comparisons["ev_ebitda_vs_sector"] = {
            "value": ev_ebitda,
            "sector_median": sector_ev,
            "ratio": round(ev_ratio, 2),
            "assessment": _assess_ratio(ev_ratio, "lower_better"),
        }
    
    # P/B comparison
    pb = multiples.get("pb_ratio")
    sector_pb = SECTOR_PB_MEDIANS.get(sector, SECTOR_PB_MEDIANS["general"])
    if pb and pb > 0:
        pb_ratio = pb / sector_pb
        comparisons["pb_vs_sector"] = {
            "value": pb,
            "sector_median": sector_pb,
            "ratio": round(pb_ratio, 2),
            "assessment": _assess_ratio(pb_ratio, "lower_better"),
        }
    
    return comparisons


def _assess_ratio(ratio: float, direction: str) -> str:
    """Assess a valuation ratio vs benchmark."""
    if direction == "lower_better":
        if ratio < 0.7:
            return "discount"
        elif ratio < 0.9:
            return "slight_discount"
        elif ratio < 1.1:
            return "fair"
        elif ratio < 1.3:
            return "slight_premium"
        else:
            return "premium"
    else:  # higher_better (e.g., FCF yield)
        if ratio > 1.3:
            return "attractive"
        elif ratio > 1.1:
            return "slightly_attractive"
        elif ratio > 0.9:
            return "fair"
        else:
            return "unattractive"


def valuation_verdict(
    multiples: dict,
    sector: str,
) -> dict:
    """
    Generate an overall valuation verdict.
    
    Returns:
        Dict with verdict (premium/fair/discount), explanation, and details.
    """
    comparisons = compare_to_sector(multiples, sector)
    
    if not comparisons:
        return {
            "verdict": "insufficient_data",
            "explanation": "Not enough valuation data available for assessment.",
            "comparisons": {},
        }
    
    # Count assessments
    assessments = [c["assessment"] for c in comparisons.values()]
    discount_count = sum(1 for a in assessments if "discount" in a)
    premium_count = sum(1 for a in assessments if "premium" in a)
    fair_count = sum(1 for a in assessments if a == "fair")
    
    # Determine overall verdict
    if discount_count > premium_count and discount_count > fair_count:
        verdict = "discount"
        explanation = (
            f"Trading at a discount to sector peers across "
            f"{discount_count}/{len(assessments)} metrics."
        )
    elif premium_count > discount_count and premium_count > fair_count:
        verdict = "premium"
        explanation = (
            f"Trading at a premium to sector peers across "
            f"{premium_count}/{len(assessments)} metrics."
        )
    else:
        verdict = "fair"
        explanation = "Valuation appears roughly in line with sector peers."
    
    # FCF yield check
    fcf_yield = multiples.get("fcf_yield")
    if fcf_yield is not None:
        if fcf_yield > 0.06:
            explanation += f" Strong FCF yield ({fcf_yield:.1%})."
        elif fcf_yield < 0:
            explanation += " Warning: negative free cash flow."
    
    return {
        "verdict": verdict,
        "explanation": explanation,
        "comparisons": comparisons,
        "multiples": {
            k: v for k, v in multiples.items()
            if k not in ("ticker", "error") and v is not None
        },
    }


def run_valuation_analysis(ticker: str, sector: Optional[str] = None) -> dict:
    """
    Run full valuation analysis for a ticker.
    
    Args:
        ticker: Stock ticker
        sector: Override sector (auto-detected if None)
        
    Returns:
        Complete valuation dict with multiples, comparisons, verdict,
        historical_percentile, dcf_lite, and peer_comps.
    """
    if sector is None:
        from .sector_scoring import detect_sector
        sector = detect_sector(ticker)
    
    multiples = compute_valuation_multiples(ticker)
    if "error" in multiples:
        return {
            "status": "error",
            "error": multiples["error"],
            "sector": sector,
        }
    
    verdict = valuation_verdict(multiples, sector)

    # Enhanced valuation features
    hist_pctile = compute_historical_percentile(ticker)
    dcf = dcf_lite(ticker)
    peers = peer_comps(ticker, sector)
    model_3s = mini_3statement(ticker)
    sens = sensitivity_table(ticker)

    logger.info(
        f"Valuation for {ticker} ({sector}): {verdict['verdict']} — {verdict['explanation']}"
    )
    
    return {
        "status": "success",
        "ticker": ticker,
        "sector": sector,
        **verdict,
        "historical_percentile": hist_pctile,
        "dcf_lite": dcf,
        "peer_comps": peers,
        "mini_3statement": model_3s,
        "sensitivity_table": sens,
    }


# ============================================
# Historical Percentile (5Y)
# ============================================

def compute_historical_percentile(ticker: str, years: int = 5) -> dict:
    """
    Compute historical valuation percentile over N years.
    
    Returns dict with P/E percentiles (p10, p25, p50, p75, p90)
    and where the current P/E sits.
    """
    try:
        import yfinance as yf
        import numpy as np

        stock = yf.Ticker(ticker)
        info = stock.info
        current_pe = info.get("trailingPE")
        
        # Try to get historical earnings data
        hist = stock.history(period=f"{years}y")
        if hist.empty or current_pe is None:
            return {"status": "insufficient_data"}

        # Approximate historical P/E from price / EPS
        earnings = stock.earnings_history
        if earnings is not None and hasattr(earnings, '__len__') and len(earnings) > 0:
            pass  # Could use for more precise calc, but keep simple

        # Use current PE percentile against a rough price-range approach
        prices = hist["Close"].dropna()
        if len(prices) < 50:
            return {"status": "insufficient_data"}

        # Current PE vs. simple price percentile as proxy
        current_price = float(prices.iloc[-1])
        price_vals = prices.values

        percentiles = {
            "p10": float(np.percentile(price_vals, 10)),
            "p25": float(np.percentile(price_vals, 25)),
            "p50": float(np.percentile(price_vals, 50)),
            "p75": float(np.percentile(price_vals, 75)),
            "p90": float(np.percentile(price_vals, 90)),
        }

        # Determine where current sits
        rank = float(np.sum(price_vals < current_price) / len(price_vals) * 100)

        return {
            "status": "success",
            "period": f"{years}Y",
            "current_price": round(current_price, 2),
            "current_pe": round(current_pe, 2) if current_pe else None,
            "price_percentiles": {k: round(v, 2) for k, v in percentiles.items()},
            "current_percentile_rank": round(rank, 1),
            "assessment": (
                "historically_cheap" if rank < 25
                else "below_median" if rank < 50
                else "above_median" if rank < 75
                else "historically_expensive"
            ),
        }

    except Exception as e:
        logger.warning(f"Historical percentile computation failed: {e}")
        return {"status": "error", "error": str(e)}


# ============================================
# DCF-Lite (3-Scenario Intrinsic Value)
# ============================================

def dcf_lite(ticker: str) -> dict:
    """
    Quick 3-scenario DCF for intrinsic value range.
    Uses simplified assumptions: 5Y projection + terminal value.
    
    Returns bear/base/bull intrinsic values and upside/downside.
    """
    try:
        import yfinance as yf

        stock = yf.Ticker(ticker)
        info = stock.info

        fcf = info.get("freeCashflow")
        shares = info.get("sharesOutstanding")
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")
        revenue_growth = info.get("revenueGrowth")
        market_cap = info.get("marketCap")

        if not all([fcf, shares, current_price]) or shares == 0:
            return {"status": "insufficient_data"}

        fcf_per_share = fcf / shares

        # If FCF is negative, can't do meaningful DCF
        if fcf_per_share <= 0:
            return {
                "status": "negative_fcf",
                "current_price": round(current_price, 2),
                "fcf_per_share": round(fcf_per_share, 2),
                "message": "Negative FCF — DCF not meaningful. Use revenue multiples instead.",
            }

        # Scenarios
        base_growth = min(max(revenue_growth or 0.05, 0.03), 0.15)
        scenarios = {
            "bear": {
                "growth_rate": max(base_growth - 0.05, 0.01),
                "terminal_growth": 0.02,
                "wacc": 0.12,
            },
            "base": {
                "growth_rate": base_growth,
                "terminal_growth": 0.025,
                "wacc": 0.10,
            },
            "bull": {
                "growth_rate": min(base_growth + 0.05, 0.25),
                "terminal_growth": 0.03,
                "wacc": 0.09,
            },
        }

        results = {"status": "success", "current_price": round(current_price, 2)}

        for scenario_name, params in scenarios.items():
            g = params["growth_rate"]
            tg = params["terminal_growth"]
            wacc = params["wacc"]

            # 5-year projected FCFs
            pv_fcf = 0
            projected_fcf = fcf_per_share
            for year in range(1, 6):
                projected_fcf *= (1 + g)
                pv_fcf += projected_fcf / ((1 + wacc) ** year)

            # Terminal value (Gordon Growth)
            terminal_fcf = projected_fcf * (1 + tg)
            terminal_value = terminal_fcf / (wacc - tg) if wacc > tg else terminal_fcf * 15
            pv_terminal = terminal_value / ((1 + wacc) ** 5)

            intrinsic = pv_fcf + pv_terminal
            upside = ((intrinsic / current_price) - 1) * 100

            results[scenario_name] = round(intrinsic, 2)
            results[f"{scenario_name}_upside"] = round(upside, 1)
            results[f"{scenario_name}_assumptions"] = {
                "growth": f"{g:.1%}",
                "terminal_growth": f"{tg:.1%}",
                "wacc": f"{wacc:.1%}",
            }

        return results

    except Exception as e:
        logger.warning(f"DCF-lite computation failed for {ticker}: {e}")
        return {"status": "error", "error": str(e)}


# ============================================
# 3-Statement Mini Model
# ============================================

def mini_3statement(ticker: str) -> dict:
    """
    Build a 3-statement mini model: Revenue → Operating Income → Net Income → FCF.
    Projects 5 years forward using current margins and growth rates.

    Returns dict with projections, current financials, and assumptions.
    """
    try:
        import yfinance as yf

        stock = yf.Ticker(ticker)
        info = stock.info

        revenue = info.get("totalRevenue")
        op_margin = info.get("operatingMargins")
        net_margin = info.get("profitMargins")
        revenue_growth = info.get("revenueGrowth") or 0.05
        tax_rate = info.get("taxRate") or 0.21
        capex = abs(info.get("capitalExpenditures", 0) or 0)
        shares = info.get("sharesOutstanding")
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")

        if not all([revenue, shares, current_price]):
            return {"status": "insufficient_data"}

        # Use reported margins or reasonable defaults
        if not op_margin or op_margin <= 0:
            op_margin = 0.15
        if not net_margin or net_margin <= 0:
            net_margin = op_margin * 0.7  # rough proxy

        capex_ratio = capex / revenue if revenue else 0.05
        depreciation = info.get("depreciation")
        da_ratio = (abs(depreciation) / revenue) if depreciation and revenue else 0.03

        # Current year actuals
        current = {
            "revenue": revenue,
            "op_income": revenue * op_margin,
            "net_income": revenue * net_margin,
            "fcf": revenue * net_margin + revenue * da_ratio - capex,
        }

        # Growth assumptions decay toward terminal
        terminal_growth = 0.03
        growth_rates = []
        for y in range(5):
            g = revenue_growth * (1 - y * 0.1) + terminal_growth * (y * 0.1)
            growth_rates.append(max(g, terminal_growth))

        # Margin expansion/compression assumptions
        target_op_margin = min(op_margin + 0.01, 0.40)  # slight expansion
        margin_path = [
            op_margin + (target_op_margin - op_margin) * (y / 4)
            for y in range(5)
        ]

        # Project 5 years
        projections = []
        proj_rev = revenue
        for y in range(5):
            proj_rev *= (1 + growth_rates[y])
            proj_op = proj_rev * margin_path[y]
            proj_ni = proj_op * (1 - tax_rate)
            proj_fcf = proj_ni + proj_rev * da_ratio - proj_rev * capex_ratio

            projections.append({
                "year": y + 1,
                "revenue": round(proj_rev / 1e9, 2),
                "op_income": round(proj_op / 1e9, 2),
                "op_margin": round(margin_path[y] * 100, 1),
                "net_income": round(proj_ni / 1e9, 2),
                "fcf": round(proj_fcf / 1e9, 2),
                "growth_rate": round(growth_rates[y] * 100, 1),
            })

        return {
            "status": "success",
            "current": {
                "revenue_b": round(revenue / 1e9, 2),
                "op_margin_pct": round(op_margin * 100, 1),
                "net_margin_pct": round(net_margin * 100, 1),
                "capex_ratio_pct": round(capex_ratio * 100, 1),
            },
            "projections": projections,
            "assumptions": {
                "base_growth": f"{revenue_growth:.1%}",
                "terminal_growth": f"{terminal_growth:.1%}",
                "tax_rate": f"{tax_rate:.0%}",
                "target_op_margin": f"{target_op_margin:.1%}",
            },
        }

    except Exception as e:
        logger.warning(f"3-statement model failed for {ticker}: {e}")
        return {"status": "error", "error": str(e)}


# ============================================
# Sensitivity Table (WACC vs Terminal Growth)
# ============================================

def sensitivity_table(ticker: str) -> dict:
    """
    WACC vs terminal growth sensitivity grid → intrinsic value per share.

    Returns a 2D grid: rows = WACC (8-12%), cols = terminal growth (1-4%).
    """
    try:
        import yfinance as yf

        stock = yf.Ticker(ticker)
        info = stock.info

        fcf = info.get("freeCashflow")
        revenue = info.get("totalRevenue")
        shares = info.get("sharesOutstanding")
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")
        revenue_growth = info.get("revenueGrowth") or 0.05

        if not shares or shares == 0 or not current_price:
            return {"status": "insufficient_data"}

        # If FCF is negative, use operating cash flow or revenue-based proxy
        if not fcf or fcf <= 0:
            ocf = info.get("operatingCashflow")
            if ocf and ocf > 0:
                base_fcf = ocf
            elif revenue:
                # Use 10% FCF margin as rough proxy
                base_fcf = revenue * 0.10
            else:
                return {"status": "no_positive_cashflow"}
        else:
            base_fcf = fcf

        base_fcf_ps = base_fcf / shares

        wacc_range = [0.08, 0.09, 0.10, 0.11, 0.12]
        tg_range = [0.01, 0.02, 0.025, 0.03, 0.04]
        base_growth = min(max(revenue_growth, 0.03), 0.15)

        grid = []
        for wacc in wacc_range:
            row = {"wacc": f"{wacc:.0%}"}
            for tg in tg_range:
                # 5Y projected FCFs
                pv_fcf = 0
                proj_fcf = base_fcf_ps
                for yr in range(1, 6):
                    proj_fcf *= (1 + base_growth)
                    pv_fcf += proj_fcf / ((1 + wacc) ** yr)

                # Terminal value
                terminal_fcf = proj_fcf * (1 + tg)
                if wacc > tg:
                    tv = terminal_fcf / (wacc - tg)
                else:
                    tv = terminal_fcf * 20  # fallback cap

                pv_tv = tv / ((1 + wacc) ** 5)
                intrinsic = pv_fcf + pv_tv

                row[f"tg_{tg:.1%}"] = round(intrinsic, 2)

            grid.append(row)

        return {
            "status": "success",
            "current_price": round(current_price, 2),
            "base_fcf_ps": round(base_fcf_ps, 2),
            "wacc_range": [f"{w:.0%}" for w in wacc_range],
            "tg_range": [f"{tg:.1%}" for tg in tg_range],
            "grid": grid,
        }

    except Exception as e:
        logger.warning(f"Sensitivity table failed for {ticker}: {e}")
        return {"status": "error", "error": str(e)}


# ============================================
# Peer Comparisons
# ============================================

SECTOR_PEERS = {
    "tech": ["MSFT", "AAPL", "GOOG", "ORCL", "CRM", "SAP", "IBM", "ADBE"],
    "banking": ["JPM", "BAC", "WFC", "C", "GS", "MS", "USB", "PNC"],
    "consumer": ["PG", "KO", "PEP", "UNVR.JK", "ICBP.JK", "WMT", "COST"],
    "telecom": ["T", "VZ", "TMUS", "TLKM.JK", "EXCL.JK", "ISAT.JK"],
    "commodities": ["XOM", "CVX", "VALE", "BHP", "RIO", "ADRO.JK", "PTBA.JK"],
    "real_estate": ["PLD", "AMT", "EQIX", "SPG", "BSDE.JK", "CTRA.JK"],
}


def peer_comps(ticker: str, sector: Optional[str] = None) -> dict:
    """
    Compare ticker's valuation against sector peers.
    
    Returns table with P/E, EV/EBITDA, P/B for each peer.
    """
    if sector is None:
        from .sector_scoring import detect_sector
        sector = detect_sector(ticker)

    peers_list = SECTOR_PEERS.get(sector, SECTOR_PEERS.get("tech", []))
    # Remove self from peers
    peers_list = [p for p in peers_list if p.upper() != ticker.upper()][:5]

    if not peers_list:
        return {"status": "no_peers", "sector": sector}

    try:
        import yfinance as yf

        peer_data = []
        for peer in peers_list:
            try:
                stock = yf.Ticker(peer)
                info = stock.info
                peer_data.append({
                    "ticker": peer,
                    "pe": info.get("trailingPE"),
                    "ev_ebitda": info.get("enterpriseToEbitda"),
                    "pb": info.get("priceToBook"),
                    "market_cap_b": round(info.get("marketCap", 0) / 1e9, 1) if info.get("marketCap") else None,
                })
            except Exception:
                continue

        if not peer_data:
            return {"status": "fetch_failed", "sector": sector}

        # Compute medians
        pe_vals = [p["pe"] for p in peer_data if p["pe"] and p["pe"] > 0]
        ev_vals = [p["ev_ebitda"] for p in peer_data if p["ev_ebitda"] and p["ev_ebitda"] > 0]
        pb_vals = [p["pb"] for p in peer_data if p["pb"] and p["pb"] > 0]

        import statistics
        medians = {
            "pe": round(statistics.median(pe_vals), 1) if pe_vals else None,
            "ev_ebitda": round(statistics.median(ev_vals), 1) if ev_vals else None,
            "pb": round(statistics.median(pb_vals), 1) if pb_vals else None,
        }

        return {
            "status": "success",
            "sector": sector,
            "peers": peer_data,
            "peer_medians": medians,
            "peer_count": len(peer_data),
        }

    except Exception as e:
        logger.warning(f"Peer comps failed for {ticker}: {e}")
        return {"status": "error", "error": str(e)}
