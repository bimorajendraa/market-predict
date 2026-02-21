"""
Sector-Aware Scoring Framework.
Detects stock sector and applies sector-specific metric weights and risk flags.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================
# Sector Detection
# ============================================

# Manual sector overrides for known tickers  
TICKER_SECTORS: dict[str, str] = {
    # Indonesian Banks
    "BBCA": "banking", "BBRI": "banking", "BMRI": "banking",
    "BBNI": "banking", "BBTN": "banking", "BRIS": "banking",
    "BDMN": "banking", "BNGA": "banking", "PNBN": "banking", "MEGA": "banking",
    # Indonesian Telco
    "TLKM": "telecom", "EXCL": "telecom", "ISAT": "telecom",
    # Indonesian Consumer
    "UNVR": "consumer", "ICBP": "consumer", "INDF": "consumer",
    "KLBF": "consumer",
    # Indonesian Mining/Commodities
    "ANTM": "commodities", "PTBA": "commodities", "INCO": "commodities",
    "ADRO": "commodities", "ITMG": "commodities",
    # Indonesian Real Estate/Property
    "BSDE": "real_estate", "CTRA": "real_estate", "SMRA": "real_estate",
    # Indonesian Tower/Infrastructure
    "TOWR": "tower_infra", "TBIG": "tower_infra",
    "JSMR": "infrastructure", "WIKA": "infrastructure", "WSKT": "infrastructure",
    # US Tech
    "AAPL": "tech", "MSFT": "tech", "GOOGL": "tech", "AMZN": "tech",
    "META": "tech", "NVDA": "tech", "ORCL": "tech", "CRM": "tech",
    "INTC": "tech", "AMD": "tech", "TSLA": "tech",
    "ADBE": "tech", "NOW": "tech", "PYPL": "tech",
    # Streaming
    "NFLX": "streaming", "DIS": "streaming", "SPOT": "streaming",
    # US Tower
    "AMT": "tower_infra", "CCI": "tower_infra", "SBAC": "tower_infra",
    # US Banking
    "JPM": "banking", "BAC": "banking", "WFC": "banking", "GS": "banking",
    "MS": "banking", "C": "banking",
}

# yfinance sector -> our sector key  
YFINANCE_SECTOR_MAP: dict[str, str] = {
    "technology": "tech",
    "financial services": "banking",
    "consumer cyclical": "consumer",
    "consumer defensive": "consumer",
    "communication services": "telecom",
    "basic materials": "commodities",
    "energy": "commodities",
    "real estate": "real_estate",
    "industrials": "industrials",
    "healthcare": "healthcare",
    "utilities": "utilities",
}


def detect_sector(ticker: str) -> str:
    """
    Detect the sector for a given ticker.
    
    Priority:
    1. Manual override table
    2. yfinance info.sector
    3. Default "general"
    
    Returns:
        Sector key string (e.g., 'tech', 'banking', 'consumer')
    """
    base_ticker = ticker.split(".")[0].upper()
    
    # Check manual overrides
    if base_ticker in TICKER_SECTORS:
        return TICKER_SECTORS[base_ticker]
    
    # Try yfinance
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        yf_sector = info.get("sector", "").lower()
        if yf_sector in YFINANCE_SECTOR_MAP:
            sector = YFINANCE_SECTOR_MAP[yf_sector]
            logger.info(f"Detected sector for {ticker}: {sector} (from yfinance: {yf_sector})")
            return sector
    except Exception as e:
        logger.debug(f"yfinance sector detection failed for {ticker}: {e}")
    
    return "general"


# ============================================
# Sector-Specific Metric Weights
# ============================================

SECTOR_WEIGHTS: dict[str, dict[str, float]] = {
    "tech": {
        "revenue_growth": 2.0,
        "operating_margin": 1.8,
        "roe": 1.5,
        "roa": 1.0,
        "current_ratio": 0.8,
        "debt_to_equity": 1.2,
        "pe_ratio": 1.5,
        "eps_growth": 2.0,
        "free_cash_flow_yield": 1.5,
    },
    "banking": {
        "roe": 2.0,
        "roa": 1.5,
        "net_interest_margin": 2.5,
        "non_performing_loan": 2.0,
        "capital_adequacy_ratio": 2.0,
        "loan_to_deposit_ratio": 1.5,
        "cost_to_income": 1.5,
        "debt_to_equity": 0.5,  # Less relevant for banks
        "revenue_growth": 1.0,
    },
    "consumer": {
        "revenue_growth": 1.5,
        "operating_margin": 2.0,
        "roe": 1.5,
        "current_ratio": 1.5,
        "debt_to_equity": 1.5,
        "dividend_yield": 1.5,
        "pe_ratio": 1.2,
    },
    "telecom": {
        "revenue_growth": 1.0,
        "operating_margin": 1.5,
        "roe": 1.5,
        "debt_to_equity": 2.0,
        "dividend_yield": 2.0,
        "free_cash_flow_yield": 1.5,
        "ebitda_margin": 2.0,
    },
    "commodities": {
        "revenue_growth": 1.0,
        "operating_margin": 1.5,
        "roe": 1.0,
        "debt_to_equity": 2.0,
        "current_ratio": 1.5,
        "dividend_yield": 1.5,
        "free_cash_flow_yield": 2.0,
    },
    "real_estate": {
        "revenue_growth": 1.0,
        "roe": 1.0,
        "debt_to_equity": 2.5,
        "current_ratio": 2.0,
        "dividend_yield": 2.0,
        "nav_discount": 2.0,
    },
    "tower_infra": {
        "revenue_growth": 1.5,
        "operating_margin": 2.0,
        "roe": 1.0,
        "debt_to_equity": 2.0,
        "interest_coverage": 2.5,
        "free_cash_flow_yield": 2.0,
        "ebitda_margin": 2.0,
    },
    "streaming": {
        "revenue_growth": 2.5,
        "operating_margin": 1.5,
        "roe": 1.0,
        "subscriber_growth": 2.5,
        "free_cash_flow_yield": 1.5,
        "debt_to_equity": 1.0,
        "pe_ratio": 1.5,
    },
    "infrastructure": {
        "revenue_growth": 1.0,
        "operating_margin": 1.5,
        "roe": 1.5,
        "debt_to_equity": 2.5,
        "current_ratio": 1.5,
        "interest_coverage": 2.0,
        "free_cash_flow_yield": 1.5,
    },
    "general": {
        # Default balanced weights
        "revenue_growth": 1.5,
        "operating_margin": 1.5,
        "roe": 1.5,
        "roa": 1.0,
        "current_ratio": 1.0,
        "debt_to_equity": 1.5,
        "pe_ratio": 1.0,
        "dividend_yield": 1.0,
    },
}


def get_sector_weights(ticker: str) -> dict[str, float]:
    """Get sector-specific metric weights for a ticker."""
    sector = detect_sector(ticker)
    weights = SECTOR_WEIGHTS.get(sector, SECTOR_WEIGHTS["general"])
    logger.info(f"Using {sector} sector weights for {ticker}: {len(weights)} metrics")
    return weights


# ============================================
# Risk Flags
# ============================================

RISK_FLAG_DEFINITIONS = {
    # ── Universal flags ──
    "high_leverage": {
        "metric": "debt_to_equity",
        "threshold": 2.0,
        "direction": "above",
        "severity": "warning",
        "message": "High leverage: D/E ratio above 2.0x",
        "sectors": None,  # applies to all
    },
    "negative_fcf": {
        "metric": "free_cash_flow",
        "threshold": 0,
        "direction": "below",
        "severity": "warning",
        "message": "Negative free cash flow",
        "sectors": None,
    },
    "declining_revenue": {
        "metric": "revenue_growth",
        "threshold": -0.05,
        "direction": "below",
        "severity": "caution",
        "message": "Revenue declining (>5% YoY decrease)",
        "sectors": None,
    },
    "low_profitability": {
        "metric": "operating_margin",
        "threshold": 0.05,
        "direction": "below",
        "severity": "caution",
        "message": "Low operating margin (<5%)",
        "sectors": None,
    },
    "overvalued": {
        "metric": "pe_ratio",
        "threshold": 50,
        "direction": "above",
        "severity": "info",
        "message": "Potentially overvalued: P/E above 50x",
        "sectors": None,
    },

    # ── Banking-specific ──
    "low_nim": {
        "metric": "net_interest_margin",
        "threshold": 0.02,
        "direction": "below",
        "severity": "warning",
        "message": "Low NIM (<2%): core lending profitability weak",
        "sectors": ["banking"],
    },
    "high_npl": {
        "metric": "non_performing_loan",
        "threshold": 0.05,
        "direction": "above",
        "severity": "warning",
        "message": "High NPL ratio (>5%): credit quality deteriorating",
        "sectors": ["banking"],
    },
    "low_car": {
        "metric": "capital_adequacy_ratio",
        "threshold": 0.12,
        "direction": "below",
        "severity": "caution",
        "message": "Capital adequacy below 12%: regulatory buffer thin",
        "sectors": ["banking"],
    },

    # ── Tech-specific ──
    "high_sbc": {
        "metric": "sbc_to_revenue",
        "threshold": 0.10,
        "direction": "above",
        "severity": "caution",
        "message": "SBC > 10% of revenue: shareholder dilution risk",
        "sectors": ["tech"],
    },
    "low_rd_spend": {
        "metric": "rd_to_revenue",
        "threshold": 0.10,
        "direction": "below",
        "severity": "caution",
        "message": "R&D < 10% of revenue: competitive moat may weaken",
        "sectors": ["tech"],
    },

    # ── Consumer-specific ──
    "inventory_bloat": {
        "metric": "inventory_days",
        "threshold": 120,
        "direction": "above",
        "severity": "caution",
        "message": "Inventory days > 120: potential markdown / write-off risk",
        "sectors": ["consumer"],
    },

    # ── Commodities-specific ──
    "capex_heavy": {
        "metric": "capex_to_revenue",
        "threshold": 0.30,
        "direction": "above",
        "severity": "caution",
        "message": "Capex > 30% of revenue: heavy reinvestment cycle",
        "sectors": ["commodities"],
    },
    "thin_coverage": {
        "metric": "interest_coverage",
        "threshold": 3.0,
        "direction": "below",
        "severity": "warning",
        "message": "Interest coverage below 3x: debt service at risk",
        "sectors": ["commodities", "real_estate"],
    },

    # ── Tower/Infra-specific ──
    "tower_high_leverage": {
        "metric": "net_debt_to_ebitda",
        "threshold": 5.0,
        "direction": "above",
        "severity": "warning",
        "message": "Net debt/EBITDA > 5x: high leverage for tower company",
        "sectors": ["tower_infra"],
    },
    "tower_low_interest_cover": {
        "metric": "interest_coverage",
        "threshold": 2.0,
        "direction": "below",
        "severity": "warning",
        "message": "Interest coverage < 2x: debt servicing at risk",
        "sectors": ["tower_infra", "infrastructure"],
    },

    # ── Streaming-specific ──
    "streaming_content_bloat": {
        "metric": "content_spend_ratio",
        "threshold": 0.60,
        "direction": "above",
        "severity": "caution",
        "message": "Content spend > 60% of revenue: margin compression risk",
        "sectors": ["streaming"],
    },
}


def compute_risk_flags(
    drivers: list[dict], sector: str = "general",
) -> list[dict]:
    """
    Compute risk flags based on financial metrics.
    
    Args:
        drivers: List of driver dicts from financial scoring.
        sector: Current sector for filtering sector-specific flags.
        
    Returns:
        List of triggered risk flag dicts.
    """
    driver_map = {d.get("metric", ""): d.get("value") for d in drivers 
                  if d.get("status") == "computed" and d.get("value") is not None}
    
    flags = []
    for flag_id, definition in RISK_FLAG_DEFINITIONS.items():
        # Sector filter: None = universal, else must match
        allowed_sectors = definition.get("sectors")
        if allowed_sectors is not None and sector not in allowed_sectors:
            continue

        metric = definition["metric"]
        if metric not in driver_map:
            continue
            
        value = float(driver_map[metric])
        threshold = definition["threshold"]
        
        triggered = False
        if definition["direction"] == "above" and value > threshold:
            triggered = True
        elif definition["direction"] == "below" and value < threshold:
            triggered = True
        
        if triggered:
            flags.append({
                "flag_id": flag_id,
                "severity": definition["severity"],
                "message": definition["message"],
                "metric": metric,
                "value": value,
                "threshold": threshold,
                "sector_specific": allowed_sectors is not None,
            })
    
    return flags


def compute_sector_score(
    ticker: str,
    base_score: float,
    drivers: list[dict],
) -> dict:
    """
    Compute sector-aware composite score.
    
    Returns:
        Dict with sector, base_score, sector_adjusted_score, risk_flags.
    """
    sector = detect_sector(ticker)
    risk_flags = compute_risk_flags(drivers, sector=sector)
    
    # Risk penalty
    risk_penalty = 0.0
    for flag in risk_flags:
        if flag["severity"] == "warning":
            risk_penalty += 5.0
        elif flag["severity"] == "caution":
            risk_penalty += 2.5
    
    adjusted_score = max(0, min(100, base_score - risk_penalty))
    
    return {
        "sector": sector,
        "base_score": round(base_score, 1),
        "risk_penalty": round(risk_penalty, 1),
        "sector_adjusted_score": round(adjusted_score, 1),
        "risk_flags": risk_flags,
        "risk_flag_count": len(risk_flags),
    }
