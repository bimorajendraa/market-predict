"""
Sector-Specific Key Questions for Decision Memos.
5 questions per sector — if any answer changes, the rating should change.
"""

# ============================================
# Key Questions by Sector
# ============================================

SECTOR_KEY_QUESTIONS: dict[str, list[dict]] = {
    "tech": [
        {
            "question": "Is the cloud/SaaS transition accelerating or decelerating?",
            "metric": "revenue_growth",
            "trigger": "If recurring revenue mix < 50% or cloud growth < 15%, downgrade.",
        },
        {
            "question": "Is stock-based compensation (SBC) diluting shareholder value?",
            "metric": "dilution",
            "trigger": "If SBC > 10% of revenue or dilution > 3% per year, flag risk.",
        },
        {
            "question": "Is R&D spend generating returns or being cut?",
            "metric": "rd_ratio",
            "trigger": "If R&D < 10% of revenue (for software), competitive moat weakens.",
        },
        {
            "question": "Is free cash flow conversion healthy despite capex investments?",
            "metric": "fcf_margin",
            "trigger": "If FCF margin < 10% for 2+ quarters while capex rises, flag cash burn.",
        },
        {
            "question": "Is the total addressable market (TAM) expanding or saturating?",
            "metric": "revenue_qoq",
            "trigger": "If revenue growth decelerating 3+ consecutive quarters, potential TAM issue.",
        },
    ],
    "banking": [
        {
            "question": "Is Net Interest Margin (NIM) expanding or compressing?",
            "metric": "net_interest_margin",
            "trigger": "If NIM declines > 20bp QoQ for 2 consecutive quarters, downgrade.",
        },
        {
            "question": "Are non-performing loans (NPLs) trending up?",
            "metric": "non_performing_loan",
            "trigger": "If NPL ratio > 3% or rising > 50bp QoQ, flag credit risk.",
        },
        {
            "question": "Is capital adequacy sufficient for growth + stress scenarios?",
            "metric": "capital_adequacy_ratio",
            "trigger": "If CAR < 12% or declining toward regulatory minimum, critical risk.",
        },
        {
            "question": "Is the cost-to-income ratio improving with digital adoption?",
            "metric": "cost_income_ratio",
            "trigger": "If CIR > 50% and not improving, efficiency concerns.",
        },
        {
            "question": "Is loan growth outpacing deposit growth (funding gap)?",
            "metric": "loan_deposit_ratio",
            "trigger": "If LDR > 95%, liquidity risk increases; < 70% suggests under-deployment.",
        },
    ],
    "consumer": [
        {
            "question": "Is organic revenue growth positive (ex-M&A, ex-FX)?",
            "metric": "revenue_growth",
            "trigger": "If organic growth < 0% for 2+ quarters, brand weakening.",
        },
        {
            "question": "Is inventory growth outpacing sales growth?",
            "metric": "inventory_sales_ratio",
            "trigger": "If inventory grows > 2x revenue growth, markdown/write-off risk.",
        },
        {
            "question": "Is gross margin stable or compressing?",
            "metric": "gross_margin",
            "trigger": "If gross margin compresses > 200bp YoY, pricing power weakening.",
        },
        {
            "question": "Is the company gaining or losing market share?",
            "metric": "revenue_growth",
            "trigger": "If revenue growth < sector median for 3+ quarters, share loss likely.",
        },
        {
            "question": "Is the brand strong enough to pass through input cost inflation?",
            "metric": "operating_margin",
            "trigger": "If op margin declining while revenue grows, lost pricing power.",
        },
    ],
    "commodities": [
        {
            "question": "Are realized prices above or below cycle average?",
            "metric": "revenue_growth",
            "trigger": "If realized prices < 5Y avg and debt/equity > 1.5x, cycle risk.",
        },
        {
            "question": "Is management adding capacity at cycle peak?",
            "metric": "capex_intensity",
            "trigger": "If capex/revenue > 25% near cycle high, return destruction risk.",
        },
        {
            "question": "Is the balance sheet prepared for a downturn?",
            "metric": "debt_to_equity",
            "trigger": "If D/E > 1.5x and commodity prices declining, solvency risk.",
        },
        {
            "question": "Is free cash flow being returned or re-invested poorly?",
            "metric": "fcf_yield",
            "trigger": "If FCF yield < 5% at cycle mid/peak, capital allocation concern.",
        },
        {
            "question": "Are reserves/resources being replaced sustainably?",
            "metric": "production_growth",
            "trigger": "If reserve replacement < 100% for 3+ years, depletion risk.",
        },
    ],
    "industrials": [
        {
            "question": "Is the order backlog growing or shrinking?",
            "metric": "revenue_growth",
            "trigger": "If backlog declines > 10% YoY, future revenue at risk.",
        },
        {
            "question": "Is working capital dragging on cash flow?",
            "metric": "cfo_to_net_income",
            "trigger": "If CFO/NI < 0.8x for 2+ quarters, working capital problem.",
        },
        {
            "question": "Are margins expanding with operating leverage?",
            "metric": "operating_margin",
            "trigger": "If revenue grows but margins flat/decline, structural cost issue.",
        },
        {
            "question": "Is capex cycle peaking (risk of over-investment)?",
            "metric": "capex_intensity",
            "trigger": "If capex/revenue > 15% for industrials, watch for return dilution.",
        },
        {
            "question": "Is the company exposed to geopolitical supply chain disruption?",
            "metric": "geographic_concentration",
            "trigger": "If > 30% revenue from single-risk geography, monitor closely.",
        },
    ],
    "tower_infra": [
        {
            "question": "Is tenancy ratio (tenants per tower) growing?",
            "metric": "tenancy_ratio",
            "trigger": "If tenancy ratio declines 2+ consecutive quarters, utilization risk.",
        },
        {
            "question": "Are average lease terms long enough for revenue visibility?",
            "metric": "avg_lease_term",
            "trigger": "If avg lease term < 8 years or shortening, revenue at risk.",
        },
        {
            "question": "Is net debt / EBITDA manageable given capex requirements?",
            "metric": "net_debt_to_ebitda",
            "trigger": "If net debt/EBITDA > 5x and rising, leverage risk.",
        },
        {
            "question": "Are escalator clauses keeping pace with inflation?",
            "metric": "escalator_rate",
            "trigger": "If escalator rate < CPI, real revenue erosion.",
        },
        {
            "question": "Is interest coverage sufficient for debt servicing?",
            "metric": "interest_coverage",
            "trigger": "If interest coverage < 2x, debt servicing risk.",
        },
    ],
    "streaming": [
        {
            "question": "Are subscriber net adds accelerating or decelerating?",
            "metric": "subscriber_growth",
            "trigger": "If net adds negative for 2+ quarters, market saturation.",
        },
        {
            "question": "Is ARPU trending up (price hikes sticking) or down (mix shift)?",
            "metric": "arpu",
            "trigger": "If ARPU declining > 3% YoY while subs stagnate, pricing power lost.",
        },
        {
            "question": "Is churn rate stable or increasing?",
            "metric": "churn_rate",
            "trigger": "If churn > 5% monthly or rising 2+ consecutive quarters, retention problem.",
        },
        {
            "question": "Is content spend growing faster than revenue?",
            "metric": "content_spend_ratio",
            "trigger": "If content spend / revenue > 60% and rising, margin compression.",
        },
        {
            "question": "Is ad-tier revenue contributing meaningfully?",
            "metric": "ad_revenue_share",
            "trigger": "If ad-tier < 5% of revenue after 4+ quarters, monetization failure.",
        },
    ],
    "infrastructure": [
        {
            "question": "Is asset utilization near capacity or underutilized?",
            "metric": "utilization_rate",
            "trigger": "If utilization < 60%, overcapacity; if > 95%, bottleneck risk.",
        },
        {
            "question": "Is the capex cycle peaking or troughing?",
            "metric": "capex_intensity",
            "trigger": "If capex/revenue > 30% for 3+ years, watch for return dilution.",
        },
        {
            "question": "Are regulatory tariffs/tolls being maintained or cut?",
            "metric": "tariff_stability",
            "trigger": "If regulator mandates tariff cut > 10%, revenue at risk.",
        },
        {
            "question": "Is the contract/concession renewal pipeline secure?",
            "metric": "contract_expiry",
            "trigger": "If > 20% of revenue from contracts expiring within 2 years, flag.",
        },
        {
            "question": "Is debt maturity profile manageable (no near-term wall)?",
            "metric": "debt_maturity_profile",
            "trigger": "If > 30% of debt matures within 12 months and refinancing difficult, critical.",
        },
    ],
    "general": [
        {
            "question": "Is revenue growth sustainable or one-time driven?",
            "metric": "revenue_growth",
            "trigger": "If revenue growth < 0% for 2+ consecutive quarters, downgrade.",
        },
        {
            "question": "Is the company generating positive free cash flow?",
            "metric": "fcf_margin",
            "trigger": "If FCF negative for 2+ quarters, cash burn concern.",
        },
        {
            "question": "Is leverage appropriate for the business cycle?",
            "metric": "debt_to_equity",
            "trigger": "If D/E > 2.0x and rising, financial risk increases.",
        },
        {
            "question": "Is management returning capital effectively?",
            "metric": "buyback_yield",
            "trigger": "If no dividends/buybacks despite positive FCF, governance concern.",
        },
        {
            "question": "Are there regulatory or litigation risks that could impair value?",
            "metric": "regulatory_risk",
            "trigger": "Monitor news for regulation changes, lawsuits, or compliance issues.",
        },
    ],
}


# ============================================
# Style Classification (Growth/Value/Quality/Turnaround)
# ============================================

STYLE_CRITERIA = {
    "growth": {
        "description": "High revenue growth, reinvesting heavily, lower current yield",
        "checks": {
            "revenue_growth": (">=", 0.15),
            "earnings_growth": (">=", 0.15),
        },
    },
    "value": {
        "description": "Below-median multiples, stable cash flows, dividend focus",
        "checks": {
            "pe_forward": ("<=", 15.0),
            "dividend_yield": (">=", 0.02),
        },
    },
    "quality": {
        "description": "High margins, strong ROIC, consistent execution",
        "checks": {
            "operating_margin": (">=", 0.15),
            "roe": (">=", 0.15),
        },
    },
    "turnaround": {
        "description": "Depressed margins/growth, but improving trajectory or catalyst",
        "checks": {
            "operating_margin": ("<=", 0.05),
            "revenue_growth": ("<=", 0.0),
        },
    },
}

SIZING_GUIDELINES = {
    "high_conviction": {
        "weight": "4-6% of portfolio",
        "criteria": "Quality + Growth scores > 70, thesis on-track, coverage PASS",
    },
    "standard": {
        "weight": "2-4% of portfolio",
        "criteria": "Composite score > 50, no critical risk flags",
    },
    "exploratory": {
        "weight": "0.5-2% of portfolio",
        "criteria": "Thesis at-risk OR coverage gaps OR turnaround play",
    },
    "avoid": {
        "weight": "0%",
        "criteria": "Thesis broken OR critical risk flags OR composite < 30",
    },
}


def get_sector_questions(sector: str) -> list[dict]:
    """Get the 5 key questions for a sector."""
    return SECTOR_KEY_QUESTIONS.get(sector, SECTOR_KEY_QUESTIONS["general"])


def classify_style(metrics: dict) -> list[str]:
    """
    Classify investment style based on financial metrics.
    A stock can match multiple styles (e.g., quality + growth).

    Returns:
        List of matching style names.
    """
    styles = []
    for style, criteria in STYLE_CRITERIA.items():
        match = True
        for metric, (op, threshold) in criteria["checks"].items():
            value = metrics.get(metric)
            if value is None:
                match = False
                break
            if op == ">=" and value < threshold:
                match = False
                break
            if op == "<=" and value > threshold:
                match = False
                break
        if match:
            styles.append(style)
    return styles if styles else ["general"]


def get_sizing_recommendation(
    composite_score: float,
    thesis_status: str,
    risk_flags: list,
    coverage_passed: bool,
) -> dict:
    """
    Determine position sizing guideline.

    Returns:
        Dict with size_tier, weight, and criteria.
    """
    has_critical = any(f.get("severity") == "critical" for f in risk_flags)

    if thesis_status == "broken" or has_critical or composite_score < 30:
        tier = "avoid"
    elif thesis_status == "at_risk" or not coverage_passed or composite_score < 50:
        tier = "exploratory"
    elif composite_score >= 70 and thesis_status == "on_track" and coverage_passed:
        tier = "high_conviction"
    else:
        tier = "standard"

    guide = SIZING_GUIDELINES[tier]
    return {
        "size_tier": tier,
        "weight": guide["weight"],
        "criteria": guide["criteria"],
    }
