"""
yfinance Fundamentals module for Finance Analytics.
Fetches real financial data from Yahoo Finance API instead of relying on
parsed HTML/PDF reports.

Extracts: revenue, net_income, operating_income, total_equity, total_debt,
current_assets, current_liabilities, operating_cash_flow, capex, EPS.
"""

import logging
from datetime import datetime
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_fundamentals(ticker: str) -> list[dict]:
    """
    Fetch real financial data from yfinance API.

    Uses yf.Ticker to pull quarterly/annual financials, balance sheet,
    and cash flow data. Returns a list of financial facts ready for
    insertion into the financial_facts table.

    Args:
        ticker: Stock ticker (e.g., 'BBCA.JK', 'AAPL')

    Returns:
        List of fact dicts with: ticker, period, metric, value, unit, currency, source_url
    """
    logger.info(f"Fetching fundamentals from yfinance for {ticker}")
    facts: list[dict] = []

    try:
        stock = yf.Ticker(ticker)
        
        # Quick validation: check if we can get basic info or history
        # Note: yfinance often doesn't raise exceptions for invalid tickers, just empty data.
        if not stock.info or (stock.history(period="1d").empty and not stock.info.get('symbol')):
             logger.warning(f"Ticker '{ticker}' appears to be invalid or delisted (no info/history found).")

        info = stock.info or {}
        currency = info.get("currency", "USD")

        # ── Quarterly Financials (Income Statement) ──
        try:
            q_financials = stock.quarterly_financials
            if q_financials is not None and not q_financials.empty:
                for col in q_financials.columns:
                    period = _date_to_period(col, quarterly=True)
                    data = q_financials[col]

                    _add_fact(facts, ticker, period, "revenue",
                             _safe_get(data, ["Total Revenue", "Revenue"]),
                             currency)
                    _add_fact(facts, ticker, period, "net_income",
                             _safe_get(data, ["Net Income", "Net Income Common Stockholders"]),
                             currency)
                    _add_fact(facts, ticker, period, "operating_income",
                             _safe_get(data, ["Operating Income", "EBIT"]),
                             currency)
                    
                    # Try to get EPS directly or calculate it
                    eps = _safe_get(data, ["Diluted EPS", "Basic EPS"])
                    if eps is None:
                        net_income = _safe_get(data, ["Net Income", "Net Income Common Stockholders"])
                        shares = _safe_get(data, ["Diluted Average Shares", "Basic Average Shares", "Share Issued"])
                        if net_income and shares and shares != 0:
                            eps = net_income / shares
                    
                    _add_fact(facts, ticker, period, "eps", eps, currency, unit="per_share")

                logger.info(f"  Quarterly financials: {len(q_financials.columns)} periods")
        except Exception as e:
            logger.warning(f"  Quarterly financials not available: {e}")

        # ── Quarterly Balance Sheet ──
        try:
            q_balance = stock.quarterly_balance_sheet
            if q_balance is not None and not q_balance.empty:
                for col in q_balance.columns:
                    period = _date_to_period(col, quarterly=True)
                    data = q_balance[col]

                    _add_fact(facts, ticker, period, "total_equity",
                             _safe_get(data, ["Total Stockholders Equity",
                                              "Stockholders Equity",
                                              "Total Equity Gross Minority Interest"]),
                             currency)
                    _add_fact(facts, ticker, period, "total_debt",
                             _safe_get(data, ["Total Debt", "Long Term Debt",
                                              "Total Non Current Liabilities Net Minority Interest"]),
                             currency)
                    _add_fact(facts, ticker, period, "current_assets",
                             _safe_get(data, ["Current Assets", "Total Current Assets"]),
                             currency)
                    _add_fact(facts, ticker, period, "current_liabilities",
                             _safe_get(data, ["Current Liabilities",
                                              "Total Current Liabilities"]),
                             currency)

                logger.info(f"  Quarterly balance sheet: {len(q_balance.columns)} periods")
        except Exception as e:
            logger.warning(f"  Quarterly balance sheet not available: {e}")

        # ── Quarterly Cash Flow ──
        try:
            q_cashflow = stock.quarterly_cashflow
            if q_cashflow is not None and not q_cashflow.empty:
                for col in q_cashflow.columns:
                    period = _date_to_period(col, quarterly=True)
                    data = q_cashflow[col]

                    _add_fact(facts, ticker, period, "operating_cash_flow",
                             _safe_get(data, ["Operating Cash Flow",
                                              "Total Cash From Operating Activities",
                                              "Cash Flow From Continuing Operating Activities"]),
                             currency)
                    _add_fact(facts, ticker, period, "capex",
                             _safe_get(data, ["Capital Expenditure",
                                              "Capital Expenditures"]),
                             currency)

                logger.info(f"  Quarterly cash flow: {len(q_cashflow.columns)} periods")
        except Exception as e:
            logger.warning(f"  Quarterly cash flow not available: {e}")

        # ── EPS from info ──
        try:
            trailing_eps = info.get("trailingEps")
            if trailing_eps is not None:
                now = datetime.now()
                q = (now.month - 1) // 3 + 1
                current_period = f"Q{q}-{now.year}"
                _add_fact(facts, ticker, current_period, "eps",
                         trailing_eps, currency, unit="per_share")
                logger.info(f"  Trailing EPS: {trailing_eps}")
        except Exception as e:
            logger.warning(f"  EPS extraction failed: {e}")

        # ── Annual Financials (for YoY comparison) ──
        try:
            a_financials = stock.financials
            if a_financials is not None and not a_financials.empty:
                for col in a_financials.columns:
                    period = _date_to_period(col, quarterly=False)
                    data = a_financials[col]

                    _add_fact(facts, ticker, period, "revenue",
                             _safe_get(data, ["Total Revenue", "Revenue"]),
                             currency)
                    _add_fact(facts, ticker, period, "net_income",
                             _safe_get(data, ["Net Income", "Net Income Common Stockholders"]),
                             currency)

                    # Try to get EPS directly or calculate it
                    eps = _safe_get(data, ["Diluted EPS", "Basic EPS"])
                    if eps is None:
                        net_income = _safe_get(data, ["Net Income", "Net Income Common Stockholders"])
                        shares = _safe_get(data, ["Diluted Average Shares", "Basic Average Shares", "Share Issued"])
                        if net_income and shares and shares != 0:
                            eps = net_income / shares
                    
                    _add_fact(facts, ticker, period, "eps", eps, currency, unit="per_share")

                logger.info(f"  Annual financials: {len(a_financials.columns)} periods")
        except Exception as e:
            logger.warning(f"  Annual financials not available: {e}")

    except Exception as e:
        logger.error(f"Failed to fetch fundamentals for {ticker}: {e}")

    logger.info(f"Total facts extracted for {ticker}: {len(facts)}")
    return facts


def _date_to_period(date_col, quarterly: bool = True) -> str:
    """
    Convert a pandas Timestamp column header to a period string.

    Args:
        date_col: Pandas Timestamp
        quarterly: If True, return Q-format; otherwise FY-format

    Returns:
        Period string like 'Q3-2025' or 'FY-2025'
    """
    try:
        dt = date_col.to_pydatetime()
        year = dt.year
        if quarterly:
            quarter = (dt.month - 1) // 3 + 1
            return f"Q{quarter}-{year}"
        else:
            return f"FY-{year}"
    except Exception:
        return "UNKNOWN"


def _safe_get(data, keys: list[str]) -> Optional[float]:
    """
    Try multiple keys to extract a value from financial data.
    Returns the first non-null value found.
    """
    import math

    for key in keys:
        try:
            if key in data.index:
                val = data[key]
                if val is not None and not (isinstance(val, float) and math.isnan(val)):
                    return float(val)
        except (KeyError, TypeError, ValueError):
            continue
    return None


def _add_fact(
    facts: list[dict],
    ticker: str,
    period: str,
    metric: str,
    value: Optional[float],
    currency: str,
    unit: str = "amount",
) -> None:
    """Add a fact to the list if value is not None."""
    if value is None:
        return

    facts.append({
        "ticker": ticker,
        "period": period,
        "metric": metric,
        "value": value,
        "unit": unit,
        "currency": currency,
        "source_url": f"yfinance:{ticker}",
    })
