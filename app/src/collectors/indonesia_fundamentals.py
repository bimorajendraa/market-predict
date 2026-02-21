"""
Indonesia Fundamentals Collector.
Collects quarterly fundamentals, bank-specific KPIs, and corporate actions
for Indonesian tickers, then stores them into dedicated DB tables.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

import yfinance as yf

from ..analysis.currency_utils import infer_scale_for_value
from ..db import (
    insert_corporate_action,
    insert_idx_filing,
    insert_financial_fact,
    upsert_bank_metrics,
    upsert_fundamentals_quarterly,
)

logger = logging.getLogger(__name__)

ID_BANKS = {"BBCA", "BBRI", "BMRI", "BBNI", "BBTN", "BRIS", "BDMN", "BNGA", "PNBN", "MEGA"}


def is_indonesia_ticker(ticker: str) -> bool:
    return ticker.upper().endswith(".JK")


def _base_ticker(ticker: str) -> str:
    return ticker.split(".")[0].upper()


def _to_period(dt_like: Any) -> str:
    if hasattr(dt_like, "to_pydatetime"):
        dt = dt_like.to_pydatetime()
    elif isinstance(dt_like, datetime):
        dt = dt_like
    elif isinstance(dt_like, date):
        dt = datetime(dt_like.year, dt_like.month, dt_like.day)
    else:
        return "UNKNOWN"
    q = (dt.month - 1) // 3 + 1
    return f"Q{q}-{dt.year}"


def _safe_series_get(series: Any, keys: list[str]) -> Optional[float]:
    import math

    for key in keys:
        try:
            if key in series.index:
                val = series[key]
                if val is None:
                    continue
                if isinstance(val, float) and math.isnan(val):
                    continue
                return float(val)
        except Exception:
            continue
    return None


def collect_indonesia_fundamentals(ticker: str) -> dict[str, Any]:
    """Collect and persist Indonesia-focused fundamentals into dedicated tables."""
    if not is_indonesia_ticker(ticker):
        return {
            "ticker": ticker,
            "status": "skipped",
            "reason": "non_indonesia_ticker",
            "fundamentals_upserted": 0,
            "bank_metrics_upserted": 0,
            "corporate_actions_inserted": 0,
            "idx_filings_inserted": 0,
        }

    stock = yf.Ticker(ticker)
    info = stock.info or {}
    currency = info.get("currency") or "IDR"

    quarterly_fin = stock.quarterly_financials
    quarterly_bs = stock.quarterly_balance_sheet

    period_rows: dict[str, dict[str, Any]] = {}

    if quarterly_fin is not None and not quarterly_fin.empty:
        for col in quarterly_fin.columns:
            period = _to_period(col)
            row = period_rows.setdefault(period, {"currency": currency, "unit": "raw", "scale": "1"})
            s = quarterly_fin[col]
            row["revenue"] = _safe_series_get(s, ["Total Revenue", "Revenue"])
            row["operating_income"] = _safe_series_get(s, ["Operating Income", "EBIT"])
            row["net_income"] = _safe_series_get(s, ["Net Income", "Net Income Common Stockholders"])
            row["eps"] = _safe_series_get(s, ["Diluted EPS", "Basic EPS"])
            row["net_interest_income"] = _safe_series_get(s, ["Net Interest Income"])
            row["interest_income"] = _safe_series_get(s, ["Interest Income"])
            row["interest_expense"] = _safe_series_get(s, ["Interest Expense"])
            row["operating_expense"] = _safe_series_get(s, ["Operating Expense"])
            row["source_url"] = f"https://finance.yahoo.com/quote/{ticker}/financials"

    if quarterly_bs is not None and not quarterly_bs.empty:
        for col in quarterly_bs.columns:
            period = _to_period(col)
            row = period_rows.setdefault(period, {"currency": currency, "unit": "raw", "scale": "1"})
            s = quarterly_bs[col]
            row["total_assets"] = _safe_series_get(s, ["Total Assets"])
            row["total_equity"] = _safe_series_get(
                s,
                ["Total Stockholders Equity", "Stockholders Equity", "Total Equity Gross Minority Interest"],
            )
            row["total_debt"] = _safe_series_get(s, ["Total Debt", "Long Term Debt"])
            row["shares_outstanding"] = _safe_series_get(s, ["Share Issued", "Ordinary Shares Number"])
            row["source_url"] = f"https://finance.yahoo.com/quote/{ticker}/balance-sheet"

    fundamentals_upserted = 0
    idx_filings_inserted = 0

    for period, row in period_rows.items():
        for key in ["revenue", "operating_income", "net_income", "total_assets", "total_equity", "total_debt"]:
            if row.get(key) is not None:
                row["scale"] = infer_scale_for_value(row.get(key), currency)
                break

        upsert_fundamentals_quarterly(ticker=ticker, period=period, values=row)
        fundamentals_upserted += 1

        filing_url = row.get("source_url") or f"https://finance.yahoo.com/quote/{ticker}/financials"
        filing_id = insert_idx_filing(
            ticker=ticker,
            filing_type="quarterly_report",
            url=filing_url,
            period=period,
            source="idx_ir",
            doc_kind="html",
            title=f"{ticker} {period} quarterly financial summary",
        )
        if filing_id:
            idx_filings_inserted += 1

        metric_map = {
            "revenue": row.get("revenue"),
            "operating_income": row.get("operating_income"),
            "net_income": row.get("net_income"),
            "eps": row.get("eps"),
            "total_assets": row.get("total_assets"),
            "total_equity": row.get("total_equity"),
            "total_debt": row.get("total_debt"),
            "shares_outstanding": row.get("shares_outstanding"),
        }
        for metric, value in metric_map.items():
            if value is None:
                continue
            try:
                insert_financial_fact(
                    ticker=ticker,
                    period=period,
                    metric=metric,
                    value=float(value),
                    unit=row.get("unit", "raw"),
                    currency=currency,
                    source_url=row.get("source_url"),
                )
            except Exception:
                continue

    bank_metrics_upserted = 0
    if _base_ticker(ticker) in ID_BANKS:
        quarter = _latest_period(period_rows.keys())
        if quarter:
            fin = period_rows.get(quarter, {})
            nim = info.get("netInterestMargin")
            npl = info.get("nonPerformingLoanRatio")
            car = info.get("capitalAdequacyRatio")
            ldr = info.get("loanToDepositRatio")
            casa = info.get("casaRatio")
            bopo = info.get("costIncomeRatio")
            coc = info.get("costOfCredit")

            bank_values = {
                "currency": currency,
                "unit": "ratio",
                "scale": "1",
                "nim": _ratio_to_decimal(nim),
                "npl": _ratio_to_decimal(npl),
                "car_kpmm": _ratio_to_decimal(car),
                "ldr": _ratio_to_decimal(ldr),
                "casa": _ratio_to_decimal(casa),
                "bopo": _ratio_to_decimal(bopo),
                "cost_of_credit": _ratio_to_decimal(coc),
                "source_url": f"https://finance.yahoo.com/quote/{ticker}/analysis",
            }

            # Derive proxy ratios from available quarterly statements when direct metrics are missing
            total_assets = fin.get("total_assets")
            total_equity = fin.get("total_equity")
            revenue = fin.get("revenue")
            net_interest_income = fin.get("net_interest_income")
            if net_interest_income is None:
                ii = fin.get("interest_income")
                ie = fin.get("interest_expense")
                if ii is not None and ie is not None:
                    net_interest_income = float(ii) - abs(float(ie))

            if bank_values["nim"] is None and net_interest_income is not None and total_assets not in (None, 0):
                bank_values["nim"] = float(net_interest_income) / float(total_assets)

            if bank_values["car_kpmm"] is None and total_equity not in (None, 0) and total_assets not in (None, 0):
                bank_values["car_kpmm"] = float(total_equity) / float(total_assets)

            if bank_values["bopo"] is None:
                op_exp = fin.get("operating_expense")
                if op_exp is not None and revenue not in (None, 0):
                    bank_values["bopo"] = abs(float(op_exp)) / float(revenue)

            upsert_bank_metrics(ticker=ticker, period=quarter, values=bank_values)
            bank_metrics_upserted += 1

            alias_map = {
                "net_interest_margin": bank_values["nim"],
                "non_performing_loan": bank_values["npl"],
                "capital_adequacy_ratio": bank_values["car_kpmm"],
                "loan_to_deposit_ratio": bank_values["ldr"],
                "casa_ratio": bank_values["casa"],
                "cost_to_income": bank_values["bopo"],
                "cost_of_credit": bank_values["cost_of_credit"],
            }
            for metric, value in alias_map.items():
                if value is None:
                    continue
                insert_financial_fact(
                    ticker=ticker,
                    period=quarter,
                    metric=metric,
                    value=float(value),
                    unit="ratio",
                    currency=currency,
                    source_url=bank_values["source_url"],
                )

            insert_idx_filing(
                ticker=ticker,
                filing_type="earnings_release",
                url=bank_values["source_url"],
                period=quarter,
                source="earnings_pr",
                doc_kind="html",
                title=f"{ticker} {quarter} earnings metrics",
            )

    corp_actions = 0
    corp_actions += _collect_dividends(stock, ticker, currency)
    corp_actions += _collect_splits(stock, ticker)
    corp_actions += _collect_share_count(stock, ticker)

    return {
        "ticker": ticker,
        "status": "success",
        "fundamentals_upserted": fundamentals_upserted,
        "bank_metrics_upserted": bank_metrics_upserted,
        "corporate_actions_inserted": corp_actions,
        "idx_filings_inserted": idx_filings_inserted,
    }


def _ratio_to_decimal(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    return f / 100.0 if f > 1 else f


def _latest_period(periods: Any) -> Optional[str]:
    items = list(periods)
    if not items:
        return None

    def key_fn(p: str) -> tuple[int, int]:
        try:
            q, y = p.split("-")
            return int(y), int(q.replace("Q", ""))
        except Exception:
            return (0, 0)

    return sorted(items, key=key_fn, reverse=True)[0]


def _collect_dividends(stock: Any, ticker: str, currency: str) -> int:
    count = 0
    try:
        div = stock.dividends
        if div is None or len(div) == 0:
            return 0
        for idx, val in div.tail(24).items():
            d = idx.date() if hasattr(idx, "date") else idx
            inserted = insert_corporate_action(
                ticker=ticker,
                action_date=d,
                action_type="dividend",
                amount=float(val),
                currency=currency,
                source_url=f"https://finance.yahoo.com/quote/{ticker}/history?p={ticker}",
            )
            if inserted:
                count += 1
    except Exception as e:
        logger.debug(f"Dividend collection failed for {ticker}: {e}")
    return count


def _collect_splits(stock: Any, ticker: str) -> int:
    count = 0
    try:
        spl = stock.splits
        if spl is None or len(spl) == 0:
            return 0
        for idx, val in spl.tail(12).items():
            d = idx.date() if hasattr(idx, "date") else idx
            ratio = str(val)
            inserted = insert_corporate_action(
                ticker=ticker,
                action_date=d,
                action_type="split" if float(val) >= 1 else "reverse_split",
                ratio=ratio,
                source_url=f"https://finance.yahoo.com/quote/{ticker}/history?p={ticker}",
            )
            if inserted:
                count += 1
    except Exception as e:
        logger.debug(f"Split collection failed for {ticker}: {e}")
    return count


def _collect_share_count(stock: Any, ticker: str) -> int:
    count = 0
    try:
        shares = stock.get_shares_full(start="2019-01-01")
        if shares is None or len(shares) == 0:
            return 0
        for idx, val in shares.tail(12).items():
            d = idx.date() if hasattr(idx, "date") else idx
            inserted = insert_corporate_action(
                ticker=ticker,
                action_date=d,
                action_type="share_count",
                shares_outstanding=float(val),
                source_url=f"https://finance.yahoo.com/quote/{ticker}/key-statistics",
            )
            if inserted:
                count += 1
    except Exception as e:
        logger.debug(f"Share count collection failed for {ticker}: {e}")
    return count
