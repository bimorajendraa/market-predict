"""
Market Price Fetcher module for Finance Analytics.
Fetches daily OHLCV data from Yahoo Finance.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import yfinance as yf

from ..db import get_db_cursor

logger = logging.getLogger(__name__)


def fetch_prices(
    ticker: str,
    days: int = 90,
    end_date: Optional[datetime] = None,
) -> list[dict]:
    """
    Fetch daily OHLCV prices from Yahoo Finance.

    Args:
        ticker: Stock ticker (e.g., 'AAPL', 'BBCA.JK')
        days: Number of days of history to fetch
        end_date: End date (defaults to today)

    Returns:
        List of dicts with: ticker, date, open, high, low, close, volume
    """
    end = end_date or datetime.now()
    start = end - timedelta(days=days)

    logger.info(
        f"Fetching {days} days of prices for {ticker} "
        f"({start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')})"
    )

    try:
        stock = yf.Ticker(ticker)
        df = stock.history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
        )

        if df.empty:
            logger.warning(f"No price data returned for {ticker}")
            return []

        results = []
        for date_idx, row in df.iterrows():
            results.append({
                "ticker": ticker,
                "date": date_idx.date(),
                "open": round(float(row["Open"]), 4),
                "high": round(float(row["High"]), 4),
                "low": round(float(row["Low"]), 4),
                "close": round(float(row["Close"]), 4),
                "volume": int(row["Volume"]),
            })

        logger.info(f"Fetched {len(results)} price records for {ticker}")
        return results

    except Exception as e:
        logger.error(f"Failed to fetch prices for {ticker}: {e}")
        raise


def save_prices(prices: list[dict]) -> int:
    """
    Upsert prices into market_prices table.

    Args:
        prices: List of price dicts from fetch_prices

    Returns:
        Number of records upserted
    """
    if not prices:
        return 0

    count = 0
    with get_db_cursor() as cursor:
        for p in prices:
            cursor.execute(
                """
                INSERT INTO market_prices
                    (ticker, date, open, high, low, close, volume)
                VALUES
                    (%(ticker)s, %(date)s, %(open)s, %(high)s,
                     %(low)s, %(close)s, %(volume)s)
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                p,
            )
            count += 1

    logger.info(f"Upserted {count} price records")
    return count


def get_returns(ticker: str, days: int = 30) -> Optional[float]:
    """
    Calculate return over the specified number of days from stored prices.

    Args:
        ticker: Stock ticker
        days: Number of trading days

    Returns:
        Return as decimal (e.g., 0.05 for 5%), or None if insufficient data
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT close, date FROM market_prices
            WHERE ticker = %(ticker)s
            ORDER BY date DESC
            LIMIT %(limit)s
            """,
            {"ticker": ticker, "limit": days + 1},
        )
        rows = cursor.fetchall()

    if len(rows) < 2:
        return None

    latest_close = float(rows[0]["close"])
    oldest_close = float(rows[-1]["close"])

    if oldest_close == 0:
        return None

    return (latest_close - oldest_close) / oldest_close


def run_market_fetch(ticker: str, days: int = 90) -> dict:
    """
    Full pipeline: fetch prices from Yahoo Finance and save to DB.

    Args:
        ticker: Stock ticker
        days: Days of history

    Returns:
        Result summary dict
    """
    logger.info(f"Running market price fetch for {ticker} ({days} days)")

    prices = fetch_prices(ticker, days)
    count = save_prices(prices)

    return {
        "ticker": ticker,
        "days_requested": days,
        "records_fetched": len(prices),
        "records_upserted": count,
    }
