"""
Database module for Finance Analytics.
Provides PostgreSQL connection and helper functions.
"""

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator, Optional
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from .config import config

logger = logging.getLogger(__name__)


def get_connection() -> psycopg.Connection:
    """Create and return a new database connection."""
    return psycopg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        dbname=config.POSTGRES_DB,
        row_factory=dict_row,
    )


@contextmanager
def get_db_cursor() -> Generator[psycopg.Cursor, None, None]:
    """Context manager for database cursor with auto-commit."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            yield cursor
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()


def insert_fetch_job(
    source: str,
    doc_type: str,
    url: str,
    ticker: Optional[str] = None,
    status: str = "pending",
    http_code: Optional[int] = None,
    checksum: Optional[str] = None,
    raw_object_key: Optional[str] = None,
    error: Optional[str] = None,
) -> UUID:
    """
    Insert a new fetch job record.
    
    Returns:
        UUID of the inserted job.
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO fetch_jobs 
                (source, ticker, doc_type, url, status, http_code, checksum, raw_object_key, error, fetched_at)
            VALUES 
                (%(source)s, %(ticker)s, %(doc_type)s, %(url)s, %(status)s, 
                 %(http_code)s, %(checksum)s, %(raw_object_key)s, %(error)s, %(fetched_at)s)
            RETURNING id
            """,
            {
                "source": source,
                "ticker": ticker,
                "doc_type": doc_type,
                "url": url,
                "status": status,
                "http_code": http_code,
                "checksum": checksum,
                "raw_object_key": raw_object_key,
                "error": error,
                "fetched_at": datetime.utcnow() if status != "pending" else None,
            },
        )
        result = cursor.fetchone()
        return result["id"]


def update_fetch_job(
    job_id: UUID,
    status: str,
    http_code: Optional[int] = None,
    checksum: Optional[str] = None,
    raw_object_key: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Update an existing fetch job record."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            UPDATE fetch_jobs
            SET status = %(status)s,
                http_code = %(http_code)s,
                checksum = %(checksum)s,
                raw_object_key = %(raw_object_key)s,
                error = %(error)s,
                fetched_at = NOW()
            WHERE id = %(job_id)s
            """,
            {
                "job_id": job_id,
                "status": status,
                "http_code": http_code,
                "checksum": checksum,
                "raw_object_key": raw_object_key,
                "error": error,
            },
        )


def insert_news_item(
    source: str,
    title: str,
    url: str,
    ticker: Optional[str] = None,
    published_at: Optional[datetime] = None,
    body: Optional[str] = None,
    checksum: Optional[str] = None,
) -> Optional[UUID]:
    """
    Insert a news item with deduplication by URL.
    
    Returns:
        UUID of the inserted item, or None if already exists.
    """
    with get_db_cursor() as cursor:
        try:
            cursor.execute(
                """
                INSERT INTO news_items 
                    (ticker, source, published_at, title, url, body, checksum)
                VALUES 
                    (%(ticker)s, %(source)s, %(published_at)s, %(title)s, %(url)s, %(body)s, %(checksum)s)
                ON CONFLICT (url) DO NOTHING
                RETURNING id
                """,
                {
                    "ticker": ticker,
                    "source": source,
                    "published_at": published_at,
                    "title": title,
                    "url": url,
                    "body": body,
                    "checksum": checksum,
                },
            )
            result = cursor.fetchone()
            return result["id"] if result else None
        except Exception as e:
            logger.warning(f"Could not insert news item: {e}")
            return None


def insert_financial_fact(
    ticker: str,
    period: str,
    metric: str,
    value: float,
    unit: Optional[str] = None,
    currency: Optional[str] = None,
    source_url: Optional[str] = None,
) -> UUID:
    """Insert a financial fact record."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO financial_facts 
                (ticker, period, metric, value, unit, currency, source_url)
            VALUES 
                (%(ticker)s, %(period)s, %(metric)s, %(value)s, %(unit)s, %(currency)s, %(source_url)s)
            RETURNING id
            """,
            {
                "ticker": ticker,
                "period": period,
                "metric": metric,
                "value": value,
                "unit": unit,
                "currency": currency,
                "source_url": source_url,
            },
        )
        result = cursor.fetchone()
        return result["id"]


def check_duplicate_by_checksum(table: str, checksum: str) -> bool:
    """Check if a record with the given checksum already exists."""
    with get_db_cursor() as cursor:
        cursor.execute(
            f"SELECT EXISTS(SELECT 1 FROM {table} WHERE checksum = %(checksum)s)",
            {"checksum": checksum},
        )
        result = cursor.fetchone()
        return result["exists"]


def get_fetch_jobs_by_status(status: str, limit: int = 100) -> list[dict[str, Any]]:
    """Get fetch jobs by status."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM fetch_jobs 
            WHERE status = %(status)s 
            ORDER BY created_at DESC 
            LIMIT %(limit)s
            """,
            {"status": status, "limit": limit},
        )
        return cursor.fetchall()


# ============================================
# Financial Scores
# ============================================

def insert_financial_score(
    ticker: str,
    period: str,
    score: float,
    drivers_json: list[dict],
) -> UUID:
    """Insert a financial score record with explainable drivers."""
    import json as _json

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO scores_financial
                (ticker, period, score, drivers_json)
            VALUES
                (%(ticker)s, %(period)s, %(score)s, %(drivers_json)s)
            RETURNING id
            """,
            {
                "ticker": ticker,
                "period": period,
                "score": score,
                "drivers_json": _json.dumps(drivers_json),
            },
        )
        result = cursor.fetchone()
        return result["id"]


def get_latest_score(ticker: str, period: Optional[str] = None) -> Optional[dict[str, Any]]:
    """Get the most recent financial score for a ticker."""
    with get_db_cursor() as cursor:
        if period:
            cursor.execute(
                """
                SELECT * FROM scores_financial
                WHERE ticker = %(ticker)s AND period = %(period)s
                ORDER BY created_at DESC LIMIT 1
                """,
                {"ticker": ticker, "period": period},
            )
        else:
            cursor.execute(
                """
                SELECT * FROM scores_financial
                WHERE ticker = %(ticker)s
                ORDER BY created_at DESC LIMIT 1
                """,
                {"ticker": ticker},
            )
        return cursor.fetchone()


# ============================================
# News Sentiment
# ============================================

def insert_news_sentiment(
    ticker: str,
    date: Any,
    headline: Optional[str],
    sentiment: str,
    impact: float,
    events_json: list,
    sources_json: list,
) -> UUID:
    """Insert a news sentiment analysis result."""
    import json as _json

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO news_sentiment
                (ticker, date, headline, sentiment, impact, events_json, sources_json)
            VALUES
                (%(ticker)s, %(date)s, %(headline)s, %(sentiment)s,
                 %(impact)s, %(events_json)s, %(sources_json)s)
            RETURNING id
            """,
            {
                "ticker": ticker,
                "date": date or datetime.utcnow(),
                "headline": headline,
                "sentiment": sentiment,
                "impact": impact,
                "events_json": _json.dumps(events_json),
                "sources_json": _json.dumps(sources_json),
            },
        )
        result = cursor.fetchone()
        return result["id"]


def get_news_for_ticker(ticker: str, limit: int = 50) -> list[dict[str, Any]]:
    """Get news sentiment records for a ticker."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM news_sentiment
            WHERE ticker = %(ticker)s
            ORDER BY date DESC
            LIMIT %(limit)s
            """,
            {"ticker": ticker, "limit": limit},
        )
        return cursor.fetchall()


# ============================================
# Market Prices
# ============================================

def upsert_market_price(
    ticker: str,
    date: Any,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: int,
) -> None:
    """Upsert a market price record (dedup by ticker+date)."""
    with get_db_cursor() as cursor:
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
            {
                "ticker": ticker,
                "date": date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            },
        )


def get_market_prices(ticker: str, days: int = 90) -> list[dict[str, Any]]:
    """Get market prices for a ticker ordered by date descending."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM market_prices
            WHERE ticker = %(ticker)s
            ORDER BY date DESC
            LIMIT %(limit)s
            """,
            {"ticker": ticker, "limit": days},
        )
        return cursor.fetchall()


# ============================================
# Company Summary
# ============================================

def insert_company_summary(
    ticker: str,
    period: str,
    rating: str,
    narrative: str,
    evidence_json: dict,
) -> UUID:
    """Insert a company summary record."""
    import json as _json

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO company_summary
                (ticker, period, rating, narrative, evidence_json)
            VALUES
                (%(ticker)s, %(period)s, %(rating)s, %(narrative)s, %(evidence_json)s)
            RETURNING id
            """,
            {
                "ticker": ticker,
                "period": period,
                "rating": rating,
                "narrative": narrative,
                "evidence_json": _json.dumps(evidence_json),
            },
        )
        result = cursor.fetchone()
        return result["id"]


def get_financial_facts(
    ticker: str,
    periods: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """Get financial facts for a ticker, optionally filtered by periods."""
    with get_db_cursor() as cursor:
        if periods:
            cursor.execute(
                """
                SELECT * FROM financial_facts
                WHERE ticker = %(ticker)s AND period = ANY(%(periods)s)
                ORDER BY period DESC, metric
                """,
                {"ticker": ticker, "periods": periods},
            )
        else:
            cursor.execute(
                """
                SELECT * FROM financial_facts
                WHERE ticker = %(ticker)s
                ORDER BY period DESC, metric
                """,
                {"ticker": ticker},
            )
        return cursor.fetchall()

