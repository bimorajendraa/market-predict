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


# ============================================
# Filings Raw
# ============================================

def insert_filing_raw(
    ticker: str,
    source: str,
    filing_type: str,
    url: str,
    filing_date: Optional[str] = None,
    sha256: Optional[str] = None,
    stored_path: Optional[str] = None,
    accession_number: Optional[str] = None,
    cik: Optional[str] = None,
) -> UUID:
    """Insert a raw filing record for audit trail."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO filings_raw
                (ticker, source, filing_type, filing_date, url, sha256,
                 stored_path, accession_number, cik)
            VALUES
                (%(ticker)s, %(source)s, %(filing_type)s, %(filing_date)s,
                 %(url)s, %(sha256)s, %(stored_path)s, %(accession_number)s, %(cik)s)
            RETURNING id
            """,
            {
                "ticker": ticker,
                "source": source,
                "filing_type": filing_type,
                "filing_date": filing_date,
                "url": url,
                "sha256": sha256,
                "stored_path": stored_path,
                "accession_number": accession_number,
                "cik": cik,
            },
        )
        result = cursor.fetchone()
        return result["id"]


def get_filings_for_ticker(
    ticker: str, filing_type: Optional[str] = None
) -> list[dict[str, Any]]:
    """Get raw filings for a ticker, optionally filtered by type."""
    with get_db_cursor() as cursor:
        if filing_type:
            cursor.execute(
                """
                SELECT * FROM filings_raw
                WHERE ticker = %(ticker)s AND filing_type = %(ft)s
                ORDER BY filing_date DESC
                """,
                {"ticker": ticker, "ft": filing_type},
            )
        else:
            cursor.execute(
                """
                SELECT * FROM filings_raw
                WHERE ticker = %(ticker)s
                ORDER BY filing_date DESC
                """,
                {"ticker": ticker},
            )
        return cursor.fetchall()


# ============================================
# Filings Extracted
# ============================================

def insert_filing_extracted(
    filing_id: UUID,
    metric: str,
    value: float,
    unit: Optional[str] = None,
    period_end: Optional[str] = None,
    context: Optional[str] = None,
    confidence: float = 1.0,
    extractor_version: str = "v1",
) -> UUID:
    """Insert an extracted metric from a filing."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO filings_extracted
                (filing_id, metric, value, unit, period_end, context,
                 confidence, extractor_version)
            VALUES
                (%(filing_id)s, %(metric)s, %(value)s, %(unit)s,
                 %(period_end)s, %(context)s, %(confidence)s, %(version)s)
            RETURNING id
            """,
            {
                "filing_id": filing_id,
                "metric": metric,
                "value": value,
                "unit": unit,
                "period_end": period_end,
                "context": context,
                "confidence": confidence,
                "version": extractor_version,
            },
        )
        result = cursor.fetchone()
        return result["id"]


def get_extracted_metrics(filing_id: UUID) -> list[dict[str, Any]]:
    """Get all extracted metrics for a filing."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM filings_extracted WHERE filing_id = %(fid)s ORDER BY metric",
            {"fid": filing_id},
        )
        return cursor.fetchall()


# ============================================
# Thesis
# ============================================

def insert_thesis(
    ticker: str,
    base_thesis: str,
    sector: Optional[str] = None,
    bull_case: Optional[str] = None,
    bear_case: Optional[str] = None,
    kpis_json: Optional[list] = None,
    triggers_json: Optional[list] = None,
) -> UUID:
    """Insert or update an investment thesis for a ticker."""
    import json as _json
    with get_db_cursor() as cursor:
        # Upsert: delete old thesis for ticker, insert new
        cursor.execute("DELETE FROM thesis WHERE ticker = %(t)s", {"t": ticker})
        cursor.execute(
            """
            INSERT INTO thesis
                (ticker, sector, base_thesis, bull_case, bear_case,
                 kpis_json, triggers_json)
            VALUES
                (%(ticker)s, %(sector)s, %(base)s, %(bull)s, %(bear)s,
                 %(kpis)s, %(triggers)s)
            RETURNING id
            """,
            {
                "ticker": ticker,
                "sector": sector,
                "base": base_thesis,
                "bull": bull_case,
                "bear": bear_case,
                "kpis": _json.dumps(kpis_json or []),
                "triggers": _json.dumps(triggers_json or []),
            },
        )
        result = cursor.fetchone()
        return result["id"]


def get_thesis(ticker: str) -> Optional[dict[str, Any]]:
    """Get the current thesis for a ticker."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM thesis WHERE ticker = %(t)s ORDER BY updated_at DESC LIMIT 1",
            {"t": ticker},
        )
        return cursor.fetchone()


def update_thesis_status(ticker: str, status: str) -> None:
    """Update thesis status (on_track / at_risk / broken)."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "UPDATE thesis SET status = %(s)s WHERE ticker = %(t)s",
            {"s": status, "t": ticker},
        )


# ============================================
# Pipeline Runs (Audit)
# ============================================

def start_pipeline_run(
    ticker: str,
    period: Optional[str] = None,
    run_type: str = "pipeline",
    config_snapshot: Optional[dict] = None,
) -> UUID:
    """Start a pipeline run for audit tracking. Returns run_id."""
    import json as _json
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO pipeline_runs
                (ticker, period, run_type, config_snapshot, status)
            VALUES
                (%(ticker)s, %(period)s, %(run_type)s, %(config)s, 'running')
            RETURNING id
            """,
            {
                "ticker": ticker,
                "period": period,
                "run_type": run_type,
                "config": _json.dumps(config_snapshot or {}),
            },
        )
        result = cursor.fetchone()
        return result["id"]


def complete_pipeline_run(
    run_id: UUID,
    status: str = "completed",
    sources_json: Optional[list] = None,
    row_counts_json: Optional[dict] = None,
    error: Optional[str] = None,
) -> None:
    """Mark a pipeline run as completed/failed."""
    import json as _json
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_runs
            SET status = %(status)s,
                sources_json = %(sources)s,
                row_counts_json = %(counts)s,
                error = %(error)s,
                completed_at = NOW()
            WHERE id = %(id)s
            """,
            {
                "id": run_id,
                "status": status,
                "sources": _json.dumps(sources_json or []),
                "counts": _json.dumps(row_counts_json or {}),
                "error": error,
            },
        )


def get_pipeline_run(run_id: UUID) -> Optional[dict[str, Any]]:
    """Get a pipeline run by ID."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM pipeline_runs WHERE id = %(id)s",
            {"id": run_id},
        )
        return cursor.fetchone()


def get_pipeline_runs_for_ticker(
    ticker: str, limit: int = 10
) -> list[dict[str, Any]]:
    """Get recent pipeline runs for a ticker."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM pipeline_runs
            WHERE ticker = %(t)s ORDER BY created_at DESC LIMIT %(l)s
            """,
            {"t": ticker, "l": limit},
        )
        return cursor.fetchall()


# ============================================
# IDX / Indonesia Fundamentals
# ============================================

def insert_idx_filing(
    ticker: str,
    filing_type: str,
    url: str,
    filing_date: Optional[str] = None,
    period: Optional[str] = None,
    source: str = "idx_ir",
    doc_kind: Optional[str] = None,
    title: Optional[str] = None,
    checksum: Optional[str] = None,
) -> Optional[UUID]:
    """Insert IDX/IR filing metadata with dedup by ticker+url."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO idx_filings
                (ticker, source, filing_type, filing_date, period, url, checksum, doc_kind, title)
            VALUES
                (%(ticker)s, %(source)s, %(filing_type)s, %(filing_date)s, %(period)s,
                 %(url)s, %(checksum)s, %(doc_kind)s, %(title)s)
            ON CONFLICT (ticker, url) DO NOTHING
            RETURNING id
            """,
            {
                "ticker": ticker,
                "source": source,
                "filing_type": filing_type,
                "filing_date": filing_date,
                "period": period,
                "url": url,
                "checksum": checksum,
                "doc_kind": doc_kind,
                "title": title,
            },
        )
        row = cursor.fetchone()
        return row["id"] if row else None


def upsert_fundamentals_quarterly(
    ticker: str,
    period: str,
    values: dict[str, Any],
) -> None:
    """Upsert structured fundamentals quarterly record."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO fundamentals_quarterly
                (ticker, period, statement_date, currency, unit, scale,
                 revenue, operating_income, net_income, eps,
                 total_assets, total_equity, total_debt, shares_outstanding,
                 source_url)
            VALUES
                (%(ticker)s, %(period)s, %(statement_date)s, %(currency)s, %(unit)s, %(scale)s,
                 %(revenue)s, %(operating_income)s, %(net_income)s, %(eps)s,
                 %(total_assets)s, %(total_equity)s, %(total_debt)s, %(shares_outstanding)s,
                 %(source_url)s)
            ON CONFLICT (ticker, period)
            DO UPDATE SET
                statement_date = EXCLUDED.statement_date,
                currency = EXCLUDED.currency,
                unit = EXCLUDED.unit,
                scale = EXCLUDED.scale,
                revenue = EXCLUDED.revenue,
                operating_income = EXCLUDED.operating_income,
                net_income = EXCLUDED.net_income,
                eps = EXCLUDED.eps,
                total_assets = EXCLUDED.total_assets,
                total_equity = EXCLUDED.total_equity,
                total_debt = EXCLUDED.total_debt,
                shares_outstanding = EXCLUDED.shares_outstanding,
                source_url = EXCLUDED.source_url,
                updated_at = NOW()
            """,
            {
                "ticker": ticker,
                "period": period,
                "statement_date": values.get("statement_date"),
                "currency": values.get("currency", "IDR"),
                "unit": values.get("unit", "raw"),
                "scale": values.get("scale", "1"),
                "revenue": values.get("revenue"),
                "operating_income": values.get("operating_income"),
                "net_income": values.get("net_income"),
                "eps": values.get("eps"),
                "total_assets": values.get("total_assets"),
                "total_equity": values.get("total_equity"),
                "total_debt": values.get("total_debt"),
                "shares_outstanding": values.get("shares_outstanding"),
                "source_url": values.get("source_url"),
            },
        )


def upsert_bank_metrics(
    ticker: str,
    period: str,
    values: dict[str, Any],
) -> None:
    """Upsert bank KPI record for Indonesian banks."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO bank_metrics
                (ticker, period, statement_date, currency, unit, scale,
                 nim, npl, car_kpmm, ldr, casa, bopo, cost_of_credit, source_url)
            VALUES
                (%(ticker)s, %(period)s, %(statement_date)s, %(currency)s, %(unit)s, %(scale)s,
                 %(nim)s, %(npl)s, %(car_kpmm)s, %(ldr)s, %(casa)s, %(bopo)s, %(cost_of_credit)s,
                 %(source_url)s)
            ON CONFLICT (ticker, period)
            DO UPDATE SET
                statement_date = EXCLUDED.statement_date,
                currency = EXCLUDED.currency,
                unit = EXCLUDED.unit,
                scale = EXCLUDED.scale,
                nim = EXCLUDED.nim,
                npl = EXCLUDED.npl,
                car_kpmm = EXCLUDED.car_kpmm,
                ldr = EXCLUDED.ldr,
                casa = EXCLUDED.casa,
                bopo = EXCLUDED.bopo,
                cost_of_credit = EXCLUDED.cost_of_credit,
                source_url = EXCLUDED.source_url,
                updated_at = NOW()
            """,
            {
                "ticker": ticker,
                "period": period,
                "statement_date": values.get("statement_date"),
                "currency": values.get("currency", "IDR"),
                "unit": values.get("unit", "ratio"),
                "scale": values.get("scale", "1"),
                "nim": values.get("nim"),
                "npl": values.get("npl"),
                "car_kpmm": values.get("car_kpmm"),
                "ldr": values.get("ldr"),
                "casa": values.get("casa"),
                "bopo": values.get("bopo"),
                "cost_of_credit": values.get("cost_of_credit"),
                "source_url": values.get("source_url"),
            },
        )


def insert_corporate_action(
    ticker: str,
    action_date: Any,
    action_type: str,
    amount: Optional[float] = None,
    currency: Optional[str] = None,
    ratio: Optional[str] = None,
    shares_outstanding: Optional[float] = None,
    payout_date: Optional[Any] = None,
    source_url: Optional[str] = None,
    metadata_json: Optional[dict[str, Any]] = None,
) -> Optional[UUID]:
    """Insert corporate action with dedup and metadata."""
    import json as _json

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO corporate_actions
                (ticker, action_date, action_type, amount, currency, ratio,
                 shares_outstanding, payout_date, source_url, metadata_json)
            VALUES
                (%(ticker)s, %(action_date)s, %(action_type)s, %(amount)s, %(currency)s,
                 %(ratio)s, %(shares_outstanding)s, %(payout_date)s, %(source_url)s,
                 %(metadata_json)s)
            ON CONFLICT (ticker, action_date, action_type, ratio) DO NOTHING
            RETURNING id
            """,
            {
                "ticker": ticker,
                "action_date": action_date,
                "action_type": action_type,
                "amount": amount,
                "currency": currency,
                "ratio": ratio,
                "shares_outstanding": shares_outstanding,
                "payout_date": payout_date,
                "source_url": source_url,
                "metadata_json": _json.dumps(metadata_json or {}),
            },
        )
        row = cursor.fetchone()
        return row["id"] if row else None


def get_latest_bank_metrics(ticker: str, limit: int = 4) -> list[dict[str, Any]]:
    """Get latest bank metrics by period descending."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM bank_metrics
            WHERE ticker = %(ticker)s
            ORDER BY period DESC
            LIMIT %(limit)s
            """,
            {"ticker": ticker, "limit": limit},
        )
        return cursor.fetchall()


def get_latest_fundamentals_quarterly(ticker: str, limit: int = 6) -> list[dict[str, Any]]:
    """Get latest fundamentals quarterly rows."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM fundamentals_quarterly
            WHERE ticker = %(ticker)s
            ORDER BY period DESC
            LIMIT %(limit)s
            """,
            {"ticker": ticker, "limit": limit},
        )
        return cursor.fetchall()


def get_recent_corporate_actions(ticker: str, years: int = 5) -> list[dict[str, Any]]:
    """Get recent corporate actions for memo/monitoring usage."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM corporate_actions
            WHERE ticker = %(ticker)s
              AND action_date >= (CURRENT_DATE - (%(years)s::int * INTERVAL '1 year'))
            ORDER BY action_date DESC
            """,
            {"ticker": ticker, "years": years},
        )
        return cursor.fetchall()

