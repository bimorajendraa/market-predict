"""
Prefect Flow module for Finance Analytics.
Orchestrates the full pipeline: scrape → parse → analyze → summarize.
"""

import logging
from datetime import timedelta
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

from ..collectors.news_rss import scrape_rss, EXAMPLE_FEEDS
from ..collectors.company_reports import crawl_reports, EXAMPLE_REPORT_PAGES
from ..storage import ensure_bucket_exists

logger = logging.getLogger(__name__)


# ============================================
# Scraping Tasks (Original)
# ============================================

@task(
    name="collect-news-rss",
    description="Scrape RSS feeds for news articles",
    retries=2,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def collect_news_task(feed_urls: list[str]) -> dict:
    """Prefect task for collecting news from RSS feeds."""
    task_logger = get_run_logger()
    task_logger.info(f"Starting RSS collection with {len(feed_urls)} feeds")

    try:
        inserted_ids = scrape_rss(feed_urls)
        task_logger.info(f"RSS collection complete: {len(inserted_ids)} items inserted")
        return {
            "status": "success",
            "feeds_processed": len(feed_urls),
            "items_inserted": len(inserted_ids),
            "item_ids": [str(id) for id in inserted_ids],
        }
    except Exception as e:
        task_logger.error(f"RSS collection failed: {e}")
        return {"status": "failed", "error": str(e)}


@task(
    name="collect-company-reports",
    description="Crawl pages and download company reports",
    retries=2,
    retry_delay_seconds=120,
)
def collect_reports_task(
    page_urls: list[str],
    use_playwright: bool = False,
    download_limit: Optional[int] = None,
) -> dict:
    """Prefect task for collecting company reports."""
    task_logger = get_run_logger()
    task_logger.info(f"Starting report collection from {len(page_urls)} pages")

    try:
        job_ids = crawl_reports(page_urls, use_playwright, download_limit)
        task_logger.info(f"Report collection complete: {len(job_ids)} jobs processed")
        return {
            "status": "success",
            "pages_processed": len(page_urls),
            "jobs_created": len(job_ids),
            "job_ids": [str(id) for id in job_ids],
        }
    except Exception as e:
        task_logger.error(f"Report collection failed: {e}")
        return {"status": "failed", "error": str(e)}


@task(name="ensure-storage")
def ensure_storage_task() -> dict:
    """Ensure MinIO bucket exists."""
    task_logger = get_run_logger()
    try:
        ensure_bucket_exists()
        return {"status": "success"}
    except Exception as e:
        task_logger.error(f"Storage setup failed: {e}")
        return {"status": "failed", "error": str(e)}


# ============================================
# New Analytics Tasks
# ============================================

@task(
    name="parse-reports",
    description="Parse downloaded reports into financial_facts",
    retries=1,
    retry_delay_seconds=30,
)
def parse_reports_task(ticker: str, period: str) -> dict:
    """Parse stored HTML/PDF reports and extract financial metrics."""
    task_logger = get_run_logger()
    task_logger.info(f"Parsing reports for {ticker} ({period})")

    try:
        from ..parsers.html_parser import parse_html_report
        from ..parsers.pdf_parser import parse_pdf_bytes
        from ..storage import download_raw
        from ..db import get_db_cursor, insert_financial_fact

        with get_db_cursor() as cursor:
            cursor.execute(
                """
                SELECT id, url, raw_object_key, doc_type
                FROM fetch_jobs
                WHERE status = 'success'
                  AND raw_object_key IS NOT NULL
                  AND (ticker = %(ticker)s OR ticker IS NULL)
                ORDER BY fetched_at DESC LIMIT 50
                """,
                {"ticker": ticker},
            )
            jobs = cursor.fetchall()

        total_facts = 0
        for job in jobs:
            obj_key = job["raw_object_key"]
            source_url = job.get("url", "")
            try:
                raw_data = download_raw(obj_key)
                if obj_key.endswith(".pdf"):
                    facts = parse_pdf_bytes(raw_data, ticker, period, source_url)
                else:
                    html_content = raw_data.decode("utf-8", errors="replace")
                    facts = parse_html_report(html_content, ticker, period, source_url)
                for fact in facts:
                    insert_financial_fact(
                        ticker=fact["ticker"], period=fact["period"],
                        metric=fact["metric"], value=fact["value"],
                        unit=fact.get("unit"), currency=fact.get("currency"),
                        source_url=fact.get("source_url"),
                    )
                    total_facts += 1
            except Exception as e:
                task_logger.warning(f"Failed to parse {obj_key}: {e}")

        return {"status": "success", "reports_parsed": len(jobs), "facts_extracted": total_facts}
    except Exception as e:
        task_logger.error(f"Parse task failed: {e}")
        return {"status": "failed", "error": str(e)}


@task(
    name="fetch-market-prices",
    description="Fetch OHLCV from Yahoo Finance",
    retries=2,
    retry_delay_seconds=60,
)
def fetch_market_task(ticker: str, days: int = 90) -> dict:
    """Fetch and store daily market prices."""
    task_logger = get_run_logger()
    task_logger.info(f"Fetching market prices for {ticker} ({days} days)")

    try:
        from ..market.price_fetcher import run_market_fetch
        result = run_market_fetch(ticker, days)
        return {"status": "success", **result}
    except Exception as e:
        task_logger.error(f"Market fetch failed: {e}")
        return {"status": "failed", "error": str(e)}


@task(
    name="analyze-financials",
    description="Compute financial score with drivers",
    retries=1,
)
def analyze_financials_task(ticker: str, period: str) -> dict:
    """Run financial scoring."""
    task_logger = get_run_logger()
    task_logger.info(f"Running financial scoring for {ticker} ({period})")

    try:
        from ..analysis.financial_scoring import run_financial_scoring
        from ..db import insert_financial_score

        result = run_financial_scoring(ticker, period)
        if result["drivers"]:
            insert_financial_score(ticker, period, result["score"], result["drivers"])
        return {"status": "success", "score": result["score"]}
    except Exception as e:
        task_logger.error(f"Financial scoring failed: {e}")
        return {"status": "failed", "error": str(e)}


@task(
    name="analyze-sentiment",
    description="Analyze news sentiment with event tagging",
    retries=1,
)
def analyze_sentiment_task(ticker: str) -> dict:
    """Run news sentiment analysis."""
    task_logger = get_run_logger()
    task_logger.info(f"Running sentiment analysis for {ticker}")

    try:
        from ..analysis.news_sentiment import run_news_sentiment
        from ..db import insert_news_sentiment

        results = run_news_sentiment(ticker)
        for sr in results:
            insert_news_sentiment(
                ticker=sr["ticker"], date=sr["date"], headline=sr["headline"],
                sentiment=sr["sentiment"], impact=sr["impact"],
                events_json=sr["events_json"], sources_json=sr["sources_json"],
            )
        return {"status": "success", "items_analyzed": len(results)}
    except Exception as e:
        task_logger.error(f"Sentiment analysis failed: {e}")
        return {"status": "failed", "error": str(e)}


@task(
    name="generate-summary",
    description="Generate narrative company summary",
    retries=1,
)
def generate_summary_task(ticker: str, period: str) -> dict:
    """Generate and store company summary."""
    task_logger = get_run_logger()
    task_logger.info(f"Generating summary for {ticker} ({period})")

    try:
        from ..summary.generator import run_summary_generation
        result = run_summary_generation(ticker, period)
        return {"status": "success", "rating": result["rating"]}
    except Exception as e:
        task_logger.error(f"Summary generation failed: {e}")
        return {"status": "failed", "error": str(e)}


# ============================================
# Flows
# ============================================

@flow(
    name="finance-scraping-flow",
    description="Scraping flow for news and company reports",
    version="2.0.0",
    retries=1,
    retry_delay_seconds=300,
)
def scraping_flow(
    feed_urls: Optional[list[str]] = None,
    report_page_urls: Optional[list[str]] = None,
    use_playwright: bool = False,
    download_limit: Optional[int] = 10,
    run_news: bool = True,
    run_reports: bool = True,
) -> dict:
    """Main Prefect flow for scraping (original pipeline)."""
    flow_logger = get_run_logger()
    flow_logger.info("Starting scraping flow")

    results = {"storage": None, "news": None, "reports": None}

    results["storage"] = ensure_storage_task()
    if results["storage"]["status"] != "success":
        flow_logger.error("Storage setup failed, aborting flow")
        return results

    if run_news:
        urls = feed_urls or EXAMPLE_FEEDS
        results["news"] = collect_news_task(urls)

    if run_reports:
        urls = report_page_urls or EXAMPLE_REPORT_PAGES
        results["reports"] = collect_reports_task(urls, use_playwright, download_limit)

    flow_logger.info("Scraping flow complete")
    return results


@flow(
    name="finance-analytics-flow",
    description="Full pipeline: scrape → parse → analyze → summarize",
    version="1.0.0",
)
def finance_analytics_flow(
    ticker: str = "AAPL",
    period: str = "Q3-2025",
    feed_urls: Optional[list[str]] = None,
    report_page_urls: Optional[list[str]] = None,
    use_playwright: bool = False,
    days: int = 90,
) -> dict:
    """
    Full finance analytics pipeline.
    Steps: scrape → parse → analyze (scoring + sentiment) → market → summarize.
    """
    flow_logger = get_run_logger()
    flow_logger.info(f"Starting finance analytics flow for {ticker} ({period})")

    results = {}

    # Step 1: Scrape
    flow_logger.info("Step 1: Scraping data")
    results["scrape"] = scraping_flow(
        feed_urls=feed_urls,
        report_page_urls=report_page_urls,
        use_playwright=use_playwright,
    )

    # Step 2: Parse
    flow_logger.info("Step 2: Parsing reports")
    results["parse"] = parse_reports_task(ticker, period)

    # Step 3: Fetch market data
    flow_logger.info("Step 3: Fetching market data")
    results["market"] = fetch_market_task(ticker, days)

    # Step 4: Analyze
    flow_logger.info("Step 4: Running analysis")
    results["financial_scoring"] = analyze_financials_task(ticker, period)
    results["sentiment"] = analyze_sentiment_task(ticker)

    # Step 5: Summarize
    flow_logger.info("Step 5: Generating summary")
    results["summary"] = generate_summary_task(ticker, period)

    flow_logger.info("Finance analytics flow complete")
    return results


@flow(name="news-only-flow", description="Flow for RSS news collection only")
def news_only_flow(feed_urls: Optional[list[str]] = None) -> dict:
    """Run only news collection."""
    return scraping_flow(feed_urls=feed_urls, run_news=True, run_reports=False)


@flow(name="reports-only-flow", description="Flow for company reports collection only")
def reports_only_flow(
    report_page_urls: Optional[list[str]] = None,
    use_playwright: bool = False,
    download_limit: Optional[int] = 10,
) -> dict:
    """Run only report collection."""
    return scraping_flow(
        report_page_urls=report_page_urls,
        use_playwright=use_playwright,
        download_limit=download_limit,
        run_news=False,
        run_reports=True,
    )


def run_flow(
    flow_type: str = "all",
    feed_urls: Optional[list[str]] = None,
    report_page_urls: Optional[list[str]] = None,
    use_playwright: bool = False,
) -> dict:
    """
    Run the appropriate flow based on type.

    Args:
        flow_type: 'all', 'news', or 'reports'
    """
    if flow_type == "news":
        return news_only_flow(feed_urls=feed_urls)
    elif flow_type == "reports":
        return reports_only_flow(
            report_page_urls=report_page_urls,
            use_playwright=use_playwright,
        )
    else:
        return scraping_flow(
            feed_urls=feed_urls,
            report_page_urls=report_page_urls,
            use_playwright=use_playwright,
        )


if __name__ == "__main__":
    result = finance_analytics_flow()
    print(f"Flow result: {result}")
