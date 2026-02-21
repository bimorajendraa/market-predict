"""
Main CLI entry point for Finance Analytics.
Provides command-line interface for scraping, parsing, analysis, and summary generation.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .config import config
from .collectors.news_rss import scrape_rss, EXAMPLE_FEEDS
from .collectors.company_reports import crawl_reports, EXAMPLE_REPORT_PAGES
from .pipelines.prefect_flow import run_flow, scraping_flow
from .storage import ensure_bucket_exists

# Setup rich console
console = Console()

# Setup logging
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)

logger = logging.getLogger(__name__)

# Suppress yfinance logging noise
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
try:
    from pandas.errors import Pandas4Warning
    warnings.simplefilter(action='ignore', category=Pandas4Warning)
except ImportError:
    pass


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.version_option(version="0.2.0", prog_name="finance-analytics")
def cli(verbose: bool):
    """
    Finance Analytics — Scraping, Parsing, Scoring & Summary Pipeline

    Collect news and reports, parse financials, analyze sentiment,
    fetch market data, and generate company summaries.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


# ============================================
# Scraping Commands (Original)
# ============================================

@cli.command("run-news")
@click.option(
    "--feeds", "-f",
    multiple=True,
    help="RSS feed URLs to scrape (can specify multiple)",
)
@click.option(
    "--file", "-F",
    type=click.Path(exists=True),
    help="JSON file containing list of feed URLs",
)
@click.option(
    "--ticker", "-t",
    default=None,
    help="Stock ticker — auto-generates feeds from Yahoo Finance, CNBC ID, etc.",
)
def run_news(feeds: tuple, file: Optional[str], ticker: Optional[str]):
    """
    Run RSS news collection.

    Examples:
        python -m src.main run-news
        python -m src.main run-news --ticker BBCA.JK
        python -m src.main run-news -f https://example.com/feed.xml
        python -m src.main run-news -F feeds.json
    """
    console.print("[bold blue]Starting RSS News Collection[/bold blue]")

    # Build feed list
    feed_urls = list(feeds)

    if file:
        try:
            with open(file) as f:
                file_feeds = json.load(f)
                if isinstance(file_feeds, list):
                    feed_urls.extend(file_feeds)
                elif isinstance(file_feeds, dict) and "feeds" in file_feeds:
                    feed_urls.extend(file_feeds["feeds"])
        except Exception as e:
            console.print(f"[red]Error reading feed file: {e}[/red]")
            sys.exit(1)

    # Auto-generate feeds for ticker if provided
    if ticker and not feed_urls:
        from .collectors.news_rss import get_feeds_for_ticker
        feed_urls = get_feeds_for_ticker(ticker)
        console.print(f"[cyan]Auto-generated {len(feed_urls)} feeds for {ticker}[/cyan]")
    elif not feed_urls:
        console.print("[yellow]No feeds specified, using default feeds[/yellow]")
        feed_urls = EXAMPLE_FEEDS

    console.print(f"Processing {len(feed_urls)} RSS feeds...")
    if ticker:
        console.print(f"[cyan]Assigning ticker: {ticker}[/cyan]")

    try:
        # Ensure bucket exists
        ensure_bucket_exists()

        # Run collection (pass ticker for assignment)
        inserted_ids = scrape_rss(feed_urls, ticker=ticker)

        # Display results
        table = Table(title="Collection Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Feeds Processed", str(len(feed_urls)))
        table.add_row("Items Inserted", str(len(inserted_ids)))
        if ticker:
            table.add_row("Ticker", ticker)

        console.print(table)
        console.print("[bold green]News collection complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("News collection failed")
        sys.exit(1)


@cli.command("run-reports")
@click.option(
    "--pages", "-p",
    multiple=True,
    help="Page URLs to crawl for reports (can specify multiple)",
)
@click.option(
    "--file", "-F",
    type=click.Path(exists=True),
    help="JSON file containing list of page URLs",
)
@click.option(
    "--playwright", "-P",
    is_flag=True,
    help="Use Playwright for JS-rendered pages",
)
@click.option(
    "--limit", "-l",
    type=int,
    default=10,
    help="Maximum reports to download per page",
)
def run_reports(pages: tuple, file: Optional[str], playwright: bool, limit: int):
    """
    Run company reports collection.

    Examples:
        python -m src.main run-reports
        python -m src.main run-reports -p https://company.com/ir
        python -m src.main run-reports -P --limit 5
    """
    console.print("[bold blue]Starting Company Reports Collection[/bold blue]")

    # Build page list
    page_urls = list(pages)

    if file:
        try:
            with open(file) as f:
                file_pages = json.load(f)
                if isinstance(file_pages, list):
                    page_urls.extend(file_pages)
                elif isinstance(file_pages, dict) and "pages" in file_pages:
                    page_urls.extend(file_pages["pages"])
        except Exception as e:
            console.print(f"[red]Error reading page file: {e}[/red]")
            sys.exit(1)

    # Use example pages if none provided
    if not page_urls:
        console.print("[yellow]No pages specified, using example pages[/yellow]")
        page_urls = EXAMPLE_REPORT_PAGES

    console.print(f"Crawling {len(page_urls)} pages for reports...")
    if playwright:
        console.print("[cyan]Using Playwright for JS rendering[/cyan]")

    try:
        # Ensure bucket exists
        ensure_bucket_exists()

        # Run collection
        job_ids = crawl_reports(page_urls, use_playwright=playwright, download_limit=limit)

        # Display results
        table = Table(title="Collection Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Pages Crawled", str(len(page_urls)))
        table.add_row("Reports Processed", str(len(job_ids)))
        table.add_row("Download Limit", str(limit))

        console.print(table)
        console.print("[bold green]Report collection complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Report collection failed")
        sys.exit(1)


# ============================================
# New Analytics Commands
# ============================================

@cli.command("run-parse")
@click.option("--ticker", "-t", required=True, help="Stock ticker symbol")
@click.option("--period", "-p", default=None, help="Reporting period (e.g., Q3-2025)")
def run_parse(ticker: str, period: Optional[str]):
    """
    Parse downloaded reports and extract financial metrics.

    Parses HTML/PDF reports from MinIO storage into the financial_facts table.

    Examples:
        python -m src.main run-parse --ticker AAPL
        python -m src.main run-parse --ticker BBCA.JK --period Q3-2025
    """
    from .parsers.html_parser import parse_html_report
    from .parsers.pdf_parser import parse_pdf_bytes
    from .parsers.period_detector import detect_period
    from .storage import download_raw
    from .db import get_fetch_jobs_by_status, insert_financial_fact

    console.print(f"[bold blue]Parsing Reports for {ticker}[/bold blue]")

    try:
        # Get completed fetch jobs for this ticker
        from .db import get_db_cursor
        with get_db_cursor() as cursor:
            cursor.execute(
                """
                SELECT id, url, raw_object_key, doc_type
                FROM fetch_jobs
                WHERE status = 'success'
                  AND raw_object_key IS NOT NULL
                  AND (ticker = %(ticker)s OR ticker IS NULL)
                ORDER BY fetched_at DESC
                LIMIT 50
                """,
                {"ticker": ticker},
            )
            jobs = cursor.fetchall()

        if not jobs:
            console.print("[yellow]No completed fetch jobs found to parse[/yellow]")
            return

        console.print(f"Found {len(jobs)} reports to parse")
        total_facts = 0

        for job in jobs:
            obj_key = job["raw_object_key"]
            source_url = job.get("url", "")
            report_period = period  # None triggers auto-detection in parsers

            # If no period provided, also try detecting from source URL
            if not report_period:
                report_period = detect_period(source_url, fallback=None)

            try:
                raw_data = download_raw(obj_key)

                if obj_key.endswith(".pdf"):
                    facts = parse_pdf_bytes(raw_data, ticker, report_period, source_url)
                else:
                    html_content = raw_data.decode("utf-8", errors="replace")
                    facts = parse_html_report(html_content, ticker, report_period, source_url)

                for fact in facts:
                    insert_financial_fact(
                        ticker=fact["ticker"],
                        period=fact["period"],
                        metric=fact["metric"],
                        value=fact["value"],
                        unit=fact.get("unit"),
                        currency=fact.get("currency"),
                        source_url=fact.get("source_url"),
                    )
                    total_facts += 1

                console.print(f"  ✓ {obj_key}: {len(facts)} metrics extracted")

            except Exception as e:
                console.print(f"  ✗ {obj_key}: {e}")
                logger.warning(f"Failed to parse {obj_key}: {e}")

        # Display results
        table = Table(title="Parse Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Reports Processed", str(len(jobs)))
        table.add_row("Facts Extracted", str(total_facts))
        console.print(table)
        console.print("[bold green]Parsing complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Parsing failed")
        sys.exit(1)


@cli.command("run-market")
@click.option("--ticker", "-t", required=True, help="Stock ticker (e.g., AAPL, BBCA.JK)")
@click.option("--days", "-d", default=90, help="Number of days of history (default: 90)")
def run_market(ticker: str, days: int):
    """
    Fetch daily OHLCV prices from Yahoo Finance.

    Examples:
        python -m src.main run-market --ticker AAPL
        python -m src.main run-market --ticker BBCA.JK --days 30
    """
    from .market.price_fetcher import run_market_fetch

    console.print(f"[bold blue]Fetching Market Prices for {ticker}[/bold blue]")

    try:
        result = run_market_fetch(ticker, days)

        table = Table(title="Market Data Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Ticker", result["ticker"])
        table.add_row("Days Requested", str(result["days_requested"]))
        table.add_row("Records Fetched", str(result["records_fetched"]))
        table.add_row("Records Upserted", str(result["records_upserted"]))
        console.print(table)
        console.print("[bold green]Market data fetch complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Market data fetch failed")
        sys.exit(1)


@cli.command("run-analyze")
@click.option("--ticker", "-t", required=True, help="Stock ticker symbol")
@click.option("--period", "-p", required=True, help="Reporting period (e.g., Q3-2025)")
def run_analyze(ticker: str, period: str):
    """
    Run financial scoring and news sentiment analysis.

    Examples:
        python -m src.main run-analyze --ticker AAPL --period Q3-2025
        python -m src.main run-analyze --ticker BBCA.JK --period Q3-2025
    """
    from .analysis.financial_scoring import run_financial_scoring
    from .analysis.news_sentiment import run_news_sentiment
    from .db import insert_financial_score, insert_news_sentiment

    console.print(f"[bold blue]Running Analysis for {ticker} ({period})[/bold blue]")

    try:
        # Financial scoring
        console.print("\n[cyan]Step 1: Financial Scoring[/cyan]")
        score_result = run_financial_scoring(ticker, period)
        score = score_result["score"]
        drivers = score_result["drivers"]

        if drivers:
            score_id = insert_financial_score(ticker, period, score, drivers)
            console.print(f"  Score: [bold]{score:.1f}/100[/bold] (id: {score_id})")
        else:
            console.print("  [yellow]No financial data available for scoring[/yellow]")

        # Display top drivers
        if drivers:
            driver_table = Table(title="Top Financial Drivers")
            driver_table.add_column("Metric", style="cyan")
            driver_table.add_column("Value", style="yellow")
            driver_table.add_column("Sub-Score", style="green")
            driver_table.add_column("Contribution", style="magenta")

            for d in drivers[:5]:
                val = f"{d['value']:.4f}" if d['value'] is not None else "N/A"
                driver_table.add_row(
                    d["metric"], val,
                    f"{d['sub_score']:.1f}", f"{d['contribution']:.2f}",
                )
            console.print(driver_table)

        # News sentiment
        console.print("\n[cyan]Step 2: News Sentiment Analysis[/cyan]")
        sentiment_results = run_news_sentiment(ticker)

        for sr in sentiment_results:
            sid = insert_news_sentiment(
                ticker=sr["ticker"],
                date=sr["date"],
                headline=sr["headline"],
                sentiment=sr["sentiment"],
                impact=sr["impact"],
                events_json=sr["events_json"],
                sources_json=sr["sources_json"],
            )

        if sentiment_results:
            pos = sum(1 for r in sentiment_results if r["sentiment"] == "positive")
            neg = sum(1 for r in sentiment_results if r["sentiment"] == "negative")
            neu = sum(1 for r in sentiment_results if r["sentiment"] == "neutral")
            console.print(
                f"  Analyzed {len(sentiment_results)} news: "
                f"[green]{pos} positive[/green], "
                f"[red]{neg} negative[/red], "
                f"{neu} neutral"
            )
        else:
            console.print("  [yellow]No unanalyzed news found[/yellow]")

        console.print("\n[bold green]Analysis complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Analysis failed")
        sys.exit(1)


@cli.command("run-summary")
@click.option("--ticker", "-t", required=True, help="Stock ticker symbol")
@click.option("--period", "-p", required=True, help="Reporting period (e.g., Q3-2025)")
def run_summary(ticker: str, period: str):
    """
    Generate a narrative company summary.

    Combines financial drivers, news events, and market returns
    into an explainable summary with rating.

    Examples:
        python -m src.main run-summary --ticker AAPL --period Q3-2025
    """
    from .summary.generator import run_summary_generation

    console.print(f"[bold blue]Generating Summary for {ticker} ({period})[/bold blue]")

    try:
        result = run_summary_generation(ticker, period)

        # Display results
        console.print(f"\n[bold]Rating: {result['rating']}[/bold]")
        console.print(f"\n{result['narrative']}")

        table = Table(title="Summary Metadata")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Ticker", result["ticker"])
        table.add_row("Period", result["period"])
        table.add_row("Rating", result["rating"])

        evidence = result.get("evidence_json", {})
        table.add_row("Financial Drivers", str(len(evidence.get("financial_drivers", []))))
        table.add_row("News Events", str(len(evidence.get("news_events", []))))

        mr = evidence.get("market_returns", {})
        r7 = mr.get("7d")
        r30 = mr.get("30d")
        table.add_row("7d Return", f"{r7:+.1%}" if r7 is not None else "N/A")
        table.add_row("30d Return", f"{r30:+.1%}" if r30 is not None else "N/A")

        console.print(table)
        console.print("[bold green]Summary generation complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Summary generation failed")
        sys.exit(1)


# ============================================
# Full Pipeline Command
# ============================================

@cli.command("run-pipeline")
@click.option("--ticker", "-t", required=True, help="Stock ticker (e.g., BBCA.JK, AAPL)")
@click.option("--period", "-p", default=None, help="Reporting period (auto-detected if omitted)")
@click.option("--days", "-d", default=90, help="Market data history in days (default: 90)")
@click.option(
    "--playwright", "-P",
    is_flag=True,
    help="Use Playwright for JS-rendered IR pages",
)
@click.option(
    "--ir-pages",
    default=None,
    help="Comma-separated list of IR page URLs to use instead of auto-discovery",
)
def run_pipeline(ticker: str, period: Optional[str], days: int, playwright: bool, ir_pages: Optional[str]):
    """
    Run the FULL pipeline end-to-end for a single ticker.

    Automatically: scrape news -> discover & download reports ->
    parse -> analyze sentiment -> financial scoring ->
    fetch market data -> generate summary.

    Examples:
        python -m src.main run-pipeline --ticker BBCA.JK
        python -m src.main run-pipeline --ticker AAPL --period Q3-2025
        python -m src.main run-pipeline --ticker TLKM.JK -P
    """
    from .collectors.news_rss import get_feeds_for_ticker, scrape_rss
    from .collectors.company_reports import discover_ir_pages, crawl_reports
    from .collectors.yfinance_fundamentals import fetch_fundamentals
    from .db import (
        get_db_cursor, insert_financial_fact,
        insert_financial_score, insert_news_sentiment,
    )
    from .market.price_fetcher import run_market_fetch
    from .analysis.financial_scoring import run_financial_scoring
    from .analysis.news_sentiment import run_news_sentiment
    from .analysis.technical_analysis import run_technical_analysis
    from .summary.generator import run_summary_generation

    console.print(f"\n[bold magenta]{'='*60}[/bold magenta]")
    console.print(f"[bold magenta]  FINANCE PIPELINE -- {ticker}[/bold magenta]")
    console.print(f"[bold magenta]{'='*60}[/bold magenta]\n")

    # ── Cleanup: fresh start for this ticker ─────────────────────
    console.print("[dim]Cleaning old pipeline data for fresh analysis...[/dim]")
    try:
        with get_db_cursor() as cur:
            cleanup_tables = [
                "news_items", "news_sentiment", "scores_financial",
                "company_summary", "financial_facts", "market_prices",
            ]
            for table in cleanup_tables:
                cur.execute(
                    f"DELETE FROM {table} WHERE ticker = %(ticker)s",
                    {"ticker": ticker},
                )
                deleted = cur.rowcount
                if deleted > 0:
                    console.print(f"[dim]  {table}: {deleted} rows deleted[/dim]")
        console.print("[dim]  Cleanup complete.[/dim]\n")
    except Exception as e:
        console.print(f"[yellow]  Cleanup warning: {e}[/yellow]\n")

    results = {}

    # ── Step 1: Scrape News ──────────────────────────────────────
    console.print("[bold cyan]>> Step 1/8: Scraping News[/bold cyan]")
    try:
        ensure_bucket_exists()
        feed_urls = get_feeds_for_ticker(ticker)
        console.print(f"  Auto-generated {len(feed_urls)} feeds")
        inserted_ids = scrape_rss(feed_urls, ticker=ticker)
        results["news"] = {"status": "success", "items": len(inserted_ids)}
        console.print(f"  [green][OK] Collected {len(inserted_ids)} news items[/green]")

        # Show news metrics: items in DB (last 14d) vs items inserted this run
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) as c FROM news_items WHERE ticker = %(t)s AND created_at >= NOW() - INTERVAL '14 days'",
                    {"t": ticker},
                )
                news_in_db_14d = cur.fetchone()["c"]
            console.print(f"  [dim]  news_items_in_db_last_14d: {news_in_db_14d}  |  inserted_this_run: {len(inserted_ids)}[/dim]")
            results["news"]["items_in_db_14d"] = news_in_db_14d
            results["news"]["items_inserted"] = len(inserted_ids)
        except Exception:
            pass

        if len(inserted_ids) < 10:
            console.print(f"  [yellow][!] Less than 10 items -- some feeds may be unavailable[/yellow]")
    except Exception as e:
        results["news"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] News scraping failed: {e}[/red]")

    # ── Step 2: Discover & Download Reports ──────────────────────
    console.print("\n[bold cyan]>> Step 2/8: Discovering & Downloading Reports[/bold cyan]")
    try:
        if ir_pages:
            ir_page_list = [url.strip() for url in ir_pages.split(",") if url.strip()]
            console.print(f"  Using {len(ir_page_list)} manually specified IR pages")
        else:
            ir_page_list = discover_ir_pages(ticker)
        if ir_page_list:
            console.print(f"  Found {len(ir_page_list)} IR pages:")
            for p in ir_page_list[:5]:
                console.print(f"    - {p}")
            job_ids = crawl_reports(ir_page_list, use_playwright=playwright, download_limit=10)
            results["reports"] = {"status": "success", "jobs": len(job_ids), "pages": len(ir_page_list)}
            console.print(f"  [green][OK] Downloaded {len(job_ids)} reports[/green]")
        else:
            results["reports"] = {"status": "skipped", "reason": "No IR pages found", "jobs": 0}
            console.print(f"  [yellow][!] No IR pages found for {ticker} -- skipping[/yellow]")

        # SEC EDGAR fallback for US tickers
        reports_downloaded = results.get("reports", {}).get("jobs", 0)
        is_us_ticker = not ticker.upper().endswith(".JK")

        if is_us_ticker and reports_downloaded == 0:
            console.print("\n  [bold cyan]>> Step 2b: SEC EDGAR Fallback[/bold cyan]")
            try:
                from .collectors.sec_edgar import collect_sec_filings
                sec_result = collect_sec_filings(ticker, max_downloads=5)
                sec_downloaded = sec_result.get("downloaded", 0)
                sec_status = sec_result.get("primary_source_status", "NOT_FOUND")
                results["sec_edgar"] = sec_result

                if sec_downloaded > 0:
                    results["reports"]["jobs"] = results["reports"].get("jobs", 0) + sec_downloaded
                    results["reports"]["sec_filings"] = sec_downloaded
                    console.print(f"  [green][OK] SEC EDGAR: {sec_downloaded} filings downloaded[/green]")
                else:
                    console.print(f"  [yellow][!] SEC EDGAR: status={sec_status}, 0 filings downloaded[/yellow]")
            except Exception as e:
                results["sec_edgar"] = {"status": "failed", "error": str(e)}
                console.print(f"  [yellow][!] SEC EDGAR fallback failed: {e}[/yellow]")

    except Exception as e:
        results["reports"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Report discovery failed: {e}[/red]")

    # ── Step 3: Fetch Financial Data (yfinance) ──────────────────
    console.print("\n[bold cyan]>> Step 3/8: Fetching Financial Data (yfinance)[/bold cyan]")
    total_facts = 0
    detected_period = period
    try:
        facts = fetch_fundamentals(ticker)
        for fact in facts:
            insert_financial_fact(
                ticker=fact["ticker"], period=fact["period"],
                metric=fact["metric"], value=fact["value"],
                unit=fact.get("unit"), currency=fact.get("currency"),
                source_url=fact.get("source_url"),
            )
            total_facts += 1

        # Auto-detect period from most recent quarterly data
        if not detected_period and facts:
            quarterly_periods = sorted(
                set(f["period"] for f in facts if f["period"].startswith("Q")),
                reverse=True,
            )
            if quarterly_periods:
                detected_period = quarterly_periods[0]
                console.print(f"  Period detected from yfinance: {detected_period}")

        results["financials"] = {"status": "success", "facts": total_facts}
        console.print(f"  [green][OK] {total_facts} financial facts from yfinance[/green]")
    except Exception as e:
        results["financials"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Financial data fetch failed: {e}[/red]")

    # Fallback period if still not detected
    if not detected_period:
        from datetime import datetime
        now = datetime.now()
        q = (now.month - 1) // 3 + 1
        detected_period = f"Q{q}-{now.year}"
        console.print(f"  [yellow]Period auto-set to {detected_period}[/yellow]")

    # ── Step 4: Sentiment Analysis ───────────────────────────────
    console.print("\n[bold cyan]>> Step 4/8: Analyzing News Sentiment[/bold cyan]")
    console.print("  Filtering to company-relevant news only (14-day lookback)...")
    try:
        sentiment_results = run_news_sentiment(ticker)
        for sr in sentiment_results:
            insert_news_sentiment(
                ticker=sr["ticker"], date=sr["date"], headline=sr["headline"],
                sentiment=sr["sentiment"], impact=sr["impact"],
                events_json=sr["events_json"], sources_json=sr["sources_json"],
            )
        if sentiment_results:
            pos = sum(1 for r in sentiment_results if r["sentiment"] == "positive")
            neg = sum(1 for r in sentiment_results if r["sentiment"] == "negative")
            neu = sum(1 for r in sentiment_results if r["sentiment"] == "neutral")
            results["sentiment"] = {
                "status": "success", "total": len(sentiment_results),
                "positive": pos, "negative": neg, "neutral": neu,
            }
            console.print(
                f"  [green][OK] {len(sentiment_results)} company-relevant items:[/green] "
                f"[green]{pos} pos[/green], [red]{neg} neg[/red], {neu} neutral"
            )
        else:
            results["sentiment"] = {"status": "skipped", "reason": "No relevant news"}
            console.print(f"  [yellow][!] No company-relevant news found[/yellow]")
    except Exception as e:
        results["sentiment"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Sentiment analysis failed: {e}[/red]")

    # ── Step 5: Financial Scoring ────────────────────────────────
    console.print(f"\n[bold cyan]>> Step 5/8: Financial Scoring ({detected_period})[/bold cyan]")
    try:
        score_result = run_financial_scoring(ticker, detected_period)
        score = score_result["score"]
        drivers = score_result["drivers"]
        explanation = score_result.get("explanation", "")
        coverage_factor = score_result.get("coverage_factor", 1.0)
        if drivers:
            insert_financial_score(ticker, detected_period, score, drivers)
            results["scoring"] = {
                "status": "success", "score": score,
                "coverage_factor": coverage_factor,
            }
            console.print(f"  [green][OK] Score: {score:.1f}/100  (Coverage: {coverage_factor:.0%})[/green]")
            # Show detailed breakdown
            if explanation:
                console.print(f"\n[dim]{explanation}[/dim]")
        else:
            results["scoring"] = {"status": "skipped", "reason": "No financial data", "coverage_factor": 0.0}
            console.print(f"  [yellow][!] No financial data for scoring[/yellow]")
    except Exception as e:
        results["scoring"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Scoring failed: {e}[/red]")

    # ── Step 6: Market Prices ────────────────────────────────────
    console.print(f"\n[bold cyan]>> Step 6/8: Fetching Market Prices ({days}d)[/bold cyan]")
    try:
        market_result = run_market_fetch(ticker, days)
        results["market"] = {"status": "success", **market_result}
        console.print(f"  [green][OK] {market_result['records_fetched']} price records[/green]")
    except Exception as e:
        results["market"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Market fetch failed: {e}[/red]")

    # ── Step 6.5: Technical Analysis ────────────────────────────
    console.print(f"\n[bold cyan]>> Step 6.5/8: Technical Analysis[/bold cyan]")
    tech_levels = {}
    try:
        tech_levels = run_technical_analysis(ticker)
        if tech_levels.get("status") == "ok":
            results["technical"] = {"status": "success"}
            console.print(f"  [green][OK] Current Price: {tech_levels['current_price']}[/green]")
            if tech_levels.get("buy_zone"):
                bz = tech_levels["buy_zone"]
                console.print(f"       Buy Zone:  {bz['range_low']} - {bz['range_high']}")
            if tech_levels.get("sell_zone"):
                sz = tech_levels["sell_zone"]
                console.print(f"       Sell Zone: {sz['range_low']} - {sz['range_high']}")
            if tech_levels.get("support"):
                console.print(f"       Support:   {tech_levels['support']}")
            if tech_levels.get("resistance"):
                console.print(f"       Resistance:{tech_levels['resistance']}")
        else:
            results["technical"] = {"status": "skipped", "reason": "Insufficient data"}
            console.print("  [yellow][!] Insufficient data for technical analysis[/yellow]")
    except Exception as e:
        results["technical"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Technical analysis failed: {e}[/red]")

    # ── Step 6.6: Sector Scoring & Risk Flags ──────────────────────
    console.print(f"\n[bold cyan]>> Step 6.6: Sector-Aware Scoring[/bold cyan]")
    try:
        from .analysis.sector_scoring import compute_sector_score, detect_sector
        sector = detect_sector(ticker)
        base_score = results.get("scoring", {}).get("score", 0.0)
        scoring_drivers = []
        # Grab drivers from DB if scoring was done
        if results.get("scoring", {}).get("status") == "success":
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT drivers_json FROM scores_financial WHERE ticker=%(t)s AND period=%(p)s ORDER BY created_at DESC LIMIT 1",
                    {"t": ticker, "p": detected_period},
                )
                row = cur.fetchone()
                if row:
                    import json as _json
                    drivers_raw = row.get("drivers_json", [])
                    if isinstance(drivers_raw, str):
                        scoring_drivers = _json.loads(drivers_raw)
                    else:
                        scoring_drivers = drivers_raw

        sector_result = compute_sector_score(ticker, base_score, scoring_drivers)
        results["sector_scoring"] = {"status": "success", **sector_result}
        console.print(f"  [green][OK] Sector: {sector} | Score: {sector_result['sector_adjusted_score']:.1f} (base: {base_score:.1f}, risk penalty: -{sector_result['risk_penalty']:.1f})[/green]")
        if sector_result.get("risk_flags"):
            for flag in sector_result["risk_flags"]:
                console.print(f"  [yellow]  ⚠ {flag['message']} ({flag['metric']}={flag['value']})[/yellow]")
    except Exception as e:
        results["sector_scoring"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Sector scoring failed: {e}[/red]")

    # ── Step 6.7: Valuation Analysis ──────────────────────────────
    console.print(f"\n[bold cyan]>> Step 6.7: Valuation Analysis[/bold cyan]")
    valuation_result = {}
    try:
        from .analysis.valuation import run_valuation_analysis
        valuation_result = run_valuation_analysis(ticker)
        if valuation_result.get("status") == "ok":
            results["valuation"] = {"status": "success", "verdict": valuation_result.get("verdict")}
            console.print(f"  [green][OK] Verdict: {valuation_result['verdict'].upper()} — {valuation_result.get('explanation', '')}[/green]")
            for key, comp in valuation_result.get("comparisons", {}).items():
                console.print(f"  [dim]  {key}: {comp.get('value')} vs sector {comp.get('sector_median')} ({comp.get('assessment')})[/dim]")
        else:
            results["valuation"] = {"status": "skipped", "reason": valuation_result.get("error", "Insufficient data")}
            console.print(f"  [yellow][!] Valuation: {valuation_result.get('error', 'Insufficient data')}[/yellow]")
    except Exception as e:
        results["valuation"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Valuation analysis failed: {e}[/red]")

    # ── Step 7: AI Model Training & Prediction ───────────────────
    console.print(f"\n[bold cyan]>> Step 7/8: AI Stock Prediction[/bold cyan]")
    ml_pred = {}
    try:
        from .analysis.model_predictor import predict_latest, load_model
        from .analysis.model_trainer import train_model

        # Auto-train if no model exists
        existing = load_model(ticker)
        if not existing:
            console.print("  [cyan]No model found — training on historical data...[/cyan]")
            train_result = train_model(ticker)
            if train_result["status"] == "success":
                console.print(f"  [green]  Model trained! Accuracy: {train_result['accuracy']:.2%}[/green]")
                console.print(f"  Top features:")
                for feat in train_result.get("top_features", [])[:5]:
                    console.print(f"    - {feat['Feature']}: {feat['Gain']:.1f}")
            else:
                console.print(f"  [yellow]  Training failed: {train_result.get('reason')}[/yellow]")

        ml_pred = predict_latest(ticker)
        if ml_pred.get("signal") and ml_pred["signal"] != "Unknown":
            console.print(f"  [green][OK] Signal: {ml_pred['signal']} (Conf: {ml_pred.get('confidence', 0):.2f})[/green]")
            if ml_pred.get("stop_loss"):
                console.print(f"       Stop Loss: {ml_pred['stop_loss']}")
        else:
            console.print(f"  [yellow][!] Prediction: {ml_pred.get('signal', 'N/A')} — {ml_pred.get('reason', '')}[/yellow]")
        results["ml_prediction"] = ml_pred
    except Exception as e:
        console.print(f"  [red][FAIL] Prediction failed: {e}[/red]")
        results["ml_prediction"] = {"error": str(e)}

    # ── Step 8: Generate Summary ─────────────────────────────────
    console.print(f"\n[bold cyan]>> Step 8/8: Generating Summary[/bold cyan]")
    try:
        summary = run_summary_generation(
            ticker, detected_period,
            pipeline_results=results,
            technical_levels=tech_levels,
        )
        results["summary"] = {"status": "success", "rating": summary["rating"]}
        console.print(f"  [green][OK] Rating: [bold]{summary['rating']}[/bold] | Confidence: {summary.get('confidence', 'N/A')}[/green]")
        console.print(f"\n[dim]{'-'*60}[/dim]")
        console.print(f"\n{summary['narrative']}")
    except Exception as e:
        results["summary"] = {"status": "failed", "error": str(e)}
        console.print(f"  [red][FAIL] Summary failed: {e}[/red]")

    # ── Final Report ─────────────────────────────────────────────
    console.print(f"\n[bold magenta]{'='*60}[/bold magenta]")
    console.print(f"[bold magenta]  PIPELINE COMPLETE -- {ticker}[/bold magenta]")
    console.print(f"[bold magenta]{'='*60}[/bold magenta]\n")

    table = Table(title=f"Pipeline Results -- {ticker}")
    table.add_column("Step", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details", style="dim")

    status_icons = {"success": "[green]OK[/green]", "failed": "[red]FAIL[/red]", "skipped": "[yellow]SKIP[/yellow]"}

    for step_name, step_result in results.items():
        status = step_result.get("status", "unknown")
        icon = status_icons.get(status, "?")
        # Build detail string
        detail_parts = [f"{k}={v}" for k, v in step_result.items() if k != "status"]
        detail = ", ".join(detail_parts[:3])
        table.add_row(step_name.title(), f"{icon} {status}", detail)

    console.print(table)


# ============================================
# Flow & Utility Commands
# ============================================

@cli.command("train-model")
@click.option("--ticker", required=True, help="Ticker to train model for")
def train_model_cmd(ticker: str):
    """
    Train ML model for a specific ticker.
    Downloads data, engineers features, and saves LightGBM model.
    """
    try:
        from .analysis.model_trainer import train_model
        result = train_model(ticker)
        if result["status"] == "success":
            console.print(f"[green]Model trained successfully![/green]")
            console.print(f"  Path: {result['path']}")
            console.print(f"  Accuracy (CV): {result['accuracy']:.2%}")
            console.print("  Top Features:")
            for feat in result['top_features'][:5]:
                console.print(f"    - {feat['Feature']}: {feat['Gain']:.2f}")
        else:
            console.print(f"[red]Training failed: {result.get('reason')}[/red]")
            
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Training failed")
        sys.exit(1)


@cli.command("run-flow")
@click.option(
    "--type", "-t",
    "flow_type",
    type=click.Choice(["all", "news", "reports"]),
    default="all",
    help="Type of flow to run",
)
@click.option(
    "--playwright", "-P",
    is_flag=True,
    help="Use Playwright for JS-rendered pages",
)
def run_flow_cmd(flow_type: str, playwright: bool):
    """
    Run Prefect flow for orchestrated pipeline.

    Examples:
        python -m src.main run-flow
        python -m src.main run-flow --type news
        python -m src.main run-flow --type reports -P
    """
    console.print(f"[bold blue]Running Prefect Flow: {flow_type}[/bold blue]")

    try:
        result = run_flow(
            flow_type=flow_type,
            use_playwright=playwright,
        )

        console.print("\n[bold]Flow Results:[/bold]")
        console.print_json(json.dumps(result, indent=2, default=str))
        console.print("[bold green]Flow execution complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Flow execution failed")
        sys.exit(1)


@cli.command("init-storage")
def init_storage():
    """Initialize MinIO bucket."""
    console.print("[bold blue]Initializing MinIO Storage[/bold blue]")

    try:
        ensure_bucket_exists()
        console.print(f"[green]Bucket '{config.MINIO_BUCKET}' is ready[/green]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)


@cli.command("check-config")
def check_config():
    """Display current configuration."""
    table = Table(title="Current Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="yellow")

    table.add_row("Postgres Host", config.POSTGRES_HOST)
    table.add_row("Postgres Port", str(config.POSTGRES_PORT))
    table.add_row("Postgres DB", config.POSTGRES_DB)
    table.add_row("MinIO Endpoint", config.MINIO_ENDPOINT)
    table.add_row("MinIO Bucket", config.MINIO_BUCKET)
    table.add_row("Rate Limit", f"{config.RATE_LIMIT_MIN}-{config.RATE_LIMIT_MAX}s")
    table.add_row("Max Retries", str(config.MAX_RETRIES))
    table.add_row("Log Level", config.LOG_LEVEL)

    # Show scoring weights
    table.add_row("─── Scoring Weights ───", "───")
    for metric, weight in config.SCORING_WEIGHTS.items():
        table.add_row(f"  {metric}", f"{weight:.2f}")

    console.print(table)


# ============================================
# Institutional Research Commands
# ============================================

@cli.command("run-memo")
@click.option("--ticker", "-t", required=True, help="Stock ticker (e.g., ORCL, BBCA.JK)")
@click.option("--period", "-p", default=None, help="Reporting period (e.g., Q4-2025)")
@click.option("--days", "-d", default=90, help="Market data history in days")
@click.option("--output", "-o", default=None, help="Output directory for memo file")
def run_memo(ticker: str, period: Optional[str], days: int, output: Optional[str]):
    """Generate institutional investment memo for a ticker."""
    console.print(f"\n[bold blue]═══ Investment Memo: {ticker} ═══[/bold blue]\n")

    try:
        from .summary.memo_generator import run_memo_generation
        from .pipelines.audit import AuditTracker

        # Auto-detect period if not specified
        if not period:
            from datetime import datetime as _dt
            now = _dt.now()
            quarter = (now.month - 1) // 3 + 1
            period = f"Q{quarter}-{now.year}"
            console.print(f"  Auto-detected period: {period}")

        # Start audit tracking
        audit = AuditTracker(ticker, period, run_type="memo")
        audit.start(config_snapshot={"days": days, "output": output})

        # Run the underlying pipeline first
        console.print("\n[bold cyan]>> Running data collection pipeline...[/bold cyan]")
        pipeline_results = _run_memo_pipeline(ticker, period, days, audit)

        # Generate memo
        console.print("\n[bold cyan]>> Generating Investment Memo...[/bold cyan]")
        memo_result = run_memo_generation(
            ticker=ticker,
            period=period,
            pipeline_results=pipeline_results,
            output_dir=output,
        )

        audit.complete(status="completed")

        # Display result
        console.print(f"\n[bold green]✅ Memo saved to: {memo_result['memo_path']}[/bold green]")
        console.print(f"  Rating: {memo_result.get('rating', 'N/A')}")
        console.print(f"  Confidence: {memo_result.get('confidence', 0):.0%}")
        console.print(f"  Thesis: {memo_result.get('thesis_status', 'N/A')}")
        console.print(f"  Coverage: {'PASS' if memo_result.get('coverage_passed') else 'FAIL'}")

    except Exception as e:
        console.print(f"\n[bold red]Error generating memo: {e}[/bold red]")
        logger.exception("Memo generation failed")
        sys.exit(1)


def _run_memo_pipeline(ticker: str, period: str, days: int,
                       audit=None) -> dict:
    """Run data collection pipeline for memo generation (lightweight)."""
    results = {}

    # Step 1: Financial data from yfinance
    try:
        from .collectors.yfinance_fundamentals import fetch_fundamentals
        from .db import insert_financial_fact
        facts = fetch_fundamentals(ticker)
        for fact in facts:
            insert_financial_fact(
                ticker=fact["ticker"], period=fact["period"],
                metric=fact["metric"], value=fact["value"],
                unit=fact.get("unit"), currency=fact.get("currency"),
                source_url=fact.get("source_url"),
            )
        results["financial_facts"] = len(facts)
        if audit:
            audit.record_step("financial_facts", details={"count": len(facts)})
    except Exception as e:
        logger.warning(f"Financial facts fetch failed: {e}")
        results["financial_facts"] = 0

    # Step 2: Market prices
    try:
        from .market.price_fetcher import run_market_fetch
        price_result = run_market_fetch(ticker, days=days)
        price_count = price_result.get("records_upserted", 0)
        results["market_prices"] = price_count
        if audit:
            audit.record_step("market_prices", details={"count": price_count})
    except Exception as e:
        logger.warning(f"Market prices fetch failed: {e}")
        results["market_prices"] = 0

    # Step 3: News sentiment
    try:
        from .analysis.news_sentiment import run_news_sentiment
        sentiment_result = run_news_sentiment(ticker)
        results["news_sentiment"] = sentiment_result
        if audit:
            audit.record_step("news_sentiment")
    except Exception as e:
        logger.warning(f"News sentiment failed: {e}")
        results["news_sentiment"] = {}

    # Step 4: Financial scoring
    try:
        from .analysis.financial_scoring import compute_financial_features, compute_score
        features = compute_financial_features(ticker, period)
        score, drivers, coverage = compute_score(features, ticker=ticker)
        score_result = {"score": score, "drivers": drivers, "coverage": coverage}
        results["financial_score"] = score_result
        if audit:
            audit.record_step("financial_scoring", details={"score": score})
    except Exception as e:
        logger.warning(f"Financial scoring failed: {e}")
        results["financial_score"] = {}

    # Step 5: Valuation
    try:
        from .analysis.valuation import run_valuation_analysis
        val_result = run_valuation_analysis(ticker)
        results["valuation"] = val_result
        if audit:
            audit.record_step("valuation")
    except Exception as e:
        logger.warning(f"Valuation failed: {e}")
        results["valuation"] = {}

    # Step 6: Sector scoring
    try:
        from .analysis.sector_scoring import compute_sector_score
        # Use financial score drivers if available
        fs = results.get("financial_score", {})
        base_score = fs.get("score", 50)
        drivers = fs.get("drivers", [])
        sector_result = compute_sector_score(ticker, base_score, drivers)
        results["sector_scoring"] = sector_result
        if audit:
            audit.record_step("sector_scoring")
    except Exception as e:
        logger.warning(f"Sector scoring failed: {e}")
        results["sector_scoring"] = {}

    # Step 7: Technical analysis
    try:
        from .analysis.technical_analysis import run_technical_analysis
        tech_result = run_technical_analysis(ticker)
        results["technical"] = tech_result
        if audit:
            audit.record_step("technical_analysis")
    except Exception as e:
        logger.warning(f"Technical analysis failed: {e}")
        results["technical"] = {}

    if audit:
        results["audit"] = audit.get_summary()

    return results


@cli.command("run-universe")
@click.option("--watchlist", "-w", required=True, help="Path to YAML watchlist file")
@click.option("--period", "-p", default=None, help="Reporting period")
@click.option("--days", "-d", default=90, help="Market data history in days")
@click.option("--output", "-o", default="output", help="Output directory for memos")
def run_universe(watchlist: str, period: Optional[str], days: int, output: str):
    """Run investment memo generation across a watchlist of tickers."""
    console.print(f"\n[bold blue]═══ Universe Run: {watchlist} ═══[/bold blue]\n")

    try:
        import yaml
        with open(watchlist, "r") as f:
            data = yaml.safe_load(f)

        tickers = data.get("tickers", []) if isinstance(data, dict) else data
        if not tickers:
            console.print("[red]No tickers found in watchlist[/red]")
            return

        console.print(f"  Found {len(tickers)} tickers: {', '.join(tickers[:10])}")

        results_summary = []
        for i, ticker in enumerate(tickers):
            console.print(f"\n[bold cyan]── [{i+1}/{len(tickers)}] {ticker} ──[/bold cyan]")
            try:
                from .summary.memo_generator import run_memo_generation
                pipeline_results = _run_memo_pipeline(ticker, period or "latest", days)
                memo_result = run_memo_generation(
                    ticker=ticker,
                    period=period or "latest",
                    pipeline_results=pipeline_results,
                    output_dir=output,
                )
                results_summary.append({
                    "ticker": ticker,
                    "status": "success",
                    "rating": memo_result.get("rating"),
                    "confidence": memo_result.get("confidence"),
                })
                console.print(f"  [green]✅ {ticker}: {memo_result.get('rating')} ({memo_result.get('confidence', 0):.0%})[/green]")
            except Exception as e:
                results_summary.append({"ticker": ticker, "status": "failed", "error": str(e)})
                console.print(f"  [red]❌ {ticker}: {e}[/red]")

        # Summary table
        console.print("\n[bold]═══ Universe Summary ═══[/bold]")
        table = Table()
        table.add_column("Ticker", style="cyan")
        table.add_column("Status")
        table.add_column("Rating")
        table.add_column("Confidence")
        for r in results_summary:
            status_color = "green" if r["status"] == "success" else "red"
            table.add_row(
                r["ticker"],
                f"[{status_color}]{r['status']}[/{status_color}]",
                r.get("rating", "N/A"),
                f"{r.get('confidence', 0):.0%}" if r.get("confidence") else "N/A",
            )
        console.print(table)

    except FileNotFoundError:
        console.print(f"[red]Watchlist file not found: {watchlist}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Universe run failed")
        sys.exit(1)


@cli.command("run-diff")
@click.option("--ticker", "-t", required=True, help="Stock ticker")
@click.option("--from", "from_period", required=True, help="Earlier period (e.g., Q3-2025)")
@click.option("--to", "to_period", required=True, help="Later period (e.g., Q4-2025)")
def run_diff(ticker: str, from_period: str, to_period: str):
    """Compare a ticker across two periods — show what changed."""
    console.print(f"\n[bold blue]═══ Period Diff: {ticker} ({from_period} → {to_period}) ═══[/bold blue]\n")

    try:
        from .db import get_financial_facts

        facts_from = get_financial_facts(ticker, [from_period])
        facts_to = get_financial_facts(ticker, [to_period])

        # Build metric maps
        map_from = {f["metric"]: f["value"] for f in facts_from}
        map_to = {f["metric"]: f["value"] for f in facts_to}

        all_metrics = sorted(set(list(map_from.keys()) + list(map_to.keys())))

        if not all_metrics:
            console.print(f"[yellow]No financial facts found for {ticker} in either period.[/yellow]")
            return

        table = Table(title=f"Metric Changes: {ticker}")
        table.add_column("Metric", style="cyan")
        table.add_column(from_period, justify="right")
        table.add_column(to_period, justify="right")
        table.add_column("Change", justify="right")

        for metric in all_metrics:
            val_from = map_from.get(metric)
            val_to = map_to.get(metric)

            from_str = f"{float(val_from):.2f}" if val_from is not None else "—"
            to_str = f"{float(val_to):.2f}" if val_to is not None else "—"

            if val_from is not None and val_to is not None:
                change = float(val_to) - float(val_from)
                if float(val_from) != 0:
                    pct = (change / abs(float(val_from))) * 100
                    change_str = f"{change:+.2f} ({pct:+.1f}%)"
                else:
                    change_str = f"{change:+.2f}"
                change_color = "green" if change > 0 else "red" if change < 0 else "white"
                change_str = f"[{change_color}]{change_str}[/{change_color}]"
            else:
                change_str = "N/A"

            table.add_row(metric, from_str, to_str, change_str)

        console.print(table)
        console.print(f"\n  Metrics compared: {len(all_metrics)}")
        console.print(f"  In {from_period}: {len(map_from)} metrics")
        console.print(f"  In {to_period}: {len(map_to)} metrics")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Diff generation failed")
        sys.exit(1)


@cli.command("run-thesis")
@click.option("--ticker", "-t", required=True, help="Stock ticker")
@click.option("--init", "init_flag", is_flag=True, help="Initialize thesis from sector template")
@click.option("--sector", "-s", default=None, help="Override sector for template")
def run_thesis(ticker: str, init_flag: bool, sector: Optional[str]):
    """Initialize or check investment thesis for a ticker."""
    console.print(f"\n[bold blue]═══ Thesis Tracker: {ticker} ═══[/bold blue]\n")

    try:
        from .analysis.thesis_tracker import init_thesis, check_thesis, format_thesis_report

        if init_flag:
            console.print(f"  Initializing thesis for {ticker}...")
            result = init_thesis(ticker, sector=sector)
            console.print(f"  [green]✅ Thesis initialized (sector: {result.get('sector', 'N/A')})[/green]")
            console.print(f"  Status: {result.get('status', 'on_track').upper()}")
            console.print(f"\n  BASE: {result.get('base_thesis', '')}")
            console.print(f"  BULL: {result.get('bull_case', '')}")
            console.print(f"  BEAR: {result.get('bear_case', '')}")

            kpis = result.get("kpis", [])
            if kpis:
                console.print(f"\n  KPIs ({len(kpis)}):")
                for kpi in kpis:
                    console.print(f"    • {kpi.get('name', '')} (target: {kpi.get('target', '')})")
        else:
            result = check_thesis(ticker)
            report = format_thesis_report(result)
            console.print(report)

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        logger.exception("Thesis operation failed")
        sys.exit(1)


@cli.command("run-quality")
@click.option("--check", "-c", "check_type", required=True,
              type=click.Choice(["feeds", "db", "all"]),
              help="What to check: feeds, db, or all")
def run_quality(check_type: str):
    """Run quality/health checks on pipeline infrastructure."""
    console.print(f"\n[bold blue]═══ Quality Check: {check_type} ═══[/bold blue]\n")

    issues = []

    if check_type in ("feeds", "all"):
        console.print("[bold cyan]>> Feed Health Check[/bold cyan]")
        try:
            from .collectors.feed_health import get_health_manager
            health = get_health_manager()
            summary = health.get_health_summary()
            total = summary.get("total_feeds", 0)
            healthy = summary.get("healthy_feeds", 0)
            disabled = summary.get("disabled_feeds", 0)

            console.print(f"  Total feeds tracked: {total}")
            console.print(f"  Healthy: [green]{healthy}[/green]")
            if disabled > 0:
                console.print(f"  Disabled: [red]{disabled}[/red]")
                issues.append(f"{disabled} feeds disabled")

            # Show disabled feeds
            disabled_list = summary.get("disabled_list", [])
            for feed_url in disabled_list:
                console.print(f"    ❌ {feed_url}")

        except Exception as e:
            console.print(f"  [red]Feed health check failed: {e}[/red]")
            issues.append(f"Feed health error: {e}")

    if check_type in ("db", "all"):
        console.print("\n[bold cyan]>> Database Check[/bold cyan]")
        try:
            from .db import get_db_cursor
            with get_db_cursor() as cursor:
                tables = [
                    "fetch_jobs", "news_items", "financial_facts",
                    "scores_financial", "news_sentiment", "market_prices",
                    "company_summary", "filings_raw", "filings_extracted",
                    "thesis", "pipeline_runs",
                ]
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                        result = cursor.fetchone()
                        count = result["cnt"] if result else 0
                        color = "green" if count > 0 else "yellow"
                        console.print(f"  {table}: [{color}]{count} rows[/{color}]")
                        if count == 0:
                            issues.append(f"{table} is empty")
                    except Exception:
                        console.print(f"  {table}: [red]table not found[/red]")
                        issues.append(f"{table} table missing — run schema.sql")
        except Exception as e:
            console.print(f"  [red]Database check failed: {e}[/red]")
            issues.append(f"DB connection error: {e}")

    # Summary
    console.print(f"\n[bold]{'─' * 40}[/bold]")
    if issues:
        console.print(f"[yellow]⚠ {len(issues)} issue(s) found:[/yellow]")
        for issue in issues:
            console.print(f"  • {issue}")
    else:
        console.print("[bold green]✅ All checks passed![/bold green]")


if __name__ == "__main__":
    cli()
