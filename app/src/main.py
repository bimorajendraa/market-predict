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
            results["reports"] = {"status": "skipped", "reason": "No IR pages found"}
            console.print(f"  [yellow][!] No IR pages found for {ticker} -- skipping[/yellow]")
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


if __name__ == "__main__":
    cli()
