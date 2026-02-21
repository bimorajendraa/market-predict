"""
RSS News Collector module.
Scrapes RSS feeds and stores news items in the database.
Includes URL canonicalization and title-similarity dedup.
"""

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from uuid import UUID

import feedparser
from bs4 import BeautifulSoup

from .base import BaseCollector, FetchResult, log_job_result
from ..db import insert_news_item, insert_fetch_job, update_fetch_job
from ..storage import upload_raw

logger = logging.getLogger(__name__)


# ============================================
# URL canonicalization helpers
# ============================================
_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "tsrc", "mod", "ref", "src", "ftag", "ncid",
}


def canonicalize_url(url: str) -> str:
    """Strip tracking params, fragments, trailing slashes for dedup."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=False)
    clean_qs = {k: v for k, v in qs.items() if k.lower() not in _TRACKING_PARAMS}
    clean_query = urlencode(clean_qs, doseq=True) if clean_qs else ""
    cleaned = urlunparse((
        parsed.scheme, parsed.netloc, parsed.path.rstrip("/"),
        parsed.params, clean_query, "",
    ))
    return cleaned.lower()


def title_similarity(a: str, b: str) -> float:
    """Jaccard similarity between title word sets (0-1)."""
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


class RSSCollector(BaseCollector):
    """Collector for RSS news feeds."""

    def __init__(self):
        super().__init__(source="rss")
        self._seen_urls: set[str] = set()
        self._seen_titles: list[str] = []

    def _is_duplicate_title(self, title: str, threshold: float = 0.80) -> bool:
        """Check if title is too similar to a previously seen title."""
        for seen in self._seen_titles:
            if title_similarity(title, seen) >= threshold:
                return True
        return False

    def parse_feed(self, feed_url: str, content: bytes) -> list[dict]:
        """
        Parse RSS feed content and extract news items.
        
        Args:
            feed_url: The URL of the feed
            content: Raw feed content
            
        Returns:
            List of parsed news item dictionaries
        """
        items = []
        
        try:
            # Parse the feed
            feed = feedparser.parse(content)
            
            if feed.bozo and feed.bozo_exception:
                if len(feed.entries) > 0:
                    logger.debug(f"Feed parse warning (handled): {feed.bozo_exception}")
                else:
                    logger.debug(f"Feed parse warning: {feed.bozo_exception}")
            
            # Extract feed source name
            feed_title = feed.feed.get("title", feed_url)
            
            for entry in feed.entries:
                raw_url = entry.get("link", "")
                if not raw_url:
                    continue

                # URL dedup
                canon = canonicalize_url(raw_url)
                if canon in self._seen_urls:
                    continue
                self._seen_urls.add(canon)

                title = entry.get("title", "No Title")

                # Title similarity dedup
                if self._is_duplicate_title(title):
                    logger.debug(f"Skipped similar title: {title[:60]}...")
                    continue
                self._seen_titles.append(title)

                # Extract published date
                published_at = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    published_at = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    published_at = datetime(*entry.updated_parsed[:6])
                
                # Extract body/summary
                body = ""
                if hasattr(entry, "summary"):
                    # Strip HTML from summary
                    soup = BeautifulSoup(entry.summary, "lxml")
                    body = soup.get_text(separator=" ", strip=True)
                elif hasattr(entry, "content"):
                    for content_item in entry.content:
                        soup = BeautifulSoup(content_item.value, "lxml")
                        body = soup.get_text(separator=" ", strip=True)
                        break
                
                # Extract ticker if present in categories
                ticker = None
                if hasattr(entry, "tags"):
                    for tag in entry.tags:
                        term = tag.get("term", "")
                        # Look for stock ticker patterns (e.g., $AAPL or AAPL)
                        if term.startswith("$") or (term.isupper() and len(term) <= 5):
                            ticker = term.lstrip("$")
                            break
                
                item = {
                    "title": title,
                    "url": raw_url,
                    "published_at": published_at,
                    "body": body[:5000] if body else None,  # Limit body length
                    "ticker": ticker,
                    "source": feed_title,
                }
                
                items.append(item)
                    
        except Exception as e:
            logger.error(f"Failed to parse RSS feed: {e}")
            
        return items

    def collect(self, feed_urls: list[str]) -> list[UUID]:
        """
        Collect news from multiple RSS feeds.
        Skips feeds disabled by health manager, records results.

        Args:
            feed_urls: List of RSS feed URLs to scrape

        Returns:
            List of inserted news item IDs
        """
        from .feed_health import get_health_manager
        health = get_health_manager()

        # Filter out disabled feeds
        enabled_urls = health.get_enabled_feeds(feed_urls)
        skipped_disabled = len(feed_urls) - len(enabled_urls)
        if skipped_disabled > 0:
            logger.info(f"Skipping {skipped_disabled} disabled feeds")

        inserted_ids = []
        
        for feed_url in enabled_urls:
            logger.info(f"Processing RSS feed: {feed_url}")
            
            # Create fetch job
            job_id = insert_fetch_job(
                source="rss",
                doc_type="feed",
                url=feed_url,
                status="fetching",
            )
            
            # Fetch the feed
            result = self.fetch_url_safe(feed_url)
            
            if not result.success:
                update_fetch_job(
                    job_id=job_id,
                    status="failed",
                    http_code=result.http_code,
                    error=result.error,
                )
                log_job_result(
                    str(job_id), feed_url, "failed", result.duration, result.error
                )
                health.record_result(
                    feed_url, success=False,
                    http_code=result.http_code, error=result.error
                )
                continue
            
            # Store raw feed content
            try:
                object_key, checksum = upload_raw(
                    data=result.content,
                    source="rss",
                    doc_type="feed",
                    content_type=result.content_type,
                    url=feed_url,
                )
            except Exception as e:
                logger.error(f"Failed to upload feed content: {e}")
                object_key, checksum = None, result.checksum
            
            # Update fetch job with success
            update_fetch_job(
                job_id=job_id,
                status="success",
                http_code=result.http_code,
                checksum=checksum,
                raw_object_key=object_key,
            )
            log_job_result(str(job_id), feed_url, "success", result.duration)
            health.record_result(feed_url, success=True, http_code=result.http_code)
            
            # Parse and store news items
            items = self.parse_feed(feed_url, result.content)
            logger.info(f"Found {len(items)} news items in feed")
            
            for item in items:
                # Calculate checksum for deduplication
                item_content = f"{item['title']}:{item['url']}".encode()
                item_checksum = self.calculate_checksum(item_content)
                
                # Assign default ticker if item doesn't have one from RSS tags
                item_ticker = item["ticker"]
                if not item_ticker and hasattr(self, "_default_ticker"):
                    item_ticker = self._default_ticker

                # Insert news item
                news_id = insert_news_item(
                    source=item["source"],
                    title=item["title"],
                    url=item["url"],
                    ticker=item_ticker,
                    published_at=item["published_at"],
                    body=item["body"],
                    checksum=item_checksum,
                )
                
                if news_id:
                    inserted_ids.append(news_id)
                    logger.debug(f"Inserted news item: {item['title'][:50]}...")
                else:
                    logger.debug(f"Skipped duplicate: {item['title'][:50]}...")
        
        # Log feed health summary
        report = health.get_health_report()
        logger.info(
            f"RSS collection complete. Inserted {len(inserted_ids)} new items. "
            f"Feed health: {report['enabled']} enabled, {report['disabled']} disabled "
            f"of {report['total_tracked']} tracked"
        )
        return inserted_ids


def scrape_rss(feed_urls: list[str], ticker: str | None = None) -> list[UUID]:
    """
    Scrape multiple RSS feeds and store news items.

    Args:
        feed_urls: List of RSS feed URLs
        ticker: Optional ticker to assign to items that don't have one

    Returns:
        List of inserted news item IDs
    """
    collector = RSSCollector()
    if ticker:
        collector._default_ticker = ticker
    return collector.collect(feed_urls)


# ============================================
# Feed Registry: General market feeds (always scraped)
# ============================================
DEFAULT_FEEDS_GENERAL = [
    # ---- Indonesia News (8 feeds) ----
    "https://www.cnbcindonesia.com/market/rss",
    "https://www.cnbcindonesia.com/news/rss",
    "https://www.kontan.co.id/investasi/rss",
    "https://www.kontan.co.id/bisnis/rss",
    "https://market.bisnis.com/rss",
    "https://finance.detik.com/rss",
    "https://www.tempo.co/rss/bisnis",
    "https://rss.idnfinancials.com/latest",
    # ---- International News (13 feeds) ----
    "https://id.investing.com/rss/news.rss",
    "https://www.investing.com/rss/news.rss",
    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "https://seekingalpha.com/market_currents.xml",
    "https://www.reutersagency.com/feed/?taxonomy=best-sectors&post_type=best",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://www.ft.com/markets?format=rss",
    "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US",
    "https://www.benzinga.com/feed/",
    "https://seekingalpha.com/feed.xml",
    "https://feeds.barrons.com/barrons/best_count",
]

# ============================================
# Feed Templates: {ticker} is replaced with the actual ticker
# ============================================
TICKER_FEED_TEMPLATES = [
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US",
]

# Keep backward compatibility
EXAMPLE_FEEDS = DEFAULT_FEEDS_GENERAL

# ============================================
# Company name aliases for Google News queries
# ============================================
_GOOGLE_NEWS_NAMES: dict[str, str] = {
    "BBCA": "Bank Central Asia",
    "BBRI": "Bank Rakyat Indonesia",
    "BMRI": "Bank Mandiri",
    "BBNI": "Bank Negara Indonesia",
    "TLKM": "Telkom Indonesia",
    "ASII": "Astra International",
    "UNVR": "Unilever Indonesia",
    "GOTO": "GoTo Gojek Tokopedia",
    "BRIS": "Bank Syariah Indonesia",
}


def get_feeds_for_ticker(ticker: str) -> list[str]:
    """
    Generate a comprehensive list of RSS feed URLs for a given ticker.

    Combines:
    1. General market feeds (Indonesia + international)
    2. Ticker-specific feeds generated from templates
    3. Google News RSS queries per ticker / company name
    4. Region-specific feeds for .JK tickers

    Args:
        ticker: Stock ticker symbol (e.g., 'BBCA.JK', 'TLKM.JK')

    Returns:
        Deduplicated list of feed URLs
    """
    feeds = list(DEFAULT_FEEDS_GENERAL)

    # Add ticker-specific feeds
    for template in TICKER_FEED_TEMPLATES:
        feeds.append(template.format(ticker=ticker))

    base_ticker = ticker.split(".")[0].upper()

    # Google News RSS — English query
    feeds.append(
        f"https://news.google.com/rss/search?q={ticker}+stock&hl=en&gl=US&ceid=US:en"
    )

    # For Indonesian stocks (.JK suffix)
    if ticker.upper().endswith(".JK"):
        feeds.append(
            f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=ID&lang=id-ID"
        )

        # Google News RSS — Indonesian query with company name
        company_name = _GOOGLE_NEWS_NAMES.get(base_ticker, base_ticker)
        feeds.append(
            f"https://news.google.com/rss/search?q={company_name.replace(' ', '+')}+saham&hl=id&gl=ID&ceid=ID:id"
        )
    else:
        # Google News RSS — English company name query
        company_name = _GOOGLE_NEWS_NAMES.get(base_ticker, base_ticker)
        if company_name != base_ticker:
            feeds.append(
                f"https://news.google.com/rss/search?q={company_name.replace(' ', '+')}+stock&hl=en&gl=US&ceid=US:en"
            )

    return list(dict.fromkeys(feeds))  # Deduplicate preserving order
