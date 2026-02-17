"""
Company Reports Collector module.
Crawls company pages and downloads PDF/HTML reports.
Supports both regular requests and Playwright for JS-heavy pages.
"""

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse
from uuid import UUID

from bs4 import BeautifulSoup

from .base import BaseCollector, FetchResult, log_job_result
from ..db import insert_fetch_job, update_fetch_job, check_duplicate_by_checksum
from ..storage import upload_raw

logger = logging.getLogger(__name__)


class CompanyReportsCollector(BaseCollector):
    """Collector for company financial reports (PDF/HTML)."""

    def __init__(self):
        super().__init__(source="company_reports")
        self._playwright = None
        self._browser = None

    def _init_playwright(self):
        """Lazily initialize Playwright browser."""
        if self._playwright is None:
            try:
                from playwright.sync_api import sync_playwright
                self._playwright = sync_playwright().start()
                self._browser = self._playwright.chromium.launch(headless=True)
                logger.info("Playwright browser initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Playwright: {e}")
                self._playwright = False  # Mark as unavailable
                
    def _close_playwright(self):
        """Close Playwright browser."""
        if self._browser:
            self._browser.close()
        if self._playwright and self._playwright is not False:
            self._playwright.stop()
        self._playwright = None
        self._browser = None

    @staticmethod
    def _is_direct_file_url(url: str) -> bool:
        """Check if URL points directly to a downloadable file (PDF, etc.)."""
        parsed = urlparse(url)
        path = parsed.path.lower()
        return path.endswith((".pdf", ".xlsx", ".xls", ".doc", ".docx", ".zip"))

    def fetch_with_playwright(self, url: str, timeout: int = 30000) -> FetchResult:
        """
        Fetch page using Playwright for JS-rendered content.
        
        For direct file URLs (e.g. PDFs), falls back to regular HTTP requests
        since Playwright cannot render them and triggers a download instead.
        
        Args:
            url: URL to fetch
            timeout: Timeout in milliseconds
            
        Returns:
            FetchResult with rendered content
        """
        import time

        # Direct file URLs (PDFs, etc.) cannot be rendered by Playwright;
        # the browser triggers a download instead of navigating.
        # Fall back to regular HTTP request for these.
        if self._is_direct_file_url(url):
            logger.info(f"[{self.source}] Direct file URL detected, using HTTP request: {url}")
            return self.fetch_url_safe(url)

        start_time = time.time()
        
        self._init_playwright()
        
        if self._playwright is False:
            return FetchResult(
                success=False,
                url=url,
                error="Playwright not available",
            )
        
        try:
            self.rate_limit_delay()
            
            page = self._browser.new_page()
            page.set_default_timeout(timeout)
            
            response = page.goto(url, wait_until="networkidle")
            content = page.content().encode("utf-8")
            
            duration = time.time() - start_time
            checksum = self.calculate_checksum(content)
            
            status_code = response.status if response else None
            
            page.close()
            
            if status_code == 200:
                logger.info(
                    f"[{self.source}] Playwright fetched {url} | "
                    f"Status: {status_code} | "
                    f"Size: {len(content)} bytes | "
                    f"Duration: {duration:.2f}s"
                )
                return FetchResult(
                    success=True,
                    url=url,
                    content=content,
                    http_code=status_code,
                    content_type="text/html",
                    checksum=checksum,
                    duration=duration,
                )
            else:
                return FetchResult(
                    success=False,
                    url=url,
                    http_code=status_code,
                    error=f"HTTP {status_code}",
                    duration=duration,
                )
                
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[{self.source}] Playwright error for {url}: {e}")
            return FetchResult(
                success=False,
                url=url,
                error=str(e),
                duration=duration,
            )

    def extract_report_links(
        self, 
        page_url: str, 
        content: bytes,
        extensions: tuple = (".pdf", ".PDF"),
    ) -> list[dict]:
        """
        Extract report download links from a page.
        
        Args:
            page_url: Base URL of the page
            content: HTML content
            extensions: File extensions to look for
            
        Returns:
            List of report link dictionaries
        """
        links = []
        soup = BeautifulSoup(content, "lxml")
        
        # Special handling for SEC EDGAR
        if "sec.gov" in page_url:
            # Parse the main filing table (class="tableFile2")
            for tr in soup.find_all("tr"):
                text = tr.get_text(" ", strip=True).lower()
                doc_type = None
                
                if "10-k" in text and "10-k/a" not in text: # Avoid amendments for simplicity?
                    doc_type = "annual_report"
                elif "10-q" in text and "10-q/a" not in text:
                    doc_type = "quarterly_report"
                
                if doc_type:
                    # Find the "Documents" link
                    doc_link = tr.find("a", string="Documents") or tr.find("a", id="documentsbutton")
                    if doc_link and doc_link.get("href"):
                         full_url = urljoin(page_url, doc_link["href"])
                         
                         # Check if we haven't added this URL yet
                         if not any(l["url"] == full_url for l in links):
                             links.append({
                                 "url": full_url,
                                 "title": f"SEC {doc_type.replace('_', ' ').title()}",
                                 "ticker": self._extract_ticker(page_url, soup),
                                 "doc_type": doc_type,
                             })
            
            # If we found SEC links, return them (don't mix with generic scraping if successful)
            if links:
                return links
        
        # Generic scraping for non-SEC or if SEC parse failed
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            title = a_tag.get_text(strip=True) or "Untitled"
            title_lower = title.lower()
            
            # Check if it's a document link
            is_document = any(href.lower().endswith(ext.lower()) for ext in extensions)
            
            # Also look for download links
            if not is_document:
                onclick = a_tag.get("onclick", "")
                if "download" in href.lower() or "download" in onclick.lower():
                    is_document = True
            
            if is_document:
                # Resolve relative URLs
                full_url = urljoin(page_url, href)
                
                # Extract link text as title
                if title == "Untitled" and "interactive data" in full_url.lower():
                    title = "Interactive Data"
                
                # Try to extract ticker from page content or URL
                ticker = self._extract_ticker(page_url, soup)
                
                # Determine document type (generic)
                doc_type = "annual_report"
                if "quarterly" in title_lower or "q1" in title_lower or "q2" in title_lower or "10-q" in title_lower:
                    doc_type = "quarterly_report"
                elif "presentation" in title_lower:
                    doc_type = "presentation"
                elif "earnings" in title_lower:
                    doc_type = "earnings"
                elif "10-k" in title_lower:
                    doc_type = "annual_report"
                
                links.append({
                    "url": full_url,
                    "title": title,
                    "ticker": ticker,
                    "doc_type": doc_type,
                })
        
        return links

    def _extract_ticker(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """Try to extract stock ticker from URL or page content."""
        # Common patterns in URLs
        url_patterns = [
            r"/([A-Z]{2,5})/",  # /AAPL/
            r"ticker=([A-Z]{2,5})",  # ticker=AAPL
            r"symbol=([A-Z]{2,5})",  # symbol=AAPL
        ]
        
        for pattern in url_patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        # Look in meta tags
        meta_ticker = soup.find("meta", {"name": "ticker"})
        if meta_ticker:
            return meta_ticker.get("content")
        
        return None

    def download_report(
        self, 
        report_url: str, 
        ticker: Optional[str] = None,
        doc_type: str = "report",
        use_playwright: bool = False,
    ) -> tuple[Optional[UUID], Optional[str]]:
        """
        Download a report and store it.
        
        Args:
            report_url: URL of the report
            ticker: Stock ticker
            doc_type: Type of document
            use_playwright: Whether to use Playwright
            
        Returns:
            Tuple of (job_id, object_key) or (job_id, None) on failure
        """
        # Create fetch job
        job_id = insert_fetch_job(
            source=self.source,
            ticker=ticker,
            doc_type=doc_type,
            url=report_url,
            status="fetching",
        )
        
        # Fetch the report
        if use_playwright:
            result = self.fetch_with_playwright(report_url)
        else:
            result = self.fetch_url_safe(report_url)
        
        if not result.success:
            update_fetch_job(
                job_id=job_id,
                status="failed",
                http_code=result.http_code,
                error=result.error,
            )
            log_job_result(str(job_id), report_url, "failed", result.duration, result.error)
            return job_id, None
        
        # Check for duplicates
        if check_duplicate_by_checksum("fetch_jobs", result.checksum):
            update_fetch_job(
                job_id=job_id,
                status="duplicate",
                http_code=result.http_code,
                checksum=result.checksum,
            )
            log_job_result(str(job_id), report_url, "duplicate", result.duration)
            return job_id, None
        
        # Upload to MinIO
        try:
            object_key, checksum = upload_raw(
                data=result.content,
                source=self.source,
                doc_type=doc_type,
                ticker=ticker,
                content_type=result.content_type,
                url=report_url,
            )
            
            update_fetch_job(
                job_id=job_id,
                status="success",
                http_code=result.http_code,
                checksum=checksum,
                raw_object_key=object_key,
            )
            log_job_result(str(job_id), report_url, "success", result.duration)
            
            return job_id, object_key
            
        except Exception as e:
            update_fetch_job(
                job_id=job_id,
                status="failed",
                http_code=result.http_code,
                checksum=result.checksum,
                error=str(e),
            )
            log_job_result(str(job_id), report_url, "failed", result.duration, str(e))
            return job_id, None

    def collect(
        self, 
        page_urls: list[str],
        use_playwright: bool = False,
        download_limit: Optional[int] = None,
    ) -> list[UUID]:
        """
        Crawl pages and download company reports.
        
        Args:
            page_urls: List of page URLs to crawl for report links
            use_playwright: Whether to use Playwright for page fetching
            download_limit: Maximum number of reports to download per page
            
        Returns:
            List of fetch job IDs
        """
        job_ids = []
        
        try:
            for page_url in page_urls:
                logger.info(f"Crawling page for reports: {page_url}")
                
                # Fetch the listing page
                if use_playwright:
                    result = self.fetch_with_playwright(page_url)
                else:
                    result = self.fetch_url_safe(page_url)
                
                if not result.success:
                    logger.error(f"Failed to fetch listing page: {page_url}")
                    continue
                
                # Extract report links
                report_links = self.extract_report_links(page_url, result.content)
                logger.info(f"Found {len(report_links)} report links on page")
                
                # Apply download limit
                if download_limit:
                    report_links = report_links[:download_limit]
                
                # Download each report
                for report in report_links:
                    job_id, object_key = self.download_report(
                        report_url=report["url"],
                        ticker=report["ticker"],
                        doc_type=report["doc_type"],
                        use_playwright=use_playwright,
                    )
                    job_ids.append(job_id)
                    
        finally:
            self._close_playwright()
        
        logger.info(f"Report collection complete. Processed {len(job_ids)} reports.")
        return job_ids


def crawl_reports(
    page_urls: list[str],
    use_playwright: bool = False,
    download_limit: Optional[int] = None,
) -> list[UUID]:
    """
    Crawl pages and download company reports.
    
    Args:
        page_urls: List of page URLs containing report links
        use_playwright: Whether to use Playwright for JS-heavy pages
        download_limit: Maximum reports to download per page
        
    Returns:
        List of fetch job IDs
    """
    collector = CompanyReportsCollector()
    return collector.collect(page_urls, use_playwright, download_limit)


# Example report page URLs (placeholders - replace with actual company IR pages)
EXAMPLE_REPORT_PAGES = [
    # These are placeholder URLs - replace with actual investor relations pages
    "https://investor.apple.com/investor-relations/default.aspx",  # Apple IR
    "https://www.microsoft.com/en-us/investor",  # Microsoft IR
]


# ============================================
# Known IR pages: validated mapping of ticker → IR page URLs
# For IDX stocks: company IR pages + IDX profile
# For US stocks: company IR pages + SEC EDGAR EFTS search
# ============================================
KNOWN_IR_PAGES: dict[str, list[str]] = {
    # ── Indonesia (.JK tickers) ──────────────────────────────────
    "BBCA": [
        "https://www.bca.co.id/en/about-bca/investor-relations/financial-report",
    ],
    "BBRI": [
        "https://www.ir-bri.com/financial-reports.html",
    ],
    "BMRI": [
        "https://bankmandiri.co.id/en/investor-relations/financial-report",
    ],
    "BBNI": [
        "https://www.bni.co.id/en-us/investorrelations/financialreport",
    ],
    "TLKM": [
        "https://www.telkom.co.id/sites/about-telkom/en_US/page/investor-relations",
    ],
    "ASII": [
        "https://www.astra.co.id/Investor-Relations/Annual-Report",
    ],
    "UNVR": [
        "https://www.unilever.co.id/investor-relations/",
    ],
    "GOTO": [
        "https://investors.gotocompany.com/financial-information/sec-filings",
    ],
    "BRIS": [
        "https://www.bankbsi.co.id/company/investor-relation/laporan-keuangan",
    ],
    "ICBP": [
        "https://www.indofoodcbp.com/investor-relations",
    ],
    # ── US Stocks ────────────────────────────────────────────────
    "ORCL": [
        "https://investor.oracle.com/financial-reporting/sec-filings",
    ],
    "AAPL": [
        "https://investor.apple.com/sec-filings/default.aspx",
    ],
    "MSFT": [
        "https://www.microsoft.com/en-us/investor/sec-filings.aspx",
    ],
    "GOOGL": [
        "https://abc.xyz/investor/",
    ],
    "AMZN": [
        "https://ir.aboutamazon.com/sec-filings/default.aspx",
    ],
    "TSLA": [
        "https://ir.tesla.com/sec-filings",
    ],
    "META": [
        "https://investor.fb.com/sec-filings/default.aspx",
    ],
    "NVDA": [
        "https://investor.nvidia.com/financial-info/sec-filings",
    ],
}

# Company name for SEC EDGAR EFTS queries (US tickers)
_SEC_COMPANY_NAMES: dict[str, str] = {
    "ORCL": "Oracle",
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet",
    "AMZN": "Amazon",
    "TSLA": "Tesla",
    "META": "Meta Platforms",
    "NVDA": "NVIDIA",
}

# Common IR path suffixes to try for unknown companies
IR_PATH_SUFFIXES = [
    "/investor-relations",
    "/investors",
    "/ir",
    "/hubungan-investor",
    "/en/investor-relations",
    "/en/investor-relation",
    "/investor-relations/financial-report",
    "/annual-report",
]


def discover_ir_pages(ticker: str) -> list[str]:
    """
    Auto-discover investor relations pages for a ticker.

    Strategies:
    1. Check KNOWN_IR_PAGES for known stocks (IDX + US)
    2. Use yfinance to find company website → try common IR suffixes
    3. For .JK tickers: include IDX company profile page
    4. For US tickers: SEC EDGAR EFTS full-text search API

    Args:
        ticker: Stock ticker (e.g., 'BBCA.JK', 'AAPL')

    Returns:
        List of discovered IR page URLs
    """
    import requests

    pages: list[str] = []
    base_ticker = ticker.split(".")[0].upper()

    logger.info(f"Discovering IR pages for {ticker}...")

    # Strategy 1: Known IR pages
    if base_ticker in KNOWN_IR_PAGES:
        pages.extend(KNOWN_IR_PAGES[base_ticker])
        logger.info(f"  Found {len(KNOWN_IR_PAGES[base_ticker])} known IR pages for {base_ticker}")

    # Strategy 2: yfinance company website
    try:
        import yfinance as yf

        info = yf.Ticker(ticker).info
        website = info.get("website") or ""
        if website:
            website = website.rstrip("/")
            logger.info(f"  yfinance website: {website}")

            # Try common IR path suffixes
            for suffix in IR_PATH_SUFFIXES:
                candidate = f"{website}{suffix}"
                if candidate not in pages:
                    pages.append(candidate)

    except Exception as e:
        logger.warning(f"  yfinance lookup failed: {e}")

    # Strategy 3: IDX company profile (for .JK tickers)
    if ticker.upper().endswith(".JK"):
        idx_url = (
            f"https://www.idx.co.id/en/listed-companies/company-profiles/{base_ticker}"
        )
        if idx_url not in pages:
            pages.append(idx_url)
        logger.info(f"  Added IDX profile page for {base_ticker}")
    else:
        # Strategy 4: SEC EDGAR EFTS full-text search (for US tickers)
        company_name = _SEC_COMPANY_NAMES.get(base_ticker, base_ticker)
        sec_url = (
            f"https://efts.sec.gov/LATEST/search-index?q=%22{company_name}%22"
            f"&forms=10-K,10-Q&dateRange=custom&startdt=2024-01-01"
        )
        if sec_url not in pages:
            pages.append(sec_url)
        logger.info(f"  Added SEC EDGAR EFTS search for {base_ticker} ({company_name})")

    # Validate: only keep URLs that respond
    valid_pages: list[str] = []
    
    # Create session with headers
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com/",
    })

    for url in pages:
        try:
            # Use GET with stream=True (many sites reject HEAD with 403/405)
            resp = session.get(url, timeout=10, allow_redirects=True, stream=True)
            resp.close()  # Don't download full body
            if resp.status_code < 400:
                valid_pages.append(url)
                logger.info(f"  ✓ Valid: {url}")
            elif resp.status_code in (403, 405):
                # 403/405 often happens with protected pages.
                # Treat as valid so Playwright can attempt to fetch it later.
                valid_pages.append(url)
                logger.warning(f"  ! Provisional ({resp.status_code}): {url}")
            else:
                logger.debug(f"  ✗ {resp.status_code}: {url}")
        except Exception:
            logger.debug(f"  ✗ Unreachable: {url}")

    if not valid_pages:
        logger.warning(f"No valid IR pages found for {ticker}.")
        # Fallback: return all candidates unvalidated so user can try with Playwright
        logger.info("  Returning unvalidated candidates as fallback.")
        return pages[:3] if pages else []

    logger.info(f"Discovered {len(valid_pages)} valid IR pages for {ticker}")
    return valid_pages


