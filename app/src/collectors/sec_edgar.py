"""
SEC EDGAR Filing Downloader.
Downloads financial reports (10-K, 10-Q, 8-K) directly from SEC EDGAR
for US tickers using the full-text search API and filing archives.
"""

import logging
import re
import time
from typing import Optional

import requests

from ..storage import upload_raw

logger = logging.getLogger(__name__)

# SEC requires a proper User-Agent header with a real email address
SEC_USER_AGENT = "finance-analytics celapceluporeo@gmail.com"

# SEC rate limit: max 10 requests/second, we use 0.5s between requests
SEC_RATE_LIMIT = 0.5

# CIK lookup table for common US tickers (to avoid extra lookup requests)
KNOWN_CIKS: dict[str, str] = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "GOOGL": "0001652044",
    "AMZN": "0001018724",
    "TSLA": "0001318605",
    "META": "0001326801",
    "NVDA": "0001045810",
    "ORCL": "0001341439",
    "CRM": "0001108524",
    "INTC": "0000050863",
    "AMD": "0000002488",
    "IBM": "0000051143",
    "NFLX": "0001065280",
    "PYPL": "0001633917",
    "ADBE": "0000796343",
    "CSCO": "0000858877",
    "QCOM": "0000804328",
    "TXN": "0000097476",
    "AVGO": "0001649338",
    "NOW": "0001373715",
}

# Primary source status enum
PRIMARY_STATUS_AVAILABLE = "AVAILABLE"
PRIMARY_STATUS_BLOCKED = "BLOCKED_403"
PRIMARY_STATUS_NOT_FOUND = "NOT_FOUND"
PRIMARY_STATUS_NOT_CONFIGURED = "NOT_CONFIGURED"


def _sec_headers() -> dict:
    """Return headers compliant with SEC access policy."""
    return {
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json, text/html",
    }


def lookup_cik(ticker: str) -> Optional[str]:
    """
    Look up SEC CIK number for a ticker.
    
    First checks the known CIK table, then queries SEC company search API.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'ORCL')
        
    Returns:
        CIK number as zero-padded string, or None if not found.
    """
    base_ticker = ticker.split(".")[0].upper()
    
    # Check known CIKs first
    if base_ticker in KNOWN_CIKS:
        return KNOWN_CIKS[base_ticker]
    
    # Dynamic lookup via SEC company tickers JSON
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        logger.info(f"Looking up CIK for {base_ticker} from SEC...")
        resp = requests.get(url, headers=_sec_headers(), timeout=15)
        time.sleep(SEC_RATE_LIMIT)
        
        if resp.status_code == 200:
            data = resp.json()
            for entry in data.values():
                if entry.get("ticker", "").upper() == base_ticker:
                    cik = str(entry["cik_str"]).zfill(10)
                    logger.info(f"Found CIK for {base_ticker}: {cik}")
                    # Cache for future use
                    KNOWN_CIKS[base_ticker] = cik
                    return cik
            
            logger.warning(f"Ticker {base_ticker} not found in SEC company list")
            return None
        else:
            logger.warning(f"SEC company tickers lookup failed: HTTP {resp.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"CIK lookup failed for {base_ticker}: {e}")
        return None


def fetch_sec_filings(
    ticker: str,
    forms: list[str] | None = None,
    max_results: int = 10,
) -> list[dict]:
    """
    Search SEC EDGAR for recent filings.
    
    Uses the EDGAR full-text search API (EFTS) to find filings.
    
    Args:
        ticker: Stock ticker symbol
        forms: Filing types to search for (default: 10-K, 10-Q, 8-K)
        max_results: Maximum number of filings to return
        
    Returns:
        List of filing metadata dicts with accession_number, form_type,
        filing_date, description, url.
    """
    if forms is None:
        forms = ["10-K", "10-Q", "8-K"]
    
    base_ticker = ticker.split(".")[0].upper()
    cik = lookup_cik(ticker)
    
    if not cik:
        logger.warning(f"Cannot search SEC filings: CIK not found for {ticker}")
        return []
    
    # Use EDGAR submissions API (more reliable than EFTS for filings list)
    cik_no_pad = cik.lstrip("0")
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    
    logger.info(f"Fetching SEC filing list for {base_ticker} (CIK: {cik})...")
    try:
        resp = requests.get(url, headers=_sec_headers(), timeout=20)
        time.sleep(SEC_RATE_LIMIT)
        
        if resp.status_code != 200:
            logger.warning(f"SEC submissions API returned HTTP {resp.status_code}")
            return []
        
        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        
        if not recent:
            logger.warning(f"No recent filings found for {base_ticker}")
            return []
        
        # Extract filing list
        filings = []
        form_list = recent.get("form", [])
        date_list = recent.get("filingDate", [])
        accession_list = recent.get("accessionNumber", [])
        primary_doc_list = recent.get("primaryDocument", [])
        desc_list = recent.get("primaryDocDescription", [])
        
        for i in range(min(len(form_list), 100)):  # Scan up to 100 filings
            form_type = form_list[i]
            if form_type not in forms:
                continue
            
            accession = accession_list[i].replace("-", "")
            accession_formatted = accession_list[i]
            primary_doc = primary_doc_list[i] if i < len(primary_doc_list) else ""
            
            filing_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik_no_pad}/{accession}/{primary_doc}"
            )
            
            filings.append({
                "ticker": base_ticker,
                "form_type": form_type,
                "filing_date": date_list[i] if i < len(date_list) else "",
                "accession_number": accession_formatted,
                "description": desc_list[i] if i < len(desc_list) else form_type,
                "url": filing_url,
                "primary_document": primary_doc,
                "cik": cik,
            })
            
            if len(filings) >= max_results:
                break
        
        logger.info(f"Found {len(filings)} SEC filings for {base_ticker}: "
                     f"{', '.join(f['form_type'] for f in filings)}")
        return filings
        
    except Exception as e:
        logger.error(f"SEC filing search failed for {base_ticker}: {e}")
        return []


def download_filing(filing: dict) -> Optional[dict]:
    """
    Download a single SEC filing and store in MinIO.
    
    Args:
        filing: Filing metadata dict from fetch_sec_filings()
        
    Returns:
        Dict with download result metadata, or None on failure.
    """
    url = filing.get("url", "")
    if not url:
        return None
    
    ticker = filing.get("ticker", "UNKNOWN")
    form_type = filing.get("form_type", "")
    filing_date = filing.get("filing_date", "")
    
    logger.info(f"Downloading SEC filing: {form_type} ({filing_date}) for {ticker}")
    
    try:
        resp = requests.get(url, headers=_sec_headers(), timeout=30)
        time.sleep(SEC_RATE_LIMIT)
        
        if resp.status_code == 403:
            logger.warning(f"SEC blocked access (403) to {url}")
            return {"status": "blocked", "http_code": 403, "url": url}
        
        if resp.status_code != 200:
            logger.warning(f"SEC download failed: HTTP {resp.status_code} for {url}")
            return {"status": "failed", "http_code": resp.status_code, "url": url}
        
        content = resp.content
        content_type = resp.headers.get("Content-Type", "text/html")
        
        # Upload to MinIO
        try:
            object_key, checksum = upload_raw(
                data=content,
                source="sec_edgar",
                doc_type=form_type.lower().replace("-", ""),
                content_type=content_type,
                url=url,
            )
        except Exception as e:
            logger.error(f"Failed to upload SEC filing to storage: {e}")
            object_key = None
            checksum = None
        
        return {
            "status": "success",
            "http_code": 200,
            "url": url,
            "ticker": ticker,
            "form_type": form_type,
            "filing_date": filing_date,
            "content_length": len(content),
            "content_type": content_type,
            "object_key": object_key,
            "checksum": checksum,
        }
        
    except Exception as e:
        logger.error(f"SEC filing download error: {e}")
        return {"status": "error", "error": str(e), "url": url}


def collect_sec_filings(
    ticker: str,
    forms: list[str] | None = None,
    max_downloads: int = 5,
) -> dict:
    """
    Full SEC EDGAR collection pipeline for a US ticker.
    
    Searches for filings, downloads them, and reports status.
    
    Args:
        ticker: US stock ticker
        forms: Filing types to search for
        max_downloads: Max number of filings to download
        
    Returns:
        Dict with filings_found, downloaded, failed, primary_source_status.
    """
    base_ticker = ticker.split(".")[0].upper()
    
    # Check if this is a US ticker (no .JK, etc.)
    if "." in ticker and not ticker.upper().endswith(".JK"):
        # Unknown exchange
        pass
    
    # For .JK (Indonesian) tickers, SEC is not applicable
    if ticker.upper().endswith(".JK"):
        logger.info(f"SEC EDGAR not applicable for Indonesian ticker {ticker}")
        return {
            "filings_found": 0,
            "downloaded": 0,
            "failed": 0,
            "primary_source_status": PRIMARY_STATUS_NOT_CONFIGURED,
            "details": [],
        }
    
    # Search for filings
    filings = fetch_sec_filings(ticker, forms=forms, max_results=max_downloads)
    
    if not filings:
        return {
            "filings_found": 0,
            "downloaded": 0,
            "failed": 0,
            "primary_source_status": PRIMARY_STATUS_NOT_FOUND,
            "details": [],
        }
    
    # Download filings
    results = []
    downloaded = 0
    failed = 0
    blocked = False
    
    for filing in filings[:max_downloads]:
        result = download_filing(filing)
        if result:
            results.append(result)
            if result.get("status") == "success":
                downloaded += 1
            elif result.get("status") == "blocked":
                blocked = True
                failed += 1
            else:
                failed += 1
    
    # Determine primary source status
    if downloaded > 0:
        status = PRIMARY_STATUS_AVAILABLE
    elif blocked:
        status = PRIMARY_STATUS_BLOCKED
    else:
        status = PRIMARY_STATUS_NOT_FOUND
    
    logger.info(
        f"SEC collection for {base_ticker}: "
        f"{len(filings)} found, {downloaded} downloaded, {failed} failed. "
        f"Status: {status}"
    )
    
    return {
        "filings_found": len(filings),
        "downloaded": downloaded,
        "failed": failed,
        "primary_source_status": status,
        "details": results,
    }
