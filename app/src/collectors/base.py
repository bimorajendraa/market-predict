"""
Base collector module with common functionality.
Provides rate limiting, retry logic, and logging utilities.
"""

import hashlib
import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Any

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from rich.console import Console
from rich.logging import RichHandler

from ..config import config

# Setup rich logging
console = Console()
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)

logger = logging.getLogger(__name__)


class FetchResult:
    """Result of a fetch operation."""

    def __init__(
        self,
        success: bool,
        url: str,
        content: Optional[bytes] = None,
        http_code: Optional[int] = None,
        content_type: Optional[str] = None,
        checksum: Optional[str] = None,
        error: Optional[str] = None,
        duration: float = 0.0,
    ):
        self.success = success
        self.url = url
        self.content = content
        self.http_code = http_code
        self.content_type = content_type
        self.checksum = checksum
        self.error = error
        self.duration = duration

    def __repr__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        return f"FetchResult({status}, url={self.url}, code={self.http_code}, duration={self.duration:.2f}s)"


class BaseCollector(ABC):
    """Base class for all collectors with common functionality."""

    def __init__(self, source: str):
        self.source = source
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "FinanceAnalytics/1.0 (finance-analytics-bot@example.com)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.sec.gov/",
            "Upgrade-Insecure-Requests": "1",
        })

    def rate_limit_delay(self) -> None:
        """Apply random delay between requests for rate limiting."""
        delay = random.uniform(config.RATE_LIMIT_MIN, config.RATE_LIMIT_MAX)
        logger.debug(f"Rate limit delay: {delay:.2f}s")
        time.sleep(delay)

    @staticmethod
    def calculate_checksum(data: bytes) -> str:
        """Calculate SHA256 checksum of data."""
        return hashlib.sha256(data).hexdigest()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def fetch_url(self, url: str, timeout: Optional[int] = None) -> FetchResult:
        """
        Fetch content from URL with retry logic.
        
        Args:
            url: URL to fetch
            timeout: Request timeout in seconds
            
        Returns:
            FetchResult with content or error information
        """
        timeout = timeout or config.REQUEST_TIMEOUT
        start_time = time.time()
        
        try:
            self.rate_limit_delay()
            
            response = self.session.get(url, timeout=timeout, allow_redirects=True)
            duration = time.time() - start_time
            
            content = response.content
            checksum = self.calculate_checksum(content)
            content_type = response.headers.get("Content-Type", "")
            
            if response.status_code == 200:
                logger.info(
                    f"[{self.source}] Fetched {url} | "
                    f"Status: {response.status_code} | "
                    f"Size: {len(content)} bytes | "
                    f"Duration: {duration:.2f}s"
                )
                return FetchResult(
                    success=True,
                    url=url,
                    content=content,
                    http_code=response.status_code,
                    content_type=content_type,
                    checksum=checksum,
                    duration=duration,
                )
            else:
                logger.warning(
                    f"[{self.source}] Non-200 response for {url} | "
                    f"Status: {response.status_code} | "
                    f"Duration: {duration:.2f}s"
                )
                return FetchResult(
                    success=False,
                    url=url,
                    http_code=response.status_code,
                    error=f"HTTP {response.status_code}",
                    duration=duration,
                )
                
        except requests.RequestException as e:
            duration = time.time() - start_time
            logger.error(f"[{self.source}] Request failed for {url}: {e}")
            raise  # Let tenacity handle retry
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[{self.source}] Unexpected error for {url}: {e}")
            return FetchResult(
                success=False,
                url=url,
                error=str(e),
                duration=duration,
            )

    def fetch_url_safe(self, url: str, timeout: Optional[int] = None) -> FetchResult:
        """
        Fetch URL with all retries exhausted returning error instead of raising.
        
        Use this when you want to continue processing even if fetch fails.
        """
        try:
            return self.fetch_url(url, timeout)
        except Exception as e:
            logger.error(f"[{self.source}] All retries exhausted for {url}: {e}")
            return FetchResult(
                success=False,
                url=url,
                error=f"All retries exhausted: {e}",
            )

    @abstractmethod
    def collect(self, *args, **kwargs) -> list[Any]:
        """Main collection method to be implemented by subclasses."""
        pass


def log_job_result(
    job_id: str,
    url: str,
    status: str,
    duration: float,
    error: Optional[str] = None,
) -> None:
    """Log a job result with consistent formatting."""
    if error:
        logger.error(f"Job {job_id} | URL: {url} | Status: {status} | Duration: {duration:.2f}s | Error: {error}")
    else:
        logger.info(f"Job {job_id} | URL: {url} | Status: {status} | Duration: {duration:.2f}s")
