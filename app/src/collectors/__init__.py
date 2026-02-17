"""Collectors package for Finance Analytics."""

from .base import BaseCollector
from .news_rss import scrape_rss
from .company_reports import crawl_reports

__all__ = ["BaseCollector", "scrape_rss", "crawl_reports"]
