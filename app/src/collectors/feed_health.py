"""
RSS Feed Health Manager.
Tracks feed success/failure and auto-disables feeds that fail repeatedly.
Stores state in feeds_health.json for persistence without requiring a database.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default location for health tracking file
HEALTH_FILE = Path("feeds_health.json")

# Auto-disable threshold: if a feed fails this many times, it gets disabled
FAIL_THRESHOLD = 3


class FeedHealthManager:
    """
    Manages RSS feed health tracking.

    Tracks success/failure per feed URL and auto-disables feeds
    that fail >= FAIL_THRESHOLD times consecutively.
    """

    def __init__(self, health_file: Optional[Path] = None):
        self._file = health_file or HEALTH_FILE
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self):
        """Load health data from JSON file."""
        if self._file.exists():
            try:
                with open(self._file, "r") as f:
                    self._data = json.load(f)
                logger.debug(f"Loaded feed health data: {len(self._data)} feeds tracked")
            except Exception as e:
                logger.warning(f"Failed to load feed health file: {e}")
                self._data = {}
        else:
            self._data = {}

    def _save(self):
        """Persist health data to JSON file."""
        try:
            with open(self._file, "w") as f:
                json.dump(self._data, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f"Failed to save feed health file: {e}")

    def _get_entry(self, url: str) -> dict:
        """Get or create health entry for a feed URL."""
        if url not in self._data:
            self._data[url] = {
                "url": url,
                "enabled": True,
                "fail_count": 0,
                "total_successes": 0,
                "total_failures": 0,
                "last_status": None,
                "last_http_code": None,
                "last_checked_at": None,
                "last_success_at": None,
                "disabled_at": None,
                "disabled_reason": None,
            }
        return self._data[url]

    def is_feed_enabled(self, url: str) -> bool:
        """Check if a feed is enabled (not auto-disabled due to failures)."""
        entry = self._get_entry(url)
        return entry.get("enabled", True)

    def record_result(self, url: str, success: bool, http_code: Optional[int] = None,
                      error: Optional[str] = None):
        """
        Record the result of a feed fetch attempt.

        Args:
            url: Feed URL
            success: Whether the fetch was successful
            http_code: HTTP status code (if available)
            error: Error message (if failed)
        """
        entry = self._get_entry(url)
        now = datetime.now(timezone.utc).isoformat()
        entry["last_checked_at"] = now
        entry["last_http_code"] = http_code

        if success:
            entry["fail_count"] = 0  # Reset consecutive fail counter
            entry["total_successes"] = entry.get("total_successes", 0) + 1
            entry["last_status"] = "success"
            entry["last_success_at"] = now
            # Re-enable if previously disabled and now succeeding
            if not entry.get("enabled", True):
                entry["enabled"] = True
                entry["disabled_at"] = None
                entry["disabled_reason"] = None
                logger.info(f"Feed re-enabled after success: {url}")
        else:
            entry["fail_count"] = entry.get("fail_count", 0) + 1
            entry["total_failures"] = entry.get("total_failures", 0) + 1
            entry["last_status"] = f"failed: {error or http_code or 'unknown'}"

            # Auto-disable if threshold reached
            if entry["fail_count"] >= FAIL_THRESHOLD and entry.get("enabled", True):
                entry["enabled"] = False
                entry["disabled_at"] = now
                entry["disabled_reason"] = (
                    f"Auto-disabled after {entry['fail_count']} consecutive failures. "
                    f"Last error: {error or http_code or 'unknown'}"
                )
                logger.warning(
                    f"Feed auto-disabled ({entry['fail_count']} failures): {url}"
                )

        self._save()

    def get_enabled_feeds(self, feed_urls: list[str]) -> list[str]:
        """Filter a list of feed URLs, returning only enabled ones."""
        enabled = []
        skipped = []
        for url in feed_urls:
            if self.is_feed_enabled(url):
                enabled.append(url)
            else:
                skipped.append(url)

        if skipped:
            logger.info(
                f"Feed health: {len(enabled)} enabled, {len(skipped)} disabled/skipped"
            )
            for url in skipped[:5]:
                entry = self._get_entry(url)
                logger.debug(
                    f"  Skipped: {url} "
                    f"(fails={entry.get('fail_count', 0)}, "
                    f"reason={entry.get('disabled_reason', 'N/A')})"
                )

        return enabled

    def get_health_report(self) -> dict:
        """
        Generate a summary health report for all tracked feeds.

        Returns:
            Dict with total, enabled, disabled counts and details.
        """
        total = len(self._data)
        enabled = sum(1 for e in self._data.values() if e.get("enabled", True))
        disabled = total - enabled

        # Top failing feeds
        failing = sorted(
            [e for e in self._data.values() if e.get("fail_count", 0) > 0],
            key=lambda e: e.get("fail_count", 0),
            reverse=True,
        )

        return {
            "total_tracked": total,
            "enabled": enabled,
            "disabled": disabled,
            "top_failing": [
                {
                    "url": f.get("url", ""),
                    "fail_count": f.get("fail_count", 0),
                    "last_status": f.get("last_status", ""),
                    "enabled": f.get("enabled", True),
                }
                for f in failing[:10]
            ],
        }

    def reset_feed(self, url: str):
        """Reset a feed's health status (re-enable and clear counters)."""
        if url in self._data:
            self._data[url]["enabled"] = True
            self._data[url]["fail_count"] = 0
            self._data[url]["disabled_at"] = None
            self._data[url]["disabled_reason"] = None
            self._save()
            logger.info(f"Feed health reset: {url}")

    def reset_all(self):
        """Reset all feed health data."""
        self._data = {}
        self._save()
        logger.info("All feed health data reset")


# Module-level singleton
_health_manager: Optional[FeedHealthManager] = None


def get_health_manager() -> FeedHealthManager:
    """Get the module-level FeedHealthManager singleton."""
    global _health_manager
    if _health_manager is None:
        _health_manager = FeedHealthManager()
    return _health_manager
