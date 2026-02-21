"""
Pipeline Audit Pack.
Tracks pipeline runs, sources used, row counts, and config snapshots
for reproducibility and compliance.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class AuditTracker:
    """Tracks a single pipeline run for audit purposes."""

    def __init__(self, ticker: str, period: Optional[str] = None,
                 run_type: str = "pipeline"):
        self.ticker = ticker
        self.period = period
        self.run_type = run_type
        self.run_id: Optional[UUID] = None
        self.sources: list[dict] = []
        self.row_counts: dict[str, int] = {}
        self.steps: list[dict] = []
        self.config_snapshot: dict = {}
        self.started_at = datetime.now(timezone.utc)

    def start(self, config_snapshot: Optional[dict] = None) -> Optional[UUID]:
        """Start the audit trail. Returns run_id."""
        self.config_snapshot = config_snapshot or {}
        try:
            from ..db import start_pipeline_run
            self.run_id = start_pipeline_run(
                ticker=self.ticker,
                period=self.period,
                run_type=self.run_type,
                config_snapshot=self.config_snapshot,
            )
            logger.info(f"Audit: started run {self.run_id} for {self.ticker}")
            return self.run_id
        except Exception as e:
            logger.warning(f"Audit: could not start run tracking: {e}")
            return None

    def record_source(self, source_type: str, url: str,
                      sha256: Optional[str] = None,
                      status: str = "success") -> None:
        """Record a data source used in this run."""
        self.sources.append({
            "type": source_type,
            "url": url,
            "sha256": sha256,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def record_step(self, step_name: str, status: str = "success",
                    details: Optional[dict] = None) -> None:
        """Record a pipeline step completion."""
        self.steps.append({
            "step": step_name,
            "status": status,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def set_row_count(self, table: str, count: int) -> None:
        """Record row count for a table."""
        self.row_counts[table] = count

    def complete(self, status: str = "completed",
                 error: Optional[str] = None) -> None:
        """Complete the audit trail."""
        if not self.run_id:
            return
        try:
            from ..db import complete_pipeline_run
            complete_pipeline_run(
                run_id=self.run_id,
                status=status,
                sources_json=self.sources,
                row_counts_json=self.row_counts,
                error=error,
            )
            logger.info(f"Audit: completed run {self.run_id} ({status})")
        except Exception as e:
            logger.warning(f"Audit: could not complete run tracking: {e}")

    def get_summary(self) -> dict:
        """Get audit summary for memo appendix."""
        return {
            "run_id": str(self.run_id) if self.run_id else None,
            "ticker": self.ticker,
            "period": self.period,
            "run_type": self.run_type,
            "started_at": self.started_at.isoformat(),
            "sources_count": len(self.sources),
            "steps_completed": len(self.steps),
            "row_counts": self.row_counts,
            "config_snapshot": self.config_snapshot,
        }


def format_audit_report(audit_data: dict) -> str:
    """Format audit data into human-readable report."""
    lines = [
        "═══════════════════════════════════════",
        "AUDIT & REPRODUCIBILITY PACK",
        "═══════════════════════════════════════",
        f"Run ID: {audit_data.get('run_id', 'N/A')}",
        f"Ticker: {audit_data.get('ticker', 'N/A')}",
        f"Period: {audit_data.get('period', 'N/A')}",
        f"Started: {audit_data.get('started_at', 'N/A')}",
        f"Sources: {audit_data.get('sources_count', 0)}",
        f"Steps: {audit_data.get('steps_completed', 0)}",
        "",
        "Row Counts:",
    ]

    for table, count in audit_data.get("row_counts", {}).items():
        lines.append(f"  {table}: {count}")

    return "\n".join(lines)
