"""
Incremental ingestion with watermark tracking.

Implements:
  - High-water mark persistence (max transaction_date ingested)
  - Lookback window for late-arriving data
  - Cross-batch deduplication by transaction_id
  - Idempotent incremental runs (no duplicate inserts)

In production, the watermark store would be persisted as a Delta table or
key-value store. For this assessment, an in-memory store with JSON
serialization is used for demonstration.
"""
import json
from datetime import datetime, timezone

from config.pipeline_config import LOOKBACK_WINDOW_HOURS


class WatermarkStore:
    """Manages the high-water mark for incremental ingestion."""

    def __init__(self, pipeline_name="transactions_ingestion"):
        self.store = {
            "pipeline_name": pipeline_name,
            "high_water_mark": None,
            "last_run_at": None,
            "total_records_ingested": 0,
            "run_history": [],
        }

    def get_watermark(self):
        """Return the current high-water mark, or None for first run."""
        return self.store["high_water_mark"]

    def update_watermark(self, new_max_date, run_count, run_status="success"):
        """Update the watermark store after a successful ingestion run."""
        self.store["last_run_at"] = datetime.now(timezone.utc).isoformat()
        self.store["run_history"].append({
            "run_at": self.store["last_run_at"],
            "records_ingested": run_count,
            "previous_watermark": self.store["high_water_mark"],
            "new_watermark": new_max_date,
            "status": run_status,
        })
        if new_max_date and (
            self.store["high_water_mark"] is None
            or new_max_date > self.store["high_water_mark"]
        ):
            self.store["high_water_mark"] = new_max_date
        self.store["total_records_ingested"] += run_count

    def compute_effective_watermark(self):
        """
        Apply a lookback window to the watermark for late-arriving data handling.
        Subtracts LOOKBACK_WINDOW_HOURS from the raw watermark.
        """
        raw_wm = self.get_watermark()
        if raw_wm is None:
            return None
        parsed = datetime.strptime(raw_wm, "%Y-%m-%dT%H:%M:%SZ")
        adjusted_ts = parsed.timestamp() - (LOOKBACK_WINDOW_HOURS * 3600)
        adjusted_dt = datetime.fromtimestamp(adjusted_ts, tz=timezone.utc)
        return adjusted_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def to_json(self):
        """Serialize the watermark store to JSON."""
        return json.dumps(self.store, indent=2)


def safe_parse_date(date_str):
    """Safely parse an ISO 8601 UTC date string, returning None if invalid."""
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return None


def deduplicate_against_existing(new_records, existing_records):
    """
    Remove records that already exist (by transaction_id) in the existing set.
    Returns (truly_new, already_seen) tuple.
    """
    existing_ids = {r["transaction_id"] for r in existing_records}
    truly_new = [r for r in new_records if r["transaction_id"] not in existing_ids]
    already_seen = [r for r in new_records if r["transaction_id"] in existing_ids]
    return truly_new, already_seen


def filter_by_watermark(records, effective_watermark):
    """
    Filter records to only those at or after the effective watermark.
    Records with invalid dates are skipped.
    """
    if effective_watermark is None:
        return list(records)
    wm_dt = datetime.strptime(effective_watermark, "%Y-%m-%dT%H:%M:%SZ")
    return [
        r for r in records
        if safe_parse_date(r.get("transaction_date")) is not None
        and safe_parse_date(r["transaction_date"]) >= wm_dt
    ]
