"""Pure streaming primitives shared by jobs and tests (event-time, windows, validation)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Set, Tuple


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str


def parse_event_time(value: str) -> Optional[datetime]:
    """Parse ISO-8601 timestamps with optional Z suffix."""
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def validate_raw_sighting(record: Mapping[str, Any]) -> Tuple[bool, list[ValidationIssue]]:
    """Return (is_valid, issues) for bird sighting ingestion (MVP checks, JSON shapes)."""
    issues: list[ValidationIssue] = []

    if not record.get("user_id"):
        issues.append(ValidationIssue("missing_user_id", "user_id is required"))
    if not record.get("species_code"):
        issues.append(ValidationIssue("missing_species_code", "species_code is required"))

    count = record.get("count")
    if count is not None:
        if not isinstance(count, int):
            issues.append(ValidationIssue("invalid_count_type", "count must be an integer"))
        elif count < 0:
            issues.append(ValidationIssue("negative_count", "count must be non-negative"))

    et = record.get("event_time")
    if et is None:
        issues.append(ValidationIssue("missing_event_time", "event_time is required"))
    elif parse_event_time(str(et)) is None:
        issues.append(ValidationIssue("invalid_event_time", "event_time must be ISO-8601"))

    event_id = record.get("event_id")
    if not event_id:
        issues.append(ValidationIssue("missing_event_id", "event_id is required"))

    location_id = record.get("location_id")
    if not location_id:
        issues.append(ValidationIssue("missing_location_id", "location_id is required"))

    return (len(issues) == 0), issues


WINDOW_SECONDS = 15 * 60


def hotspot_window_identity(
    location_id: str,
    species_code: str,
    event_time_utc: datetime,
    window_seconds: int = WINDOW_SECONDS,
) -> Tuple[str, str, str]:
    """Stable key for hotspot aggregation: (location_id, species_code, window_start_iso_Z)."""
    w_start, _ = window_bounds_for_event_time(event_time_utc, window_seconds)
    return (location_id, species_code, w_start.isoformat().replace("+00:00", "Z"))


def window_bounds_for_event_time(event_time_utc: datetime, window_seconds: int = WINDOW_SECONDS) -> Tuple[datetime, datetime]:
    """Tumbling event-time window [start, end) in UTC, aligned to the epoch."""
    if event_time_utc.tzinfo is None:
        event_time_utc = event_time_utc.replace(tzinfo=timezone.utc)
    et = event_time_utc.astimezone(timezone.utc)
    epoch = int(et.timestamp())
    start_epoch = epoch - (epoch % window_seconds)
    start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end = start + timedelta(seconds=window_seconds)
    return start, end


def watermark_from_max_event_time(
    max_event_time: Optional[datetime],
    allowed_lateness: timedelta,
) -> Optional[datetime]:
    """Simplified Flink-style watermark: WM = max(event_time) - allowed_lateness."""
    if max_event_time is None:
        return None
    return max_event_time - allowed_lateness


def is_event_late_for_closed_window(
    event_time: datetime,
    window_end: datetime,
    watermark: Optional[datetime],
) -> bool:
    """True if the window is already 'closed' by the watermark (event would be dropped or side-output in Flink)."""
    if watermark is None:
        return False
    return event_time < window_end <= watermark


def should_drop_or_route_late_event(
    event_time: datetime,
    window_end: datetime,
    watermark: Optional[datetime],
    max_lateness: timedelta,
) -> str:
    """
    MVP policy: within allowed lateness after window end, still count; beyond -> late.
    Returns: 'on_time' | 'within_lateness' | 'late'
    """
    if watermark is None:
        return "on_time"
    if watermark < window_end:
        return "on_time"
    if event_time >= window_end:
        return "on_time"
    # Event belongs to a window that has been closed by WM
    if watermark <= event_time + max_lateness:
        return "within_lateness"
    return "late"


class InMemoryDeduplicator:
    """
    Flink analog: keyed state per event_id.
    Production: TTL + RocksDB state backend; replay still needs idempotent sinks or out-of-band dedup.
    """

    def __init__(self) -> None:
        self._seen: Set[str] = set()

    def is_duplicate(self, event_id: str) -> bool:
        if event_id in self._seen:
            return True
        self._seen.add(event_id)
        return False
