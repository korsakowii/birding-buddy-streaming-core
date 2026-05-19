"""Hotspot window key + lateness policy (pure, no broker)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from stream_processor.streaming_logic import (
    hotspot_window_identity,
    should_drop_or_route_late_event,
)


def test_hotspot_window_identity_stable_per_event_time_bucket() -> None:
    et = datetime(2026, 5, 16, 15, 44, tzinfo=timezone.utc)
    assert hotspot_window_identity("loc_456", "bkcchi", et) == (
        "loc_456",
        "bkcchi",
        "2026-05-16T15:30:00Z",
    )


def test_hotspot_lateness_policy_late_matches_job_skip_behavior() -> None:
    """When policy is 'late', hotspot_aggregation_job skips aggregate updates (DQ path)."""
    et = datetime(2026, 5, 16, 12, 10, tzinfo=timezone.utc)
    window_end = datetime(2026, 5, 16, 12, 15, tzinfo=timezone.utc)
    max_lateness = timedelta(minutes=5)
    wm_far = datetime(2026, 5, 16, 12, 30, tzinfo=timezone.utc)
    assert should_drop_or_route_late_event(et, window_end, wm_far, max_lateness) == "late"
