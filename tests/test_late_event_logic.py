from __future__ import annotations

from datetime import datetime, timedelta, timezone

from stream_processor.streaming_logic import (
    should_drop_or_route_late_event,
    watermark_from_max_event_time,
    window_bounds_for_event_time,
)


def test_window_alignment_15_minutes() -> None:
    et = datetime(2026, 5, 16, 15, 37, 12, tzinfo=timezone.utc)
    start, end = window_bounds_for_event_time(et)
    assert start == datetime(2026, 5, 16, 15, 30, tzinfo=timezone.utc)
    assert end == datetime(2026, 5, 16, 15, 45, tzinfo=timezone.utc)


def test_watermark_trails_max_event_time_by_lateness() -> None:
    m = datetime(2026, 5, 16, 16, 0, tzinfo=timezone.utc)
    lateness = timedelta(minutes=2)
    wm = watermark_from_max_event_time(m, lateness)
    assert wm == datetime(2026, 5, 16, 15, 58, tzinfo=timezone.utc)


def test_within_lateness_vs_late_routing() -> None:
    et = datetime(2026, 5, 16, 12, 10, tzinfo=timezone.utc)
    window_end = datetime(2026, 5, 16, 12, 15, tzinfo=timezone.utc)
    max_lateness = timedelta(minutes=5)

    wm_close = datetime(2026, 5, 16, 12, 14, tzinfo=timezone.utc)
    assert should_drop_or_route_late_event(et, window_end, wm_close, max_lateness) == "on_time"

    wm_closed = datetime(2026, 5, 16, 12, 15, tzinfo=timezone.utc)
    assert should_drop_or_route_late_event(et, window_end, wm_closed, max_lateness) == "within_lateness"

    wm_far = datetime(2026, 5, 16, 12, 30, tzinfo=timezone.utc)
    assert should_drop_or_route_late_event(et, window_end, wm_far, max_lateness) == "late"