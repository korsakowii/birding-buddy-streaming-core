from __future__ import annotations

from datetime import datetime, timedelta, timezone

from producer.generate_sightings import apply_invalid_variant, base_sighting
from stream_processor.streaming_logic import parse_event_time, validate_raw_sighting


def test_validation_flags_missing_fields() -> None:
    ok, issues = validate_raw_sighting({"event_id": "e1"})
    assert ok is False
    codes = {i.code for i in issues}
    assert "missing_user_id" in codes
    assert "missing_species_code" in codes


def test_validation_accepts_good_record() -> None:
    now = datetime.now(timezone.utc)
    r = base_sighting(event_id="evt_ok", user_id="u1", event_time=now, species_code="amecro", common_name="American Crow")
    ok, issues = validate_raw_sighting(r)
    assert ok is True and issues == []


def test_base_sighting_shape() -> None:
    now = datetime(2026, 5, 16, 15, 10, tzinfo=timezone.utc)
    r = base_sighting(event_id="evt_001", user_id="user_123", event_time=now)
    assert r["event_id"] == "evt_001"
    assert r["user_id"] == "user_123"
    assert r["event_time"].endswith("Z")
    assert isinstance(r["count"], int)


def test_invalid_variants_set_expected_errors() -> None:
    now = datetime.now(timezone.utc)
    base = base_sighting(event_id="evt_x", user_id="user_1", event_time=now)
    assert apply_invalid_variant(base, "missing_user")["user_id"] == ""
    assert apply_invalid_variant(base, "missing_species")["species_code"] == ""
    assert apply_invalid_variant(base, "negative_count")["count"] < 0
    assert apply_invalid_variant(base, "bad_event_time")["event_time"] == "not-a-timestamp"


def test_late_style_event_time_is_in_past() -> None:
    now = datetime.now(timezone.utc)
    past = now - timedelta(hours=3)
    r = base_sighting(event_id="evt_late", user_id="user_1", event_time=past)
    parsed = parse_event_time(r["event_time"])
    assert parsed is not None and parsed < now
