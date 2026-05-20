"""Cleaning-path routing without Kafka (mirrors sighting_cleaning_job semantics)."""

from __future__ import annotations

from datetime import datetime, timezone

from producer.generate_sightings import apply_invalid_variant, base_sighting
from stream_processor.streaming_logic import InMemoryDeduplicator

from tests.cleaning_pipeline import cleaning_emission_bucket


def test_duplicate_second_observation_not_clean_twice() -> None:
    deduper = InMemoryDeduplicator()
    now = datetime(2026, 5, 16, 15, 10, tzinfo=timezone.utc)
    record = base_sighting(event_id="evt_dup", user_id="user_1", event_time=now)
    assert cleaning_emission_bucket(record, deduper) == "clean"
    assert cleaning_emission_bucket(dict(record), deduper) == "dq_duplicate"


def test_invalid_negative_count_routes_to_dlq_bucket() -> None:
    deduper = InMemoryDeduplicator()
    now = datetime.now(timezone.utc)
    bad = apply_invalid_variant(
        base_sighting(event_id="evt_neg", user_id="user_1", event_time=now),
        "negative_count",
    )
    assert cleaning_emission_bucket(bad, deduper) == "dlq"


def test_invalid_missing_species_routes_to_dlq_bucket() -> None:
    deduper = InMemoryDeduplicator()
    now = datetime.now(timezone.utc)
    bad = apply_invalid_variant(
        base_sighting(event_id="evt_ms", user_id="user_1", event_time=now),
        "missing_species",
    )
    assert cleaning_emission_bucket(bad, deduper) == "dlq"
