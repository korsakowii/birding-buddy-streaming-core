"""Pure cleaning-path outcomes for tests (no broker)."""

from __future__ import annotations

from typing import Any, Mapping

from stream_processor.streaming_logic import InMemoryDeduplicator, validate_raw_sighting


def cleaning_emission_bucket(record: Mapping[str, Any], deduper: InMemoryDeduplicator) -> str:
    """
    Mirror sighting_cleaning_job routing for valid paths:
    dlq | dq_duplicate | clean
    """
    valid, _ = validate_raw_sighting(record)
    if not valid:
        return "dlq"
    event_id = record.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        return "dlq"
    if deduper.is_duplicate(event_id):
        return "dq_duplicate"
    return "clean"

