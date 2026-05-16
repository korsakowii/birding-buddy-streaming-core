"""
Job 1: Raw sighting ingestion, validation, deduplication.

Flink mapping (comments inline):
- KeyBy(event_id) -> ValueState[Boolean] for "seen" (or MapState with TTL in production).
- Side outputs: invalid -> dead-letter stream; quality signals -> side output topic.
- Kafka offset + checkpoint => at-least-once; dedup makes output effectively effectively-once for keys.
"""

from __future__ import annotations

import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, Set

# Ensure repo root is importable when running `python stream_processor/sighting_cleaning_job.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stream_processor.kafka_io import json_deserializer, make_consumer, make_producer, produce_json
from stream_processor.streaming_logic import validate_raw_sighting


TOPIC_IN = "bird_sightings_raw"
TOPIC_CLEAN = "bird_sightings_clean"
TOPIC_DLQ = "dead_letter_events"
TOPIC_QUALITY = "data_quality_events"


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


def main() -> None:
    group = os.environ.get("KAFKA_GROUP_SIGHTING_CLEAN", "sighting-cleaning-job")
    consumer = make_consumer(group, [TOPIC_IN])
    producer = make_producer()
    dedup = InMemoryDeduplicator()
    running = True

    def stop(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print("sighting_cleaning_job: consuming", TOPIC_IN, flush=True)

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error(), flush=True)
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        try:
            record = json_deserializer(msg.value())
        except Exception as exc:  # noqa: BLE001 — MVP: route bad JSON to DLQ
            produce_json(
                producer,
                TOPIC_DLQ,
                key=key,
                value={
                    "dead_letter_id": f"dl_{uuid.uuid4()}",
                    "original_topic": TOPIC_IN,
                    "reason_code": "json_parse_error",
                    "detail": str(exc),
                    "raw_preview": (msg.value() or b"")[:512].decode("utf-8", errors="replace"),
                    "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                },
            )
            producer.flush()
            continue

        if not isinstance(record, dict):
            produce_json(
                producer,
                TOPIC_DLQ,
                key=key,
                value={
                    "dead_letter_id": f"dl_{uuid.uuid4()}",
                    "original_topic": TOPIC_IN,
                    "reason_code": "invalid_record_type",
                    "detail": "Record JSON must be an object",
                    "payload": record,
                    "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                },
            )
            producer.flush()
            continue

        valid, issues = validate_raw_sighting(record)
        event_id: Optional[str] = record.get("event_id") if isinstance(record.get("event_id"), str) else None

        if not valid:
            produce_json(
                producer,
                TOPIC_DLQ,
                key=record.get("location_id") or key,
                value={
                    "dead_letter_id": f"dl_{uuid.uuid4()}",
                    "original_topic": TOPIC_IN,
                    "reason_code": "validation_failed",
                    "issues": [i.__dict__ for i in issues],
                    "payload": record,
                    "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                },
            )
            produce_json(
                producer,
                TOPIC_QUALITY,
                key=record.get("location_id") or key,
                value={
                    "signal_type": "validation_failed",
                    "event_id": event_id,
                    "issues": [i.__dict__ for i in issues],
                    "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                },
            )
            producer.flush()
            continue

        assert event_id is not None
        if dedup.is_duplicate(event_id):
            produce_json(
                producer,
                TOPIC_QUALITY,
                key=record.get("location_id"),
                value={
                    "signal_type": "duplicate_event_id",
                    "event_id": event_id,
                    "detail": "Duplicate event_id after retry or at-least-once replay",
                    "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                },
            )
            producer.poll(0)
            continue

        partition_key = str(record.get("location_id", ""))
        produce_json(producer, TOPIC_CLEAN, key=partition_key, value=record)
        producer.poll(0)

    consumer.close()
    producer.flush()
    print("sighting_cleaning_job: stopped.", flush=True)


if __name__ == "__main__":
    main()
