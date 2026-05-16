"""
Job 3: Target species alerts — dynamic preferences + sightings join.

Flink mapping:
- preferences stream: broadcast state (or keyed co-process on user_id if reshuffled).
- sightings stream: keyed by user would be wrong globally; keyed by species_code or location
  is common for fan-out; here MVP does in-memory lookup: active targets by species_code.
- Duplicate suppression: keyed state (user_id, species_code, location_id, window_start).
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stream_processor.kafka_io import json_deserializer, make_consumer, make_producer, produce_json
from stream_processor.streaming_logic import parse_event_time, window_bounds_for_event_time


TOPIC_PREFS = "target_species_preferences"
TOPIC_SIGHTINGS = "bird_sightings_clean"
TOPIC_ALERTS = "species_alerts"

PreferenceKey = Tuple[str, str]  # user_id, species_code
SuppressionKey = Tuple[str, str, str, str]  # user_id, species_code, location_id, window_start_iso


@dataclass
class Preference:
    priority: str
    active: bool
    radius_km: float
    updated_at: str


def load_preference(record: dict[str, Any]) -> Optional[Preference]:
    try:
        return Preference(
            priority=str(record.get("priority", "medium")),
            active=bool(record.get("active", True)),
            radius_km=float(record.get("radius_km", 0)),
            updated_at=str(record.get("updated_at", "")),
        )
    except (TypeError, ValueError):
        return None


class PreferenceStore:
    """Flink analog: BroadcastState / Queryable state from preference changelog."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_user_species: Dict[PreferenceKey, Preference] = {}
        self._species_to_users: Dict[str, Set[str]] = {}

    def apply(self, record: dict[str, Any]) -> None:
        uid = str(record.get("user_id", ""))
        sp = str(record.get("species_code", ""))
        if not uid or not sp:
            return
        pref = load_preference(record)
        if pref is None:
            return
        key: PreferenceKey = (uid, sp)
        with self._lock:
            existed = key in self._by_user_species
            if pref.active:
                self._by_user_species[key] = pref
                self._species_to_users.setdefault(sp, set()).add(uid)
            else:
                self._by_user_species.pop(key, None)
                if sp in self._species_to_users:
                    self._species_to_users[sp].discard(uid)
                    if not self._species_to_users[sp]:
                        del self._species_to_users[sp]

    def users_for_species(self, species_code: str) -> Dict[str, Preference]:
        with self._lock:
            users = list(self._species_to_users.get(species_code, ()))
            out: Dict[str, Preference] = {}
            for u in users:
                p = self._by_user_species.get((u, species_code))
                if p and p.active:
                    out[u] = p
            return out


def preference_consumer_loop(store: PreferenceStore, running: threading.Event) -> None:
    consumer = make_consumer(os.environ.get("KAFKA_GROUP_PREFS_ALERT", "target-species-alert-job-prefs"), [TOPIC_PREFS])
    while not running.is_set():
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            record = json_deserializer(msg.value())
        except Exception:
            continue
        if isinstance(record, dict):
            store.apply(record)
    consumer.close()


def main() -> None:
    store = PreferenceStore()
    stop_running = threading.Event()
    bg = threading.Thread(target=preference_consumer_loop, args=(store, stop_running), daemon=True)
    bg.start()

    group = os.environ.get("KAFKA_GROUP_ALERT", "target-species-alert-job-sightings")
    consumer = make_consumer(group, [TOPIC_SIGHTINGS])
    producer = make_producer()
    suppressed: Set[SuppressionKey] = set()
    running = True

    def stop(*_: Any) -> None:
        nonlocal running
        running = False
        stop_running.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print("target_species_alert_job: consuming", TOPIC_PREFS, "+", TOPIC_SIGHTINGS, flush=True)

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            producer.poll(0)
            continue
        if msg.error():
            print("Consumer error:", msg.error(), flush=True)
            continue

        record = json_deserializer(msg.value())
        if not isinstance(record, dict):
            continue

        species = str(record.get("species_code", ""))
        loc = str(record.get("location_id", ""))
        sighting_user = str(record.get("user_id", ""))
        et = parse_event_time(str(record.get("event_time"))) if record.get("event_time") else None
        if et is None or not species or not loc:
            continue

        w_start, w_end = window_bounds_for_event_time(et)
        w_start_iso = w_start.isoformat().replace("+00:00", "Z")
        w_end_iso = w_end.isoformat().replace("+00:00", "Z")

        for user_id, pref in store.users_for_species(species).items():
            if user_id == sighting_user:
                continue  # MVP: don't alert users to their own checklists
            skey: SuppressionKey = (user_id, species, loc, w_start_iso)
            if skey in suppressed:
                continue
            suppressed.add(skey)

            produce_json(
                producer,
                TOPIC_ALERTS,
                key=user_id,
                value={
                    "alert_id": f"al_{uuid.uuid4()}",
                    "user_id": user_id,
                    "species_code": species,
                    "location_id": loc,
                    "window_start": w_start_iso,
                    "window_end": w_end_iso,
                    "sighting_event_id": record.get("event_id"),
                    "detected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "priority": pref.priority,
                },
            )
            producer.poll(0)

    consumer.close()
    producer.flush()
    print("target_species_alert_job: stopped.", flush=True)


if __name__ == "__main__":
    main()
