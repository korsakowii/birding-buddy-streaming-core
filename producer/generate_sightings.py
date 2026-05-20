"""
Synthetic bird sighting producer for Birding Buddy streaming demo.

Emits to `bird_sightings_raw` with configurable mixes of:
- healthy sightings
- duplicate event_id (producer retry simulation)
- late events (event_time earlier than "now", arrives after fresher events)
- invalid payloads (missing fields, bad timestamps, negative counts)
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")


SPECIES = [
    ("amecro", "American Crow"),
    ("bkcchi", "Black-capped Chickadee"),
    ("daejun", "Dark-eyed Junco"),
    ("mallar3", "Mallard"),
    ("goose1", "Canada Goose"),
]
SPECIES_MAP = dict(SPECIES)

LOCATIONS = [
    ("loc_456", 37.8715, -122.273),
    ("loc_712", 37.7699, -122.4682),
    ("loc_903", 47.6062, -122.3321),
]


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def base_sighting(
    *,
    event_id: str,
    user_id: str,
    event_time: datetime,
    species_code: Optional[str] = None,
    common_name: Optional[str] = None,
    location_id: Optional[str] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    count: int = 1,
    confidence: float = 0.92,
    source: str = "manual_log",
) -> dict[str, Any]:
    sp = species_code or random.choice(SPECIES)[0]
    name = common_name or SPECIES_MAP.get(sp, "Unknown species")
    loc = location_id or random.choice(LOCATIONS)[0]
    lat_v = lat if lat is not None else next(ll for l, ll, _ in LOCATIONS if l == loc)
    lng_v = lng if lng is not None else next(ln for l, _, ln in LOCATIONS if l == loc)
    return {
        "event_id": event_id,
        "user_id": user_id,
        "species_code": sp,
        "common_name": name,
        "location_id": loc,
        "lat": lat_v,
        "lng": lng_v,
        "event_time": iso_z(event_time),
        "count": count,
        "confidence": confidence,
        "source": source,
    }


def apply_invalid_variant(record: dict[str, Any], variant: str) -> dict[str, Any]:
    r = dict(record)
    if variant == "missing_user":
        r["user_id"] = ""
    elif variant == "missing_species":
        r["species_code"] = ""
    elif variant == "negative_count":
        r["count"] = -3
    elif variant == "bad_event_time":
        r["event_time"] = "not-a-timestamp"
    return r


def main() -> None:
    p = argparse.ArgumentParser(description="Produce synthetic bird sightings to Kafka/Redpanda.")
    p.add_argument("--rate", type=float, default=5.0, help="Average events per second (poisson-ish sleep).")
    p.add_argument("--dup-rate", type=float, default=0.12, help="Probability of emitting a duplicate event_id.")
    p.add_argument("--late-rate", type=float, default=0.08, help="Probability of emitting a late-arriving record.")
    p.add_argument("--invalid-rate", type=float, default=0.07, help="Probability of emitting an invalid record.")
    p.add_argument("--bootstrap", type=str, default=_DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers.")
    args = p.parse_args()

    from stream_processor.kafka_io import make_producer, produce_json

    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = args.bootstrap
    producer = make_producer()
    topic = "bird_sightings_raw"

    last_ids: list[str] = []
    rng = random.Random(42)

    print(f"generate_sightings: producing to {topic} @ ~{args.rate} evt/s", flush=True)

    invalid_variants = ["missing_user", "missing_species", "negative_count", "bad_event_time"]

    while True:
        now = datetime.now(timezone.utc)
        event_id = f"evt_{uuid.uuid4().hex[:12]}"

        if last_ids and rng.random() < args.dup_rate:
            event_id = rng.choice(last_ids)

        # Late events: backdated event_time, optionally arrive after we've emitted fresher times
        if rng.random() < args.late_rate:
            event_time = now - timedelta(hours=rng.choice([2, 6, 12, 30]))
        else:
            event_time = now - timedelta(seconds=int(rng.normalvariate(30, 20)))

        record = base_sighting(
            event_id=event_id,
            user_id=f"user_{rng.randint(1, 40)}",
            event_time=event_time,
            count=max(1, rng.randint(1, 8)),
        )

        if rng.random() < args.invalid_rate:
            record = apply_invalid_variant(record, rng.choice(invalid_variants))

        partition_key = str(record["location_id"])
        produce_json(producer, topic, key=partition_key, value=record)
        producer.poll(0)

        if event_id not in last_ids:
            last_ids.append(event_id)
            if len(last_ids) > 200:
                last_ids.pop(0)

        sleep_s = 1.0 / max(args.rate, 0.1)
        time.sleep(sleep_s * rng.uniform(0.6, 1.4))


if __name__ == "__main__":
    main()
