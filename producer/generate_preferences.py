"""
Synthetic target-species preference producer.

Emits changelog-style records to `target_species_preferences`.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stream_processor.kafka_io import default_bootstrap_servers, make_producer, produce_json


SPECIES = ["bkcchi", "amecro", "daejun", "mallar3"]


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def preference_doc(
    *,
    user_id: str,
    species_code: str,
    priority: str,
    radius_km: float,
    active: bool,
    updated_at: datetime,
) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "species_code": species_code,
        "priority": priority,
        "radius_km": radius_km,
        "active": active,
        "updated_at": iso_z(updated_at),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Produce synthetic target species preferences.")
    p.add_argument("--rate", type=float, default=0.5, help="Average preference updates per second.")
    p.add_argument("--bootstrap", type=str, default=default_bootstrap_servers(), help="Kafka bootstrap servers.")
    args = p.parse_args()

    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = args.bootstrap
    producer = make_producer()
    topic = "target_species_preferences"
    rng = random.Random(7)

    print(f"generate_preferences: producing to {topic} @ ~{args.rate} evt/s", flush=True)

    while True:
        now = datetime.now(timezone.utc)
        uid = f"user_{rng.randint(1, 40)}"
        species = rng.choice(SPECIES)
        active = rng.random() > 0.08
        record = preference_doc(
            user_id=uid,
            species_code=species,
            priority=rng.choice(["low", "medium", "high"]),
            radius_km=float(rng.choice([5, 15, 30, 50])),
            active=active,
            updated_at=now,
        )
        produce_json(producer, topic, key=uid, value=record)
        producer.poll(0)
        time.sleep(1.0 / max(args.rate, 0.05) * rng.uniform(0.7, 1.3))


if __name__ == "__main__":
    main()
