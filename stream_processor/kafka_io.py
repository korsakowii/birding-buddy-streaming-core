"""Shared Kafka helpers for local Redpanda (Kafka protocol)."""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Optional

from confluent_kafka import Consumer, Producer


def default_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")


def json_serializer(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")


def json_deserializer(raw: Optional[bytes]) -> Any:
    if raw is None:
        return None
    return json.loads(raw.decode("utf-8"))


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": default_bootstrap_servers(),
            "linger.ms": 5,
            "compression.type": "lz4",
        }
    )


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        print(f"Delivery failed for {msg.topic()}: {err}")


def produce_json(
    producer: Producer,
    topic: str,
    key: Optional[str],
    value: dict[str, Any],
    on_delivery: Callable[[Any, Any], None] = delivery_report,
) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8") if key is not None else None,
        value=json_serializer(value),
        on_delivery=on_delivery,
    )
    producer.poll(0)


def make_consumer(group_id: str, topics: list[str]) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": default_bootstrap_servers(),
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    c.subscribe(topics)
    return c
