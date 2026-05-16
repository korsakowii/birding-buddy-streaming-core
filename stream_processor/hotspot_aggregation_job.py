"""
Job 2: Hotspot aggregation — 15-minute event-time tumbling windows.

Flink mapping:
- KeyBy(location_id, species_code) — co-locates hotspot metrics on same subtask.
- Event-time windows with TumblingEventTimeWindows.of(Time.minutes(15)).
- Watermark strategy: WM = max_timestamp - allowed_lateness (see wm_generator in Flink).
- Triggers on watermark passing window end; AllowedLateness updates still merge until dropped.
"""

from __future__ import annotations

import os
import signal
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stream_processor.kafka_io import json_deserializer, make_consumer, make_producer, produce_json
from stream_processor.streaming_logic import (
    parse_event_time,
    watermark_from_max_event_time,
    should_drop_or_route_late_event,
    window_bounds_for_event_time,
)


TOPIC_IN = "bird_sightings_clean"
TOPIC_OUT = "hotspot_activity_metrics"
TOPIC_QUALITY = "data_quality_events"

WindowKey = Tuple[str, str, str]  # location_id, species_code, window_start_iso


@dataclass
class WindowAgg:
    sighting_count: int = 0
    unique_birders: Set[str] = field(default_factory=set)


def main() -> None:
    allowed_lateness = timedelta(seconds=int(os.environ.get("HOTSPOT_ALLOWED_LATENESS_SEC", "120")))
    group = os.environ.get("KAFKA_GROUP_HOTSPOT", "hotspot-aggregation-job")

    consumer = make_consumer(group, [TOPIC_IN])
    producer = make_producer()
    windows: Dict[WindowKey, WindowAgg] = {}
    max_event_time: Optional[datetime] = None
    running = True

    def stop(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print("hotspot_aggregation_job: consuming", TOPIC_IN, flush=True)

    def emit_completed_windows() -> None:
        nonlocal windows, max_event_time
        wm = watermark_from_max_event_time(max_event_time, allowed_lateness)
        if wm is None:
            return
        to_delete: list[WindowKey] = []
        for key, agg in windows.items():
            location_id, species_code, ws_iso = key
            window_start = parse_event_time(ws_iso)
            if window_start is None:
                to_delete.append(key)
                continue
            _, window_end = window_bounds_for_event_time(window_start)
            if window_end <= wm:
                produce_json(
                    producer,
                    TOPIC_OUT,
                    key=location_id,
                    value={
                        "metric_id": f"hm_{uuid.uuid4()}",
                        "location_id": location_id,
                        "species_code": species_code,
                        "window_start": window_start.isoformat().replace("+00:00", "Z"),
                        "window_end": window_end.isoformat().replace("+00:00", "Z"),
                        "sighting_count": agg.sighting_count,
                        "unique_birders": len(agg.unique_birders),
                        "watermark_at_emit": wm.isoformat().replace("+00:00", "Z"),
                    },
                )
                to_delete.append(key)
        for k in to_delete:
            del windows[k]

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            emit_completed_windows()
            producer.poll(0)
            continue
        if msg.error():
            print("Consumer error:", msg.error(), flush=True)
            continue

        record = json_deserializer(msg.value())
        et_raw = record.get("event_time")
        et = parse_event_time(str(et_raw)) if et_raw is not None else None
        if et is None:
            continue

        if max_event_time is None or et > max_event_time:
            max_event_time = et

        location_id = str(record.get("location_id", ""))
        species_code = str(record.get("species_code", ""))
        user_id = str(record.get("user_id", ""))

        w_start, w_end = window_bounds_for_event_time(et)
        wm = watermark_from_max_event_time(max_event_time, allowed_lateness)
        late_status = should_drop_or_route_late_event(et, w_end, wm, allowed_lateness)

        if late_status == "late":
            produce_json(
                producer,
                TOPIC_QUALITY,
                key=location_id,
                value={
                    "signal_type": "late_event_dropped",
                    "detail": "Event arrived beyond allowed lateness after watermark",
                    "event_id": record.get("event_id"),
                    "event_time": et.isoformat().replace("+00:00", "Z"),
                    "window_end": w_end.isoformat().replace("+00:00", "Z"),
                    "watermark": wm.isoformat().replace("+00:00", "Z") if wm else None,
                    "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                },
            )
            producer.poll(0)
            emit_completed_windows()
            continue

        key: WindowKey = (location_id, species_code, w_start.isoformat().replace("+00:00", "Z"))
        agg = windows.setdefault(key, WindowAgg())
        raw_count = record.get("count", 1)
        if isinstance(raw_count, int) and raw_count >= 0:
            delta = raw_count
        else:
            delta = 1
        agg.sighting_count += delta
        if user_id:
            agg.unique_birders.add(user_id)

        emit_completed_windows()
        producer.poll(0)

    consumer.close()
    producer.flush()
    print("hotspot_aggregation_job: stopped.", flush=True)


if __name__ == "__main__":
    main()
