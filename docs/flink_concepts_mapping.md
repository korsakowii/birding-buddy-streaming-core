# Flink Concepts Mapping

This MVP is **Python + Kafka consumers/producers**, but the structure mirrors how you would implement the same pipeline in **Flink DataStream API**.

## Sighting validation + dedup (`sighting_cleaning_job.py`)

| MVP piece | Flink equivalent |
|-----------|-------------------|
| `validate_raw_sighting` | `map` / `ProcessFunction` with explicit validation |
| Duplicate `event_id` handling | `KeyedProcessFunction` with **`ValueState<Boolean>`** or dedup state with TTL |
| Emit invalid records | **Side output** (`OutputTag`) to a **dead-letter stream** |
| Emit `data_quality_events` | Second side output or downstream `sink` on a quality tag |
| At-least-once + dedup | Kafka offset commits + **idempotent updates** per key |

**Checkpoint mapping:** Flink would snapshot the dedup state and offsets together; after crash, late retries may re-attempt ingestion but state prevents duplicate **downstream** emissions.

## Hotspot window aggregation (`hotspot_aggregation_job.py`)

| MVP piece | Flink equivalent |
|-----------|-------------------|
| `window_bounds_for_event_time` | `TumblingEventTimeWindows.of(Time.minutes(15))` |
| `watermark_from_max_event_time` | `WatermarkStrategy.forBoundedOutOfOrderness(...)` (bounded lateness generator) |
| Per-window `sighting_count` / `unique_birders` | `KeyedProcessFunction` + **`MapState<window, accumulator>`** or built-in `window(…).aggregate(…)` |
| Late beyond allowed lateness | `allowedLateness` window behavior + **side output** for too-late events |

## Target species alerts (`target_species_alert_job.py`)

| MVP piece | Flink equivalent |
|-----------|-------------------|
| Preference updates thread/consumer | **`BroadcastStream`** of preference rules or keyed changelog join |
| In-memory `PreferenceStore` | **`BroadcastState`** (dimension table) or `KeyedCoProcessFunction` |
| Match sighting to interested users | Join keyed by **species** (with fan-out) or geospatial enrich first |
| Alert suppression tuple | Keyed state: `(user_id, species_code, location_id, window_start)` |

**Production twist:** radius filtering should use **spatial joins** or pre-materialized location groups; the schema already carries `radius_km` for that story.

## Delivery semantics (how to speak about them)

- **Kafka producer** defaults are often *at-least-once* unless transactions are enabled.
- **Flink + Kafka** classic pattern: **checkpoint** barriers align application state with source offsets; enable Kafka transactions for **exactly-once** sinks when required.
- This repo demonstrates **why** you still design for **idempotency**: duplicates and replays happen even when frameworks are “correct.”
