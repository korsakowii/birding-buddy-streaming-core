# Streaming concepts (engineering notes)

This document describes **design intent** for Birding Buddy Streaming Core: a **single-node**, **Python + Redpanda** runnable MVP that illustrates Kafka-compatible semantics without claiming production Flink operations.

---

## Why a Kafka-compatible event log

Bird sightings are produced asynchronously (offline capture, delayed sync). Multiple downstream uses (hotspot rollups vs personalized alerts) must evolve independently. A partitioned append-only log provides:

- **Durability** between producer and processor versions  
- **Replay** after logic changes or partial failures  
- **Fan-out** via independent consumer groups  
- Explicit **at-least-once** behavior unless stronger guarantees are engineered end-to-end  

Redpanda speaks the Kafka protocol; the concepts transfer to managed Kafka clusters.

---

## Topic layering: raw / clean / DQ / DLQ

| Layer | Topic(s) | Role |
|-------|-----------|------|
| Raw ingest | `bird_sightings_raw` | Append-only source of truth for synthetic uploads |
| Clean contract | `bird_sightings_clean` | Validated, deduplicated facts safe for analytics joins |
| Operational signals | `data_quality_events` | Dupes, validation summaries, late-event routing signals |
| Poison handling | `dead_letter_events` | Unprocessable payloads + diagnostics |
| Derived outputs | `hotspot_activity_metrics`, `species_alerts` | Windowed metrics and preference-driven alerts |
| Preferences | `target_species_preferences` | Changelog-style dimension updates |

Separation keeps **business outputs** readable while **failure modes** remain observable without corrupting aggregates.

---

## Partition-key choices

- **`location_id`** on sightings (raw → clean path): co-locates hotspot-heavy geography for discussion of locality and skew (not a production partitioning prescription alone).  
- **`user_id`** on alerts: preserves per-recipient ordering where relevant.  

Keys should match dominant read patterns and skew mitigation strategies in a real deployment.

---

## Event-time vs processing-time

- **Event-time** (`event_time`): when the observation occurred — drives tumbling windows and fairness of hotspot metrics.  
- **Processing-time**: wall-clock observation inside jobs — used only indirectly (e.g., synthetic watermark advancement from max seen event-time).  

Late field uploads stress the gap between the two.

---

## Stateful deduplication

Retries can reproduce the same `event_id`. The cleaning job maintains **seen-id state** (in-memory in this MVP). Conceptually this is **keyed state**: Flink would persist it with TTL and checkpoints.

---

## Late events / watermark-style logic

The hotspot job advances a simplified **watermark** from `max(event_time) − allowed_lateness`. Events whose assigned window is sealed under that policy may be counted under **within_lateness**, skipped with **`late_event_dropped`** on `data_quality_events`, or handled differently in product — the MVP chooses explicit signaling over silent drops inside aggregates.

---

## Replay-aware processing

Consumers rely on Kafka offsets; restarting may **re-read** messages. Dedup keys and stable output identifiers reduce duplicated side effects when replay overlaps committed offsets.

---

## DLQ and data-quality side-output pattern

Invalid payloads route to **`dead_letter_events`** with reasons and payloads. Duplicate and policy outcomes emit to **`data_quality_events`**. This mirrors Flink **side outputs**: main stream stays clean; ops telemetry stays parallel.

---

## Preference changelog / dimension-style join

Target species preferences behave like a **slowly changing dimension**. The alert job consumes preference updates into an in-memory store and matches **`species_code`** against clean sightings — analogous to **broadcast state** or a compacted changelog join in Flink.

---

## What would change in a production Flink deployment

- RocksDB **keyed state** + **TTL** instead of unbounded process memory  
- **Watermark strategies** and **window operators** native to the runtime  
- **Checkpointing / savepoints** aligned with Kafka transactional semantics where needed  
- **Parallelism**, rescaling, and metric dashboards for lag and checkpoint duration  

---

## Project boundary (explicit non-claims)

This repo **does not** provide multi-node HA, Schema Registry enforcement, PyFlink execution, Kubernetes deployment, transactional exactly-once sinks, real checklist APIs, or geospatial radius joins — those belong in **`docs/production_hardening.md`** and downstream engineering work.
