# Operational tradeoffs

Human-readable summary of design choices and FAQ-style prompts grounded in Birding Buddy Streaming Core.

## Two-minute narrative overview

“I built **Birding Buddy Streaming Core**, a small Kafka-style demo for a real-time birding intelligence product. Producers emit **bird sighting events** from the field—some are **late**, some are **duplicates** from retries, and some are **invalid** on purpose so I can show **data-quality routing**.

“The first job validates and **deduplicates by `event_id`**, writing clean sightings to a curated topic while pushing bad rows to a **dead-letter topic** and operational signals to **data quality events**. That mirrors Flink **keyed state** and **side outputs**.

“Downstream, I compute **15-minute event-time tumbling windows** per **hotspot and species**, with a simplified **watermark** and **allowed lateness** so I can explain what happens when a checklist syncs hours later.

“A third job maintains **user target species preferences**—think a broadcast dimension table—and joins sightings to emit **species alerts**, with **suppression** so users aren’t spammed within the same time/place bucket.

“The code is Python for clarity, running on **Redpanda** locally, but I documented how each piece maps to **Flink** state, windows, and checkpoint offsets so reviewers can connect it to the stack they use.”

## FAQ-style prompts (short answers)

### Why Kafka?
Durable log + replay + fan-out: one **clean sighting stream** feeds multiple independent consumers (hotspots vs alerts) without coupling their release cycles.

### Why Flink?
For **event-time windows**, **managed state**, and **backpressure-aware** execution at scale. This MVP uses Python to keep Docker light, but the **same DAG** is natural in Flink.

### How do you handle duplicates?
**Keyed dedup state** (TTL in production) and **idempotent sinks**. Treat duplicates as expected under *at-least-once*.

### How do you handle late events?
**Watermarks** + **allowed lateness**; too-late events go to **monitoring / side outputs** or a **batch reconciler** depending on requirements.

### How do you design partition keys?
Pick keys that match **hot access patterns** and avoid skew when possible: **`location_id`** for hotspots; **`user_id`** for alert delivery.

### What are the delivery semantics?
Kafka often provides **at-least-once**; end-to-end **exactly-once** needs transactional patterns + **idempotent** processing. Talk honestly about **business-level idempotency**.

### How would you make it production-ready?
Contract tests, auth, key rotation, compaction for preference topics, exactly-once where needed, durable state stores, autoscaling, and **SLO dashboards** on lag + DLQ volume.

### How does streaming relate to downstream analytics work?
Streaming pipelines keep **derived datasets** and **online features** fresh; recurring engineering themes are **quality**, **late data**, **skew**, **replays**, and **governance**.
