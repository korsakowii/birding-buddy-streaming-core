# Failure Scenarios (engineering discussion)

Each scenario is phrased so you can answer: what breaks, what’s the risk, what mitigations apply, and what design tradeoffs matter.

## 1) Duplicate event caused by producer retry

**What happens:** the same `event_id` may arrive twice; without dedup, analytics double-count sightings and users get duplicate alerts.

**Risk:** inflated rarity signals, noisy alerts, incorrect hotspot ranking.

**Handling strategy:** **idempotent sink**, **keyed dedup state** with TTL, and/or **canonical store** (e.g., upsert to OLTP by `event_id`). Emit a `data_quality_events` signal for visibility.

**Engineering takeaway:** *At-least-once is normal; exactly-once is a system property built from idempotent operations + barrier checkpoints—not a magic Kafka flag.*

## 2) Late bird sighting uploaded after a field trip

**What happens:** `event_time` is hours in the past while the record arrives “now”; event-time windows may already be closed.

**Risk:** missing the sighting in aggregates **or** re-opening windows and perturbing stable metrics.

**Handling strategy:** bounded **allowed lateness**, **side outputs** for too-late events, and separate “late refresh” batch jobs when business needs absolute completeness.

**Engineering takeaway:** *watermarks are a completeness/latency trade-off; you pick policies per use case (dashboards vs billing vs science-grade datasets).*

## 3) Invalid record due to missing `species_code`

**What happens:** consumer can’t interpret species semantics; downstream joins mis-key or crash without guardrails.

**Risk:** poison messages blocking processing if uncaught; subtle bias if you “guess” missing fields.

**Handling strategy:** validate early, route to **`dead_letter_events`**, metric the rate in **`data_quality_events`**, and alert the upstream team.

**Engineering takeaway:** *treat validation as a product surface: DLQ volume is SLO-bearing.*

## 4) Popular hotspot causing skew

**What happens:** one `location_id` dominates traffic; a single partition / task becomes hot.

**Risk:** straggler tasks, growing consumer lag, missed SLAs for **unrelated** keys pinned to the same partition if poorly keyed.

**Handling strategy:** **salting** hot keys, **two-phase** aggregate (local combine + shuffle), **split** topics (raw vs regional), and monitoring **per-partition lag**.

**Engineering takeaway:** *partitioning is a physical execution decision, not just a schema detail.*

## 5) Stream processor restart and replay

**What happens:** tasks restart from last committed offsets; some messages are reprocessed.

**Risk:** duplicate side effects if sinks aren’t idempotent; windows may re-emit if state isn’t restored.

**Handling strategy:** **checkpoint** state + offsets; idempotent producers; **upsert** sinks; design outputs with stable keys (`metric_id`, `alert_id`).

**Engineering takeaway:** *replay is a feature—Kafka decouples reprocessing from the source systems, but only if your logic is replay-safe.*

## 6) Slow sink causing backpressure

**What happens:** external API / database can’t keep up; records accumulate in Kafka off-heap paths and operator buffers.

**Risk:** rising lag, GC pressure, eventual consumer timeouts; unbounded in-memory growth in naive apps.

**Handling strategy:** rate limit / bulk writers, autoscale Flink **task parallelism**, bounded buffers with **drop policies** only when explicitly allowed, and consumer tuning (`max.poll.interval.ms`, partition pause when sinks stall).

**Engineering takeaway:** *backpressure is how a healthy system says “slow down”; you need observability on lag and time-in-queue.*
