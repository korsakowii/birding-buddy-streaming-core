# Production hardening (what comes after this MVP)

This repo is intentionally small. Moving toward production means replacing demo shortcuts with operable systems: **managed state**, **schemas**, **correctness under failure**, and **SLO-driven monitoring**.

---

## Replace in-memory state with Flink keyed state + TTL

- **Dedup (`event_id`)** and **alert suppression** keys should live in **RocksDB state** (or an equivalent) with **TTL** so memory does not grow forever and replays have clear semantics.
- Encode **key scope** explicitly (`event_id` vs `(user, species, location, window)`).

---

## Use checkpointing and savepoints

- **Checkpoints** bind **Kafka offsets** to **operator state** for crash recovery.
- **Savepoints** enable **code changes**, **rescaling**, and **migration** without losing in-flight window state.

---

## Use schema registry

- Register **Avro / JSON Schema / Protobuf** for every public topic; enforce **compatibility** (backward/forward) in CI.
- Couple **schema versioning** to consumer deployments to prevent “silent decode” incidents.

---

## Use transactional / idempotent sinks

- Prefer **idempotent writes** (`UPSERT` by natural key) where the sink allows it.
- For Kafka producers, enable **idempotence**; for end-to-end exactly-once where required, design **two-phase commit** patterns (Flink Kafka sink + transactions) **and** idempotent business keys.

---

## Monitor consumer lag, checkpoint duration, state size, and backpressure

- **Consumer lag** per partition: catch hotspots and sink slowness early.
- **Checkpoint duration / failures**: flink cluster health is “can we snapshot state on time?”
- **State size growth**: unexpected cardinality explosions (bad keys) show up here first.
- **Backpressure / busy time**: reveals stragglers before lag becomes an outage.

---

## Handle hot keys / hotspot skew

- **Salting** or **splitting** ultra-hot keys; **pre-aggregation** before shuffle; **custom partitioners** with load metrics.
- For birding, a viral rare bird stakeout is the same shape as a flash sale SKU—**one key dominating a partition**.

---

## Add real geospatial radius matching

- Today’s MVP matches **`species_code`** and leaves **`radius_km`** as schema hook.
- Production: **project coordinates**, use **geohash / H3** bucketing, or a **spatial index** to avoid O(users × sightings) scans.

---

## Integrate eBird or real observation sources later

- Treat providers as **bounded + unbounded** mixes: bulk historical backfill + live API/stream.
- Add **provenance** (`source`, `provider_record_id`) and **PII policies** before exposing alerts publicly.
