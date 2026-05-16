# Kafka Topics

Environment default: **Kafka protocol** broker at `KAFKA_BOOTSTRAP_SERVERS` (compose maps **19092** on the host).

Below, **key** is the Kafka message key (bytes) producers choose for ordering / co-partitioning.

---

## `bird_sightings_raw` (input)

**Purpose:** Append-only ingestion of checklist-style bird sighting events (mix of good, late, duplicate, invalid for demos).

**Key:** `location_id` — co-partitions hotspot-heavy locations so later `hotspot_aggregation_job` could align partitions (production would validate skew).

**Value schema:** JSON per `schemas/bird_sighting.schema.json`.

**Retention assumption:** short–medium (e.g., **24h–7d**) in MVP; longer in production for replay/backfill with tiered storage.

**Example event:**

```json
{
  "event_id": "evt_001",
  "user_id": "user_123",
  "species_code": "amecro",
  "common_name": "American Crow",
  "location_id": "loc_456",
  "lat": 37.8715,
  "lng": -122.273,
  "event_time": "2026-05-16T15:10:00Z",
  "count": 3,
  "confidence": 0.92,
  "source": "manual_log"
}
```

---

## `target_species_preferences` (input)

**Purpose:** Changelog of what a user is “chasing” (target species), including deactivations (`active: false`).

**Key:** `user_id` — keeps a single user’s preference updates ordered per partition.

**Value schema:** JSON per `schemas/target_species_preference.schema.json`.

**Retention assumption:** compacted topic in production; for the MVP demo, **7–30d** is enough.

**Example event:**

```json
{
  "user_id": "user_123",
  "species_code": "bkcchi",
  "priority": "high",
  "radius_km": 30,
  "active": true,
  "updated_at": "2026-05-16T10:00:00Z"
}
```

## `bird_sightings_clean` (output / intermediate)

**Purpose:** Validated, deduplicated sightings ready for analytics consumers.

**Key:** `location_id`

**Value schema:** same as raw when valid (schema still `bird_sighting.schema.json`).

**Retention assumption:** **7–30d** typical for feature stores / analytics sinks.

---

## `hotspot_activity_metrics` (output)

**Purpose:** 15-minute **event-time** tumbling metrics per `(location_id, species_code)`.

**Key:** `location_id`

**Value schema:** JSON object (documented shape — no separate JSON schema file in MVP):

```json
{
  "metric_id": "hm_…",
  "location_id": "loc_456",
  "species_code": "bkcchi",
  "window_start": "2026-05-16T15:30:00Z",
  "window_end": "2026-05-16T15:45:00Z",
  "sighting_count": 12,
  "unique_birders": 5,
  "watermark_at_emit": "2026-05-16T15:46:00Z"
}
```

**Retention assumption:** **30–180d** if used for dashboards / seasonal comparisons.

---

## `species_alerts` (output)

**Purpose:** User-facing “your target species was reported” notifications (suppressed per user/species/location/window in MVP).

**Key:** `user_id` — keeps per-user alert ordering.

**Value schema:** JSON per `schemas/species_alert.schema.json` (fields may include extras like `metric_id` in future).

**Retention assumption:** short (**24h–7d**) if alerts are also persisted in an app database.

---

## `data_quality_events` (output / observability)

**Purpose:** Operational telemetry for *data issues*, not bird facts: duplicates, validation failures, late events dropped from aggregation, etc.

**Key:** usually `location_id` (or `user_id` when that’s the closest dimension).

**Value schema:** JSON with `signal_type` discriminator (MVP is flexible JSON).

**Retention assumption:** **30–90d** to power monitoring; can be sampled in prod.

---

## `dead_letter_events` (output / DLQ)

**Purpose:** Poison pill records that cannot be safely interpreted as valid sightings.

**Key:** `location_id` when present (else omitted / random partition).

**Value schema:** JSON with `reason_code`, `payload` / `raw_preview`, and `ingested_at`.

**Retention assumption:** longer (**90d+**) for incident forensics; may be encrypted / access-controlled in prod.
