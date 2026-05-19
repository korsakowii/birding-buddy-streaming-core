# Sample synthetic outputs

Representative JSON shapes from a **local synthetic run**. Values are illustrative; real runs use generated IDs and timestamps.

---

## `bird_sightings_clean`

Valid, deduplicated sighting (partition key often `location_id`):

```json
{
  "event_id": "evt_a1b2c3d4e5f6",
  "user_id": "user_12",
  "species_code": "bkcchi",
  "common_name": "Black-capped Chickadee",
  "location_id": "loc_456",
  "lat": 37.8715,
  "lng": -122.273,
  "event_time": "2026-05-16T15:37:00Z",
  "count": 2,
  "confidence": 0.92,
  "source": "manual_log"
}
```

---

## `data_quality_events`

**Duplicate `event_id`** (retry simulation — second arrival):

```json
{
  "signal_type": "duplicate_event_id",
  "event_id": "evt_a1b2c3d4e5f6",
  "detail": "Duplicate event_id after retry or at-least-once replay",
  "observed_at": "2026-05-16T15:38:22Z"
}
```

**Late sighting dropped from hotspot aggregation** (watermark-style policy):

```json
{
  "signal_type": "late_event_dropped",
  "detail": "Event arrived beyond allowed lateness after watermark",
  "event_id": "evt_late_field_trip",
  "event_time": "2026-05-16T08:00:00Z",
  "window_end": "2026-05-16T08:15:00Z",
  "watermark": "2026-05-16T15:30:00Z",
  "observed_at": "2026-05-16T15:31:05Z"
}
```

**Validation failure** (paired with DLQ):

```json
{
  "signal_type": "validation_failed",
  "event_id": "evt_bad",
  "issues": [
    {"code": "missing_species_code", "message": "species_code is required"}
  ],
  "observed_at": "2026-05-16T15:39:01Z"
}
```

---

## `dead_letter_events`

Invalid payload retained for diagnosis (`reason_code` discriminates):

```json
{
  "dead_letter_id": "dl_9f3e…",
  "original_topic": "bird_sightings_raw",
  "reason_code": "validation_failed",
  "issues": [
    {"code": "negative_count", "message": "count must be non-negative"}
  ],
  "payload": {
    "event_id": "evt_bad_count",
    "user_id": "user_5",
    "species_code": "amecro",
    "location_id": "loc_712",
    "event_time": "2026-05-16T14:00:00Z",
    "count": -1
  },
  "ingested_at": "2026-05-16T15:39:02Z"
}
```

---

## `hotspot_activity_metrics`

15-minute **event-time** tumbling aggregate for `(location_id, species_code)`:

```json
{
  "metric_id": "hm_c4d8…",
  "location_id": "loc_456",
  "species_code": "bkcchi",
  "window_start": "2026-05-16T15:30:00Z",
  "window_end": "2026-05-16T15:45:00Z",
  "sighting_count": 24,
  "unique_birders": 7,
  "watermark_at_emit": "2026-05-16T15:46:10Z"
}
```

---

## `species_alerts`

Preference match on species (suppressed per user/species/location/window in the MVP):

```json
{
  "alert_id": "al_88b2…",
  "user_id": "user_3",
  "species_code": "bkcchi",
  "location_id": "loc_456",
  "window_start": "2026-05-16T15:30:00Z",
  "window_end": "2026-05-16T15:45:00Z",
  "sighting_event_id": "evt_a1b2c3d4e5f6",
  "detected_at": "2026-05-16T15:38:01Z",
  "priority": "high"
}
```

---

See also `docs/streaming_concepts.md` for how these streams relate.
