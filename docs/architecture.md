# Architecture

Birding Buddy Streaming Core is a **small, local** pipeline that shows how a real-world “community sightings” product would **ingest**, **clean**, **aggregate**, and **alert** in near real time.

## End-to-end view

```mermaid
flowchart LR
  subgraph Edge
    Mobile[Field birders / checklist apps]
  end

  subgraph Ingest
    Raw[bird_sightings_raw]
    Prefs[target_species_preferences]
  end

  subgraph Processing
    CleanJob[Sighting cleaning + dedup]
    HotJob[Hotspot window aggregation]
    AlertJob[Target species matcher]
  end

  subgraph Quality
    DLQ[dead_letter_events]
    DQ[data_quality_events]
  end

  subgraph Outputs
    Clean[bird_sightings_clean]
    Metrics[hotspot_activity_metrics]
    Alerts[species_alerts]
  end

  Mobile --> Raw
  Mobile --> Prefs
  Raw --> CleanJob
  CleanJob --> Clean
  CleanJob --> DLQ
  CleanJob --> DQ
  Clean --> HotJob
  Clean --> AlertJob
  Prefs --> AlertJob
  HotJob --> Metrics
  HotJob --> DQ
  AlertJob --> Alerts
```

## Why Kafka (or Kafka-compatible Redpanda) fits here

- **Durable buffer** when processors restart or temporarily fall behind.
- **Replay** for backfills: reprocess historical sightings when alert rules change.
- **Fan-out**: the same clean sighting stream feeds **hotspot metrics** and **personal alerts** without tight coupling.
- **Key-based scaling**: partition keys echo how you would scale real consumers (location hotspots vs user alerts).

## What is intentionally out of scope (MVP)

- Exactly-once end-to-end guarantees (requires transactional Kafka + idempotent sinks + Flink two-phase commit patterns).
- Geo distance joins for `radius_km` (hook is documented; production would use a spatial index / join library).
- Auth, schema registry enforcement, and fine-grained ACLs.

This keeps the demo **honest**: it shows the *shapes* of production problems without pretending to be a full platform.
