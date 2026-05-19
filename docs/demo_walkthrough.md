# Demo walkthrough (local synthetic run)

This guide matches the **Makefile** and `scripts/run_demo.sh`. Run everything from the repo root: `birding-buddy-streaming-core/`.

---

## Terminal 0 — one-time setup

```bash
cd birding-buddy-streaming-core
make setup
source .venv/bin/activate
export KAFKA_BOOTSTRAP_SERVERS=localhost:19092
```

```bash
make test
```

**Expected:** pytest reports all tests passed (validation, dedup, window/watermark helpers).

---

## Terminal 0 — broker

```bash
make start
make list-topics
```

**Expected:** Docker shows `birding-redpanda` healthy; `rpk topic list` includes `bird_sightings_raw`, `bird_sightings_clean`, `hotspot_activity_metrics`, `species_alerts`, `data_quality_events`, `dead_letter_events`, `target_species_preferences`.

---

## Option A — scripted producer launcher

```bash
bash scripts/run_demo.sh
```

**Expected:**

- Compose starts and waits until `rpk cluster info` works.
- Preferences are produced for ~12 seconds (seed data for alerts).
- The sighting producer runs in the **foreground** (stop with Ctrl+C).
- The script prints the **three processor commands** to open next.

---

## Option B — manual producers

**Preferences (keep running or run in bursts):**

```bash
make produce-preferences
```

**Sightings:**

```bash
make produce-sightings
```

**Expected (raw topic):** mostly valid JSON; occasional **missing `user_id` / `species_code`**, **negative `count`**, **`event_time` garbage**, **duplicate `event_id`**, and **older `event_time`** (late relative to “now”).

Quick raw peek:

```bash
docker exec -it birding-redpanda rpk topic consume bird_sightings_raw \
  -X brokers=127.0.0.1:9092 -n 15 -f '%v\n'
```

---

## Terminals 1–3 — processors (order is flexible)

**Cleaning / dedup:**

```bash
make clean-job
```

**Hotspot windows:**

```bash
make hotspot-job
```

**Alerts:**

```bash
make alert-job
```

**Expected (logs):** each job prints it is consuming; no stack traces when the broker is up.

---

## Inspect outputs

Copy-paste helpers:

```bash
bash scripts/inspect_topics.sh
```

Or Makefile shortcuts (streaming; Ctrl+C to exit):

```bash
make consume-clean
make consume-hotspots
make consume-alerts
make consume-dq
make consume-dlq
```

---

## What you should see (and what it proves)

| Observation | Topic | Proof in the story |
|-------------|-------|---------------------|
| Invalid rows **never** appear here | `bird_sightings_clean` | validation gate + DLQ routing |
| **No duplicate `event_id`** in clean | `bird_sightings_clean` | stateful dedup (Flink: keyed state) |
| `signal_type: duplicate_event_id` | `data_quality_events` | duplicates detected after successful first insert |
| `reason_code: validation_failed` / bad JSON | `dead_letter_events` | poison messages isolated |
| `late_event_dropped` (sometimes) | `data_quality_events` | watermark + lateness policy in hotspot path |
| rolling 15-minute buckets | `hotspot_activity_metrics` | event-time tumbling windows |
| alerts for “chase list” species you did **not** self-report | `species_alerts` | preference join + suppression key |

---

## Sixty-second verbal summary

“I run synthetic birders into **raw** with realistic problems—**retries** duplicate `event_id`, **sync delays** create **late event-time**, and **garbage** hits validation. A **cleaning** job validates, **dedupes**, emits **quality signals**, and routes **dead letters**. **Hotspot** metrics use **event-time windows** with a simple **watermark minus lateness** policy. **Alerts** join sightings to a **preference changelog**—that’s my stand-in for **broadcast state**—and suppress repeats in the same place/time bucket. Kafka is the **durable log** so I can **replay** and scale consumers independently.”

---

## Tear down

```bash
make stop
```
