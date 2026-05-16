#!/usr/bin/env bash
# Starts Redpanda, seeds preferences briefly, runs the sighting producer, and prints next steps.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker not found" >&2
  exit 1
fi

echo "==> Starting Docker Compose (Redpanda)"
docker compose up -d

echo "==> Waiting for Redpanda (cluster info)"
for i in $(seq 1 60); do
  if docker exec birding-redpanda \
    rpk cluster info -X brokers=127.0.0.1:9092 >/dev/null 2>&1; then
    break
  fi
  if [[ "$i" -eq 60 ]]; then
    echo "error: timeout waiting for Redpanda" >&2
    docker compose ps
    exit 1
  fi
  sleep 2
done

echo "==> Waiting for demo topics"
for i in $(seq 1 45); do
  if docker exec birding-redpanda rpk topic list -X brokers=127.0.0.1:9092 2>/dev/null \
    | grep -q bird_sightings_raw; then
    break
  fi
  if [[ "$i" -eq 45 ]]; then
    echo "warning: topics not listed yet — check: docker compose logs redpanda-init" >&2
  fi
  sleep 1
done

if [[ ! -x "$ROOT/.venv/bin/python" ]]; then
  echo "error: missing $ROOT/.venv/bin/python — run: make setup" >&2
  exit 1
fi

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

echo ""
echo "----------------------------------------------------------------"
echo "  Open THREE separate terminals from the repo root, with venv:"
echo "    source .venv/bin/activate"
echo "    export KAFKA_BOOTSTRAP_SERVERS=localhost:19092"
echo ""
echo "  Terminal A — cleaning / dedup:"
echo "    make clean-job"
echo ""
echo "  Terminal B — hotspot windows:"
echo "    make hotspot-job"
echo ""
echo "  Terminal C — target-species alerts:"
echo "    make alert-job"
echo ""
echo "  Inspect output (optional extra terminals):"
echo "    make consume-clean | make consume-hotspots | make consume-alerts"
echo "    make consume-dq | make consume-dlq"
echo "    bash scripts/inspect_topics.sh"
echo "----------------------------------------------------------------"
echo ""

echo "==> Seeding preferences (~12s at elevated rate)"
"$ROOT/.venv/bin/python" "$ROOT/producer/generate_preferences.py" --rate 3 &
PREFS_PID=$!
sleep 12
kill "$PREFS_PID" 2>/dev/null || true
wait "$PREFS_PID" 2>/dev/null || true

echo "==> Starting sighting producer (Ctrl+C to stop)"
echo "    Duplicate / late / invalid events are intentional — watch make consume-dq / consume-dlq"
exec "$ROOT/.venv/bin/python" "$ROOT/producer/generate_sightings.py" --rate 5
