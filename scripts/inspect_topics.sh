#!/usr/bin/env bash
# Print copy-paste rpk consume commands (run on host; broker is localhost:19092).
set -euo pipefail

cat <<'EOF'
# Host broker address (Python producers / confluent-kafka):
export KAFKA_BOOTSTRAP_SERVERS=localhost:19092

# Below: consume via rpk inside the Redpanda container (Kafka API on 127.0.0.1:9092).
# Remove -n to stream continuously; add -n 50 to print a fixed batch.

docker exec -it birding-redpanda rpk topic consume bird_sightings_raw \
  -X brokers=127.0.0.1:9092 -f '%v\n'

docker exec -it birding-redpanda rpk topic consume bird_sightings_clean \
  -X brokers=127.0.0.1:9092 -f '%v\n'

docker exec -it birding-redpanda rpk topic consume hotspot_activity_metrics \
  -X brokers=127.0.0.1:9092 -f '%v\n'

docker exec -it birding-redpanda rpk topic consume species_alerts \
  -X brokers=127.0.0.1:9092 -f '%v\n'

docker exec -it birding-redpanda rpk topic consume data_quality_events \
  -X brokers=127.0.0.1:9092 -f '%v\n'

docker exec -it birding-redpanda rpk topic consume dead_letter_events \
  -X brokers=127.0.0.1:9092 -f '%v\n'
EOF
