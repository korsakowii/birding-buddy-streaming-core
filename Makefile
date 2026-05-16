# Birding Buddy Streaming Core — local demo helpers
# Requires: Docker, Python 3.10+

.DEFAULT_GOAL := help

PYTHON      := .venv/bin/python
PIP         := .venv/bin/pip
export KAFKA_BOOTSTRAP_SERVERS ?= localhost:19092

REDPANDA_CONTAINER := birding-redpanda
RPK_INTERNAL       := docker exec $(REDPANDA_CONTAINER) rpk
RPK_BROKERS        := -X brokers=127.0.0.1:9092

.PHONY: help setup start stop test \
	produce-sightings produce-preferences \
	clean-job hotspot-job alert-job \
	list-topics consume-clean consume-hotspots consume-alerts consume-dq consume-dlq

help:
	@echo "Targets: setup | start | stop | test | produce-* | *-job | list-topics | consume-*"

setup:
	python3 -m venv .venv
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt
	@echo "Setup done. Activate: source .venv/bin/activate"

check-venv:
	@test -x $(PYTHON) || (echo "Run 'make setup' first to create .venv" >&2 && exit 1)

start:
	docker compose up -d

stop:
	docker compose down

test: check-venv
	$(PYTHON) -m pytest tests/ -q

produce-sightings: check-venv
	$(PYTHON) producer/generate_sightings.py --rate 5

produce-preferences: check-venv
	$(PYTHON) producer/generate_preferences.py --rate 0.5

clean-job: check-venv
	$(PYTHON) -m stream_processor.sighting_cleaning_job

hotspot-job: check-venv
	$(PYTHON) -m stream_processor.hotspot_aggregation_job

alert-job: check-venv
	$(PYTHON) -m stream_processor.target_species_alert_job

list-topics:
	$(RPK_INTERNAL) topic list $(RPK_BROKERS)

consume-clean:
	docker exec -it $(REDPANDA_CONTAINER) rpk topic consume bird_sightings_clean $(RPK_BROKERS) -f '%v\n'

consume-hotspots:
	docker exec -it $(REDPANDA_CONTAINER) rpk topic consume hotspot_activity_metrics $(RPK_BROKERS) -f '%v\n'

consume-alerts:
	docker exec -it $(REDPANDA_CONTAINER) rpk topic consume species_alerts $(RPK_BROKERS) -f '%v\n'

consume-dq:
	docker exec -it $(REDPANDA_CONTAINER) rpk topic consume data_quality_events $(RPK_BROKERS) -f '%v\n'

consume-dlq:
	docker exec -it $(REDPANDA_CONTAINER) rpk topic consume dead_letter_events $(RPK_BROKERS) -f '%v\n'
