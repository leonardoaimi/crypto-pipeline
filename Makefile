COMPOSE = docker-compose -f infra/docker-compose.yml

.PHONY: setup run run-synthetic backfill batch test teardown lint logs

## Build infra images, create Kafka topics, run PostgreSQL init.sql
setup:
	$(COMPOSE) pull --ignore-pull-failures
	$(COMPOSE) up -d zookeeper kafka postgres spark-master spark-worker
	$(COMPOSE) run --rm kafka-init

## Start the full stack in live mode (DATA_SOURCE=live)
run:
	DATA_SOURCE=live $(COMPOSE) up

## Start the full stack in synthetic/offline mode (DATA_SOURCE=synthetic)
run-synthetic:
	DATA_SOURCE=synthetic $(COMPOSE) up

## Run historical REST backfill into PostgreSQL (idempotent)
## Requires Phase 2 (ingestion) and Phase 5 (Spark batch)
backfill:
	@echo "Phase 2 / Phase 5 not yet implemented — run after completing both phases."

## Run the full nightly batch: reconciliation + cross_asset + dbt run + dbt test
## Requires Phase 5 (Spark batch) and Phase 6 (dbt models)
batch:
	@echo "Phase 5 / Phase 6 not yet implemented — run after completing both phases."

## Run pytest and dbt tests
test:
	pytest tests/ -v
	@echo "dbt test: Phase 6 not yet implemented."

## Stop all containers and remove named volumes
teardown:
	$(COMPOSE) down --volumes

## Run linters: black, flake8, sqlfluff
lint:
	black --check .
	flake8 .
	sqlfluff lint transform/ --dialect postgres

## Tail logs for ingestion and Spark services
logs:
	$(COMPOSE) logs -f ingestion spark-master spark-worker
