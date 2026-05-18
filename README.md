# Crypto Market Data Pipeline

A production-grade data engineering portfolio project that ingests real-time
cryptocurrency trade and OHLCV data from public exchange WebSocket and REST APIs,
streams events through Kafka, processes them with Spark Structured Streaming and
batch jobs, models analytics with dbt, and serves insights through a dashboard.
Everything runs locally via `docker-compose up` — no cloud services required.

---

## Architecture

![Architecture](docs/architecture.png)

> **Work in progress** — diagram will be added after Phase 4 (Spark Streaming).
> See [docs/architecture.md](docs/architecture.md) for the text-based data-flow overview.

---

## Tech stack

| Layer              | Technology                          |
|--------------------|-------------------------------------|
| Ingestion          | Python 3.11 (`websockets`, `httpx`) |
| Event streaming    | Apache Kafka + Zookeeper            |
| Stream processing  | Apache Spark Structured Streaming   |
| Batch processing   | Apache Spark (PySpark)              |
| Orchestration      | Apache Airflow (nice-to-have)       |
| Transformation     | dbt (postgres adapter)              |
| Storage — hot      | PostgreSQL 15                       |
| Storage — cold     | Parquet files (local volume)        |
| Containerisation   | Docker + Docker Compose             |
| Dashboard          | Grafana (default) — swappable       |
| Testing            | pytest, dbt tests                   |

---

## Quickstart

### Prerequisites

- Docker Desktop ≥ 4.x
- `make`

### Live mode (real Binance data)

```bash
cp .env.example .env
# Review .env — default credentials are fine for local use
make setup      # pull images, create Kafka topics, init PostgreSQL
make run        # start the full stack
```

### Synthetic mode (no internet required)

```bash
cp .env.example .env
# Edit .env: DATA_SOURCE=synthetic
make setup
make run-synthetic
```

### Dashboard

The active dashboard is controlled by `DASHBOARD_TYPE` in `.env`:

```
DASHBOARD_TYPE=grafana    # default — http://localhost:3000
COMPOSE_PROFILES=grafana  # must match DASHBOARD_TYPE
```

Options: `grafana` · `streamlit` · `evidence`

### Teardown

```bash
make teardown   # stop containers and remove all volumes
```

---

## Data sources

> **Work in progress** — implement in Phase 2.

---

## Pipeline architecture

> **Work in progress** — document after Phase 4.

---

## Design decisions

> **Work in progress** — document key tradeoffs after each phase:
> - Decimal vs float for price fields
> - Watermark threshold (30 s)
> - foreachBatch vs native streaming sink
> - Idempotency strategy in historical backfill
> - Historical preferred over streaming in `int_ohlcv_unified`

---

## Data quality

> **Work in progress** — document dbt tests and reconciliation strategy after Phase 6.
