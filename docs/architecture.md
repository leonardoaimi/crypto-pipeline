# Architecture

> Work in progress — diagram will be added after Phase 4 (Spark Streaming).

## Data flow

```
Binance WS / Synthetic
        │
        ▼
  ingestion service
        │
        ▼
  Kafka topics
  ├── crypto.trades.raw
  ├── crypto.ohlcv.streaming
  ├── crypto.ohlcv.historical
  ├── crypto.tickers.raw
  └── crypto.trades.dlq
        │
        ▼
  Spark Structured Streaming
  ├── trade_stream  → PostgreSQL (ohlcv_1m, ohlcv_5m)
  └── volume_stream → PostgreSQL (volume_ratio)
        │
        ▼
  Spark Batch
  ├── historical_backfill     → PostgreSQL
  ├── ohlcv_reconciliation    → PostgreSQL (reconciliation_log)
  └── cross_asset_stats       → PostgreSQL
        │
        ▼
      dbt
  ├── staging   (views)
  ├── intermediate (views)
  └── marts     (tables)
        │
        ▼
  Dashboard (Grafana / Streamlit / Evidence)
```
