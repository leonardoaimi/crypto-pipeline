"""
Spark batch job that reads 90-day historical kline data from the
crypto.ohlcv.historical Kafka topic and writes it to PostgreSQL.
Idempotent — safe to re-run; uses trade_id for deduplication.
"""
