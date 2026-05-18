"""
Fetches 90-day historical OHLCV klines from the Binance REST API and publishes
them to the crypto.ohlcv.historical Kafka topic. Used by the historical
backfill Spark job (Phase 5).
"""
