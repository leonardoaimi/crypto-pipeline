"""
Spark Structured Streaming job that consumes crypto.trades.raw and aggregates
1-minute and 5-minute OHLCV bars via tumbling windows (watermark 30 s).
Sinks results to PostgreSQL via foreachBatch and emits to crypto.ohlcv.streaming.
"""
