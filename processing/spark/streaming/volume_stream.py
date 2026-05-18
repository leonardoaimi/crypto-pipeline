"""
Spark Structured Streaming job that computes the buy/sell volume ratio from
crypto.trades.raw using a 10-minute sliding window (slide every 1 minute).
Sinks results to PostgreSQL via foreachBatch.
"""
