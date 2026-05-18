"""
Pydantic schema for an OHLCV record published to crypto.ohlcv.streaming or
crypto.ohlcv.historical. Uses Decimal for open/high/low/close/volume.
Carries both trade_timestamp and ingestion_timestamp.
"""
