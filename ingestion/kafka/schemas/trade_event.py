"""
Pydantic schema for a single trade event published to crypto.trades.raw.
Uses Decimal for price and quantity to avoid floating-point precision loss.
Carries both trade_timestamp (exchange-native) and ingestion_timestamp.
"""
