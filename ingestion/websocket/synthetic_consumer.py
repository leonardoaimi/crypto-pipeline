"""
Generates synthetic trade and ticker events that match the live Binance schema
exactly. Drop-in replacement for binance_consumer.py — toggled via
DATA_SOURCE=synthetic. Requires no network access.
"""
