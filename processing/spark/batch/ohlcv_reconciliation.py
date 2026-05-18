"""
Spark batch job that compares streaming-derived OHLCV bars against REST-sourced
historical OHLCV and writes discrepancies to the reconciliation_log table.
Historical data is treated as the source of truth.
"""
