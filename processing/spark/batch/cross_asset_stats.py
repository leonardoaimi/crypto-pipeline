"""
Spark batch job that computes 30-day rolling volatility and pairwise
correlation across all tracked symbols. Requires Spark window functions
(LAG) — results written to the cross_asset_stats PostgreSQL table.
"""
