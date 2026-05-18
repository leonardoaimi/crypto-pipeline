"""
Airflow DAG (TaskFlow API) for the nightly batch pipeline:
ohlcv_reconciliation → cross_asset_stats → dbt run → dbt test.
Nice-to-have — requires Phase 8. Until then, use `make batch`.
"""
