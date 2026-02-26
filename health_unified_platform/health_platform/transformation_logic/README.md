# Transformation Logic

Two parallel pipelines — same medallion architecture (bronze → silver → gold), different engines.

## `dbt/` — Local (DuckDB)
Used for development and local runs on Mac Mini.
- `models/silver/` — schema definitions (run once per entity)
- `merge/silver/` — MERGE scripts executed by `run_merge.py`
- SQL dialect: DuckDB

## `databricks/` — Cloud (Databricks)
Used for production runs on Databricks.
- `bronze/` — Autoloader runner (reads from `health_environment/config/databricks/sources/`)
- `silver/` — Silver runner + SQL transforms
- `gold/` — Gold runner + SQL transforms
- SQL dialect: Spark SQL

## Config lives in
`health_unified_platform/health_environment/config/`
- `sources_config.yaml` — DuckDB source mapping
- `databricks/sources/` — Databricks Autoloader configs
- `databricks/gold/` — Databricks gold configs
