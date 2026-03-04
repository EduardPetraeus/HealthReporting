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

## AI Pipeline (local post-Silver)
After silver merge, `ingestion_engine.py` triggers daily summary generation:
- `ai/text_generator.py` — generates natural language summary for the day
- `ai/embedding_engine.py` — embeds summary with sentence-transformers
- `ai/baseline_computer.py` — updates rolling baselines in `agent.patient_profile`

These are queried via MCP tools in `mcp/server.py`, not directly.

## Config lives in
`health_unified_platform/health_environment/config/`
- `sources_config.yaml` — DuckDB source mapping
- `databricks/sources/` — Databricks Autoloader configs
- `databricks/gold/` — Databricks gold configs
