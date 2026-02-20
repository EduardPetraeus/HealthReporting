# Key Paths

## Config

| Path | Purpose |
|---|---|
| `health_unified_platform/health_environment/config/environment_config.yaml` | Data lake root, DB path, env defaults |
| `health_unified_platform/health_environment/config/sources_config.yaml` | Metadata-driven source → bronze table mapping (27 sources) |

## Source Connectors

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/source_connectors/apple_health/process_health_data.py` | Parses Apple Health XML export → hive-partitioned parquet |
| `health_unified_platform/health_platform/source_connectors/csv_to_parquet.py` | Generic CSV → parquet converter (adds metadata, optional hive partitioning) |
| `health_unified_platform/health_platform/source_connectors/oura/run_oura.py` | Oura Ring entry point — fetches all 8 endpoints incrementally |
| `health_unified_platform/health_platform/source_connectors/oura/auth.py` | OAuth 2.0 token management (browser flow + auto-refresh) |
| `health_unified_platform/health_platform/source_connectors/oura/client.py` | Oura V2 API client (per-endpoint fetch methods) |
| `health_unified_platform/health_platform/source_connectors/oura/writer.py` | pyarrow parquet writer with hive partitioning |
| `health_unified_platform/health_platform/source_connectors/oura/state.py` | Incremental fetch state (last fetched date per endpoint) |

## Bronze Ingestion

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/transformation_logic/ingestion_engine.py` | Reads sources_config.yaml, loads parquet → bronze (DuckDB) |

## Silver Transformation (local, dbt-duckdb)

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/transformation_logic/dbt/` | dbt project root |
| `health_unified_platform/health_platform/transformation_logic/dbt/models/silver/` | Schema-only dbt models (one per silver entity) |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/` | Merge scripts — do the actual bronze → silver data load |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/run_merge.py` | Runner for a single merge SQL file |

## Gold Layer (Databricks target)

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/transformation_logic/databricks/gold/view/` | Legacy gold layer views |

## Databricks Framework (cloud pipeline)

| Path | Purpose |
|---|---|
| `health_unified_platform/databricks_framework/bundles/databricks.yml` | DAB entry point — run all `databricks bundle` commands from `bundles/` |
| `health_unified_platform/databricks_framework/config/sources/` | Source YAML configs (one per source, covers bronze + silver) |
| `health_unified_platform/databricks_framework/config/gold/` | Gold entity YAML configs |
| `health_unified_platform/databricks_framework/notebooks/setup/init.py` | One-time schema creation (run before first pipeline execution) |
| `health_unified_platform/databricks_framework/notebooks/bronze/bronze_autoloader.py` | Generic Autoloader notebook |
| `health_unified_platform/databricks_framework/notebooks/silver/silver_runner.py` | Generic silver runner |
| `health_unified_platform/databricks_framework/notebooks/silver/sql/` | Silver SQL files (transformation + DDL) |
| `health_unified_platform/databricks_framework/notebooks/gold/gold_runner.py` | Generic gold runner |
| `health_unified_platform/databricks_framework/notebooks/gold/sql/` | Gold SQL files (view/table definitions) |
| `health_unified_platform/databricks_framework/workflows/bronze_job.yml` | DAB job: daily bronze ingestion |
| `health_unified_platform/databricks_framework/workflows/silver_job.yml` | DAB job: daily silver merge |
| `health_unified_platform/databricks_framework/workflows/gold_job.yml` | DAB job: daily gold refresh |
| `.github/workflows/deploy.yml` | GitHub Actions CI/CD (validate on PR, deploy to prd on merge to main) |

## Deployment (legacy)

| Path | Purpose |
|---|---|
| `health_unified_platform/health_environment/deployment/databricks/` | Databricks catalog/schema DDL (pre-framework) |

## Data Lake (local filesystem)

| Path | Purpose |
|---|---|
| `/Users/Shared/data_lake/` | Root of all raw and processed data |
| `/Users/Shared/data_lake/apple_health_data/` | Apple Health parquet (hive-partitioned by domain/type/year) |
| `/Users/Shared/data_lake/oura/raw/{endpoint}/` | Oura parquet (hive-partitioned year/month/day) |
| `/Users/Shared/data_lake/lifesum/parquet/` | Lifesum CSV → parquet output |
| `/Users/Shared/data_lake/database/health_dw_{env}.db` | DuckDB file (bronze + silver) |

## State & Tokens

| Path | Purpose |
|---|---|
| `~/.config/health_reporting/oura_tokens.json` | Oura OAuth tokens (access + refresh) |
| `~/.config/health_reporting/oura_state.json` | Last fetched date per Oura endpoint |
