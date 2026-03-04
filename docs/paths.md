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

## Databricks Transformation Logic (cloud pipeline)

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/transformation_logic/databricks/bronze/bronze_autoloader.py` | Generic Autoloader notebook |
| `health_unified_platform/health_platform/transformation_logic/databricks/silver/silver_runner.py` | Generic silver runner |
| `health_unified_platform/health_platform/transformation_logic/databricks/silver/sql/` | Silver SQL files (transformation + DDL) |
| `health_unified_platform/health_platform/transformation_logic/databricks/gold/gold_runner.py` | Generic gold runner |
| `health_unified_platform/health_platform/transformation_logic/databricks/gold/sql/` | Gold SQL files (view/table definitions) |

## Databricks Config & Deployment

| Path | Purpose |
|---|---|
| `health_unified_platform/health_environment/deployment/databricks/databricks.yml` | DAB entry point — run all `databricks bundle` commands from this directory |
| `health_unified_platform/health_environment/deployment/databricks/init.py` | One-time schema creation (run before first pipeline execution) |
| `health_unified_platform/health_environment/deployment/databricks/create_catalog__health_dw.sql` | Catalog DDL |
| `health_unified_platform/health_environment/deployment/databricks/create_schemas__health_dw.sql` | Schema DDL |
| `health_unified_platform/health_environment/config/databricks/sources/` | Source YAML configs (one per source, covers bronze + silver) |
| `health_unified_platform/health_environment/config/databricks/gold/` | Gold entity YAML configs |
| `health_unified_platform/health_environment/orchestration/bronze_job.yml` | DAB job: daily bronze ingestion |
| `health_unified_platform/health_environment/orchestration/silver_job.yml` | DAB job: daily silver merge |
| `health_unified_platform/health_environment/orchestration/gold_job.yml` | DAB job: daily gold refresh |
| `.github/workflows/deploy.yml` | GitHub Actions CI/CD (validate on PR, deploy to prd on merge to main) |

## Data Lake (local filesystem)

| Path | Purpose |
|---|---|
| `/Users/Shared/data_lake/` | Root of all raw and processed data |
| `/Users/Shared/data_lake/apple_health_data/` | Apple Health parquet (hive-partitioned by domain/type/year) |
| `/Users/Shared/data_lake/oura/raw/{endpoint}/` | Oura parquet (hive-partitioned year/month/day) |
| `/Users/Shared/data_lake/lifesum/parquet/` | Lifesum CSV → parquet output |
| `/Users/Shared/data_lake/database/health_dw_{env}.db` | DuckDB file (bronze + silver) |

## AI-Native Data Model

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/ai/text_generator.py` | Template-based daily health summary generation |
| `health_unified_platform/health_platform/ai/embedding_engine.py` | sentence-transformers embeddings + vector search |
| `health_unified_platform/health_platform/ai/baseline_computer.py` | Rolling baselines + demographics → patient_profile |
| `health_unified_platform/health_platform/ai/correlation_engine.py` | Pearson correlations with lag → metric_relationships |
| `health_unified_platform/health_platform/contracts/metrics/_index.yml` | Master metric index — 9 categories, query routing |
| `health_unified_platform/health_platform/contracts/metrics/_business_rules.yml` | Composite score, alerts, anomaly detection |
| `health_unified_platform/health_platform/contracts/metrics/*.yml` | 18 individual metric semantic contracts |
| `health_unified_platform/health_platform/mcp/server.py` | FastMCP server — 8 tools for AI data access |
| `health_unified_platform/health_platform/mcp/health_tools.py` | Tool implementations (query_health, search_memory, etc.) |
| `health_unified_platform/health_platform/mcp/query_builder.py` | YAML contract → parameterized SQL |
| `health_unified_platform/health_platform/mcp/formatter.py` | Markdown-table, markdown-kv, yaml output formatting |
| `health_unified_platform/health_platform/mcp/schema_pruner.py` | Category-based schema context pruning |
| `health_unified_platform/health_platform/setup/create_agent_schema.sql` | DDL for agent.* tables (5 tables + metric_relationships) |
| `health_unified_platform/health_platform/setup/add_column_comments.sql` | COMMENT ON for all 21 silver tables (269 descriptions) |
| `health_unified_platform/health_platform/setup/seed_health_graph.sql` | Knowledge graph seed: 67 nodes, 108 edges |
| `health_unified_platform/health_platform/setup/setup_agent_schema.py` | Idempotent schema setup runner |

## State & Tokens

| Path | Purpose |
|---|---|
| `~/.config/health_reporting/oura_tokens.json` | Oura OAuth tokens (access + refresh) |
| `~/.config/health_reporting/oura_state.json` | Last fetched date per Oura endpoint |
