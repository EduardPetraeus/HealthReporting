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
| `health_unified_platform/health_platform/source_connectors/base.py` | Base connector class with shared utilities |
| `health_unified_platform/health_platform/source_connectors/oura/run_oura.py` | Oura Ring entry point — fetches all 8 endpoints incrementally |
| `health_unified_platform/health_platform/source_connectors/oura/auth.py` | OAuth 2.0 token management (browser flow + auto-refresh) |
| `health_unified_platform/health_platform/source_connectors/oura/client.py` | Oura V2 API client (per-endpoint fetch methods) |
| `health_unified_platform/health_platform/source_connectors/oura/writer.py` | pyarrow parquet writer with hive partitioning |
| `health_unified_platform/health_platform/source_connectors/oura/state.py` | Incremental fetch state (last fetched date per endpoint) |
| `health_unified_platform/health_platform/source_connectors/strava/run_strava.py` | Strava entry point — OAuth + incremental activity fetch + athlete stats |
| `health_unified_platform/health_platform/source_connectors/strava/auth.py` | OAuth 2.0 Authorization Code flow for Strava API |
| `health_unified_platform/health_platform/source_connectors/strava/client.py` | Strava API client (activities, athlete stats) |
| `health_unified_platform/health_platform/source_connectors/strava/writer.py` | Parquet writer with hive partitioning for Strava data |
| `health_unified_platform/health_platform/source_connectors/strava/state.py` | Incremental fetch state per Strava endpoint |
| `health_unified_platform/health_platform/source_connectors/withings/run_withings.py` | Withings entry point — full/incremental fetch with 30-day overlap |
| `health_unified_platform/health_platform/source_connectors/weather/run_weather.py` | Open-Meteo entry point — daily weather data (default 90 days) |
| `health_unified_platform/health_platform/source_connectors/weather/client.py` | Open-Meteo API client (free, no auth required) |
| `health_unified_platform/health_platform/source_connectors/sundhed_dk/run_sundhed_dk.py` | sundhed.dk entry point — MitID browser auth + scrape 5 clinical sections |
| `health_unified_platform/health_platform/source_connectors/sundhed_dk/browser.py` | Playwright browser management for MitID authentication |
| `health_unified_platform/health_platform/source_connectors/sundhed_dk/scraper.py` | HTML scraper for sundhed.dk sections |
| `health_unified_platform/health_platform/source_connectors/sundhed_dk/parsers.py` | Parsers for lab_results, medications, vaccinations, ejournal, appointments |
| `health_unified_platform/health_platform/source_connectors/lab/lab_ingestion.py` | Lab PDF ingestion — scans directory, parses blood test PDFs → parquet |
| `health_unified_platform/health_platform/source_connectors/lab/pdf_parser.py` | Multi-format lab PDF parser (GetTested, sundhed.dk, microbiome) |
| `health_unified_platform/health_platform/source_connectors/genetics/genetics_ingestion.py` | 23andMe ingestion — routes PDF/CSV/JSON to parsers → 5 parquet streams |
| `health_unified_platform/health_platform/source_connectors/genetics/pdf_parser.py` | 23andMe PDF parser (health risk, carrier status, ancestry, traits) |
| `health_unified_platform/health_platform/source_connectors/genetics/csv_parser.py` | 23andMe ancestry CSV + family tree JSON parser |
| `health_unified_platform/health_platform/source_connectors/lifesum/run_lifesum_pdf.py` | Lifesum PDF download orchestrator (queries silver for latest date) |

## Bronze Ingestion

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/transformation_logic/ingestion_engine.py` | Reads sources_config.yaml, loads parquet → bronze (DuckDB) |

## Silver Transformation (local, dbt-duckdb)

| Path | Purpose |
|---|---|
| `health_unified_platform/health_platform/transformation_logic/dbt/` | dbt project root |
| `health_unified_platform/health_platform/transformation_logic/dbt/models/silver/` | Schema-only dbt models (49 silver entities) |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/` | Merge scripts — do the actual bronze → silver data load |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/run_merge.py` | Runner for a single merge SQL file |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_strava_activities.sql` | Strava activities merge |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_weather_daily.sql` | Open-Meteo daily weather merge |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_lab_pdf_results.sql` | Lab blood test results merge |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_23andme_ancestry.sql` | 23andMe ancestry segments merge |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_23andme_family_tree.sql` | 23andMe family tree merge |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_23andme_health_findings.sql` | 23andMe health findings merge |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_withings_*.sql` | Withings merge scripts (7: activity, blood_pressure, ecg, pulse_wave, sleep, weight, workouts, body_temperature) |
| `health_unified_platform/health_platform/transformation_logic/dbt/merge/silver/merge_lifesum_*.sql` | Lifesum merge scripts (4: food, bodyfat, exercise, weighins) |

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
| `/Users/Shared/data_lake/strava/raw/{endpoint}/` | Strava parquet (activities, athlete_stats) |
| `/Users/Shared/data_lake/withings/raw/{endpoint}/` | Withings parquet (weight, blood_pressure, sleep, etc.) |
| `/Users/Shared/data_lake/weather/raw/open_meteo/` | Open-Meteo daily weather parquet (hive-partitioned year/month/day) |
| `/Users/Shared/data_lake/min sundhed/raw/` | sundhed.dk scraped data (lab_results, medications, vaccinations, ejournal, appointments) |
| `/Users/Shared/data_lake/lab_results/downloads/` | Lab blood test PDF files (input) |
| `/Users/Shared/data_lake/lab_results/parquet/` | Lab results parsed parquet output |
| `/Users/Shared/data_lake/genetics/23andme/downloads/` | 23andMe PDF/CSV/JSON exports (input) |
| `/Users/Shared/data_lake/genetics/23andme/parquet/` | 23andMe parsed parquet output (5 streams) |
| `/Users/Shared/data_lake/lifesum/parquet/` | Lifesum CSV → parquet output |
| `/Users/Shared/data_lake/database/health_dw_{env}.db` | DuckDB file (bronze + silver + agent) |

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
| `~/.config/health_reporting/strava_tokens.json` | Strava OAuth tokens (access + refresh) |
| `~/.config/health_reporting/strava_state.json` | Last fetched date per Strava endpoint |
| `~/.config/health_reporting/withings_tokens.json` | Withings OAuth tokens (access + refresh) |
