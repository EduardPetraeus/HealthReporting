# CONTEXT.md — HealthReporting Platform

## project_purpose
Personal health data platform that ingests, transforms, and surfaces data from multiple wearable
and nutrition sources. Goal is a unified view of physical activity, sleep, nutrition, biomarkers,
and recovery metrics.

## owner
Claus Eduard Petraeus

## repository
https://github.com/EduardPetraeus/HealthReporting

---

## architecture

### design_principles
- Medallion architecture: bronze → silver → gold
- Tech-agnostic design (local-first with DuckDB, cloud target is Databricks)
- Metadata-driven via YAML config files
- Dev/prd separation via `HEALTH_ENV` environment variable
- Individual files over monolithic scripts
- snake_case for everything
- All code (variable names, comments, docstrings, print messages) in English
- One silver table per data entity — multiple sources can write to the same table

### local_stack
- Runtime: DuckDB (file-based, stored at `/Users/Shared/data_lake/database/`)
- Storage format: Parquet (hive-partitioned)
- Data lake root: `/Users/Shared/data_lake/`
- Orchestration: Python
- Config: YAML
- Silver transformation: dbt-duckdb (schema) + run_merge.py (data)

### cloud_target
- Databricks (Unity Catalog)
- Catalog: `health_dw`
- Schemas: bronze, silver, gold (DDL exists in `deployment/databricks/`)

### hardware
- Mac Mini M4, 24GB RAM

---

## repository_structure

```
HealthReporting/
├── health_unified_platform/
│   ├── health_environment/
│   │   ├── config/
│   │   │   ├── environment_config.yaml         # paths, env, db_name, output_format
│   │   │   └── sources_config.yaml             # metadata-driven source → bronze table mapping
│   │   └── deployment/
│   │       └── databricks/
│   │           ├── create_catalog__health_dw.sql
│   │           └── create_schemas__health_dw.sql
│   └── health_platform/
│       ├── metadata/
│       │   └── daily_annotations__manual.sql
│       ├── source_connectors/
│       │   ├── csv_to_parquet.py               # generic CSV → parquet converter
│       │   ├── apple_health/
│       │   │   └── process_health_data.py      # Apple Health XML → parquet
│       │   └── oura/
│       │       ├── run_oura.py                 # entry point (fetch all endpoints)
│       │       ├── auth.py                     # OAuth 2.0 token management
│       │       ├── client.py                   # Oura V2 API client
│       │       ├── writer.py                   # pyarrow parquet writer
│       │       └── state.py                    # incremental fetch state
│       └── transformation_logic/
│           ├── ingestion_engine.py             # reads sources_config.yaml, loads parquet → bronze
│           ├── dbt/
│           │   ├── models/silver/              # schema-only dbt models (one per entity)
│           │   └── merge/
│           │       ├── run_merge.py            # runner for a single merge SQL file
│           │       └── silver/                 # merge scripts (staging → MERGE → drop)
│           └── databricks/
│               ├── silver/                     # legacy Databricks notebooks (reference)
│               └── gold/
│                   └── view/                   # gold views (Databricks target)
└── legacy_on_premise_dw/                       # DEPRECATED — SQL Server + SSIS + Tabular Model
```

---

## data_sources

| source       | status   | bronze tables | silver tables |
|--------------|----------|---------------|---------------|
| apple_health | active   | 15            | heart_rate, step_count, toothbrushing, daily_walking_gait, mindful_session, body_temperature, respiratory_rate, water_intake, daily_energy_by_source |
| oura         | active   | 8             | daily_sleep, daily_activity, daily_readiness, heart_rate (shared), workout, daily_spo2, daily_stress, personal_info |
| lifesum      | active   | 1             | daily_meal |
| withings     | planned  | —             | — |
| strava       | planned  | —             | — |
| gettested    | planned  | —             | — |

---

## ingestion_engine
- Entry point: `ingestion_engine.py`
- Reads `sources_config.yaml` to discover sources (27 configured as of last update)
- Loads parquet files (hive-partitioned, union_by_name for schema evolution)
- Creates `bronze` schema + tables in DuckDB
- Adds `_ingested_at` and `_source_env` metadata columns
- Run with: `HEALTH_ENV=dev python ingestion_engine.py`

---

## silver_layer

### pattern
1. `dbt run --select <model>` — creates empty typed table (run once)
2. `python run_merge.py silver/merge_<source>_<entity>.sql` — loads data

### merge_script_structure
Each merge SQL file contains exactly 3 statements (split on `;`):
1. `CREATE OR REPLACE TABLE silver.<entity>__staging AS ...` — dedup + cast + hash
2. `MERGE INTO silver.<entity> ...` — insert new / update changed
3. `DROP TABLE IF EXISTS silver.<entity>__staging`

### business_key_hash
`md5()` of the business key columns — used as the MERGE join condition.

### row_hash
`md5()` of all measured/factual columns — update only fires when this differs.

---

## current_status
- Bronze ingestion: fully operational for Apple Health (15 types), Oura (8 endpoints), Lifesum (1)
- Silver layer: 17 tables built and populated locally via dbt + merge scripts
- Oura connector: OAuth 2.0, incremental fetch, 8 endpoints live
- Gold layer: minimal (2 views, Databricks target)
- Reporting destination: not yet decided

---

## next_steps
- Add remaining Apple Health silver entities (vo2_max, height, physical_effort)
- Add Withings source connector
- Design and build gold layer for local DuckDB
- Decide reporting/visualisation destination (Metabase, Power BI, Streamlit, etc.)
- Add CI/CD via GitHub Actions
- Migrate from DuckDB-local to Databricks when ready
