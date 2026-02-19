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
- CI/CD via GitHub Actions (planned)
- snake_case for everything
- All code (variable names, comments, docstrings, print messages) in English

### local_stack
- Runtime: DuckDB (file-based, stored at `/Users/Shared/data_lake/database/`)
- Storage format: Parquet (hive-partitioned)
- Data lake root: `/Users/Shared/data_lake/`
- Orchestration: Python
- Config: YAML

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
│   │   │   ├── environment_config.yaml     # paths, env, db_name, output_format
│   │   │   └── sources_config.yaml         # metadata-driven source → bronze table mapping
│   │   └── deployment/
│   │       └── databricks/
│   │           ├── create_catalog__health_dw.sql
│   │           └── create_schemas__health_dw.sql
│   └── health_platform/
│       ├── metadata/
│       │   └── daily_annotations__manual.sql
│       ├── source_connectors/
│       │   └── apple_health/
│       │       └── process_health_data.py  # parses Apple Health XML export → parquet
│       └── transformation_logic/
│           ├── ingestion_engine.py         # reads sources_config.yaml, loads parquet → bronze (DuckDB)
│           └── databricks/
│               ├── silver/
│               │   ├── notebook/           # SQL transformation notebooks per entity
│               │   └── table/              # CREATE TABLE DDL per silver entity
│               └── gold/
│                   └── view/               # vw_daily_annotations, vw_heart_rate_avg_per_day
└── legacy_on_premise_dw/                   # DEPRECATED — SQL Server + SSIS + Tabular Model
```

---

## data_sources

| source       | status         | notes                                      |
|--------------|----------------|--------------------------------------------|
| apple_health | in progress    | XML → parquet connector exists, 4 entities in sources_config |
| oura         | planned        |                                            |
| withings     | planned        |                                            |
| strava       | planned        |                                            |
| lifesum      | planned        |                                            |
| gettested    | planned        |                                            |

---

## ingestion_engine
- Entry point: `ingestion_engine.py`
- Reads `sources_config.yaml` to discover sources
- Loads parquet files (hive-partitioned, union_by_name for schema evolution)
- Creates `bronze` schema + tables in DuckDB
- Adds `_ingested_at` and `_source_env` metadata columns
- Run with: `HEALTH_ENV=dev python ingestion_engine.py`

### sources_in_config (as of latest commit)
- `stg_apple_health_toothbrushing`
- `stg_apple_health_step_count`
- `stg_apple_health_heart_rate`
- `stg_apple_health_vo2_max`

---

## current_status
- Bronze ingestion engine works locally via DuckDB
- Apple Health XML → parquet connector exists
- Silver layer partially built (Databricks notebooks for 9 entities)
- Gold layer minimal (2 views)
- Rebuilding from scratch with broader source coverage and cleaner structure
- Reporting destination not yet decided

---

## next_steps (to be updated)
- Define full source list and bronze schema per source
- Build silver transformation logic for all sources
- Design gold layer (reporting entities)
- Decide reporting/visualisation destination
- Add CI/CD via GitHub Actions
- Migrate from DuckDB-local to Databricks when ready
