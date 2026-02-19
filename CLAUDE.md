# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Personal health data platform (owner: Claus Eduard Petraeus) that ingests, transforms, and surfaces data from wearable devices and nutrition apps. Uses a **medallion architecture** (bronze -> silver -> gold) with a local-first approach on DuckDB, targeting Databricks as the cloud platform.

## Running the Platform Locally

### Apple Health XML -> Parquet (source connector)
```bash
python health_unified_platform/health_platform/source_connectors/apple_health/process_health_data.py \
  --input /path/to/eksport.xml \
  --output /Users/Shared/data_lake/apple_health_data
```

### Bronze Ingestion (parquet -> DuckDB)
```bash
# Run from repository root; defaults to dev environment
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py
```

### Silver/Gold Transformations
The SQL/Python notebooks in `transformation_logic/databricks/silver/notebook/` and `gold/view/` are designed for Databricks execution, not local standalone use.

## Key Paths

| Path | Purpose |
|---|---|
| `health_unified_platform/health_environment/config/environment_config.yaml` | Data lake root, DB path, env defaults |
| `health_unified_platform/health_environment/config/sources_config.yaml` | Metadata-driven source -> bronze table mapping |
| `health_unified_platform/health_platform/source_connectors/` | Per-source connectors (currently only apple_health) |
| `health_unified_platform/health_platform/transformation_logic/ingestion_engine.py` | Main bronze ingestion engine |
| `health_unified_platform/health_platform/transformation_logic/databricks/silver/` | Silver layer DDL (`table/`) and transformation notebooks (`notebook/`) |
| `health_unified_platform/health_platform/transformation_logic/databricks/gold/view/` | Gold layer views |
| `health_unified_platform/health_environment/deployment/databricks/` | Databricks catalog/schema DDL |
| `legacy_on_premise_dw/` | Deprecated SQL Server + SSIS + Tabular Model (reference only) |

## Architecture

### Data Flow
```
Apple Health XML export
  -> process_health_data.py (stream-parse, categorize by domain, output hive-partitioned parquet)
  -> /Users/Shared/data_lake/apple_health_data/{Domain}/{DataType}/year={YYYY}/data_batch.parquet
  -> ingestion_engine.py (reads sources_config.yaml, loads parquet into DuckDB bronze schema)
  -> /Users/Shared/data_lake/database/health_dw_{env}.db (bronze.stg_* tables)
  -> Databricks silver notebooks (staging table -> dedup -> MERGE INTO silver -> drop staging)
  -> Gold views
```

### Metadata-Driven Ingestion
The ingestion engine is config-driven: `sources_config.yaml` maps each source to a glob pattern and target bronze table. Adding a new source means adding a YAML entry — no code changes needed for bronze ingestion.

### Silver Layer Patterns (Databricks)
- **Staging + MERGE**: Each notebook creates a temp staging table with ROW_NUMBER deduplication, MERGEs into the persistent silver table using hash-based change detection (`sha2(concat_ws(...))`), then drops the staging table.
- **Surrogate keys**: `sk_date` (YYYYMMDD int) and `sk_time` (HHMM string) link facts to date/time dimensions.
- **Audit columns**: `load_datetime` and `update_datetime` on all silver tables.
- **Dimensions**: `dim_date` and `dim_time` are generated via PySpark notebooks (not SQL).

### Environment Separation
`HEALTH_ENV` env var controls dev vs prd. The DuckDB file is named `health_dw_{env}.db`. The Databricks catalog is `health_dw` with schemas: bronze, silver, gold.

## Conventions

- **snake_case** for all names (tables, columns, variables, files)
- **All code in English** (variable names, comments, docstrings, print messages)
- Prefix bronze tables with `stg_`
- Individual files over monolithic scripts
- Tech-agnostic: local logic uses DuckDB/Python; cloud logic uses Databricks SQL/PySpark

## Dependencies

Python 3.9 virtual environment (`.venv/`) with key packages: `duckdb`, `pandas`, `pyyaml`, `dbt-core`, `dbt-duckdb`. No `requirements.txt` exists — dependencies are managed directly in the venv.

## Tooling

- **Databricks MCP server** configured in `.mcp.json` for Claude Code integration
- **VS Code** with DuckDB file viewer for parquet/csv and SQL Tools connection
- No CI/CD, no test framework, no linter currently configured
