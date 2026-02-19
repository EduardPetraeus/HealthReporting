# Runbook

## Prerequisites

```bash
# Activate venv (always run from repo root)
source .venv/bin/activate

# Close any open DuckDB connections (VS Code SQL Tools, duckdb CLI)
# before running any script — DuckDB only allows one writer at a time.
```

---

## Apple Health: XML → Parquet

```bash
python health_unified_platform/health_platform/source_connectors/apple_health/process_health_data.py \
  --input /path/to/eksport.xml \
  --output /Users/Shared/data_lake/apple_health_data
```

---

## Lifesum (or any CSV): CSV → Parquet

```bash
python health_unified_platform/health_platform/source_connectors/csv_to_parquet.py \
  --input /path/to/file.csv \
  --output /Users/Shared/data_lake/lifesum/parquet/food \
  --source-name lifesum_food
```

---

## Oura Ring: Fetch from API → Parquet

First-time: opens a browser for OAuth login. Subsequent runs: incremental from last fetched date.

```bash
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py
```

---

## Bronze Ingestion: Parquet → DuckDB

Reads `sources_config.yaml`, loads all configured parquet sources into `bronze.stg_*` tables.

```bash
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py
```

---

## Silver: Create Table Schema (run once per new table)

Uses dbt to create the empty schema-only table. Must be run before the first merge.

```bash
DBT_DIR=health_unified_platform/health_platform/transformation_logic/dbt
HEALTH_ENV=dev dbt run --project-dir "$DBT_DIR" --profiles-dir "$DBT_DIR" --select <model_name>

# Example — create all silver tables at once:
HEALTH_ENV=dev dbt run --project-dir "$DBT_DIR" --profiles-dir "$DBT_DIR"
```

---

## Silver: Load Data (run_merge.py)

Runs a staging → MERGE INTO silver → drop staging pipeline for one entity.

```bash
cd health_unified_platform/health_platform/transformation_logic/dbt/merge

# Single script
HEALTH_ENV=dev python run_merge.py silver/merge_oura_daily_sleep.sql

# All Oura scripts
for f in silver/merge_oura_daily_sleep.sql \
          silver/merge_oura_daily_activity.sql \
          silver/merge_oura_daily_readiness.sql \
          silver/merge_oura_heartrate.sql \
          silver/merge_oura_workout.sql \
          silver/merge_oura_daily_spo2.sql \
          silver/merge_oura_daily_stress.sql \
          silver/merge_oura_personal_info.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done

# All Apple Health scripts
for f in silver/merge_apple_health_*.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done
```

---

## Typical End-to-End Run (Oura)

```bash
source .venv/bin/activate

# 1. Fetch new data from API
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py

# 2. Load into bronze
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py

# 3. Merge into silver
cd health_unified_platform/health_platform/transformation_logic/dbt/merge
for f in silver/merge_oura_*.sql; do HEALTH_ENV=dev python run_merge.py "$f"; done
```

---

## Troubleshooting

**DuckDB lock error** (`Could not set lock on file`):
Close all other DuckDB connections — VS Code SQL Tools extension, open `duckdb` CLI sessions, or other Python processes. DuckDB allows only one writer at a time.

**Semicolons in SQL comments** break `run_merge.py` (it splits on all `;`):
Avoid semicolons inside `--` comments in merge scripts.

**Oura `day` column contains day-of-month integer** (e.g. "21") instead of full date:
This is a Hive partition key conflict. Merge scripts for daily Oura endpoints use `make_date(year, month, day)` to reconstruct the full date. Do not use `day::DATE` directly for these tables.
