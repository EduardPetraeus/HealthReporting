# HealthReporting

Personal health data platform (owner: Claus Eduard Petraeus) that ingests, transforms, and surfaces data from wearable devices and nutrition apps.

## Architecture

Medallion architecture (bronze → silver → gold) with a local-first approach on DuckDB, targeting Databricks as the cloud platform.

```
Source APIs / files  →  parquet (hive-partitioned)
  →  bronze (DuckDB stg_* tables via ingestion_engine.py)
  →  silver (dbt-duckdb schema + run_merge.py)
  →  gold (Databricks views — planned)
```

## Data Sources

| Source | Status | Entities |
|---|---|---|
| Apple Health | Active | activity, heart rate, steps, walking gait, sleep, nutrition, hygiene, body metrics |
| Oura Ring | Active | sleep, activity, readiness, heart rate, workouts, SpO2, stress, personal info |
| Lifesum | Active | food/nutrition logs |
| Withings | Planned | weight, body composition, blood pressure |
| Strava | Planned | workouts, GPS activities |
| GetTested | Planned | lab results, blood tests |

## Silver Tables (17)

`heart_rate` · `step_count` · `toothbrushing` · `daily_meal` · `daily_walking_gait` · `mindful_session` · `body_temperature` · `respiratory_rate` · `water_intake` · `daily_energy_by_source` · `daily_sleep` · `daily_activity` · `daily_readiness` · `daily_spo2` · `daily_stress` · `workout` · `personal_info`

## Quick Start

```bash
source .venv/bin/activate

# Fetch Oura data (incremental)
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py

# Load all sources into bronze
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py

# Merge into silver
cd health_unified_platform/health_platform/transformation_logic/dbt/merge
for f in silver/merge_oura_*.sql; do HEALTH_ENV=dev python run_merge.py "$f"; done
```

See `docs/runbook.md` for the full runbook, `docs/architecture.md` for design details, and `docs/paths.md` for key file locations.

## Stack

- **Local**: Python 3.9, DuckDB, dbt-duckdb, pyarrow, pandas
- **Cloud target**: Databricks (Unity Catalog)
- **Storage**: Parquet (hive-partitioned), DuckDB file at `/Users/Shared/data_lake/database/`
