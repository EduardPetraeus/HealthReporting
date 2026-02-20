# HealthReporting

Personal health data platform (owner: Claus Eduard Petraeus) that ingests, transforms, and surfaces data from wearable devices and nutrition apps.

## Architecture

Medallion architecture (bronze → silver → gold) running on two parallel tracks:

**Local (DuckDB)** — development and experimentation:
```
Source APIs / files  →  parquet (hive-partitioned)
  →  bronze (DuckDB stg_* tables via ingestion_engine.py)
  →  silver (dbt-duckdb schema + run_merge.py)
  →  gold (DuckDB views)
```

**Cloud (Databricks)** — production pipeline:
```
Cloud storage (parquet)
  →  bronze (Autoloader / cloudFiles → Delta table)
  →  silver (MERGE INTO → Delta table, driven by YAML + SQL)
  →  gold (CREATE OR REPLACE VIEW, driven by YAML + SQL)
```

The cloud pipeline is fully generic and metadata-driven: adding a new source requires one YAML config file and one SQL file — no Python changes. Deployed via Databricks Asset Bundles (DAB) with GitHub Actions CI/CD.

See `docs/databricks_framework.md` for the full cloud pipeline reference.

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

See `docs/runbook.md` for the full runbook, `docs/architecture.md` for design details, `docs/paths.md` for key file locations, and `docs/databricks_framework.md` for the cloud pipeline reference.

## Stack

- **Local**: Python 3.9, DuckDB, dbt-duckdb, pyarrow, pandas
- **Cloud**: Databricks (Unity Catalog, Delta Lake, Autoloader, Asset Bundles)
- **CI/CD**: GitHub Actions → auto-deploy on merge to main
- **Storage**: Parquet (hive-partitioned) locally at `/Users/Shared/data_lake/`; ADLS/S3 on cloud
