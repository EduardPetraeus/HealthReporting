# Architecture

## Data Flow

```
Apple Health XML export
  -> source_connectors/apple_health/process_health_data.py
     (stream-parse, categorize by domain, hive-partitioned parquet)
  -> /Users/Shared/data_lake/apple_health_data/{Domain}/{DataType}/year=YYYY/...

Oura Ring API (OAuth 2.0, V2 API)
  -> source_connectors/oura/run_oura.py
     (incremental fetch per endpoint, chunked for heartrate)
  -> /Users/Shared/data_lake/oura/raw/{endpoint}/year=YYYY/month=MM/day=DD/

CSV exports (e.g. Lifesum)
  -> source_connectors/csv_to_parquet.py
     (generic converter, adds metadata columns, optional hive partitioning)
  -> /Users/Shared/data_lake/{source}/parquet/

All sources:
  -> ingestion_engine.py (reads sources_config.yaml, loads parquet into DuckDB bronze schema)
  -> health_dw_{env}.db :: bronze.stg_* tables

Silver (local, DuckDB):
  -> dbt run --select <model>   (creates empty schema-only table)
  -> python run_merge.py silver/merge_<source>_<entity>.sql
     (staging table -> dedup -> MERGE INTO silver -> drop staging)
  -> health_dw_{env}.db :: silver.* tables

Gold:
  -> Views defined in transformation_logic/databricks/gold/view/ (Databricks target)
```

## Metadata-Driven Ingestion

The ingestion engine is config-driven: `sources_config.yaml` maps each source to a glob pattern and target bronze table. Adding a new source = adding a YAML entry — no code changes needed for bronze ingestion.

## Silver Layer Pattern (Local DuckDB)

- **Schema definition**: dbt models in `dbt/models/silver/` define the table schema via `SELECT ... WHERE false`. Run once with `dbt run --select <model>`.
- **Data loading**: Merge scripts in `dbt/merge/silver/` do the actual data movement. Each script:
  1. `CREATE OR REPLACE TABLE silver.<entity>__staging AS` — dedup with ROW_NUMBER, cast types, compute hashes
  2. `MERGE INTO silver.<entity>` — insert new rows, update changed rows (hash comparison)
  3. `DROP TABLE IF EXISTS silver.<entity>__staging`
- **Surrogate keys**: `sk_date` (YYYYMMDD int) and `sk_time` (HHMM string) link facts to date/time dimensions.
- **Change detection**: `business_key_hash` (md5 of business key) + `row_hash` (md5 of all measured columns). Update only fires when row_hash differs.
- **Audit columns**: `load_datetime` (first insert) and `update_datetime` (last change) on all silver tables.
- **One table per entity**: Silver tables are source-agnostic. Multiple sources can merge into the same table (e.g., Oura + Apple Health both write to `silver.heart_rate`).

## Hive Partition Gotcha (Oura daily endpoints)

Oura daily endpoints are stored with Hive partitioning (`year=YYYY/month=MM/day=DD`). When DuckDB reads these with `hive_partitioning=true`, the `day` partition key (just the day-of-month integer, e.g. "21") overwrites the `day` column from the parquet file (the full ISO date "2025-11-21"). Merge scripts for these endpoints reconstruct the full date using `make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER)`.

## Silver Tables (17 entities)

| Table | Sources | Key |
|---|---|---|
| `heart_rate` | Apple Health, Oura | timestamp + source_name |
| `step_count` | Apple Health | date + source_name |
| `toothbrushing` | Apple Health | start_datetime |
| `daily_meal` | Lifesum | date + meal_name + food_name |
| `daily_walking_gait` | Apple Health | date (6 mobility metrics aggregated) |
| `mindful_session` | Apple Health | start_datetime |
| `body_temperature` | Apple Health | start_datetime |
| `respiratory_rate` | Apple Health | start_datetime |
| `water_intake` | Apple Health | date |
| `daily_energy_by_source` | Apple Health | date + source_name |
| `daily_sleep` | Oura | date |
| `daily_activity` | Oura | date |
| `daily_readiness` | Oura | date |
| `daily_spo2` | Oura | date |
| `daily_stress` | Oura | date |
| `workout` | Oura | workout_id (UUID) |
| `personal_info` | Oura | user_id |

## Gold Layer (Local)

Views defined for DuckDB. Minimal — designed for reporting/BI consumption.

---

## Databricks Cloud Pipeline

The Databricks framework is a production-grade, metadata-driven implementation of the same medallion architecture. It runs on Spark/Delta Lake and is deployed via Databricks Asset Bundles (DAB).

### Data Flow

```
Cloud storage (parquet, same format as local)
  → bronze_autoloader.py
    (cloudFiles Autoloader, incremental or full, adds source_system + _ingested_at)
  → health_dw.bronze.stg_<source_name> (Delta table)

  → silver_runner.py
    (reads YAML config → reads SQL file → substitutes variables → spark.sql())
  → health_dw.silver.<entity> (Delta table, MERGE INTO or INSERT INTO REPLACE WHERE)

  → gold_runner.py
    (reads YAML config → reads SQL file → substitutes {target} → spark.sql())
  → health_dw.gold.<entity> (Delta view or table)
```

### Config-Driven Design

Every pipeline behaviour is controlled by YAML files and SQL files. The Python runners are generic executors that never change.

| File type | Location | Controls |
|---|---|---|
| Source YAML | `config/sources/*.yml` | Bronze paths, load mode, silver SQL reference, unique key |
| Silver SQL | `notebooks/silver/sql/*.sql` | Transformation logic, DDL, merge strategy |
| Gold YAML | `config/gold/*.yml` | Gold entity type, target, SQL reference |
| Gold SQL | `notebooks/gold/sql/*.sql` | View or table definition |

### Source Isolation

Each source system owns its rows. The composite unique key `(source_system, record_id)` prevents cross-source collisions. A full reload of one source (e.g. Oura) never modifies another source's rows (e.g. Apple Health) in the same silver table.

Enforced at the SQL level:
- **Merge mode**: `MERGE ON (source_system, record_id)` + `WHERE source_system = '{source_system}'` in the USING clause
- **Insert overwrite mode**: `INSERT INTO ... REPLACE WHERE source_system = '{source_system}'`

### Multiple Sources → Same Silver Table

Both `apple_health_heart_rate` and `oura_heart_rate` write to `health_dw.silver.heart_rate` using the same `heart_rate.sql` file. This works because source connectors normalise bronze output to a shared column schema before writing to parquet.

If a source requires different transformation logic, it points to a different `sql_file` in its YAML while keeping the same `target_table`.

### Dev / Production Separation

Controlled by DAB targets in `bundles/databricks.yml`. Dev mode prefixes job names and uses an isolated workspace path. Production deploys automatically on merge to main via GitHub Actions.

For full detail: see `docs/databricks_framework.md`.

---

## Oura Connector

- OAuth 2.0 Authorization Code flow; tokens stored in `~/.config/health_reporting/oura_tokens.json`
- State tracking per endpoint in `~/.config/health_reporting/oura_state.json` (incremental fetch)
- Heartrate endpoint chunked in 7-day windows (API limit)
- 8 endpoints: daily_sleep, daily_activity, daily_readiness, heartrate, workout, daily_spo2, daily_stress, personal_info
