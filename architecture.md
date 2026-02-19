# Architecture

## Data Flow

```
Apple Health XML export
  -> process_health_data.py (stream-parse, categorize by domain, output hive-partitioned parquet)
  -> /Users/Shared/data_lake/apple_health_data/{Domain}/{DataType}/year={YYYY}/data_batch.parquet
  -> ingestion_engine.py (reads sources_config.yaml, loads parquet into DuckDB bronze schema)
  -> /Users/Shared/data_lake/database/health_dw_{env}.db (bronze.stg_* tables)
  -> Databricks silver notebooks (staging table -> dedup -> MERGE INTO silver -> drop staging)
  -> Gold views
```

## Metadata-Driven Ingestion

The ingestion engine is config-driven: `sources_config.yaml` maps each source to a glob pattern and target bronze table. Adding a new source means adding a YAML entry — no code changes needed for bronze ingestion.

## Silver Layer Patterns (Databricks)

- **Staging + MERGE**: Each notebook creates a temp staging table with ROW_NUMBER deduplication, MERGEs into the persistent silver table using hash-based change detection (`sha2(concat_ws(...))`), then drops the staging table.
- **Surrogate keys**: `sk_date` (YYYYMMDD int) and `sk_time` (HHMM string) link facts to date/time dimensions.
- **Audit columns**: `load_datetime` and `update_datetime` on all silver tables.
- **Dimensions**: `dim_date` and `dim_time` are generated via PySpark notebooks (not SQL).