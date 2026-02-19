# Runbook

## Apple Health XML -> Parquet

```bash
python health_unified_platform/health_platform/source_connectors/apple_health/process_health_data.py \
  --input /path/to/eksport.xml \
  --output /Users/Shared/data_lake/apple_health_data
```

## Bronze Ingestion (parquet -> DuckDB)

```bash
# Run from repository root; defaults to dev environment
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py
```

## Silver/Gold Transformations

SQL/Python notebooks in `transformation_logic/databricks/silver/notebook/` and `gold/view/` are designed for Databricks execution, not local standalone use.