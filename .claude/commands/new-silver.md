Scaffold a new silver transform for source: $ARGUMENTS

1. Read an existing silver SQL in `transformation_logic/databricks/silver/sql/` to understand the MERGE pattern
2. Read the source YAML config in `health_environment/config/databricks/sources/` for $ARGUMENTS
3. Create a new SQL file following the same MERGE INTO pattern with:
   - Correct source table name (`bronze.stg_$ARGUMENTS`)
   - All columns mapped with correct types
   - `_ingested_at` as the watermark column
   - A `_updated_at` timestamp on the target
4. Note scale-up path: DLT expectations, quarantine table, DQ monitoring
