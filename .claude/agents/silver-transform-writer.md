---
name: silver-transform-writer
description: Write a complete silver SQL MERGE transform for a source. Use when implementing a new silver layer entity or when asked to write a silver transform.
---

# Silver Transform Writer

Write a complete, production-ready silver SQL MERGE transform.

## Steps

1. Read the source YAML config from `health_environment/config/databricks/sources/<source>.yml`
2. Read an existing silver SQL (e.g. the most complete one in `transformation_logic/databricks/silver/sql/`) as a pattern reference
3. Generate the MERGE INTO statement:

```sql
-- silver/<source>_<entity>.sql
-- Incremental MERGE from bronze.stg_<source> into silver.<entity>
-- Watermark: _ingested_at

MERGE INTO silver.<entity> AS target
USING (
  SELECT <columns>
  FROM bronze.stg_<source>
  WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM silver.<entity>)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY <natural_key> ORDER BY _ingested_at DESC) = 1
) AS source
ON target.<natural_key> = source.<natural_key>
WHEN MATCHED THEN UPDATE SET <all_columns>, _updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (<all_columns>, _updated_at) VALUES (<all_columns>, current_timestamp());
```

4. Add header comment with: source, entity, natural key, watermark column, scale-up path
5. Place in `transformation_logic/databricks/silver/sql/<source>_<entity>.sql`

## Scale-up note
Add to comment: "Enterprise upgrade: replace with DLT pipeline using @dlt.expect for row-level DQ and quarantine table for failed rows."
