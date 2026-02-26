---
name: yaml-config-writer
description: Generate a complete source YAML config for a new data source. Use when adding a new source to the platform or when asked to configure a source.
---

# YAML Config Writer

Generate a complete source YAML configuration for a new data source.

## Steps

1. Read an existing source YAML in `health_environment/config/databricks/sources/` to understand the schema
2. Ask (or infer from context): source name, fields, data types, partition strategy, update frequency
3. Generate the YAML config following the exact same structure as existing configs
4. Ensure:
   - `source_system` matches the connector name exactly
   - Partition strategy is consistent: Apple Health uses `domain/type/year=YYYY/`, Oura uses `year=YYYY/month=MM/day=DD/`
   - All field types are Spark-compatible (`StringType`, `LongType`, `TimestampType`, etc.)
   - Incremental key is specified (usually `_ingested_at` or a natural timestamp field)

5. Place the file in `health_environment/config/databricks/sources/<source_name>.yml`
6. Note: a matching silver SQL transform will also be needed — use the `silver-transform-writer` agent

## Scale-up note
In an enterprise setting, source configs would be generated from a data contract registry (e.g., Confluent Schema Registry or a DataHub connector). YAML is our lightweight equivalent.
