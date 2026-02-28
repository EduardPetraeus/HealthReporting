# ADR-003: YAML-Driven Metadata Pipeline

## Status
Accepted

## Date
2026-02-28

## Context
Adding a new data source should not require code changes to the ingestion engine. The platform must support multiple sources with different schemas, endpoints, and ingestion patterns without a proliferation of one-off scripts.

## Decision
All source configuration lives in `sources_config.yaml`. Adding a new source = adding a YAML entry. The ingestion engine reads the config and handles the rest.

Config structure per source:
```yaml
source_name:
  type: api | file | parquet
  endpoint: ...
  schema: [field definitions]
  bronze_table: stg_<source>
  incremental_key: ...
```

## Consequences
- No new Python code required for a new source (only YAML)
- The ingestion engine (`ingestion_engine.py`) is the single point of change for cross-source improvements
- Source configs are the source of truth for schema — agents must update `sources_config.yaml` when adding/modifying sources
- Config validation is mandatory before adding a new source

## Alternatives Considered
- **One script per source**: Simple to start, but does not scale. 10 sources = 10 scripts to maintain.
- **dbt sources**: Powerful but requires dbt for everything. Current stack mixes DuckDB local + Databricks cloud — dbt-duckdb adds complexity.
- **Hardcoded schemas in Python**: No single source of truth, hard to audit.

## Scale-Up Path
At enterprise scale: replace YAML with a metadata catalog (Unity Catalog tags, or a dedicated metadata database). YAML-driven is the right PoC pattern — it demonstrates the principle without the overhead.

## Agent Rule
Agents must always update `sources_config.yaml` when adding or modifying a source. Agents must never hardcode source schemas in Python scripts — all schema definitions belong in the config.
