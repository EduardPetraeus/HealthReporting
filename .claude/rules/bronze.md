---
paths:
  - "health_unified_platform/**/bronze/**"
  - "health_unified_platform/**/connectors/**"
  - "health_unified_platform/**/ingest/**"
---

# Bronze Layer Rules

- All bronze tables prefixed with `stg_`
- Always include `_ingested_at TIMESTAMP` column set to `current_timestamp()`
- Always include `source_system STRING` column (e.g. `'apple_health'`, `'oura'`)
- Incremental writes only — never truncate and reload
- Partition by `year=YYYY/` at minimum; daily sources also use `month=MM/day=DD/`
- Raw data is never transformed — cast only, no business logic in bronze
