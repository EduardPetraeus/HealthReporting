---
paths:
  - "health_unified_platform/**/silver/**"
  - "transformation_logic/databricks/silver/**"
---

# Silver Layer Rules

- Use MERGE INTO pattern — never INSERT OVERWRITE
- Watermark on `_ingested_at` for incremental processing
- Set `_updated_at = current_timestamp()` on target rows
- Deduplicate on natural key before merging
- No hardcoded dates or user IDs
- Scale-up note: add `@dlt.expect` / quarantine table when moving to DLT
