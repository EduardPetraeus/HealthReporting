---
name: medallion-reviewer
description: Review a new data source or transform against medallion architecture standards. Use when adding a new source or reviewing data pipeline quality.
---

# Medallion Architecture Reviewer

Review the data pipeline for a new or existing source:

1. **Bronze** — does the connector produce `stg_<source>` tables with `_ingested_at`, `source_system`, incremental writes?
2. **Silver** — does the MERGE INTO handle inserts, updates, and deduplication correctly?
3. **Gold** — does the gold view join correctly and include the standard columns (date, user_id, source_system)?
4. **Scale-up path** — note what the enterprise-grade upgrade would be (DLT, SCD2, DQ expectations, quarantine)
5. **YAML completeness** — is the source registered in all relevant config files?
