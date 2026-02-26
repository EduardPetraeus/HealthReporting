---
name: build-validator
description: Validate the Databricks Asset Bundle before pushing. Use when asked to validate, check, or verify the bundle.
---

# Build Validator

Validate the Databricks Asset Bundle (DAB):

1. Check `health_unified_platform/health_environment/deployment/databricks/databricks.yml` for syntax and required fields
2. Verify all referenced job files and notebooks exist on disk
3. Scan source YAML configs in `health_environment/config/databricks/sources/` — list any missing or incomplete entries
4. Scan silver SQL files in `transformation_logic/databricks/silver/sql/` — check for MERGE INTO pattern, `_ingested_at` watermark
5. Cross-reference: every source YAML should have a matching silver SQL file
6. Report a clear summary: what's complete, what's missing, what's broken
