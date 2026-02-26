# Bundle Workflows

Databricks Asset Bundle (DAB) workflow definitions included by `databricks.yml`.

Current workflow YAMLs live one level up in `databricks_framework/workflows/`:
- `bronze_job.yml` — daily bronze ingestion via Autoloader
- `silver_job.yml` — daily silver merge
- `gold_job.yml` — daily gold refresh

This folder is reserved for bundle-scoped workflow overrides per environment (dev/prd).
