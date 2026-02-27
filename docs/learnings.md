# Learnings & Architecture Notes

Running log of insights, decisions, and "would-do-differently" reflections.
Used as reference for future projects and PoC presentations.

---

## Architecture Decisions

### DuckDB + Databricks dual-stack
Running DuckDB locally and Databricks in the cloud is the right pattern for a personal PoC:
- DuckDB handles local dev, exploration, and testing — zero cost, zero setup
- Databricks handles production ingestion, storage (Delta), and scheduled jobs
- The same SQL logic runs in both environments without modification
- **Enterprise upgrade path**: replace DuckDB with a shared dev Databricks workspace

### YAML-driven metadata layer
New data sources require zero code changes — add one YAML file, everything else flows automatically through `silver_runner` and `gold_runner`. This is the correct pattern for a metadata-driven platform.

### Dev/prd separation via separate workspaces (not catalogs)
Initially planned to use two Unity Catalogs (`health_dw_dev` / `health_dw_prd`) within one workspace. Upgraded to two separate AWS Databricks workspaces (URLs stored in `~/.databrickscfg` and GitHub Secrets — never committed to repo):
- **Why**: separate workspaces give cleaner IAM, billing, and access isolation — closer to enterprise standard
- **Catalog inside each workspace**: `health_dw` with schemas `bronze`, `silver`, `gold`

### Silver runner pattern (SQL files + Python runner)
`silver_runner.py` reads YAML configs and executes SQL MERGE files. Elegant and transparent, but dbt would give this for free plus lineage, tests, and incremental materializations. Kept as-is for PoC speed — port to dbt is the documented next step.

### Medallion architecture with strict layer boundaries
Bronze = raw data. Silver = standardised. Gold = business logic. No mixing between layers. Easy to explain, easy to maintain, easy to extend.

---

## Migrations Done

### Python notebooks → pure SQL files
Early silver/gold transforms were Python notebooks with embedded SQL strings. Migrated to:
- Pure `.sql` files for all transforms
- Python runner scripts (`silver_runner.py`, `gold_runner.py`) that execute the SQL
- **Why**: SQL files are version-controlled cleanly, diff-readable, and portable

### `workspace.default` → `health_dw.bronze` as source for silver
Early silver queries read from `workspace.default.*` (the Databricks default catalog). Migrated all silver SQL to read from `health_dw.bronze.*`:
- Consistent catalog reference across all transforms
- Eliminates hidden dependency on workspace default catalog setting

### Bundle root moved up to `health_unified_platform/`
`databricks.yml` originally lived in `health_environment/deployment/databricks/`. Notebooks lived in `health_platform/transformation_logic/databricks/`. Bundle sync could not reach the notebooks.

**Fix**: moved `databricks.yml` up to `health_unified_platform/` so all assets are within sync root.

### Removed `job_clusters` from all orchestration YAMLs
AWS Databricks workspaces with serverless compute reject jobs that specify `new_cluster` configuration. Error: `Only serverless compute is supported in this workspace`.

**Fix**: remove `job_clusters` section entirely from `bronze_job.yml`, `silver_job.yml`, `gold_job.yml`. Databricks selects serverless automatically. No cluster config needed.

---

## CI/CD Setup

### GitHub Actions → Databricks deploy pipeline
- Feature branches auto-deploy to dev on push
- Merge to main auto-deploys to prd
- `bundle validate` runs on every PR as a gate
- **Secrets required**: `DATABRICKS_HOST_DEV`, `DATABRICKS_TOKEN_DEV`, `DATABRICKS_HOST_PRD`, `DATABRICKS_TOKEN_PRD`

### PAT token scope
Use `all-apis` scope for CI/CD and bundle deployment. The bundle uses Workspace API, Jobs API, Clusters API, and Unity Catalog API simultaneously.

### GitHub Secrets — always use interactive prompt
```bash
gh secret set SECRET_NAME
# paste value at prompt — never use --body flag
```
The `--body` flag exposes the secret in shell history. Always use the interactive prompt.

---

## Databricks Quirks

| Issue | Root Cause | Fix |
|---|---|---|
| `bundle sync root cannot reach notebook` | `databricks.yml` too deep in folder structure | Move `databricks.yml` to common ancestor of all assets |
| `Only serverless compute is supported` | `job_clusters` / `new_cluster` block present in job YAML | Remove `job_clusters` section entirely |
| `unknown field: environment_variables` | Task-level `environment_variables` not supported in DAB schema | Use `base_parameters` in `notebook_task` instead |
| `option 'host' already exists` in MCP | Duplicate `[DEFAULT]` section in `~/.databrickscfg` | Rewrite file with single clean `[DEFAULT]` section |
| `Standard_DS3_v2` node type rejected | Azure node type used in AWS workspace | Use `i3.xlarge` (AWS) or remove `node_type_id` for serverless |

---

## Would Do Differently From Day 1

### 1. dbt instead of raw SQL runners
`silver_runner.py` + SQL files works, but dbt gives for free:
- Incremental logic (`{{ config(materialized='incremental') }}`)
- Schema tests (not-null, unique, accepted-values)
- Auto-generated documentation and lineage graph
- Runs identically on DuckDB locally and Databricks in the cloud

### 2. Service principals instead of PAT tokens
PAT tokens are personal and expire. Enterprise standard from day 1:
- Create a **Databricks service principal** for CI/CD
- Use **OAuth M2M** (machine-to-machine) instead of PAT
- Store credentials in Databricks Secrets API or HashiCorp Vault — not in `~/.databrickscfg`

### 3. Data quality as a gate, not an afterthought
DLT expectations or Great Expectations should be a hard gate in bronze → silver:
- Bad data must never reach silver
- Failed rows routed to `bronze.quarantine_<source>` with error description
- DQ metrics logged to `gold.data_quality_metrics` daily

### 4. Governance and tagging from the start
Unity Catalog tags, PII classification, and data lineage are easy to add incrementally but **harder to retrofit** across many tables. Tag the first table on day 1 and hold the standard.

---

## Security Rules

- Tokens, passwords, and secrets are **never** shared in chat
- GitHub Secrets set via `gh secret set <NAME>` interactive prompt (no `--body` flag)
- Databricks credentials in `~/.databrickscfg` (global, never in repo)
- PII data (genetics, blood tests, diagnoses) classified as `sensitive` in UC tags

---

*Last updated: 2026-02-27*
