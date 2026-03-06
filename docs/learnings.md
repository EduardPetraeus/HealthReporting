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

### Audit logging: platform-agnostic context manager
Added `AuditLogger` as a context manager that auto-detects backend:
- **DuckDB (local)**: direct INSERT/UPDATE via `duckdb.connect()`
- **Databricks**: `spark.sql()` INSERT/UPDATE against Unity Catalog Delta tables
- **Non-fatal design**: all audit errors are `warnings.warn()` — never stop the pipeline
- **Identical schema** in both backends makes local dev fully representative of cloud behaviour
- **Databricks limitation**: notebooks can't import from local utils/ packages. Solution: `audit_logger_notebook.py` as inline copy, loaded via `%run`. Enterprise pattern: package as wheel, install on cluster.
- **Delta UPDATE caveat**: `UPDATE job_runs SET status='success'` is expensive at high frequency because Delta re-writes files. Enterprise alternative: append-only + reconciliation view.
- **Python 3.9 gotcha**: `str | None` union type syntax requires Python 3.10+. Use `Optional[str]` from `typing` for 3.9 compatibility.

### Delta `DEFAULT` column constraint requires feature flag
`CREATE TABLE ... status STRING DEFAULT 'running'` fails on Databricks Serverless SQL with:
`WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED`.
**Fix**: remove `DEFAULT` from DDL — AuditLogger always writes `'running'` explicitly on INSERT anyway. Enterprise alternative: enable via `ALTER TABLE SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')` after creation.

### Audit schema deployment (2026-02-28)
Deployed `health-platform-dev.audit` with `job_runs`, `table_runs`, `v_platform_overview`.
Verified: JOIN on `job_id` FK works, view aggregates correctly across 7-day window.
Data landscape after deployment:
- `workspace.default`: 14 raw tables (old pipeline — lifesum, oura, strava, withings)
- `health_dw.silver`: 11 silver tables (old pipeline — still has data)
- `health-platform-dev`: new catalog — audit schema live, bronze/silver/gold empty (no autoloader yet)

---

## AI-Native Data Model Learnings (2026-03-04)

### Parallel subagent dispatch works — but column names diverge
Dispatched 4 subagents in parallel (SQL DDL, YAML contracts, AI modules, MCP server). All completed successfully, but the AI modules used different column names than the DDL (e.g., `key` vs `profile_key`, `metric_a` vs `source_metric`, `generated_at` vs `created_at`). **Fix**: after parallel dispatch, always diff generated code against DDL schemas. Budget 30-60 min for column name alignment.

### SQL file parsing: semicolons inside quoted strings
`sql.split(";")` breaks when SQL strings contain semicolons (e.g., `'Highly individual; trend matters more than absolute value'`). Required a character-level state machine parser (`split_sql_statements()`) that tracks `in_string` state. **Lesson**: never use naive semicolon splitting for SQL files with descriptive text.

### Python 3.9 vs 3.12 for modern packages
The `mcp` package requires Python 3.10+. Project venv is Python 3.9. Created separate `.venv-ai/` with Python 3.12 (`/opt/homebrew/bin/python3.12`) for AI-specific dependencies. **Lesson**: pin Python version in pyproject.toml and upgrade project venv when adding modern dependencies.

### DuckDB HNSW index persistence
`CREATE INDEX ... USING HNSW` fails on persistent databases with: `HNSW indexes can only be created in in-memory databases`. **Fix**: `SET hnsw_enable_experimental_persistence = true` before creating the index. This is an experimental feature in DuckDB vss extension — monitor for stability.

### Template-based text generation vs LLM
Chose template-based generation (`text_generator.py`) over LLM API calls for daily summaries. Benefits: deterministic, zero cost, no latency, no API dependency. The generated text is good enough for embedding and semantic search. **Lesson**: for structured data → text, templates are often better than LLMs.

### Vector search quality with general-purpose embeddings
all-MiniLM-L6-v2 (general-purpose, 384-dim) produces semantically meaningful results for health summaries. "Excellent recovery day" finds days with high sleep + readiness scores. "High stress levels" finds the anomaly day with 20,700 stress minutes. Domain-specific fine-tuning may not be necessary for this use case.

### Semantic Contracts as "Virtual Gold"
YAML computation recipes effectively replace materialized Gold views at N=1 scale. Adding a new metric requires only a YAML file, not DDL changes. The query_builder reads YAML, substitutes parameters, and executes SQL — same result as a Gold view, but on-demand. **Caveat**: requires well-maintained YAML files; no automatic schema drift detection yet.

### localStorage + `unsafe-inline` CSP — accepted risk (2026-03-06)

The chat UI stores the API token in `localStorage` and uses `unsafe-inline` for both script and style CSP directives. This is an accepted trade-off for this deployment:

**Why it's acceptable:**
- App binds to `127.0.0.1`, accessible only via Tailscale VPN (no public internet)
- No user-generated content rendered as raw HTML — all output is escaped via `escapeHtml()`
- Single-user deployment, no multi-tenant attack surface
- `X-Frame-Options: DENY` and `X-Content-Type-Options: nosniff` headers set

**If the app ever goes public, revisit:**
- Switch token storage from `localStorage` to `HttpOnly` cookie (requires server-side session)
- Replace `unsafe-inline` with nonce-based CSP (`script-src 'nonce-<random>'`)
- Add `Strict-Transport-Security` header
- Consider CSRF tokens for state-changing endpoints

### Keychain-first secrets management (2026-03-06)

All secrets are stored in `~/Library/Keychains/claude.keychain-db` and read via the shared `get_secret()` utility. Environment variables serve as fallback for CI/testing only. The `.env` file pattern has been fully removed from Oura auth in favour of keychain reads.

*Last updated: 2026-03-06*
