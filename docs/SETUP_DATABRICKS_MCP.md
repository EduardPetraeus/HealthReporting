# Databricks MCP Setup

## What is configured

A **self-hosted stdio MCP server** from the [databricks-mcp-server](https://github.com/databricks-community/mcp-server-databricks) package is configured at the project level in `.mcp.json`. This enables Claude Code to query Unity Catalog, inspect schemas, run SQL, manage jobs, and more — directly from the chat session.

## Configuration

### `.mcp.json` (project-level)

```json
{
  "mcpServers": {
    "databricks": {
      "command": "$HOME/.ai-dev-kit/.venv/bin/python",
      "args": ["$HOME/.ai-dev-kit/repo/databricks-mcp-server/run_server.py"],
      "env": {
        "DATABRICKS_CONFIG_PROFILE": "DEFAULT",
        "DATABRICKS_DEFAULT_CATALOG": "health-platform-dev"
      }
    }
  }
}
```

`DATABRICKS_DEFAULT_CATALOG` is a **convention marker** — it documents the safe default. The MCP server itself is catalog-agnostic (catalog must be specified per query or as a parameter).

### `~/.databrickscfg`

Auth is handled via the `DEFAULT` profile:

```ini
[DEFAULT]
host  = https://dbc-23749322-8818.cloud.databricks.com
token = <PAT — stored locally, never in repo>
```

## Catalog structure

| Catalog | Purpose | Status |
|---|---|---|
| `health-platform-dev` | Active dev target (bundle deploys here) | Schemas created, tables being populated |
| `health-platform-prd` | Active prd target (bundle deploys here) | Schemas created, isolated |
| `health_dw` | Legacy catalog from earlier platform version | Has silver + gold data |

> **Note:** `health_dw` currently holds the main silver/gold data from earlier deployments. As new bundle deployments run, data will populate `health-platform-dev`. Both catalogs have schemas: `bronze`, `silver`, `gold`.

Catalog names contain hyphens — always use backtick quoting in SQL:

```sql
-- Correct
SELECT * FROM `health-platform-dev`.silver.heart_rate

-- Wrong (will error)
SELECT * FROM health-platform-dev.silver.heart_rate
```

## Available MCP tools

The self-hosted server exposes 60+ tools. Key ones for data work:

| Tool | Use |
|---|---|
| `mcp__databricks__execute_sql` | Run SQL queries against any catalog/schema |
| `mcp__databricks__get_table_details` | Inspect table schema, columns, partitions |
| `mcp__databricks__manage_uc_objects` | List/create/get catalogs, schemas, volumes |
| `mcp__databricks__list_warehouses` | List SQL warehouses |
| `mcp__databricks__get_best_warehouse` | Auto-select cheapest running warehouse |
| `mcp__databricks__get_current_user` | Verify auth identity |
| `mcp__databricks__list_jobs` | List Databricks jobs |
| `mcp__databricks__run_job_now` | Trigger a job run |
| `mcp__databricks__manage_uc_grants` | Inspect/manage Unity Catalog permissions |

## Example queries

```sql
-- List schemas in dev catalog
SHOW SCHEMAS IN `health-platform-dev`

-- List tables
SHOW TABLES IN `health-platform-dev`.silver

-- Count rows in legacy silver table
SELECT count(*) FROM health_dw.silver.heart_rate

-- Sanity check current context
SELECT current_catalog(), current_schema()

-- Sample data
SELECT * FROM health_dw.silver.heart_rate LIMIT 10
```

## Verification (run to confirm setup)

```
mcp__databricks__get_current_user        → <your-databricks-email> ✅
mcp__databricks__manage_uc_objects       → lists health-platform-dev, prd, health_dw ✅
execute_sql: SHOW SCHEMAS IN `health-platform-dev`  → bronze, silver, gold ✅
execute_sql: SELECT count(*) FROM health_dw.silver.heart_rate → 1,004,696 ✅
```

---

## ⚠️ DEV / PRD SAFETY RULE

> **ALL MCP SQL queries in Claude Code default to `health-platform-dev` or `health_dw`.**
>
> Any query touching **`health-platform-prd`** must be preceded by **explicit user confirmation** in the conversation.
>
> Claude Code will not query `health-platform-prd` without the user explicitly saying so.

This is a workflow convention — the MCP server has no technical lock. The `DATABRICKS_DEFAULT_CATALOG` env var documents the intended safe default.

## Deploy workflow: feature branches → dev

When working on feature branches, deploy changes directly to the dev environment before opening a PR:

```bash
# From health_unified_platform/
databricks bundle deploy --target dev
```

This deploys the current branch's notebooks, jobs, and config to `health-platform-dev`. You do **not** need to merge to `main` first — dev deployments are safe to run from any branch. The GitHub Actions pipeline handles auto-deploy to prd after merging to `main`.

---

## Why self-hosted stdio vs Databricks Managed MCP

Databricks offers a managed HTTP-based MCP endpoint (`/api/2.0/mcp`). We use the self-hosted stdio server instead because:

| | Managed MCP (HTTP) | Self-hosted stdio (current) |
|---|---|---|
| Auth | PAT header / OAuth | `~/.databrickscfg` — already configured |
| Tools | SQL, Vector Search, Genie, UC Functions | Everything: SQL, Jobs, UC, Pipelines, Dashboards, Compute |
| Setup needed | New MCP entry + OAuth app | Nothing — already works |

The managed MCP option is noted as a **future PoC alternative** if remote HTTP transport is needed for enterprise demo scenarios (e.g., showing remote MCP connectivity from a client machine without local `databrickscfg`).
