# Databricks Framework

Generic, metadata-driven cloud pipeline for the health platform. Handles bronze ingestion, silver transformation, and gold views entirely through configuration — no Python changes needed when adding new sources or entities.

## Overview

```
Cloud storage (parquet)
  → Bronze (Autoloader / cloudFiles → Delta table)
  → Silver (MERGE INTO or INSERT INTO REPLACE WHERE → Delta table)
  → Gold (CREATE OR REPLACE VIEW/TABLE)
```

Driven by: **YAML configs** + **SQL files**
Deployed by: **Databricks Asset Bundles (DAB)**
CI/CD: **GitHub Actions** → auto-deploy to production on merge to main

---

## Folder Structure

```
health_unified_platform/databricks_framework/
├── bundles/
│   └── databricks.yml              ← DAB entry point
├── config/
│   ├── sources/                    ← one YAML per source (bronze + silver)
│   │   ├── apple_health_heart_rate.yml
│   │   └── oura_heart_rate.yml
│   └── gold/                       ← one YAML per gold entity
│       └── daily_heart_rate_summary.yml
├── notebooks/
│   ├── setup/
│   │   └── init.py                 ← one-time schema creation (run once)
│   ├── bronze/
│   │   └── bronze_autoloader.py    ← generic Autoloader notebook
│   ├── silver/
│   │   ├── silver_runner.py        ← generic silver runner
│   │   └── sql/
│   │       ├── heart_rate.sql
│   │       └── _template_insert_overwrite.sql
│   └── gold/
│       ├── gold_runner.py          ← generic gold runner
│       └── sql/
│           └── daily_heart_rate_summary.sql
├── workflows/
│   ├── bronze_job.yml              ← DAB job: daily bronze ingestion
│   ├── silver_job.yml              ← DAB job: daily silver merge
│   └── gold_job.yml                ← DAB job: daily gold refresh
└── .github/
    └── workflows/
        └── deploy.yml              ← GitHub Actions CI/CD
```

---

## How It Works

### Source Config (YAML)

Each source is described by a single YAML file in `config/sources/`. The file covers both bronze ingestion and silver transformation — no Python changes needed.

```yaml
name: apple_health_heart_rate
source_system: apple_health          # stamped on every row; part of the merge key

bronze:
  autoloader:
    source_path: "abfss://raw@account.dfs.core.windows.net/apple_health/heart_rate/"
    format: parquet
    checkpoint_location: "abfss://checkpoints@account.dfs.core.windows.net/..."
    schema_location:     "abfss://schemas@account.dfs.core.windows.net/..."
    target_table: health_dw.bronze.stg_apple_health_heart_rate
    load_mode: incremental           # incremental (cloudFiles streaming) | full (batch)
    options: {}                      # extra Spark reader options

silver:
  sql_file: heart_rate.sql           # file in notebooks/silver/sql/
  target_table: health_dw.silver.heart_rate
  load_mode: merge                   # merge (MERGE INTO) | insert_overwrite
  unique_key:
    - source_system
    - record_id
```

### Silver SQL Files

SQL files live in `notebooks/silver/sql/`. They receive three template variables from `silver_runner.py`:

| Variable | Example value |
|---|---|
| `{source_system}` | `apple_health` |
| `{bronze_table}` | `health_dw.bronze.stg_apple_health_heart_rate` |
| `{silver_table}` | `health_dw.silver.heart_rate` |

Each SQL file starts with `CREATE TABLE IF NOT EXISTS` (idempotent) followed by the merge or insert statement. All transformation logic lives in SQL — the Python runners never change.

**Multiple sources → same silver table**: two sources can point to the same `sql_file` and `target_table` when their bronze schemas are normalised to the same column names. Source isolation is enforced by `source_system` in the merge key — a full reload of one source never touches another source's rows.

**Merge mode** (`load_mode: merge`):
```sql
MERGE INTO {silver_table} AS target
USING (SELECT ... FROM {bronze_table} WHERE source_system = '{source_system}') AS source
ON target.source_system = source.source_system AND target.record_id = source.record_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

**Insert overwrite mode** (`load_mode: insert_overwrite`):
```sql
INSERT INTO {silver_table}
REPLACE WHERE source_system = '{source_system}'
SELECT ... FROM {bronze_table} WHERE source_system = '{source_system}'
```

### Gold Config (YAML)

```yaml
name: daily_heart_rate_summary
type: view                           # view | table
target: health_dw.gold.daily_heart_rate_summary
sql_file: daily_heart_rate_summary.sql
```

Gold SQL files receive one template variable: `{target}`. The SQL contains the full `CREATE OR REPLACE VIEW {target} AS ...` statement.

---

## Adding a New Source

1. Create `config/sources/<source_name>.yml` — fill in bronze paths and silver SQL reference
2. Create or reuse a SQL file in `notebooks/silver/sql/`
3. Add one task block to `workflows/bronze_job.yml`:
   ```yaml
   - task_key: bronze_<source_name>
     job_cluster_key: bronze_cluster
     notebook_task:
       notebook_path: ../notebooks/bronze/bronze_autoloader
       base_parameters:
         source_name: <source_name>
         config_root: ${var.config_root}
   ```
4. `git push` → CI/CD deploys automatically

No Python changes. No runner changes. The framework is closed for modification, open for extension.

---

## Adding a New Gold Entity

1. Create `config/gold/<entity_name>.yml`
2. Create `notebooks/gold/sql/<entity_name>.sql` with `CREATE OR REPLACE VIEW {target} AS ...`

The gold job picks it up automatically on the next run (`entity_name: ""` processes all configs).

---

## First Deployment

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- `health_dw` catalog exists (create in Databricks UI or with admin rights)
- Cloud storage account (ADLS Gen2 / S3) with External Location registered in Unity Catalog
- Databricks CLI >= 0.200 installed: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`

### Steps

```bash
# 1. Fill in real values in databricks.yml (workspace URLs) and source YAMLs (storage paths)

# 2. Authenticate
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=<your-token>

# 3. Deploy the bundle to dev
cd health_unified_platform/databricks_framework/bundles
databricks bundle deploy --target dev

# 4. Find the deployed workspace path and update variables in databricks.yml
#    (check workspace file browser after deploy)

# 5. Run init.py once — creates bronze, silver, gold schemas
#    Open notebooks/setup/init.py in the Databricks workspace UI and run it

# 6. Trigger the pipeline
databricks bundle run bronze_pipeline --target dev
databricks bundle run silver_pipeline --target dev
databricks bundle run gold_pipeline   --target dev

# 7. Verify
# Query: SELECT * FROM health_dw.gold.daily_heart_rate_summary ORDER BY day DESC
```

### Deploy to Production

```bash
databricks bundle deploy --target prd
```

Or merge to `main` — GitHub Actions deploys automatically.

---

## CI/CD (GitHub Actions)

File: `.github/workflows/deploy.yml`

| Trigger | Job | Action |
|---|---|---|
| Pull request | `validate` | `databricks bundle validate --target dev` |
| Merge to `main` | `deploy-prd` | `databricks bundle deploy --target prd` |
| Manual dispatch | `deploy-dev` or `deploy-prd` | Deploy to chosen target |

Required GitHub secrets:

| Secret | Description |
|---|---|
| `DATABRICKS_HOST_DEV` | Dev workspace URL |
| `DATABRICKS_TOKEN_DEV` | Dev PAT or service principal token |
| `DATABRICKS_HOST_PRD` | Production workspace URL |
| `DATABRICKS_TOKEN_PRD` | Production PAT or service principal token |

---

## Dev / Production Separation

Controlled by DAB targets in `bundles/databricks.yml`. Key differences:

| Setting | Dev | Prd |
|---|---|---|
| `mode` | `development` (prefixes job names, isolated) | `production` |
| `workspace.host` | Dev workspace URL | Prd workspace URL |
| `env` variable | `dev` | `prd` |
| Deploy command | `bundle deploy --target dev` | `bundle deploy --target prd` |

---

## Runtime Dependencies

| Dependency | Notes |
|---|---|
| Databricks Runtime 15.4+ | `pyyaml` and `pyspark` available by default — no extra libraries needed |
| Unity Catalog | Catalog `health_dw` + schemas `bronze`, `silver`, `gold` |
| External Location | Grants the cluster access to cloud storage paths |
| Databricks CLI 0.200+ | For local and CI/CD deployment |

---

## Relationship to Local Pipeline

The local DuckDB pipeline (`health_platform/`) and this Databricks framework are parallel implementations of the same medallion architecture. They share:
- The same source connectors (parquet output format)
- The same silver entity names and column conventions
- The same `source_system` isolation pattern

They differ in execution engine (DuckDB vs Spark), deployment (local script vs DAB), and scale. The Databricks framework is the production cloud target; the local pipeline is for development and experimentation.
