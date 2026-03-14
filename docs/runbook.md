# Runbook

## Prerequisites

```bash
# Activate venv (always run from repo root)
source .venv/bin/activate

# Close any open DuckDB connections (VS Code SQL Tools, duckdb CLI)
# before running any script — DuckDB only allows one writer at a time.
```

---

## Apple Health: XML → Parquet

```bash
python health_unified_platform/health_platform/source_connectors/apple_health/process_health_data.py \
  --input /path/to/eksport.xml \
  --output /Users/Shared/data_lake/apple_health_data
```

---

## Lifesum (or any CSV): CSV → Parquet

```bash
python health_unified_platform/health_platform/source_connectors/csv_to_parquet.py \
  --input /path/to/file.csv \
  --output /Users/Shared/data_lake/lifesum/parquet/food \
  --source-name lifesum_food
```

---

## Oura Ring: Fetch from API → Parquet

First-time: opens a browser for OAuth login. Subsequent runs: incremental from last fetched date.

```bash
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py
```

---

## Strava: Fetch from API → Parquet

First-time: opens a browser for OAuth login (requires `STRAVA_CLIENT_ID` and `STRAVA_CLIENT_SECRET` in claude.keychain-db). Subsequent runs: tokens auto-refresh, only new data fetched. Fetches activities (incremental) and athlete stats (always refreshed).

```bash
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/strava/run_strava.py
```

---

## Withings: Fetch from API → Parquet

First-time: opens a browser for OAuth login (requires `WITHINGS_CLIENT_ID` and `WITHINGS_API_KEY` in claude.keychain-db). Full load from 2020-01-01 on first run. Subsequent runs: incremental with 30-day overlap window for dedup safety.

```bash
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/withings/run_withings.py
```

---

## Weather (Open-Meteo): Fetch from API → Parquet

Fetches daily weather data from Open-Meteo (free, no API key required). Default: last 90 days. Override with `WEATHER_PAST_DAYS` env var.

```bash
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/weather/run_weather.py

# Custom range (e.g. 365 days)
WEATHER_PAST_DAYS=365 HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/weather/run_weather.py
```

---

## sundhed.dk: Web Scrape → Parquet

Scrapes clinical data from sundhed.dk via MitID-authenticated browser session (Playwright). Full load only — no incremental sync (data changes quarterly at most). Supports 5 sections: lab_results, medications, vaccinations, ejournal, appointments.

Requires MitID authentication — a browser window opens for interactive login.

```bash
# All sections
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/sundhed_dk/run_sundhed_dk.py

# Specific sections only
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/sundhed_dk/run_sundhed_dk.py \
  --sections lab_results,medications
```

---

## Lab Results: PDF → Parquet

Scans a directory for blood test PDFs (GetTested, sundhed.dk, etc.), parses markers, computes business keys, and writes a single parquet file. Supports multiple lab formats with auto-detection.

```bash
python -c "
from pathlib import Path
from health_platform.source_connectors.lab.lab_ingestion import ingest_lab_pdfs

ingest_lab_pdfs(
    pdf_dir=Path('/Users/Shared/data_lake/lab_results/downloads'),
    output_dir=Path('/Users/Shared/data_lake/lab_results/parquet'),
)
"
```

---

## 23andMe Genetics: PDF/CSV/JSON → Parquet

Scans a directory for 23andMe exports (PDF health reports, ancestry CSV, family tree JSON). Routes each file to the appropriate parser and writes 5 bronze parquet streams: health_findings, ancestry_segments, family_tree, ancestry_composition, genetic_traits.

```bash
python -c "
from pathlib import Path
from health_platform.source_connectors.genetics.genetics_ingestion import ingest_genetics_data

ingest_genetics_data(
    input_dir=Path('/Users/Shared/data_lake/genetics/23andme/downloads'),
    output_dir=Path('/Users/Shared/data_lake/genetics/23andme/parquet'),
)
"
```

---

## Lifesum PDF: Download Nutrition Report

Queries the latest data date from silver.daily_meal, then downloads a 7-day PDF from Lifesum covering the gap.

```bash
python -m health_platform.source_connectors.lifesum.run_lifesum_pdf
```

---

## Bronze Ingestion: Parquet → DuckDB

Reads `sources_config.yaml`, loads all configured parquet sources into `bronze.stg_*` tables.

```bash
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py
```

---

## Silver: Create Table Schema (run once per new table)

Uses dbt to create the empty schema-only table. Must be run before the first merge.

```bash
DBT_DIR=health_unified_platform/health_platform/transformation_logic/dbt
HEALTH_ENV=dev dbt run --project-dir "$DBT_DIR" --profiles-dir "$DBT_DIR" --select <model_name>

# Example — create all silver tables at once:
HEALTH_ENV=dev dbt run --project-dir "$DBT_DIR" --profiles-dir "$DBT_DIR"
```

---

## Silver: Load Data (run_merge.py)

Runs a staging → MERGE INTO silver → drop staging pipeline for one entity.

```bash
cd health_unified_platform/health_platform/transformation_logic/dbt/merge

# Single script
HEALTH_ENV=dev python run_merge.py silver/merge_oura_daily_sleep.sql

# All Oura scripts
for f in silver/merge_oura_daily_sleep.sql \
          silver/merge_oura_daily_activity.sql \
          silver/merge_oura_daily_readiness.sql \
          silver/merge_oura_heartrate.sql \
          silver/merge_oura_workout.sql \
          silver/merge_oura_daily_spo2.sql \
          silver/merge_oura_daily_stress.sql \
          silver/merge_oura_personal_info.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done

# All Apple Health scripts
for f in silver/merge_apple_health_*.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done

# Strava
HEALTH_ENV=dev python run_merge.py silver/merge_strava_activities.sql

# Weather
HEALTH_ENV=dev python run_merge.py silver/merge_weather_daily.sql

# Lab results
HEALTH_ENV=dev python run_merge.py silver/merge_lab_pdf_results.sql

# 23andMe Genetics (3 tables)
for f in silver/merge_23andme_ancestry.sql \
          silver/merge_23andme_family_tree.sql \
          silver/merge_23andme_health_findings.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done

# Withings (all endpoints)
for f in silver/merge_withings_*.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done

# Lifesum (food, bodyfat, exercise, weighins)
for f in silver/merge_lifesum_*.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done
```

---

## Typical End-to-End Run (Oura)

```bash
source .venv/bin/activate

# 1. Fetch new data from API
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py

# 2. Load into bronze
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py

# 3. Merge into silver
cd health_unified_platform/health_platform/transformation_logic/dbt/merge
for f in silver/merge_oura_*.sql; do HEALTH_ENV=dev python run_merge.py "$f"; done
```

---

## Typical End-to-End Run (All API Sources)

```bash
source .venv/bin/activate

# 1. Fetch new data from all API sources
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/strava/run_strava.py
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/withings/run_withings.py
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/weather/run_weather.py

# 2. Load all parquet into bronze
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py

# 3. Merge all sources into silver
cd health_unified_platform/health_platform/transformation_logic/dbt/merge
for f in silver/merge_oura_*.sql silver/merge_apple_health_*.sql \
         silver/merge_strava_*.sql silver/merge_weather_*.sql \
         silver/merge_withings_*.sql silver/merge_lifesum_*.sql \
         silver/merge_lab_*.sql silver/merge_23andme_*.sql; do
  HEALTH_ENV=dev python run_merge.py "$f"
done
```

---

---

## Databricks Framework: First Deployment

### Prerequisites

- Databricks CLI >= 0.200 installed
- `health_dw` catalog exists in Unity Catalog
- Storage account paths filled in — edit `health_environment/config/databricks/sources/*.yml` and replace `yourstorageaccount` with the real account name and container paths
- Workspace URLs filled in — edit `health_environment/deployment/databricks/databricks.yml` targets

```bash
# Authenticate (or use ~/.databrickscfg profile)
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=<pat-or-service-principal-token>

# Deploy bundle to dev
cd health_unified_platform/health_environment/deployment/databricks
databricks bundle deploy --target dev

# Run one-time schema init (from workspace UI: open init.py and run)
# Creates health_dw.bronze, health_dw.silver, health_dw.gold schemas

# Trigger pipeline manually
databricks bundle run bronze_pipeline --target dev
databricks bundle run silver_pipeline --target dev
databricks bundle run gold_pipeline   --target dev

# Deploy to production
databricks bundle deploy --target prd
```

After first deploy: check the workspace file browser to find where configs and notebooks landed, then update the `config_root` / `sql_root` / `gold_config_root` / `gold_sql_root` variables in `databricks.yml` to match.

---

## Databricks Framework: Onboarding a New Source

1. Create `health_environment/config/databricks/sources/<source_name>.yml` — copy an existing one and fill in paths
2. Create or reuse a SQL file in `transformation_logic/databricks/silver/sql/`
3. Add a task block to `health_environment/orchestration/bronze_job.yml`
4. `git push` → GitHub Actions deploys to production automatically

No Python changes required.

---

## Troubleshooting

**DuckDB lock error** (`Could not set lock on file`):
Close all other DuckDB connections — VS Code SQL Tools extension, open `duckdb` CLI sessions, or other Python processes. DuckDB allows only one writer at a time.

**Semicolons in SQL comments** break `run_merge.py` (it splits on all `;`):
Avoid semicolons inside `--` comments in merge scripts.

**Oura `day` column contains day-of-month integer** (e.g. "21") instead of full date:
This is a Hive partition key conflict. Merge scripts for daily Oura endpoints use `make_date(year, month, day)` to reconstruct the full date. Do not use `day::DATE` directly for these tables.

---

## AI-Native Data Model Setup

One-time setup for the agent schema, knowledge graph, and memory tiers. Requires Python 3.12+ (`.venv-ai/`).

```bash
# Activate AI venv (Python 3.12, has sentence-transformers + mcp)
source .venv-ai/bin/activate

# Run idempotent schema setup (creates agent tables, comments, seeds graph)
HEALTH_ENV=dev python health_unified_platform/health_platform/setup/setup_agent_schema.py
```

### Backfill Daily Summaries + Embeddings

```bash
source .venv-ai/bin/activate
python -c "
import sys; sys.path.insert(0, 'health_unified_platform')
import duckdb
con = duckdb.connect('/Users/Shared/data_lake/database/health_dw_dev.db')

from health_platform.ai.text_generator import backfill_summaries
from health_platform.ai.baseline_computer import compute_all_baselines, compute_demographics
from health_platform.ai.embedding_engine import EmbeddingEngine

backfill_summaries(con)
compute_all_baselines(con)
compute_demographics(con)
EmbeddingEngine().backfill_daily_summaries(con)

con.close()
"
```

### Start MCP Server (for Claude Code integration)

```bash
source .venv-ai/bin/activate
cd health_unified_platform
HEALTH_ENV=dev python -m health_platform.mcp.server
```

The MCP server is also configured in `.mcp.json` for automatic startup by Claude Code.
