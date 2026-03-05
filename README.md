# HealthReporting

A personal health data platform that ingests, transforms, and surfaces data from wearable devices and nutrition apps.

## Architecture

Dual-stack architecture with shared Silver layer (see [ADR-005](docs/adr/ADR-005-ai-native-data-model.md)):

**Local (DuckDB) — AI-Native 2+2:**
```
Source APIs / files  →  parquet (hive-partitioned)
  →  bronze (DuckDB stg_* tables via ingestion_engine.py)
  →  silver (21 tables, dbt-duckdb + run_merge.py, COMMENT ON every column)
  →  agent memory (patient profile, daily summaries + embeddings, knowledge graph)
  →  semantic contracts (18 YAML metrics) → MCP server (8 tools)
```

**Cloud (Databricks) — Traditional Medallion:**
```
Cloud storage (parquet)
  →  bronze (Autoloader / cloudFiles → Delta table)
  →  silver (MERGE INTO → Delta table, driven by YAML + SQL)
  →  gold (CREATE OR REPLACE VIEW, driven by YAML + SQL)
```

The local stack replaces Gold with AI-native components: an AI agent (Claude Code) queries health data via typed MCP tools backed by YAML semantic contracts — never raw SQL. The cloud pipeline remains traditional medallion for BI dashboards.

See `docs/databricks_framework.md` for the cloud pipeline reference and `docs/architecture.md` for the full dual-stack design.

## Data Sources

| Source | Status | Entities |
|---|---|---|
| Apple Health | Active | activity, heart rate, steps, walking gait, sleep, nutrition, hygiene, body metrics |
| Oura Ring | Active | sleep, activity, readiness, heart rate, workouts, SpO2, stress, personal info |
| Lifesum | Active | food/nutrition logs |
| Withings | Planned | weight, body composition, blood pressure |
| Strava | Planned | workouts, GPS activities |
| GetTested | Planned | lab results, blood tests |
| DNA Complete (WGS) | Planned | genetic variants (WGS), SNP profile, pharmacogenomics, risk categories |

## Silver Tables (21)

`heart_rate` · `step_count` · `toothbrushing` · `daily_meal` · `daily_walking_gait` · `mindful_session` · `body_temperature` · `respiratory_rate` · `water_intake` · `daily_energy_by_source` · `daily_sleep` · `daily_activity` · `daily_readiness` · `daily_spo2` · `daily_stress` · `workout` · `personal_info` · `blood_pressure` · `weight` · `daily_annotations` · `metric_relationships`

## AI-Native Components (Local)

| Component | Description |
|---|---|
| `agent.patient_profile` | Core memory — 9 entries (demographics + baselines), always in context |
| `agent.daily_summaries` | Recall memory — 91 daily narratives with 384-dim embeddings |
| `agent.health_graph` | Knowledge graph — 67 nodes, 108 edges (biomarkers, supplements, conditions) |
| `agent.knowledge_base` | Archival memory — accumulated insights, vector-searchable |
| `contracts/metrics/` | 18 YAML semantic contracts + index + business rules |
| `mcp/server.py` | MCP server with 8 tools (query_health, search_memory, get_profile, etc.) |

## Cross-Analysis: Genetics x Health Data

DNA Complete whole genome sequencing (WGS) provides a static genetic context layer for all dynamic health metrics. Unlike time-series data from wearables, genetic data is sequenced once and valid for life — it acts as a permanent lens through which all other health signals are interpreted.

The cross-analysis strategy maps specific gene variants to existing silver tables:
- **Sleep** — CLOCK, PER2, PER3 variants contextualize sleep_score, timing, and latency
- **Cardiovascular** — APOE, MTHFR, ACE variants inform heart_rate, blood_pressure, and readiness interpretation
- **Metabolism** — FTO, MC4R, TCF7L2 variants add context to weight, calorie, and activity data
- **Stress response** — COMT, BDNF variants correlate with stress_level, HRV, and readiness
- **Athletic performance** — ACTN3, ACE, PPARGC1A variants inform workout effectiveness and activity_score
- **Pharmacogenomics** — CYP1A2, CYP2D6, CYP2C19 variants affect supplement metabolism and caffeine timing

This transforms HealthReporting from a reactive monitoring platform into a genetically-informed health intelligence system.

## Quick Start

```bash
source .venv/bin/activate

# Fetch Oura data (incremental)
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py

# Load all sources into bronze
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py

# Merge into silver
cd health_unified_platform/health_platform/transformation_logic/dbt/merge
for f in silver/merge_oura_*.sql; do HEALTH_ENV=dev python run_merge.py "$f"; done
```

See `docs/runbook.md` for the full runbook, `docs/architecture.md` for design details, `docs/paths.md` for key file locations, and `docs/databricks_framework.md` for the cloud pipeline reference.

## Stack

- **Local**: Python 3.9/3.12, DuckDB, dbt-duckdb, sentence-transformers, MCP, pyarrow, pandas
- **Cloud**: Databricks (Unity Catalog, Delta Lake, Autoloader, Asset Bundles)
- **AI**: sentence-transformers (all-MiniLM-L6-v2), DuckDB VSS (HNSW), FastMCP
- **CI/CD**: GitHub Actions → auto-deploy on merge to main
- **Storage**: Parquet (hive-partitioned) locally; ADLS/S3 on cloud
