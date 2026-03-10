# HealthReporting

Personal health intelligence platform. Ingests data from wearables, nutrition apps, lab results, and genetics — transforms it through a medallion architecture — and surfaces insights via an AI agent with 16 MCP tools, anomaly detection, and evidence-backed recommendations.

**1300+ tests · 68 bronze sources · 45 silver tables · 16 MCP tools · fully automated daily sync**

## Architecture

Dual-stack with shared Silver layer ([ADR-005](docs/adr/ADR-005-ai-native-data-model.md)):

```
┌─────────────────────────────────────────────────────────────────────┐
│ LOCAL (DuckDB) — AI-Native 2+2                                     │
│                                                                     │
│  Source APIs/files → parquet (hive-partitioned)                     │
│    → Bronze (stg_* tables via ingestion_engine.py)                  │
│    → Silver (45 tables, dbt-duckdb + MERGE, COMMENT ON columns)    │
│    → Agent Memory (patient profile, daily summaries + embeddings,  │
│                    knowledge graph, insight archive)                │
│    → Semantic Contracts (26 YAML metrics) → MCP Server (16 tools)  │
│                                                                     │
│  Gold replaced by AI-native layer — Claude queries via typed MCP   │
│  tools backed by YAML contracts. Never raw SQL.                    │
├─────────────────────────────────────────────────────────────────────┤
│ CLOUD (Databricks) — Traditional Medallion                         │
│                                                                     │
│  Cloud storage → Bronze (Autoloader → Delta)                       │
│    → Silver (MERGE INTO → Delta, YAML-driven)                      │
│    → Gold (Kimball star schema — 10 dimensions + 8 fact tables)    │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | Status | Coverage |
|---|---|---|
| Apple Health | **Active** | 35 types — activity, heart rate, steps, sleep, walking gait, nutrition, body metrics, ECG, SpO2, HRV, VO2 max, audio exposure |
| Oura Ring | **Active** | 18 API + 7 CSV — sleep, activity, readiness, heart rate, stress, resilience, SpO2, workouts |
| Withings | **Active** | 7 types — weight, body composition, blood pressure, temperature |
| Lifesum | **Active** | Food/nutrition logs, body measurements, exercise, weighins |
| 23andMe | **Active** | Genetic variants — SNP parsing, trait reports, ancestry |
| Lab/GetTested | **Active** | Blood test PDF parsing — automated biomarker extraction |
| sundhed.dk | Scaffold | Danish national health portal (Playwright-based) |
| Strava | Scaffold | Workouts, GPS activities |
| Weather | Scaffold | Environmental context |

## Intelligence Layer

| Component | Description |
|---|---|
| **Claude Chat Engine** | Multi-turn health conversations with tool-use (function calling) and SSE streaming |
| **16 MCP Tools** | `query_health`, `search_memory`, `get_profile`, `discover_correlations`, `get_metric_definition`, `record_insight`, `get_schema_context`, `run_custom_query`, `detect_anomalies`, `forecast_metric`, `get_cross_source_insights`, `get_recommendations`, `explain_recommendation`, and more |
| **Anomaly Detection** | Multi-stream z-score analysis, constellation patterns, temporal degradation tracking |
| **Correlation Engine** | 30+ metric pairs, cross-domain delayed effects, Pearson with lag analysis |
| **Trend Forecaster** | Linear regression-based metric forecasting |
| **Recommendation Engine** | Evidence-backed, CDS/FDA-safe health recommendations |
| **PubMed Evidence** | Literature search and citation for clinical context |
| **Notifications** | ntfy.sh push notifications with severity-based alerting |

## AI-Native Data Model

| Layer | Contents |
|---|---|
| `agent.patient_profile` | Core memory — demographics, baselines, conditions (always in context) |
| `agent.daily_summaries` | Daily narratives with 384-dim embeddings (sentence-transformers) |
| `agent.health_graph` | Semantic knowledge graph — biomarkers, supplements, conditions, edges |
| `agent.knowledge_base` | Accumulated insights — vector-searchable via DuckDB VSS (HNSW) |
| `contracts/metrics/` | 26 YAML semantic contracts + business rules + index |

## Genetics Integration

23andMe SNP data provides a static genetic context for all dynamic health metrics — sequenced once, valid for life:

- **Sleep** — CLOCK, PER2, PER3 variants contextualize sleep patterns
- **Cardiovascular** — APOE, MTHFR, ACE inform heart rate, blood pressure interpretation
- **Metabolism** — FTO, MC4R, TCF7L2 add context to weight and activity data
- **Stress response** — COMT, BDNF correlate with HRV and readiness
- **Athletic performance** — ACTN3, ACE, PPARGC1A inform workout effectiveness
- **Pharmacogenomics** — CYP1A2, CYP2D6, CYP2C19 affect supplement metabolism

## Apps

- **Desktop** — pywebview dashboard with live health stats and Claude chat (S2/5 sessions complete)
- **Mobile** — SwiftUI scaffold with direct Claude API integration (backend ready)

## Quick Start

```bash
source .venv/bin/activate

# Full daily sync (what the 06:00 automation runs)
HEALTH_ENV=dev bash scripts/daily_sync.sh

# Or run individual steps:
HEALTH_ENV=dev python health_unified_platform/health_platform/source_connectors/oura/run_oura.py
HEALTH_ENV=dev python health_unified_platform/health_platform/transformation_logic/ingestion_engine.py

# Run tests
pytest tests/ -x -q
```

See `docs/runbook.md` for the full runbook, `docs/architecture.md` for design details, and `docs/paths.md` for key file paths.

## Stack

- **Local**: Python 3.9/3.12, DuckDB, dbt-duckdb, sentence-transformers, FastMCP, pywebview
- **Cloud**: Databricks (Unity Catalog, Delta Lake, Autoloader, Asset Bundles)
- **AI**: Claude (chat + tool-use), sentence-transformers (all-MiniLM-L6-v2), DuckDB VSS (HNSW)
- **CI/CD**: GitHub Actions — bundle validation, AI PR review, governance checks, auto-deploy
- **Storage**: Parquet (hive-partitioned) locally; ADLS/S3 on cloud
- **Testing**: 1300+ tests — pytest, smoke tests, data quality checks

## Gold Layer (Cloud)

Kimball star schema for BI dashboards (Databricks):

- **10 dimension tables** — date, source, metric type, etc.
- **8 fact tables/views** — daily aggregates, trends, cross-source metrics

## Project Status

Built in 25 sessions over 3 days. See `docs/PROJECT_PLAN.md` for phase details and `docs/CHANGELOG.md` for session history.

| Phase | Status |
|---|---|
| A: Foundation (bronze + silver) | Complete |
| B: Intelligence (chat + MCP + anomaly) | Complete |
| C: Clinical (lab + genetics + recommendations) | In progress — gene-health mapping next |
| D: Apps (desktop + mobile + notifications) | In progress — PDF reports next |
| E: Cloud (Databricks gold) | Partial — star schema done |
| F: Tech debt + docs | In progress |
