# TODO

## Prioritized Work Order

Overall sequence — work top to bottom. Each block depends on the ones above.

| # | Task | Blocks | Section |
|---|--------|-------------|---------|
| ~~1~~ | ~~Databricks workspace URLs + secrets~~ | ~~Everything on Databricks~~ | Resolved |
| 2 | Full data model (all sources) | Connectors + gold design | Data Model |
| 3 | P0 quick wins (logging, path validation) | Stable local pipeline | Optimization |
| 4 | Withings API connector | Withings scheduled job | Withings Connector |
| 5 | Strava API connector | Strava scheduled job | Strava Connector |
| 6 | Min Sundhed connector (sundhed.dk) | Clinical data in the platform | Min Sundhed Connector |
| 7 | Lifesum login agent | Daily food data | Lifesum Connector |
| 8 | Apple Health auto-ingest zip flow | Apple in scheduled flow | Apple Auto-Ingest |
| 9 | Scheduled jobs — all sources in Databricks | Full automation | Scheduled Jobs |
| 10 | Web app MVP (Streamlit + Cloudflare) | Dashboard from iPhone | Web App |
| 11 | Data SLA + monitoring | Data quality in prd | Data SLA |

## Configuration

- [x] **`databricks.yml` workspace URLs** — dev and prd workspace URLs configured
- [x] **GitHub Secrets** — `DATABRICKS_HOST_DEV`, `DATABRICKS_TOKEN_DEV`, `DATABRICKS_HOST_PRD`, `DATABRICKS_TOKEN_PRD` set up. CI/CD runs fully.
- [x] **`.env.example`** — documented: `HEALTH_ENV`, Oura, Databricks (local), Withings/Strava placeholders
- [x] **Re-enable branch protection status check** — `Validate bundle` enabled as required check on `main`.

## Databricks Framework — Complete Coverage

Work in progress. Currently only example configs exist.

- [ ] **Source YAML configs** — 2 / 27 configured in `health_environment/config/databricks/sources/`
- [ ] **Silver SQL transforms** — 1 / 18 implemented in `transformation_logic/databricks/silver/sql/`
- [ ] **Gold configs + SQL** — 1 entity in `health_environment/config/databricks/gold/` + `transformation_logic/databricks/gold/sql/`

See `README.md` files in each folder for the specific remaining items.

## Apple Health Connector

- [ ] **Workout elements** — XML contains `Workout`, `WorkoutEvent`, `WorkoutStatistics` elements that are currently ignored. Add parsing alongside `Record` elements.
- [ ] **Partition consistency** — Apple Health uses `domain/type/year=YYYY/`, Oura uses `year=YYYY/month=MM/day=DD/`. Consider aligning to one scheme.
- [ ] **End-to-end wrapper script** — single shell script that runs: XML -> parquet -> bronze ingestion -> silver merge for all Apple Health types
- [ ] **State file** — track last ingested export date (`~/.config/health_reporting/apple_health_state.json`) to enable true incremental runs

## Features — Prioritized Backlog

### Tier 1 — High value, builds on existing infrastructure

- [ ] **Personal health API** — FastAPI on Mac Mini exposing the gold layer as a REST endpoint. Foundation for everything else — widgets, Shortcuts, other apps, Databricks. No new infrastructure.
  - [ ] **Cloudflare Tunnel** — gives Mac Mini a fixed public HTTPS URL without a static IP or open router. Free. Databricks and other systems can call the API externally.
  - [ ] **API key authentication** — simple Bearer token header. Versioned endpoints (`/v1/gold/heart_rate`). Rate limiting.
  - [ ] **Databricks custom connector** — Databricks notebook/job that calls the health API and writes to a Delta table. Demonstrates the health platform as a first-class data source. PoC value for enterprise.
  - [ ] **Custom Spark data source** — packages the HTTP call as a proper Spark connector: `spark.read.format("health_api").option("entity", "heart_rate").load()`. Enterprise-grade PoC.
- [ ] **Weekly health digest** — automatic Markdown report every Monday via GitHub Actions cron. Previous week vs the week before across all silver tables. No new infrastructure.
- [ ] **Anomaly detector** — flag unusual measurements (very high heart rate, very low sleep score) with simple statistics (z-score or percentile). Pure SQL/Python.
- [ ] **Master data annotation app** — local Streamlit app for manual annotation of context that wearables cannot capture: illness, travel, stress, new training routine. Stored in `gold.daily_annotations` and joined on all gold views.

### Tier 2 — High value, more work

- [ ] **Local Streamlit dashboard** — interactive health dashboard running on Mac Mini, reading directly from DuckDB or health API. Multi-page layout: sleep / activity / vitals / trends. Accessible from phone/iPad via local network.
- [ ] **Correlation engine** — automatically find correlations across metrics. "Days with sleep score > 80 result in X% lower resting heart rate the next day." Pure SQL, no ML.
- [ ] **Composite health score** — one daily number calculated from sleep + activity + stress + readiness. Defined as a gold view with weighted inputs.
- [ ] **Withings connector** — see dedicated section. Withings: weight, body fat %, muscle mass, blood pressure. PoC for "plug in any source with one YAML".
- [ ] **Strava connector** — see dedicated section. Running, cycling, rowing — distance, pace, HR zones, elevation.
- [ ] **Min Sundhed connector** — see dedicated section. Clinical data from sundhed.dk: blood tests, diagnoses, medications.
- [ ] **Family platform** — add `user_id` dimension. The architecture already supports source isolation — relatively small refactor to track multiple users' data in the same platform.

### Tier 3 — Enterprise PoC material

- [ ] **Databricks AI/BI dashboard** — BI tool directly on the gold layer. Shows trends, scores, and anomalies. No extra infrastructure when Databricks is configured.
- [ ] **Databricks Genie Space** — natural language interface to gold data. "What was my best week in January?" Killer demo for enterprise. Only requires gold tables + Databricks configuration.
- [ ] **Genie Everywhere — Microsoft Teams** — embed Genie Space in Teams via Copilot Studio. No code, few clicks, OAuth per user. Relevant for day job: eliminates ad-hoc "can you pull..." requests. Requires Pro SQL warehouse (not Free Edition).
- [ ] **Claude API + SQL backend** — alternative to Genie without Databricks dependency. FastAPI + DuckDB/Postgres backend with Claude API for natural language -> SQL. Own the entire stack, no vendor lock-in. Can be sold as a product — Genie integration saved for consulting engagements.
- [ ] **Databricks PoC accelerator** — package this repo as a "30-minute Databricks PoC starter": medallion, CI/CD, dev/prd, metadata-driven. Use internally at the enterprise as an accelerator for new data projects.
- [ ] **Prediction model** — based on yesterday's sleep + activity, predict today's readiness. Simple linear regression. Requires minimum 6 months of historical data.

### Tier 4 — Passive income

- [ ] **Health data platform template** — sell the entire architecture (medallion + connectors + dashboard) as a Databricks template for data engineers who want to track their own health. Starting point: this repo polished and generalized. Price: $149-299.
- [ ] **Personal health API as SaaS** — hosted version of the health API with Oura/Apple Health integration. Monthly subscription for others who want the same setup without building it themselves.
- [ ] **Anonymized benchmark** — "Your sleep score is in the top 30% for men aged 35-45." Requires opt-in data from other users and privacy architecture.

## Data Strategy

> **This section requires a dedicated interview session — do not build from this draft alone.**
> The items below are a rough draft created without proper input. Before any implementation,
> schedule a structured interview/workshop session where the owner defines vision,
> priorities, and principles from scratch. Use the draft as a starting point for questions,
> not as a specification.

The overarching direction for the platform — why we build it, where it goes, and how decisions are made. Foundation for all governance, MDM, and quality work below.

- [ ] **Schedule data strategy interview session** — dedicated session where the AI interviews the owner on: platform vision, target audience, data domains, guiding principles, 3-horizon roadmap, and PoC narrative. Output: `docs/data_strategy.md`.
- [ ] **Write `docs/data_strategy.md`** *(draft only — rewrite after interview)* — platform vision, guiding principles, target audience (personal + PoC demo), data domains (vitals, sleep, activity, nutrition, clinical, genetic), 3-horizon roadmap (1: stable bronze/silver/gold; 2: quality gates + governance; 3: AI/Genie/multi-tenant)
- [ ] **Define data domains** — formally name and bound each domain: which sources belong, who owns it, what questions it answers. Drives tagging, lineage, and Genie Space organization.
- [ ] **Define decision framework** — when do we use DuckDB vs Databricks? When do we use dbt vs raw SQL? When do we use DLT vs Jobs? Document the criteria so future decisions are consistent.
- [ ] **Align strategy with PoC narrative** — every architectural decision should have a "why this demonstrates enterprise readiness" sentence. Document in `docs/data_strategy.md` so it can be used directly in enterprise presentations.

## Data Governance, MDM & Best Practices

Plan and implement enterprise-grade data governance. High PoC value for the enterprise context.

### Data Governance

- [ ] **Data Owner** — document ownership per data source: `source_system`, responsible person, update frequency, sensitivity level
- [ ] **Data Lineage** — trace data from raw source -> bronze -> silver -> gold. Implement via Unity Catalog Lineage (automatic in Databricks) + manual Mermaid diagram in `docs/data_lineage.md`
- [ ] **Data Retention policy** — define how long raw data (bronze parquet) and transformed data (silver/gold Delta) are kept. Implement TTL deletion or archiving
- [ ] **Access Control** — Unity Catalog GRANT statements: who may read bronze/silver/gold? Implement `GRANT SELECT ON SCHEMA` per layer. Document in `docs/access_control.md`
- [ ] **Audit log** — enable Unity Catalog audit log in Databricks. Track who reads/writes what and when
- [ ] **GDPR compliance** — map personally identifiable data (PII) per table. Define `right_to_erasure` procedure: which tables must be deleted if a user requests it?
- [ ] **Data Classification** — tagging of sensitivity level per table/column: `public`, `internal`, `confidential`, `sensitive` (e.g., genetics, blood tests)

### Master Data Management (MDM)

- [ ] **`user_id` dimension** — define a master `user_id` across all sources. Oura, Withings, Apple Health all use different user identifiers — map them to one canonical `user_id` in a `dim_user` table
- [ ] **`dim_user`** — master table: `user_id`, `user_name`, `date_of_birth`, `biological_sex`, `source_system_ids` (JSON map). Foundation for multi-tenant + family platform
- [ ] **`dim_source_system`** — master table of data sources: `source_system`, `display_name`, `category` (wearable/nutrition/clinical/genetic), `ingest_frequency`, `owner`, `sla_hours`
- [ ] **Metric standardization** — define canonical metric names across sources: `heart_rate_bpm` (not `hr`, not `bpm`, not `heartRate`). Document in `docs/metric_dictionary.md`
- [ ] **Unit standardization** — all distance in km, weight in kg, temperature in Celsius. The silver layer converts at ingest. Document conversion rules

### Data Tagging Plan

- [ ] **Unity Catalog Tags** — tag tables and columns with metadata in Databricks UC: `source_system`, `pii_level`, `freshness_sla`, `domain` (health/fitness/clinical/nutrition)
- [ ] **Column-level PII tagging** — tag columns with `pii=true` for: name, date of birth, GPS coordinates, genetic markers, diagnosis codes. Foundation for GDPR masking
- [ ] **Domain tagging** — group tables into domains: `vitals`, `sleep`, `activity`, `nutrition`, `clinical`, `genetic`. Used for governance reporting and Genie Space context
- [ ] **Databricks `manage_uc_tags` MCP tool** — implement tagging via MCP: `mcp__databricks__manage_uc_tags` on all existing tables
- [ ] **Tag conventions** — document all approved tags and values in `docs/tagging_conventions.md`. Avoid free-text tags

### Data Quality Framework

- [ ] **`gold.data_quality_metrics`** — daily DQ table: `table_name`, `column_name`, `null_rate`, `distinct_count`, `min_value`, `max_value`, `anomaly_flag`, `checked_at`
- [ ] **DLT Expectations** — `@dlt.expect_or_drop` on the silver layer: heart_rate_bpm BETWEEN 30 AND 250, sleep_score BETWEEN 0 AND 100, weight_kg BETWEEN 20 AND 300
- [ ] **Schema enforcement** — enable Delta schema evolution policies: `mergeSchema=false` in prd, `mergeSchema=true` in dev. Breaking schema changes fail explicitly
- [ ] **Quarantine tables** — `bronze.quarantine_<source>` per source. Rows that fail DQ checks are routed here instead of being dropped. Audit trail of what went wrong
- [ ] **Freshness SLA** — see "Data SLA" section below — directly tied to governance

### Best Practices — PoC Documentation

- [ ] **`docs/governance.md`** — consolidated governance document: ownership, classification, retention, access control, GDPR procedure
- [ ] **`docs/metric_dictionary.md`** — all canonical metric names, units, calculation rules, source definitions
- [ ] **`docs/data_lineage.md`** — Mermaid diagram of data flow from source to gold
- [ ] **`docs/tagging_conventions.md`** — approved tags, values, and usage
- [ ] **`docs/access_control.md`** — GRANT matrix: who has access to what

---

## Databricks Framework — Enterprise Scale-Up

Inspired by [yasarkocyigit/daq-databricks-dab](https://github.com/yasarkocyigit/daq-databricks-dab) as a PoC reference for enterprise-grade Databricks architecture.

- [ ] **DLT data quality expectations** — add `@dlt.expect` / `@dlt.expect_or_drop` on silver entities. Provides automatic row-level quality metrics in the Databricks UI without extra code.
- [ ] **Quarantine tables** — route failed rows to `bronze.quarantine_<source>` instead of dropping them. Requires one additional `WHEN NOT MATCHED BY SOURCE` arm in MERGE or `expect_or_quarantine` in DLT.
- [ ] **DLT pipeline monitoring job** — separate DAB job that runs after silver/gold and validates: row count, freshness (`max(_ingested_at) < NOW() - INTERVAL 25 HOURS`), and null rate on key columns. Sends alert if threshold is exceeded.
- [ ] **Gold `depends_on` ordering** — add explicit `depends_on: [silver_pipeline]` in gold DAB job config. Ensures correct ordering and avoids stale gold views on silver failures.

### Tier 5 — Wild ideas. No limits.

- [ ] **Digital twin** — a simulation model of yourself. Feed in: "what happens to my readiness if I sleep 6 hours for 3 nights in a row + train heavy?" The model simulates the answer based on your own historical patterns. Nobody else has this.

- [ ] **Biological age calculator** — combine HRV, resting heart rate, sleep quality, VO2 max, BMI, and activity level into one number: your biological age. "You are 38 years old, but your body is 31." Updated daily. Based on peer-reviewed biomarker research.

- [ ] **AI health coach with full context** — LLM with access to *all* your health data + annotations. Answers: "Why did I sleep poorly in January?" and "What is the biggest difference between my best and worst weeks?" Not generic advice — personalized to your own dataset.

- [ ] **Pre-symptom illness detection** — HRV, skin temperature, and SpO2 typically drop 1-2 days before you feel sick. Train a model on your own data that flags: "something is coming." Oura already does this partially — your version is transparent and explainable.

- [ ] **Environmental correlation engine** — automatic cross-reference with weather API, air quality API, and pollen data. "Your sleep is 18% worse on days with high pollen count." Pure data engineering task, zero ML.

- [ ] **Health-aware calendar** — integrate with Google Calendar. Analyze your energy and focus patterns (when is your HRV highest? when do you sleep best?) and suggest automatically: "Schedule your most important meetings Tuesday-Thursday 9-11. Avoid deep work Monday morning."

- [ ] **Real-time health streaming** — Kafka + Spark Structured Streaming pipeline. Oura sends events in near-real-time. Instead of batch ingest once a day: continuous stream into Delta. Foundation for live dashboard and instant anomaly alerts.

- [ ] **Corporate wellness platform** — same architecture, `user_id` dimension, multi-tenant deployment. Sell to companies as an anonymized wellness dashboard: "Your team's average sleep score has dropped 12% since Q3." GDPR-compliant by design because everything is aggregated and opt-in.

- [ ] **Federated health intelligence** — train ML models across users *without* sharing raw data. Only model gradients are synchronized. Federated learning means your data never leaves your device — but you benefit from 1000 users' patterns. Groundbreaking in health data.

- [ ] **Genomics integration** — connect 23andMe or AncestryDNA to the platform. Cross-reference genetic markers with your wearable data. "You have an APOE variant that increases sleep need — and your data confirms it: you perform 22% better with 8+ hours of sleep."

- [ ] **Health data passport** — cryptographically verifiable credential (W3C Verifiable Credential standard). You can prove to a doctor, insurance company, or employer: "my average resting heart rate is 52 bpm, verified by my own data system" — without sharing raw data. Self-determination over your own data.

- [ ] **Voice health analysis** — record 30 seconds of speech every morning. Analyze acoustic features (pitch variation, speech rate, pause patterns) that correlate with stress, fatigue, and depression. No invasive sensor — just your phone's microphone.

## Tools to install

- [ ] Google Antigravity — `antigravity.google/download` -> Download for Apple Silicon
- [ ] ~~GSD (Get Shit Done)~~ — **PAUSE** (2026-02-26). Crypto token attached, recommends `--dangerously-skip-permissions`, global npm install. Re-evaluate May 2026 — if it is still active and clean, we can look at it.
- [ ] Figma — create free Starter profile
- [ ] Apify — activate as Claude Connector
- [ ] Wispr Flow — evaluate for voice-to-text

## Mac Mini setup

- [ ] Disable sleep — never turn off
- [ ] Enable auto-restart after power failure
- [ ] SSH access
- [ ] Claude Desktop always running (required for Cowork scheduled tasks)

## Claude Cowork — Scheduled Jobs

Point Cowork to `~/builder-automation/`

| Time | Job |
|-----|-----|
| 06:00 | Apify scraper content from Reddit, LinkedIn, YouTube, Twitter/X -> Notion |
| 07:00 | Claude generates LinkedIn drafts from scraped content -> Notion |
| 07:00 Monday | Competitor research: scrape Gumroad, Etsy, Lemon Squeezy -> Notion |
| 08:00 | Post approved drafts to LinkedIn, Reddit, Twitter/X, GitHub |
| 22:00 | Collect metrics: engagement, followers, sales -> Notion dashboard |
| On-demand | Translate products to Spanish, German, French, Chinese |

Set up via `/schedule` in Cowork.

## GitHub repos and profiles for inspiration

- [ ] **[databrickslabs](https://github.com/databrickslabs)** — Databricks Labs org: accelerators, tools, and PoC patterns. Review repos relevant to medallion, CI/CD, and data quality.
- [ ] **[databricks-solutions/ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit)** — Official Databricks AI dev kit. Evaluate whether patterns from here can elevate PoC quality.
- [ ] **[great-expectations](https://github.com/great-expectations)** — Data quality and validation. Natural next step after dbt tests — evaluate as an enterprise-grade DQ layer on top of silver.
- [ ] **[mauran/API-Danmark](https://github.com/mauran/API-Danmark)** — Collection of Danish public APIs. Can be used as a data source or inspiration for the connector pattern in the platform.

## Repo work

- [x] Full repo review for optimization potential — see Optimization section
- [ ] Review best practices and see the .md file — use it
- [ ] Start plan mode and work from todo
- [ ] Run GSD `/gsd:discuss` on the repo

## Optimization — Quick Wins (< 30 min)

Findings from repo review 2026-02-26.

- [ ] **P0 — Structured logging** — replace all `print()` with the `logging` module in `ingestion_engine.py`. Provides traceable logs on Databricks and local debugging.
- [ ] **P0 — Path validation** — add `if not input_path.exists(): sys.exit(...)` in `csv_to_parquet.py` before `pd.read_csv()`. Fails with a clear message instead of cryptic pandas errors.
- [ ] **P1 — Consistent path resolution** — `ingestion_engine.py` uses a hardcoded relative path; `run_merge.py` uses `Path(__file__).resolve().parents[4]`. Consolidate into a shared `get_config_path()` utility.
- [ ] **P1 — YAML schema validation** — add `jsonschema` validation of `sources_config.yaml` and `environment_config.yaml` at load. Runtime errors become parse errors with precise messages.

## Optimization — Larger Tasks (> 30 min)

- [ ] **P2 — dbt schema tests** — add `tests:` blocks in silver `schema.yml` with not-null, unique, and range validation per silver entity. Requires ~5-10 tests per table x 17 tables.
- [ ] **P2 — Persistent state tracking** — create a `bronze._state_tracking` table with `last_fetched_date` per source/endpoint. Eliminates the hardcoded `DEFAULT_LOOKBACK_DAYS=90` and makes incremental load auditable.
- [ ] **P2 — Merge scripts refactor** — 17 merge scripts likely have duplicated MERGE INTO logic. Refactor into a single parameterized template in the style of `silver_runner.py`.
- [ ] **P3 — End-to-end integration tests** — pytest suite that runs the full medallion pipeline: bronze ingestion -> silver merge -> row count validation + schema evolution test.
- [ ] **P4 — Delta Live Tables migration** — refactor Jobs to DLT with `@dlt.expect` for automatic retry, schema versioning, and data quality SLAs. Enterprise/PoC milestone.

## Claude Code subagents

- ~~Explorer — scan codebase for snippets and dependencies~~ — built-in, documented in CLAUDE.md
- ~~Researcher — fetch external docs (Synapse, Databricks)~~ — built-in, documented in CLAUDE.md
- ~~Historian — context/memory across sessions~~ — built-in via memory system

## Figma

- [ ] Create profile
- [ ] Test Claude Code -> Figma MCP flow
- [ ] Use for design collaboration with collaborator on HR products

## Learning & Certification

### Free badges (quick wins — take them now)

- [ ] **Lakehouse Fundamentals** — 4 short videos + quiz -> LinkedIn badge. [databricks.com/learn/training](https://www.databricks.com/learn/training/home)
- [ ] **Generative AI Fundamentals** — 4 short videos -> LinkedIn badge
- [ ] **AI Agent Fundamentals** — LinkedIn badge
- [ ] **AWS Platform Architect accreditation** — free assessment -> badge
- [ ] **Platform Administrator accreditation** — free learning pathway + assessment

### Paid certifications (~$200 each)

- [ ] **Data Engineer Associate** — directly relevant, first priority. Prepare with free training + Databricks Academy.
- [ ] **Data Engineer Professional** — after Associate is passed.

### Events

- [ ] **Data + AI Summit 2026** — June 15-18, San Francisco. Put in calendar. Consider whether it is worth attending (community edition tickets / remote stream).

---

## Content ideas

- "How I use Claude Code subagents as a data engineer"
- "My Mac Mini runs 24/7 with Claude Cowork"
- Claude Code + GSD workflow documentation
- Build in public about the entire setup
- "How I set up Databricks Genie in Teams in 30 minutes" (LinkedIn)
- "3 OAuth patterns for embedding Genie in your apps" (technical authority)
- "Why your data team should stop answering ad-hoc questions" (problem-aware)
- Screen recording: Genie + Copilot Studio -> Teams setup (YouTube/TikTok)

## Dropped

- ~~OpenClaw~~ — replaced by Claude Cowork + Apify connector
- ~~Ollama locally~~ — quality is too low for agentic workflows, use Gemini 3 Pro free in Antigravity

## Quality & Testing

- [ ] **dbt tests** — add `schema.yml` with not-null, unique, accepted-values tests per silver entity
- [ ] **Freshness checks** — validate last ingested timestamp per source (bronze -> silver lag)
- [ ] **Row count reconciliation** — bronze vs silver row counts after each merge run

## Cleanup

- [ ] **`health_environment/deployment/databricks/`** — catalog/schema DDL scripts (`create_catalog__health_dw.sql`, `create_schemas__health_dw.sql`) may be superseded by `init.py` — consider archiving

## New data sources — iCloud health data

Source: `/Users/Shared/sundhedsdata`

- [ ] **Blood test (PDF)** — parse PDF report to structured rows -> `silver.blood_test`. Fields: marker, value, unit, reference range, date. Connector: Python PDF parser (pdfplumber).
- [ ] **23andMe genetic data (CSV + JSON + PDF)** — import raw genotype CSV + health report JSON -> `silver.genetic_ancestry`, `silver.genetic_health_risk`. Keep source file names as `source_file` metadata.
- [ ] **Microbiome (PDF)** — parse microbiome report -> `silver.microbiome_snapshot`. Fields: bacteria group, percentage, reference range, date.

All three follow the standard medallion pattern: raw file -> bronze parquet -> silver merge via YAML config.

## Silver — Date and Time Dimensions

Port from `archive/legacy_databricks_pipeline/silver/` — preserve all naming.

- [x] **`dim_date`** — ported to `transformation_logic/databricks/silver/dim_date.py`. Runs MERGE INTO `health_dw.silver.dim_date`. ~~`archive/.../notebook/date.py` deleted.~~
- [x] **`dim_time`** — ported to `transformation_logic/databricks/silver/dim_time.py`. Runs MERGE INTO `health_dw.silver.dim_time`. ~~`archive/.../notebook/time.py` deleted.~~
- [ ] **Localization `dim_date`** — `month_name` and `day_name` are currently in English (Spark default). Add Danish locale: `month_name` -> januar/februar/.../december, `day_name` -> mandag/tirsdag/.../sondag. Implementation: either `F.when()` lookup chain or Spark session locale `spark.conf.set("spark.sql.session.timeZone", ...)` + custom mapping dict.
- [ ] **Localization `dim_time`** — `ampm_code` is AM/PM (English). Consider a Danish day period column: `dagsperiode` -> nat (00-06), morgen (06-09), formiddag (09-12), middag (12-14), eftermiddag (14-17), aften (17-22), sen aften (22-00).
- [ ] **YAML config** — add `dim_date` and `dim_time` as entries in `sources_config.yaml` (type: `generated`, not parquet source). Follow the metadata-driven pattern.
- [ ] **Gold joins** — when dim_date and dim_time are in silver, add date/time joins to relevant gold views (heart_rate, sleep, activity).

### Legacy SQL — ported and deleted from archive

All files are ported as `.py` Databricks notebooks and archive copies are deleted.

| File | Ported to (SQL) | Archive deleted |
|-----|-----------------|-----------------|
| `blood_oxygen` | `silver/sql/blood_oxygen.sql` | [x] |
| `blood_pressure` | `silver/sql/blood_pressure.sql` | [x] |
| `daily_activity` | `silver/sql/daily_activity.sql` | [x] |
| `daily_annotations` | `silver/sql/daily_annotations.sql` | [x] |
| `daily_meal` | `silver/sql/daily_meal.sql` | [x] |
| `daily_readiness` | `silver/sql/daily_readiness.sql` | [x] |
| `daily_sleep` | `silver/sql/daily_sleep.sql` | [x] |
| `heart_rate` | `silver/sql/oura_heart_rate.sql` (renamed — `heart_rate.sql` is YAML template) | [x] |
| `weight` | `silver/sql/weight.sql` | [x] |

**Gold views** ported:
- [x] `vw_daily_annotations.sql` -> `gold/sql/vw_daily_annotations.sql`
- [x] `vw_heart_rate_avg_per_day.sql` -> `gold/sql/vw_heart_rate_avg_per_day.sql`

### Silver notebooks — cleanup and migration to SQL + dbt

The ported notebooks are currently `.py` files with `# MAGIC %sql` blocks (Databricks notebook format). This is an intermediate solution — the goal is pure SQL files:

- [x] **Clean up Python wrappers** — converted all 9 silver + 2 gold `.py` magic notebook files to pure `.sql` files in `silver/sql/` and `gold/sql/`. `.py` wrappers deleted (PR #19).
- [ ] **dbt version** — write all 9 silver tables as dbt models (`models/silver/*.sql`) with `{{ config(materialized='incremental', unique_key=...) }}`. This is the enterprise pattern and supports both DuckDB (local) and Databricks (cloud). dbt handles MERGE/INSERT OVERWRITE automatically via incremental strategy.
- [ ] **Update source references** — all notebooks point to `workspace.default.*` (legacy Databricks workspace). Update to `health_dw.bronze.stg_*` when the bronze layer is ready.
- [ ] **Choose long-term strategy** — decide whether the silver layer should run as (a) pure Databricks SQL notebooks, (b) dbt-core with dbt-databricks adapter, or (c) both in parallel (dbt locally on DuckDB + Databricks notebooks in cloud). Document the decision in `docs/architecture.md`.

## Scheduled Databricks Jobs — Fully Automated Data Load

Goal: source -> report without manual intervention. All jobs run on Databricks with DAB job configs.

| Job | Frequency | Source | Output |
|-----|----------|-------|--------|
| Oura ingest | Daily 03:00 UTC | Oura REST API | `bronze.stg_oura_*` -> `silver.*` |
| Withings ingest | Daily 03:30 UTC | Withings REST API | `bronze.stg_withings_*` -> `silver.*` |
| Strava ingest | Daily 03:45 UTC | Strava REST API | `bronze.stg_strava_*` -> `silver.*` |
| Apple Health ingest | Weekly Mon 04:00 UTC | Zip -> XML -> parquet | `bronze.stg_apple_*` -> `silver.*` |
| Lifesum ingest | Daily 04:00 UTC | Lifesum scraper/agent | `bronze.stg_lifesum_*` -> `silver.*` |
| Min Sundhed ingest | Weekly Mon 04:30 UTC | FHIR API / scraper | `bronze.stg_minsundhed_*` -> `silver.*` |
| Gold refresh | Daily 05:00 UTC | Silver -> Gold | All gold views/tables |
| SLA monitor | Daily 06:00 UTC | Gold layer | Alert if SLA is breached |

- [ ] **Oura daily job** — DAB job: call Oura REST API -> parquet -> bronze -> silver. Use existing `ingestion_engine.py` + `silver_runner.py`.
- [ ] **Withings daily job** — DAB job: call Withings REST API -> parquet -> bronze -> silver.
- [ ] **Strava daily job** — DAB job: call Strava REST API -> parquet -> bronze -> silver.
- [ ] **Apple Health weekly job** — DAB job: trigger zip flow (see below) -> XML -> parquet -> bronze -> silver. Run on Monday.
- [ ] **Lifesum daily job** — DAB job: call Lifesum scraper/agent (see below) -> parquet -> bronze -> silver.
- [ ] **Min Sundhed weekly job** — DAB job: call FHIR API / scraper -> parquet -> bronze -> silver. Run on Monday.
- [ ] **Gold refresh job** — DAB job: run all gold SQL transforms after silver. `depends_on: [silver_pipeline]`.
- [ ] **SLA monitor job** — DAB job: validate freshness + row count per source. Alert via webhook/email if SLA is breached.
- [ ] **DAB job configs** — define all of the above in `databricks.yml`. Dev workspace runs the same jobs with the `--dev` flag.

## Apple Health Auto-Ingest — Zip -> XML -> Parquet

Automatic flow from iCloud export to bronze parquet without manual intervention.

- [ ] **Zip detection and moving** — Python script that monitors (`/Users/Shared/sundhedsdata/`) for a new `export.zip`. Copies to Mac Mini local working directory (`/Users/Shared/data_lake/apple_health/raw/`).
- [ ] **Automatic unzip** — script unzips to `~/health_data/apple_health/unzipped/YYYY-MM-DD/`. Preserves date versioning so old exports are not overwritten.
- [ ] **XML -> Parquet** — calls existing `xml_to_parquet.py` on the unpacked `export.xml`. Output: parquet files per type in `~/health_data/apple_health/parquet/`.
- [ ] **End-to-end wrapper** — single Python orchestrator that chains: zip detection -> moving -> unzip -> XML parsing -> bronze ingestion. Triggered by Databricks weekly job via SSH or local launchd.
- [ ] **Launchd plist (Mac Mini)** — local `launchd` job that looks for a new zip weekly and runs the wrapper automatically. Backup trigger independent of Databricks.

## Lifesum Connector — Daily Food Data

Automatic download of weekly report from Lifesum. Run daily — always fresh food data for the last 7 days.

- [ ] **Lifesum login agent** — Python script that logs into Lifesum with credentials from `.env` (never hardcoded). Use Playwright for headless browser login -> navigate to export page -> download CSV/JSON for the last 7 days.
- [ ] **Fallback: Lifesum API/GDPR export** — investigate whether Lifesum offers an official export API or GDPR data export endpoint that can be automated without a headless browser.
- [ ] **Bronze ingest** — save downloaded report as parquet -> `bronze.stg_lifesum_food_log`. Fields: date, meal, food item, calories, protein, carbohydrate, fat, fiber.
- [ ] **Silver transform** — `silver.lifesum_food_log` with standardized columns and `_ingested_at`. YAML config + `silver_runner.py`.
- [ ] **Credentials** — Lifesum email + password in `.env` (local) and Databricks secrets (prd). Document in `.env.example`.

## Web App — Mobile & Browser Dashboard

Accessible from iPhone and desktop browser. Shows gold layer data live.

- [ ] **Framework** — Streamlit MVP (fastest, runs on Mac Mini) -> exposed via Cloudflare Tunnel. Next iteration: React/Next.js for better mobile UX.
- [ ] **Streamlit MVP** — reads from DuckDB (dev) or Health API (prd). Run on Mac Mini port 8501.
- [ ] **Mobile access** — expose via Cloudflare Tunnel (see `Personal health API -> Cloudflare Tunnel`). HTTPS from iPhone Safari without an open router.
- [ ] **Authentication** — Cloudflare Access (free) as a login gate in front of the app.
- [ ] **Dashboard pages**:
  - [ ] **Overview** — composite health score, today's metrics, SLA status (green/red per source)
  - [ ] **Sleep** — sleep score, sleep phases, HRV trend
  - [ ] **Activity** — steps, active calories, workouts
  - [ ] **Nutrition** — calories, macros (Lifesum), trends
  - [ ] **Vitals** — resting heart rate, blood oxygen, weight (Withings)
  - [ ] **Anomalies** — flag unusual measurements
- [ ] **Responsive layout** — use `st.columns()` that works on mobile. Test on iPhone Safari.

## Data SLA — Freshness & Quality Guarantees

SLA defined per data source. Monitoring job runs daily at 06:00 UTC and writes to `gold.data_sla_status`.

| Source | Freshness SLA | Min. rows | Alert |
|-------|--------------|-------------|-------|
| Oura | < 25 hours | > 0 / day | Email + webhook |
| Withings | < 25 hours | > 0 / day | Email + webhook |
| Strava | < 25 hours | > 0 / day | Email + webhook |
| Apple Health | < 8 days | > 100 / week | Email |
| Lifesum | < 25 hours | > 0 / day | Email + webhook |
| Min Sundhed | < 8 days | > 0 / week | Email |

- [ ] **`gold.data_sla_status`** — table with columns: `source_system`, `last_ingested_at`, `hours_since_last_ingest`, `sla_hours`, `sla_met`, `actual_rows`, `min_rows_sla`, `rows_sla_met`, `checked_at`.
- [ ] **SLA monitor notebook** — populates `gold.data_sla_status` and sends alert if `sla_met = false`. Use Databricks notification destinations (email / Slack webhook).
- [ ] **SLA widget in web app** — top section in dashboard: green/red status per source with timestamp for last updated.
- [ ] **Quarantine tracking** — include number of rows in `bronze.quarantine_*` per source in the SLA report. High quarantine rate = data quality problem.
- [ ] **Alert channel** — configure Databricks notification destination + Slack/email webhook in `.env`.

## Full Data Model — All Sources

Document the complete schema for all bronze/silver/gold tables across all sources. Foundation for connector development and gold views.

- [ ] **Source overview** — map all sources and their raw attributes. Template per source: `source_system`, `endpoints`, `raw fields`, `update frequency`, `key for deduplication`.
- [ ] **Bronze schema** — document all `stg_*` tables: columns, types, partitioning, `_ingested_at`. One table per endpoint/file type.
- [ ] **Silver schema** — document all standardized silver tables: cleaned types, renamed columns, `sk_` keys, `_source_system`, `_valid_from`. Include `dim_date` and `dim_time` relationships.
- [ ] **Gold schema** — document all gold views and fact/dim tables. Which silver tables are joined? Which metrics are calculated?
- [ ] **ERD (Entity Relationship Diagram)** — draw relationships: `dim_date` <- `fact_*`, `user_id` across. Save as `docs/data_model.md` + optional Mermaid diagram.
- [ ] **Source table**:

| Source | Frequency | Bronze tables | Silver tables |
|-------|----------|-----------------|-----------------|
| Oura | Daily | `stg_oura_sleep`, `stg_oura_readiness`, `stg_oura_activity`, `stg_oura_heart_rate`, `stg_oura_spo2`, `stg_oura_stress`, `stg_oura_tags` | `oura_sleep`, `oura_readiness`, `oura_activity`, `oura_heart_rate`, `oura_spo2` |
| Withings | Daily | `stg_withings_measurements`, `stg_withings_sleep`, `stg_withings_activity` | `withings_body_composition`, `withings_blood_pressure`, `withings_sleep`, `withings_activity` |
| Apple Health | Weekly | `stg_apple_*` (per XML type) | `apple_heart_rate`, `apple_steps`, `apple_workouts`, `apple_sleep`, `apple_hrv` |
| Lifesum | Daily | `stg_lifesum_food_log` | `lifesum_food_log` |
| Strava | Daily | `stg_strava_activities`, `stg_strava_streams` | `strava_activities`, `strava_hr_zones` |
| Min Sundhed | Weekly | `stg_minsundhed_observations`, `stg_minsundhed_conditions`, `stg_minsundhed_medications` | `minsundhed_lab_results`, `minsundhed_diagnoses`, `minsundhed_medications` |
| Blood test PDF | Manual | `stg_blood_test_pdf` | `blood_test` |
| 23andMe | One-time | `stg_23andme_genotype` | `genetic_ancestry`, `genetic_health_risk` |
| Microbiome PDF | Manual | `stg_microbiome_pdf` | `microbiome_snapshot` |
| Generated | — | — | `dim_date`, `dim_time` |

## Withings API Connector

Withings Health Mate API (OAuth2). Data: body composition, blood pressure, sleep, activity.

- [ ] **OAuth2 flow** — register app at developer.withings.com. Client ID/secret in `.env`. Authorization code flow with refresh token. Store tokens in `~/.config/health_reporting/withings_tokens.json`.
- [ ] **Endpoints**:
  - `/measure?action=getmeas` — weight, BMI, body fat %, muscle mass, bone mass, hydration, blood pressure, heart rate
  - `/v2/sleep?action=get` — sleep phases (light, deep, REM), sleep score, snoring, apnea flag
  - `/v2/activity?action=getactivity` — steps, calories, distance, active minutes, elevation
- [ ] **Bronze ingest** — `stg_withings_measurements`, `stg_withings_sleep`, `stg_withings_activity`. Partitioning: `year/month/day`. Fields from API preserved raw + `_ingested_at`.
- [ ] **Silver transforms**:
  - `silver.withings_body_composition` — weight, body fat %, muscle mass, BMI, hydration
  - `silver.withings_blood_pressure` — systolic, diastolic, heart rate, date/time
  - `silver.withings_sleep` — sleep start, sleep end, phases, score
  - `silver.withings_activity` — date, steps, calories, distance, active_min
- [ ] **YAML config** — add all Withings endpoints in `sources_config.yaml`. Follow the Oura pattern.
- [ ] **Credentials** — `WITHINGS_CLIENT_ID`, `WITHINGS_CLIENT_SECRET`, `WITHINGS_ACCESS_TOKEN`, `WITHINGS_REFRESH_TOKEN` in `.env.example`.

## Strava API Connector

Strava REST API (OAuth2). Data: activities (running, cycling, rowing, etc.) with detailed metrics.

- [ ] **OAuth2 flow** — register app at strava.com/settings/api. Scope: `activity:read_all`. Client ID/secret in `.env`. Refresh token flow. Store tokens in `~/.config/health_reporting/strava_tokens.json`.
- [ ] **Endpoints**:
  - `GET /athlete/activities` — list of activities: type, date, distance, duration, elevation, avg/max HR, calories
  - `GET /activities/{id}/streams` — detailed time series: HR, pace, power, cadence, altitude, lat/lon per second
  - `GET /athlete/zones` — HR zones defined by the athlete
- [ ] **Rate limits** — 100 req/15 min, 1000 req/day. Implement exponential backoff + request queue.
- [ ] **Bronze ingest** — `stg_strava_activities` (one row per activity), `stg_strava_streams` (time series). Partitioning: `year/month/day`.
- [ ] **Silver transforms**:
  - `silver.strava_activities` — activity type, date, distance_km, duration_sec, elevation_m, avg_hr, max_hr, calories, avg_pace
  - `silver.strava_hr_zones` — distribution of time in HR zone 1-5 per activity
- [ ] **YAML config** — add Strava endpoints in `sources_config.yaml`.
- [ ] **Credentials** — `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET`, `STRAVA_ACCESS_TOKEN`, `STRAVA_REFRESH_TOKEN` in `.env.example`.

## Min Sundhed Connector — sundhed.dk

Clinical data from the Danish national health register via sundhed.dk. Login requires MitID.

- [ ] **Access method — investigate**:
  - **Option A: Sundhedsdatastyrelsen FHIR R4 API** — official API with NemID/MitID OAuth. Endpoints: `/Patient`, `/Observation` (lab), `/Condition` (diagnoses), `/MedicationRequest`. Requires application for access.
  - **Option B: Playwright scraper** — headless MitID login on sundhed.dk -> navigate to "My Data" -> download PDF/CSV export. Complex due to MitID flow.
  - **Option C: FHIR export** — sundhed.dk offers manual FHIR export (JSON). Automate download via Playwright post-login.
  - Recommended: start with Option C (manual), build toward Option A (automatic).
- [ ] **Bronze ingest** — `stg_minsundhed_observations` (lab results), `stg_minsundhed_conditions` (diagnoses), `stg_minsundhed_medications` (medications), `stg_minsundhed_vaccinations`.
- [ ] **Silver transforms**:
  - `silver.minsundhed_lab_results` — marker (LOINC/local code), value, unit, reference interval, date, laboratory
  - `silver.minsundhed_diagnoses` — ICD10 code, description, date, treatment location
  - `silver.minsundhed_medications` — ATC code, preparation, dosage, start date, status
- [ ] **Join with blood test PDF** — `silver.blood_test` (PDF parser) + `silver.minsundhed_lab_results` can be joined on marker + date for double validation.
- [ ] **Credentials** — MitID credentials never in code. Store encrypted in macOS Keychain. Access via the `keyring` library.
