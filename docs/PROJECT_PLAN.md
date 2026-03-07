# PROJECT_PLAN.md — HealthReporting

> Last updated: 2026-03-07 (Audit cleanup)
> Current phase: **Phase 3b — AI-Native Data Model (COMPLETE) + Phase 5 (Databricks, parallel) + Phase 7 (AI Governance, active) + Roadmap Iterations A1 ✅, C1/A5 ✅ — next: A2 Data Quality Shield**

---

## Phase Overview

| Phase | Name | Status | Target |
|-------|------|--------|--------|
| 0 | Governance & Project Setup | ✅ done | Feb 2026 |
| 1 | Foundation & Bronze Layer | ✅ done | Feb 2026 |
| 2 | Silver Layer — Core Transformations | 🔵 in progress | Mar 2026 |
| 3 | Gold Layer — Reporting Entities | 🔵 cloud only | Apr 2026 |
| 3b | AI-Native Data Model (Local) | ✅ done | Mar 2026 |
| 4 | Visualization & Reporting | ⬜ not started | Jun 2026 |
| 5 | Cloud Migration (Databricks) | 🔵 in progress | Q2 2026 |
| 6 | CI/CD & Automation | 🔵 in progress | Q2 2026 |
| 7 | AI Governance Framework | 🔵 in progress | Q2 2026 |

---

## Phase 0 — Governance & Project Setup

**Goal:** Establish project structure, governance files, and working conventions.

| Task | Status | Notes |
|------|--------|-------|
| CLAUDE.md session rules | ✅ done | Includes mandatory_session_protocol |
| PROJECT_PLAN.md | ✅ done | This file |
| ARCHITECTURE.md | ✅ done | docs/ARCHITECTURE.md |
| CHANGELOG.md | ✅ done | docs/CHANGELOG.md |
| Claude Code custom commands | ✅ done | /status, /plan-session, /end-session |
| CONTEXT.md | ✅ done | docs/CONTEXT.md |
| Custom agents (12) | ✅ done | code-reviewer, build-validator, etc. |
| AI governance framework | ✅ done | ~/ai-ledelse.md + session protocol |

---

## Phase 1 — Foundation & Bronze Layer

**Goal:** All data sources ingested into bronze with consistent schema and metadata.

| Task | Status | Notes |
|------|--------|-------|
| Apple Health XML → parquet | ✅ done | 15 types via process_health_data.py |
| Oura connector | ✅ done | OAuth 2.0, incremental, 8 endpoints |
| Lifesum connector | ✅ done | csv_to_parquet.py |
| Withings connector | ⬜ not started | API-based — planned |
| Strava connector | ⬜ not started | API-based — planned |
| GetTested connector | ⬜ not started | Manual/export — planned |
| sources_config.yaml — full schema | ✅ done | 27 sources defined (19 Apple Health + 8 Oura + 1 Lifesum) |
| Ingestion engine — all active sources | ✅ done | Works for Apple Health, Oura, Lifesum |
| Bronze validation tests | ⬜ not started | Row counts, null checks |

**Exit criteria:** All 3 active sources ingested into bronze, validated, repeatable. *(Withings/Strava/GetTested are stretch goals.)*

---

## Phase 2 — Silver Layer — Core Transformations

**Goal:** Cleaned, typed, deduplicated entities for all active sources.

| Task | Status | Notes |
|------|--------|-------|
| Silver schema design (all entities) | ✅ done | 21 dbt models |
| Apple Health silver transforms | ✅ done | 9 entities: heart_rate, step_count, toothbrushing, body_temperature, respiratory_rate, water_intake, daily_energy_by_source, daily_walking_gait, mindful_session |
| Oura silver transforms | ✅ done | 9 entities: daily_sleep, daily_activity, daily_readiness, heart_rate (shared), workout, daily_spo2, daily_stress, personal_info |
| Lifesum silver transforms | ✅ done | daily_meal |
| Withings silver transforms | ✅ done | blood_pressure + weight merge scripts merged |
| DuckDB local silver runner | ✅ done | run_merge.py — 21 merge scripts working locally |
| Silver validation tests | ⬜ not started | TDD framework — pytest + conftest.py |
| dbt schema tests on all 17 entities | ⬜ not started | not-null, unique, accepted-values |

**Exit criteria:** All active source entities in silver, validated, repeatable locally.

---

## Phase 3 — Gold Layer — Reporting Entities

**Goal:** Aggregated, cross-source entities ready for reporting.

**Note:** Gold layer is now **cloud-only** (Databricks). Locally, Gold is replaced by the AI-Native Data Model (Phase 3b). See ADR-005.

| Task | Status | Notes |
|------|--------|-------|
| Gold entity design (Databricks) | 🔵 partial | 3 views exist (daily_heart_rate_summary, vw_daily_annotations, vw_heart_rate_avg_per_day) |
| Cross-source joins (sleep + activity + nutrition) | ⬜ not started | Databricks Gold only |
| Composite health score | ✅ done locally | YAML recipe in `_business_rules.yml`; Databricks Gold TBD |
| Biomarker tracking views | ⬜ not started | |
| Gold validation tests | ⬜ not started | |

**Exit criteria:** Reporting-ready views covering all key health dimensions (cloud) + Semantic Contracts (local).

---

## Phase 3b — AI-Native Data Model (Local Stack)

**Goal:** Replace local Gold layer with AI-native 2+2 architecture: Agent Memory + Semantic Contracts + MCP tools. See ADR-005.

| Task | Status | Notes |
|------|--------|-------|
| M0: Agent schema DDL (5 tables + metric_relationships) | ✅ done | `setup/create_agent_schema.sql` |
| M0: COMMENT ON all 21 silver tables (~269 column descriptions) | ✅ done | `setup/add_column_comments.sql` |
| M0: Initial YAML contracts (expanded to 18 in M4) | ✅ done | `contracts/metrics/` |
| M1: text_generator.py (template-based daily summaries) | ✅ done | `ai/text_generator.py` (373 lines) |
| M1: baseline_computer.py (6 baselines + demographics) | ✅ done | `ai/baseline_computer.py` (337 lines) |
| M1: Health knowledge graph (67 nodes, 108 edges) | ✅ done | `setup/seed_health_graph.sql` |
| M1: Backfill daily summaries (91 days) | ✅ done | 2025-11-21 to 2026-02-19 |
| M2: embedding_engine.py (sentence-transformers) | ✅ done | `ai/embedding_engine.py` (277 lines) |
| M2: DuckDB VSS + HNSW indexes | ✅ done | Experimental persistence enabled |
| M2: Backfill embeddings (91 summaries) | ✅ done | all-MiniLM-L6-v2, 384-dim |
| M3: MCP server (8 tools) | ✅ done | `mcp/server.py` (168 lines) |
| M3: health_tools.py (tool implementations) | ✅ done | `mcp/health_tools.py` (621 lines) |
| M3: query_builder.py (YAML → SQL) | ✅ done | `mcp/query_builder.py` (247 lines) |
| M3: formatter.py (markdown output) | ✅ done | `mcp/formatter.py` (205 lines) |
| M3: schema_pruner.py (category-based pruning) | ✅ done | `mcp/schema_pruner.py` (235 lines) |
| M4: 18 metric YAML contracts | ✅ done | `contracts/metrics/*.yml` (~1,500 lines) |
| M4: _index.yml + _business_rules.yml | ✅ done | Master index + composite score + alerts |
| M5: ingestion_engine.py post-merge trigger | ✅ done | Auto-generates daily summary after ingest |
| M5: .mcp.json updated (local, gitignored) | ✅ done | Health MCP server config |
| M5: ADR-005 | ✅ done | `docs/adr/ADR-005-ai-native-data-model.md` |
| M5: 55/55 pytest tests green | ✅ done | All phases validated |
| Wire MCP server into Claude Code | ✅ done | .mcp.json configured, 8 tools verified, 55 smoke tests |
| Deprecate local Gold views | ⬜ not started | Keep Databricks Gold |

**Exit criteria:** AI agent queries health data via MCP tools with >80% accuracy. All memory tiers populated. ✅ ACHIEVED.

---

## Phase 4 — Visualization & Reporting

**Goal:** Choose and implement reporting destination.

| Task | Status | Notes |
|------|--------|-------|
| Evaluate options | ⬜ not started | Streamlit, Evidence, Superset, Power BI, Databricks AI/BI |
| Build dashboards | ⬜ not started | |
| Connect to gold layer | ⬜ not started | |
| Databricks Genie Space | ⬜ not started | "What was my best week in January?" |

---

## Phase 5 — Cloud Migration (Databricks)

**Goal:** Full platform running on Databricks with Unity Catalog.

| Task | Status | Notes |
|------|--------|-------|
| Catalog + schemas DDL | ✅ done | health-platform-dev/prd, schemas: bronze, silver, gold, audit |
| Audit framework (job_runs, table_runs) | ✅ done | AuditLogger context manager, Delta tables live |
| Databricks Asset Bundle (DAB) | ✅ done | Bundle deployed to dev, deploy.yml CI/CD |
| Orchestration workflows | ✅ done | bronze_job.yml, silver_job.yml, gold_job.yml |
| Silver SQL (Databricks) | 🔵 partial | 10 SQL files deployed; autoloader not running live data |
| Bronze → Databricks live data | ⬜ not started | Autoloader config needs source path setup |
| Full end-to-end run in cloud | ⬜ not started | |
| DLT Expectations (data quality gates) | ⬜ not started | Bronze → silver quarantine pattern |
| Unity Catalog Tags (pii_level, domain) | ⬜ not started | |

---

## Phase 6 — CI/CD & Automation

**Goal:** Automated testing, deployment, and data refresh.

| Task | Status | Notes |
|------|--------|-------|
| GitHub Actions setup | ✅ done | deploy.yml live |
| Bundle validation on PRs | ✅ done | Runs build-validator on every PR |
| Auto-deploy to dev on push | ✅ done | Feature branches → dev |
| Auto-deploy to prd on merge | ✅ done | Main → prd |
| Automated tests | ⬜ not started | pytest TDD framework |
| Oura daily job (cron) | ✅ done | `scripts/daily_sync.sh` + `com.health.daily-sync.plist` — runs daily at 06:00 |
| Code-reviewer agent as PR gate | ⬜ not started | AI governance experiment |

---

## Phase 7 — AI Governance Framework

**Goal:** Structured control layer for AI agents — ensuring agents follow strategic priorities (TODO.md), not just nearest instruction (PROJECT_PLAN.md). PoC showcase for enterprise enterprise context.

| Task | Status | Notes |
|------|--------|-------|
| /prioritize command (merges TODO + PROJECT_PLAN) | ✅ done | .claude/commands/prioritize.md |
| docs/AI_GOVERNANCE.md (internal governance framework) | ✅ done | PoC version of ~/ai-ledelse.md |
| MEMORY.md cross-session context | ✅ done | ~/.claude/projects/.../memory/MEMORY.md |
| Mandatory session protocol (CLAUDE.md) | ✅ done | on_session_start / during / end |
| Specialised agents (12 agents) | ✅ done | code-reviewer, security-reviewer, build-validator, etc. |
| CI/CD as unenforced gate (GitHub Actions) | ✅ done | deploy.yml — bundle validation + deploy |
| ADRs as agent guardrails (docs/adr/) | ✅ done | 4 ADRs: DuckDB, Medallion, YAML, Feature branch |
| scripts/validate_naming.py (Layer 3, Tier 1) | ✅ done | snake_case + hardcoded path enforcement |
| scripts/governance_check.py (Layer 3, Tier 1) | ✅ done | CHANGELOG + ARCHITECTURE update gate |
| scripts/ai_pr_review.py (Layer 3, Tier 3) | ✅ done | Claude Haiku PR reviewer, PASS/WARN/FAIL verdict |
| .github/workflows/ai-review.yml | ✅ done | GitHub Actions AI PR gate |
| .github/workflows/governance-check.yml | ✅ done | GitHub Actions governance gate |
| .pre-commit-config.yaml (Tier 1, local) | ✅ done | naming + black + ruff + gitleaks |
| docs/decisions/DECISIONS.md (Layer 4) | ✅ done | Session-level decision log |
| docs/COST_LOG.md (Layer 4) | ✅ done | AI cost tracking |
| docs/SPRINT_LOG.md (Layer 2) | ✅ done | Sprint planning and retrospectives |
| AI_GOVERNANCE.md → full 7-layer framework | ✅ done | Rewritten from PoC to complete framework doc |
| Claude Code hooks as enforcement layer | ✅ done | pre_commit_guard.sh (PreToolUse) + post_commit.sh (PostToolUse) in scripts/hooks/ |
| CLAUDE.md: governance_sync block | ✅ done | PR #41 — first action in on_session_start; auto-detects governance file drift before session begins |
| CLAUDE.md: model_routing section | ✅ done | PR #42 — 11-task routing table; agent self-identifies model; flags mismatches; end-of-session routing recommendation |
| CLAUDE.md: security_protocol section | ✅ done | PR #43 — continuous security awareness; 3 scan levels (per-file, per-session, periodic); health data special rules |
| CLAUDE.md: mandatory_task_reporting section | ✅ done | PR #44 — 4-part post-task reporting; cannot be disabled; flags out-of-scope tasks; ports ai-ledelse.md s.6.4 |
| ai-ledelse.md: governance sync observation (s.25) | ✅ done | PR #41 — added to Observationer fra praksis |
| ai-ledelse.md: section 19 Dynamic Model Routing | ✅ done | PR #42 — inserted; sections 19-26 renumbered to 20-27 |
| ai-ledelse.md: section 24 Security som infrastruktur | ✅ done | PR #43 — inserted; sections 24-27 renumbered to 25-28; now 28 sections total |
| ANTHROPIC_API_KEY → GitHub Secrets | ⬜ not started | Manual: `gh secret set ANTHROPIC_API_KEY` — required for AI PR review to function |
| Master agent PoC (supervisor spawning sub-agents) | ⬜ not started | Reads all MD files, acts as architecture guard rail |
| ai-ledelse.md bidirectional sync process | 🔵 in progress | 3 delta-file syncs completed; formal sync protocol defined in AI_GOVERNANCE.md Layer 7 |

**Exit criteria:** `/prioritize` produces a ranked Top 3 that reflects P0 strategic items from TODO.md — not just technical backlog from PROJECT_PLAN.md.

---

## MVP Iteration Roadmap

Per `MASTER_PLAN.md`: `A1 → C1/A5 → A2 → B1 → A3 → B3 → B2 → C2`

| Iteration | Name | Status | Notes |
|-----------|------|--------|-------|
| A1 | MCP Goes Live | ✅ done | MCP server wired into Claude Code, 8 tools, 55 smoke tests |
| C1/A5 | Daily Sync + API + Withings | ✅ done | 6-step sync pipeline, FastAPI server, Withings connectors merged |
| **A2** | Data Quality Shield | ⬜ next | Automated quality checks — nulls, duplicates, stale data, schema drift |
| B1 | LLM-Powered Chat Engine | ⬜ planned | Replace keyword routing with Claude tool-use (function calling) |
| A3 | Smart Alerts | ⬜ planned | Proactive anomaly detection + ntfy.sh notifications |
| B3 | Conversation Memory | ⬜ planned | Multi-turn chat context management |
| B2 | Streaming Responses | ⬜ planned | SSE streaming in FastAPI |
| C2 | Apple Health Auto-Import | ⬜ planned | Automated Apple Health XML ingestion |
