# PROJECT_PLAN.md — HealthReporting

> Last updated: 2026-03-11 (governance sync — aligned with build-logs and codebase scan)
> Current phase: **Phase 3b done + Phase 5 (Databricks) + Phase 7 (Governance). Iterations done: A1-A9, B1-B4, C1 (2/3), C2, D2 (2/5), D4, DS1-DS4, E0, F1-F3. 0 P1 audit findings open. 1334 tests passing, 195 integration tests deselected. Next: C1 S3 Gene-Health / D2 S3 Desktop PDF / P2 audit findings.**

---

## Current State (2026-03-11)

| Area | Count | Status |
|---|---|---|
| **Source connectors** (`run_*.py`) | 5 | Oura, Withings, Strava, sundhed.dk, Weather |
| **Bronze sources** (config entries) | ~59 | Apple Health 35+, Oura 18 API + 7 CSV, Lifesum, Withings, Strava, sundhed.dk |
| **Silver merge scripts** | 59 | All active sources |
| **Silver dbt models** | 51 | Schema models in `_schema.yml` |
| **Gold views (Databricks)** | 21 | Cloud gold layer |
| **Gold views (local)** | 18 | Local gold / Kimball star |
| **Agent schema tables** | 9 | patient_profile, daily_summaries, health_graph, knowledge_base, etc. |
| **MCP tools** | 17 | Full AI tool layer |
| **Metric YAML contracts** | 26 | 24 metric + `_index.yml` + `_business_rules.yml` |
| **ADRs** | 5 | All accepted |
| **CI/CD workflows** | 4 | claude.yml, deploy.yml, ai-review.yml, ci.yml |
| **Desktop app files** | 9 | pywebview — dashboard + chat done |
| **API server files** | 8 | FastAPI, Bearer auth, Tailscale ready |
| **Test files** | 56 | 1334 passing, ~65% coverage |

---

## Phase Overview

| Phase | Name | Status | Target |
|-------|------|--------|--------|
| 0 | Governance & Project Setup | Done | Feb 2026 |
| 1 | Foundation & Bronze Layer | Done | Feb 2026 |
| 2 | Silver Layer — Core Transformations | In progress | Mar 2026 |
| 3 | Gold Layer — Reporting Entities | Cloud only | Apr 2026 |
| 3b | AI-Native Data Model (Local) | Done | Mar 2026 |
| 4 | Visualization & Reporting | Not started | Jun 2026 |
| 5 | Cloud Migration (Databricks) | In progress | Q2 2026 |
| 6 | CI/CD & Automation | In progress | Q2 2026 |
| 7 | AI Governance Framework | In progress | Q2 2026 |

---

## Owner Context

Active pancreatic insufficiency investigation (elastase 105 ug/g) requiring cross-domain correlation between diet, digestion, wearables, microbiome, and genetics.

---

## Vision

The world's most advanced personal health intelligence system for a single human being. Not a dashboard. Not a tracker. An autonomous research infrastructure that continuously observes your body, interprets signals against the latest science, and surfaces actionable insights no clinician could produce manually. Multi-source data fusion, persistent collection, AI-driven pattern recognition, and causal reasoning. All legal, all open source, all yours.

## Principles

1. **Sovereignty first.** Your data never leaves your infrastructure unless you choose to share it.
2. **Evidence over intuition.** Every claim the system makes must trace back to a paper or a dataset.
3. **Transparency over accuracy.** A simple model you understand beats a complex model you trust blindly.
4. **Iterate, don't architect.** Ship iteration 1 before designing iteration 5.
5. **One person, full stack.** Personal infrastructure project that happens to be open source.

---

## Category 1: Integrations

Data source connectors — ingestion from external systems into bronze.

| # | Source | Type | Status | Endpoints | Bronze | Silver | Gold | Notes |
|---|--------|------|--------|-----------|--------|--------|------|-------|
| 1 | Apple Health | XML export | Done | 35+ types | 35+ | 9 | Yes | Via HAE app -> FastAPI |
| 2 | Oura Ring | API + CSV | Done | 18 API + 7 CSV | 25 | 9 | Yes | OAuth2, daily sync |
| 3 | Lifesum | CSV export | Done | 5 types | 5 | 1 | Yes | food, body, exercise |
| 4 | Withings | API | Done | 9 endpoints | 7 | 2 | Partial | sleep/ecg/pwv silver missing |
| 5 | Strava | API | Done | 2 types | 2 | 1 | Yes | Unified with workout |
| 6 | sundhed.dk | Playwright | Done | 5 sections | 5 | 0 | No | MitID 2FA, silver missing |
| 7 | Lab Results | PDF + YAML | Done | Variable | Yes | 1 | Yes | GetTested + manual |
| 8 | 23andMe | PDF/CSV/JSON | Done | Variable | Yes | 1 | No | 3 parsers, 50 SNPs |
| 9 | Weather | API | Configured | 1 | 0 | 0 | No | YAML ready, not built |
| 10 | Garmin | API | Deferred | Unknown | 0 | 0 | No | API access unclear |
| 11 | CGM (Libre) | Apple Health | Future | Via AH | — | — | — | ~500-700 kr |
| 12 | DNA Complete | WGS | Research | 4-5M variants | 0 | 0 | No | 30x coverage |

**Integration gaps (data objects available but not ingested to silver):**
- Withings: sleep_summary, sleep_raw, heart_list, ecg_session, pwv (bronze exists)
- sundhed.dk: lab_results, medications, vaccinations, ejournal, appointments (bronze exists)
- Apple Health: ~20 types in bronze without dedicated silver transforms
- Oura CSV: dailyresilience, cardiovascular_age, temperature, sleepmodel (partially covered)

---

## Category 2: Database Solution

### Local Stack (DuckDB)

AI-Native 2+2 architecture (ADR-005):

| Layer | Schema | Tables | Purpose |
|-------|--------|--------|---------|
| Raw Store | `bronze` | ~59 staging tables | 1:1 with source, append-only parquet |
| Unified Health Store | `silver` | 51 dbt models, 59 merge scripts | Deduplicated, typed, incremental merge |
| Gold (Kimball) | `gold` | 18 local views | Star schema for analytics |
| Agent Memory | `agent` | 9 tables | AI context: profile, summaries, graph, knowledge |
| Genetics | `genetics` | Genetic profile | Static genetic findings |
| Chat | `agent_chat` | Conversation history | Optional |

**Database file:** `/Users/Shared/data_lake/database/health_dw_{dev|prd}.db`

### Cloud Stack (Databricks)

Traditional medallion on Delta Lake with Unity Catalog:

| Layer | Catalog (dev) | Catalog (prd) | Status |
|-------|---------------|---------------|--------|
| Bronze | health-platform-dev | health-platform-prd | Schema exists, autoloader not configured |
| Silver | health-platform-dev | health-platform-prd | 10 SQL files deployed, not running |
| Gold | health-platform-dev | health-platform-prd | 21 gold views deployed |
| Audit | health-platform-dev | health-platform-prd | job_runs + table_runs tables live |

---

## Category 3: Architecture

### Dual-Stack Design

```
LOCAL (Mac Mini M4)                    CLOUD (Databricks)
─────────────────────                  ──────────────────
Raw Parquet files                      Raw Parquet -> Autoloader
        │                                      │
Bronze (DuckDB)                        Bronze (Delta Lake)
        │                                      │
Silver (dbt-duckdb)  <── SHARED ──>   Silver (Delta Lake)
        │                                      │
Gold (Kimball star)                    Gold (Delta Lake)
        │
Agent Memory (embeddings, graph)
        │
Semantic Contracts (YAML)
        │
MCP Server (17 tools)
```

### Architecture Decision Records (ADRs)

| ADR | Decision | Status |
|-----|----------|--------|
| ADR-001 | DuckDB as local runtime, Databricks as cloud target | Accepted |
| ADR-002 | Medallion architecture (Bronze -> Silver -> Gold) | Accepted |
| ADR-003 | YAML-driven source configuration | Accepted |
| ADR-004 | Feature branch workflow + PR-based deployment | Accepted |
| ADR-005 | AI-native 2+2 architecture replacing local Gold | Accepted |

### Draw.io Diagrams (in docs/)

- `c4-level1-context.drawio` — System context
- `c4-level2-containers.drawio` — Container diagram
- `c4-level3-components.drawio` — Component diagram
- `solution-architecture.drawio` — Full solution architecture

---

## Category 4: Cloud (Databricks)

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| Catalog + schemas DDL | Done | — | health-platform-dev/prd |
| Audit framework | Done | — | AuditLogger, Delta tables |
| DAB bundle | Done | — | Deployed to dev |
| Orchestration workflows (DAB) | Done | — | bronze/silver/gold job YAMLs |
| Silver SQL deployed | Partial | — | 10 of 28 SQL files |
| Gold views deployed | Done | — | 21 gold views in cloud |
| Bronze autoloader | Not started | Medium | Needs source path setup |
| Full end-to-end cloud run | Not started | Medium | Blocked by autoloader |
| DLT Expectations | Not started | Low | Data quality gates |
| Unity Catalog Tags | Not started | Low | pii_level, domain |
| AI/BI Dashboards | Not started | Low | Genie Space |
| Multi-user readiness | Not started | Low | user_id isolation |

---

## Category 5: Local Stack & AI

| Component | Status | Details |
|-----------|--------|---------|
| DuckDB runtime | Done | Full pipeline running locally |
| dbt-duckdb | Done | 51 schema models, 59 merge scripts |
| Gold Kimball star | Done | 18 local views |
| Agent Memory | Done | 9 tables: patient_profile, daily_summaries, health_graph, knowledge_base, etc. |
| Embeddings | Done | all-MiniLM-L6-v2, 384-dim, HNSW index |
| MCP Server | Done | 17 tools via stdio |
| Semantic Contracts | Done | 26 YAML files (24 metrics + index + business rules) |
| Daily sync | Done | launchd 06:00, 4-step pipeline |
| FastAPI server | Done | 8 API server files, Bearer auth, Tailscale ready |
| Text generator | Done | Template-based daily summaries |
| Baseline computer | Done | Rolling baselines + demographics |
| Correlation engine | Done | Pearson with lag, 32 pairs |
| Anomaly detection | Done | Z-score, constellation patterns |
| Recommendation engine | Done | CDS/FDA-safe, evidence-backed |
| PubMed evidence | Done | GRADE scoring, 90-day cache |
| Trend forecaster | Done | Linear regression |
| Notification system | Done | ntfy.sh push notifications |

---

## Category 6: Audit & Quality

| Audit | Status | Key Findings |
|-------|--------|--------------|
| Wave 1: Security | Done | 0 Critical, 1 HIGH (mobile.py SQL injection), 9 MEDIUM |
| Wave 2A: Test Coverage | Done | 61.7% baseline, gaps in embedding/recommendation/auth |
| Wave 2B: Test Coverage | Done | +122 tests, coverage -> ~65% |
| Wave 3: Code Quality | Done | Ruff clean, centralized sql_safety, dedup get_db_path |
| Wave 4: Architecture | Done | 77->0 sys.path.insert, structured logging |
| Wave 5: CI/CD | Done | Consolidated pipeline, SHA-pinned actions, Dependabot |

**Remaining P1 security items:** 4 hardening tasks from full audit (2026-03-09)

**Test infrastructure:**
- 56 test files, 1334 tests passing, 0 failures
- 195 integration tests (deselected, pyproject.toml addopts)
- pytest + conftest.py + fixtures
- Coverage target: 75% (currently ~65%)

---

## Category 7: Desktop App

**Technology:** pywebview (pure Python, no JS toolchain)

| Session | Task | Status | Deliverable |
|---------|------|--------|-------------|
| D2 S1 | Foundation | Done | pywebview + DesktopAPI + dashboard + Chart.js |
| D2 S2 | Chat | Done | Claude streaming + chat history + multi-turn |
| D2 S3 | PDF Reports | Todo | weasyprint, date picker, preview, export, da/en |
| D2 S4 | Polish + Tests | Todo | Data Explorer, error handling, keyboard shortcuts |
| D2 S5 | .app Bundle | Todo | py2app -> HealthReporting.app, double-click launch |

**Desktop app files:** 9 files in codebase.

---

## Category 8: Mobile App (Laege i Lommen)

**Technology:** SwiftUI native + direct Claude API

| Component | Status | Notes |
|-----------|--------|-------|
| Architecture design | Done | DS3 session |
| Backend endpoints (/v1/mobile/*) | Done | FastAPI sync/chat/alerts |
| SwiftUI scaffold | Scaffolded | Repo: laege-i-lommen |
| Apple Developer account | Blocked | $99/yr required for TestFlight |
| Full app build | Todo | 5-7 sessions estimated |

---

## Category 9: Missing Data Sources & Objects

### Sources not yet connected

| Source | Type | Priority | Effort | Notes |
|--------|------|----------|--------|-------|
| Garmin Connect | API | Low | M | API access being investigated |
| CGM (FreeStyle Libre) | Device -> Apple Health | Low | S | ~500-700 kr, auto-flows via AH |
| DNA Complete (WGS) | File | Low | L | 30x whole genome, research phase |
| MyFitnessPal | API/export | Not planned | — | Lifesum covers nutrition |
| Fitbit | API | Not planned | — | Oura covers same metrics |
| Blood work services | Various | Not planned | — | GetTested + sundhed.dk covers this |

### Data objects with bronze but no silver

| Source | Object | Priority | Notes |
|--------|--------|----------|-------|
| Withings | sleep_summary | High | Data exists, transform missing |
| Withings | sleep_raw | Medium | Raw sleep data |
| Withings | heart_list | Medium | HR data from Withings |
| Withings | ecg_session | Medium | ECG recordings |
| Withings | pwv | Low | Pulse wave velocity |
| sundhed.dk | lab_results | High | Clinical lab results |
| sundhed.dk | medications | Medium | Prescription history |
| sundhed.dk | vaccinations | Low | Vaccination records |
| sundhed.dk | ejournal | Low | Hospital journal entries |
| sundhed.dk | appointments | Low | Clinical appointments |
| Apple Health | ~20 additional types | Low | Various metrics in bronze |
| Weather | open_meteo | Low | YAML configured, not built |

---

## Category 10: Other / Cross-Cutting

| Topic | Status | Notes |
|-------|--------|-------|
| Clinician Export (FHIR + PDF) | Planned | D3 — for doctor appointments |
| Clinical Coding (SNOMED/LOINC) | Planned | C3 — terminology_mapper.py |
| Ruff format cleanup | Open | 106 files, non-blocking |
| Gene-health mapping | Todo (C1 S3) | 50 SNPs -> metric mapping, risk scores |
| GPS/Location for workouts | Planned (DS5) | PII-sensitive decision |
| Smart Alerts (A3) | Planned | Proactive anomaly -> ntfy.sh |
| Streaming responses (B2) | Planned | SSE in FastAPI |

---

## Rolling Plan (next 2-4 weeks)

| # | Task | Category | Priority | Effort | Notes |
|---|------|----------|----------|--------|-------|
| 1 | C1 S3: Gene-Health Integration | Integrations | High | S | Gene-to-metric mapping, risk scores |
| 2 | D2 S3: Desktop PDF Reports | Desktop App | High | M | weasyprint, date picker, da/en |
| 3 | P1 Security Fixes | Audit | High | S | 4 remaining hardening items |
| 4 | Withings silver transforms | Database | Medium | M | sleep, ecg, pwv, heart_list |
| 5 | sundhed.dk silver transforms | Database | Medium | M | 5 clinical sections |
| 6 | D2 S4: Desktop Polish | Desktop App | Medium | M | Data Explorer, error handling |
| 7 | D2 S5: Desktop .app Bundle | Desktop App | Medium | S | py2app packaging |
| 8 | D1: Mobile App Build | Mobile App | Medium | XL | SwiftUI, needs Apple Dev account |
| 9 | Databricks autoloader | Cloud | Low | M | Source path setup |
| 10 | Test coverage -> 75% | Audit | Low | M | Embedding, recommendation, auth |

---

## Completed Phases

| Phase | Sessions | Date | Highlights |
|-------|----------|------|------------|
| A: Foundation (A1-A9) | 8 | 2026-03-07/08 | 68 bronze sources, 45 silver tables |
| B: Intelligence (B1-B4) | 3 | 2026-03-08 | Claude chat, PubMed, anomaly, 32 correlations |
| C: Clinical (partial) | 2.5 | 2026-03-07/08 | Lab PDF, 23andMe genetics, recommendations |
| D: Apps (partial) | 3 | 2026-03-08 | Desktop S1+S2, Mobile scaffold, notifications |
| E: Cloud (E0 only) | 1 | 2026-03-08 | Gold Kimball: 10 dim + 8 fact |
| F: Tech Debt (F1-F2) | 1.5 | 2026-03-08 | C4 docs, metric dictionary, lineage |
| DS: Design Stories (1-4) | 4 | 2026-03-08 | API expansion, HAE, mobile arch, desktop arch |
| QA: Full Audit (W1-W5) | 5 | 2026-03-09/11 | 1334 tests, ruff clean, CI pipeline |

---

## Wild Ideas (long-term)

- **Digital Twin** — Computational metabolism model, run simulations
- **Federated N-of-1 Trials** — Formalized crossover experiments
- **Real-time Biometric Streaming** — CGM + continuous wearable data
- **AI Clinician Sparring Partner** — Claude with full data access for doctor visit prep
- **Longevity Dashboard** — Composite biological age, track over time
- **Open Source Sovereign Health Protocol** — Package as reproducible framework

---

## Risks & Blockers

| Risk | Impact | Mitigation |
|------|--------|------------|
| Apple Developer account ($99/yr) | Blocks mobile app | Purchase when ready for TestFlight |
| Garmin API access | Blocks A4 | Investigating alternatives |
| CGM cost (~500-700 kr) | Delays real-time data | Auto-flows via Apple Health when ready |
| Databricks autoloader | Blocks full cloud pipeline | Can run manually in meantime |

---

## Decisions Log (last 5)

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-11 | Restructured PLAN.md into 10 categories | Master overview needed across all workstreams |
| 2026-03-10 | All P1 security fixes verified complete | Wave 1-3 addressed all P1 findings |
| 2026-03-09 | Full audit before new features | 1 P0 security + 4 P1 data integrity found and fixed |
| 2026-03-08 | Desktop: pywebview (not Electron/Tauri) | 100% Python code reuse, no JS toolchain |
| 2026-03-08 | Mobile: SwiftUI + direct Claude API | Native iOS feel, no server dependency |

---
---

## Appendix A: AI Governance Task Detail

*Full task list from Phase 7 — AI Governance Framework. Preserved for PR/script reference traceability.*

**Goal:** Structured control layer for AI agents — ensuring agents follow strategic priorities (TODO.md), not just nearest instruction (PROJECT_PLAN.md). PoC showcase for enterprise architecture.

| Task | Status | Notes |
|------|--------|-------|
| /prioritize command (merges TODO + PROJECT_PLAN) | Done | .claude/commands/prioritize.md |
| docs/AI_GOVERNANCE.md (internal governance framework) | Done | PoC version of ~/ai-ledelse.md |
| MEMORY.md cross-session context | Done | ~/.claude/projects/.../memory/MEMORY.md |
| Mandatory session protocol (CLAUDE.md) | Done | on_session_start / during / end |
| Specialised agents (12 agents) | Done | code-reviewer, security-reviewer, build-validator, etc. |
| CI/CD as unenforced gate (GitHub Actions) | Done | deploy.yml — bundle validation + deploy |
| ADRs as agent guardrails (docs/adr/) | Done | 5 ADRs: DuckDB, Medallion, YAML, Feature branch, AI-native |
| scripts/validate_naming.py (Layer 3, Tier 1) | Done | snake_case + hardcoded path enforcement |
| scripts/governance_check.py (Layer 3, Tier 1) | Done | CHANGELOG + ARCHITECTURE update gate |
| scripts/ai_pr_review.py (Layer 3, Tier 3) | Done | Claude Haiku PR reviewer, PASS/WARN/FAIL verdict |
| .github/workflows/ai-review.yml | Done | GitHub Actions AI PR gate |
| .github/workflows/governance-check.yml | Done | GitHub Actions governance gate |
| .pre-commit-config.yaml (Tier 1, local) | Done | naming + black + ruff + gitleaks |
| docs/decisions/DECISIONS.md (Layer 4) | Done | Session-level decision log |
| docs/COST_LOG.md (Layer 4) | Done | AI cost tracking |
| docs/SPRINT_LOG.md (Layer 2) | Done | Sprint planning and retrospectives |
| AI_GOVERNANCE.md -> full 7-layer framework | Done | Rewritten from PoC to complete framework doc |
| Claude Code hooks as enforcement layer | Done | pre_commit_guard.sh (PreToolUse) + post_commit.sh (PostToolUse) in scripts/hooks/ |
| CLAUDE.md: governance_sync block | Done | PR #41 — first action in on_session_start; auto-detects governance file drift before session begins |
| CLAUDE.md: model_routing section | Done | PR #42 — 11-task routing table; agent self-identifies model; flags mismatches; end-of-session routing recommendation |
| CLAUDE.md: security_protocol section | Done | PR #43 — continuous security awareness; 3 scan levels (per-file, per-session, periodic); health data special rules |
| CLAUDE.md: mandatory_task_reporting section | Done | PR #44 — 4-part post-task reporting; cannot be disabled; flags out-of-scope tasks; ports ai-ledelse.md s.6.4 |
| ai-ledelse.md: governance sync observation (s.25) | Done | PR #41 — added to Observationer fra praksis |
| ai-ledelse.md: section 19 Dynamic Model Routing | Done | PR #42 — inserted; sections 19-26 renumbered to 20-27 |
| ai-ledelse.md: section 24 Security som infrastruktur | Done | PR #43 — inserted; sections 24-27 renumbered to 25-28; now 28 sections total |
| ANTHROPIC_API_KEY -> GitHub Secrets | Not started | Manual: `gh secret set ANTHROPIC_API_KEY` — required for AI PR review to function |
| Master agent PoC (supervisor spawning sub-agents) | Not started | Reads all MD files, acts as architecture guard rail |
| ai-ledelse.md bidirectional sync process | In progress | 3 delta-file syncs completed; formal sync protocol defined in AI_GOVERNANCE.md Layer 7 |

**Exit criteria:** `/prioritize` produces a ranked Top 3 that reflects P0 strategic items from TODO.md — not just technical backlog from PROJECT_PLAN.md.

---

## Appendix B: AI-Native Data Model Milestones

*Full milestone breakdown for Phase 3b — AI-Native Data Model (Local Stack). Preserved for file path and line count reference.*

**Goal:** Replace local Gold layer with AI-native 2+2 architecture: Agent Memory + Semantic Contracts + MCP tools. See ADR-005.

| Task | Status | Notes |
|------|--------|-------|
| M0: Agent schema DDL (5 tables + metric_relationships) | Done | `setup/create_agent_schema.sql` |
| M0: COMMENT ON all 21 silver tables (~269 column descriptions) | Done | `setup/add_column_comments.sql` |
| M0: Initial YAML contracts (expanded to 26 in total) | Done | `contracts/metrics/` |
| M1: text_generator.py (template-based daily summaries) | Done | `ai/text_generator.py` (373 lines) |
| M1: baseline_computer.py (6 baselines + demographics) | Done | `ai/baseline_computer.py` (337 lines) |
| M1: Health knowledge graph (67 nodes, 108 edges) | Done | `setup/seed_health_graph.sql` |
| M1: Backfill daily summaries (91 days) | Done | 2025-11-21 to 2026-02-19 |
| M2: embedding_engine.py (sentence-transformers) | Done | `ai/embedding_engine.py` (277 lines) |
| M2: DuckDB VSS + HNSW indexes | Done | Experimental persistence enabled |
| M2: Backfill embeddings (91 summaries) | Done | all-MiniLM-L6-v2, 384-dim |
| M3: MCP server (17 tools) | Done | `mcp/server.py` |
| M3: health_tools.py (tool implementations) | Done | `mcp/health_tools.py` |
| M3: query_builder.py (YAML -> SQL) | Done | `mcp/query_builder.py` |
| M3: formatter.py (markdown output) | Done | `mcp/formatter.py` |
| M3: schema_pruner.py (category-based pruning) | Done | `mcp/schema_pruner.py` |
| M4: 26 metric YAML contracts | Done | `contracts/metrics/*.yml` (24 metrics + index + business rules) |
| M4: _index.yml + _business_rules.yml | Done | Master index + composite score + alerts |
| M5: ingestion_engine.py post-merge trigger | Done | Auto-generates daily summary after ingest |
| M5: .mcp.json updated (local, gitignored) | Done | Health MCP server config |
| M5: ADR-005 | Done | `docs/adr/ADR-005-ai-native-data-model.md` |
| M5: 55/55 pytest tests green | Done | All phases validated |
| Wire MCP server into Claude Code | Done | .mcp.json configured, 17 tools verified |
| Deprecate local Gold views | Not started | Keep Databricks Gold |

**Exit criteria:** AI agent queries health data via MCP tools with >80% accuracy. All memory tiers populated. ACHIEVED.

---

## Appendix C: MVP Iteration History

*Full iteration log. Preserved from original PROJECT_PLAN.md for historical traceability.*

Per `MASTER_PLAN.md`: `A1 -> C1/A5 -> A2 -> B1 -> A3 -> B3 -> B2 -> C2`

| Iteration | Name | Status | Notes |
|-----------|------|--------|-------|
| A1 | MCP Goes Live | Done | MCP server wired into Claude Code, 17 tools, smoke tests |
| C1/A5 | Daily Sync + API + Withings | Done | 6-step sync pipeline, FastAPI server, Withings connectors merged |
| A2 | Data Quality Shield | Done | Session 3: quality_rules.yaml (48 tables), schema_drift, dbt _schema.yml (49 models), bronze validation |
| A3 | Smart Alerts | Planned | Proactive anomaly detection + ntfy.sh notifications |
| B2 | Streaming Responses | Planned | SSE streaming in FastAPI |
| A8 | Lifesum CSV Expansion | Done | Bodymeasures, exercise, weighins, bodyfat merge scripts + sources_config entries |
| A9 | Workout Unification | Done | Strava to silver.workout merge, Lifesum exercise to workout, source_system column, cross-source duplicate detection |
| B1 | LLM Chat Engine Rewrite | Done | Claude tool-use (function calling), SSE streaming endpoint, multi-turn chat history |
| B3 | Multi-Stream Anomaly Detection | Done | Z-score analysis, constellation patterns, temporal degradation tracking |
| B4 | Expanded Correlation Engine | Done | 9 to 30+ metric pairs, cross-domain delayed effects |
| C2 | Intelligence Layer | Done | Trend forecaster (linear regression), recommendation engine (evidence-backed, CDS-safe) |
| D4 | Notification System | Done | ntfy.sh integration, severity-based push notifications |
| F1 | Infrastructure Cleanup | Done | Session 3: pyproject.toml, .editorconfig, conftest.py, logging fix |
| F2 | Documentation + Cleanup | Done | Metric dictionary, data lineage, changelog, WeasyPrint dependency confirmed |

---

## Appendix D: Phase Detail (Legacy)

*Detailed task tables from original Phase 0-6, preserved for completeness.*

### Phase 0 — Governance & Project Setup

**Goal:** Establish project structure, governance files, and working conventions.

| Task | Status | Notes |
|------|--------|-------|
| CLAUDE.md session rules | Done | Includes mandatory_session_protocol |
| PROJECT_PLAN.md | Done | This file |
| ARCHITECTURE.md | Done | docs/ARCHITECTURE.md |
| CHANGELOG.md | Done | docs/CHANGELOG.md |
| Claude Code custom commands | Done | /status, /plan-session, /end-session |
| CONTEXT.md | Done | docs/CONTEXT.md |
| Custom agents (12) | Done | code-reviewer, build-validator, etc. |
| AI governance framework | Done | ~/ai-ledelse.md + session protocol |

### Phase 1 — Foundation & Bronze Layer

**Goal:** All data sources ingested into bronze with consistent schema and metadata.

| Task | Status | Notes |
|------|--------|-------|
| Apple Health XML -> parquet | Done | 35+ types via process_health_data.py |
| Oura connector | Done | OAuth 2.0, incremental, 18 API + 7 CSV endpoints |
| Lifesum connector | Done | csv_to_parquet.py |
| Withings connector | Done | OAuth 2.0 keychain auth, 9 endpoints, full load 2020->now (1,947 records) |
| Strava connector | Done | OAuth 2.0 keychain auth, dedicated writer/state, 428 parquet files |
| sundhed.dk connector | Done | Playwright + MitID 2FA, 5 sections (lab, meds, vax, ejournal, appointments), 52 tests |
| GetTested connector | Not started | Manual/export — planned |
| sources_config.yaml — full schema | Done | ~59 sources defined |
| Ingestion engine — all active sources | Done | Works for Apple Health, Oura, Lifesum |
| Bronze validation tests | Done | Session 3: A2 quality shield — 48 tables, 6 check types |

**Exit criteria:** All active sources ingested into bronze, validated, repeatable.

### Phase 2 — Silver Layer — Core Transformations

**Goal:** Cleaned, typed, deduplicated entities for all active sources.

| Task | Status | Notes |
|------|--------|-------|
| Silver schema design (all entities) | Done | 51 dbt models |
| Apple Health silver transforms | Done | 9 entities: heart_rate, step_count, toothbrushing, body_temperature, respiratory_rate, water_intake, daily_energy_by_source, daily_walking_gait, mindful_session |
| Oura silver transforms | Done | 9 entities: daily_sleep, daily_activity, daily_readiness, heart_rate (shared), workout, daily_spo2, daily_stress, personal_info |
| Lifesum silver transforms | Done | daily_meal |
| Withings silver transforms | Done | blood_pressure + weight merge scripts merged |
| DuckDB local silver runner | Done | run_merge.py — 59 merge scripts working locally |
| Silver validation tests | Done | Session 3: dbt _schema.yml with 51 models |
| dbt schema tests on all entities | Done | 51 silver models in _schema.yml |

**Exit criteria:** All active source entities in silver, validated, repeatable locally.

### Phase 3 — Gold Layer — Reporting Entities

**Goal:** Aggregated, cross-source entities ready for reporting.

**Note:** Gold layer is now **cloud-only** (Databricks). Locally, Gold is replaced by the AI-Native Data Model (Phase 3b). See ADR-005.

| Task | Status | Notes |
|------|--------|-------|
| Gold entity design (Databricks) | Done | 21 gold views deployed to cloud |
| Gold views (local) | Done | 18 local gold views |
| Composite health score | Done locally | YAML recipe in `_business_rules.yml`; Databricks Gold TBD |
| Cross-source joins (sleep + activity + nutrition) | Not started | Databricks Gold only |
| Biomarker tracking views | Not started | |
| Gold validation tests | Not started | |

**Exit criteria:** Reporting-ready views covering all key health dimensions (cloud) + Semantic Contracts (local).

### Phase 4 — Visualization & Reporting

**Goal:** Choose and implement reporting destination.

| Task | Status | Notes |
|------|--------|-------|
| Evaluate options | Not started | Streamlit, Evidence, Superset, Power BI, Databricks AI/BI |
| Build dashboards | Not started | |
| Connect to gold layer | Not started | |
| Databricks Genie Space | Not started | "What was my best week in January?" |

### Phase 5 — Cloud Migration (Databricks)

**Goal:** Full platform running on Databricks with Unity Catalog.

| Task | Status | Notes |
|------|--------|-------|
| Catalog + schemas DDL | Done | health-platform-dev/prd, schemas: bronze, silver, gold, audit |
| Audit framework (job_runs, table_runs) | Done | AuditLogger context manager, Delta tables live |
| Databricks Asset Bundle (DAB) | Done | Bundle deployed to dev, deploy.yml CI/CD |
| Orchestration workflows | Done | bronze_job.yml, silver_job.yml, gold_job.yml |
| Silver SQL (Databricks) | Partial | 10 SQL files deployed; autoloader not running live data |
| Bronze -> Databricks live data | Not started | Autoloader config needs source path setup |
| Full end-to-end run in cloud | Not started | |
| DLT Expectations (data quality gates) | Not started | Bronze -> silver quarantine pattern |
| Unity Catalog Tags (pii_level, domain) | Not started | |

### Phase 6 — CI/CD & Automation

**Goal:** Automated testing, deployment, and data refresh.

| Task | Status | Notes |
|------|--------|-------|
| GitHub Actions setup | Done | 4 workflows: claude.yml, deploy.yml, ai-review.yml, ci.yml |
| Bundle validation on PRs | Done | Runs build-validator on every PR |
| Auto-deploy to dev on push | Done | Feature branches -> dev |
| Auto-deploy to prd on merge | Done | Main -> prd |
| Automated tests | In progress | 1334 unit tests green, 195 integration tests (marked, deselected). 1 MCP import issue remains (mcp 1.26.0 breaking change). |
| Oura daily job (cron) | Done | `scripts/daily_sync.sh` + `com.health.daily-sync.plist` — runs daily at 06:00 |
| Code-reviewer agent as PR gate | Not started | AI governance experiment |
