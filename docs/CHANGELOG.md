# CHANGELOG.md — HealthReporting Session Log

> Each session gets an entry. Most recent first.

---

## [Unreleased] — 2026-03-10

### Fixed
- **Governance:** validate_naming.py no longer flags `__init__.py` as non-snake_case (dunder file exclusion)
- **Governance:** validate_naming.py no longer flags its own `re.compile` pattern as a hardcoded prd reference
- **CI:** Removed CLAUDE.md existence check from standards-check.yml (CLAUDE.md is gitignored — contains local paths and security config)
- **CI:** Ignore filelock CVEs in pip-audit (transitive dep, fix requires Python 3.10+, CI runs 3.9)

---

## [Unreleased] — 2026-03-08

### Added
- **A8:** Lifesum CSV expansion — bodymeasures, exercise, weighins, bodyfat merge scripts + sources_config entries
- **A9:** Workout unification — Strava to silver.workout merge, Lifesum exercise to workout, source_system column, cross-source duplicate detection
- **B1:** LLM chat engine rewrite — Claude tool-use (function calling), SSE streaming endpoint, multi-turn chat history
- **B3:** Multi-stream anomaly detection — z-score analysis, constellation patterns, temporal degradation tracking
- **B4:** Expanded correlation engine — 9 to 30+ metric pairs, cross-domain delayed effects
- **C2:** Intelligence layer — trend forecaster (linear regression), recommendation engine (evidence-backed, CDS-safe)
- **D4:** Notification system — ntfy.sh integration, severity-based push notifications
- **F2:** Documentation — metric dictionary, data lineage diagram, WeasyPrint dependency confirmed

### Changed
- daily_sync.sh: added anomaly detection step (now 8 steps)
- _business_rules.yml: added constellation patterns, notification rules, recommendation rules
- MCP tools: added detect_anomalies, forecast_metric, get_cross_source_insights, get_recommendations, explain_recommendation

---

## 2026-03-06 — Session 011: Keychain Migration, Cleanup, Security Hardening

**Phase:** Post-review cleanup + secrets consolidation
**Branch:** `feature/iteration-1-mcp-goes-live`
**Goal:** Commit session 010 leftovers, migrate Oura auth from .env to Keychain, consolidate duplicated keychain patterns

### What was done

**Post-review commits**
- Committed `server.py` (Literal import, type constraint) and `chat_ui.py` (localStorage, isBusy lock, retry button, CSP headers) from session 010 review

**Cleanup**
- Added `docs/*.html` to `.gitignore` for generated diagram files
- Rewrote `run_chat.sh` from Streamlit launcher to FastAPI/uvicorn launcher
- Removed `.claude/settings.json` from git tracking (already in .gitignore)

**Shared keychain utility (NEW)**
- Created `utils/keychain.py` with `get_secret(key_name, fallback_env=True)` — single function for all macOS Keychain reads
- 7 test cases in `test_keychain_util.py` (all mocked, no real keychain access)

**Oura auth migration**
- Removed `python-dotenv` dependency from Oura `auth.py`
- `OURA_CLIENT_ID` and `OURA_CLIENT_SECRET` now read from `claude.keychain-db`
- Error messages include keychain setup instructions

**Keychain consolidation**
- `api/auth.py`: replaced 25-line `_load_token_from_keychain()` with `get_secret("HEALTH_API_TOKEN")`
- `api/chat_engine.py`: replaced 25-line `_get_api_key()` with `get_secret("ANTHROPIC_API_KEY")`
- `test_api.py`: updated fixture to mock `subprocess.run` instead of removed function

**Security documentation**
- `docs/learnings.md`: documented localStorage + `unsafe-inline` CSP risk acceptance for VPN-only deployment
- `docs/learnings.md`: documented keychain-first secrets management pattern
- `scripts/security-hardening.sh`: verification script for keychain timeout, FileVault, DNS, firewall (read-only)

### Stats
- 8 commits, 11 files changed (3 new, 8 modified)
- 229 tests passing (up from 222 — 7 new keychain tests)
- Net code reduction: ~57 lines removed (duplicated subprocess patterns)

### Architecture changes
- New: `utils/keychain.py` — shared secrets access layer
- Modified: all keychain reads now go through single utility
- Removed: `.env` file dependency from Oura connector

### Carried over
- Delete legacy `app/` directory (Streamlit UI — already deleted locally, not committed)
- Iteration 4: Data Quality Shield (dbt tests)
- Merge to main: pending Claus approval + iPhone live test
- Remaining modified files: `docs/SETUP_DATABRICKS_MCP.md`, `scripts/daily_sync.sh`

---

## 2026-03-06 — Session 010: Claude API Chat + Multi-AI Review

**Phase:** Roadmap Iteration 3 completion (AI-powered chat)
**Branch:** `feature/iteration-1-mcp-goes-live`
**Goal:** Replace keyword-based chat with Claude API intelligence, run multi-AI review pipeline

### What was done

**Claude API Integration — "Læge i lommen"**
- Created `chat_engine.py` — Claude Sonnet 4 integration with context gathering + system prompt
- Server.py delegates to `generate_response()` instead of keyword routing
- Context-aware: gathers relevant health data per question, includes patient profile
- Multi-topic support: `if` instead of `elif` allows asking about sleep AND steps
- Responds in user's language (Danish/English) with markdown tables, insights, baselines
- Lazy import of `anthropic` — tests run in Python 3.9 without SDK
- API key from claude.keychain-db with env var fallback

**Mobile UI Improvements (from UI review)**
- iOS keyboard: `interactive-widget=resizes-content` + visualViewport handler
- Safe area insets for iPhone home indicator
- Quick action buttons: min 44px touch targets
- Removed `user-scalable=no` (accessibility)
- Single newline → `<br>` in markdown renderer
- Table overflow: scrollable wrapper for narrow screens
- Typing indicator styled as bot bubble
- Auto-scroll: requestAnimationFrame for reliability
- Login input: enterkeyhint=go, spellcheck=false

**Security fixes (from security + Gemini review)**
- YAML-sourced identifiers now validated via `_validate_identifier`
- Silent `except: pass` → `logger.debug(exc_info=True)` in chat_engine
- Exception type name matching instead of string matching for auth errors
- `logger.exception` for API failures (includes stack trace)

### Multi-AI Review Pipeline
- **Gemini review (gemini-2.5-flash):** 8 findings addressed — silent error suppression, elif→if, auth error detection, dead code analysis
- **Security review (Claude subagent):** YAML SQL injection path identified and fixed
- **UI review (Claude subagent):** 3 HIGH, 4 MEDIUM, 4 LOW findings — all HIGH and MEDIUM fixed

### Stats
- 4 commits, 4 files changed
- 222 tests passing (all green)
- 3 AI reviews completed (Gemini + 2 Claude subagents)
- 0 security issues remaining

### Architecture changes
- New: `chat_engine.py` — AI-powered response generation (Claude API)
- Modified: server.py routes to chat_engine instead of keyword routing
- `_route_question` retained as test fallback only

### Carried over
- Iteration 4: Data Quality Shield (dbt tests)
- Merge to main: pending Claus approval + iPhone live test
- Clean up docs/architecture-diagram*.html and app/ directory

---

## 2026-03-04 — Session 008: AI-Native Data Model — Full Implementation

**Phase:** Phase 3b — AI-Native Data Model
**Goal:** Implement complete 2+2 architecture replacing local Gold layer with Agent Memory + Semantic Contracts + MCP tools

### What was done
- Created `agent` schema with 5 tables: patient_profile, daily_summaries, health_graph, health_graph_edges, knowledge_base
- Added `silver.metric_relationships` table for computed correlations
- Applied COMMENT ON to all 21 silver tables (269 column descriptions)
- Seeded health knowledge graph: 67 nodes, 108 edges (biomarkers, supplements, conditions, concepts, activities)
- Built `ai/text_generator.py` — template-based daily health summaries from silver tables
- Built `ai/embedding_engine.py` — sentence-transformers (all-MiniLM-L6-v2, 384-dim) with DuckDB VSS
- Built `ai/baseline_computer.py` — 6 rolling baselines + demographics → patient_profile
- Built `ai/correlation_engine.py` — Pearson correlations with lag support
- Built `mcp/server.py` — FastMCP server with 8 tools (query_health, search_memory, get_profile, etc.)
- Built `mcp/health_tools.py`, `query_builder.py`, `formatter.py`, `schema_pruner.py`
- Created 18 metric YAML contracts + `_index.yml` + `_business_rules.yml`
- Created `setup/setup_agent_schema.py` — idempotent schema setup runner
- Created 7 test files with 55 tests (all green)
- Modified `ingestion_engine.py` — post-merge daily summary trigger
- Ran setup against real dev database (296MB): 7 DDL, 269 comments, 67 nodes + 108 edges
- Backfilled 91 daily summaries (2025-11-21 to 2026-02-19) with embeddings
- Computed 9 patient profile entries (5 baselines + 4 demographics)
- Created HNSW index on daily_summaries (experimental persistence)
- Verified vector search returns semantically relevant results
- Created ADR-005 documenting 2+2 architecture decision
- 47 new files, 6,814 lines added, 2 files modified

### Architecture changes
- **Major:** Local stack redesigned from 5-layer medallion to 2+2 AI-native architecture
- Gold layer eliminated locally, replaced by Semantic Contracts (YAML)
- New `agent` schema with MemGPT-inspired tiered memory
- MCP server exposes 8 typed tools — AI never writes raw SQL
- Dual-stack: local AI-native + cloud traditional medallion (unchanged)
- sentence-transformers + DuckDB VSS for vector similarity search

### Carried over
- Wire MCP server into Claude Code for live queries
- Deprecate local Gold views
- ANTHROPIC_API_KEY → GitHub Secrets
- Bronze → Databricks live data

---

## 2026-02-28 — Session 007: CLAUDE.md Governance Extensions + ai-ledelse.md Sync

**Phase:** Phase 7 — AI Governance Framework
**Goal:** Execute three delta-file instructions (governance_sync, model_routing, security_protocol) + port mandatory_task_reporting from ai-ledelse.md s.6.4 — all synced to CLAUDE.md

### What was done
- Added `governance_sync` block to CLAUDE.md `on_session_start` — auto-detects drift between codebase and governance files before every session (PR #41)
- Added observation to `~/ai-ledelse.md` section 25 (Observationer fra praksis) — governance sync as drift-detection pattern (PR #41)
- Added `model_routing` section to CLAUDE.md — 11-task routing table (Sonnet/Opus/Haiku), agent self-identification, mismatch flagging, session-end model recommendation (PR #42)
- Added section 19 "Dynamic Model Routing" to `~/ai-ledelse.md` — renumbered sections 19-26 → 20-27 (PR #42)
- Added `security_protocol` section to CLAUDE.md — continuous security awareness, 3 scan levels (per-file/session/periodic), health data rules, incident response (PR #43)
- Added section 24 "Security som infrastruktur" to `~/ai-ledelse.md` — renumbered sections 24-27 → 25-28; now 28 sections total (PR #43)
- Added `mandatory_task_reporting` section to CLAUDE.md — 4-part post-task protocol (task status, goal impact, session progress, next step); cannot be disabled; flags out-of-scope tasks (PR #44)
- Updated `docs/PROJECT_PLAN.md` — 7 new ✅ tasks, ai-ledelse.md sync → 🔵 in progress
- 137 commits, velocity 16.71x

### Architecture changes
- CLAUDE.md now has 5 governance sections: `mandatory_session_protocol`, `governance_sync`, `model_routing`, `security_protocol`, `mandatory_task_reporting`
- ai-ledelse.md expanded from 26 → 28 sections (3 new sections added via delta files)

### Carried over
- ANTHROPIC_API_KEY → GitHub Secrets (manual: `gh secret set ANTHROPIC_API_KEY`) — blocks AI PR review
- Master agent PoC (supervisor spawning sub-agents)
- TDD setup — pytest framework for silver validation
- Bronze → Databricks live data (Autoloader source path setup)

---

## 2026-02-28 — Session 006: Claude Code Hooks + ai-ledelse.md expansion

**Phase:** Phase 7 — AI Governance Framework
**Goal:** Implement Claude Code in-process hooks (enforcement layer) + expand ~/ai-ledelse.md with 10 new sections

### What was done
- Expanded `~/ai-ledelse.md` from 16 → 26 sections (10 new: prompt engineering, status reporting, data governance, rollback, metrics, AI productivity measurement, training, code ownership, vendor strategy, compliance)
- Created `scripts/hooks/pre_commit_guard.sh` — PreToolUse hook: warns on direct main commits (soft enforcement)
- Created `scripts/hooks/post_commit.sh` — PostToolUse hook: auto-runs productivity tracker after every git commit
- Updated `.claude/settings.json` with PreToolUse (Bash) + PostToolUse (Bash) hook configuration
- Updated `docs/AI_GOVERNANCE.md` — Layer 3 section with full Claude Code Hooks architecture and hook execution model
- Merged PR #40 (feature/claude-code-hooks) — all GitHub Actions checks passed
- 127 commits, velocity 15.37x

### Carried over
- ANTHROPIC_API_KEY → GitHub Secrets (manual: `gh secret set ANTHROPIC_API_KEY`)
- Master agent PoC (supervisor spawning sub-agents)
- TDD setup — pytest framework for silver validation
- Bronze → Databricks live data (Autoloader source path setup)

---

## 2026-02-28 — Session 005: AI Governance Enforcement Layer

**Phase:** Phase 7 — AI Governance Framework
**Goal:** Full 7-layer AI governance enforcement — ADRs, pre-commit, AI PR review, GitHub Actions gates, observability docs

### What was done
- Created `docs/adr/` with 4 ADRs: DuckDB, Medallion, YAML-driven pipeline, Feature branch workflow
- Created `scripts/validate_naming.py` — Layer 3 Tier 1: snake_case + hardcoded path enforcement (pre-commit + CI)
- Created `scripts/governance_check.py` — Layer 3 Tier 1: CHANGELOG/ARCHITECTURE update gate (CI)
- Created `scripts/ai_pr_review.py` — Layer 3 Tier 3: Claude Haiku PR reviewer, posts PASS/WARN/FAIL comment
- Created `.github/workflows/ai-review.yml` — GitHub Actions AI PR gate (requires ANTHROPIC_API_KEY secret)
- Created `.github/workflows/governance-check.yml` — GitHub Actions governance + naming check
- Created `.pre-commit-config.yaml` — local enforcement: validate-naming, black, ruff, gitleaks
- Created `docs/decisions/DECISIONS.md` — Layer 4 decision log (13 historical decisions documented)
- Created `docs/COST_LOG.md` — Layer 4 cost tracking (sessions + PR review costs)
- Created `docs/SPRINT_LOG.md` — Layer 2 sprint planning and retrospectives (Sprints 001-004)
- Rewrote `docs/AI_GOVERNANCE.md` — full 7-layer framework (Constitution → Evolution), maturity model, references
- Updated `CLAUDE.md` — added pre-commit + ADR references to Tooling section
- Updated `docs/PROJECT_PLAN.md` — Phase 7 expanded with 11 new tasks (10 ✅ done, 4 ⬜ remaining)

### Architecture changes
- Layer 3 enforcement fully implemented: Tier 1 (deterministic) + Tier 3 (probabilistic) + pre-commit (local)
- Layer 4 observability: decision log, cost tracking, sprint log all live
- Project maturity: Level 1 (Foundation) → Level 3 (Enforced)
- GitHub Actions gates: 3 workflows (deploy.yml existing + ai-review.yml + governance-check.yml)

### Manual action required
- `gh secret set ANTHROPIC_API_KEY` — required for ai-review.yml to function

### What's next
- ANTHROPIC_API_KEY to GitHub Secrets (manual)
- Claude Code hooks (pre/post tool-call enforcement in settings.json)
- Master agent PoC (supervisor pattern)
- TDD setup (pytest framework for silver validation)

---

## 2026-02-28 — Session 003: AI Governance Framework

**Phase:** Phase 7 (AI Governance) — new phase
**Goal:** Struktureret kontrollag for AI-agenter — merger TODO.md + PROJECT_PLAN.md siloer

### What was done
- Added Phase 7: AI Governance Framework to docs/PROJECT_PLAN.md (10 tasks, 5 done, 5 planned)
- Created docs/AI_GOVERNANCE.md — PoC governance framework document (problem, what IS built, next level, open questions)
- Created .claude/commands/prioritize.md — slash command that merges ~/TODO.md + PROJECT_PLAN.md into ranked priority table with Top 3
- Added ai-ledelse.md merge note to ~/TODO.md (outside repo)
- Phase 7 Phase Overview row added to project plan table

### What changed in architecture
- New command: /prioritize — bridges strategic backlog (TODO.md) with technical backlog (PROJECT_PLAN.md)
- New doc: docs/AI_GOVERNANCE.md — documents existing governance patterns + next-level roadmap
- Project plan: Phase 7 added with 10 tasks covering AI agent governance

### What's next
- code-reviewer as automatic GitHub Actions PR gate
- Claude Code hooks as enforcement layer
- Master agent PoC (supervisor pattern)

---

## 2026-02-28 — Session 002: Audit Framework + Governance Setup

**Phase:** Phase 2 (Silver) + Phase 5 (Databricks) — parallel
**Goal:** Deploy audit logging framework to Databricks dev, add project governance framework

### What was done
- Deployed Databricks Asset Bundle to dev (bundle validate + deploy)
- Fixed Delta DEFAULT constraint issue (PR #35) — `DEFAULT 'running'` not supported on Serverless SQL
- Created audit schema in health-platform-dev: `job_runs`, `table_runs`, `v_platform_overview`
- Verified AuditLogger writes correctly to Delta tables
- Added AI governance P0 items to ~/TODO.md
- Created ~/ai-ledelse.md — AI management framework document
- Created project governance layer: PROJECT_PLAN.md, ARCHITECTURE.md, CHANGELOG.md
- Added CLAUDE.md mandatory session protocol (on_session_start, during_session, on_session_end)
- Added slash commands: /status, /plan-session, /end-session

### What changed in architecture
- Audit layer added: `health-platform-dev.audit` catalog live with Delta tables
- AuditLogger context manager available in `health_platform/utils/audit_logger.py`
- Governance docs added: docs/PROJECT_PLAN.md, docs/ARCHITECTURE.md, docs/CHANGELOG.md
- Commands added: .claude/commands/status.md, plan-session.md, end-session.md

### What's next
- TDD setup: pytest framework with conftest.py, RED tests for bronze/silver
- Oura daily job: automated bronze → silver via DAB workflow
- Live data flow to health-platform-dev (Autoloader source path setup)

---

## 2026-02-28 — Session 001: Governance Setup

**Phase:** Phase 0 — Governance & Project Setup
**Goal:** Establish project governance framework for Claude Code sessions

### What was done
- Created `CLAUDE.md` (session rules and code conventions)
- Created `docs/PROJECT_PLAN.md` (6-phase plan with milestones)
- Created `docs/ARCHITECTURE.md` (mermaid diagrams, current state)
- Created `docs/CHANGELOG.md` (this file)
- Created `.claude/commands/status.md` (/status)
- Created `.claude/commands/plan-session.md` (/plan-session)
- Created `.claude/commands/end-session.md` (/end-session)

### What changed in architecture
- No code changes — governance layer added

### What's next
- Begin Phase 2: silver validation tests (TDD)
- Begin Phase 5: live data flow to Databricks cloud

---

<!-- TEMPLATE for new entries:

## YYYY-MM-DD — Session NNN: [Short Title]

**Phase:** Phase X — [Name]
**Goal:** [One-liner]

### What was done
- Item 1
- Item 2

### What changed in architecture
- Description or "No architecture changes"

### What's next
- Item 1
- Item 2

-->
