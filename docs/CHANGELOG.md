# CHANGELOG.md — HealthReporting Session Log

> Each session gets an entry. Most recent first.

---

## 2026-03-06 — Session 009: Iterations 1-3 — MCP Live + Daily Sync + Remote Access

**Phase:** Roadmap Iterations 1-3 (autonomous execution)
**Branch:** `feature/iteration-1-mcp-goes-live`
**Goal:** Wire MCP into Claude Code, automate daily Oura sync, build FastAPI for remote access

### What was done

**Iteration 1 — MCP Goes Live**
- Fixed `health_tools.py`: `id` → `insight_id`, VARCHAR → VARCHAR[] tags, added `is_active` filter
- Fixed `embedding_engine.py`: matching `insight_id` references in backfill
- Fixed `test_mcp_integration.py`: knowledge_base fixture aligned with real DB schema
- Created `tests/test_mcp_smoke.py` — 55+ tests covering all 8 MCP tools + 10 standard health questions

**Iteration 2 — Automated Daily Refresh**
- Created `scripts/daily_sync.sh` — 4-step pipeline (Oura fetch → Bronze → Silver → Summary + Embedding)
- Created `scripts/launchd/com.health.daily-sync.plist` — daily at 06:00
- ntfy.sh push notifications on success/failure

**Iteration 3 — Remote Access**
- Created `health_platform/api/server.py` — FastAPI with 5 endpoints (/health, /v1/chat, /v1/query, /v1/profile, /v1/alerts)
- Created `health_platform/api/auth.py` — Bearer token from macOS Keychain + env var fallback
- Created `scripts/launchd/com.health.api-server.plist` — always-on with KeepAlive
- Created `tests/test_api.py` — 14 tests for all endpoints

**Security hardening**
- SQL injection prevention: `_SAFE_IDENTIFIER` regex validator in `_parse_metric_ref`
- Expanded `run_custom_query` forbidden keywords (ATTACH, DETACH, COPY, CALL, PRAGMA)
- Removed unused imports (yaml, format_as_yaml, columns variable)
- Code review + security review via subagents

### Stats
- 5 commits, 11 files changed, 1744 lines added
- 222 tests passing (all green)
- 0 security issues found

### Architecture changes
- New: FastAPI REST layer (`api/server.py`, `api/auth.py`) — keyword-based chat routing
- New: launchd automation (daily sync + always-on API server)
- No breaking changes to existing MCP server or data model

### Manual steps required (not done by Claude)
- Install Tailscale on Mac Mini + iPhone + laptop
- Load launchd plists: `launchctl load ~/Library/LaunchAgents/com.health.daily-sync.plist`
- Set API token in Keychain: `security add-generic-password -s health-api -a token -w <your-token>`

### Carried over
- Iteration 4: Data Quality Shield (dbt tests, data quality suite)
- Iteration 5-10: remaining roadmap items
- Tailscale installation (manual)
- ANTHROPIC_API_KEY → GitHub Secrets

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
