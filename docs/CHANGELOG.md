# CHANGELOG.md — HealthReporting Session Log

> Each session gets an entry. Most recent first.

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
