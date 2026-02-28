# SPRINT_LOG.md — Sprint Planning & Review

> Layer 2: Orchestration — sprint-level tracking across sessions
> Each sprint = 1 week or one logical milestone. Each session is logged in CHANGELOG.md.
> Use `/prioritize` at sprint start to merge TODO.md + PROJECT_PLAN.md into ranked scope.

---

## Sprint Template

```markdown
## Sprint NNN — [Sprint Goal]

**Dates:** YYYY-MM-DD → YYYY-MM-DD
**Phase focus:** Phase X — [Name]
**Sprint goal:** [Single sentence: what does "done" look like for this sprint?]

### Scope (committed)
- [ ] Task 1 — [source: TODO.md / PROJECT_PLAN.md phase X]
- [ ] Task 2
- [ ] Task 3

### Discovered tasks (do NOT start — log only)
- [ ] [task discovered mid-sprint — carry to next sprint]

### Review
**Completed:** X/Y tasks
**Carried over:** [list]
**Blockers encountered:** [list or "none"]
**Retro note:** [1 sentence — what took longer than expected, what would you do differently]
```

---

## Sprint 001 — Foundation & Governance Setup

**Dates:** 2026-02-01 → 2026-02-14
**Phase focus:** Phase 0 — Governance, Phase 1 — Bronze
**Sprint goal:** Repo structure, CLAUDE.md, first bronze connector working end-to-end.

### Scope
- [x] CLAUDE.md + session protocol
- [x] PROJECT_PLAN.md + ARCHITECTURE.md + CHANGELOG.md
- [x] Apple Health XML → parquet connector
- [x] Oura OAuth connector (8 endpoints)
- [x] sources_config.yaml (27 sources)
- [x] Ingestion engine for 3 active sources

### Review
**Completed:** 6/6 tasks
**Carried over:** Bronze validation tests (deprioritised)
**Retro note:** Oura hive partitioning overwrites `day` column — document in MEMORY.md as gotcha.

---

## Sprint 002 — Silver Layer

**Dates:** 2026-02-14 → 2026-02-21
**Phase focus:** Phase 2 — Silver Layer
**Sprint goal:** All 3 active sources have validated silver transforms running locally.

### Scope
- [x] Silver schema design (21 dbt models)
- [x] Apple Health silver transforms (9 entities)
- [x] Oura silver transforms (9 entities)
- [x] Lifesum silver transforms (daily_meal)
- [x] DuckDB silver runner (run_merge.py)

### Discovered tasks
- [ ] Withings silver transforms (partial — blood_pressure + weight merge scripts exist but not complete)
- [ ] TDD framework — pytest + conftest.py (carry to Phase 6)

### Review
**Completed:** 5/5 core tasks
**Carried over:** Silver validation tests, TDD framework
**Retro note:** `run_merge.py` splits SQL on ALL semicolons — never put `;` inside comments. Documented in MEMORY.md.

---

## Sprint 003 — Databricks + Audit

**Dates:** 2026-02-21 → 2026-02-28
**Phase focus:** Phase 5 — Cloud Migration, Phase 6 — CI/CD
**Sprint goal:** Databricks Asset Bundle deployed to dev, audit framework live in Delta tables.

### Scope
- [x] Catalog + schemas DDL (health-platform-dev/prd)
- [x] AuditLogger context manager
- [x] Databricks Asset Bundle (DAB) deployed
- [x] Silver SQL (10 files) deployed to Databricks
- [x] GitHub Actions (deploy.yml) live

### Review
**Completed:** 5/5 tasks
**Carried over:** Autoloader live data, full end-to-end cloud run
**Retro note:** Python 3.9 requires `Optional[str]` from typing — not `str | None`. Documented.

---

## Sprint 004 — AI Governance Framework

**Dates:** 2026-02-28 → 2026-02-28 (single day — intensive)
**Phase focus:** Phase 7 — AI Governance Framework
**Sprint goal:** Full AI governance enforcement layer — ADRs, pre-commit, AI PR review, GitHub Actions gates.

### Scope
- [x] /prioritize command (merges TODO.md + PROJECT_PLAN.md)
- [x] docs/AI_GOVERNANCE.md (full 7-layer framework)
- [x] docs/adr/ (4 ADRs: DuckDB, Medallion, YAML, Feature branch)
- [x] scripts/validate_naming.py (Layer 3, Tier 1 enforcement)
- [x] scripts/governance_check.py (Layer 3, Tier 1 enforcement)
- [x] scripts/ai_pr_review.py (Layer 3, Tier 3 — AI PR gate)
- [x] .github/workflows/ai-review.yml (GitHub Actions AI gate)
- [x] .github/workflows/governance-check.yml (GitHub Actions governance gate)
- [x] .pre-commit-config.yaml (local enforcement: naming + black + ruff + gitleaks)
- [x] docs/decisions/DECISIONS.md (Layer 4 decision log)
- [x] docs/COST_LOG.md (Layer 4 cost tracking)
- [x] docs/SPRINT_LOG.md (Layer 2 sprint structure — this file)

### Discovered tasks
- [ ] ANTHROPIC_API_KEY → GitHub Secrets (manual: `gh secret set ANTHROPIC_API_KEY`)
- [ ] Claude Code hooks (pre/post tool-call enforcement in settings.json)
- [ ] Master agent PoC (supervisor spawning sub-agents)
- [ ] TDD setup (pytest framework)
- [ ] Architecture diagram (self-contained HTML)

### Review
**Completed:** 12/12 tasks
**Carried over:** See discovered tasks above
**Retro note:** Context window hit limit mid-session — compaction kicked in. Use /prioritize at start of each sprint, not just sessions.

---

## Next Sprint — TBD

Use `/prioritize` to determine scope based on:
1. Discovered tasks from Sprint 004
2. Remaining Phase 2 tasks (silver validation tests)
3. Phase 5 tasks (autoloader live data)
4. TODO.md P0 items (income, momentum)
