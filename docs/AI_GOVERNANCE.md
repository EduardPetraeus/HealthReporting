# AI Governance Framework

> HealthReporting implementation of the 7-layer AI governance blueprint.
> External reference (living document, not in repo): `~/ai-ledelse.md`
> Last updated: 2026-02-28 — Session 005

---

## The Problem

**AI agents are obedient, not intelligent.**
They optimise for the nearest instruction — not the organisation's overarching goals.

At solo scale: agent proposes technical backlog tasks while `~/TODO.md` contains P0 strategic items (income, momentum, blockers) that are never surfaced.
At team scale (50 developers): 50 diverging implementations of the same pattern.
At enterprise scale (500+): organisational chaos without a governance layer.

The solution is not more documentation. It is *active enforcement*.

---

## Framework Overview — 7 Layers

```
┌─────────────────────────────────────────────────────────┐
│  Layer 7: EVOLUTION — Who updates the rules?            │
│  ADR lifecycle, constitution reviews, framework drift   │
├─────────────────────────────────────────────────────────┤
│  Layer 6: TEAM GOVERNANCE — Multi-agent, multi-human    │
│  Roles, ownership, conflict resolution, escalation      │
├─────────────────────────────────────────────────────────┤
│  Layer 5: KNOWLEDGE — Continuity & memory               │
│  MEMORY.md, ADRs, context propagation, onboarding       │
├─────────────────────────────────────────────────────────┤
│  Layer 4: OBSERVABILITY — What is happening?            │
│  Audit trails, decision logs, cost tracking, metrics    │
├─────────────────────────────────────────────────────────┤
│  Layer 3: ENFORCEMENT — Automated gates                 │
│  CI/CD checks, AI PR review, pre-commit, security scan  │
├─────────────────────────────────────────────────────────┤
│  Layer 2: ORCHESTRATION — Session & Sprint              │
│  Session protocols, sprint planning, scope management   │
├─────────────────────────────────────────────────────────┤
│  Layer 1: CONSTITUTION — Static rules                   │
│  CLAUDE.md, ADRs, security policy, naming conventions  │
└─────────────────────────────────────────────────────────┘
```

Each layer depends on the one below. No enforcement without constitution.
No team governance without observability. Implement bottom-up.

---

## Layer 1: Constitution — Static Rules

**Status: IMPLEMENTED**

### CLAUDE.md — Agent constitution
The most important file in the framework. All agents read it before acting.
CLAUDE.md is not guidance — it is law.

Contents:
- Project identity (what we build, who owns it)
- Code conventions (naming, language, formatting)
- Architecture principles (patterns, layers, boundaries)
- Session protocol (start, during, end)
- Forbidden list (what agents must never do)
- Definition of done

Rules:
- Max ~200 lines — agent must parse it fast
- No prose — bullet points and short rules
- No "nice to have" — only hard rules
- Versioned in git — changes require review
- Critical rules go at the TOP — not buried at the bottom

### Architecture Decision Records (ADRs)
Accepted decisions that agents must not reopen without explicit human approval.
Location: `docs/adr/`

Current ADRs:
- ADR-001: DuckDB as local runtime
- ADR-002: Medallion architecture (bronze → silver → gold)
- ADR-003: YAML-driven pipeline configuration
- ADR-004: Feature branch workflow

### Security Constitution
Rules that apply regardless of context (enforced via pre-commit + CI):
- No secrets, tokens, passwords, or workspace URLs in committed files
- No hardcoded user paths (`/Users/`) in Python code
- No PII in logs, sample data, or test fixtures
- GitHub Secrets for all credentials
- No direct commits to main

### Naming Constitution
Deterministic rules enforced by `scripts/validate_naming.py`:
- `snake_case` for all names (files, variables, tables, columns)
- Bronze tables prefixed with `stg_`
- Gold views prefixed with `vw_`
- All code and documentation in English (no Danish in repo files)
- Branch names: `feature/`, `fix/`, `docs/`, `refactor/`

---

## Layer 2: Orchestration — Session & Sprint

**Status: IMPLEMENTED**

### Session Protocol (CLAUDE.md)

Three mandatory lifecycle hooks per session:

**on_session_start:**
1. Read `docs/PROJECT_PLAN.md`, `docs/ARCHITECTURE.md`, last 3 `docs/CHANGELOG.md` entries
2. Present sprint status: phase, last session summary, top 3 suggested tasks
3. Confirm scope — no coding before scope is agreed
4. Restate agreed scope as mini sprint goal

**during_session:**
- After each major task: 1-2 line summary → state what is next
- After 3+ tasks: pause and show ✅ Done / 🔵 In progress / ⬜ Remaining

**on_session_end:**
1. Full session summary (built, carried over, new issues)
2. Auto-update: CHANGELOG.md, PROJECT_PLAN.md, ARCHITECTURE.md
3. Final message: "Session [NNN] closed. [X] tasks completed, [Y] carried over."

### /prioritize Command
`.claude/commands/prioritize.md` — strategic/tactical bridge.

Merges `~/TODO.md` (strategic backlog) with `docs/PROJECT_PLAN.md` (technical backlog)
into a single ranked list: BLOCKER → P0 → ACTIVE → P1 → BACKLOG.
Ends with "Top 3 for this session" — not blocked, highest impact, single-session completable.

### Sprint Structure
`docs/SPRINT_LOG.md` tracks sprint goals, scope, and retrospectives.

```
Sprint = 1 week or 1 logical milestone
Session = 1 sitting (1-3 hours)
Sprint goal = single sentence: what does "done" look like?
Sprint scope = 5-8 concrete tasks from /prioritize output
```

Sprint ceremonies (automated via session protocol):
- Sprint start: `/prioritize` → confirm scope
- Daily standup: session protocol on_session_start
- Sprint review: session summary at on_session_end
- Sprint retro: retro note in SPRINT_LOG.md

---

## Layer 3: Enforcement — Automated Gates

**Status: IMPLEMENTED**

The only enforcement that cannot be overridden by an agent is GitHub Actions.
CLAUDE.md is probabilistic (agent *should* follow it). CI/CD is deterministic (code *cannot* merge without it).

### Tier 1 — Deterministic (fast, local + CI)

**Pre-commit hooks** (`.pre-commit-config.yaml`):
- `validate-naming` — snake_case filenames, no hardcoded `/Users/` paths
- `black` — Python formatting (24.3.0)
- `ruff` — Python linting with auto-fix (v0.3.4)
- `gitleaks` — secret scanning (v8.18.4)

Install: `pip install pre-commit && pre-commit install`

**GitHub Actions: Governance Check** (`.github/workflows/governance-check.yml`):
- Code changed but CHANGELOG.md not updated → warning
- Connector/bronze files changed but ARCHITECTURE.md not updated → warning
- Workflow files changed but CHANGELOG.md not updated → **hard fail**

**GitHub Actions: Naming Check** (same workflow):
- Runs `scripts/validate_naming.py` on every PR

### Tier 2 — Tests (not yet implemented)
- Unit tests (pytest)
- Silver data validation (row counts, null checks, schema match)
- dbt schema tests on all 17 silver entities

### Tier 3 — AI Review (probabilistic)

**GitHub Actions: AI PR Review** (`.github/workflows/ai-review.yml`):
- Model: `claude-haiku-4-5-20251001` (cost-efficient, high-frequency)
- Feeds: PR diff + CLAUDE.md + ARCHITECTURE.md + ADRs
- Posts structured PR comment with verdict:
  - **PASS** — follows all conventions, governance updated, no security issues
  - **WARN** — minor issues, can merge but developer should be aware (exit 0)
  - **FAIL** — security violation, hardcoded secret, ADR breach (exit 1 → blocks CI)

Requires: `ANTHROPIC_API_KEY` in GitHub Secrets.
Setup: `gh secret set ANTHROPIC_API_KEY` (interactive prompt, key never shown in terminal)

### Tier 4 — Human Review (highest trust)
Human reviews PR with agent comments as input. Final approval always human.

### Claude Code Hooks (Tier 0 — In-process enforcement)

**Status: IMPLEMENTED**

Claude Code hooks execute inside the agent's own process — before or after every tool call.
Unlike CI/CD (which catches violations after commit), hooks catch violations at the moment of action.

**Architecture:** `.claude/settings.json` → `scripts/hooks/`

| Hook | Type | Trigger | Action |
|------|------|---------|--------|
| `pre_commit_guard.sh` | PreToolUse / Bash | `git commit` detected | Warn on direct main commits |
| `post_commit.sh` | PostToolUse / Bash | `git commit` detected | Auto-run productivity tracker |
| Black formatter | PostToolUse / Write\|Edit | `.py` file written | Auto-format Python (in-place) |

**Hook execution model:**

```
Agent decides to call Bash("git commit -m ...")
  ↓
PreToolUse hook fires: pre_commit_guard.sh
  → Checks branch: is it main?
  → If main AND not productivity tracker: prints governance warning
  → Exits 0 (soft enforcement) — tool is allowed to proceed
  ↓
Bash tool executes: git commit runs
  ↓
PostToolUse hook fires: post_commit.sh
  → Detects 'git commit' in tool input
  → Runs update_productivity.py automatically
  → Prints: [GOVERNANCE] Productivity tracker updated after commit.
```

**Enforcement levels:**

| Level | Mechanism | Override possible? |
|-------|-----------|-------------------|
| Exit 0 (warning) | Prints to model context — agent sees it | Yes (agent may ignore) |
| Exit 2 (block) | Tool call is cancelled — command never runs | No (hard block) |

Current hooks use exit 0 (warning level). Upgrade path: change exit 0 → exit 2 for hard blocks
on specific violations (e.g., accidental `git push --force origin main`).

**Why in-process hooks over pre-commit alone:**
Pre-commit hooks only run on `git commit` — they miss agent actions that don't go through git.
Claude Code hooks intercept every tool call, including: Write, Edit, Bash, WebFetch, MCP calls.

---

## Layer 4: Observability — What Is Happening?

**Status: IMPLEMENTED (tracking files created)**

### Audit Trail
- `docs/CHANGELOG.md` — session-level: what was done, by whom, when
- Git commit messages — change-level: `feat(bronze): add oura connector`
- `docs/SPRINT_LOG.md` — sprint-level: goals, scope, retro notes

### Decision Log
`docs/decisions/DECISIONS.md` — when agents make non-trivial choices not covered by ADRs.

Format:
```
| Date | Session | Decision | Rationale | Alternatives considered |
```

### Cost Tracking
`docs/COST_LOG.md` — token usage and estimated API costs per session.

Guardrails:
- Monthly cost > $20 → review and optimise prompt sizes
- Single session > $2 → split into smaller sessions
- PR review cost > $0.05 → switch to lighter model

### Quality Metrics (target state)
- Test coverage (%)
- Lint errors per commit
- AI review pass rate (% PRs passing first time)
- Governance compliance (% sessions with correct start/end protocol)
- Architecture drift score (deviations from ARCHITECTURE.md)

---

## Layer 5: Knowledge — Continuity & Memory

**Status: IMPLEMENTED**

### Memory Hierarchy

```
Level 1: CLAUDE.md          → Permanent, rarely changed (constitution)
Level 2: ARCHITECTURE.md    → Updated at milestones (system state)
Level 3: PROJECT_PLAN.md    → Updated every session (tactical)
Level 4: CHANGELOG.md       → Append-only (history)
Level 5: MEMORY.md          → Auto-generated cross-session context
Level 6: Session context    → Disappears when session closes
```

`~/.claude/projects/.../memory/MEMORY.md` gives agents cross-session context.
Without it, every session starts from scratch.
PROJECT_PLAN.md + CHANGELOG.md are the explicit version — instruction set for the agent.

### Architecture Decision Records
Accepted decisions that agents cannot reopen without human approval.
See `docs/adr/`. ADR lifecycle: `Proposed → Accepted → Superseded | Deprecated`.

### Why ADRs are critical with AI
Without ADRs, an agent will suggest "a better solution" — which is the same solution
evaluated and rejected 3 weeks ago. ADRs prevent decision loops.

### Context Propagation
```
Session N ends → CHANGELOG + PROJECT_PLAN updated
Session N+1 starts → Agent reads CHANGELOG + PROJECT_PLAN → has full context
```

---

## Layer 6: Team Governance — Multi-Agent, Multi-Human

**Status: DESIGNED (not implemented — solo project)**

### Ownership Model (enterprise target state)
```
CLAUDE.md owner → Tech Lead / Platform team
ADR owner       → Author + reviewer (PR process)
SESSION protocol → Every developer follows same protocol
MEMORY.md       → Per-developer, but shared patterns promoted to CLAUDE.md
```

### Role Matrix
| Role | Owns | Can change | Cannot change |
|------|------|------------|---------------|
| Developer | Feature branch | Own CLAUDE.local.md | Org CLAUDE.md |
| Tech Lead | CLAUDE.md | Architecture ADRs | Security constitution |
| Platform | CI/CD gates | Enforcement rules | Individual ADRs |
| Security | Security constitution | All security rules | — |

### Conflict Resolution
If agent recommendation contradicts an existing ADR:
1. Agent flags the conflict explicitly
2. Human reviews (does the ADR need updating, or is the agent wrong?)
3. If ADR superseded: create new ADR, reference old one
4. If agent wrong: no action, session note added to DECISIONS.md

---

## Layer 7: Evolution — Who Updates the Rules?

**Status: DESIGNED (protocol defined)**

### Constitution Review Cadence
- CLAUDE.md → reviewed at each major phase milestone
- ADRs → reviewed when a new decision would contradict an accepted one
- Security constitution → reviewed when new tooling or connectors are added
- AI_GOVERNANCE.md → reviewed when `~/ai-ledelse.md` is updated externally

### Framework Drift Prevention
Risk: governance files become stale and lose authority.

Prevention:
- `on_session_end` protocol mandates governance file updates
- `governance_check.py` CI gate enforces CHANGELOG update with every code change
- Quarterly review prompt in SPRINT_LOG.md

### Meta-Governance Process
1. New pattern observed in practice → note in `docs/learnings.md`
2. Pattern validated across 3+ sessions → promote to CLAUDE.md or new ADR
3. CLAUDE.md change → PR with review (no direct commit to main)
4. ADR superseded → create new ADR referencing old one, mark old as Superseded

### Sync with ~/ai-ledelse.md
`~/ai-ledelse.md` is the living external document (not in repo).
When updated externally:
1. Read both files side by side
2. Identify new patterns or decisions in ai-ledelse.md not yet in this repo
3. Merge relevant insights: new ADRs, updated CLAUDE.md rules, new governance patterns
4. Commit as `docs: sync AI governance framework with ai-ledelse.md`

---

## Maturity Model

| Level | Name | Description | What we have |
|-------|------|-------------|--------------|
| 0 | Ad-hoc | No governance, agents do whatever | — |
| 1 | Foundation | CLAUDE.md exists, basic session protocol | ✅ Done |
| 2 | Structured | ADRs, MEMORY.md, specialised agents, /prioritize | ✅ Done |
| **3** | **Enforced** | **Pre-commit, CI gates, AI PR review, cost tracking** | **✅ Done (Feb 2026)** |
| 4 | Measured | Dashboard, quality metrics, drift score, test coverage >70% | ⬜ Next |
| 5 | Self-optimising | Master agent, automated retro, framework updates itself | ⬜ Future |

**Current maturity: Level 3 — Enforced**

---

## Open Questions

1. **Who owns CLAUDE.md in a team?** Needs a review process — like a Terms of Service
   that all contributors accept. Currently sole owner.

2. **Deterministic vs probabilistic governance?** CI/CD gates are deterministic (pass/fail).
   Agent-based review is probabilistic (judgment). Architecture uses both.

3. **How to prevent CLAUDE.md drift?** Currently manual. Could automate via on_session_end:
   agent suggests additions based on decisions made during session.

4. **Cross-agent memory?** Sub-agents spawned via Task tool do not inherit MEMORY.md.
   Shared memory across agent instances is an open problem.

5. **Master agent pattern?** A supervisor agent that reads all governance MD files and
   acts as a guardrail for all sub-agents. Every spawned agent is bounded by master context.
   This is the next level beyond current implementation.

---

## References

| Resource | Description |
|----------|-------------|
| `~/ai-ledelse.md` | Living external document (updated independently, not in repo) |
| `CLAUDE.md` | Primary governance file for this project |
| `docs/adr/` | Architecture Decision Records (4 ADRs) |
| `docs/decisions/DECISIONS.md` | Session-level decision log |
| `docs/SPRINT_LOG.md` | Sprint planning and retrospectives |
| `docs/COST_LOG.md` | AI usage and cost tracking |
| `.claude/commands/prioritize.md` | Strategic/tactical bridge command |
| `scripts/validate_naming.py` | Layer 3 Tier 1 enforcement |
| `scripts/governance_check.py` | Layer 3 Tier 1 governance gate |
| `scripts/ai_pr_review.py` | Layer 3 Tier 3 AI PR review |
| `.pre-commit-config.yaml` | Local enforcement hooks |
| `.github/workflows/ai-review.yml` | GitHub Actions AI gate |
| `.github/workflows/governance-check.yml` | GitHub Actions governance gate |
