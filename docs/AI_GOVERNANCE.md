# AI Governance Framework

> PoC version — HealthReporting as blueprint
> External reference: `~/ai-ledelse.md` (living document, updated independently)
> Last updated: 2026-02-28

---

## Problem

AI agents follow the nearest instruction, not overarching intent.

Concrete symptom: `/plan-session` reads `docs/PROJECT_PLAN.md` and proposes technical backlog tasks — while `~/TODO.md` contains P0 strategic items (income, momentum, blockers) that are never surfaced. Result: the agent optimizes for the wrong thing.

At team scale (50 developers, each with their own Claude Code), this diverges into 50 different architectures, conventions, and priorities. The question this framework answers: **how do you govern AI agents the same way you govern a software system?**

---

## What IS built (HealthReporting PoC)

### 1. CLAUDE.md as constitution
`CLAUDE.md` and `CLAUDE.local.md` define non-negotiable rules: language, branching, security, session protocol, commit discipline. The agent must follow these before any user instruction. This is the equivalent of a corporate code of conduct enforced at the tool level.

### 2. Mandatory session protocol
`CLAUDE.md` defines three lifecycle hooks:
- `on_session_start` — read governance files, present sprint status, confirm scope before coding
- `during_session` — automatic checkpoints after each major task
- `on_session_end` — update CHANGELOG, PROJECT_PLAN, ARCHITECTURE before closing

This prevents sessions from starting or ending without governance state being updated.

### 3. Specialised agents (12 agents)
`.claude/agents/` contains purpose-built agents with bounded scope:
- `security-reviewer` — mandatory before every PR merge
- `code-reviewer` — conventions + medallion quality check
- `build-validator` — DAB bundle completeness
- `medallion-reviewer`, `test-writer`, `yaml-config-writer`, etc.

Specialisation prevents generalist agents from making broad, unchecked changes.

### 4. MEMORY.md as cross-session context
`~/.claude/projects/.../memory/MEMORY.md` persists architectural decisions, gotchas, and conventions across sessions. Without this, every session starts from scratch and re-discovers the same mistakes.

### 5. /prioritize command (strategic → tactical bridge)
`.claude/commands/prioritize.md` merges `~/TODO.md` (strategic backlog) with `docs/PROJECT_PLAN.md` (technical backlog) into a single ranked list. Ensures P0 items from TODO.md are surfaced alongside in-progress technical tasks. Closes the silo between strategic intent and agent execution.

### 6. CI/CD as unenforced gate
GitHub Actions (`deploy.yml`) runs bundle validation on every PR and auto-deploys to dev/prd on merge. This is a structural guardrail: broken bundles cannot deploy. It is not probabilistic — it is deterministic enforcement.

---

## Next level (not built yet)

### code-reviewer as automatic PR gate
Currently, `code-reviewer` is a manual slash command. The next step: run it as a GitHub Actions step on every PR, posting a structured review comment. The agent becomes a mandatory reviewer — not optional.

### Claude Code hooks as enforcement layer
`settings.json` supports pre/post tool-call hooks. Example: before every `git push`, run `/security-review`. Before every file write, validate against CLAUDE.md conventions. This moves governance from advisory to enforced.

### Master agent (supervisor pattern)
A supervisor agent that reads all governance MD files (CLAUDE.md, ARCHITECTURE.md, AI_GOVERNANCE.md, ai-ledelse.md) and acts as a guard rail for all other agents. Every sub-agent spawned by the master is bounded by the master's context. This is the "AI master agent" concept — one agent that knows the full architectural intent and can veto or redirect other agents.

---

## Open questions

1. **Who owns CLAUDE.md in a team?** In a solo project, the developer owns it. In a team, it needs a review process — like a Terms of Service that all contributors accept.

2. **Deterministic vs probabilistic governance?** CI/CD gates are deterministic (pass/fail). Agent-based review is probabilistic (good/bad judgment). The right architecture uses both: CI for structural rules, agents for contextual review.

3. **How do you prevent CLAUDE.md drift?** If the file is never updated, it becomes stale. Need a process: after every architectural decision, update CLAUDE.md. This is currently manual — could be automated via `on_session_end`.

4. **Cross-agent memory?** Currently, MEMORY.md is read by the main agent. Sub-agents (spawned via Task tool) do not inherit it. Shared memory across agent instances is an open problem.

---

## Reference

- `~/ai-ledelse.md` — the living external document (updated independently, not committed to repo)
- `CLAUDE.md` — primary governance file for this project
- `.claude/commands/prioritize.md` — strategic/tactical bridge command
- `.claude/agents/` — specialised agent definitions
- `docs/PROJECT_PLAN.md` — Phase 7: AI Governance Framework tasks
