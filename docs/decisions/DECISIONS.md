# DECISIONS.md — Architecture Decision Log

> Layer 4: Observability — session-level decision tracking
> Append-only. One row per non-trivial architectural or implementation decision.
> Agents: log here when making any decision not covered by an existing ADR.
> Reference: `docs/adr/` for accepted long-term decisions.

---

| Date | Session | Decision | Rationale | Alternatives considered |
|------|---------|----------|-----------|------------------------|
| 2026-02-28 | 001 | DuckDB as local runtime | Zero-config, fast, SQL-compatible with Databricks SQL dialect | SQLite (no window functions), PostgreSQL (too heavy) |
| 2026-02-28 | 001 | Medallion architecture (bronze → silver → gold) | Industry standard, clear separation of raw vs clean vs aggregated | Single-layer lake, ELT monolith |
| 2026-02-28 | 001 | YAML-driven pipeline config | New source = YAML entry, no code change. Scales to 50+ sources | Code-driven config, dbt-only approach |
| 2026-02-28 | 001 | Feature branch workflow for all changes | Every change reviewed, no direct commits to main | Trunk-based development (too risky solo) |
| 2026-02-28 | 002 | Oura daily summary over raw epochs | Reduces storage 10x. Gold layer does not need epoch granularity | Raw heartrate epochs (stored separately), hourly aggregates |
| 2026-02-28 | 002 | Silver MERGE INTO with `_ingested_at` watermark | Idempotent, incremental, handles late-arriving data | Full refresh (too slow), SCD2 (over-engineered for solo) |
| 2026-02-28 | 003 | `source_system` column propagated from bronze through gold | Enables multi-source joins and audit trails at every layer | Source implicit in table name (not queryable) |
| 2026-02-28 | 003 | AuditLogger as context manager | Auto-handles start/end, exception-safe, no boilerplate per script | Decorator pattern, manual try/finally |
| 2026-02-28 | 004 | GSD connector paused (security concerns) | API requires sharing credentials in chat — violates security constitution | Build anyway (rejected: never bypass security rules) |
| 2026-02-28 | 005 | ADRs as agent guardrails (docs/adr/) | Agents cannot reopen closed decisions without explicit human approval | Comments in code, verbal agreement |
| 2026-02-28 | 005 | Pre-commit hooks for naming + secrets | First line of defense — catches violations before they're committed | CI-only enforcement (too late in the loop) |
| 2026-02-28 | 005 | Claude Haiku for AI PR review | Cost-efficient for high-frequency PR checks. Sonnet for complex architectural decisions | Sonnet (10x cost, overkill for PR review), GPT-4 (not aligned with existing Anthropic stack) |
| 2026-02-28 | 005 | FAIL verdict = exit code 1 (CI hard block) | Security violations and architecture breaches must block merge, not just warn | Soft fail only (too easy to ignore) |

---

## How to use this file

**When to add a row:**
- Any decision not covered by an existing ADR that a future agent might question
- When choosing between two valid approaches
- When reversing a previous decision (note supersedes which row)

**What NOT to add:**
- Trivial implementation choices (which variable to name something)
- Decisions already captured in an ADR
- Completed tasks (those go in CHANGELOG.md)

**Format:**
```
| YYYY-MM-DD | Session NNN | Short decision statement | 1-sentence rationale | Alternatives |
```
