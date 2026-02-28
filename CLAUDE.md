# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Personal health data platform (owner: Claus Eduard Petraeus) that ingests, transforms, and surfaces data from wearable devices and nutrition apps. Uses a **medallion architecture** (bronze -> silver -> gold) with a local-first approach on DuckDB, targeting Databricks as the cloud platform.

---

## Mandatory Session Protocol

These rules are NOT optional. Follow them automatically without being asked.

### on_session_start (always, before any coding)
1. Read `docs/PROJECT_PLAN.md`, `docs/ARCHITECTURE.md`, last 3 entries in `docs/CHANGELOG.md`
2. Present a sprint status to the user:
   - Current phase and progress
   - What was done last session
   - Top 3 suggested tasks for this session (based on project plan priority)
3. Ask: "What should we tackle today?" — do NOT start coding until scope is confirmed
4. After confirmation, restate the agreed scope as a mini sprint goal

### during_session (automatic checkpoints)
After completing each major task or subtask:
1. Briefly confirm what was just completed (1-2 lines)
2. State what you are moving to next
3. If you have completed 3+ tasks, pause and present: ✅ Done / 🔵 In progress / ⬜ Remaining

### on_session_end (always, before closing)
When the user says they are done, or when the agreed scope is completed:
1. Present a full session summary (what was built, what carried over, new issues)
2. Update automatically:
   - `docs/CHANGELOG.md` — new entry at top
   - `docs/PROJECT_PLAN.md` — update task statuses
   - `docs/ARCHITECTURE.md` — update if components were added/changed
3. Final message: "Session [NNN] closed. [X] tasks completed, [Y] carried over."

### if_user_forgets
- If the user starts coding without a session start: pause and run the start protocol first
- If the user says "bye", "done", "tak for nu" or similar: run end protocol before closing
- Never let a session end without updating the governance files

### definition_of_done
A feature is done when:
1. Code works locally with DuckDB (`HEALTH_ENV=dev`)
2. Config is updated (`sources_config.yaml`)
3. Governance files are updated (via end-session protocol)

---

## Environment Separation

`HEALTH_ENV` env var controls dev vs prd. The DuckDB file is named `health_dw_{env}.db`. The Databricks catalog is `health_dw` with schemas: bronze, silver, gold.

## Conventions

- **snake_case** for all names (tables, columns, variables, files)
- **All code and documentation in English** — this includes `.md` files, comments, docstrings, variable names, and print messages. No Danish in any repo file.
- Prefix bronze tables with `stg_`
- Individual files over monolithic scripts
- Tech-agnostic: local logic uses DuckDB/Python; cloud logic uses Databricks SQL/PySpark
- Metadata-driven via YAML — adding a new source = adding a YAML entry, no code changes

## Tooling

- **Databricks MCP server** configured in `.mcp.json` for Claude Code integration
- **VS Code** with DuckDB file viewer for parquet/csv and SQL Tools connection
- Python 3.9 venv (`.venv/`) — key packages: `duckdb`, `pandas`, `pyyaml`, `dbt-core`, `dbt-duckdb`
- CI/CD via GitHub Actions — bundle validation, AI PR review, governance checks on every PR
- **Pre-commit hooks** — install with `pip install pre-commit && pre-commit install` (naming, black, ruff, gitleaks)
- **Architecture Decision Records** — `docs/adr/` — accepted decisions agents must not reopen without human approval

## Further Context

- Key paths and config files: `docs/paths.md`
- How to run the platform locally: `docs/runbook.md`
- Architecture, data flow, silver/gold patterns: `docs/architecture.md`
- Project context is located in the /docs folder. Review `docs/architecture.md` or `docs/CONTEXT.md` only when explicitly relevant to the task at hand


## Branching Strategy

Always work in a feature branch — never commit directly to `main`.

Branch naming:
- `feature/` — new functionality
- `fix/` — bug fixes
- `docs/` — documentation only
- `refactor/` — restructuring without new functionality

Workflow:
1. `git checkout -b feature/<short-description>`
2. Commit and push
3. Open PR → GitHub Actions validates bundle
4. Merge to main → auto-deploys to Databricks prd
5. After merging: `git checkout main && git pull && git branch -d <branch>`

Feature branches should be deployed to dev before opening a PR:
```bash
cd health_unified_platform && databricks bundle deploy --target dev
```
This deploys directly from the feature branch — no merge to `main` required for dev testing.
GitHub is configured to automatically delete remote head branches after merge.

## Productivity Tracking

After every git commit, automatically run:

```bash
.venv/bin/python update_productivity.py
```

This updates `claude_code_productivity.json` with git-derived metrics (files changed, lines added/removed, refactor ratio, code density, commit hour, time between commits, streak days, task type). Never ask the user to do this manually — it is always Claude Code's responsibility.

`claude_code_productivity.json` may be committed directly to `main` without a PR — it is a tracking file, not code.

## Scalability & PoC Mindset

This repo is used as a **PoC platform for enterprise architecture demonstrations** (Pandora lead engineer context). Every design decision should be made with scalability in mind — even when the current scale is personal/small.

Principles:
- **Build for extension, not just current need** — YAML-driven configs, generic runners, clear separation of concerns
- **Document the scale-up path** — when a simpler approach is chosen, note what the enterprise-grade alternative would be (e.g., DLT vs Jobs, SCD2 vs SCD1, DQ expectations)
- **Avoid hard-coded assumptions** — no hardcoded user IDs, paths, or single-source logic
- **Think multi-tenant** — `source_system` isolation, `user_id` dimension readiness, catalog/schema separation
- **PoC patterns worth demonstrating**: medallion architecture, metadata-driven pipelines, CI/CD for data, dev/prd environment separation, data quality gates

## Subagent Usage

Claude Code has three built-in subagent patterns — use them proactively:

| Agent | When to use |
|---|---|
| **Explorer** (`Explore`) | Scan codebase for snippets, dependencies, patterns — e.g. "find all silver SQL transforms", "which files use source_system" |
| **Researcher** (`general-purpose`) | Fetch external docs, search Databricks/Synapse references, investigate unfamiliar APIs |
| **Historian** (memory system) | Cross-session context lives in `~/.claude/projects/.../memory/`. Read `MEMORY.md` at session start for project state. |

## Learnings Log

After completing any task that involves an architectural decision, a mistake fixed, or a pattern worth remembering, append a note to `docs/learnings.md`. This includes:
- Architectural decisions and the reasoning behind them
- Mistakes made and how they were fixed (with concrete example)
- Databricks/DuckDB/dbt quirks discovered in practice
- "Would do differently" reflections
- Security decisions

Do this proactively — do not wait to be asked.

## Security Review

Run the `security-reviewer` agent (via `/security-review`) before every PR merge and before every Databricks deployment. This is mandatory, not optional.

What to check:
- No secrets, tokens, PAT tokens, passwords, or workspace URLs in committed files
- No sensitive data in print/log statements
- No PII in sample data or test fixtures
- GitHub Secrets used for all credentials — never hardcoded

If a security issue is found: fix it before merging. Never bypass with `--no-verify`.

## Verification

Before considering a task done, verify:
- **Python scripts** — run the script, check output is correct
- **SQL transforms** — run a row count / sample query on the result table
- **Databricks bundle** — use the `build-validator` agent to check completeness
- **New connectors** — ingest a small sample and verify bronze table output

If you cannot verify it, say so explicitly.

## Context Management

When compacting, always preserve:
- Current branch name and what we were working on
- List of modified files
- `HEALTH_ENV` value in use
- Any pending TODO items from the current task

## Custom Commands & Agents

Slash commands (`.claude/commands/`):
- `/commit-push-pr` — stage, commit, push, open PR, run productivity tracker
- `/new-feature <name>` — checkout main, pull, create feature branch
- `/new-silver <source>` — scaffold silver transform for a source
- `/health-digest` — summarize DuckDB silver table freshness

Agents (`.claude/agents/`):
- `code-simplifier` — clean up generated code after a feature
- `build-validator` — validate Databricks bundle completeness
- `medallion-reviewer` — review a new source against architecture standards
- `code-reviewer` — PR review against conventions and PoC quality standards
- `test-writer` — write dbt schema tests and pytest scaffolding
- `yaml-config-writer` — generate YAML config for a new data source
- `silver-transform-writer` — write complete silver SQL MERGE transforms
- `gold-view-writer` — build gold layer views from silver tables
- `data-quality-checker` — freshness, row count, null rate, anomaly checks
- `documentation-writer` — write/update docs and architecture decisions
- `poc-presenter` — prepare Pandora PoC demo materials
- `security-reviewer` — check secrets, data handling, code safety

## Databricks Skills

Skills-base-sti: `/Users/clauseduardpetraeus/.ai-dev-kit/repo/databricks-skills/`

Load disse skills som kontekst ved relevante opgaver:

| Skill | Sti | Relevans |
|---|---|---|
| Asset Bundles | `databricks-asset-bundles/SKILL.md` | DAB deployment — aktivt i brug |
| Jobs | `databricks-jobs/SKILL.md` | Job scheduling og workflows |
| Unity Catalog | `databricks-unity-catalog/SKILL.md` | UC governance og catalog |
| DBSQL | `databricks-dbsql/SKILL.md` | SQL warehouse queries |
| Config | `databricks-config/SKILL.md` | Authentication og profile setup |
| Metric Views | `databricks-metric-views/SKILL.md` | Metrics og BI-lag (gold) |
| AIBI Dashboards | `databricks-aibi-dashboards/SKILL.md` | Dashboards og visualisering |
| Docs | `databricks-docs/SKILL.md` | Doc lookup via MCP |

MCP-server konfigureret i `.mcp.json` (projektniveau). Tools tilgængelige som `mcp__databricks__*`.

## Usage Optimization
- **Be concise**: Provide brief explanations and avoid repeating project context unless asked.
- **Targeted Reading**: Only read files in `docs/` if the user's request requires specific architectural or path knowledge.
- **Ignore Legacy**: Never index or read the `archive/legacy_on_premise_dw/` directory.