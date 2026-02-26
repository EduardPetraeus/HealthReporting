# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Personal health data platform (owner: Claus Eduard Petraeus) that ingests, transforms, and surfaces data from wearable devices and nutrition apps. Uses a **medallion architecture** (bronze -> silver -> gold) with a local-first approach on DuckDB, targeting Databricks as the cloud platform.

## Environment Separation

`HEALTH_ENV` env var controls dev vs prd. The DuckDB file is named `health_dw_{env}.db`. The Databricks catalog is `health_dw` with schemas: bronze, silver, gold.

## Conventions

- **snake_case** for all names (tables, columns, variables, files)
- **All code in English** (variable names, comments, docstrings, print messages)
- Prefix bronze tables with `stg_`
- Individual files over monolithic scripts
- Tech-agnostic: local logic uses DuckDB/Python; cloud logic uses Databricks SQL/PySpark
- Metadata-driven via YAML — adding a new source = adding a YAML entry, no code changes

## Tooling

- **Databricks MCP server** configured in `.mcp.json` for Claude Code integration
- **VS Code** with DuckDB file viewer for parquet/csv and SQL Tools connection
- Python 3.9 venv (`.venv/`) — key packages: `duckdb`, `pandas`, `pyyaml`, `dbt-core`, `dbt-duckdb`
- No CI/CD, no test framework, no linter currently configured

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

Feature branches auto-deploy to Databricks dev on push (once secrets are configured).

## Productivity Tracking

After every git commit, automatically run:

```bash
.venv/bin/python update_productivity.py
```

This updates `claude_code_productivity.json` with git-derived metrics (files changed, lines added/removed, refactor ratio, code density, commit hour, time between commits, streak days, task type). Never ask the user to do this manually — it is always Claude Code's responsibility.

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

## Usage Optimization
- **Be concise**: Provide brief explanations and avoid repeating project context unless asked.
- **Targeted Reading**: Only read files in `docs/` if the user's request requires specific architectural or path knowledge.
- **Ignore Legacy**: Never index or read the `archive/legacy_on_premise_dw/` directory.