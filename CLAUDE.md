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


## Usage Optimization
- **Be concise**: Provide brief explanations and avoid repeating project context unless asked.
- **Targeted Reading**: Only read files in `docs/` if the user's request requires specific architectural or path knowledge.
- **Ignore Legacy**: Never index or read the `legacy_on_premise_dw/` directory.