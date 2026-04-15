# ADR-012 — Audit Schema as Observability Layer in DuckDB
Status: ACCEPTED
Date: 2026-03-26
Decision-maker: Architect Brain (Tier 2), cleared 2026-03-26

## Context
HealthReporting lacked a binding between pipeline executions and individual job-runs.
`audit.job_runs` and `audit.table_runs` existed and were actively used, but were not
linked to an overarching pipeline execution. Steps failed silently and there was no
human-readable overview of what ran when.

## Decision
`pipeline_run_id` (UUID) is added to `audit.job_runs`.
`daily_sync.sh` generates the UUID at startup and exports it to all steps.
All scripts using `AuditLogger` receive `pipeline_run_id` via
`os.environ.get('PIPELINE_RUN_ID')` — NULL for manual/ad-hoc runs.
Row counts are added to `run_merge.py` via COUNT before/after MERGE.

## Phase 1 scope
Only the existing audit infrastructure is extended.
No existing DDL on bronze/silver/gold tables is changed.
`_pipeline_run_id_insert/_update` columns on data tables are Phase 2
(coordinated with PR #209).

## Consequences
- All pipeline executions are queryable as a single unit
- Failures are traceable from pipeline level down to table level
- Foundation for Phase 2: traceability to individual data rows
- Foundation for pipeline monitoring dashboard (OG-22)

## Alternatives considered
- Separate `pipeline_runs` table: rejected — existing `job_runs` covers the need
- Separate `error_log` table: rejected — inline `error_message` is sufficient
