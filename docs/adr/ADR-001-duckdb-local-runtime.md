# ADR-001: DuckDB as Local Runtime

## Status
Accepted

## Date
2026-02-28

## Context
The platform needs a local database for development and testing before cloud deployment. Options evaluated: SQLite, PostgreSQL (local), DuckDB.

## Decision
Use DuckDB as the local runtime. Use Databricks (Unity Catalog) as the cloud target.

## Consequences
- All SQL must be DuckDB-compatible locally (ANSI SQL + DuckDB extensions)
- Databricks SQL may require minor syntax adjustments at deploy time
- Local path: `health_dw_{env}.db` — controlled by `HEALTH_ENV` env var
- Only one writer at a time — VS Code SQL Tools must be closed before running scripts
- Parquet files are the interchange format between local and cloud

## Alternatives Considered
- **SQLite**: No columnar storage, poor analytical performance, no Parquet support
- **PostgreSQL local**: Requires running a server, heavyweight for dev, poor Parquet integration
- **DuckDB**: Zero-config, columnar, OLAP-optimized, native Parquet read/write, Python-native

## Agent Rule
Agents must not suggest replacing DuckDB with another local runtime without opening a new ADR and getting explicit human approval.
