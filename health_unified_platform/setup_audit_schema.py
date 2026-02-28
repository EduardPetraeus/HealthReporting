"""
setup_audit_schema.py — One-time DuckDB audit schema setup.

Creates the audit schema and tables (job_runs, table_runs) plus
the v_platform_overview view in the local DuckDB database.

Usage:
    HEALTH_ENV=dev python health_unified_platform/setup_audit_schema.py
"""

import os
import sys
from pathlib import Path

import duckdb
import yaml


def load_config() -> dict:
    config_path = (
        Path(__file__).resolve().parent
        / "health_environment"
        / "config"
        / "environment_config.yaml"
    )
    if not config_path.exists():
        sys.exit(f"Config not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_db_path(config: dict) -> str:
    env = os.environ.get("HEALTH_ENV", config["defaults"]["environment"])
    db_name = config["defaults"]["database_name"]
    db_root = config["paths"]["db_root"]
    return os.path.join(db_root, f"{db_name}_{env}.db")


DDL = """
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.job_runs (
    job_id           VARCHAR PRIMARY KEY,
    job_name         VARCHAR NOT NULL,
    job_type         VARCHAR NOT NULL,
    source_system    VARCHAR,
    env              VARCHAR,
    start_time       TIMESTAMPTZ,
    end_time         TIMESTAMPTZ,
    duration_seconds DOUBLE,
    status           VARCHAR DEFAULT 'running',
    error_message    VARCHAR,
    rows_processed   BIGINT,
    rows_inserted    BIGINT,
    rows_updated     BIGINT,
    rows_deleted     BIGINT
);

CREATE TABLE IF NOT EXISTS audit.table_runs (
    run_id        VARCHAR PRIMARY KEY,
    job_id        VARCHAR REFERENCES audit.job_runs(job_id),
    table_name    VARCHAR NOT NULL,
    layer         VARCHAR,
    operation     VARCHAR,
    rows_before   BIGINT,
    rows_after    BIGINT,
    rows_changed  BIGINT,
    start_time    TIMESTAMPTZ,
    end_time      TIMESTAMPTZ,
    status        VARCHAR,
    error_message VARCHAR
);

CREATE OR REPLACE VIEW audit.v_platform_overview AS
WITH last_success AS (
    SELECT
        source_system,
        job_type,
        MAX(end_time) AS last_successful_run
    FROM audit.job_runs
    WHERE status = 'success'
    GROUP BY source_system, job_type
),
week_stats AS (
    SELECT
        source_system,
        job_type,
        COUNT(*) FILTER (WHERE status = 'success') AS success_count,
        COUNT(*) FILTER (WHERE status = 'error')   AS error_count,
        COUNT(*)                                    AS total_count,
        SUM(rows_processed)                         AS total_rows_processed
    FROM audit.job_runs
    WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '7 days'
    GROUP BY source_system, job_type
)
SELECT
    ls.source_system,
    ls.job_type,
    ls.last_successful_run,
    COALESCE(ws.success_count, 0)    AS success_7d,
    COALESCE(ws.error_count, 0)      AS error_7d,
    COALESCE(ws.total_count, 0)      AS total_runs_7d,
    ROUND(
        100.0 * COALESCE(ws.success_count, 0) / NULLIF(COALESCE(ws.total_count, 0), 0),
        1
    )                                AS success_rate_pct,
    COALESCE(ws.total_rows_processed, 0) AS total_rows_7d
FROM last_success ls
LEFT JOIN week_stats ws
    ON ls.source_system = ws.source_system
    AND ls.job_type = ws.job_type
ORDER BY ls.source_system, ls.job_type;
"""


def main():
    config = load_config()
    db_path = get_db_path(config)
    print(f"Setting up audit schema in: {db_path}")

    con = duckdb.connect(db_path)
    try:
        for statement in [s.strip() for s in DDL.split(";") if s.strip()]:
            con.execute(statement)
        print("audit.job_runs   — OK")
        print("audit.table_runs — OK")
        print("audit.v_platform_overview — OK")
        print("\nAudit schema ready.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
