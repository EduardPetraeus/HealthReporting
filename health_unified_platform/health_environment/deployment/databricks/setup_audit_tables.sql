-- setup_audit_tables.sql
-- Delta DDL for audit schema in Databricks Unity Catalog.
-- Run once per environment (dev / prd) via the init.py notebook.
--
-- PoC scale-up note:
--   job_runs UPDATE is expensive at high frequency in Delta.
--   Enterprise pattern: append-only + reconciliation view.
--   Native Databricks Job Run API replaces job_runs in full production setup.

CREATE SCHEMA IF NOT EXISTS ${catalog}.audit;

CREATE TABLE IF NOT EXISTS ${catalog}.audit.job_runs (
    job_id           STRING  NOT NULL,
    job_name         STRING  NOT NULL,
    job_type         STRING  NOT NULL,
    source_system    STRING,
    env              STRING,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    duration_seconds DOUBLE,
    status           STRING,
    error_message    STRING,
    rows_processed   LONG,
    rows_inserted    LONG,
    rows_updated     LONG,
    rows_deleted     LONG
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'false'
);

CREATE TABLE IF NOT EXISTS ${catalog}.audit.table_runs (
    run_id        STRING NOT NULL,
    job_id        STRING,
    table_name    STRING NOT NULL,
    layer         STRING,
    operation     STRING,
    rows_before   LONG,
    rows_after    LONG,
    rows_changed  LONG,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    status        STRING,
    error_message STRING
)
USING DELTA;

CREATE OR REPLACE VIEW ${catalog}.audit.v_platform_overview AS
WITH last_success AS (
    SELECT
        source_system,
        job_type,
        MAX(end_time) AS last_successful_run
    FROM ${catalog}.audit.job_runs
    WHERE status = 'success'
    GROUP BY source_system, job_type
),
week_stats AS (
    SELECT
        source_system,
        job_type,
        COUNT_IF(status = 'success')   AS success_count,
        COUNT_IF(status = 'error')     AS error_count,
        COUNT(*)                       AS total_count,
        SUM(rows_processed)            AS total_rows_processed
    FROM ${catalog}.audit.job_runs
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY source_system, job_type
)
SELECT
    ls.source_system,
    ls.job_type,
    ls.last_successful_run,
    COALESCE(ws.success_count, 0)   AS success_7d,
    COALESCE(ws.error_count, 0)     AS error_7d,
    COALESCE(ws.total_count, 0)     AS total_runs_7d,
    ROUND(
        100.0 * COALESCE(ws.success_count, 0) / NULLIF(COALESCE(ws.total_count, 0), 0),
        1
    )                               AS success_rate_pct,
    COALESCE(ws.total_rows_processed, 0) AS total_rows_7d
FROM last_success ls
LEFT JOIN week_stats ws
    ON ls.source_system = ws.source_system
    AND ls.job_type = ws.job_type
ORDER BY ls.source_system, ls.job_type;
