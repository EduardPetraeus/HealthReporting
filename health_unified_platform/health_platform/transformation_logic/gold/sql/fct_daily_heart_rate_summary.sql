-- =============================================================================
-- fct_daily_heart_rate_summary.sql
-- Gold fact: daily heart rate statistics per source and combined
--
-- Target: gold.fct_daily_heart_rate_summary (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.heart_rate, silver.resting_heart_rate
-- Grain: one row per day per source
--
-- Replaces: databricks/gold/sql/daily_heart_rate_summary.sql (DuckDB version)
--
-- Output columns:
--   sk_date        INTEGER — FK to dim_date
--   day            DATE    — calendar day of readings
--   source_name    VARCHAR — source device/app or 'all' for cross-source
--   reading_count  BIGINT  — number of heart rate readings that day
--   avg_bpm        DOUBLE  — average heart rate
--   min_bpm        DOUBLE  — minimum heart rate
--   max_bpm        DOUBLE  — peak heart rate
--   p50_bpm        DOUBLE  — median heart rate
--   p95_bpm        DOUBLE  — 95th-percentile heart rate
--   resting_hr     DOUBLE  — resting heart rate (from dedicated table)
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_daily_heart_rate_summary AS

WITH per_source AS (
    SELECT
        sk_date,
        CAST(timestamp AS DATE)                  AS day,
        source_name,
        COUNT(*)                                 AS reading_count,
        ROUND(AVG(bpm), 1)                       AS avg_bpm,
        ROUND(MIN(bpm), 1)                       AS min_bpm,
        ROUND(MAX(bpm), 1)                       AS max_bpm,
        ROUND(quantile_cont(bpm, 0.5), 1)        AS p50_bpm,
        ROUND(quantile_cont(bpm, 0.95), 1)       AS p95_bpm
    FROM silver.heart_rate
    WHERE bpm > 0
    GROUP BY 1, 2, 3
),

cross_source AS (
    SELECT
        sk_date,
        CAST(timestamp AS DATE)                  AS day,
        'all'                                    AS source_name,
        COUNT(*)                                 AS reading_count,
        ROUND(AVG(bpm), 1)                       AS avg_bpm,
        ROUND(MIN(bpm), 1)                       AS min_bpm,
        ROUND(MAX(bpm), 1)                       AS max_bpm,
        ROUND(quantile_cont(bpm, 0.5), 1)        AS p50_bpm,
        ROUND(quantile_cont(bpm, 0.95), 1)       AS p95_bpm
    FROM silver.heart_rate
    WHERE bpm > 0
    GROUP BY 1, 2
),

combined AS (
    SELECT * FROM per_source
    UNION ALL
    SELECT * FROM cross_source
)

SELECT
    c.sk_date,
    c.day,
    c.source_name,
    c.reading_count,
    c.avg_bpm,
    c.min_bpm,
    c.max_bpm,
    c.p50_bpm,
    c.p95_bpm,
    rhr.resting_hr_bpm                           AS resting_hr

FROM combined c
LEFT JOIN silver.resting_heart_rate rhr
    ON c.sk_date = rhr.sk_date
    AND (c.source_name = rhr.source_name OR c.source_name = 'all')

ORDER BY c.day DESC, c.source_name;
