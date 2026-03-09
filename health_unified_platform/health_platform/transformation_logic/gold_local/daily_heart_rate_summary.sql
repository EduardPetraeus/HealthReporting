-- =============================================================================
-- daily_heart_rate_summary.sql
-- Gold view: daily heart rate statistics per source and combined
--
-- Target: gold.daily_heart_rate_summary (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW
-- Ported from: databricks/gold/sql/daily_heart_rate_summary.sql
--
-- DuckDB changes:
--   PERCENTILE(col, 0.5) -> QUANTILE_CONT(col, 0.5)
--   Column alignment with local silver schema:
--     heart_rate_bpm -> bpm, recorded_at -> timestamp, source_system -> source_name
-- =============================================================================

CREATE OR REPLACE VIEW gold.daily_heart_rate_summary AS

WITH per_source AS (
    SELECT
        CAST(timestamp AS DATE)               AS day,
        source_name,
        COUNT(*)                              AS reading_count,
        ROUND(AVG(bpm), 1)                   AS avg_bpm,
        ROUND(MIN(bpm), 1)                   AS min_bpm,
        ROUND(MAX(bpm), 1)                   AS max_bpm,
        ROUND(QUANTILE_CONT(bpm, 0.5), 1)   AS p50_bpm,
        ROUND(QUANTILE_CONT(bpm, 0.95), 1)  AS p95_bpm
    FROM silver.heart_rate
    WHERE bpm > 0
    GROUP BY 1, 2
),

cross_source AS (
    SELECT
        CAST(timestamp AS DATE)               AS day,
        'all'                                 AS source_name,
        COUNT(*)                              AS reading_count,
        ROUND(AVG(bpm), 1)                   AS avg_bpm,
        ROUND(MIN(bpm), 1)                   AS min_bpm,
        ROUND(MAX(bpm), 1)                   AS max_bpm,
        ROUND(QUANTILE_CONT(bpm, 0.5), 1)   AS p50_bpm,
        ROUND(QUANTILE_CONT(bpm, 0.95), 1)  AS p95_bpm
    FROM silver.heart_rate
    WHERE bpm > 0
    GROUP BY 1
)

SELECT * FROM per_source
UNION ALL
SELECT * FROM cross_source
ORDER BY day DESC, source_name
