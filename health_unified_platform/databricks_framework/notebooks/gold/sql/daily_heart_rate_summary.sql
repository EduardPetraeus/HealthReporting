-- =============================================================================
-- daily_heart_rate_summary.sql
-- Gold view: daily heart rate statistics per source and combined
--
-- Template variables:
--   {target}  — health_dw.gold.daily_heart_rate_summary
--
-- Output columns:
--   day                DATE    — calendar day of readings
--   source_system      STRING  — apple_health | oura | all (cross-source rollup)
--   reading_count      BIGINT  — number of heart rate readings that day
--   avg_bpm            DOUBLE  — average heart rate
--   min_bpm            DOUBLE  — minimum heart rate (resting proxy)
--   max_bpm            DOUBLE  — peak heart rate
--   p50_bpm            DOUBLE  — median heart rate
--   p95_bpm            DOUBLE  — 95th-percentile heart rate (effort indicator)
-- =============================================================================

CREATE OR REPLACE VIEW {target} AS

WITH per_source AS (
    SELECT
        CAST(recorded_at AS DATE)             AS day,
        source_system,
        COUNT(*)                              AS reading_count,
        ROUND(AVG(heart_rate_bpm), 1)         AS avg_bpm,
        ROUND(MIN(heart_rate_bpm), 1)         AS min_bpm,
        ROUND(MAX(heart_rate_bpm), 1)         AS max_bpm,
        ROUND(PERCENTILE(heart_rate_bpm, 0.5), 1)  AS p50_bpm,
        ROUND(PERCENTILE(heart_rate_bpm, 0.95), 1) AS p95_bpm
    FROM health_dw.silver.heart_rate
    WHERE heart_rate_bpm > 0
    GROUP BY 1, 2
),

cross_source AS (
    SELECT
        CAST(recorded_at AS DATE)             AS day,
        'all'                                 AS source_system,
        COUNT(*)                              AS reading_count,
        ROUND(AVG(heart_rate_bpm), 1)         AS avg_bpm,
        ROUND(MIN(heart_rate_bpm), 1)         AS min_bpm,
        ROUND(MAX(heart_rate_bpm), 1)         AS max_bpm,
        ROUND(PERCENTILE(heart_rate_bpm, 0.5), 1)  AS p50_bpm,
        ROUND(PERCENTILE(heart_rate_bpm, 0.95), 1) AS p95_bpm
    FROM health_dw.silver.heart_rate
    WHERE heart_rate_bpm > 0
    GROUP BY 1
)

SELECT * FROM per_source
UNION ALL
SELECT * FROM cross_source
ORDER BY day DESC, source_system
