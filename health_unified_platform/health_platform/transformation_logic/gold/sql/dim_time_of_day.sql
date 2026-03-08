-- =============================================================================
-- dim_time_of_day.sql
-- Gold dimension: time-of-day dimension for intraday analysis
--
-- Target: gold.dim_time_of_day (DuckDB local)
-- Materialization: CREATE TABLE (static, 1440 rows)
--
-- Output columns:
--   sk_time          VARCHAR  — surrogate key ('HHMM' format, matches silver.*.sk_time)
--   hour             INTEGER  — hour (0-23)
--   minute           INTEGER  — minute (0-59)
--   time_label       VARCHAR  — 'HH:MM' display format
--   time_bucket      VARCHAR  — time period bucket
--   is_sleeping_hours BOOLEAN — true for typical sleeping hours (22:00-06:59)
--   is_active_hours  BOOLEAN  — true for typical active hours (07:00-21:59)
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_time_of_day AS

WITH time_series AS (
    SELECT
        h.hour,
        m.minute
    FROM generate_series(0, 23) AS h(hour)
    CROSS JOIN generate_series(0, 59) AS m(minute)
)

SELECT
    LPAD(CAST(hour AS VARCHAR), 2, '0') || LPAD(CAST(minute AS VARCHAR), 2, '0')
                                                 AS sk_time,
    hour,
    minute,
    LPAD(CAST(hour AS VARCHAR), 2, '0') || ':' || LPAD(CAST(minute AS VARCHAR), 2, '0')
                                                 AS time_label,
    CASE
        WHEN hour BETWEEN 0  AND 5  THEN 'night'
        WHEN hour BETWEEN 6  AND 8  THEN 'early_morning'
        WHEN hour BETWEEN 9  AND 11 THEN 'morning'
        WHEN hour BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN hour BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'late_evening'
    END                                          AS time_bucket,
    hour < 7 OR hour >= 22                       AS is_sleeping_hours,
    hour >= 7 AND hour < 22                      AS is_active_hours

FROM time_series
ORDER BY hour, minute;
