-- =============================================================================
-- dim_date.sql
-- Gold dimension: calendar date dimension for all date-grain analysis
--
-- Target: gold.dim_date (DuckDB local)
-- Materialization: CREATE TABLE (static, regenerate periodically)
--
-- Output columns:
--   sk_date        INTEGER  — surrogate key (YYYYMMDD format, matches silver.*.sk_date)
--   date           DATE     — calendar date
--   year           INTEGER  — calendar year
--   quarter        INTEGER  — quarter (1-4)
--   month          INTEGER  — month (1-12)
--   month_name     VARCHAR  — full month name (January, February, ...)
--   iso_week       INTEGER  — ISO week number (1-53)
--   day_of_week    INTEGER  — day of week (1=Monday, 7=Sunday)
--   day_name       VARCHAR  — full day name (Monday, Tuesday, ...)
--   day_of_month   INTEGER  — day within month (1-31)
--   day_of_year    INTEGER  — day within year (1-366)
--   is_weekend     BOOLEAN  — true for Saturday/Sunday
--   day_type       VARCHAR  — 'weekday' or 'weekend'
--   season         VARCHAR  — northern hemisphere season
--   year_month     VARCHAR  — 'YYYY-MM' for grouping
--   days_ago       INTEGER  — days from current_date (0 = today)
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_date AS

WITH date_series AS (
    SELECT UNNEST(generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day'))::DATE AS date
)

SELECT
    CAST(strftime(date, '%Y%m%d') AS INTEGER)   AS sk_date,
    date,
    YEAR(date)                                   AS year,
    QUARTER(date)                                AS quarter,
    MONTH(date)                                  AS month,
    monthname(date)                              AS month_name,
    weekofyear(date)                             AS iso_week,
    isodow(date)                                 AS day_of_week,
    dayname(date)                                AS day_name,
    DAY(date)                                    AS day_of_month,
    dayofyear(date)                              AS day_of_year,
    isodow(date) IN (6, 7)                       AS is_weekend,
    CASE
        WHEN isodow(date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END                                          AS day_type,
    CASE
        WHEN MONTH(date) IN (3, 4, 5)   THEN 'spring'
        WHEN MONTH(date) IN (6, 7, 8)   THEN 'summer'
        WHEN MONTH(date) IN (9, 10, 11) THEN 'autumn'
        ELSE 'winter'
    END                                          AS season,
    strftime(date, '%Y-%m')                      AS year_month,
    DATEDIFF('day', date, current_date)          AS days_ago

FROM date_series
ORDER BY date;
