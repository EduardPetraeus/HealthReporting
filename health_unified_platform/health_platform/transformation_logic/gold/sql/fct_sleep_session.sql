-- =============================================================================
-- fct_sleep_session.sql
-- Gold fact: sleep session details with contributor scores
--
-- Target: gold.fct_sleep_session (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.daily_sleep
-- Grain: one row per sleep session (one per day from Oura)
--
-- Note: Oura daily_sleep provides contributor scores (0-100) rather than
--       raw sleep stage durations. deep_sleep/rem/efficiency are scores.
--
-- Output columns:
--   sk_date               INTEGER — FK to dim_date
--   day                   DATE    — sleep day (date the sleep belongs to)
--   sleep_score           INTEGER — overall sleep score
--   deep_sleep_score      INTEGER — deep sleep contributor (0-100)
--   rem_sleep_score       INTEGER — REM sleep contributor (0-100)
--   efficiency_score      INTEGER — sleep efficiency contributor (0-100)
--   latency_score         INTEGER — sleep latency contributor (0-100)
--   restfulness_score     INTEGER — restfulness contributor (0-100)
--   timing_score          INTEGER — timing contributor (0-100)
--   total_sleep_score     INTEGER — total sleep contributor (0-100)
--   avg_contributor       DOUBLE  — average of all contributor scores
--   weakest_contributor   VARCHAR — name of the lowest-scoring contributor
--   weakest_score         INTEGER — score of the weakest contributor
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_sleep_session AS

SELECT
    sk_date,
    day,
    sleep_score,
    contributor_deep_sleep                       AS deep_sleep_score,
    contributor_rem_sleep                         AS rem_sleep_score,
    contributor_efficiency                        AS efficiency_score,
    contributor_latency                           AS latency_score,
    contributor_restfulness                       AS restfulness_score,
    contributor_timing                            AS timing_score,
    contributor_total_sleep                       AS total_sleep_score,
    ROUND(
        (COALESCE(contributor_deep_sleep, 0) +
         COALESCE(contributor_rem_sleep, 0) +
         COALESCE(contributor_efficiency, 0) +
         COALESCE(contributor_latency, 0) +
         COALESCE(contributor_restfulness, 0) +
         COALESCE(contributor_timing, 0) +
         COALESCE(contributor_total_sleep, 0)
        ) / NULLIF(
            (CASE WHEN contributor_deep_sleep IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN contributor_rem_sleep IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN contributor_efficiency IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN contributor_latency IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN contributor_restfulness IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN contributor_timing IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN contributor_total_sleep IS NOT NULL THEN 1 ELSE 0 END
            ), 0),
        1
    )                                            AS avg_contributor,
    CASE LEAST(
            COALESCE(contributor_deep_sleep, 999),
            COALESCE(contributor_rem_sleep, 999),
            COALESCE(contributor_efficiency, 999),
            COALESCE(contributor_latency, 999),
            COALESCE(contributor_restfulness, 999),
            COALESCE(contributor_timing, 999),
            COALESCE(contributor_total_sleep, 999)
        )
        WHEN contributor_deep_sleep   THEN 'deep_sleep'
        WHEN contributor_rem_sleep    THEN 'rem_sleep'
        WHEN contributor_efficiency   THEN 'efficiency'
        WHEN contributor_latency      THEN 'latency'
        WHEN contributor_restfulness  THEN 'restfulness'
        WHEN contributor_timing       THEN 'timing'
        WHEN contributor_total_sleep  THEN 'total_sleep'
        ELSE 'unknown'
    END                                          AS weakest_contributor,
    LEAST(
        COALESCE(contributor_deep_sleep, 999),
        COALESCE(contributor_rem_sleep, 999),
        COALESCE(contributor_efficiency, 999),
        COALESCE(contributor_latency, 999),
        COALESCE(contributor_restfulness, 999),
        COALESCE(contributor_timing, 999),
        COALESCE(contributor_total_sleep, 999)
    )                                            AS weakest_score

FROM silver.daily_sleep

ORDER BY day DESC;
