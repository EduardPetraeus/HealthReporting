-- =============================================================================
-- fct_sleep_session.sql
-- Gold fact: sleep session details with contributor scores
--
-- Target: gold.fct_sleep_session (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per sleep session (one per day from Oura)
-- Ported from: databricks/gold/sql/fct_sleep_session.sql
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_sleep_session AS

SELECT
    sk_date,
    day,
    sleep_score,
    deep_sleep                                          AS deep_sleep_score,
    rem_sleep                                           AS rem_sleep_score,
    efficiency                                          AS efficiency_score,
    latency                                             AS latency_score,
    restfulness                                         AS restfulness_score,
    timing                                              AS timing_score,
    total_sleep                                         AS total_sleep_score,
    ROUND(
        (COALESCE(deep_sleep, 0) +
         COALESCE(rem_sleep, 0) +
         COALESCE(efficiency, 0) +
         COALESCE(latency, 0) +
         COALESCE(restfulness, 0) +
         COALESCE(timing, 0) +
         COALESCE(total_sleep, 0)
        ) / NULLIF(
            (CASE WHEN deep_sleep IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN rem_sleep IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN efficiency IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN latency IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN restfulness IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN timing IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN total_sleep IS NOT NULL THEN 1 ELSE 0 END
            ), 0),
        1
    )                                                   AS avg_contributor,
    CASE
        WHEN deep_sleep IS NULL AND rem_sleep IS NULL AND efficiency IS NULL
             AND latency IS NULL AND restfulness IS NULL AND timing IS NULL
             AND total_sleep IS NULL THEN NULL
        ELSE CASE LEAST(
                COALESCE(deep_sleep, 999),
                COALESCE(rem_sleep, 999),
                COALESCE(efficiency, 999),
                COALESCE(latency, 999),
                COALESCE(restfulness, 999),
                COALESCE(timing, 999),
                COALESCE(total_sleep, 999)
            )
            WHEN deep_sleep   THEN 'deep_sleep'
            WHEN rem_sleep    THEN 'rem_sleep'
            WHEN efficiency   THEN 'efficiency'
            WHEN latency      THEN 'latency'
            WHEN restfulness  THEN 'restfulness'
            WHEN timing       THEN 'timing'
            WHEN total_sleep  THEN 'total_sleep'
            ELSE NULL
        END
    END                                                 AS weakest_contributor,
    NULLIF(
        LEAST(
            COALESCE(deep_sleep, 999),
            COALESCE(rem_sleep, 999),
            COALESCE(efficiency, 999),
            COALESCE(latency, 999),
            COALESCE(restfulness, 999),
            COALESCE(timing, 999),
            COALESCE(total_sleep, 999)
        ),
        999
    )                                                   AS weakest_score

FROM silver.daily_sleep

ORDER BY day DESC
