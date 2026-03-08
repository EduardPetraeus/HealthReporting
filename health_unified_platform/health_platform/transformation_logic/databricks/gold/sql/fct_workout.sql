-- =============================================================================
-- fct_workout.sql
-- Gold fact: individual workout sessions with dimension FKs
--
-- Target: health_dw.gold.fct_workout (Databricks)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per workout session
-- =============================================================================

CREATE OR REPLACE VIEW {target} AS

SELECT
    w.sk_date,
    LPAD(CAST(HOUR(w.start_datetime) AS STRING), 2, '0') ||
    LPAD(CAST(MINUTE(w.start_datetime) AS STRING), 2, '0')
                                                        AS sk_time,
    w.day,
    w.workout_id,
    w.activity,
    w.intensity,
    w.duration_seconds,
    ROUND(w.duration_seconds / 60.0, 1)                 AS duration_minutes,
    w.calories,
    w.distance_meters,
    ROUND(w.distance_meters / 1000.0, 2)                AS distance_km,
    w.start_datetime,
    w.end_datetime,
    w.source,
    w.label

FROM health_dw.silver.workout w

ORDER BY w.day DESC, w.start_datetime DESC
