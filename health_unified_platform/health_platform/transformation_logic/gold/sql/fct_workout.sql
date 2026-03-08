-- =============================================================================
-- fct_workout.sql
-- Gold fact: individual workout sessions with dimension FKs
--
-- Target: gold.fct_workout (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.workout
-- Grain: one row per workout session
--
-- Output columns:
--   sk_date            INTEGER  — FK to dim_date
--   sk_time            VARCHAR  — FK to dim_time_of_day (workout start)
--   day                DATE     — workout date
--   workout_id         VARCHAR  — unique workout identifier
--   activity           VARCHAR  — raw activity name (joinable to dim_workout_type)
--   intensity          VARCHAR  — low/medium/high
--   duration_seconds   DOUBLE   — workout duration in seconds
--   duration_minutes   DOUBLE   — workout duration in minutes
--   calories           DOUBLE   — calories burned
--   distance_meters    DOUBLE   — distance covered
--   distance_km        DOUBLE   — distance in kilometers
--   start_datetime     TIMESTAMP— workout start time
--   end_datetime       TIMESTAMP— workout end time
--   source             VARCHAR  — data source
--   label              VARCHAR  — user label
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_workout AS

SELECT
    w.sk_date,
    LPAD(CAST(HOUR(w.start_datetime) AS VARCHAR), 2, '0') ||
    LPAD(CAST(MINUTE(w.start_datetime) AS VARCHAR), 2, '0')
                                                 AS sk_time,
    w.day,
    w.workout_id,
    w.activity,
    w.intensity,
    w.duration_seconds,
    ROUND(w.duration_seconds / 60.0, 1)          AS duration_minutes,
    w.calories,
    w.distance_meters,
    ROUND(w.distance_meters / 1000.0, 2)         AS distance_km,
    w.start_datetime,
    w.end_datetime,
    w.source,
    w.label

FROM silver.workout w

ORDER BY w.day DESC, w.start_datetime DESC;
