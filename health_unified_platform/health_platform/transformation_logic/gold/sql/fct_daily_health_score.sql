-- =============================================================================
-- fct_daily_health_score.sql
-- Gold fact: composite daily health score combining sleep, readiness, activity
--
-- Target: gold.fct_daily_health_score (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.daily_sleep, silver.daily_readiness, silver.daily_activity,
--         silver.daily_stress, silver.workout
-- Grain: one row per day
-- Weights: sleep 0.35, readiness 0.35, activity 0.30 (from _business_rules.yml)
--
-- Output columns:
--   sk_date                   INTEGER — FK to dim_date
--   day                       DATE    — calendar day
--   sleep_score               INTEGER — Oura sleep score
--   readiness_score           INTEGER — Oura readiness score
--   activity_score            INTEGER — Oura activity score
--   composite_health_score    DOUBLE  — weighted composite (35/35/30)
--   health_status             VARCHAR — excellent/good/fair/poor
--   steps                     INTEGER — daily step count
--   total_calories            INTEGER — total calories burned
--   active_calories           INTEGER — active calories burned
--   temperature_deviation     DOUBLE  — body temp deviation from baseline
--   stress_day_summary        VARCHAR — Oura stress summary
--   is_workout_day            BOOLEAN — had at least one workout
--   workout_count             INTEGER — number of workouts that day
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_daily_health_score AS

WITH daily_workouts AS (
    SELECT
        sk_date,
        COUNT(*)             AS workout_count
    FROM silver.workout
    GROUP BY sk_date
)

SELECT
    COALESCE(s.sk_date, r.sk_date, a.sk_date)   AS sk_date,
    COALESCE(s.day, r.day, a.day)                AS day,
    s.sleep_score,
    r.readiness_score,
    a.activity_score,
    ROUND(
        COALESCE(s.sleep_score, 0) * 0.35 +
        COALESCE(r.readiness_score, 0) * 0.35 +
        COALESCE(a.activity_score, 0) * 0.30,
        1
    )                                            AS composite_health_score,
    CASE
        WHEN COALESCE(s.sleep_score, 0) * 0.35 +
             COALESCE(r.readiness_score, 0) * 0.35 +
             COALESCE(a.activity_score, 0) * 0.30 >= 85 THEN 'excellent'
        WHEN COALESCE(s.sleep_score, 0) * 0.35 +
             COALESCE(r.readiness_score, 0) * 0.35 +
             COALESCE(a.activity_score, 0) * 0.30 >= 70 THEN 'good'
        WHEN COALESCE(s.sleep_score, 0) * 0.35 +
             COALESCE(r.readiness_score, 0) * 0.35 +
             COALESCE(a.activity_score, 0) * 0.30 >= 55 THEN 'fair'
        ELSE 'poor'
    END                                          AS health_status,
    a.steps,
    a.total_calories,
    a.active_calories,
    r.temperature_deviation,
    st.day_summary                               AS stress_day_summary,
    w.workout_count IS NOT NULL                  AS is_workout_day,
    COALESCE(w.workout_count, 0)                 AS workout_count

FROM silver.daily_sleep s
FULL OUTER JOIN silver.daily_readiness r
    ON s.sk_date = r.sk_date
FULL OUTER JOIN silver.daily_activity a
    ON COALESCE(s.sk_date, r.sk_date) = a.sk_date
LEFT JOIN silver.daily_stress st
    ON COALESCE(s.sk_date, r.sk_date, a.sk_date) = st.sk_date
LEFT JOIN daily_workouts w
    ON COALESCE(s.sk_date, r.sk_date, a.sk_date) = w.sk_date

ORDER BY day DESC;
