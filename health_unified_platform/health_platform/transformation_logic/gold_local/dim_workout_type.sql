-- =============================================================================
-- dim_workout_type.sql
-- Gold dimension: workout activity classification
--
-- Target: gold.dim_workout_type (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (rebuild when new activities appear)
-- Ported from: databricks/gold/sql/dim_workout_type.sql
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_workout_type AS

WITH distinct_activities AS (
    SELECT DISTINCT activity
    FROM silver.workout
    WHERE activity IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY activity)               AS sk_workout_type,
    activity                                            AS activity_raw,
    REPLACE(activity, '_', ' ')                         AS activity_name,
    CASE
        WHEN LOWER(activity) IN ('running', 'cycling', 'swimming', 'rowing', 'elliptical',
                                  'stair_climbing', 'jump_rope', 'hiking', 'walking',
                                  'cross_country_skiing', 'indoor_cycling', 'treadmill')
            THEN 'cardio'
        WHEN LOWER(activity) IN ('strength_training', 'weight_training', 'functional_training',
                                  'traditional_strength_training', 'core_training',
                                  'high_intensity_interval_training')
            THEN 'strength'
        WHEN LOWER(activity) IN ('yoga', 'pilates', 'stretching', 'flexibility')
            THEN 'flexibility'
        WHEN LOWER(activity) IN ('meditation', 'breathing', 'cooldown', 'recovery')
            THEN 'recovery'
        WHEN LOWER(activity) IN ('crossfit', 'circuit_training', 'bootcamp')
            THEN 'mixed'
        ELSE 'other'
    END                                                 AS activity_category,
    LOWER(activity) IN ('running', 'cycling', 'swimming', 'rowing', 'elliptical',
                         'stair_climbing', 'jump_rope', 'hiking', 'walking',
                         'cross_country_skiing', 'indoor_cycling', 'treadmill',
                         'crossfit', 'circuit_training', 'bootcamp',
                         'high_intensity_interval_training')
                                                        AS is_cardio,
    LOWER(activity) IN ('running', 'cycling', 'hiking', 'walking',
                         'cross_country_skiing', 'swimming')
                                                        AS is_outdoor,
    CASE
        WHEN LOWER(activity) IN ('running', 'cycling', 'swimming', 'rowing', 'hiking',
                                  'walking', 'cross_country_skiing', 'elliptical', 'treadmill')
            THEN 'aerobic'
        WHEN LOWER(activity) IN ('strength_training', 'weight_training', 'traditional_strength_training',
                                  'jump_rope', 'stair_climbing')
            THEN 'anaerobic'
        WHEN LOWER(activity) IN ('crossfit', 'circuit_training', 'bootcamp',
                                  'high_intensity_interval_training', 'functional_training')
            THEN 'mixed'
        ELSE 'aerobic'
    END                                                 AS estimated_energy_system

FROM distinct_activities
ORDER BY activity
