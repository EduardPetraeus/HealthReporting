-- =============================================================================
-- dim_health_zone.sql
-- Gold dimension: pre-computed health zones for score classification
--
-- Target: gold.dim_health_zone (DuckDB local)
-- Materialization: CREATE TABLE (static reference data)
-- Source: _business_rules.yml thresholds + metric YAML thresholds
--
-- Boundary convention:
--   score_min is INCLUSIVE (>=), score_max is EXCLUSIVE (<)
--   NULL means unbounded in that direction
--   Join pattern: score >= COALESCE(score_min, -9999) AND score < COALESCE(score_max, 9999)
--
-- Output columns:
--   sk_health_zone  INTEGER  — surrogate key (auto-increment)
--   zone_type       VARCHAR  — metric being classified
--   zone_label      VARCHAR  — human-readable zone name
--   score_min       DOUBLE   — inclusive lower bound (NULL = no lower bound)
--   score_max       DOUBLE   — exclusive upper bound (NULL = no upper bound)
--   zone_color      VARCHAR  — visualization color
--   zone_severity   INTEGER  — 1=best, 4=worst
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_health_zone AS

SELECT * FROM (VALUES
    -- Sleep score zones (from sleep_score.yml)
    ( 1, 'sleep_score',      'excellent', 85,   NULL, 'green',  1),
    ( 2, 'sleep_score',      'good',      70,   85,   'yellow', 2),
    ( 3, 'sleep_score',      'fair',      55,   70,   'orange', 3),
    ( 4, 'sleep_score',      'poor',      NULL, 55,   'red',    4),

    -- Readiness score zones (from readiness_score.yml)
    ( 5, 'readiness_score',  'excellent', 85,   NULL, 'green',  1),
    ( 6, 'readiness_score',  'good',      70,   85,   'yellow', 2),
    ( 7, 'readiness_score',  'fair',      55,   70,   'orange', 3),
    ( 8, 'readiness_score',  'poor',      NULL, 55,   'red',    4),

    -- Activity score zones (from activity_score.yml)
    ( 9, 'activity_score',   'excellent', 85,   NULL, 'green',  1),
    (10, 'activity_score',   'good',      70,   85,   'yellow', 2),
    (11, 'activity_score',   'fair',      55,   70,   'orange', 3),
    (12, 'activity_score',   'poor',      NULL, 55,   'red',    4),

    -- Resting heart rate zones (from resting_heart_rate.yml)
    (13, 'resting_hr',       'athletic',  40,   56,   'green',  1),
    (14, 'resting_hr',       'normal',    56,   71,   'yellow', 2),
    (15, 'resting_hr',       'elevated',  71,   86,   'orange', 3),
    (16, 'resting_hr',       'high',      86,   NULL, 'red',    4),

    -- Composite health score zones (from _business_rules.yml)
    (17, 'composite_health', 'excellent', 85,   NULL, 'green',  1),
    (18, 'composite_health', 'good',      70,   85,   'yellow', 2),
    (19, 'composite_health', 'fair',      55,   70,   'orange', 3),
    (20, 'composite_health', 'poor',      NULL, 55,   'red',    4),

    -- Blood pressure zones (systolic, from blood_pressure.yml)
    (21, 'blood_pressure',   'optimal',   NULL, 120,  'green',  1),
    (22, 'blood_pressure',   'elevated',  120,  130,  'yellow', 2),
    (23, 'blood_pressure',   'high_stage1', 130, 140, 'orange', 3),
    (24, 'blood_pressure',   'high_stage2', 140, NULL, 'red',   4),

    -- SpO2 zones (from blood_oxygen.yml)
    (25, 'spo2',             'normal',    95,   NULL, 'green',  1),
    (26, 'spo2',             'low',       90,   95,   'orange', 3),
    (27, 'spo2',             'critical',  NULL, 90,   'red',    4),

    -- Steps zones (from steps.yml)
    (28, 'steps',            'excellent', 10000, NULL, 'green', 1),
    (29, 'steps',            'good',      7000,  10000, 'yellow', 2),
    (30, 'steps',            'fair',      5000,  7000, 'orange', 3),
    (31, 'steps',            'poor',      NULL,  5000, 'red',    4),

    -- Water intake zones in ml (from water_intake.yml)
    (32, 'water_intake',     'excellent', 2000, NULL, 'green',  1),
    (33, 'water_intake',     'good',      1500, 2000, 'yellow', 2),
    (34, 'water_intake',     'poor',      NULL, 1500, 'red',    4),

    -- Calories zones (from calories.yml)
    (35, 'calories',         'high',      3000, NULL, 'orange', 3),
    (36, 'calories',         'moderate',  2000, 3000, 'green',  1),
    (37, 'calories',         'low',       1200, 2000, 'yellow', 2),
    (38, 'calories',         'very_low',  NULL, 1200, 'red',    4),

    -- Protein zones in grams (from protein.yml)
    (39, 'protein',          'excellent', 130,  NULL, 'green',  1),
    (40, 'protein',          'good',      100,  130,  'yellow', 2),
    (41, 'protein',          'low',       60,   100,  'orange', 3),
    (42, 'protein',          'very_low',  NULL, 60,   'red',    4)

) AS t(sk_health_zone, zone_type, zone_label, score_min, score_max, zone_color, zone_severity);
