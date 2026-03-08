-- =============================================================================
-- dim_health_zone.sql
-- Gold dimension: pre-computed health zones for score classification
--
-- Target: gold.dim_health_zone (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (static reference data)
-- Ported from: databricks/gold/sql/dim_health_zone.sql
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_health_zone (
    sk_health_zone  INTEGER,
    zone_type       VARCHAR,
    zone_label      VARCHAR,
    score_min       DOUBLE,
    score_max       DOUBLE,
    zone_color      VARCHAR,
    zone_severity   INTEGER
);

INSERT INTO gold.dim_health_zone
VALUES
    -- Sleep score zones
    ( 1, 'sleep_score',      'excellent', 85,   NULL, 'green',  1),
    ( 2, 'sleep_score',      'good',      70,   85,   'yellow', 2),
    ( 3, 'sleep_score',      'fair',      55,   70,   'orange', 3),
    ( 4, 'sleep_score',      'poor',      NULL, 55,   'red',    4),
    -- Readiness score zones
    ( 5, 'readiness_score',  'excellent', 85,   NULL, 'green',  1),
    ( 6, 'readiness_score',  'good',      70,   85,   'yellow', 2),
    ( 7, 'readiness_score',  'fair',      55,   70,   'orange', 3),
    ( 8, 'readiness_score',  'poor',      NULL, 55,   'red',    4),
    -- Activity score zones
    ( 9, 'activity_score',   'excellent', 85,   NULL, 'green',  1),
    (10, 'activity_score',   'good',      70,   85,   'yellow', 2),
    (11, 'activity_score',   'fair',      55,   70,   'orange', 3),
    (12, 'activity_score',   'poor',      NULL, 55,   'red',    4),
    -- Resting heart rate zones
    (13, 'resting_hr',       'athletic',  40,   56,   'green',  1),
    (14, 'resting_hr',       'normal',    56,   71,   'yellow', 2),
    (15, 'resting_hr',       'elevated',  71,   86,   'orange', 3),
    (16, 'resting_hr',       'high',      86,   NULL, 'red',    4),
    -- Composite health score zones
    (17, 'composite_health', 'excellent', 85,   NULL, 'green',  1),
    (18, 'composite_health', 'good',      70,   85,   'yellow', 2),
    (19, 'composite_health', 'fair',      55,   70,   'orange', 3),
    (20, 'composite_health', 'poor',      NULL, 55,   'red',    4),
    -- Blood pressure zones (systolic)
    (21, 'blood_pressure',   'optimal',   NULL, 120,  'green',  1),
    (22, 'blood_pressure',   'elevated',  120,  130,  'yellow', 2),
    (23, 'blood_pressure',   'high_stage1', 130, 140, 'orange', 3),
    (24, 'blood_pressure',   'high_stage2', 140, NULL, 'red',   4),
    -- SpO2 zones
    (25, 'spo2',             'normal',    95,   NULL, 'green',  1),
    (26, 'spo2',             'low',       90,   95,   'orange', 3),
    (27, 'spo2',             'critical',  NULL, 90,   'red',    4),
    -- Steps zones
    (28, 'steps',            'excellent', 10000, NULL, 'green', 1),
    (29, 'steps',            'good',      7000,  10000, 'yellow', 2),
    (30, 'steps',            'fair',      5000,  7000, 'orange', 3),
    (31, 'steps',            'poor',      NULL,  5000, 'red',    4),
    -- Water intake zones in ml
    (32, 'water_intake',     'excellent', 2000, NULL, 'green',  1),
    (33, 'water_intake',     'good',      1500, 2000, 'yellow', 2),
    (34, 'water_intake',     'poor',      NULL, 1500, 'red',    4),
    -- Calories zones
    (35, 'calories',         'high',      3000, NULL, 'orange', 3),
    (36, 'calories',         'moderate',  2000, 3000, 'green',  1),
    (37, 'calories',         'low',       1200, 2000, 'yellow', 2),
    (38, 'calories',         'very_low',  NULL, 1200, 'red',    4),
    -- Protein zones in grams
    (39, 'protein',          'excellent', 130,  NULL, 'green',  1),
    (40, 'protein',          'good',      100,  130,  'yellow', 2),
    (41, 'protein',          'low',       60,   100,  'orange', 3),
    (42, 'protein',          'very_low',  NULL, 60,   'red',    4)
