-- =============================================================================
-- dim_metric.sql
-- Gold dimension: master metric catalog for cross-domain analysis
--
-- Target: gold.dim_metric (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (static, derived from semantic contracts)
-- Ported from: databricks/gold/sql/dim_metric.sql
--
-- DuckDB change: source_table references use silver.TABLE instead of
-- health_dw.silver.TABLE
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_metric (
    sk_metric            INTEGER,
    metric_name          VARCHAR,
    display_name         VARCHAR,
    category             VARCHAR,
    unit                 VARCHAR,
    source_table         VARCHAR,
    grain                VARCHAR,
    body_system_code     VARCHAR,
    threshold_optimal_min DOUBLE,
    threshold_optimal_max DOUBLE
);

INSERT INTO gold.dim_metric
VALUES
    -- Sleep metrics
    ( 1, 'sleep_score',           'Sleep Score',            'sleep',       'score',   'silver.daily_sleep',        'daily',           'sleep_circadian',  85,    NULL),
    ( 2, 'deep_sleep',            'Deep Sleep Contributor', 'sleep',       'score',   'silver.daily_sleep',        'daily',           'sleep_circadian',  NULL,  NULL),
    ( 3, 'sleep_efficiency',      'Sleep Efficiency',       'sleep',       'score',   'silver.daily_sleep',        'daily',           'sleep_circadian',  NULL,  NULL),
    ( 4, 'sleep_latency',         'Sleep Latency',          'sleep',       'score',   'silver.daily_sleep',        'daily',           'sleep_circadian',  NULL,  NULL),
    ( 5, 'rem_sleep',             'REM Sleep Contributor',  'sleep',       'score',   'silver.daily_sleep',        'daily',           'sleep_circadian',  NULL,  NULL),
    ( 6, 'total_sleep',           'Total Sleep',            'sleep',       'score',   'silver.daily_sleep',        'daily',           'sleep_circadian',  NULL,  NULL),
    -- Vitals metrics
    ( 7, 'resting_heart_rate',    'Resting Heart Rate',     'vitals',      'bpm',     'silver.resting_heart_rate', 'daily',           'cardiovascular',   40,    55),
    ( 8, 'heart_rate',            'Heart Rate',             'vitals',      'bpm',     'silver.heart_rate',         'per_measurement', 'cardiovascular',   NULL,  NULL),
    ( 9, 'blood_oxygen',          'Blood Oxygen (SpO2)',    'vitals',      '%',       'silver.blood_oxygen',       'per_measurement', 'respiratory',      95,    NULL),
    (10, 'blood_pressure_sys',    'Systolic BP',            'vitals',      'mmHg',    'silver.blood_pressure',     'per_measurement', 'cardiovascular',   NULL,  119),
    (11, 'blood_pressure_dia',    'Diastolic BP',           'vitals',      'mmHg',    'silver.blood_pressure',     'per_measurement', 'cardiovascular',   NULL,  79),
    (12, 'body_temperature',      'Body Temperature',       'vitals',      'C',       'silver.body_temperature',   'per_measurement', 'immune',           NULL,  NULL),
    (13, 'respiratory_rate',      'Respiratory Rate',       'vitals',      'brpm',    'silver.respiratory_rate',   'daily',           'respiratory',      NULL,  NULL),
    -- Activity metrics
    (14, 'steps',                 'Daily Steps',            'activity',    'steps',   'silver.daily_activity',     'daily',           'musculoskeletal',  10000, NULL),
    (15, 'active_calories',       'Active Calories',        'activity',    'kcal',    'silver.daily_activity',     'daily',           'metabolic',        NULL,  NULL),
    (16, 'activity_score',        'Activity Score',         'activity',    'score',   'silver.daily_activity',     'daily',           'musculoskeletal',  85,    NULL),
    (17, 'workout_duration',      'Workout Duration',       'activity',    'seconds', 'silver.workout',            'per_workout',     'musculoskeletal',  NULL,  NULL),
    -- Recovery metrics
    (18, 'readiness_score',       'Readiness Score',        'recovery',    'score',   'silver.daily_readiness',    'daily',           'recovery',         85,    NULL),
    (19, 'stress_level',          'Stress Level',           'recovery',    'score',   'silver.daily_stress',       'daily',           'mental',           NULL,  NULL),
    -- Body composition
    (20, 'weight',                'Weight',                 'body',        'kg',      'silver.weight',             'per_measurement', 'body_composition', NULL,  NULL),
    (21, 'fat_mass',              'Fat Mass',               'body',        'kg',      'silver.weight',             'per_measurement', 'body_composition', NULL,  NULL),
    (22, 'muscle_mass',           'Muscle Mass',            'body',        'kg',      'silver.weight',             'per_measurement', 'body_composition', NULL,  NULL),
    -- Nutrition metrics
    (23, 'calories_intake',       'Calorie Intake',         'nutrition',   'kcal',    'silver.daily_meal',         'daily',           'metabolic',        2000,  2999),
    (24, 'protein',               'Protein Intake',         'nutrition',   'g',       'silver.daily_meal',         'daily',           'metabolic',        130,   NULL),
    (25, 'water_intake',          'Water Intake',           'nutrition',   'ml',      'silver.water_intake',       'daily',           'metabolic',        2000,  NULL),
    -- Other metrics
    (26, 'mindful_minutes',       'Mindful Minutes',        'mindfulness', 'minutes', 'silver.mindful_session',    'daily',           'mental',           NULL,  NULL),
    (27, 'walking_speed',         'Walking Speed',          'gait',        'km/h',    'silver.daily_walking_gait', 'daily',           'locomotion',       NULL,  NULL),
    (28, 'toothbrushing_duration','Toothbrushing Duration', 'hygiene',     'seconds', 'silver.toothbrushing',      'per_measurement', 'oral_health',      NULL,  NULL)
