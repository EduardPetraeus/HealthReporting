-- =============================================================================
-- dim_body_system.sql
-- Gold dimension: body system classification for cross-domain analysis
--
-- Target: gold.dim_body_system (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (static reference data)
-- Ported from: databricks/gold/sql/dim_body_system.sql
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_body_system (
    sk_body_system    INTEGER,
    body_system_code  VARCHAR,
    body_system_name  VARCHAR,
    metric_category   VARCHAR
);

INSERT INTO gold.dim_body_system
VALUES
    (1, 'cardiovascular',     'Cardiovascular System',     'vitals'),
    (2, 'respiratory',        'Respiratory System',        'vitals'),
    (3, 'metabolic',          'Metabolic System',          'nutrition'),
    (4, 'sleep_circadian',    'Sleep & Circadian Rhythm',  'sleep'),
    (5, 'musculoskeletal',    'Musculoskeletal System',    'activity'),
    (6, 'gastrointestinal',   'Gastrointestinal System',   'lab_results'),
    (7, 'immune',             'Immune System',             'lab_results'),
    (8, 'mental',             'Mental Health',             'mindfulness'),
    (9, 'body_composition',   'Body Composition',          'body'),
    (10, 'oral_health',       'Oral Health',               'hygiene'),
    (11, 'locomotion',        'Locomotion & Gait',         'gait'),
    (12, 'recovery',          'Recovery & Stress',         'recovery')
