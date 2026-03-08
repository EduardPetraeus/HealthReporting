-- =============================================================================
-- dim_body_system.sql
-- Gold dimension: body system classification for cross-domain analysis
--
-- Target: gold.dim_body_system (DuckDB local)
-- Materialization: CREATE TABLE (static reference data)
-- Source: _index.yml metric categories mapped to body systems
--
-- Output columns:
--   sk_body_system    INTEGER  — surrogate key
--   body_system_code  VARCHAR  — short code for joins
--   body_system_name  VARCHAR  — human-readable name
--   metric_category   VARCHAR  — maps to _index.yml category
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_body_system AS

SELECT * FROM (VALUES
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
) AS t(sk_body_system, body_system_code, body_system_name, metric_category);
