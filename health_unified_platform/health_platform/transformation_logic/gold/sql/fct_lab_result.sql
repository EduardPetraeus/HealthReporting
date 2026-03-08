-- =============================================================================
-- fct_lab_result.sql
-- Gold fact: lab test results with marker dimension FK
--
-- Target: gold.fct_lab_result (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.lab_results
-- Grain: one row per marker per test date
--
-- Output columns:
--   sk_date              INTEGER — FK to dim_date (derived from test_date)
--   test_date            DATE    — date of the lab test
--   test_id              VARCHAR — unique test identifier
--   test_type            VARCHAR — type of lab test
--   test_name            VARCHAR — name of the test
--   lab_name             VARCHAR — laboratory name
--   marker_name          VARCHAR — joinable to dim_lab_marker
--   marker_category      VARCHAR — marker category
--   value_numeric        DOUBLE  — measured value
--   value_text           VARCHAR — text value (for non-numeric results)
--   unit                 VARCHAR — measurement unit
--   reference_min        DOUBLE  — lower reference bound
--   reference_max        DOUBLE  — upper reference bound
--   status               VARCHAR — normal/low/high
--   deviation_from_ref_pct DOUBLE — how far from reference range (%)
--   body_system          VARCHAR — FK-ready body system code
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_lab_result AS

SELECT
    CAST(strftime(test_date, '%Y%m%d') AS INTEGER) AS sk_date,
    test_date,
    test_id,
    test_type,
    test_name,
    lab_name,
    marker_name,
    marker_category,
    value_numeric,
    value_text,
    unit,
    reference_min,
    reference_max,
    status,

    -- Deviation from reference range as percentage
    CASE
        WHEN value_numeric IS NULL THEN NULL
        WHEN reference_min IS NOT NULL AND value_numeric < reference_min
            THEN ROUND((value_numeric - reference_min) / NULLIF(reference_min, 0) * 100, 1)
        WHEN reference_max IS NOT NULL AND value_numeric > reference_max
            THEN ROUND((value_numeric - reference_max) / NULLIF(reference_max, 0) * 100, 1)
        ELSE 0.0
    END                                          AS deviation_from_ref_pct,

    -- Body system mapping
    CASE marker_category
        WHEN 'digestive_enzymes'  THEN 'gastrointestinal'
        WHEN 'immune_markers'     THEN 'immune'
        WHEN 'fatty_acids'        THEN 'cardiovascular'
        WHEN 'vitamins'           THEN 'metabolic'
        WHEN 'gut_inflammation'   THEN 'gastrointestinal'
        WHEN 'gut_permeability'   THEN 'gastrointestinal'
        WHEN 'stool_analysis'     THEN 'gastrointestinal'
        ELSE 'metabolic'
    END                                          AS body_system

FROM silver.lab_results

ORDER BY test_date DESC, marker_category, marker_name;
