-- =============================================================================
-- fct_lab_result.sql
-- Gold fact: lab test results with marker dimension FK
--
-- Target: health_dw.gold.fct_lab_result (Databricks)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per marker per test date
-- =============================================================================

CREATE OR REPLACE VIEW {target} AS

SELECT
    CAST(date_format(test_date, 'yyyyMMdd') AS INTEGER)  AS sk_date,
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
    END                                                 AS deviation_from_ref_pct,

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
    END                                                 AS body_system

FROM health_dw.silver.lab_results

ORDER BY test_date DESC, marker_category, marker_name
