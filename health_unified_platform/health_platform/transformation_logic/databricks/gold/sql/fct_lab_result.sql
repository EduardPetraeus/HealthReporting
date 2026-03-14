-- =============================================================================
-- fct_lab_result.sql
-- Gold fact: lab test results enriched with marker metadata
--
-- Target: health_dw.gold.fct_lab_result (Databricks)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per marker per test date
-- Joins: silver.lab_results + silver.dim_marker_catalog + silver.dim_reference_range
-- =============================================================================

CREATE OR REPLACE VIEW {target} AS

SELECT
    CAST(date_format(lr.test_date, 'yyyyMMdd') AS INTEGER)  AS sk_date,
    lr.test_id,
    lr.test_date,
    lr.test_type,
    lr.test_name,
    lr.lab_name,
    lr.marker_name,
    lr.marker_category,
    COALESCE(mc.marker_domain, lr.marker_category)          AS marker_domain,
    COALESCE(mc.body_system, 'unknown')                     AS body_system,
    lr.value_numeric,
    lr.value_text,
    lr.unit                                                 AS original_unit,
    COALESCE(mc.canonical_unit, lr.unit)                    AS canonical_unit,
    lr.reference_min,
    lr.reference_max,
    lr.reference_direction,
    lr.status,
    mc.is_log_scale,
    mc.detection_limit,
    mc.description                                          AS marker_description,
    rr.reference_min                                        AS catalog_ref_min,
    rr.reference_max                                        AS catalog_ref_max,
    rr.notes                                                AS reference_notes,

    -- Deviation from reference range as percentage
    CASE
        WHEN lr.value_numeric IS NULL THEN NULL
        WHEN lr.reference_min IS NOT NULL AND lr.value_numeric < lr.reference_min
            THEN ROUND((lr.value_numeric - lr.reference_min) / NULLIF(lr.reference_min, 0) * 100, 1)
        WHEN lr.reference_max IS NOT NULL AND lr.value_numeric > lr.reference_max
            THEN ROUND((lr.value_numeric - lr.reference_max) / NULLIF(lr.reference_max, 0) * 100, 1)
        ELSE 0.0
    END                                                     AS deviation_from_ref_pct,

    lr.load_datetime

FROM health_dw.silver.lab_results lr
LEFT JOIN health_dw.silver.dim_marker_catalog mc
    ON LOWER(lr.marker_name) = mc.marker_key
LEFT JOIN health_dw.silver.dim_reference_range rr
    ON mc.marker_key = rr.marker_key
    AND rr.source = CASE
        WHEN lr.lab_name = 'GetTested' THEN 'gettested'
        WHEN lr.lab_name = 'sundhed.dk' THEN 'sundhed_dk'
        ELSE 'gettested'
    END
    AND rr.effective_to IS NULL

ORDER BY lr.test_date DESC, lr.marker_category, lr.marker_name
