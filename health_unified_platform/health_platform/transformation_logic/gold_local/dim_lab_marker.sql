-- =============================================================================
-- dim_lab_marker.sql
-- Gold dimension: lab marker catalog with reference ranges
--
-- Target: gold.dim_lab_marker (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (rebuild when new lab tests arrive)
-- Ported from: databricks/gold/sql/dim_lab_marker.sql
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_lab_marker AS

WITH distinct_markers AS (
    SELECT DISTINCT
        marker_name,
        marker_category,
        unit,
        reference_min,
        reference_max,
        test_type
    FROM silver.lab_results
    WHERE marker_name IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY marker_category, marker_name) AS sk_lab_marker,
    marker_name,
    REPLACE(marker_name, '_', ' ')                      AS marker_display_name,
    marker_category,
    CASE marker_category
        WHEN 'digestive_enzymes'  THEN 'gastrointestinal'
        WHEN 'immune_markers'     THEN 'immune'
        WHEN 'fatty_acids'        THEN 'cardiovascular'
        WHEN 'vitamins'           THEN 'metabolic'
        WHEN 'gut_inflammation'   THEN 'gastrointestinal'
        WHEN 'gut_permeability'   THEN 'gastrointestinal'
        WHEN 'stool_analysis'     THEN 'gastrointestinal'
        ELSE 'metabolic'
    END                                                 AS body_system,
    unit,
    reference_min,
    reference_max,
    test_type

FROM distinct_markers
ORDER BY marker_category, marker_name
