-- =============================================================================
-- vw_daily_annotations.sql
-- Gold view: valid daily annotations (training events, incidents, travel, etc.)
--
-- Target: gold.vw_daily_annotations_valid (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW
-- Ported from: databricks/gold/sql/vw_daily_annotations.sql
-- =============================================================================

CREATE OR REPLACE VIEW gold.vw_daily_annotations_valid AS

SELECT
    sk_date,
    annotation_type AS activity_type,
    annotation      AS comment
FROM silver.daily_annotations
WHERE is_valid = TRUE
