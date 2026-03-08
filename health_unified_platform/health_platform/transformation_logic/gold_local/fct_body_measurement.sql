-- =============================================================================
-- fct_body_measurement.sql
-- Gold fact: body composition measurements
--
-- Target: gold.fct_body_measurement (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per measurement
-- Ported from: databricks/gold/sql/fct_body_measurement.sql
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_body_measurement AS

SELECT
    sk_date,
    sk_time,
    datetime,
    weight_kg,
    fat_mass_kg,
    muscle_mass_kg,
    bone_mass_kg,
    hydration_kg,
    ROUND(fat_mass_kg / NULLIF(weight_kg, 0) * 100, 1)    AS fat_pct,
    ROUND(muscle_mass_kg / NULLIF(weight_kg, 0) * 100, 1) AS muscle_pct

FROM silver.weight
WHERE weight_kg > 0

ORDER BY datetime DESC
