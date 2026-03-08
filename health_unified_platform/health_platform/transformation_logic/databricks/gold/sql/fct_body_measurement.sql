-- =============================================================================
-- fct_body_measurement.sql
-- Gold fact: body composition measurements
--
-- Target: health_dw.gold.fct_body_measurement (Databricks)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per measurement
-- =============================================================================

CREATE OR REPLACE VIEW {target} AS

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

FROM health_dw.silver.weight
WHERE weight_kg > 0

ORDER BY datetime DESC
