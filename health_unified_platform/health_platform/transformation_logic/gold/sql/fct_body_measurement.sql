-- =============================================================================
-- fct_body_measurement.sql
-- Gold fact: body composition measurements
--
-- Target: gold.fct_body_measurement (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.weight
-- Grain: one row per measurement
--
-- Output columns:
--   sk_date        INTEGER   — FK to dim_date
--   sk_time        VARCHAR   — FK to dim_time_of_day
--   datetime       TIMESTAMP — measurement timestamp
--   weight_kg      DOUBLE    — total body weight
--   fat_mass_kg    DOUBLE    — fat mass
--   muscle_mass_kg DOUBLE    — muscle mass
--   bone_mass_kg   DOUBLE    — bone mass
--   hydration_kg   DOUBLE    — hydration mass
--   fat_pct        DOUBLE    — fat as percentage of weight
--   muscle_pct     DOUBLE    — muscle as percentage of weight
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

ORDER BY datetime DESC;
