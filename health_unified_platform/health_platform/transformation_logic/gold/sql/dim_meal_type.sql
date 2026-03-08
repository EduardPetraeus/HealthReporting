-- =============================================================================
-- dim_meal_type.sql
-- Gold dimension: meal type classification for nutrition analysis
--
-- Target: gold.dim_meal_type (DuckDB local)
-- Materialization: CREATE TABLE (static reference data)
-- Source: DISTINCT meal_type from silver.daily_meal + static enrichment
--
-- Output columns:
--   sk_meal_type       INTEGER  — surrogate key
--   meal_type          VARCHAR  — raw meal type from silver
--   meal_display_name  VARCHAR  — human-friendly name
--   meal_order         INTEGER  — chronological order (1=breakfast, 4=snack)
--   is_main_meal       BOOLEAN  — true for breakfast/lunch/dinner
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_meal_type AS

SELECT * FROM (VALUES
    (1, 'breakfast', 'Breakfast', 1, true),
    (2, 'lunch',     'Lunch',     2, true),
    (3, 'dinner',    'Dinner',    3, true),
    (4, 'snack',     'Snack',     4, false)
) AS t(sk_meal_type, meal_type, meal_display_name, meal_order, is_main_meal);
