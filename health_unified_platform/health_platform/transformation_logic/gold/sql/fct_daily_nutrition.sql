-- =============================================================================
-- fct_daily_nutrition.sql
-- Gold fact: aggregated daily nutrition from individual meal items
--
-- Target: gold.fct_daily_nutrition (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.daily_meal (aggregated to daily grain)
-- Grain: one row per day
--
-- Output columns:
--   sk_date            INTEGER — FK to dim_date
--   day                DATE    — calendar day
--   total_calories     DOUBLE  — total calorie intake
--   total_protein_g    DOUBLE  — total protein in grams
--   total_carbs_g      DOUBLE  — total carbohydrates in grams
--   total_fat_g        DOUBLE  — total fat in grams
--   total_fiber_g      DOUBLE  — total fiber in grams
--   total_sugar_g      DOUBLE  — total sugar in grams
--   protein_pct        DOUBLE  — protein as % of calories
--   carbs_pct          DOUBLE  — carbs as % of calories
--   fat_pct            DOUBLE  — fat as % of calories
--   breakfast_calories  DOUBLE — calories from breakfast
--   lunch_calories     DOUBLE  — calories from lunch
--   dinner_calories    DOUBLE  — calories from dinner
--   snack_calories     DOUBLE  — calories from snacks
--   item_count         BIGINT  — number of food items logged
--   meal_count         BIGINT  — number of distinct meals logged
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_daily_nutrition AS

SELECT
    sk_date,
    date                                         AS day,
    ROUND(SUM(calories), 0)                      AS total_calories,
    ROUND(SUM(protein), 1)                       AS total_protein_g,
    ROUND(SUM(carbs), 1)                         AS total_carbs_g,
    ROUND(SUM(fat), 1)                           AS total_fat_g,
    ROUND(SUM(carbs_fiber), 1)                   AS total_fiber_g,
    ROUND(SUM(carbs_sugar), 1)                   AS total_sugar_g,

    -- Macro percentages (protein=4cal/g, carbs=4cal/g, fat=9cal/g)
    ROUND(
        SUM(protein) * 4.0 / NULLIF(SUM(calories), 0) * 100, 1
    )                                            AS protein_pct,
    ROUND(
        SUM(carbs) * 4.0 / NULLIF(SUM(calories), 0) * 100, 1
    )                                            AS carbs_pct,
    ROUND(
        SUM(fat) * 9.0 / NULLIF(SUM(calories), 0) * 100, 1
    )                                            AS fat_pct,

    -- Per-meal calorie breakdown
    ROUND(SUM(CASE WHEN LOWER(meal_type) = 'breakfast' THEN calories ELSE 0 END), 0)
                                                 AS breakfast_calories,
    ROUND(SUM(CASE WHEN LOWER(meal_type) = 'lunch' THEN calories ELSE 0 END), 0)
                                                 AS lunch_calories,
    ROUND(SUM(CASE WHEN LOWER(meal_type) = 'dinner' THEN calories ELSE 0 END), 0)
                                                 AS dinner_calories,
    ROUND(SUM(CASE WHEN LOWER(meal_type) = 'snack' THEN calories ELSE 0 END), 0)
                                                 AS snack_calories,

    COUNT(*)                                     AS item_count,
    COUNT(DISTINCT meal_type)                    AS meal_count

FROM silver.daily_meal
GROUP BY sk_date, date

ORDER BY date DESC;
