-- =============================================================================
-- dim_supplement.sql
-- Gold dimension: supplement catalog with classification
--
-- Target: health_dw.gold.dim_supplement (Databricks)
-- Materialization: CREATE OR REPLACE TABLE (rebuild when supplement log changes)
-- =============================================================================

CREATE OR REPLACE TABLE {target} AS

WITH distinct_supplements AS (
    SELECT DISTINCT
        supplement_name,
        dose,
        unit,
        target
    FROM health_dw.silver.supplement_log
    WHERE supplement_name IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY supplement_name)         AS sk_supplement,
    supplement_name,
    CASE
        WHEN LOWER(supplement_name) LIKE '%vitamin%' OR LOWER(supplement_name) LIKE '%d3%'
             OR LOWER(supplement_name) LIKE '%b12%' OR LOWER(supplement_name) LIKE '%folate%'
             OR LOWER(supplement_name) LIKE '%folic%'
            THEN 'vitamin'
        WHEN LOWER(supplement_name) LIKE '%magnesium%' OR LOWER(supplement_name) LIKE '%zinc%'
             OR LOWER(supplement_name) LIKE '%iron%' OR LOWER(supplement_name) LIKE '%calcium%'
             OR LOWER(supplement_name) LIKE '%selenium%'
            THEN 'mineral'
        WHEN LOWER(supplement_name) LIKE '%omega%' OR LOWER(supplement_name) LIKE '%epa%'
             OR LOWER(supplement_name) LIKE '%dha%' OR LOWER(supplement_name) LIKE '%fish oil%'
            THEN 'fatty_acid'
        WHEN LOWER(supplement_name) LIKE '%probiotic%' OR LOWER(supplement_name) LIKE '%lactobacillus%'
             OR LOWER(supplement_name) LIKE '%bifidobacterium%'
            THEN 'probiotic'
        WHEN LOWER(supplement_name) LIKE '%enzyme%' OR LOWER(supplement_name) LIKE '%creon%'
             OR LOWER(supplement_name) LIKE '%lipase%' OR LOWER(supplement_name) LIKE '%protease%'
            THEN 'enzyme'
        WHEN LOWER(supplement_name) LIKE '%protein%' OR LOWER(supplement_name) LIKE '%creatine%'
             OR LOWER(supplement_name) LIKE '%glutamine%' OR LOWER(supplement_name) LIKE '%bcaa%'
            THEN 'amino_acid'
        ELSE 'other'
    END                                                 AS category,
    dose,
    unit,
    target                                              AS target_condition

FROM distinct_supplements
ORDER BY supplement_name
