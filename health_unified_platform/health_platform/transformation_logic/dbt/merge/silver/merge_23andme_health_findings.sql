-- MERGE bronze health report findings into silver.genetic_health_findings
-- Pattern: staging CTE with ROW_NUMBER dedup → MERGE with SCD1 hash comparison

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.genetic_health_findings__staging AS
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY business_key_hash
               ORDER BY load_datetime DESC
           ) AS rn
    FROM bronze.stg_23andme_health_reports
    WHERE report_name IS NOT NULL
      AND category IS NOT NULL
)
SELECT
    report_name,
    category,
    condition,
    gene,
    snp_id,
    genotype,
    risk_level,
    result_summary,
    variant_detected,
    source_file,
    business_key_hash,
    row_hash,
    load_datetime,
    update_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: MERGE into target
MERGE INTO silver.genetic_health_findings AS target
USING silver.genetic_health_findings__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    report_name = src.report_name,
    category = src.category,
    condition = src.condition,
    gene = src.gene,
    snp_id = src.snp_id,
    genotype = src.genotype,
    risk_level = src.risk_level,
    result_summary = src.result_summary,
    variant_detected = src.variant_detected,
    source_file = src.source_file,
    row_hash = src.row_hash,
    update_datetime = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    report_name, category, condition, gene, snp_id, genotype,
    risk_level, result_summary, variant_detected, source_file,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.report_name, src.category, src.condition, src.gene, src.snp_id,
    src.genotype, src.risk_level, src.result_summary, src.variant_detected,
    src.source_file, src.business_key_hash, src.row_hash,
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.genetic_health_findings__staging;
