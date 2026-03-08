-- MERGE bronze ancestry segments into silver.ancestry_segments
-- Adds computed pct_of_chromosome via chromosome length reference CTE

-- Chromosome lengths (GRCh37/hg19)
CREATE OR REPLACE TABLE silver.ancestry_segments__staging AS
WITH chr_lengths AS (
    SELECT * FROM (VALUES
        ('chr1', 249250621), ('chr2', 243199373), ('chr3', 198022430),
        ('chr4', 191154276), ('chr5', 180915260), ('chr6', 171115067),
        ('chr7', 159138663), ('chr8', 146364022), ('chr9', 141213431),
        ('chr10', 135534747), ('chr11', 135006516), ('chr12', 133851895),
        ('chr13', 115169878), ('chr14', 107349540), ('chr15', 102531392),
        ('chr16', 90354753), ('chr17', 81195210), ('chr18', 78077248),
        ('chr19', 59128983), ('chr20', 63025520), ('chr21', 48129895),
        ('chr22', 51304566), ('chrX', 155270560), ('chrY', 59373566)
    ) AS t(chromosome, length_bp)
),
deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY business_key_hash
               ORDER BY load_datetime DESC
           ) AS rn
    FROM bronze.stg_23andme_ancestry
    WHERE ancestry_category IS NOT NULL
      AND chromosome IS NOT NULL
)
SELECT
    d.ancestry_category,
    d.copy,
    d.chromosome,
    d.start_bp,
    d.end_bp,
    d.segment_length_bp,
    ROUND(d.segment_length_bp * 100.0 / NULLIF(c.length_bp, 0), 2) AS pct_of_chromosome,
    d.source_file,
    d.business_key_hash,
    d.row_hash,
    d.load_datetime,
    d.update_datetime
FROM deduped d
LEFT JOIN chr_lengths c ON d.chromosome = c.chromosome
WHERE d.rn = 1;

-- Step 2: MERGE into target
MERGE INTO silver.ancestry_segments AS target
USING silver.ancestry_segments__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    ancestry_category = src.ancestry_category,
    copy = src.copy,
    chromosome = src.chromosome,
    start_bp = src.start_bp,
    end_bp = src.end_bp,
    segment_length_bp = src.segment_length_bp,
    pct_of_chromosome = src.pct_of_chromosome,
    source_file = src.source_file,
    row_hash = src.row_hash,
    update_datetime = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    ancestry_category, copy, chromosome, start_bp, end_bp,
    segment_length_bp, pct_of_chromosome, source_file,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.ancestry_category, src.copy, src.chromosome, src.start_bp, src.end_bp,
    src.segment_length_bp, src.pct_of_chromosome, src.source_file,
    src.business_key_hash, src.row_hash, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.ancestry_segments__staging;
