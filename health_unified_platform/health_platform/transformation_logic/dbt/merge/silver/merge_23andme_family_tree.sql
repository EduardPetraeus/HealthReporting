-- MERGE bronze family tree data into silver.family_tree

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.family_tree__staging AS
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY business_key_hash
               ORDER BY load_datetime DESC
           ) AS rn
    FROM bronze.stg_23andme_family_tree
    WHERE person_id IS NOT NULL
)
SELECT
    person_id,
    first_name,
    last_name,
    relationship_to_user,
    generation,
    side,
    num_shared_segments,
    has_dna_match,
    profile_url,
    source_file,
    business_key_hash,
    row_hash,
    load_datetime,
    update_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: MERGE into target
MERGE INTO silver.family_tree AS target
USING silver.family_tree__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    first_name = src.first_name,
    last_name = src.last_name,
    relationship_to_user = src.relationship_to_user,
    generation = src.generation,
    side = src.side,
    num_shared_segments = src.num_shared_segments,
    has_dna_match = src.has_dna_match,
    profile_url = src.profile_url,
    source_file = src.source_file,
    row_hash = src.row_hash,
    update_datetime = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    person_id, first_name, last_name, relationship_to_user,
    generation, side, num_shared_segments, has_dna_match,
    profile_url, source_file, business_key_hash, row_hash,
    load_datetime, update_datetime
) VALUES (
    src.person_id, src.first_name, src.last_name, src.relationship_to_user,
    src.generation, src.side, src.num_shared_segments, src.has_dna_match,
    src.profile_url, src.source_file, src.business_key_hash, src.row_hash,
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.family_tree__staging;
