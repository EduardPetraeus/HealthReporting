-- merge_daily_stoic_quotes.sql
-- Per-source merge: daily-stoic DuckDB -> silver.stoic_quotes
-- Business key: date (one quote per day)
-- Daily Stoic quotes with author attribution.
--
-- Usage: python run_merge.py silver/merge_daily_stoic_quotes.sql

CREATE TABLE IF NOT EXISTS silver.stoic_quotes (
    sk_date INTEGER,
    day DATE,
    quote_text VARCHAR,
    author VARCHAR,
    business_key_hash VARCHAR,
    row_hash VARCHAR,
    load_datetime TIMESTAMP,
    update_datetime TIMESTAMP
);

CREATE OR REPLACE TABLE silver.stoic_quotes__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_daily_stoic_quotes
    WHERE date IS NOT NULL
)
SELECT
    CAST(REPLACE(date, '-', '') AS INTEGER)    AS sk_date,
    CAST(date AS DATE)                         AS day,
    text                                       AS quote_text,
    author                                     AS author,
    md5(date)                                  AS business_key_hash,
    md5(
        coalesce(text, '')                                       || '||' ||
        coalesce(author, '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.stoic_quotes AS target
USING silver.stoic_quotes__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date             = src.sk_date,
    day                 = src.day,
    quote_text          = src.quote_text,
    author              = src.author,
    row_hash            = src.row_hash,
    update_datetime     = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, quote_text, author,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.quote_text, src.author,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

COMMENT ON TABLE silver.stoic_quotes IS 'Daily Stoic quotes with author attribution. One row per day.';
COMMENT ON COLUMN silver.stoic_quotes.sk_date IS 'Surrogate date key (YYYYMMDD integer)';
COMMENT ON COLUMN silver.stoic_quotes.day IS 'Calendar date';
COMMENT ON COLUMN silver.stoic_quotes.quote_text IS 'Full text of the Stoic quote';
COMMENT ON COLUMN silver.stoic_quotes.author IS 'Author or source of the quote';
COMMENT ON COLUMN silver.stoic_quotes.business_key_hash IS 'MD5 hash of the business key (date)';
COMMENT ON COLUMN silver.stoic_quotes.row_hash IS 'MD5 hash of non-key columns for change detection';
COMMENT ON COLUMN silver.stoic_quotes.load_datetime IS 'Timestamp when the row was first loaded into silver';
COMMENT ON COLUMN silver.stoic_quotes.update_datetime IS 'Timestamp when the row was last updated in silver';

DROP TABLE IF EXISTS silver.stoic_quotes__staging;
