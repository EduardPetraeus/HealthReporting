-- =============================================================================
-- vw_genetic_summary.sql
-- Gold view: unified genetic summary combining health findings, ancestry, traits
--
-- Target: health_dw.gold.vw_genetic_summary (Databricks)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per genetic record (health finding, ancestry segment, or trait)
-- Sources: silver.genetic_health_findings, silver.genetic_ancestry, silver.genetic_traits
-- =============================================================================

CREATE OR REPLACE VIEW {target} AS

SELECT
    'health_finding'                                        AS record_type,
    ghf.category,
    ghf.report_name                                         AS name,
    ghf.condition                                           AS detail,
    ghf.result_summary                                      AS result,
    ghf.risk_level,
    ghf.gene,
    ghf.snp_id,
    ghf.genotype,
    CAST(NULL AS DOUBLE)                                    AS numeric_value,
    ghf.variant_detected,
    ghf.load_datetime
FROM health_dw.silver.genetic_health_findings ghf

UNION ALL

SELECT
    'ancestry'                                              AS record_type,
    'ancestry'                                              AS category,
    ga.population                                           AS name,
    ga.region                                               AS detail,
    ga.population || ': ' || CAST(ga.percentage AS VARCHAR) || '%' AS result,
    'informational'                                         AS risk_level,
    NULL                                                    AS gene,
    NULL                                                    AS snp_id,
    NULL                                                    AS genotype,
    ga.percentage                                           AS numeric_value,
    false                                                   AS variant_detected,
    ga.load_datetime
FROM health_dw.silver.genetic_ancestry ga

UNION ALL

SELECT
    'trait'                                                 AS record_type,
    gt.category,
    gt.trait_name                                           AS name,
    gt.source                                               AS detail,
    gt.result_value                                         AS result,
    gt.clinical_relevance                                   AS risk_level,
    gt.gene,
    gt.snp_id,
    gt.genotype,
    gt.result_numeric                                       AS numeric_value,
    false                                                   AS variant_detected,
    gt.load_datetime
FROM health_dw.silver.genetic_traits gt
