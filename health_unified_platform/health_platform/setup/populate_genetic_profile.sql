-- Populate agent.genetic_profile from silver.genetic_health_findings
-- Maps risk_level → clinical_relevance, derives platform_relevance and related_metrics

CREATE SCHEMA IF NOT EXISTS agent;

INSERT OR REPLACE INTO agent.genetic_profile (
    category,
    report_name,
    result_summary,
    variant_detected,
    gene,
    snp_id,
    genotype,
    clinical_relevance,
    platform_relevance,
    related_metrics,
    load_datetime
)
SELECT
    category,
    report_name,
    result_summary,
    variant_detected,
    gene,
    snp_id,
    genotype,
    -- Map risk_level to clinical_relevance
    CASE risk_level
        WHEN 'high' THEN 'high'
        WHEN 'medium' THEN 'medium'
        WHEN 'low' THEN 'low'
        WHEN 'informational' THEN 'low'
        ELSE 'low'
    END AS clinical_relevance,
    -- Derive platform_relevance based on condition
    CASE
        WHEN LOWER(report_name) LIKE '%alzheimer%' THEN 'Baseline cognitive risk context'
        WHEN LOWER(report_name) LIKE '%parkinson%' THEN 'Motor function monitoring context'
        WHEN LOWER(report_name) LIKE '%diabetes%' THEN 'Glucose and metabolic monitoring context'
        WHEN LOWER(report_name) LIKE '%weight%' THEN 'Weight trend interpretation context'
        WHEN LOWER(report_name) LIKE '%muscle%' OR LOWER(report_name) LIKE '%actn3%' THEN 'Training type optimization context'
        WHEN LOWER(report_name) LIKE '%hemochromatosis%' THEN 'Iron and ferritin monitoring context'
        WHEN LOWER(report_name) LIKE '%macular%' THEN 'Vision health awareness context'
        WHEN LOWER(report_name) LIKE '%haplogroup%' THEN 'Ancestry and heritage context'
        ELSE 'General genetic awareness'
    END AS platform_relevance,
    -- Derive related_metrics
    CASE
        WHEN LOWER(report_name) LIKE '%alzheimer%' THEN 'sleep_score,deep_sleep,hrv'
        WHEN LOWER(report_name) LIKE '%parkinson%' THEN 'steps,walking_speed,walking_steadiness'
        WHEN LOWER(report_name) LIKE '%diabetes%' THEN 'weight,active_calories,steps'
        WHEN LOWER(report_name) LIKE '%weight%' THEN 'weight,active_calories,steps'
        WHEN LOWER(report_name) LIKE '%muscle%' THEN 'workout,active_calories'
        WHEN LOWER(report_name) LIKE '%hemochromatosis%' THEN 'lab_results'
        ELSE NULL
    END AS related_metrics,
    CURRENT_TIMESTAMP AS load_datetime
FROM silver.genetic_health_findings;
