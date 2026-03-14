-- Seed: Danish clinical biomarker reference ranges
-- Source: sundhed.dk / Danish clinical laboratory standards
-- Used by the sundhed.dk ingestion pipeline to flag out-of-range results
-- DuckDB-compatible SQL

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.ref_biomarker_ranges (
    marker_name     VARCHAR NOT NULL,
    unit            VARCHAR NOT NULL,
    ref_min         DOUBLE,
    ref_max         DOUBLE,
    sex             VARCHAR NOT NULL DEFAULT 'all',
    age_group       VARCHAR NOT NULL DEFAULT 'adult'
);

-- Truncate before re-seeding to allow idempotent runs
DELETE FROM reference.ref_biomarker_ranges WHERE 1=1;

INSERT INTO reference.ref_biomarker_ranges (marker_name, unit, ref_min, ref_max, sex, age_group) VALUES
-- =============================================================================
-- Haematology
-- =============================================================================
('Haemoglobin',                'mmol/L',       8.3,    10.5,   'male',   'adult'),
('Haemoglobin',                'mmol/L',       7.3,    9.5,    'female', 'adult'),
('Erytrocytter',               '10^12/L',      4.3,    5.7,    'male',   'adult'),
('Erytrocytter',               '10^12/L',      3.9,    5.2,    'female', 'adult'),
('Haematokrit',                'L/L',          0.39,   0.50,   'male',   'adult'),
('Haematokrit',                'L/L',          0.35,   0.46,   'female', 'adult'),
('MCV',                        'fL',           82.0,   98.0,   'all',    'adult'),
('MCH',                        'fmol',         1.7,    2.1,    'all',    'adult'),
('MCHC',                       'mmol/L',       19.5,   22.0,   'all',    'adult'),
('Leukocytter',                '10^9/L',       3.5,    8.8,    'all',    'adult'),
('Trombocytter',               '10^9/L',       145.0,  390.0,  'all',    'adult'),
('Retikulocytter',             '10^9/L',       30.0,   100.0,  'all',    'adult'),
('Sedimentationsreaktion',     'mm/t',         0.0,    12.0,   'male',   'adult'),
('Sedimentationsreaktion',     'mm/t',         0.0,    20.0,   'female', 'adult'),

-- =============================================================================
-- Inflammation
-- =============================================================================
('CRP',                        'mg/L',         0.0,    5.0,    'all',    'adult'),

-- =============================================================================
-- Kidney function
-- =============================================================================
('Kreatinin',                  'umol/L',       60.0,   105.0,  'male',   'adult'),
('Kreatinin',                  'umol/L',       45.0,   90.0,   'female', 'adult'),
('eGFR',                       'mL/min/1.73m2', 90.0,  NULL,   'all',    'adult'),
('Carbamid',                   'mmol/L',       3.5,    8.0,    'all',    'adult'),
('Urinsyre',                   'umol/L',       200.0,  450.0,  'male',   'adult'),
('Urinsyre',                   'umol/L',       150.0,  350.0,  'female', 'adult'),

-- =============================================================================
-- Liver function
-- =============================================================================
('ALAT',                       'U/L',          10.0,   70.0,   'male',   'adult'),
('ALAT',                       'U/L',          10.0,   45.0,   'female', 'adult'),
('ASAT',                       'U/L',          15.0,   45.0,   'male',   'adult'),
('ASAT',                       'U/L',          15.0,   35.0,   'female', 'adult'),
('Basisk fosfatase',           'U/L',          35.0,   105.0,  'all',    'adult'),
('GGT',                        'U/L',          10.0,   80.0,   'male',   'adult'),
('GGT',                        'U/L',          10.0,   45.0,   'female', 'adult'),
('Bilirubin, total',           'umol/L',       5.0,    25.0,   'all',    'adult'),
('Albumin',                    'g/L',          36.0,   48.0,   'all',    'adult'),
('Laktat dehydrogenase',       'U/L',          105.0,  205.0,  'all',    'adult'),

-- =============================================================================
-- Electrolytes and minerals
-- =============================================================================
('Natrium',                    'mmol/L',       137.0,  145.0,  'all',    'adult'),
('Kalium',                     'mmol/L',       3.5,    5.0,    'all',    'adult'),
('Calcium',                    'mmol/L',       2.15,   2.55,   'all',    'adult'),
('Fosfat',                     'mmol/L',       0.80,   1.50,   'all',    'adult'),
('Magnesium',                  'mmol/L',       0.70,   1.05,   'all',    'adult'),

-- =============================================================================
-- Thyroid
-- =============================================================================
('TSH',                        'mIU/L',        0.3,    4.0,    'all',    'adult'),
('T4, frit',                   'pmol/L',       12.0,   22.0,   'all',    'adult'),
('T3, frit',                   'pmol/L',       3.1,    6.8,    'all',    'adult'),

-- =============================================================================
-- Glucose metabolism
-- =============================================================================
('HbA1c',                      'mmol/mol',     0.0,    48.0,   'all',    'adult'),
('Glukose, fasting',           'mmol/L',       4.0,    6.0,    'all',    'adult'),

-- =============================================================================
-- Lipid panel
-- =============================================================================
('Kolesterol, total',          'mmol/L',       0.0,    5.0,    'all',    'adult'),
('LDL',                        'mmol/L',       0.0,    3.0,    'all',    'adult'),
('HDL',                        'mmol/L',       1.0,    NULL,   'male',   'adult'),
('HDL',                        'mmol/L',       1.2,    NULL,   'female', 'adult'),
('Triglycerid',                'mmol/L',       0.0,    2.0,    'all',    'adult'),

-- =============================================================================
-- Iron metabolism
-- =============================================================================
('Ferritin',                   'ug/L',         30.0,   400.0,  'male',   'adult'),
('Ferritin',                   'ug/L',         15.0,   150.0,  'female', 'adult'),
('Jern',                       'umol/L',       14.0,   32.0,   'male',   'adult'),
('Jern',                       'umol/L',       10.0,   28.0,   'female', 'adult'),
('Transferrin',                'g/L',          2.0,    3.6,    'all',    'adult'),
('TIBC',                       'umol/L',       45.0,   72.0,   'all',    'adult'),

-- =============================================================================
-- Vitamins
-- =============================================================================
('Vitamin D',                  'nmol/L',       50.0,   160.0,  'all',    'adult'),
('Vitamin B12',                'pmol/L',       200.0,  600.0,  'all',    'adult'),
('Folat',                      'nmol/L',       10.0,   NULL,   'all',    'adult'),

-- =============================================================================
-- Coagulation
-- =============================================================================
('D-dimer',                    'mg/L',         0.0,    0.5,    'all',    'adult'),
('INR',                        'ratio',        0.8,    1.2,    'all',    'adult'),
('APTT',                       's',            25.0,   38.0,   'all',    'adult'),
('Fibrinogen',                 'g/L',          2.0,    4.0,    'all',    'adult'),

-- =============================================================================
-- Pancreatic enzymes
-- =============================================================================
('Amylase',                    'U/L',          25.0,   125.0,  'all',    'adult'),
('Lipase',                     'U/L',          0.0,    60.0,   'all',    'adult'),

-- =============================================================================
-- Prostate (male-specific)
-- =============================================================================
('Prostata-specifikt antigen (PSA)', 'ug/L',  0.0,    4.0,    'male',   'adult'),

-- =============================================================================
-- Protein
-- =============================================================================
('Protein, total',             'g/L',          64.0,   83.0,   'all',    'adult'),
('IgG',                        'g/L',          6.1,    16.2,   'all',    'adult'),
('IgA',                        'g/L',          0.7,    4.0,    'all',    'adult'),
('IgM',                        'g/L',          0.4,    2.3,    'all',    'adult');
