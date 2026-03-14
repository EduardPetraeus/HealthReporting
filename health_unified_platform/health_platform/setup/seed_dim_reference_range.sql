-- seed_dim_reference_range.sql
-- Populates silver.dim_reference_range with all reference ranges from:
--   - blood_panel_2026-02-24.yaml (68 markers)
--   - microbiome_2026-02-23.yaml (28 markers with ranges — yeast excluded, no numeric range)
-- Total: 96 reference ranges
-- Source: GetTested / biovis Diagnostik MVZ GmbH
-- Idempotent: DELETE + INSERT

DELETE FROM silver.dim_reference_range WHERE source = 'gettested';

-- =============================================================================
-- BLOOD PANEL — Vitamins & Minerals (5 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('iron', 'gettested', 'range', 460, 575, 'mg/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('magnesium', 'gettested', 'range', 34, 40, 'mg/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('selenium', 'gettested', 'range', 96, 130, 'µg/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('vitamin_d3_25oh', 'gettested', 'min_only', 30, NULL, 'ng/ml', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Optimal: >30 ng/ml'),
('zinc', 'gettested', 'range', 6, 7.7, 'mg/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik');

-- =============================================================================
-- BLOOD PANEL — Gut & Bacterial Metabolites (6 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('indole_3_acetic_acid', 'gettested', 'range', 0.25, 1.55, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('hippuric_acid', 'gettested', 'max_only', NULL, 63, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('indoxyl_sulfate', 'gettested', 'max_only', NULL, 4.3, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('indolepropionic_acid', 'gettested', 'min_only', 0.1, NULL, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Optimal: >0.1 µmol/l'),
('p_cresol_sulfate', 'gettested', 'max_only', NULL, 45, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('putrescine', 'gettested', 'max_only', NULL, 0.22, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik');

-- =============================================================================
-- BLOOD PANEL — Fatty Acids & Lipid Metabolism (13 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('alpha_linolenic_acid', 'gettested', 'range', 0.34, 1.04, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('arachidonic_acid', 'gettested', 'range', 10, 14.3, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('dgla', 'gettested', 'range', 2.16, 3.57, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('dha', 'gettested', 'min_only', 6, NULL, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Optimal: >6%'),
('epa', 'gettested', 'min_only', 2, NULL, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Optimal: >2%'),
('gla', 'gettested', 'range', 0.15, 0.55, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('lc_omega3_index', 'gettested', 'min_only', 8, NULL, 'ratio', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Optimal: >8'),
('linoleic_acid', 'gettested', 'range', 16.8, 23.6, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('omega6_3_ratio', 'gettested', 'max_only', NULL, 8, 'ratio', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Lower is better'),
('oleic_acid', 'gettested', 'range', 16.9, 24.2, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('palmitic_acid', 'gettested', 'max_only', NULL, 25.3, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('stearic_acid', 'gettested', 'max_only', NULL, 13.8, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('eicosanoid_balance', 'gettested', 'min_only', 15, NULL, 'ratio', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Optimal: >15');

-- =============================================================================
-- BLOOD PANEL — Bile Acid Metabolism (11 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('cdca', 'gettested', 'range', 15, 60, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('cholic_acid', 'gettested', 'range', 7, 40, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('deoxycholic_acid', 'gettested', 'range', 5, 58, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('total_bile_acids', 'gettested', 'range', 0.2, 3.0, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('lithocholic_acid', 'gettested', 'range', 1, 30, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('primary_bile_acids', 'gettested', 'range', 26, 80, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('secondary_bile_acids', 'gettested', 'range', 7.5, 70, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('secondary_primary_ratio', 'gettested', 'range', 0.1, 2.4, 'ratio', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('tertiary_bile_acids', 'gettested', 'range', 0.5, 12.5, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('udca', 'gettested', 'range', 0.5, 12.5, '%', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('cytotoxic_neuroprotective_ratio', 'gettested', 'max_only', NULL, 15, 'ratio', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Lower is better');

-- =============================================================================
-- BLOOD PANEL — Amino Acids (21 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('alanine', 'gettested', 'range', 196, 481, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('arginine', 'gettested', 'range', 4.8, 36, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('asparagine', 'gettested', 'range', 33, 75, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('aspartic_acid', 'gettested', 'range', 39, 225, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('citrulline', 'gettested', 'range', 9.4, 29.9, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('gaba', 'gettested', 'range', 0.32, 0.95, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('glutamine', 'gettested', 'range', 237, 500, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('glutamic_acid', 'gettested', 'range', 99, 250, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('glycine', 'gettested', 'range', 197, 540, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('histidine', 'gettested', 'range', 46, 114, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('isoleucine', 'gettested', 'range', 35, 93, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('kynurenine', 'gettested', 'max_only', NULL, 1.20, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('leucine', 'gettested', 'range', 61, 146, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('lysine', 'gettested', 'range', 40, 102, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('phenylalanine', 'gettested', 'range', 31.5, 75.0, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('proline', 'gettested', 'range', 98, 292, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('taurine', 'gettested', 'range', 105, 239, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('threonine', 'gettested', 'range', 56, 145, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('tyrosine', 'gettested', 'range', 32, 84, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('tryptophan', 'gettested', 'range', 19.6, 42, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('valine', 'gettested', 'range', 112, 240, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik');

-- =============================================================================
-- BLOOD PANEL — Energy, Detox & Cellular Protection (4 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('adma', 'gettested', 'max_only', NULL, 0.66, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('carnitine', 'gettested', 'range', 20.2, 55.0, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('choline', 'gettested', 'range', 22, 97.5, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('sdma', 'gettested', 'max_only', NULL, 0.35, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik');

-- =============================================================================
-- BLOOD PANEL — Hormones & Stress Markers (4 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('cortisol', 'gettested', 'range', 0.06, 0.24, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('dhea_s', 'gettested', 'range', 0.3, 2.8, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('histamine_blood', 'gettested', 'max_only', NULL, 0.92, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('ido_activity', 'gettested', 'range', 18, 49, 'ratio', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik. Kynurenine/Tryptophan ratio');

-- =============================================================================
-- BLOOD PANEL — Metabolic Markers (4 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('betaine', 'gettested', 'range', 20, 68, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('lactate', 'gettested', 'range', 1490, 5440, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('spermidine', 'gettested', 'range', 2.7, 8.2, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik'),
('spermine', 'gettested', 'range', 1.9, 10.1, 'µmol/l', 'adult', NULL, 'BodyPanel Large, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Aerobic Bacteria (9 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('escherichia_coli', 'gettested', 'range', 1.0e6, 1.0e7, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('escherichia_coli_biovare', 'gettested', 'max_only', NULL, 1.0e4, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('proteus_spp', 'gettested', 'max_only', NULL, 1.0e4, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('klebsiella_spp', 'gettested', 'max_only', NULL, 1.0e4, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('pseudomonas_spp', 'gettested', 'max_only', NULL, 1.0e4, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('enterobacter_spp', 'gettested', 'max_only', NULL, 1.0e4, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('serratia_spp', 'gettested', 'max_only', NULL, 1.0e4, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('hafnia_spp', 'gettested', 'max_only', NULL, 1.0e4, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('enterococcus_spp', 'gettested', 'range', 1.0e6, 1.0e7, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Anaerobic Bacteria (4 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('bifidobacterium_spp', 'gettested', 'range', 1.0e9, 1.0e11, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('bacteroides_spp', 'gettested', 'range', 1.0e9, 1.0e11, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('lactobacillus_spp', 'gettested', 'range', 1.0e5, 1.0e7, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('clostridium_spp', 'gettested', 'max_only', NULL, 1.0e5, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Mycological (3 ranges — yeast excluded, qualitative only)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('candida_spp', 'gettested', 'max_only', NULL, 1.0e3, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('candida_albicans', 'gettested', 'max_only', NULL, 1.0e3, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal'),
('geotrichum_candidum', 'gettested', 'max_only', NULL, 1.0e3, 'CFU/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. Below detection = normal');

-- =============================================================================
-- MICROBIOME — Stool Properties (2 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('ph_value', 'gettested', 'range', 5.8, 6.5, NULL, 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('water_content', 'gettested', 'range', 75, 85, 'g/100g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Digestive Residues (3 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('quantitative_fat', 'gettested', 'max_only', NULL, 3.5, 'g/100g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('quantitative_nitrogen', 'gettested', 'max_only', NULL, 1.0, 'g/100g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('quantitative_sugar', 'gettested', 'max_only', NULL, 2.5, 'g/100g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Digestive Function (2 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('pancreatic_elastase', 'gettested', 'min_only', 200, NULL, 'µg/g', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik. >200 = normal, 100-200 = moderate insufficiency, <100 = severe'),
('bile_acids_stool', 'gettested', 'max_only', NULL, 70, 'µmol/l', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Gut Inflammation (2 ranges)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('calprotectin', 'gettested', 'max_only', NULL, 50, 'mg/l', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik'),
('alpha1_antitrypsin', 'gettested', 'max_only', NULL, 27.5, 'mg/dl', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Mucosal Immunity (1 range)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('secretory_iga', 'gettested', 'range', 510, 2040, 'µg/ml', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Gut Permeability (1 range)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('zonulin', 'gettested', 'max_only', NULL, 55, 'ng/ml', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- MICROBIOME — Stress & Intolerance (1 range)
-- =============================================================================

INSERT INTO silver.dim_reference_range (marker_key, source, reference_type, reference_min, reference_max, unit, age_group, sex, notes)
VALUES
('histamine_stool', 'gettested', 'max_only', NULL, 959, 'ng/ml', 'adult', NULL, 'Gut Microbiome XL, biovis Diagnostik');

-- =============================================================================
-- REFERENCE RANGE COUNT VERIFICATION
-- Blood panel:  5 + 6 + 13 + 11 + 21 + 4 + 4 + 4 = 68 ranges
-- Microbiome:   9 + 4 + 3 + 2 + 3 + 2 + 2 + 1 + 1 + 1 = 28 ranges
--   (yeast excluded — qualitative marker with no numeric reference range)
-- GRAND TOTAL: 96 reference ranges
-- =============================================================================
