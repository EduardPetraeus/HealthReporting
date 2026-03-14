-- seed_dim_marker_catalog.sql
-- Populates silver.dim_marker_catalog with all markers from:
--   - blood_panel_2026-02-24.yaml (68 markers)
--   - microbiome_2026-02-23.yaml (29 markers)
-- Total: 97 markers
-- Idempotent: DELETE + INSERT

DELETE FROM silver.dim_marker_catalog WHERE 1=1;

-- =============================================================================
-- BLOOD PANEL — Vitamins & Minerals (5 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('iron', 'Iron', 'minerals', 'nutritional', 'mg/l', 'Serum iron level, essential for oxygen transport and energy metabolism', 'numeric', false, NULL, 'S-Jern|Serum Iron|Fe'),
('magnesium', 'Magnesium', 'minerals', 'nutritional', 'mg/l', 'Serum magnesium, critical for muscle function, nerve signaling, and enzymatic reactions', 'numeric', false, NULL, 'S-Magnesium|Mg'),
('selenium', 'Selenium', 'minerals', 'nutritional', 'µg/l', 'Trace mineral essential for thyroid function, antioxidant defense, and immune health', 'numeric', false, NULL, 'S-Selen|Se'),
('vitamin_d3_25oh', 'Vitamin D3 (25-OH)', 'vitamins', 'nutritional', 'ng/ml', '25-hydroxyvitamin D, primary circulating form of vitamin D, indicator of vitamin D status', 'numeric', false, NULL, '25-OH-D3|Calcidiol|D-vitamin'),
('zinc', 'Zinc', 'minerals', 'nutritional', 'mg/l', 'Essential trace element for immune function, wound healing, and protein synthesis', 'numeric', false, NULL, 'S-Zink|Zn');

-- =============================================================================
-- BLOOD PANEL — Gut & Bacterial Metabolites (6 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('indole_3_acetic_acid', 'Indole-3-Acetic Acid', 'gut_metabolites', 'gastrointestinal', 'µmol/l', 'Tryptophan metabolite produced by gut bacteria, indicator of microbial diversity and gut health', 'numeric', false, NULL, 'IAA|Indol-3-eddikesyre'),
('hippuric_acid', 'Hippuric Acid', 'gut_metabolites', 'gastrointestinal', 'µmol/l', 'Microbial-host co-metabolite reflecting polyphenol metabolism and gut bacterial activity', 'numeric', false, NULL, 'Hippursyre'),
('indoxyl_sulfate', 'Indoxyl Sulfate', 'gut_metabolites', 'gastrointestinal', 'µmol/l', 'Uremic toxin produced from tryptophan by gut bacteria, marker of protein fermentation', 'numeric', false, NULL, 'Indoxylsulfat'),
('indolepropionic_acid', 'Indolepropionic Acid', 'gut_metabolites', 'gastrointestinal', 'µmol/l', 'Potent antioxidant produced by Clostridium sporogenes, protective for gut barrier integrity', 'numeric', false, NULL, 'IPA|Indolpropionsyre'),
('p_cresol_sulfate', 'p-Cresol Sulfate', 'gut_metabolites', 'gastrointestinal', 'µmol/l', 'Protein fermentation byproduct, uremic toxin reflecting gut proteolytic bacterial activity', 'numeric', false, NULL, 'pCS|p-Kresolsulfat'),
('putrescine', 'Putrescine', 'gut_metabolites', 'gastrointestinal', 'µmol/l', 'Polyamine produced by bacterial decarboxylation, marker of protein putrefaction in the gut', 'numeric', false, NULL, 'Putrescin');

-- =============================================================================
-- BLOOD PANEL — Fatty Acids & Lipid Metabolism (13 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('alpha_linolenic_acid', 'Alpha-Linolenic Acid', 'fatty_acids', 'cardiovascular', '%', 'Essential omega-3 fatty acid (ALA), precursor to EPA and DHA', 'numeric', false, NULL, 'ALA|Alpha-linolensyre'),
('arachidonic_acid', 'Arachidonic Acid', 'fatty_acids', 'cardiovascular', '%', 'Omega-6 fatty acid, precursor to pro-inflammatory eicosanoids', 'numeric', false, NULL, 'AA|Arakidonsyre'),
('dgla', 'DGLA', 'fatty_acids', 'cardiovascular', '%', 'Dihomo-gamma-linolenic acid, anti-inflammatory omega-6 precursor', 'numeric', false, NULL, 'Dihomo-gamma-linolensyre'),
('dha', 'DHA', 'fatty_acids', 'cardiovascular', '%', 'Docosahexaenoic acid, critical omega-3 for brain and cardiovascular health', 'numeric', false, NULL, 'Docosahexaensyre'),
('epa', 'EPA', 'fatty_acids', 'cardiovascular', '%', 'Eicosapentaenoic acid, anti-inflammatory omega-3 fatty acid', 'numeric', false, NULL, 'Eicosapentaensyre'),
('gla', 'GLA', 'fatty_acids', 'cardiovascular', '%', 'Gamma-linolenic acid, anti-inflammatory omega-6 fatty acid', 'numeric', false, NULL, 'Gamma-linolensyre'),
('lc_omega3_index', 'LC Omega-3 Index', 'fatty_acids', 'cardiovascular', 'ratio', 'Long-chain omega-3 index (EPA+DHA as % of total fatty acids in erythrocyte membranes)', 'numeric', false, NULL, 'Omega-3 Index'),
('linoleic_acid', 'Linoleic Acid', 'fatty_acids', 'cardiovascular', '%', 'Essential omega-6 fatty acid, most abundant polyunsaturated fatty acid in diet', 'numeric', false, NULL, 'LA|Linolsyre'),
('omega6_3_ratio', 'Omega-6/3 Ratio', 'fatty_acids', 'cardiovascular', 'ratio', 'Ratio of omega-6 to omega-3 fatty acids, indicator of inflammatory balance', 'numeric', false, NULL, 'Omega-6/Omega-3 Ratio'),
('oleic_acid', 'Oleic Acid', 'fatty_acids', 'cardiovascular', '%', 'Monounsaturated omega-9 fatty acid, primary fat in olive oil', 'numeric', false, NULL, 'Oliesyre'),
('palmitic_acid', 'Palmitic Acid', 'fatty_acids', 'cardiovascular', '%', 'Most common saturated fatty acid, associated with de novo lipogenesis', 'numeric', false, NULL, 'Palmitinsyre'),
('stearic_acid', 'Stearic Acid', 'fatty_acids', 'cardiovascular', '%', 'Saturated fatty acid with neutral effect on cholesterol levels', 'numeric', false, NULL, 'Stearinsyre'),
('eicosanoid_balance', 'Eicosanoid Balance', 'fatty_acids', 'cardiovascular', 'ratio', 'Ratio reflecting balance between pro- and anti-inflammatory eicosanoid precursors', 'numeric', false, NULL, 'Eicosanoid-balance');

-- =============================================================================
-- BLOOD PANEL — Bile Acid Metabolism (11 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('cdca', 'Chenodeoxycholic Acid', 'bile_acids', 'hepatic', '%', 'Primary bile acid produced in the liver, FXR agonist regulating bile acid synthesis', 'numeric', false, NULL, 'CDCA|Chenodeoxycholsyre'),
('cholic_acid', 'Cholic Acid', 'bile_acids', 'hepatic', '%', 'Primary bile acid synthesized from cholesterol in the liver', 'numeric', false, NULL, 'CA|Cholsyre'),
('deoxycholic_acid', 'Deoxycholic Acid', 'bile_acids', 'hepatic', '%', 'Secondary bile acid produced by bacterial 7-alpha dehydroxylation of cholic acid', 'numeric', false, NULL, 'DCA|Deoxycholsyre'),
('total_bile_acids', 'Total Bile Acids', 'bile_acids', 'hepatic', 'µmol/l', 'Sum of all bile acids in serum, marker of hepatobiliary function', 'numeric', false, NULL, 'Total galdesyrer'),
('lithocholic_acid', 'Lithocholic Acid', 'bile_acids', 'hepatic', '%', 'Secondary bile acid, potentially hepatotoxic, detoxified by sulfation', 'numeric', false, NULL, 'LCA|Lithocholsyre'),
('primary_bile_acids', 'Primary Bile Acids', 'bile_acids', 'hepatic', '%', 'Sum of liver-synthesized bile acids (CDCA + CA), reflecting hepatic synthesis', 'numeric', false, NULL, 'Primaere galdesyrer'),
('secondary_bile_acids', 'Secondary Bile Acids', 'bile_acids', 'hepatic', '%', 'Bacterially modified bile acids (DCA + LCA), reflecting gut microbial metabolism', 'numeric', false, NULL, 'Sekundaere galdesyrer'),
('secondary_primary_ratio', 'Secondary/Primary Bile Acid Ratio', 'bile_acids', 'hepatic', 'ratio', 'Ratio of secondary to primary bile acids, indicator of gut bacterial bile acid metabolism', 'numeric', false, NULL, 'Sekundaer/primaer ratio'),
('tertiary_bile_acids', 'Tertiary Bile Acids', 'bile_acids', 'hepatic', '%', 'Re-hydroxylated bile acids (UDCA), reflecting hepatic detoxification capacity', 'numeric', false, NULL, 'Tertiaere galdesyrer'),
('udca', 'Ursodeoxycholic Acid', 'bile_acids', 'hepatic', '%', 'Tertiary bile acid with hepatoprotective and anti-inflammatory properties', 'numeric', false, NULL, 'UDCA|Ursodeoxycholsyre'),
('cytotoxic_neuroprotective_ratio', 'Cytotoxic/Neuroprotective Ratio', 'bile_acids', 'hepatic', 'ratio', 'Ratio of cytotoxic (DCA+LCA) to neuroprotective (UDCA) bile acids', 'numeric', false, NULL, 'Cytotoksisk/neuroprotektiv ratio');

-- =============================================================================
-- BLOOD PANEL — Amino Acids (21 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('alanine', 'Alanine', 'amino_acids', 'metabolic', 'µmol/l', 'Non-essential amino acid, key substrate in gluconeogenesis and glucose-alanine cycle', 'numeric', false, NULL, 'Alanin'),
('arginine', 'Arginine', 'amino_acids', 'metabolic', 'µmol/l', 'Semi-essential amino acid, precursor to nitric oxide and critical for vascular function', 'numeric', false, NULL, 'Arginin'),
('asparagine', 'Asparagine', 'amino_acids', 'metabolic', 'µmol/l', 'Non-essential amino acid involved in protein synthesis and nitrogen metabolism', 'numeric', false, NULL, 'Asparagin'),
('aspartic_acid', 'Aspartic Acid', 'amino_acids', 'metabolic', 'µmol/l', 'Non-essential amino acid, key in urea cycle and neurotransmitter synthesis', 'numeric', false, NULL, 'Asparaginsyre'),
('citrulline', 'Citrulline', 'amino_acids', 'metabolic', 'µmol/l', 'Non-proteinogenic amino acid, marker of intestinal absorptive capacity and urea cycle function', 'numeric', false, NULL, 'Citrullin'),
('gaba', 'GABA', 'amino_acids', 'metabolic', 'µmol/l', 'Gamma-aminobutyric acid, primary inhibitory neurotransmitter in the central nervous system', 'numeric', false, NULL, 'Gamma-aminosmorsyre'),
('glutamine', 'Glutamine', 'amino_acids', 'metabolic', 'µmol/l', 'Most abundant amino acid, primary fuel for enterocytes and immune cells', 'numeric', false, NULL, 'Glutamin'),
('glutamic_acid', 'Glutamic Acid', 'amino_acids', 'metabolic', 'µmol/l', 'Excitatory neurotransmitter precursor, central to nitrogen metabolism', 'numeric', false, NULL, 'Glutaminsyre'),
('glycine', 'Glycine', 'amino_acids', 'metabolic', 'µmol/l', 'Simplest amino acid, involved in collagen synthesis, glutathione production, and bile conjugation', 'numeric', false, NULL, 'Glycin'),
('histidine', 'Histidine', 'amino_acids', 'metabolic', 'µmol/l', 'Essential amino acid, precursor to histamine and carnosine', 'numeric', false, NULL, 'Histidin'),
('isoleucine', 'Isoleucine', 'amino_acids', 'metabolic', 'µmol/l', 'Branched-chain amino acid (BCAA), essential for muscle protein synthesis and energy production', 'numeric', false, NULL, 'Isoleucin'),
('kynurenine', 'Kynurenine', 'amino_acids', 'metabolic', 'µmol/l', 'Tryptophan metabolite in the kynurenine pathway, marker of immune activation and IDO activity', 'numeric', false, NULL, 'Kynurenin'),
('leucine', 'Leucine', 'amino_acids', 'metabolic', 'µmol/l', 'Branched-chain amino acid (BCAA), primary activator of mTOR and muscle protein synthesis', 'numeric', false, NULL, 'Leucin'),
('lysine', 'Lysine', 'amino_acids', 'metabolic', 'µmol/l', 'Essential amino acid, important for carnitine synthesis and collagen cross-linking', 'numeric', false, NULL, 'Lysin'),
('phenylalanine', 'Phenylalanine', 'amino_acids', 'metabolic', 'µmol/l', 'Essential amino acid, precursor to tyrosine, dopamine, and catecholamines', 'numeric', false, NULL, 'Fenylalanin|Phenylalanin'),
('proline', 'Proline', 'amino_acids', 'metabolic', 'µmol/l', 'Non-essential amino acid, major component of collagen and important for tissue repair', 'numeric', false, NULL, 'Prolin'),
('taurine', 'Taurine', 'amino_acids', 'metabolic', 'µmol/l', 'Sulfur-containing amino acid, important for bile salt conjugation and cellular osmoregulation', 'numeric', false, NULL, 'Taurin'),
('threonine', 'Threonine', 'amino_acids', 'metabolic', 'µmol/l', 'Essential amino acid, important for mucin production and gut barrier integrity', 'numeric', false, NULL, 'Threonin'),
('tyrosine', 'Tyrosine', 'amino_acids', 'metabolic', 'µmol/l', 'Conditionally essential amino acid, precursor to thyroid hormones and catecholamines', 'numeric', false, NULL, 'Tyrosin'),
('tryptophan', 'Tryptophan', 'amino_acids', 'metabolic', 'µmol/l', 'Essential amino acid, precursor to serotonin, melatonin, and kynurenine pathway metabolites', 'numeric', false, NULL, 'Tryptofan'),
('valine', 'Valine', 'amino_acids', 'metabolic', 'µmol/l', 'Branched-chain amino acid (BCAA), essential for muscle metabolism and tissue repair', 'numeric', false, NULL, 'Valin');

-- =============================================================================
-- BLOOD PANEL — Energy, Detox & Cellular Protection (4 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('adma', 'ADMA', 'cellular_protection', 'cardiovascular', 'µmol/l', 'Asymmetric dimethylarginine, endogenous nitric oxide synthase inhibitor and cardiovascular risk marker', 'numeric', false, NULL, 'Asymmetrisk dimethylarginin'),
('carnitine', 'Carnitine', 'cellular_protection', 'metabolic', 'µmol/l', 'Transports long-chain fatty acids into mitochondria for beta-oxidation and energy production', 'numeric', false, NULL, 'L-Carnitin|Karnitin'),
('choline', 'Choline', 'cellular_protection', 'metabolic', 'µmol/l', 'Essential nutrient for phospholipid synthesis, neurotransmitter production, and methyl group metabolism', 'numeric', false, NULL, 'Kolin'),
('sdma', 'SDMA', 'cellular_protection', 'renal', 'µmol/l', 'Symmetric dimethylarginine, marker of renal function and glomerular filtration rate', 'numeric', false, NULL, 'Symmetrisk dimethylarginin');

-- =============================================================================
-- BLOOD PANEL — Hormones & Stress Markers (4 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('cortisol', 'Cortisol', 'hormones', 'endocrine', 'µmol/l', 'Primary stress hormone produced by the adrenal cortex, regulates metabolism and immune response', 'numeric', false, NULL, 'Kortisol'),
('dhea_s', 'DHEA-S', 'hormones', 'endocrine', 'µmol/l', 'Dehydroepiandrosterone sulfate, adrenal androgen precursor and marker of adrenal function and aging', 'numeric', false, NULL, 'DHEA-sulfat|Dehydroepiandrosteron'),
('histamine_blood', 'Histamine (Blood)', 'hormones', 'immune', 'µmol/l', 'Blood histamine level, indicator of mast cell activation and allergic/inflammatory response', 'numeric', false, NULL, 'Histamin (blod)'),
('ido_activity', 'IDO Activity', 'hormones', 'immune', 'ratio', 'Indoleamine 2,3-dioxygenase activity ratio (kynurenine/tryptophan), marker of immune-mediated tryptophan degradation', 'numeric', false, NULL, 'IDO-aktivitet');

-- =============================================================================
-- BLOOD PANEL — Metabolic Markers (4 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('betaine', 'Betaine', 'metabolic', 'metabolic', 'µmol/l', 'Trimethylglycine, methyl donor in homocysteine metabolism and osmoprotectant', 'numeric', false, NULL, 'Betain|Trimethylglycin'),
('lactate', 'Lactate', 'metabolic', 'metabolic', 'µmol/l', 'End product of anaerobic glycolysis, marker of tissue oxygenation and metabolic stress', 'numeric', false, NULL, 'Laktat|Mælkesyre'),
('spermidine', 'Spermidine', 'metabolic', 'metabolic', 'µmol/l', 'Polyamine that induces autophagy, associated with cellular renewal and longevity', 'numeric', false, NULL, 'Spermidin'),
('spermine', 'Spermine', 'metabolic', 'metabolic', 'µmol/l', 'Polyamine involved in cell growth, differentiation, and stabilization of DNA structure', 'numeric', false, NULL, 'Spermin');

-- =============================================================================
-- MICROBIOME — Aerobic Bacteria (9 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('escherichia_coli', 'Escherichia coli', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Commensal gram-negative bacterium, produces vitamin K and B12, indicator of aerobic gut flora balance', 'numeric', true, NULL, 'E. coli'),
('escherichia_coli_biovare', 'Escherichia coli Biovare', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Atypical E. coli variants (lactose-negative or hemolytic), potentially pathogenic biovars', 'numeric', true, 10000, 'E. coli biovare'),
('proteus_spp', 'Proteus spp.', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Opportunistic gram-negative bacterium, can cause urinary tract infections when overgrown', 'numeric', true, 10000, 'Proteus'),
('klebsiella_spp', 'Klebsiella spp.', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Gram-negative opportunistic pathogen, associated with dysbiosis when elevated', 'numeric', true, 10000, 'Klebsiella'),
('pseudomonas_spp', 'Pseudomonas spp.', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Gram-negative aerobic bacterium, opportunistic pathogen in immunocompromised states', 'numeric', true, 10000, 'Pseudomonas'),
('enterobacter_spp', 'Enterobacter spp.', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Gram-negative opportunistic pathogen from the Enterobacteriaceae family', 'numeric', true, 10000, 'Enterobacter'),
('serratia_spp', 'Serratia spp.', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Gram-negative bacterium, opportunistic pathogen associated with nosocomial infections', 'numeric', true, 10000, 'Serratia'),
('hafnia_spp', 'Hafnia spp.', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Gram-negative enterobacterium, produces histamine via histidine decarboxylase activity', 'numeric', true, 10000, 'Hafnia'),
('enterococcus_spp', 'Enterococcus spp.', 'aerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Gram-positive facultative anaerobe, important for gut immune modulation and colonization resistance', 'numeric', true, 10000, 'Enterococcus|Enterokokker');

-- =============================================================================
-- MICROBIOME — Anaerobic Bacteria (4 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('bifidobacterium_spp', 'Bifidobacterium spp.', 'anaerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Key probiotic genus producing short-chain fatty acids, supports gut barrier and immune function', 'numeric', true, NULL, 'Bifidobacterium|Bifidobakterier'),
('bacteroides_spp', 'Bacteroides spp.', 'anaerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Dominant anaerobic genus, critical for polysaccharide digestion and bile acid metabolism', 'numeric', true, NULL, 'Bacteroides'),
('lactobacillus_spp', 'Lactobacillus spp.', 'anaerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Lactic acid-producing probiotic genus, maintains acidic pH and inhibits pathogen colonization', 'numeric', true, 10000, 'Lactobacillus|Laktobaciller|Maelkesyrebakterier'),
('clostridium_spp', 'Clostridium spp.', 'anaerobic_bacteria', 'gastrointestinal', 'CFU/g', 'Anaerobic spore-forming genus, some species produce butyrate while others are pathogenic', 'numeric', true, 100000, 'Clostridium');

-- =============================================================================
-- MICROBIOME — Mycological (4 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('candida_spp', 'Candida spp.', 'mycological', 'gastrointestinal', 'CFU/g', 'Opportunistic yeast genus, overgrowth associated with dysbiosis and mucosal infection', 'numeric', true, 1000, 'Candida'),
('candida_albicans', 'Candida albicans', 'mycological', 'gastrointestinal', 'CFU/g', 'Most common pathogenic Candida species, can cause oral and intestinal candidiasis', 'numeric', true, 1000, 'C. albicans'),
('yeast', 'Yeast (General)', 'mycological', 'gastrointestinal', NULL, 'General yeast screening, qualitative assessment of fungal presence in stool', 'categorical', false, NULL, 'Gaer|Gaersvampe'),
('geotrichum_candidum', 'Geotrichum candidum', 'mycological', 'gastrointestinal', 'CFU/g', 'Dimorphic fungus found in gut flora, opportunistic in immunocompromised patients', 'numeric', true, 1000, 'Geotrichum');

-- =============================================================================
-- MICROBIOME — Digestive Residues & Stool Properties (5 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('ph_value', 'Stool pH', 'digestive_residues', 'gastrointestinal', NULL, 'Stool acidity/alkalinity, reflects fermentation patterns and bacterial metabolic activity', 'numeric', false, NULL, 'pH-vaerdi|Faeces pH'),
('water_content', 'Stool Water Content', 'digestive_residues', 'gastrointestinal', 'g/100g', 'Percentage of water in stool, indicator of colonic transit time and absorption', 'numeric', false, NULL, 'Vandindhold'),
('quantitative_fat', 'Quantitative Fat (Stool)', 'digestive_residues', 'gastrointestinal', 'g/100g', 'Fecal fat content, elevated levels indicate fat malabsorption or pancreatic insufficiency', 'numeric', false, NULL, 'Fedt (faeces)|Steatorrhoe'),
('quantitative_nitrogen', 'Quantitative Nitrogen (Stool)', 'digestive_residues', 'gastrointestinal', 'g/100g', 'Fecal nitrogen content, marker of protein digestion and absorption efficiency', 'numeric', false, NULL, 'Nitrogen (faeces)|Kvaelstof'),
('quantitative_sugar', 'Quantitative Sugar (Stool)', 'digestive_residues', 'gastrointestinal', 'g/100g', 'Fecal sugar content, elevated levels indicate carbohydrate malabsorption', 'numeric', false, NULL, 'Sukker (faeces)');

-- =============================================================================
-- MICROBIOME — Digestive Function (2 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('pancreatic_elastase', 'Pancreatic Elastase', 'digestive_function', 'gastrointestinal', 'µg/g', 'Pancreas-specific protease, gold standard marker for exocrine pancreatic function', 'numeric', false, NULL, 'Pankreatisk elastase|Elastase-1'),
('bile_acids_stool', 'Bile Acids (Stool)', 'digestive_function', 'gastrointestinal', 'µmol/l', 'Total bile acids in stool, marker of bile acid malabsorption and enterohepatic circulation', 'numeric', false, NULL, 'Galdesyrer (faeces)');

-- =============================================================================
-- MICROBIOME — Gut Inflammation (2 markers)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('calprotectin', 'Calprotectin', 'gut_inflammation', 'gastrointestinal', 'mg/l', 'Neutrophil-derived protein, sensitive marker of intestinal mucosal inflammation', 'numeric', false, NULL, 'Calprotectin|Faekalt calprotectin'),
('alpha1_antitrypsin', 'Alpha-1 Antitrypsin (Stool)', 'gut_inflammation', 'gastrointestinal', 'mg/dl', 'Protease inhibitor, elevated in stool indicates protein-losing enteropathy or intestinal inflammation', 'numeric', false, NULL, 'A1AT (faeces)|Alpha-1 antitrypsin');

-- =============================================================================
-- MICROBIOME — Gut Permeability (1 marker)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('zonulin', 'Zonulin', 'gut_permeability', 'gastrointestinal', 'ng/ml', 'Regulator of tight junctions, elevated levels indicate increased intestinal permeability (leaky gut)', 'numeric', false, NULL, 'Zonulin');

-- =============================================================================
-- MICROBIOME — Immune Markers (1 marker)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('secretory_iga', 'Secretory IgA', 'immune_markers', 'immune', 'µg/ml', 'Primary mucosal immunoglobulin, first-line defense against pathogens in the gut lumen', 'numeric', false, NULL, 'sIgA|Sekretorisk IgA');

-- =============================================================================
-- MICROBIOME — Stress & Intolerance (1 marker)
-- =============================================================================

INSERT INTO silver.dim_marker_catalog (marker_key, display_name, marker_domain, body_system, canonical_unit, description, data_type, is_log_scale, detection_limit, synonyms)
VALUES
('histamine_stool', 'Histamine (Stool)', 'stress_intolerance', 'gastrointestinal', 'ng/ml', 'Fecal histamine level, indicator of histamine-producing bacteria or histamine intolerance', 'numeric', false, NULL, 'Histamin (faeces)');

-- =============================================================================
-- MARKER COUNT VERIFICATION
-- Blood panel:  5 (vitamins/minerals) + 6 (gut metabolites) + 13 (fatty acids)
--             + 11 (bile acids) + 21 (amino acids) + 4 (cellular protection)
--             + 4 (hormones) + 4 (metabolic) = 68
-- Microbiome:   9 (aerobic) + 4 (anaerobic) + 4 (mycological) + 5 (digestive residues)
--             + 2 (digestive function) + 2 (gut inflammation) + 1 (gut permeability)
--             + 1 (immune) + 1 (stress) = 29
-- GRAND TOTAL: 68 + 29 = 97 markers (yeast is qualitative/categorical)
-- =============================================================================
