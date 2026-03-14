-- Seed: WHO ATC classification codes
-- Top-level groups + commonly prescribed Danish medications
-- Used by the sundhed.dk medication pipeline for classification and enrichment
-- DuckDB-compatible SQL

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.ref_atc_codes (
    atc_code    VARCHAR NOT NULL,
    atc_name    VARCHAR NOT NULL,
    atc_group   VARCHAR NOT NULL
);

-- Truncate before re-seeding to allow idempotent runs
DELETE FROM reference.ref_atc_codes WHERE 1=1;

INSERT INTO reference.ref_atc_codes (atc_code, atc_name, atc_group) VALUES
-- =============================================================================
-- ATC Top-Level Groups
-- =============================================================================
('A',       'Alimentary tract and metabolism',          'Alimentary tract and metabolism'),
('B',       'Blood and blood forming organs',           'Blood and blood forming organs'),
('C',       'Cardiovascular system',                    'Cardiovascular system'),
('D',       'Dermatologicals',                          'Dermatologicals'),
('G',       'Genitourinary system and sex hormones',    'Genitourinary system and sex hormones'),
('H',       'Systemic hormonal preparations',           'Systemic hormonal preparations'),
('J',       'Antiinfectives for systemic use',          'Antiinfectives for systemic use'),
('L',       'Antineoplastic and immunomodulating agents', 'Antineoplastic and immunomodulating agents'),
('M',       'Musculoskeletal system',                   'Musculoskeletal system'),
('N',       'Nervous system',                           'Nervous system'),
('P',       'Antiparasitic products',                   'Antiparasitic products'),
('R',       'Respiratory system',                       'Respiratory system'),
('S',       'Sensory organs',                           'Sensory organs'),
('V',       'Various',                                  'Various'),

-- =============================================================================
-- A — Alimentary tract and metabolism
-- =============================================================================
('A02BC01', 'Omeprazol',                                'Alimentary tract and metabolism'),
('A02BC02', 'Pantoprazol',                              'Alimentary tract and metabolism'),
('A02BC03', 'Lansoprazol',                              'Alimentary tract and metabolism'),
('A02BC05', 'Esomeprazol',                              'Alimentary tract and metabolism'),
('A03FA01', 'Metoclopramid',                            'Alimentary tract and metabolism'),
('A06AD11', 'Lactulose',                                'Alimentary tract and metabolism'),
('A06AD65', 'Macrogol, kombinationer',                  'Alimentary tract and metabolism'),
('A07EA06', 'Budesonid (GI)',                           'Alimentary tract and metabolism'),
('A10AB05', 'Insulin aspart',                           'Alimentary tract and metabolism'),
('A10AB06', 'Insulin glulisin',                         'Alimentary tract and metabolism'),
('A10AE04', 'Insulin glargin',                          'Alimentary tract and metabolism'),
('A10AE05', 'Insulin detemir',                          'Alimentary tract and metabolism'),
('A10AE06', 'Insulin degludec',                         'Alimentary tract and metabolism'),
('A10BA02', 'Metformin',                                'Alimentary tract and metabolism'),
('A10BD07', 'Metformin og sitagliptin',                 'Alimentary tract and metabolism'),
('A10BJ02', 'Liraglutid',                               'Alimentary tract and metabolism'),
('A10BJ06', 'Semaglutid',                               'Alimentary tract and metabolism'),
('A10BK01', 'Dapagliflozin',                            'Alimentary tract and metabolism'),
('A10BK03', 'Empagliflozin',                            'Alimentary tract and metabolism'),
('A11CC05', 'Colecalciferol (Vitamin D3)',              'Alimentary tract and metabolism'),
('A12AA04', 'Calciumcarbonat',                          'Alimentary tract and metabolism'),

-- =============================================================================
-- B — Blood and blood forming organs
-- =============================================================================
('B01AA03', 'Warfarin',                                 'Blood and blood forming organs'),
('B01AB05', 'Enoxaparin',                               'Blood and blood forming organs'),
('B01AC06', 'Acetylsalicylsyre (lavdosis)',             'Blood and blood forming organs'),
('B01AF01', 'Rivaroxaban',                              'Blood and blood forming organs'),
('B01AF02', 'Apixaban',                                 'Blood and blood forming organs'),
('B01AF03', 'Edoxaban',                                 'Blood and blood forming organs'),
('B03AA07', 'Jernfumarat',                               'Blood and blood forming organs'),
('B03BB01', 'Folsyre',                                  'Blood and blood forming organs'),

-- =============================================================================
-- C — Cardiovascular system
-- =============================================================================
('C01AA05', 'Digoxin',                                  'Cardiovascular system'),
('C01BD01', 'Amiodaron',                                'Cardiovascular system'),
('C03CA01', 'Furosemid',                                'Cardiovascular system'),
('C03DA01', 'Spironolacton',                            'Cardiovascular system'),
('C03DA04', 'Eplerenon',                                'Cardiovascular system'),
('C07AB02', 'Metoprolol',                               'Cardiovascular system'),
('C07AB07', 'Bisoprolol',                               'Cardiovascular system'),
('C07AG02', 'Carvedilol',                               'Cardiovascular system'),
('C08CA01', 'Amlodipin',                                'Cardiovascular system'),
('C08CA13', 'Lercanidipin',                             'Cardiovascular system'),
('C09AA02', 'Enalapril',                                'Cardiovascular system'),
('C09AA05', 'Ramipril',                                 'Cardiovascular system'),
('C09CA01', 'Losartan',                                 'Cardiovascular system'),
('C09CA06', 'Candesartan',                              'Cardiovascular system'),
('C09CA08', 'Olmesartan',                               'Cardiovascular system'),
('C09DX04', 'Sacubitril og valsartan',                  'Cardiovascular system'),
('C10AA01', 'Simvastatin',                              'Cardiovascular system'),
('C10AA05', 'Atorvastatin',                             'Cardiovascular system'),
('C10AA07', 'Rosuvastatin',                             'Cardiovascular system'),
('C10AX09', 'Ezetimib',                                 'Cardiovascular system'),

-- =============================================================================
-- D — Dermatologicals
-- =============================================================================
('D01AC01', 'Clotrimazol',                              'Dermatologicals'),
('D07AC01', 'Betamethason (dermal)',                    'Dermatologicals'),
('D10BA01', 'Isotretinoin',                             'Dermatologicals'),

-- =============================================================================
-- G — Genitourinary system and sex hormones
-- =============================================================================
('G03AA12', 'Drospirenon og ethinylestradiol',          'Genitourinary system and sex hormones'),
('G04CA02', 'Tamsulosin',                               'Genitourinary system and sex hormones'),

-- =============================================================================
-- H — Systemic hormonal preparations
-- =============================================================================
('H02AB06', 'Prednisolon',                              'Systemic hormonal preparations'),
('H02AB09', 'Hydrocortison',                            'Systemic hormonal preparations'),
('H03AA01', 'Levothyroxin',                             'Systemic hormonal preparations'),
('H03BB01', 'Carbimazol',                               'Systemic hormonal preparations'),

-- =============================================================================
-- J — Antiinfectives for systemic use
-- =============================================================================
('J01CA04', 'Amoxicillin',                              'Antiinfectives for systemic use'),
('J01CR02', 'Amoxicillin og clavulansyre',              'Antiinfectives for systemic use'),
('J01DB01', 'Cefalexin',                                'Antiinfectives for systemic use'),
('J01FA09', 'Clarithromycin',                           'Antiinfectives for systemic use'),
('J01FA10', 'Azithromycin',                             'Antiinfectives for systemic use'),
('J01MA02', 'Ciprofloxacin',                            'Antiinfectives for systemic use'),
('J01XE01', 'Nitrofurantoin',                           'Antiinfectives for systemic use'),
('J01XX01', 'Fosfomycin',                               'Antiinfectives for systemic use'),
('J05AE10', 'Darunavir',                                'Antiinfectives for systemic use'),

-- =============================================================================
-- L — Antineoplastic and immunomodulating agents
-- =============================================================================
('L01XE01', 'Imatinib',                                 'Antineoplastic and immunomodulating agents'),
('L04AA27', 'Fingolimod',                               'Antineoplastic and immunomodulating agents'),
('L04AB02', 'Infliximab',                               'Antineoplastic and immunomodulating agents'),
('L04AB04', 'Adalimumab',                               'Antineoplastic and immunomodulating agents'),
('L04AX03', 'Methotrexat',                              'Antineoplastic and immunomodulating agents'),

-- =============================================================================
-- M — Musculoskeletal system
-- =============================================================================
('M01AE01', 'Ibuprofen',                                'Musculoskeletal system'),
('M01AE02', 'Naproxen',                                 'Musculoskeletal system'),
('M01AH01', 'Celecoxib',                                'Musculoskeletal system'),
('M04AA01', 'Allopurinol',                              'Musculoskeletal system'),
('M05BA04', 'Alendronsyre',                             'Musculoskeletal system'),

-- =============================================================================
-- N — Nervous system
-- =============================================================================
('N02AA01', 'Morphin',                                  'Nervous system'),
('N02AA05', 'Oxycodon',                                 'Nervous system'),
('N02AX02', 'Tramadol',                                 'Nervous system'),
('N02BE01', 'Paracetamol',                              'Nervous system'),
('N03AX09', 'Lamotrigin',                               'Nervous system'),
('N03AX14', 'Levetiracetam',                            'Nervous system'),
('N05AH03', 'Olanzapin',                                'Nervous system'),
('N05AH04', 'Quetiapin',                                'Nervous system'),
('N05AX08', 'Risperidon',                               'Nervous system'),
('N05BA01', 'Diazepam',                                 'Nervous system'),
('N05CF01', 'Zopiclon',                                 'Nervous system'),
('N05CF02', 'Zolpidem',                                 'Nervous system'),
('N06AA09', 'Amitriptylin',                             'Nervous system'),
('N06AB04', 'Citalopram',                               'Nervous system'),
('N06AB06', 'Sertralin',                                'Nervous system'),
('N06AB10', 'Escitalopram',                             'Nervous system'),
('N06AX11', 'Mirtazapin',                               'Nervous system'),
('N06AX16', 'Venlafaxin',                               'Nervous system'),
('N06AX21', 'Duloxetin',                                'Nervous system'),

-- =============================================================================
-- P — Antiparasitic products
-- =============================================================================
('P01AB01', 'Metronidazol (antiparasitisk)',            'Antiparasitic products'),
('P02CA01', 'Mebendazol',                              'Antiparasitic products'),

-- =============================================================================
-- R — Respiratory system
-- =============================================================================
('R01AD09', 'Mometason (nasal)',                        'Respiratory system'),
('R03AC02', 'Salbutamol',                               'Respiratory system'),
('R03AK06', 'Salmeterol og fluticason',                 'Respiratory system'),
('R03BA02', 'Budesonid (inhalation)',                   'Respiratory system'),
('R03BB04', 'Tiotropium',                               'Respiratory system'),
('R06AE07', 'Cetirizin',                                'Respiratory system'),
('R06AX13', 'Loratadin',                                'Respiratory system'),
('R06AX27', 'Desloratadin',                             'Respiratory system'),

-- =============================================================================
-- S — Sensory organs
-- =============================================================================
('S01ED51', 'Timolol, kombinationer',                   'Sensory organs'),
('S01EE01', 'Latanoprost',                              'Sensory organs'),

-- =============================================================================
-- V — Various
-- =============================================================================
('V03AB15', 'Naloxon',                                  'Various'),
('V04CX',   'Andre diagnostiske midler',                'Various');
