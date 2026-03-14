-- Seed: Danish vaccination program and common vaccines
-- Source: SSI (Statens Serum Institut) vaccination program + travel vaccine guidelines
-- Used by the sundhed.dk vaccination pipeline for enrichment and scheduling
-- DuckDB-compatible SQL

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.ref_vaccine_registry (
    vaccine_product         VARCHAR NOT NULL,
    disease_target          VARCHAR NOT NULL,
    recommended_interval    VARCHAR NOT NULL
);

-- Truncate before re-seeding to allow idempotent runs
DELETE FROM reference.ref_vaccine_registry WHERE 1=1;

INSERT INTO reference.ref_vaccine_registry (vaccine_product, disease_target, recommended_interval) VALUES
-- =============================================================================
-- Danish Childhood Vaccination Program (Boernevaccinationsprogrammet)
-- =============================================================================
('DiTeKiPol/Act-Hib',          'Difteri, tetanus, kighoste, polio, Haemophilus influenzae type b',  '3, 5, 12 months'),
('DiTeKiPol Booster',          'Difteri, tetanus, kighoste, polio',                                  '5 years'),
('Prevenar 13',                'Pneumokokinfektion (13-valent)',                                      '3, 5, 12 months'),
('MFR 1',                      'Maeslinger, faaresyge, roede hunde',                                 '15 months'),
('MFR 2',                      'Maeslinger, faaresyge, roede hunde',                                 '4 years'),
('HPV (Gardasil 9)',           'Humant papillomavirus (HPV type 6,11,16,18,31,33,45,52,58)',         '12 years (2 doses, 0 + 6-12 months)'),
('DiTeKiPol Revaccination',   'Difteri, tetanus, kighoste, polio',                                  'Every 10 years from age 15 (tetanus/difteri)'),

-- =============================================================================
-- Adult Routine Vaccinations (Denmark)
-- =============================================================================
('Influenza (inactiveret)',     'Saesoninfluenza',                                                    'Yearly (autumn), age 65+ or risikogrupper'),
('Influenza (levende)',         'Saesoninfluenza',                                                    'Yearly (autumn), children 2-6 years (nasal)'),
('COVID-19 mRNA (Comirnaty)',  'SARS-CoV-2',                                                         'Primary series (2 doses, 0 + 3-8 weeks) + booster per SSI guidance'),
('COVID-19 mRNA (Spikevax)',   'SARS-CoV-2',                                                         'Primary series (2 doses, 0 + 4-8 weeks) + booster per SSI guidance'),
('Pneumokok (PPV23)',          'Pneumokokinfektion (23-valent polysaccharid)',                        'Single dose, age 65+ or risikogrupper'),
('Pneumokok (PCV20)',          'Pneumokokinfektion (20-valent konjugat)',                             'Single dose, age 65+ or risikogrupper'),
('Herpes zoster (Shingrix)',   'Helvedesild (herpes zoster)',                                         '2 doses (0 + 2-6 months), age 65+'),
('Tetanus-difteri (diTe)',     'Tetanus, difteri',                                                    'Booster every 10 years'),

-- =============================================================================
-- Hepatitis Vaccines
-- =============================================================================
('Hepatitis A (Havrix)',       'Hepatitis A',                                                         '2 doses (0 + 6-12 months)'),
('Hepatitis B (Engerix-B)',    'Hepatitis B',                                                         '3 doses (0, 1, 6 months)'),
('Hepatitis A+B (Twinrix)',    'Hepatitis A og B',                                                    '3 doses (0, 1, 6 months)'),

-- =============================================================================
-- Travel Vaccines
-- =============================================================================
('Gul feber (Stamaril)',       'Gul feber',                                                           'Single dose (lifetime protection per WHO)'),
('Tyfus, oral (Vivotif)',      'Tyfus (Salmonella typhi)',                                            '3 kapsler over 5 dage, booster every 3 years'),
('Tyfus, injicerbar (Typhim Vi)', 'Tyfus (Salmonella typhi)',                                        'Single dose, booster every 3 years'),
('Japansk encephalitis (Ixiaro)', 'Japansk encephalitis',                                            '2 doses (0 + 28 dage), booster after 12-24 months'),
('Rabies (Rabipur)',           'Rabies',                                                               'Pre-exposure: 3 doses (0, 7, 21-28 dage). Post-exposure: per WHO protocol'),
('Kolera (Dukoral)',           'Kolera (Vibrio cholerae)',                                             '2 doses oral (0 + 1-6 uger), booster every 2 years'),
('TBE (FSME-Immun)',          'Tick-borne encephalitis (TBE/FSME)',                                   '3 doses (0, 1-3, 5-12 months), booster every 3-5 years'),
('Meningokok ACWY (Nimenrix)', 'Meningokokinfektion (serogruppe A, C, W, Y)',                        'Single dose (risk areas or splenektomi)'),
('Meningokok B (Bexsero)',     'Meningokokinfektion (serogruppe B)',                                  '2 doses (0 + 1 month), booster if continued risk'),

-- =============================================================================
-- Occupational / Special Indication
-- =============================================================================
('BCG',                        'Tuberkulose (TB)',                                                     'Single dose (neonatal or occupational risk)'),
('Varicella (Varilrix)',       'Skoldkopper (varicella)',                                              '2 doses (0 + 4-8 uger), seronegative adults'),
('Polio (IPV booster)',        'Poliomyelitis',                                                        'Booster before travel to endemic areas');
