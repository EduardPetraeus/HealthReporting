-- Seed the Health Knowledge Graph
-- Populates agent.health_graph (nodes) and agent.health_graph_edges (edges)
-- with domain knowledge covering biomarkers, supplements, conditions,
-- concepts, and activities relevant to personal health optimization.

-- =============================================================================
-- NODES: Biomarkers
-- =============================================================================
INSERT INTO agent.health_graph (node_id, node_type, node_label, description, related_tables, related_columns) VALUES
('biomarker:sleep_score', 'biomarker', 'Sleep Score', 'Oura composite sleep quality score (0-100). Integrates duration, efficiency, staging, timing, and restfulness. Optimal: 75-100.', 'daily_sleep', 'sleep_score'),
('biomarker:readiness_score', 'biomarker', 'Readiness Score', 'Oura composite recovery and readiness score (0-100). Reflects capacity for physical and mental strain. Optimal: 70-100.', 'daily_readiness', 'readiness_score'),
('biomarker:activity_score', 'biomarker', 'Activity Score', 'Oura composite daily activity score (0-100). Considers movement volume, intensity mix, and training balance.', 'daily_activity', 'activity_score'),
('biomarker:resting_hr', 'biomarker', 'Resting Heart Rate', 'Lowest stable heart rate during sleep. Normal: 60-100 bpm. Athletes: 40-60 bpm. Lower (within healthy range) indicates better cardiovascular fitness.', 'heart_rate', 'bpm'),
('biomarker:hrv', 'biomarker', 'Heart Rate Variability', 'Variation in time between heartbeats (RMSSD). Higher HRV indicates better autonomic balance and recovery capacity. Highly individual; trend matters more than absolute value.', 'daily_readiness', 'contributor_hrv_balance'),
('biomarker:spo2', 'biomarker', 'Blood Oxygen Saturation', 'Peripheral oxygen saturation percentage. Normal: 95-100%. Below 90% is clinically concerning. Nocturnal dips may indicate sleep apnea.', 'blood_oxygen,daily_spo2', 'spo2,spo2_avg_pct'),
('biomarker:respiratory_rate', 'biomarker', 'Respiratory Rate', 'Breaths per minute at rest. Normal: 12-20. Elevated rates may indicate stress, illness, or cardiopulmonary issues.', 'respiratory_rate', 'breaths_per_min'),
('biomarker:body_temperature', 'biomarker', 'Body Temperature', 'Core or skin temperature. Deviations from personal baseline can indicate illness, hormonal changes, or circadian disruption.', 'body_temperature,daily_readiness', 'temperature_degc,temperature_deviation'),
('biomarker:blood_pressure_systolic', 'biomarker', 'Systolic Blood Pressure', 'Peak arterial pressure during heart contraction. Normal: <120 mmHg. Elevated: 120-129. High: >=130.', 'blood_pressure', 'systolic'),
('biomarker:blood_pressure_diastolic', 'biomarker', 'Diastolic Blood Pressure', 'Arterial pressure between heartbeats. Normal: <80 mmHg. High: >=90.', 'blood_pressure', 'diastolic'),
('biomarker:weight', 'biomarker', 'Body Weight', 'Total body mass in kg. Best tracked as 7-day rolling average to smooth daily fluctuations from hydration and meals.', 'weight', 'weight_kg'),
('biomarker:body_fat', 'biomarker', 'Body Fat Percentage', 'Proportion of body mass that is adipose tissue. Healthy: 10-20% men, 18-28% women. Measured by bioimpedance (smart scale).', 'weight', 'fat_mass_kg'),
('biomarker:muscle_mass', 'biomarker', 'Muscle Mass', 'Skeletal muscle mass in kg. Key for metabolic health and functional capacity. Declines with age (sarcopenia).', 'weight', 'muscle_mass_kg'),
('biomarker:steps', 'biomarker', 'Daily Steps', 'Total step count per day. 7000-10000 steps/day associated with significant mortality reduction. Diminishing returns above 12000.', 'daily_activity,step_count', 'steps,step_count'),
('biomarker:deep_sleep', 'biomarker', 'Deep Sleep', 'N3/slow-wave sleep duration contributor score. Adults need 1-2 hours. Critical for physical recovery, immune function, and memory consolidation.', 'daily_sleep', 'contributor_deep_sleep'),
('biomarker:rem_sleep', 'biomarker', 'REM Sleep', 'Rapid eye movement sleep duration contributor score. Adults need 1.5-2 hours. Important for emotional regulation, creativity, and learning.', 'daily_sleep', 'contributor_rem_sleep'),
('biomarker:sleep_efficiency', 'biomarker', 'Sleep Efficiency', 'Percentage of time in bed actually asleep. Optimal: >85%. Low efficiency often caused by sleep onset insomnia or frequent awakenings.', 'daily_sleep', 'contributor_efficiency'),
('biomarker:walking_speed', 'biomarker', 'Walking Speed', 'Average walking speed in km/h. Strong predictor of overall health and longevity. Typical: 4-6 km/h.', 'daily_walking_gait', 'walking_speed_avg_km_hr'),
('biomarker:walking_steadiness', 'biomarker', 'Walking Steadiness', 'Apple walking steadiness score. Predicts 12-month fall risk. OK: >80%. Low: 40-80%. Very Low: <40%.', 'daily_walking_gait', 'walking_steadiness_pct'),
('biomarker:breathing_disturbance', 'biomarker', 'Breathing Disturbance Index', 'Number of breathing disturbances per hour during sleep. Normal: <5. Moderate: 15-30. Proxy for sleep apnea severity.', 'daily_spo2', 'breathing_disturbance_index'),
('biomarker:stress_level', 'biomarker', 'Stress Level', 'Daytime autonomic stress assessment from HRV analysis. Categories: restored, normal, stressed. Based on sympathetic/parasympathetic balance.', 'daily_stress', 'day_summary'),
('biomarker:calorie_intake', 'biomarker', 'Calorie Intake', 'Total daily caloric intake from food logging. Accuracy depends on logging compliance. Used for energy balance analysis.', 'daily_meal', 'calories'),
('biomarker:protein_intake', 'biomarker', 'Protein Intake', 'Daily protein consumption in grams. Recommended: 0.8-2.0g per kg body weight depending on activity level and goals.', 'daily_meal', 'protein'),
('biomarker:hydration', 'biomarker', 'Hydration Level', 'Daily water intake volume and body water percentage. Target: 30-40 ml per kg body weight per day.', 'water_intake,weight', 'water_ml,hydration_kg');

-- =============================================================================
-- NODES: Supplements
-- =============================================================================
INSERT INTO agent.health_graph (node_id, node_type, node_label, description, related_tables, related_columns) VALUES
('supplement:magnesium', 'supplement', 'Magnesium', 'Essential mineral involved in 300+ enzymatic reactions. Glycinate form best for sleep and relaxation. RDA: 400-420mg men, 310-320mg women. Deficiency common (>50% of population).', NULL, NULL),
('supplement:vitamin_d', 'supplement', 'Vitamin D', 'Fat-soluble vitamin critical for bone health, immune function, and mood. Optimal serum 25(OH)D: 40-60 ng/mL. Most people need 2000-5000 IU/day, especially at northern latitudes.', NULL, NULL),
('supplement:omega3', 'supplement', 'Omega-3 Fatty Acids', 'EPA and DHA essential fatty acids. Anti-inflammatory, supports cardiovascular and brain health. Recommended: 1-3g combined EPA+DHA daily. Source: fish oil or algae.', NULL, NULL),
('supplement:melatonin', 'supplement', 'Melatonin', 'Endogenous sleep hormone. Supplemental use (0.3-3mg) can improve sleep onset latency and circadian rhythm alignment. Best taken 30-60 minutes before target bedtime.', NULL, NULL),
('supplement:creatine', 'supplement', 'Creatine Monohydrate', 'Most researched sports supplement. Improves strength, power, and muscle recovery. Also supports cognitive function. Standard dose: 3-5g/day. Safe for long-term use.', NULL, NULL),
('supplement:vitamin_b_complex', 'supplement', 'Vitamin B Complex', 'Group of 8 B vitamins essential for energy metabolism, nervous system function, and red blood cell production. Deficiency can cause fatigue, brain fog, and neuropathy.', NULL, NULL),
('supplement:zinc', 'supplement', 'Zinc', 'Essential trace mineral for immune function, wound healing, and testosterone production. RDA: 11mg men, 8mg women. Excess can deplete copper.', NULL, NULL),
('supplement:ashwagandha', 'supplement', 'Ashwagandha', 'Adaptogenic herb (Withania somnifera). Evidence for reducing cortisol, improving stress resilience, and enhancing sleep quality. Typical dose: 300-600mg standardized extract.', NULL, NULL),
('supplement:caffeine', 'supplement', 'Caffeine', 'Adenosine receptor antagonist. Improves alertness, exercise performance, and mood. Half-life: 5-6 hours. Avoid within 8-10 hours of bedtime to protect sleep quality.', NULL, NULL),
('supplement:probiotics', 'supplement', 'Probiotics', 'Live beneficial bacteria for gut health. Gut-brain axis influences mood, immunity, and inflammation. Strain-specific effects; multi-strain formulations most common.', NULL, NULL);

-- =============================================================================
-- NODES: Conditions
-- =============================================================================
INSERT INTO agent.health_graph (node_id, node_type, node_label, description, related_tables, related_columns) VALUES
('condition:poor_sleep', 'condition', 'Poor Sleep', 'Sleep score consistently below 60 or total sleep <6 hours. Impacts cognitive function, immune response, metabolic health, and emotional regulation.', 'daily_sleep', 'sleep_score'),
('condition:overtraining', 'condition', 'Overtraining Syndrome', 'Chronic imbalance between training load and recovery. Markers: declining readiness, elevated resting HR, reduced HRV, persistent fatigue, poor sleep despite exhaustion.', 'daily_readiness,daily_activity', 'readiness_score,activity_score'),
('condition:dehydration', 'condition', 'Dehydration', 'Insufficient fluid intake or excessive fluid loss. Symptoms: elevated resting HR, reduced HRV, dark urine, fatigue, headache. Even mild dehydration (2% body weight) impairs performance.', 'water_intake,weight', 'water_ml,hydration_kg'),
('condition:chronic_stress', 'condition', 'Chronic Stress', 'Sustained sympathetic nervous system activation. Markers: consistently elevated resting HR, low HRV, poor sleep quality, high stress time. Increases risk of cardiovascular disease.', 'daily_stress,daily_readiness', 'day_summary,readiness_score'),
('condition:sleep_apnea', 'condition', 'Sleep Apnea', 'Repeated breathing interruptions during sleep. Markers: high breathing disturbance index (>5/hr), nocturnal SpO2 drops, excessive daytime sleepiness, loud snoring.', 'daily_spo2', 'breathing_disturbance_index'),
('condition:insomnia', 'condition', 'Insomnia', 'Difficulty falling or staying asleep despite adequate opportunity. Markers: poor sleep efficiency (<85%), long sleep latency (>30min), frequent awakenings.', 'daily_sleep', 'contributor_efficiency,contributor_latency'),
('condition:hypertension', 'condition', 'Hypertension', 'Persistently elevated blood pressure (>=130/80 mmHg). Major risk factor for stroke, heart disease, and kidney damage. Often asymptomatic.', 'blood_pressure', 'systolic,diastolic'),
('condition:inflammation', 'condition', 'Chronic Inflammation', 'Low-grade systemic inflammation. Associated with poor diet, sedentary lifestyle, poor sleep, and chronic stress. Elevates resting HR and reduces HRV.', NULL, NULL),
('condition:circadian_disruption', 'condition', 'Circadian Disruption', 'Misalignment of internal clock with light/dark cycle. Caused by irregular sleep timing, late-night light exposure, shift work. Impacts all physiological systems.', 'daily_sleep', 'contributor_timing'),
('condition:sarcopenia', 'condition', 'Sarcopenia', 'Age-related progressive loss of muscle mass and strength. Accelerated by sedentary behavior and inadequate protein intake. Increases fall risk and metabolic dysfunction.', 'weight', 'muscle_mass_kg'),
('condition:nutrient_deficiency', 'condition', 'Nutrient Deficiency', 'Insufficient intake of essential vitamins, minerals, or macronutrients. Common deficiencies: vitamin D, magnesium, omega-3, iron. Impacts energy, immunity, and recovery.', 'daily_meal', 'calories,protein');

-- =============================================================================
-- NODES: Concepts
-- =============================================================================
INSERT INTO agent.health_graph (node_id, node_type, node_label, description, related_tables, related_columns) VALUES
('concept:circadian_rhythm', 'concept', 'Circadian Rhythm', 'Internal ~24-hour biological clock governing sleep-wake cycles, hormone release, body temperature, and metabolism. Entrained primarily by light exposure and meal timing.', NULL, NULL),
('concept:recovery', 'concept', 'Recovery', 'Physiological restoration process after physical or mental stress. Measured by readiness score, HRV trend, resting HR normalization, and sleep quality. Both active and passive modalities.', 'daily_readiness', 'readiness_score'),
('concept:autonomic_balance', 'concept', 'Autonomic Balance', 'Balance between sympathetic (fight-or-flight) and parasympathetic (rest-and-digest) nervous system activity. HRV is the primary non-invasive measure. Higher HRV = better balance.', NULL, NULL),
('concept:sleep_architecture', 'concept', 'Sleep Architecture', 'Structure and pattern of sleep stages (light, deep, REM) throughout the night. Normal: 4-6 complete cycles of ~90 minutes. Deep sleep predominates early; REM increases toward morning.', 'daily_sleep', 'contributor_deep_sleep,contributor_rem_sleep'),
('concept:training_load', 'concept', 'Training Load', 'Cumulative physical stress from exercise over time. Must balance with recovery capacity. Progressive overload drives adaptation; excessive load causes overtraining.', 'workout,daily_activity', 'duration_seconds,active_calories'),
('concept:energy_balance', 'concept', 'Energy Balance', 'Relationship between caloric intake and expenditure. Surplus leads to weight gain, deficit to weight loss. Sustainable rate: +/-500 kcal/day from maintenance.', 'daily_meal,daily_activity', 'calories,total_calories'),
('concept:gut_brain_axis', 'concept', 'Gut-Brain Axis', 'Bidirectional communication network between the gastrointestinal tract and the central nervous system. Gut microbiome composition affects mood, cognition, and immune function.', NULL, NULL),
('concept:hormesis', 'concept', 'Hormesis', 'Biological phenomenon where low-dose stressors (exercise, cold exposure, fasting) trigger adaptive responses that improve resilience. Dose-response is key: too much stress is harmful.', NULL, NULL),
('concept:sleep_debt', 'concept', 'Sleep Debt', 'Cumulative sleep deficit from consistently sleeping less than needed. Impairs cognitive function, immune response, and metabolic health. Cannot be fully repaid by weekend catch-up sleep.', 'daily_sleep', 'contributor_total_sleep'),
('concept:body_composition', 'concept', 'Body Composition', 'Ratio of fat mass to lean mass (muscle, bone, water). More informative than BMI alone. Tracked via bioimpedance scales, DEXA, or skinfold measurements.', 'weight', 'fat_mass_kg,muscle_mass_kg,bone_mass_kg'),
('concept:inflammation_cascade', 'concept', 'Inflammation Cascade', 'Chain reaction of immune responses. Acute inflammation is protective; chronic low-grade inflammation drives disease. Modulated by diet, sleep, exercise, and stress.', NULL, NULL),
('concept:heart_rate_recovery', 'concept', 'Heart Rate Recovery', 'Speed at which heart rate returns to resting after exercise. Faster recovery indicates better cardiovascular fitness and autonomic function. Drop of >20 bpm in first minute is favorable.', 'heart_rate', 'bpm');

-- =============================================================================
-- NODES: Activities
-- =============================================================================
INSERT INTO agent.health_graph (node_id, node_type, node_label, description, related_tables, related_columns) VALUES
('activity:strength_training', 'activity', 'Strength Training', 'Resistance exercise to build muscle mass, bone density, and metabolic health. Recommended: 2-4 sessions/week targeting all major muscle groups. Key for longevity.', 'workout', 'activity,duration_seconds'),
('activity:cardio', 'activity', 'Cardiovascular Exercise', 'Sustained aerobic activity (running, cycling, swimming). Improves VO2max, cardiovascular health, and mood. Zone 2 (conversational pace) is the foundation; some high-intensity work adds benefit.', 'workout', 'activity,calories'),
('activity:walking', 'activity', 'Walking', 'Low-intensity movement accessible to everyone. 7000+ steps/day reduces all-cause mortality. Post-meal walking improves glycemic control. Minimal recovery cost.', 'daily_activity,step_count', 'steps,step_count'),
('activity:meditation', 'activity', 'Meditation', 'Focused attention or open monitoring practice. 10+ minutes daily improves HRV, reduces cortisol, enhances emotional regulation. Effects compound over weeks/months of consistent practice.', 'mindful_session', 'duration_seconds'),
('activity:yoga', 'activity', 'Yoga', 'Combines physical postures, breathing techniques, and meditation. Improves flexibility, balance, stress resilience, and parasympathetic tone. Complements strength and cardio training.', 'workout', 'activity'),
('activity:swimming', 'activity', 'Swimming', 'Full-body, low-impact cardiovascular exercise. Excellent for joint-sparing fitness. Improves lung capacity and cardiovascular endurance. Water immersion has parasympathetic benefits.', 'workout', 'activity,distance_meters'),
('activity:cycling', 'activity', 'Cycling', 'Low-impact cardiovascular exercise. Effective for Zone 2 training, commuting, and active recovery. Indoor cycling enables precise power-based training.', 'workout', 'activity,distance_meters'),
('activity:stretching', 'activity', 'Stretching & Mobility', 'Flexibility and joint range-of-motion work. Reduces injury risk, improves movement quality, and can activate parasympathetic response. Best done daily, especially after training.', NULL, NULL),
('activity:cold_exposure', 'activity', 'Cold Exposure', 'Deliberate cold stress (cold showers, ice baths). Triggers norepinephrine release, reduces inflammation, improves mood, and may enhance brown fat activity. Typical protocol: 1-5 minutes at 10-15C.', NULL, NULL),
('activity:breathwork', 'activity', 'Breathwork', 'Structured breathing exercises for autonomic regulation. Box breathing and physiological sighs reduce acute stress. Wim Hof method combines cold and breath. Direct HRV modulation.', NULL, NULL);

-- =============================================================================
-- EDGES: Sleep relationships
-- =============================================================================
INSERT INTO agent.health_graph_edges (source_node_id, target_node_id, edge_type, weight, evidence, description) VALUES
('biomarker:sleep_score', 'biomarker:readiness_score', 'correlates_with', 0.85, 'observational_study', 'Sleep quality is the strongest predictor of next-day readiness. Poor sleep directly reduces recovery capacity.'),
('biomarker:deep_sleep', 'concept:recovery', 'improves', 0.9, 'meta_analysis', 'Deep (N3) sleep is when growth hormone peaks and tissue repair occurs. Essential for physical recovery.'),
('biomarker:rem_sleep', 'concept:recovery', 'improves', 0.7, 'meta_analysis', 'REM sleep supports emotional processing, memory consolidation, and creative problem solving.'),
('biomarker:sleep_efficiency', 'biomarker:sleep_score', 'correlates_with', 0.8, 'mechanistic', 'Higher sleep efficiency directly contributes to the composite sleep score.'),
('condition:poor_sleep', 'biomarker:resting_hr', 'worsens', 0.7, 'meta_analysis', 'Sleep deprivation elevates resting heart rate by 3-10 bpm due to increased sympathetic tone.'),
('condition:poor_sleep', 'biomarker:hrv', 'worsens', 0.8, 'meta_analysis', 'Even one night of poor sleep reduces HRV significantly, reflecting impaired autonomic recovery.'),
('condition:poor_sleep', 'condition:chronic_stress', 'worsens', 0.75, 'observational_study', 'Poor sleep increases cortisol and inflammatory markers, perpetuating the stress cycle.'),
('condition:insomnia', 'biomarker:sleep_score', 'worsens', 0.85, 'clinical_trial', 'Insomnia directly reduces sleep duration, efficiency, and staging quality.'),
('concept:sleep_architecture', 'biomarker:sleep_score', 'part_of', 0.9, 'mechanistic', 'Sleep staging (deep, REM, light) is a core component of the overall sleep score.'),
('concept:sleep_debt', 'biomarker:readiness_score', 'worsens', 0.8, 'observational_study', 'Accumulated sleep debt progressively reduces readiness and recovery capacity.'),
('concept:circadian_rhythm', 'biomarker:sleep_score', 'improves', 0.75, 'meta_analysis', 'Consistent circadian alignment improves sleep timing, onset latency, and architecture quality.'),
('supplement:melatonin', 'biomarker:sleep_score', 'improves', 0.5, 'meta_analysis', 'Exogenous melatonin modestly reduces sleep latency (7-12 minutes) and can shift circadian phase.'),
('supplement:magnesium', 'biomarker:sleep_score', 'improves', 0.55, 'meta_analysis', 'Magnesium glycinate activates GABA pathways and promotes muscle relaxation, supporting deeper sleep.'),
('supplement:ashwagandha', 'biomarker:sleep_score', 'improves', 0.5, 'clinical_trial', 'Ashwagandha extract (300mg) shown to improve sleep quality scores and reduce sleep onset latency in RCTs.'),
('supplement:caffeine', 'biomarker:sleep_score', 'worsens', 0.7, 'meta_analysis', 'Caffeine consumed within 8 hours of bedtime reduces total sleep time, deep sleep, and sleep efficiency.'),
('activity:meditation', 'biomarker:sleep_score', 'improves', 0.55, 'meta_analysis', 'Regular meditation practice reduces pre-sleep arousal and improves sleep quality, especially in those with insomnia.'),

-- =============================================================================
-- EDGES: Heart rate and HRV relationships
-- =============================================================================
('biomarker:hrv', 'concept:autonomic_balance', 'measured_by', 0.95, 'mechanistic', 'HRV is the gold-standard non-invasive marker of autonomic nervous system balance.'),
('biomarker:resting_hr', 'concept:autonomic_balance', 'measured_by', 0.7, 'mechanistic', 'Lower resting HR generally indicates greater parasympathetic tone, but is less sensitive than HRV.'),
('biomarker:hrv', 'biomarker:readiness_score', 'correlates_with', 0.85, 'mechanistic', 'HRV balance is one of the strongest contributors to the Oura readiness score.'),
('biomarker:resting_hr', 'biomarker:readiness_score', 'correlates_with', 0.7, 'mechanistic', 'Elevated resting HR relative to baseline reduces the readiness score.'),
('condition:chronic_stress', 'biomarker:hrv', 'worsens', 0.8, 'meta_analysis', 'Chronic psychological stress suppresses parasympathetic activity, reducing HRV.'),
('condition:chronic_stress', 'biomarker:resting_hr', 'worsens', 0.7, 'meta_analysis', 'Sustained stress elevates baseline sympathetic tone, increasing resting heart rate.'),
('condition:dehydration', 'biomarker:resting_hr', 'worsens', 0.65, 'clinical_trial', 'Even mild dehydration (1-2% body weight loss) increases resting HR as the heart compensates for reduced blood volume.'),
('condition:dehydration', 'biomarker:hrv', 'worsens', 0.6, 'clinical_trial', 'Dehydration shifts autonomic balance toward sympathetic dominance, reducing HRV.'),

-- =============================================================================
-- EDGES: Activity and exercise relationships
-- =============================================================================
('activity:strength_training', 'biomarker:muscle_mass', 'improves', 0.9, 'meta_analysis', 'Progressive resistance training is the most effective stimulus for maintaining and building muscle mass.'),
('activity:strength_training', 'condition:sarcopenia', 'prevents', 0.85, 'meta_analysis', 'Regular resistance training is the primary intervention to prevent age-related muscle loss.'),
('activity:strength_training', 'concept:training_load', 'part_of', 0.8, 'mechanistic', 'Resistance training contributes to overall training load and requires adequate recovery.'),
('activity:cardio', 'biomarker:resting_hr', 'improves', 0.8, 'meta_analysis', 'Regular aerobic exercise lowers resting heart rate through increased stroke volume and vagal tone.'),
('activity:cardio', 'biomarker:hrv', 'improves', 0.7, 'meta_analysis', 'Consistent aerobic training improves parasympathetic modulation, increasing HRV over weeks to months.'),
('activity:cardio', 'condition:hypertension', 'prevents', 0.75, 'meta_analysis', 'Regular aerobic exercise reduces systolic BP by 5-8 mmHg in hypertensive individuals.'),
('activity:cardio', 'biomarker:blood_pressure_systolic', 'improves', 0.7, 'meta_analysis', 'Aerobic exercise training reduces systolic blood pressure through improved endothelial function and reduced arterial stiffness.'),
('activity:walking', 'biomarker:steps', 'measured_by', 0.95, 'mechanistic', 'Daily step count is the primary quantitative measure of walking activity.'),
('activity:walking', 'condition:chronic_stress', 'improves', 0.5, 'observational_study', 'Regular walking, especially in nature, reduces cortisol levels and perceived stress.'),
('activity:walking', 'biomarker:readiness_score', 'improves', 0.4, 'observational_study', 'Light walking promotes recovery without adding significant training load. Active recovery modality.'),
('activity:meditation', 'biomarker:hrv', 'improves', 0.65, 'meta_analysis', 'Meditation acutely increases HRV during practice, and regular practice raises baseline HRV over time.'),
('activity:meditation', 'condition:chronic_stress', 'improves', 0.7, 'meta_analysis', 'Mindfulness meditation reduces cortisol, perceived stress, and inflammatory markers.'),
('activity:meditation', 'biomarker:stress_level', 'improves', 0.65, 'clinical_trial', 'Regular meditation shifts the daily stress profile toward more recovery time and less high-stress time.'),
('activity:yoga', 'biomarker:hrv', 'improves', 0.6, 'meta_analysis', 'Yoga practice with breathing components improves parasympathetic tone and HRV.'),
('activity:yoga', 'condition:chronic_stress', 'improves', 0.6, 'meta_analysis', 'Yoga reduces cortisol, anxiety, and perceived stress through combined physical and mindfulness components.'),
('activity:cold_exposure', 'concept:hormesis', 'part_of', 0.8, 'mechanistic', 'Cold exposure is a classic hormetic stressor that triggers norepinephrine release and adaptive cold tolerance.'),
('activity:cold_exposure', 'condition:inflammation', 'improves', 0.55, 'clinical_trial', 'Regular cold exposure reduces inflammatory markers (CRP, IL-6) through norepinephrine-mediated pathways.'),
('activity:cold_exposure', 'biomarker:stress_level', 'improves', 0.5, 'clinical_trial', 'Cold exposure trains stress resilience and may improve autonomic regulation over time.'),
('activity:breathwork', 'biomarker:hrv', 'improves', 0.6, 'clinical_trial', 'Slow breathing (5-6 breaths/min) acutely maximizes HRV through respiratory sinus arrhythmia.'),
('activity:breathwork', 'concept:autonomic_balance', 'improves', 0.65, 'clinical_trial', 'Structured breathing directly modulates the autonomic nervous system toward parasympathetic dominance.'),
('activity:swimming', 'biomarker:resting_hr', 'improves', 0.7, 'meta_analysis', 'Swimming improves cardiovascular fitness with minimal joint stress. Water immersion itself lowers HR.'),
('activity:cycling', 'biomarker:resting_hr', 'improves', 0.7, 'meta_analysis', 'Cycling is effective Zone 2 training that improves cardiovascular efficiency and lowers resting HR.'),
('activity:stretching', 'concept:recovery', 'improves', 0.4, 'expert_consensus', 'Gentle stretching and mobility work supports recovery by improving blood flow and reducing muscle tension.'),

-- =============================================================================
-- EDGES: Supplement relationships
-- =============================================================================
('supplement:magnesium', 'biomarker:hrv', 'improves', 0.5, 'meta_analysis', 'Magnesium supplementation may improve HRV, especially in deficient individuals, by supporting parasympathetic function.'),
('supplement:magnesium', 'condition:chronic_stress', 'improves', 0.55, 'meta_analysis', 'Magnesium modulates the HPA axis and GABA receptors, reducing perceived stress and anxiety.'),
('supplement:magnesium', 'biomarker:blood_pressure_systolic', 'improves', 0.45, 'meta_analysis', 'Magnesium supplementation reduces systolic BP by 2-5 mmHg through vasodilation and calcium channel modulation.'),
('supplement:vitamin_d', 'condition:inflammation', 'improves', 0.6, 'meta_analysis', 'Vitamin D modulates immune function and reduces inflammatory cytokines. Deficiency associated with chronic inflammation.'),
('supplement:vitamin_d', 'biomarker:sleep_score', 'improves', 0.4, 'observational_study', 'Vitamin D deficiency is associated with poor sleep quality. Supplementation may improve sleep, especially in deficient individuals.'),
('supplement:omega3', 'condition:inflammation', 'improves', 0.7, 'meta_analysis', 'EPA and DHA are precursors to anti-inflammatory resolvins and protectins. Strong evidence for reducing systemic inflammation.'),
('supplement:omega3', 'biomarker:resting_hr', 'improves', 0.45, 'meta_analysis', 'Omega-3 supplementation modestly reduces resting HR (1-3 bpm) through improved cardiac autonomic function.'),
('supplement:omega3', 'biomarker:blood_pressure_systolic', 'improves', 0.4, 'meta_analysis', 'Fish oil supplementation reduces systolic BP by 2-4 mmHg, primarily in hypertensive individuals.'),
('supplement:creatine', 'biomarker:muscle_mass', 'improves', 0.7, 'meta_analysis', 'Creatine enhances strength training adaptations by improving high-intensity work capacity and muscle protein synthesis signaling.'),
('supplement:creatine', 'activity:strength_training', 'improves', 0.75, 'meta_analysis', 'Creatine supplementation increases strength, power output, and training volume capacity by 5-15%.'),
('supplement:zinc', 'condition:inflammation', 'improves', 0.5, 'meta_analysis', 'Zinc is essential for immune cell function and has anti-inflammatory properties. Deficiency impairs immune response.'),
('supplement:probiotics', 'concept:gut_brain_axis', 'improves', 0.6, 'meta_analysis', 'Specific probiotic strains modulate gut-brain communication, potentially improving mood, stress resilience, and immune function.'),
('supplement:probiotics', 'condition:inflammation', 'improves', 0.5, 'clinical_trial', 'Probiotics can reduce systemic inflammatory markers by improving gut barrier function and microbial diversity.'),
('supplement:ashwagandha', 'condition:chronic_stress', 'improves', 0.65, 'meta_analysis', 'Ashwagandha (KSM-66, Sensoril extracts) reduces cortisol levels by 15-30% in chronically stressed adults.'),
('supplement:ashwagandha', 'biomarker:hrv', 'improves', 0.45, 'clinical_trial', 'Ashwagandha supplementation may improve HRV through cortisol reduction and GABAergic activity.'),
('supplement:caffeine', 'biomarker:resting_hr', 'worsens', 0.4, 'meta_analysis', 'Caffeine acutely increases heart rate by 3-5 bpm via adenosine receptor antagonism and catecholamine release.'),
('supplement:caffeine', 'biomarker:blood_pressure_systolic', 'worsens', 0.45, 'meta_analysis', 'Caffeine acutely raises systolic BP by 3-8 mmHg. Chronic consumers develop partial tolerance.'),
('supplement:vitamin_b_complex', 'condition:chronic_stress', 'improves', 0.45, 'clinical_trial', 'B vitamins support neurotransmitter synthesis and energy metabolism. Supplementation may reduce perceived stress and mental fatigue.'),

-- =============================================================================
-- EDGES: Condition relationships
-- =============================================================================
('condition:overtraining', 'biomarker:readiness_score', 'worsens', 0.9, 'expert_consensus', 'Overtraining syndrome causes progressive readiness decline as recovery capacity is chronically exceeded.'),
('condition:overtraining', 'biomarker:resting_hr', 'worsens', 0.75, 'observational_study', 'Overreaching elevates resting HR by 5-15 bpm as the body struggles to recover from accumulated training stress.'),
('condition:overtraining', 'biomarker:hrv', 'worsens', 0.8, 'observational_study', 'HRV suppression is one of the earliest and most reliable markers of overtraining.'),
('condition:overtraining', 'biomarker:sleep_score', 'worsens', 0.65, 'expert_consensus', 'Paradoxically, overtrained athletes often experience insomnia and fragmented sleep despite physical exhaustion.'),
('condition:sleep_apnea', 'biomarker:spo2', 'worsens', 0.9, 'mechanistic', 'Apneic events cause repeated oxygen desaturations, lowering average nocturnal SpO2.'),
('condition:sleep_apnea', 'biomarker:deep_sleep', 'worsens', 0.8, 'mechanistic', 'Apnea-related arousals fragment sleep architecture, severely reducing deep and REM sleep.'),
('condition:sleep_apnea', 'biomarker:readiness_score', 'worsens', 0.75, 'observational_study', 'Untreated sleep apnea consistently impairs recovery, resulting in chronically low readiness scores.'),
('condition:hypertension', 'biomarker:blood_pressure_systolic', 'measured_by', 0.95, 'mechanistic', 'Systolic blood pressure is the primary diagnostic measurement for hypertension.'),
('condition:hypertension', 'biomarker:blood_pressure_diastolic', 'measured_by', 0.9, 'mechanistic', 'Diastolic blood pressure is the secondary diagnostic criterion for hypertension.'),
('condition:inflammation', 'biomarker:resting_hr', 'worsens', 0.6, 'meta_analysis', 'Chronic low-grade inflammation is associated with elevated resting heart rate.'),
('condition:inflammation', 'biomarker:hrv', 'worsens', 0.6, 'meta_analysis', 'Inflammatory cytokines suppress vagal tone, reducing heart rate variability.'),
('condition:inflammation', 'concept:recovery', 'worsens', 0.7, 'mechanistic', 'Chronic inflammation impairs tissue repair and extends recovery time after physical stress.'),
('condition:circadian_disruption', 'condition:poor_sleep', 'worsens', 0.8, 'meta_analysis', 'Misaligned circadian rhythm is a primary driver of poor sleep quality and insomnia.'),
('condition:circadian_disruption', 'biomarker:body_temperature', 'worsens', 0.6, 'mechanistic', 'Circadian disruption flattens the normal temperature rhythm, reducing the evening drop that facilitates sleep onset.'),
('condition:nutrient_deficiency', 'concept:recovery', 'worsens', 0.65, 'expert_consensus', 'Inadequate micronutrient intake impairs enzymatic processes essential for tissue repair and adaptation.'),
('condition:nutrient_deficiency', 'condition:inflammation', 'worsens', 0.55, 'meta_analysis', 'Deficiencies in vitamin D, omega-3, magnesium, and zinc promote pro-inflammatory states.'),
('condition:sarcopenia', 'biomarker:walking_speed', 'worsens', 0.7, 'meta_analysis', 'Loss of muscle mass and strength directly reduces walking speed, a key marker of functional decline.'),
('condition:sarcopenia', 'biomarker:walking_steadiness', 'worsens', 0.65, 'meta_analysis', 'Reduced muscle mass impairs balance and gait stability, increasing fall risk.'),

-- =============================================================================
-- EDGES: Concept relationships
-- =============================================================================
('concept:circadian_rhythm', 'biomarker:body_temperature', 'correlates_with', 0.8, 'mechanistic', 'Core body temperature follows a robust circadian pattern: lowest in early morning, highest in late afternoon.'),
('concept:circadian_rhythm', 'biomarker:resting_hr', 'correlates_with', 0.6, 'mechanistic', 'Heart rate follows a circadian pattern, lowest during deep sleep, reflecting autonomic cycling.'),
('concept:training_load', 'concept:recovery', 'correlates_with', 0.9, 'mechanistic', 'Training load and recovery are inversely related. Higher load demands more recovery time and resources.'),
('concept:training_load', 'condition:overtraining', 'worsens', 0.85, 'expert_consensus', 'Excessive training load without adequate recovery is the definition of overtraining.'),
('concept:energy_balance', 'biomarker:weight', 'correlates_with', 0.9, 'mechanistic', 'Chronic energy surplus leads to weight gain; chronic deficit leads to weight loss. ~7700 kcal per kg of body fat.'),
('concept:energy_balance', 'concept:body_composition', 'correlates_with', 0.8, 'mechanistic', 'The magnitude and protein content of energy surplus/deficit determines whether changes are fat or muscle.'),
('concept:body_composition', 'biomarker:body_fat', 'measured_by', 0.85, 'mechanistic', 'Body fat percentage is a core measure of body composition.'),
('concept:body_composition', 'biomarker:muscle_mass', 'measured_by', 0.85, 'mechanistic', 'Muscle mass is a core measure of body composition and metabolic health.'),
('concept:autonomic_balance', 'concept:recovery', 'correlates_with', 0.85, 'mechanistic', 'Parasympathetic dominance (high HRV, low resting HR) facilitates and indicates recovery.'),
('concept:hormesis', 'concept:recovery', 'improves', 0.6, 'mechanistic', 'Hormetic stressors (exercise, cold, fasting) trigger adaptive responses that improve long-term recovery capacity.'),
('concept:gut_brain_axis', 'biomarker:stress_level', 'correlates_with', 0.55, 'meta_analysis', 'Gut microbiome composition influences stress reactivity, mood, and HPA axis function via the vagus nerve.'),
('concept:inflammation_cascade', 'condition:inflammation', 'part_of', 0.95, 'mechanistic', 'The inflammation cascade is the mechanistic pathway underlying chronic inflammatory states.'),
('concept:heart_rate_recovery', 'biomarker:resting_hr', 'correlates_with', 0.7, 'meta_analysis', 'Faster heart rate recovery after exercise is associated with lower resting HR and better overall cardiovascular fitness.'),
('concept:sleep_debt', 'condition:poor_sleep', 'worsens', 0.75, 'mechanistic', 'Accumulated sleep debt degrades sleep regulation, making it harder to achieve restorative sleep.'),

-- =============================================================================
-- EDGES: Cross-domain biomarker relationships
-- =============================================================================
('biomarker:spo2', 'biomarker:breathing_disturbance', 'correlates_with', 0.85, 'mechanistic', 'Higher breathing disturbance index correlates with lower average SpO2 due to repeated desaturations.'),
('biomarker:respiratory_rate', 'biomarker:stress_level', 'correlates_with', 0.6, 'mechanistic', 'Elevated respiratory rate at rest often reflects sympathetic activation and psychological stress.'),
('biomarker:steps', 'biomarker:activity_score', 'correlates_with', 0.7, 'mechanistic', 'Daily step count is a significant contributor to the overall activity score.'),
('biomarker:calorie_intake', 'concept:energy_balance', 'part_of', 0.95, 'mechanistic', 'Caloric intake is the input side of the energy balance equation.'),
('biomarker:protein_intake', 'biomarker:muscle_mass', 'improves', 0.7, 'meta_analysis', 'Adequate protein intake (1.6-2.2g/kg) is essential for muscle protein synthesis, especially with resistance training.'),
('biomarker:hydration', 'condition:dehydration', 'prevents', 0.85, 'mechanistic', 'Adequate fluid intake directly prevents dehydration and its cascading negative effects on performance and recovery.'),
('biomarker:weight', 'concept:body_composition', 'part_of', 0.7, 'mechanistic', 'Total body weight is a crude but important measure of body composition when tracked alongside fat and muscle mass.'),
('biomarker:body_temperature', 'concept:circadian_rhythm', 'measured_by', 0.75, 'mechanistic', 'Body temperature rhythm is one of the most reliable circadian markers available from wearable data.'),
('biomarker:readiness_score', 'concept:recovery', 'measured_by', 0.9, 'mechanistic', 'The Oura readiness score is the primary composite indicator of overall recovery status.'),
('biomarker:activity_score', 'concept:training_load', 'measured_by', 0.7, 'mechanistic', 'The activity score reflects daily training load relative to personal capacity and goals.'),
('biomarker:stress_level', 'concept:autonomic_balance', 'measured_by', 0.75, 'mechanistic', 'The daily stress assessment directly reflects sympathetic vs parasympathetic dominance throughout the day.');
