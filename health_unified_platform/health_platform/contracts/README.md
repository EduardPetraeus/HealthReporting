# Contracts

Semantic contracts defining metric definitions, computation SQL, thresholds, and baselines for the AI-native data model.

## Structure

```
contracts/
└── metrics/
    ├── _index.yml              # Master index — 9 categories, query routing, schema pruning
    ├── _business_rules.yml     # Composite health score (35/35/30), 5 alerts, anomaly detection
    ├── sleep_score.yml         # Sleep quality (0-100) from Oura
    ├── readiness_score.yml     # Recovery readiness (0-100) from Oura
    ├── activity_score.yml      # Activity score (0-100) from Oura
    ├── steps.yml               # Daily step count
    ├── resting_heart_rate.yml  # Resting HR from heart_rate table
    ├── blood_oxygen.yml        # SpO2 average from Oura
    ├── body_temperature.yml    # Body temperature deviation
    ├── blood_pressure.yml      # Systolic/diastolic from Withings
    ├── weight.yml              # Body weight from Withings
    ├── daily_stress.yml        # Stress minutes from Oura
    ├── walking_gait.yml        # Gait metrics from Apple Health
    ├── calories.yml            # Total/active calories
    ├── protein.yml             # Protein intake from Lifesum
    ├── water_intake.yml        # Water intake from Apple Health
    ├── workout.yml             # Workout sessions from Oura
    ├── respiratory_rate.yml    # Respiratory rate from Apple Health
    ├── mindful_session.yml     # Mindfulness minutes from Apple Health
    └── toothbrushing.yml       # Dental hygiene from Apple Health
```

## Contract Format

Each metric YAML defines:
- **metric**: name, display_name, description, category, source_table, grain
- **computations**: named SQL templates (daily_value, period_average, trend, anomaly)
- **thresholds**: optimal/good/poor ranges with labels
- **baseline**: SQL to compute personal baseline (90-day median)
- **related_metrics**: cross-metric relationships with lag
- **examples**: question → tool call mappings for few-shot prompting

Contracts are read by `mcp/query_builder.py` and exposed via MCP tools.
