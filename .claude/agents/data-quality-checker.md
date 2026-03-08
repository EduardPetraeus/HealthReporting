---
name: data-quality-checker
description: Run data quality checks on silver tables. Use when asked to check data quality, validate data, audit freshness, or flag anomalies.
---

# Data Quality Checker

Run DQ checks on the health data platform using the quality module.

## Quick run (all checks)
```bash
cd ~/Github\ repos/HealthReporting
PYTHONPATH=health_unified_platform .venv/bin/python scripts/run_quality_checks.py
```

## Single table
```bash
PYTHONPATH=health_unified_platform .venv/bin/python scripts/run_quality_checks.py --table daily_sleep
```

## Single check type
```bash
PYTHONPATH=health_unified_platform .venv/bin/python scripts/run_quality_checks.py --type freshness
```

## MCP tool
Call `check_data_quality` tool from Claude Desktop (Tool 9 in server.py).

## Configuration
Rules defined in `health_environment/config/quality_rules.yaml`.
Engine: `health_platform/quality/data_quality_checker.py`.

## Check types
- **not_null** — key columns must not contain NULL values
- **unique** — business_key_hash must be unique per table
- **freshness** — date column must be within max_hours threshold
- **row_count** — table must have minimum number of rows
- **value_range** — metric values must be within defined bounds

## Adding rules for new tables
1. Add table entry in `quality_rules.yaml`
2. Run `pytest tests/test_quality_rules_yaml.py` to validate syntax
3. Run `scripts/run_quality_checks.py --table <name>` to verify

## Scale-up note
Enterprise upgrade path:
- **Databricks DLT**: Migrate to `@dlt.expect` expectations for row-level quarantine
- **Great Expectations**: Convert YAML rules to GX expectation suites (same structure)
- Current YAML rules are the abstraction layer — framework behind them can change
