---
name: test-writer
description: Write tests for a silver entity, connector, or Python module. Creates dbt schema tests and pytest scaffolding. Use when asked to add tests, write tests, or improve test coverage.
---

# Test Writer

Write tests for the specified target.

## dbt schema tests (for silver/gold entities)

Create or update `schema.yml` in the relevant dbt model directory with:
```yaml
models:
  - name: <entity>
    columns:
      - name: <primary_key>
        tests:
          - not_null
          - unique
      - name: source_system
        tests:
          - not_null
          - accepted_values:
              values: ['apple_health', 'oura', 'withings']
      - name: _ingested_at
        tests:
          - not_null
```

## Python unit tests (for connectors/transforms)

Scaffold a `tests/test_<module>.py` file with:
- One test per public function
- Use `tmp_path` fixture for file I/O
- Mock external calls (API, DuckDB) with `unittest.mock`
- Test happy path + at least one edge case per function

## Smoke tests

Reference `verify.sh` as the integration-level smoke test. If new silver tables are added, update `verify.sh` to include them.

## Rules
- Never test private/internal functions — only public interfaces
- Tests must be runnable with: `.venv/bin/python -m pytest tests/`
- Keep tests simple — three lines beats a test fixture factory
