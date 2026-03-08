"""Tests that validate the quality_rules.yaml file itself."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from health_platform.quality.rule_loader import load_rules, _VALID_CHECK_TYPES

RULES_PATH = (
    Path(__file__).resolve().parents[1]
    / "health_unified_platform"
    / "health_environment"
    / "config"
    / "quality_rules.yaml"
)

MERGE_DIR = (
    Path(__file__).resolve().parents[1]
    / "health_unified_platform"
    / "health_platform"
    / "transformation_logic"
    / "dbt"
    / "merge"
    / "silver"
)


def _get_silver_table_names_from_merges() -> set[str]:
    """Extract silver table names from merge SQL file headers."""
    tables = set()
    for sql_file in MERGE_DIR.glob("merge_*.sql"):
        with open(sql_file, "r", encoding="utf-8") as f:
            for line in f:
                if "-> silver." in line:
                    table_name = line.split("-> silver.")[1].strip()
                    tables.add(table_name)
                    break
    return tables


class TestYamlParsing:
    def test_parses_successfully(self):
        tables = load_rules(RULES_PATH)
        assert isinstance(tables, dict)
        assert len(tables) > 0

    def test_all_check_types_valid(self):
        tables = load_rules(RULES_PATH)
        for table_name, checks in tables.items():
            for check_type in checks:
                assert (
                    check_type in _VALID_CHECK_TYPES
                ), f"Unknown check type '{check_type}' in table '{table_name}'"


class TestTableCoverage:
    def test_all_silver_tables_have_rules(self):
        """Verify every silver table from merge scripts has DQ rules."""
        tables = load_rules(RULES_PATH)
        merge_tables = _get_silver_table_names_from_merges()
        rules_tables = set(tables.keys())
        missing = merge_tables - rules_tables
        assert not missing, f"Silver tables without DQ rules: {sorted(missing)}"


class TestColumnConventions:
    def test_not_null_includes_business_key_hash(self):
        """Every table with not_null checks must include business_key_hash."""
        tables = load_rules(RULES_PATH)
        for table_name, checks in tables.items():
            if "not_null" in checks:
                assert (
                    "business_key_hash" in checks["not_null"]
                ), f"Table '{table_name}' not_null missing business_key_hash"

    def test_freshness_columns_exist(self):
        """Freshness rules must reference a valid column name."""
        tables = load_rules(RULES_PATH)
        valid_date_columns = {"day", "datetime", "timestamp", "load_datetime"}
        for table_name, checks in tables.items():
            if "freshness" in checks:
                col = checks["freshness"].get("column", "load_datetime")
                assert col in valid_date_columns, (
                    f"Table '{table_name}' freshness column '{col}' "
                    f"not in {valid_date_columns}"
                )


class TestSchemaValidation:
    """Test that malformed YAML is rejected at load time."""

    def _write_rules(self, tmp_path, tables_dict):
        path = tmp_path / "bad_rules.yaml"
        path.write_text(yaml.dump({"tables": tables_dict}))
        return path

    def test_rejects_invalid_table_name(self, tmp_path):
        path = self._write_rules(tmp_path, {"bad table;": {"not_null": ["col"]}})
        with pytest.raises(ValueError, match="Invalid table name"):
            load_rules(path)

    def test_rejects_non_numeric_max_hours(self, tmp_path):
        path = self._write_rules(
            tmp_path,
            {"good_table": {"freshness": {"column": "day", "max_hours": "banana"}}},
        )
        with pytest.raises(ValueError, match="must be numeric"):
            load_rules(path)

    def test_rejects_non_numeric_row_count_min(self, tmp_path):
        path = self._write_rules(
            tmp_path, {"good_table": {"row_count": {"min_rows": "lots"}}}
        )
        with pytest.raises(ValueError, match="must be numeric"):
            load_rules(path)

    def test_rejects_non_numeric_value_range_bound(self, tmp_path):
        path = self._write_rules(
            tmp_path,
            {"good_table": {"value_range": {"score": {"min": "low", "max": 100}}}},
        )
        with pytest.raises(ValueError, match="must be numeric"):
            load_rules(path)

    def test_rejects_invalid_column_in_not_null(self, tmp_path):
        path = self._write_rules(
            tmp_path, {"good_table": {"not_null": ["valid_col", "bad col;"]}}
        )
        with pytest.raises(ValueError, match="Invalid column"):
            load_rules(path)

    def test_rejects_trailing_newline_in_table_name(self, tmp_path):
        path = self._write_rules(tmp_path, {"daily_sleep\n": {"not_null": ["col"]}})
        with pytest.raises(ValueError, match="Invalid table name"):
            load_rules(path)
