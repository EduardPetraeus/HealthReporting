"""Tests for the DataQualityChecker engine."""

from __future__ import annotations

import pytest
import yaml
from health_platform.quality.data_quality_checker import DataQualityChecker
from health_platform.quality.models import QualityReport

pytestmark = pytest.mark.integration


@pytest.fixture
def rules_path(tmp_path):
    """Write a test rules YAML and return its path."""
    rules = {
        "tables": {
            "dq_good": {
                "not_null": ["business_key_hash", "sk_date", "day"],
                "unique": ["business_key_hash"],
                "freshness": {"column": "day", "max_hours": 49},
                "row_count": {"min_rows": 1},
                "value_range": {"score": {"min": 0, "max": 100}},
            },
            "dq_nulls": {
                "not_null": ["business_key_hash", "sk_date"],
            },
            "dq_dupes": {
                "unique": ["business_key_hash"],
            },
            "dq_stale": {
                "freshness": {"column": "day", "max_hours": 25},
            },
            "dq_range": {
                "value_range": {
                    "score": {"min": 0, "max": 100},
                    "bpm": {"min": 20, "max": 250},
                },
            },
            "dq_empty": {
                "row_count": {"min_rows": 1},
            },
            "nonexistent_table": {
                "not_null": ["business_key_hash"],
            },
        }
    }
    path = tmp_path / "quality_rules.yaml"
    with open(path, "w") as f:
        yaml.dump(rules, f)
    return path


class TestNotNull:
    def test_passes(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_good")
        not_null_results = [r for r in results if r.check_type == "not_null"]
        assert all(r.passed for r in not_null_results)

    def test_fails(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_nulls")
        not_null_results = [r for r in results if r.check_type == "not_null"]
        assert any(not r.passed for r in not_null_results)
        bkh_result = next(
            r for r in not_null_results if r.column == "business_key_hash"
        )
        assert not bkh_result.passed
        assert bkh_result.value > 0


class TestUnique:
    def test_passes(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_good")
        unique_results = [r for r in results if r.check_type == "unique"]
        assert all(r.passed for r in unique_results)

    def test_fails(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_dupes")
        unique_results = [r for r in results if r.check_type == "unique"]
        assert len(unique_results) == 1
        assert not unique_results[0].passed
        assert unique_results[0].value > 0


class TestFreshness:
    def test_passes(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_good")
        fresh_results = [r for r in results if r.check_type == "freshness"]
        assert len(fresh_results) == 1
        assert fresh_results[0].passed

    def test_stale(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_stale")
        assert len(results) == 1
        assert not results[0].passed
        assert "STALE" in results[0].message


class TestValueRange:
    def test_passes(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_good")
        range_results = [r for r in results if r.check_type == "value_range"]
        assert all(r.passed for r in range_results)

    def test_out_of_bounds(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_range")
        range_results = [r for r in results if r.check_type == "value_range"]
        score_result = next(r for r in range_results if r.column == "score")
        bpm_result = next(r for r in range_results if r.column == "bpm")
        assert not score_result.passed
        assert not bpm_result.passed


class TestRowCount:
    def test_passes(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_good")
        rc_results = [r for r in results if r.check_type == "row_count"]
        assert len(rc_results) == 1
        assert rc_results[0].passed

    def test_empty(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("dq_empty")
        assert len(results) == 1
        assert not results[0].passed
        assert "EMPTY" in results[0].message


class TestRunAllChecks:
    def test_returns_report(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        report = checker.run_all_checks()
        assert isinstance(report, QualityReport)
        assert report.total > 0
        assert report.passed + report.failed == report.total
        assert report.finished_at is not None


class TestRunCheckType:
    def test_freshness_across_tables(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_check_type("freshness")
        assert len(results) >= 2
        tables_checked = {r.table_name for r in results}
        assert "dq_good" in tables_checked
        assert "dq_stale" in tables_checked

    def test_unknown_type_returns_empty(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_check_type("nonexistent_check")
        assert len(results) == 0


class TestSchemaDrift:
    """Tests for the schema_drift check type."""

    @pytest.fixture
    def schema_drift_rules(self, tmp_path):
        rules = {
            "tables": {
                "dq_good": {
                    "schema_drift": {
                        "expected_columns": [
                            "business_key_hash",
                            "sk_date",
                            "day",
                            "score",
                        ],
                    },
                },
                "dq_nulls": {
                    "schema_drift": {
                        "expected_columns": [
                            "business_key_hash",
                            "sk_date",
                            "nonexistent_column",
                        ],
                    },
                },
            }
        }
        path = tmp_path / "schema_drift_rules.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        return path

    def test_passes_all_columns_present(self, quality_db, schema_drift_rules):
        checker = DataQualityChecker(quality_db, schema_drift_rules)
        results = checker.run_table_checks("dq_good")
        drift_results = [r for r in results if r.check_type == "schema_drift"]
        assert len(drift_results) == 1
        assert drift_results[0].passed
        assert "4 expected columns present" in drift_results[0].message

    def test_fails_missing_column(self, quality_db, schema_drift_rules):
        checker = DataQualityChecker(quality_db, schema_drift_rules)
        results = checker.run_table_checks("dq_nulls")
        drift_results = [r for r in results if r.check_type == "schema_drift"]
        assert len(drift_results) == 1
        assert not drift_results[0].passed
        assert "nonexistent_column" in drift_results[0].message
        assert drift_results[0].value > 0

    def test_schema_drift_across_tables(self, quality_db, schema_drift_rules):
        checker = DataQualityChecker(quality_db, schema_drift_rules)
        results = checker.run_check_type("schema_drift")
        assert len(results) >= 2
        passed = [r for r in results if r.passed]
        failed = [r for r in results if not r.passed]
        assert len(passed) >= 1
        assert len(failed) >= 1


class TestCompleteness:
    """Tests for the completeness check type."""

    @pytest.fixture
    def completeness_rules(self, tmp_path):
        rules = {
            "tables": {
                "dq_complete": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 0},
                },
                "dq_gaps": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 0},
                },
                "dq_empty": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 0},
                },
            }
        }
        path = tmp_path / "completeness_rules.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        return path

    @pytest.fixture
    def threshold_rules(self, tmp_path):
        rules = {
            "tables": {
                "dq_gaps": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 5},
                },
            }
        }
        path = tmp_path / "threshold_rules.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        return path

    @pytest.fixture
    def single_row_rules(self, tmp_path):
        rules = {
            "tables": {
                "dq_single": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 0},
                },
            }
        }
        path = tmp_path / "single_row_rules.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        return path

    def test_passes_no_gaps(self, quality_db, completeness_rules):
        checker = DataQualityChecker(quality_db, completeness_rules)
        results = checker.run_table_checks("dq_complete")
        comp_results = [r for r in results if r.check_type == "completeness"]
        assert len(comp_results) == 1
        assert comp_results[0].passed
        assert comp_results[0].value == 0

    def test_fails_with_gaps(self, quality_db, completeness_rules):
        checker = DataQualityChecker(quality_db, completeness_rules)
        results = checker.run_table_checks("dq_gaps")
        comp_results = [r for r in results if r.check_type == "completeness"]
        assert len(comp_results) == 1
        assert not comp_results[0].passed
        assert comp_results[0].value == 3

    def test_gap_periods_reported(self, quality_db, completeness_rules):
        checker = DataQualityChecker(quality_db, completeness_rules)
        results = checker.run_table_checks("dq_gaps")
        comp = [r for r in results if r.check_type == "completeness"][0]
        assert comp.metadata is not None
        gaps = comp.metadata["gap_periods"]
        assert len(gaps) == 2
        assert gaps[0]["gap_start"] == "2026-03-03"
        assert gaps[0]["gap_end"] == "2026-03-03"
        assert gaps[1]["gap_start"] == "2026-03-06"
        assert gaps[1]["gap_end"] == "2026-03-07"

    def test_reload_recommendations(self, quality_db, completeness_rules):
        checker = DataQualityChecker(quality_db, completeness_rules)
        results = checker.run_table_checks("dq_gaps")
        comp = [r for r in results if r.check_type == "completeness"][0]
        recs = comp.metadata["reload_recommendations"]
        assert len(recs) == 2
        assert all(r["table_name"] == "dq_gaps" for r in recs)
        assert all(r["suggested_action"] == "reload" for r in recs)
        assert recs[0]["gap_start"] == "2026-03-03"
        assert recs[1]["gap_start"] == "2026-03-06"

    def test_max_gaps_threshold_pass(self, quality_db, threshold_rules):
        checker = DataQualityChecker(quality_db, threshold_rules)
        results = checker.run_table_checks("dq_gaps")
        comp_results = [r for r in results if r.check_type == "completeness"]
        assert len(comp_results) == 1
        assert comp_results[0].passed
        assert comp_results[0].value == 3
        assert comp_results[0].threshold == 5.0

    def test_empty_table(self, quality_db, completeness_rules):
        checker = DataQualityChecker(quality_db, completeness_rules)
        results = checker.run_table_checks("dq_empty")
        comp_results = [r for r in results if r.check_type == "completeness"]
        assert len(comp_results) == 1
        assert not comp_results[0].passed
        assert "empty" in comp_results[0].message.lower()

    def test_single_row_table(self, quality_db, single_row_rules):
        quality_db.execute(
            """
            CREATE TABLE silver.dq_single (
                business_key_hash VARCHAR NOT NULL,
                sk_date INTEGER NOT NULL,
                day DATE NOT NULL,
                load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        quality_db.execute(
            """
            INSERT INTO silver.dq_single VALUES
                ('hash_1', 20260301, '2026-03-01', CURRENT_TIMESTAMP)
        """
        )
        checker = DataQualityChecker(quality_db, single_row_rules)
        results = checker.run_table_checks("dq_single")
        comp_results = [r for r in results if r.check_type == "completeness"]
        assert len(comp_results) == 1
        assert comp_results[0].passed
        assert comp_results[0].value == 0


class TestQualityReportReloadRecommendations:
    """Tests for the QualityReport.reload_recommendations property."""

    @pytest.fixture
    def gap_rules(self, tmp_path):
        rules = {
            "tables": {
                "dq_gaps": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 0},
                },
            }
        }
        path = tmp_path / "gap_rules.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        return path

    @pytest.fixture
    def clean_rules(self, tmp_path):
        rules = {
            "tables": {
                "dq_complete": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 0},
                },
            }
        }
        path = tmp_path / "clean_rules.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        return path

    def test_report_aggregates_recommendations(self, quality_db, gap_rules):
        checker = DataQualityChecker(quality_db, gap_rules)
        report = checker.run_all_checks()
        recs = report.reload_recommendations
        assert len(recs) == 2
        assert all(r["table_name"] == "dq_gaps" for r in recs)

    def test_report_empty_when_no_gaps(self, quality_db, clean_rules):
        checker = DataQualityChecker(quality_db, clean_rules)
        report = checker.run_all_checks()
        assert report.reload_recommendations == []


class TestCompletenessRuleValidation:
    """Tests for completeness rule validation in rule_loader."""

    def test_valid_rule(self, tmp_path):
        from health_platform.quality.rule_loader import load_rules

        rules = {
            "tables": {
                "test_table": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": 0},
                },
            }
        }
        path = tmp_path / "valid_rules.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        loaded = load_rules(path)
        assert "test_table" in loaded

    def test_invalid_grain(self, tmp_path):
        from health_platform.quality.rule_loader import load_rules

        rules = {
            "tables": {
                "test_table": {
                    "completeness": {"column": "day", "grain": "weekly", "max_gaps": 0},
                },
            }
        }
        path = tmp_path / "invalid_grain.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        with pytest.raises(ValueError, match="grain"):
            load_rules(path)

    def test_missing_column(self, tmp_path):
        from health_platform.quality.rule_loader import load_rules

        rules = {
            "tables": {
                "test_table": {
                    "completeness": {"grain": "daily", "max_gaps": 0},
                },
            }
        }
        path = tmp_path / "missing_col.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        with pytest.raises(ValueError, match="column"):
            load_rules(path)

    def test_negative_max_gaps(self, tmp_path):
        from health_platform.quality.rule_loader import load_rules

        rules = {
            "tables": {
                "test_table": {
                    "completeness": {"column": "day", "grain": "daily", "max_gaps": -1},
                },
            }
        }
        path = tmp_path / "neg_gaps.yaml"
        with open(path, "w") as f:
            yaml.dump(rules, f)
        with pytest.raises(ValueError, match="max_gaps"):
            load_rules(path)


class TestNonexistentTable:
    def test_handled(self, quality_db, rules_path):
        checker = DataQualityChecker(quality_db, rules_path)
        results = checker.run_table_checks("nonexistent_table")
        assert len(results) == 1
        assert not results[0].passed
        assert "does not exist" in results[0].message
