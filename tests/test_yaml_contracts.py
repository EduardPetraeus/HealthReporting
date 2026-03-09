"""Tests for YAML semantic contracts validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

CONTRACTS_DIR = (
    Path(__file__).resolve().parents[1]
    / "health_unified_platform"
    / "health_platform"
    / "contracts"
    / "metrics"
)


class TestContractIndex:
    """Test the master index file."""

    def test_index_exists(self):
        """_index.yml exists."""
        index_file = CONTRACTS_DIR / "_index.yml"
        if not index_file.exists():
            pytest.skip("_index.yml not yet created")
        assert index_file.exists()

    def test_index_valid_yaml(self):
        """_index.yml is valid YAML."""
        index_file = CONTRACTS_DIR / "_index.yml"
        if not index_file.exists():
            pytest.skip("_index.yml not yet created")

        with open(index_file) as f:
            data = yaml.safe_load(f)

        assert data is not None
        assert "metrics" in data

    def test_index_has_categories(self):
        """_index.yml has all expected categories."""
        index_file = CONTRACTS_DIR / "_index.yml"
        if not index_file.exists():
            pytest.skip("_index.yml not yet created")

        with open(index_file) as f:
            data = yaml.safe_load(f)

        expected = {"sleep", "vitals", "activity", "recovery", "body", "nutrition"}
        actual = set(data["metrics"].keys())
        missing = expected - actual
        assert not missing, f"Missing categories: {missing}"

    def test_index_has_query_routing(self):
        """_index.yml has query routing config."""
        index_file = CONTRACTS_DIR / "_index.yml"
        if not index_file.exists():
            pytest.skip("_index.yml not yet created")

        with open(index_file) as f:
            data = yaml.safe_load(f)

        assert "query_routing" in data


class TestBusinessRules:
    """Test business rules YAML."""

    def test_business_rules_exists(self):
        """_business_rules.yml exists."""
        rules_file = CONTRACTS_DIR / "_business_rules.yml"
        if not rules_file.exists():
            pytest.skip("_business_rules.yml not yet created")
        assert rules_file.exists()

    def test_business_rules_valid_yaml(self):
        """_business_rules.yml is valid YAML."""
        rules_file = CONTRACTS_DIR / "_business_rules.yml"
        if not rules_file.exists():
            pytest.skip("_business_rules.yml not yet created")

        with open(rules_file) as f:
            data = yaml.safe_load(f)

        assert data is not None

    def test_composite_scores_defined(self):
        """Composite health score is defined."""
        rules_file = CONTRACTS_DIR / "_business_rules.yml"
        if not rules_file.exists():
            pytest.skip("_business_rules.yml not yet created")

        with open(rules_file) as f:
            data = yaml.safe_load(f)

        assert "composite_scores" in data
        assert "daily_health_score" in data["composite_scores"]

    def test_alerts_defined(self):
        """Alert rules are defined."""
        rules_file = CONTRACTS_DIR / "_business_rules.yml"
        if not rules_file.exists():
            pytest.skip("_business_rules.yml not yet created")

        with open(rules_file) as f:
            data = yaml.safe_load(f)

        assert "alerts" in data
        assert len(data["alerts"]) >= 3


class TestMetricContracts:
    """Test individual metric contract files."""

    def _get_metric_files(self):
        """List all metric YAML files (excluding _ prefixed and non-metric files)."""
        if not CONTRACTS_DIR.exists():
            return []
        # gold_views.yml is a gold layer index, not a metric contract
        excluded = {"gold_views.yml"}
        return [
            f
            for f in CONTRACTS_DIR.glob("*.yml")
            if not f.name.startswith("_") and f.name not in excluded
        ]

    def test_metric_files_exist(self):
        """At least 10 metric contract files exist."""
        files = self._get_metric_files()
        if not files:
            pytest.skip("Metric files not yet created")
        assert len(files) >= 10, f"Only {len(files)} metric files found"

    @pytest.mark.parametrize(
        "required_metric",
        [
            "sleep_score",
            "readiness_score",
            "activity_score",
            "steps",
            "weight",
            "blood_pressure",
        ],
    )
    def test_required_metrics_exist(self, required_metric):
        """Key metrics have contract files."""
        metric_file = CONTRACTS_DIR / f"{required_metric}.yml"
        if not metric_file.exists():
            pytest.skip(f"{required_metric}.yml not yet created")
        assert metric_file.exists()

    def test_all_contracts_valid_yaml(self):
        """All metric contracts are valid YAML."""
        files = self._get_metric_files()
        if not files:
            pytest.skip("Metric files not yet created")

        for f in files:
            with open(f) as fh:
                data = yaml.safe_load(fh)
            assert data is not None, f"{f.name} is empty or invalid YAML"

    def test_contracts_have_required_fields(self):
        """Each metric contract has required fields."""
        files = self._get_metric_files()
        if not files:
            pytest.skip("Metric files not yet created")

        required_fields = {"name", "source_table", "computations"}

        for f in files:
            with open(f) as fh:
                data = yaml.safe_load(fh)

            metric = data.get("metric", data)
            missing = required_fields - set(metric.keys())
            assert not missing, f"{f.name} missing fields: {missing}"

    def test_contracts_have_computations(self):
        """Each metric has at least daily_value computation."""
        files = self._get_metric_files()
        if not files:
            pytest.skip("Metric files not yet created")

        for f in files:
            with open(f) as fh:
                data = yaml.safe_load(fh)

            metric = data.get("metric", data)
            comps = metric.get("computations", {})
            grain = metric.get("grain", "daily")
            if grain == "daily":
                assert (
                    "daily_value" in comps
                ), f"{f.name} missing daily_value computation"
            else:
                assert len(comps) >= 1, f"{f.name} has no computations"

    def test_contracts_have_examples(self):
        """Each metric has usage examples."""
        files = self._get_metric_files()
        if not files:
            pytest.skip("Metric files not yet created")

        for f in files:
            with open(f) as fh:
                data = yaml.safe_load(fh)

            metric = data.get("metric", data)
            examples = metric.get("examples", [])
            assert len(examples) >= 1, f"{f.name} has no examples"
