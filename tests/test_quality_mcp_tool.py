"""Tests for the check_data_quality MCP tool."""

from __future__ import annotations

import pytest
import yaml
from health_platform.mcp.health_tools import HealthTools


@pytest.fixture
def mcp_rules_path(tmp_path):
    """Write minimal test rules for MCP tool testing."""
    rules = {
        "tables": {
            "daily_sleep": {
                "not_null": ["business_key_hash", "sk_date", "day", "sleep_score"],
                "unique": ["business_key_hash"],
                "row_count": {"min_rows": 1},
                "value_range": {"sleep_score": {"min": 0, "max": 100}},
            },
        }
    }
    path = tmp_path / "quality_rules.yaml"
    with open(path, "w") as f:
        yaml.dump(rules, f)
    return path


@pytest.fixture
def tools_with_dq(seeded_db, mcp_rules_path, monkeypatch):
    """HealthTools instance with DQ rules pointing to test rules."""
    monkeypatch.setattr(
        "health_platform.quality.rule_loader._DEFAULT_RULES_PATH",
        mcp_rules_path,
    )
    return HealthTools(seeded_db)


class TestMcpDataQuality:
    def test_all_tables(self, tools_with_dq):
        result = tools_with_dq.check_data_quality()
        assert "Data Quality Report" in result
        assert "passed" in result

    def test_single_table(self, tools_with_dq):
        result = tools_with_dq.check_data_quality(table="daily_sleep")
        assert "Data Quality Report" in result

    def test_unknown_table(self, tools_with_dq):
        result = tools_with_dq.check_data_quality(table="nonexistent_xyz")
        assert "No rules defined" in result or "does not exist" in result
