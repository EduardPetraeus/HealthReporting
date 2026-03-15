"""Tests for the get_food_context MCP tool."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import health_platform.mcp.health_tools as health_tools_mod
import pytest
import yaml

# Path to the actual food_context.yml
FOOD_CONTEXT_PATH = (
    Path(__file__).resolve().parents[1]
    / "health_unified_platform"
    / "health_platform"
    / "contracts"
    / "food_context.yml"
)

SAMPLE_YAML = {
    "version": "1.0",
    "foods": [
        {
            "lifesum_name": "Protein Smoothie",
            "description": "Homemade protein shake with banana, oats, and whey",
            "ingredients": ["1 banana", "40g whey protein", "30g oats"],
            "notes": "Post-workout breakfast",
        },
        {
            "lifesum_name": "Salat med kylling",
            "description": "Grilled chicken on mixed greens",
            "ingredients": ["150g chicken breast", "mixed greens"],
            "notes": "",
        },
        {
            "lifesum_name": "Banan",
            "description": "",
            "ingredients": [],
            "notes": "",
        },
    ],
}


@pytest.fixture
def tools_with_temp_yaml(tmp_path):
    """Create HealthTools with __file__ patched to use a temp contracts dir."""
    import duckdb
    from health_platform.mcp.health_tools import HealthTools

    # Set up temp directory structure: <tmp>/contracts/food_context.yml
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    food_file = contracts_dir / "food_context.yml"
    with open(food_file, "w") as f:
        yaml.dump(SAMPLE_YAML, f)

    # HealthTools needs a DuckDB connection (not used by get_food_context)
    con = duckdb.connect(":memory:")
    tools = HealthTools(con)

    # Fake __file__ so Path(__file__).parents[1] / "contracts" points to tmp
    fake_module = tmp_path / "mcp" / "health_tools.py"
    fake_module.parent.mkdir(parents=True, exist_ok=True)
    fake_module.touch()

    return tools, food_file, str(fake_module)


def _call(tools, fake_file, food_item=None, yaml_data=None, food_file=None):
    """Call get_food_context with patched __file__."""
    if yaml_data is not None and food_file is not None:
        with open(food_file, "w") as f:
            yaml.dump(yaml_data, f)
    with patch.object(health_tools_mod, "__file__", fake_file):
        return tools.get_food_context(food_item)


class TestFoodContextYaml:
    """Tests for the actual food_context.yml file."""

    def test_yaml_is_valid(self):
        """Verify the actual food_context.yml parses without errors."""
        if not FOOD_CONTEXT_PATH.exists():
            pytest.skip("food_context.yml not yet created")

        with open(FOOD_CONTEXT_PATH) as f:
            data = yaml.safe_load(f)

        assert data is not None
        assert "version" in data
        assert "foods" in data
        assert isinstance(data["foods"], list)

    def test_yaml_has_required_fields(self):
        """Every food entry must have lifesum_name and description."""
        if not FOOD_CONTEXT_PATH.exists():
            pytest.skip("food_context.yml not yet created")

        with open(FOOD_CONTEXT_PATH) as f:
            data = yaml.safe_load(f)

        for food in data["foods"]:
            assert "lifesum_name" in food, f"Missing lifesum_name in {food}"
            assert "description" in food, f"Missing description in {food}"

    def test_yaml_no_duplicate_names(self):
        """Each lifesum_name should be unique."""
        if not FOOD_CONTEXT_PATH.exists():
            pytest.skip("food_context.yml not yet created")

        with open(FOOD_CONTEXT_PATH) as f:
            data = yaml.safe_load(f)

        names = [f["lifesum_name"] for f in data["foods"]]
        assert len(names) == len(set(names)), f"Duplicate names found: {names}"


class TestGetFoodContext:
    """Tests for the get_food_context tool method."""

    def test_returns_all_foods(self, tools_with_temp_yaml):
        """No filter returns all entries."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file)
        assert "Food Context (3 items)" in result
        assert "Protein Smoothie" in result
        assert "Banan" in result

    def test_fuzzy_match_finds_entry(self, tools_with_temp_yaml):
        """Substring match works case-insensitively."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "smoothie")
        assert "Protein Smoothie" in result
        assert "Homemade protein shake" in result

    def test_fuzzy_match_case_insensitive(self, tools_with_temp_yaml):
        """Match is case-insensitive."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "SALAT")
        assert "Salat med kylling" in result

    def test_fuzzy_match_multiple_results(self, tools_with_temp_yaml):
        """Substring can match multiple entries."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "ban")
        assert "Banan" in result

    def test_no_match_shows_available(self, tools_with_temp_yaml):
        """Unknown food shows available names."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "pizza")
        assert "No food context found" in result
        assert "Available foods" in result
        assert "Protein Smoothie" in result

    def test_empty_description_shows_placeholder(self, tools_with_temp_yaml):
        """Foods with empty description show placeholder text."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "Banan")
        assert "not yet filled in" in result

    def test_filled_description_shows_content(self, tools_with_temp_yaml):
        """Foods with description show actual content."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "Smoothie")
        assert "Homemade protein shake" in result

    def test_ingredients_rendered(self, tools_with_temp_yaml):
        """Ingredients list is rendered as bullet points."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "Smoothie")
        assert "- 1 banana" in result
        assert "- 40g whey protein" in result

    def test_notes_rendered(self, tools_with_temp_yaml):
        """Notes are rendered when present."""
        tools, _, fake_file = tools_with_temp_yaml
        result = _call(tools, fake_file, "Smoothie")
        assert "Post-workout breakfast" in result

    def test_missing_yaml_returns_error(self, tools_with_temp_yaml):
        """Missing food_context.yml returns error message."""
        tools, _, _ = tools_with_temp_yaml
        # Point to a non-existent directory
        with patch.object(
            health_tools_mod, "__file__", "/nonexistent/mcp/health_tools.py"
        ):
            result = tools.get_food_context(None)
        assert "not found" in result

    def test_empty_foods_list(self, tools_with_temp_yaml):
        """Empty foods list returns appropriate message."""
        tools, food_file, fake_file = tools_with_temp_yaml
        empty_yaml = {"version": "1.0", "foods": []}
        result = _call(tools, fake_file, yaml_data=empty_yaml, food_file=food_file)
        assert "No foods" in result or "food context" in result.lower()
