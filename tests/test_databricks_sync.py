"""
Tests for Databricks full sync — verifying all SQL files, configs,
and job definitions are complete and valid.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

# Base paths
ROOT = Path(__file__).parent.parent / "health_unified_platform" / "health_platform"
DATABRICKS_ROOT = ROOT / "transformation_logic" / "databricks"
SILVER_SQL_DIR = DATABRICKS_ROOT / "silver" / "sql"
GOLD_SQL_DIR = DATABRICKS_ROOT / "gold" / "sql"
UNITY_CATALOG_DIR = DATABRICKS_ROOT / "unity-catalog"
JOBS_DIR = DATABRICKS_ROOT / "jobs"
CONFIG_DIR = DATABRICKS_ROOT / "config"


# =============================================================================
# Required tables / files lists
# =============================================================================

REQUIRED_SILVER_SQL = [
    "daily_sleep.sql",
    "daily_activity.sql",
    "daily_readiness.sql",
    "heart_rate.sql",
    "oura_heart_rate.sql",
    "blood_pressure.sql",
    "blood_oxygen.sql",
    "weight.sql",
    "daily_meal.sql",
    "daily_annotations.sql",
    # Newly ported
    "step_count.sql",
    "daily_stress.sql",
    "workout.sql",
    "daily_spo2.sql",
    "body_temperature.sql",
    "resting_heart_rate.sql",
    "water_intake.sql",
    "daily_walking_gait.sql",
    "mindful_session.sql",
    "respiratory_rate.sql",
    "toothbrushing.sql",
    "supplement_log.sql",
    "lab_results.sql",
    "daily_energy_by_source.sql",
    "hrv.sql",
    "vo2_max.sql",
    "daily_resilience.sql",
]

REQUIRED_GOLD_SQL = [
    # Dimensions
    "dim_date.sql",
    "dim_source.sql",
    "dim_metric.sql",
    "dim_body_system.sql",
    "dim_health_zone.sql",
    "dim_meal_type.sql",
    "dim_supplement.sql",
    "dim_time_of_day.sql",
    "dim_workout_type.sql",
    "dim_lab_marker.sql",
    # Facts
    "fct_daily_health_score.sql",
    "fct_daily_vitals_summary.sql",
    "fct_daily_nutrition.sql",
    "fct_body_measurement.sql",
    "fct_lab_result.sql",
    "fct_sleep_session.sql",
    "fct_workout.sql",
    # Existing
    "daily_heart_rate_summary.sql",
    "vw_daily_annotations.sql",
    "vw_heart_rate_avg_per_day.sql",
]


# =============================================================================
# Test 1: Silver SQL files exist for all required tables
# =============================================================================


class TestSilverSqlFiles:
    """Verify all required silver SQL files exist and have content."""

    @pytest.mark.parametrize("sql_file", REQUIRED_SILVER_SQL)
    def test_silver_sql_file_exists(self, sql_file: str) -> None:
        """Each required silver SQL file must exist."""
        path = SILVER_SQL_DIR / sql_file
        assert path.exists(), f"Missing silver SQL file: {path}"

    @pytest.mark.parametrize("sql_file", REQUIRED_SILVER_SQL)
    def test_silver_sql_file_not_empty(self, sql_file: str) -> None:
        """Each silver SQL file must have real content (not just whitespace)."""
        path = SILVER_SQL_DIR / sql_file
        if path.exists():
            content = path.read_text(encoding="utf-8").strip()
            assert len(content) > 50, f"Silver SQL file is too short: {sql_file}"

    def test_silver_sql_file_count(self) -> None:
        """At minimum we need all 27 required silver SQL files."""
        sql_files = list(SILVER_SQL_DIR.glob("*.sql"))
        # Exclude template and README
        real_files = [
            f for f in sql_files if not f.name.startswith("_") and f.suffix == ".sql"
        ]
        assert len(real_files) >= len(
            REQUIRED_SILVER_SQL
        ), f"Expected >= {len(REQUIRED_SILVER_SQL)} silver SQL files, found {len(real_files)}"


# =============================================================================
# Test 2: Gold SQL files exist for all required tables
# =============================================================================


class TestGoldSqlFiles:
    """Verify all required gold SQL files exist and have content."""

    @pytest.mark.parametrize("sql_file", REQUIRED_GOLD_SQL)
    def test_gold_sql_file_exists(self, sql_file: str) -> None:
        """Each required gold SQL file must exist."""
        path = GOLD_SQL_DIR / sql_file
        assert path.exists(), f"Missing gold SQL file: {path}"

    @pytest.mark.parametrize("sql_file", REQUIRED_GOLD_SQL)
    def test_gold_sql_file_not_empty(self, sql_file: str) -> None:
        """Each gold SQL file must have real content."""
        path = GOLD_SQL_DIR / sql_file
        if path.exists():
            content = path.read_text(encoding="utf-8").strip()
            assert len(content) > 50, f"Gold SQL file is too short: {sql_file}"

    def test_gold_sql_file_count(self) -> None:
        """At minimum we need all required gold SQL files."""
        sql_files = list(GOLD_SQL_DIR.glob("*.sql"))
        real_files = [f for f in sql_files if f.suffix == ".sql"]
        assert len(real_files) >= len(
            REQUIRED_GOLD_SQL
        ), f"Expected >= {len(REQUIRED_GOLD_SQL)} gold SQL files, found {len(real_files)}"


# =============================================================================
# Test 3: Unity Catalog DDL is valid
# =============================================================================


class TestUnityCatalog:
    """Verify Unity Catalog setup files exist and are structurally valid."""

    def test_catalog_setup_exists(self) -> None:
        """catalog_setup.sql must exist."""
        assert (UNITY_CATALOG_DIR / "catalog_setup.sql").exists()

    def test_table_definitions_exists(self) -> None:
        """table_definitions.sql must exist."""
        assert (UNITY_CATALOG_DIR / "table_definitions.sql").exists()

    def test_catalog_setup_has_create_catalog(self) -> None:
        """catalog_setup.sql must contain CREATE CATALOG statement."""
        content = (UNITY_CATALOG_DIR / "catalog_setup.sql").read_text(encoding="utf-8")
        assert "CREATE CATALOG" in content.upper()

    def test_catalog_setup_has_create_schema(self) -> None:
        """catalog_setup.sql must contain CREATE SCHEMA for bronze, silver, gold."""
        content = (UNITY_CATALOG_DIR / "catalog_setup.sql").read_text(encoding="utf-8")
        for schema in ["bronze", "silver", "gold"]:
            assert (
                schema in content.lower()
            ), f"Missing schema '{schema}' in catalog_setup.sql"

    def test_catalog_setup_has_grants(self) -> None:
        """catalog_setup.sql must contain GRANT statements."""
        content = (UNITY_CATALOG_DIR / "catalog_setup.sql").read_text(encoding="utf-8")
        assert "GRANT" in content.upper()

    def test_table_definitions_has_create_table(self) -> None:
        """table_definitions.sql must contain CREATE TABLE statements."""
        content = (UNITY_CATALOG_DIR / "table_definitions.sql").read_text(
            encoding="utf-8"
        )
        assert (
            content.upper().count("CREATE TABLE") >= 5
        ), "Expected at least 5 CREATE TABLE statements in table_definitions.sql"

    def test_table_definitions_uses_delta(self) -> None:
        """table_definitions.sql must use DELTA format."""
        content = (UNITY_CATALOG_DIR / "table_definitions.sql").read_text(
            encoding="utf-8"
        )
        assert "USING DELTA" in content.upper()


# =============================================================================
# Test 4: Job YAML files are valid
# =============================================================================


class TestJobYaml:
    """Verify job YAML definitions are valid and complete."""

    def test_daily_pipeline_exists(self) -> None:
        """daily_pipeline.yml must exist."""
        assert (JOBS_DIR / "daily_pipeline.yml").exists()

    def test_weekly_pipeline_exists(self) -> None:
        """weekly_pipeline.yml must exist."""
        assert (JOBS_DIR / "weekly_pipeline.yml").exists()

    def test_daily_pipeline_valid_yaml(self) -> None:
        """daily_pipeline.yml must be valid YAML."""
        content = (JOBS_DIR / "daily_pipeline.yml").read_text(encoding="utf-8")
        # Replace ${var.xxx} placeholders to avoid YAML parse errors
        content = re.sub(r"\$\{[^}]+\}", "PLACEHOLDER", content)
        data = yaml.safe_load(content)
        assert data is not None
        assert "resources" in data

    def test_weekly_pipeline_valid_yaml(self) -> None:
        """weekly_pipeline.yml must be valid YAML."""
        content = (JOBS_DIR / "weekly_pipeline.yml").read_text(encoding="utf-8")
        content = re.sub(r"\$\{[^}]+\}", "PLACEHOLDER", content)
        data = yaml.safe_load(content)
        assert data is not None
        assert "resources" in data

    def test_daily_pipeline_has_schedule(self) -> None:
        """daily_pipeline.yml must define a schedule."""
        content = (JOBS_DIR / "daily_pipeline.yml").read_text(encoding="utf-8")
        assert "schedule" in content.lower()
        assert "quartz_cron_expression" in content

    def test_weekly_pipeline_has_schedule(self) -> None:
        """weekly_pipeline.yml must define a schedule."""
        content = (JOBS_DIR / "weekly_pipeline.yml").read_text(encoding="utf-8")
        assert "schedule" in content.lower()
        assert "SUN" in content.upper() or "7" in content

    def test_daily_pipeline_has_bronze_silver_gold(self) -> None:
        """daily_pipeline.yml must reference bronze, silver, and gold tasks."""
        content = (JOBS_DIR / "daily_pipeline.yml").read_text(encoding="utf-8")
        assert "bronze" in content.lower()
        assert "silver" in content.lower()
        assert "gold" in content.lower()


# =============================================================================
# Test 5: Environment config separation
# =============================================================================


class TestEnvConfig:
    """Verify dev and prd environment configs are properly separated."""

    def test_dev_config_exists(self) -> None:
        """dev.yml must exist."""
        assert (CONFIG_DIR / "dev.yml").exists()

    def test_prd_config_exists(self) -> None:
        """prd.yml must exist."""
        assert (CONFIG_DIR / "prd.yml").exists()

    def test_cluster_config_exists(self) -> None:
        """cluster_config.yml must exist."""
        assert (CONFIG_DIR / "cluster_config.yml").exists()

    def test_dev_config_valid_yaml(self) -> None:
        """dev.yml must be valid YAML with expected keys."""
        data = yaml.safe_load((CONFIG_DIR / "dev.yml").read_text(encoding="utf-8"))
        assert data is not None
        assert data.get("environment") == "dev"

    def test_prd_config_valid_yaml(self) -> None:
        """prd.yml must be valid YAML with expected keys."""
        data = yaml.safe_load((CONFIG_DIR / "prd.yml").read_text(encoding="utf-8"))
        assert data is not None
        assert data.get("environment") == "prd"

    def test_env_configs_differ(self) -> None:
        """dev and prd configs must have different settings."""
        dev_data = yaml.safe_load((CONFIG_DIR / "dev.yml").read_text(encoding="utf-8"))
        prd_data = yaml.safe_load((CONFIG_DIR / "prd.yml").read_text(encoding="utf-8"))
        assert dev_data["environment"] != prd_data["environment"]

    def test_cluster_config_valid_yaml(self) -> None:
        """cluster_config.yml must be valid YAML."""
        data = yaml.safe_load(
            (CONFIG_DIR / "cluster_config.yml").read_text(encoding="utf-8")
        )
        assert data is not None
        assert "clusters" in data


# =============================================================================
# Test 6: Silver SQL uses MERGE pattern
# =============================================================================

# These are the newly created silver SQL files that must follow the MERGE pattern
NEW_SILVER_SQL = [
    "step_count.sql",
    "daily_stress.sql",
    "workout.sql",
    "daily_spo2.sql",
    "body_temperature.sql",
    "resting_heart_rate.sql",
    "water_intake.sql",
    "daily_walking_gait.sql",
    "mindful_session.sql",
    "respiratory_rate.sql",
    "toothbrushing.sql",
    "supplement_log.sql",
    "lab_results.sql",
    "daily_energy_by_source.sql",
    "hrv.sql",
    "vo2_max.sql",
    "daily_resilience.sql",
]


class TestSilverMergePattern:
    """Verify all new silver SQL files follow the staging + MERGE + drop pattern."""

    @pytest.mark.parametrize("sql_file", NEW_SILVER_SQL)
    def test_uses_merge_into(self, sql_file: str) -> None:
        """Each new silver SQL must contain a MERGE INTO statement."""
        path = SILVER_SQL_DIR / sql_file
        content = path.read_text(encoding="utf-8")
        assert "MERGE INTO" in content.upper(), f"Missing MERGE INTO in {sql_file}"

    @pytest.mark.parametrize("sql_file", NEW_SILVER_SQL)
    def test_uses_sha2_for_hashing(self, sql_file: str) -> None:
        """Each new silver SQL must use sha2() for row hashing."""
        path = SILVER_SQL_DIR / sql_file
        content = path.read_text(encoding="utf-8")
        assert "sha2(" in content.lower(), f"Missing sha2() in {sql_file}"

    @pytest.mark.parametrize("sql_file", NEW_SILVER_SQL)
    def test_creates_staging_table(self, sql_file: str) -> None:
        """Each new silver SQL must create a staging table."""
        path = SILVER_SQL_DIR / sql_file
        content = path.read_text(encoding="utf-8")
        assert (
            "staging" in content.lower()
        ), f"Missing staging table pattern in {sql_file}"

    @pytest.mark.parametrize("sql_file", NEW_SILVER_SQL)
    def test_drops_staging_table(self, sql_file: str) -> None:
        """Each new silver SQL must drop the staging table after merge."""
        path = SILVER_SQL_DIR / sql_file
        content = path.read_text(encoding="utf-8")
        assert (
            "DROP TABLE IF EXISTS" in content.upper()
        ), f"Missing DROP staging in {sql_file}"

    @pytest.mark.parametrize("sql_file", NEW_SILVER_SQL)
    def test_has_row_hash_change_detection(self, sql_file: str) -> None:
        """Each new silver SQL must use row_hash for change detection."""
        path = SILVER_SQL_DIR / sql_file
        content = path.read_text(encoding="utf-8")
        assert "row_hash" in content.lower(), f"Missing row_hash in {sql_file}"

    @pytest.mark.parametrize("sql_file", NEW_SILVER_SQL)
    def test_references_health_dw_catalog(self, sql_file: str) -> None:
        """Each new silver SQL must reference health_dw catalog."""
        path = SILVER_SQL_DIR / sql_file
        content = path.read_text(encoding="utf-8")
        assert (
            "health_dw." in content
        ), f"Missing health_dw. catalog reference in {sql_file}"
