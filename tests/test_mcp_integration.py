"""Integration tests for all 8 MCP tools using in-memory DuckDB.

Tests all tools with synthetic data to verify end-to-end functionality
without requiring the real health database.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Add project to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.mcp.health_tools import HealthTools


@pytest.fixture
def mcp_db():
    """In-memory DuckDB with silver + agent data for MCP tool testing."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    # Silver tables
    con.execute("""
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.daily_sleep (day, sleep_score) VALUES
        ('2026-02-01', 82), ('2026-02-02', 75), ('2026-02-03', 91),
        ('2026-02-04', 68), ('2026-02-05', 85), ('2026-02-06', 79),
        ('2026-02-07', 88)
    """)

    con.execute("""
        CREATE TABLE silver.daily_readiness (
            sk_date INTEGER, day DATE, readiness_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.daily_readiness (day, readiness_score) VALUES
        ('2026-02-01', 79), ('2026-02-02', 72), ('2026-02-03', 88),
        ('2026-02-04', 65), ('2026-02-05', 81), ('2026-02-06', 77),
        ('2026-02-07', 85)
    """)

    con.execute("""
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER, steps INTEGER,
            active_calories INTEGER, total_calories INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.daily_activity (day, activity_score, steps) VALUES
        ('2026-02-01', 91, 12450), ('2026-02-02', 68, 5200),
        ('2026-02-03', 85, 9800), ('2026-02-04', 72, 7100),
        ('2026-02-05', 88, 11200), ('2026-02-06', 65, 4800),
        ('2026-02-07', 82, 8500)
    """)

    con.execute("""
        CREATE TABLE silver.daily_spo2 (
            sk_date INTEGER, day DATE, spo2_avg_pct DOUBLE,
            breathing_disturbance_index DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.daily_spo2 (day, spo2_avg_pct, breathing_disturbance_index) VALUES
        ('2026-02-01', 97.5, 0.8), ('2026-02-02', 96.2, 1.2),
        ('2026-02-03', 98.1, 0.5)
    """)

    con.execute("""
        CREATE TABLE silver.daily_stress (
            sk_date INTEGER, day DATE, day_summary VARCHAR,
            stress_high INTEGER, recovery_high INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.daily_stress (day, day_summary, stress_high, recovery_high) VALUES
        ('2026-02-01', 'restored', 120, 480),
        ('2026-02-02', 'stressed', 360, 240),
        ('2026-02-03', 'normal', 200, 400)
    """)

    con.execute("""
        CREATE TABLE silver.workout (
            sk_date INTEGER, day DATE, workout_id VARCHAR, activity VARCHAR,
            intensity VARCHAR, calories INTEGER, distance_meters INTEGER,
            start_datetime TIMESTAMP, end_datetime TIMESTAMP,
            duration_seconds INTEGER, label VARCHAR, source VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.workout (day, activity, intensity, calories, distance_meters,
            start_datetime, duration_seconds) VALUES
        ('2026-02-01', 'running', 'high', 450, 5000, '2026-02-01 07:00:00', 1800),
        ('2026-02-03', 'cycling', 'medium', 300, 15000, '2026-02-03 17:00:00', 3600),
        ('2026-02-05', 'running', 'high', 500, 6000, '2026-02-05 07:00:00', 2100)
    """)

    # Agent tables
    con.execute("""
        CREATE TABLE agent.patient_profile (
            category VARCHAR, profile_key VARCHAR, profile_value VARCHAR,
            last_updated_at TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO agent.patient_profile (category, profile_key, profile_value) VALUES
        ('demographics', 'age', '40'),
        ('demographics', 'biological_sex', 'male'),
        ('demographics', 'height_m', '1.9'),
        ('baselines', 'sleep_score_baseline', '86'),
        ('baselines', 'resting_hr_baseline', '49 bpm')
    """)

    con.execute("""
        CREATE TABLE agent.daily_summaries (
            day DATE, summary_text TEXT, embedding FLOAT[]
        )
    """)
    con.execute("""
        INSERT INTO agent.daily_summaries (day, summary_text) VALUES
        ('2026-02-01', 'Good sleep quality with score 82. Active day with 12450 steps and a morning run.'),
        ('2026-02-02', 'Poor sleep at 75. Low activity day with only 5200 steps. Stress levels elevated.'),
        ('2026-02-03', 'Excellent sleep score 91. Moderate activity with evening cycling session.')
    """)

    con.execute("""
        CREATE TABLE agent.knowledge_base (
            id VARCHAR PRIMARY KEY, title VARCHAR NOT NULL, content TEXT NOT NULL,
            insight_type VARCHAR NOT NULL, confidence FLOAT NOT NULL,
            tags VARCHAR DEFAULT '', embedding FLOAT[],
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    yield con
    con.close()


@pytest.fixture
def tools(mcp_db):
    """HealthTools instance backed by in-memory DuckDB."""
    return HealthTools(mcp_db)


class TestQueryHealth:
    """Tool 1: query_health — query metrics via semantic contracts."""

    def test_daily_value_range(self, tools):
        result = tools.query_health("sleep_score", "2026-02-01:2026-02-07")
        assert "sleep_score" in result
        assert "82" in result
        assert "91" in result

    def test_daily_value_single_date(self, tools):
        result = tools.query_health("sleep_score", "2026-02-03")
        assert "91" in result

    def test_period_average(self, tools):
        result = tools.query_health("sleep_score", "2026-02-01:2026-02-07", "period_average")
        assert "avg_sleep_score" in result

    def test_trend(self, tools):
        result = tools.query_health("sleep_score", "2026-02-01:2026-02-07", "trend")
        assert "rolling_7d" in result

    def test_distribution(self, tools):
        result = tools.query_health("sleep_score", "2026-02-01:2026-02-07", "distribution")
        assert "excellent" in result or "good" in result or "fair" in result

    def test_steps(self, tools):
        result = tools.query_health("steps", "2026-02-01:2026-02-07")
        assert "12450" in result

    def test_readiness(self, tools):
        result = tools.query_health("readiness_score", "2026-02-01:2026-02-07")
        assert "88" in result

    def test_activity_score(self, tools):
        result = tools.query_health("activity_score", "2026-02-01:2026-02-07")
        assert "91" in result

    def test_blood_oxygen(self, tools):
        result = tools.query_health("blood_oxygen", "2026-02-01:2026-02-03")
        assert "97.5" in result or "98.1" in result

    def test_daily_stress(self, tools):
        result = tools.query_health("daily_stress", "2026-02-01:2026-02-03")
        assert "restored" in result

    def test_workout(self, tools):
        result = tools.query_health("workout", "2026-02-01:2026-02-07")
        assert "running" in result
        assert "cycling" in result

    def test_workout_period_summary(self, tools):
        result = tools.query_health("workout", "2026-02-01:2026-02-07", "period_summary")
        assert "3" in result  # 3 workouts

    def test_invalid_metric(self, tools):
        result = tools.query_health("nonexistent", "last_7_days")
        assert "Error" in result
        assert "Unknown metric" in result

    def test_invalid_computation(self, tools):
        result = tools.query_health("sleep_score", "last_7_days", "nonexistent")
        assert "Error" in result
        assert "Unknown computation" in result

    def test_no_data_range(self, tools):
        result = tools.query_health("sleep_score", "2099-01-01:2099-01-07")
        assert "No data found" in result


class TestSearchMemory:
    """Tool 2: search_memory — text search over agent memory."""

    def test_search_finds_summaries(self, tools):
        result = tools.search_memory("sleep quality", 3)
        assert "Daily Summaries" in result

    def test_search_returns_relevant(self, tools):
        result = tools.search_memory("running", 5)
        assert "run" in result.lower()

    def test_search_no_results(self, tools):
        result = tools.search_memory("xyznonexistent", 5)
        assert "No results" in result


class TestGetProfile:
    """Tool 3: get_profile — patient core memory."""

    def test_get_all(self, tools):
        result = tools.get_profile()
        assert "Patient Profile" in result
        assert "40" in result
        assert "male" in result

    def test_filter_baselines(self, tools):
        result = tools.get_profile(["baselines"])
        assert "baselines" in result
        assert "86" in result

    def test_filter_demographics(self, tools):
        result = tools.get_profile(["demographics"])
        assert "demographics" in result
        assert "male" in result

    def test_empty_category(self, tools):
        result = tools.get_profile(["nonexistent"])
        assert "No profile data" in result


class TestDiscoverCorrelations:
    """Tool 4: discover_correlations — lag correlation analysis."""

    def test_same_day_correlation(self, tools):
        result = tools.discover_correlations(
            "daily_sleep.sleep_score", "daily_readiness.readiness_score", 1
        )
        assert "Correlation" in result
        assert "Lag (days)" in result

    def test_correlation_has_interpretation(self, tools):
        result = tools.discover_correlations(
            "daily_sleep.sleep_score", "daily_readiness.readiness_score"
        )
        assert "Strongest correlation" in result

    def test_invalid_metrics(self, tools):
        result = tools.discover_correlations(
            "nonexistent.col", "also_nonexistent.col"
        )
        assert "Error" in result or "Could not compute" in result


class TestGetMetricDefinition:
    """Tool 5: get_metric_definition — YAML contract lookup."""

    def test_sleep_score(self, tools):
        result = tools.get_metric_definition("sleep_score")
        assert "Sleep Score" in result
        assert "Computations" in result

    def test_has_thresholds(self, tools):
        result = tools.get_metric_definition("sleep_score")
        assert "Thresholds" in result or "threshold" in result.lower()

    def test_has_examples(self, tools):
        result = tools.get_metric_definition("sleep_score")
        assert "Example" in result

    def test_invalid_metric(self, tools):
        result = tools.get_metric_definition("nonexistent")
        assert "Error" in result
        assert "Unknown metric" in result


class TestRecordInsight:
    """Tool 6: record_insight — write to knowledge base."""

    def test_record_pattern(self, tools):
        result = tools.record_insight("Test pattern", "Content here", "pattern", 0.8, ["test"])
        assert "recorded successfully" in result

    def test_record_anomaly(self, tools):
        result = tools.record_insight("Anomaly found", "Details", "anomaly", 0.9)
        assert "recorded successfully" in result

    def test_invalid_type(self, tools):
        result = tools.record_insight("Bad", "Content", "invalid_type", 0.5)
        assert "Error" in result

    def test_invalid_confidence(self, tools):
        result = tools.record_insight("Bad", "Content", "pattern", 1.5)
        assert "Error" in result


class TestGetSchemaContext:
    """Tool 7: get_schema_context — schema pruning by category."""

    def test_sleep_category(self, tools):
        result = tools.get_schema_context("sleep")
        assert "daily_sleep" in result

    def test_activity_category(self, tools):
        result = tools.get_schema_context("activity")
        assert "daily_activity" in result or "workout" in result

    def test_invalid_category(self, tools):
        result = tools.get_schema_context("nonexistent")
        assert "Error" in result or "Unknown" in result

    def test_has_computations(self, tools):
        result = tools.get_schema_context("sleep")
        assert "daily_value" in result


class TestRunCustomQuery:
    """Tool 8: run_custom_query — read-only escape hatch."""

    def test_valid_select(self, tools):
        result = tools.run_custom_query(
            "SELECT COUNT(*) AS n FROM silver.daily_sleep",
            "Testing count"
        )
        assert "7" in result

    def test_blocks_delete(self, tools):
        result = tools.run_custom_query("DELETE FROM silver.daily_sleep", "Bad")
        assert "Error" in result
        assert "forbidden" in result.lower() or "DELETE" in result

    def test_blocks_drop(self, tools):
        result = tools.run_custom_query("DROP TABLE silver.daily_sleep", "Bad")
        assert "Error" in result

    def test_blocks_insert(self, tools):
        result = tools.run_custom_query(
            "INSERT INTO silver.daily_sleep VALUES (1, '2026-01-01', 80, null, null, null, null)",
            "Bad"
        )
        assert "Error" in result

    def test_empty_result(self, tools):
        result = tools.run_custom_query(
            "SELECT * FROM silver.daily_sleep WHERE sleep_score > 999",
            "No results expected"
        )
        assert "no results" in result.lower()
