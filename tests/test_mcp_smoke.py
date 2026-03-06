"""Smoke tests for all 8 MCP tools using in-memory DuckDB with real-world schema.

Validates that every tool returns correct data from synthetic test data
that matches the actual production database schema.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.mcp.health_tools import HealthTools


@pytest.fixture
def smoke_db():
    """In-memory DuckDB matching real production schema."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    # --- Silver tables (matching real DB schema) ---

    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            timestamp TIMESTAMP,
            contributor_deep_sleep INTEGER, contributor_efficiency INTEGER,
            contributor_latency INTEGER, contributor_rem_sleep INTEGER,
            contributor_restfulness INTEGER, contributor_timing INTEGER,
            contributor_total_sleep INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_sleep (day, sleep_score) VALUES
        ('2026-02-20', 82), ('2026-02-21', 75), ('2026-02-22', 91),
        ('2026-02-23', 68), ('2026-02-24', 85), ('2026-02-25', 79),
        ('2026-02-26', 88), ('2026-02-27', 77), ('2026-02-28', 93),
        ('2026-03-01', 84), ('2026-03-02', 72), ('2026-03-03', 90),
        ('2026-03-04', 81), ('2026-03-05', 86), ('2026-03-06', 78)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.daily_readiness (
            sk_date INTEGER, day DATE, readiness_score INTEGER,
            timestamp TIMESTAMP,
            temperature_deviation DOUBLE, temperature_trend_deviation DOUBLE,
            contributor_activity_balance INTEGER, contributor_body_temperature INTEGER,
            contributor_hrv_balance INTEGER, contributor_previous_day_activity INTEGER,
            contributor_previous_night INTEGER, contributor_recovery_index INTEGER,
            contributor_resting_heart_rate INTEGER, contributor_sleep_balance INTEGER,
            contributor_sleep_regularity INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_readiness (day, readiness_score) VALUES
        ('2026-02-20', 79), ('2026-02-21', 72), ('2026-02-22', 88),
        ('2026-02-23', 65), ('2026-02-24', 81), ('2026-02-25', 77),
        ('2026-02-26', 85), ('2026-02-27', 73), ('2026-02-28', 90),
        ('2026-03-01', 82), ('2026-03-02', 68), ('2026-03-03', 87),
        ('2026-03-04', 78), ('2026-03-05', 83), ('2026-03-06', 76)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER, steps INTEGER,
            timestamp TIMESTAMP,
            total_calories INTEGER, active_calories INTEGER, target_calories INTEGER,
            average_met_minutes DOUBLE, equivalent_walking_distance INTEGER,
            high_activity_time INTEGER, medium_activity_time INTEGER,
            low_activity_time INTEGER, sedentary_time INTEGER,
            resting_time INTEGER, non_wear_time INTEGER,
            high_activity_met_minutes INTEGER, medium_activity_met_minutes INTEGER,
            low_activity_met_minutes INTEGER, sedentary_met_minutes INTEGER,
            inactivity_alerts INTEGER, target_meters INTEGER, meters_to_target INTEGER,
            contributor_meet_daily_targets INTEGER, contributor_move_every_hour INTEGER,
            contributor_recovery_time INTEGER, contributor_stay_active INTEGER,
            contributor_training_frequency INTEGER, contributor_training_volume INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_activity (day, activity_score, steps, active_calories, total_calories) VALUES
        ('2026-02-20', 91, 12450, 650, 2400), ('2026-02-21', 68, 5200, 320, 2100),
        ('2026-02-22', 85, 9800, 520, 2300), ('2026-02-23', 72, 7100, 410, 2200),
        ('2026-02-24', 88, 11200, 600, 2350), ('2026-02-25', 65, 4800, 280, 2050),
        ('2026-02-26', 82, 8500, 490, 2280), ('2026-03-01', 79, 7800, 450, 2250)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.daily_stress (
            sk_date INTEGER, day DATE, day_summary VARCHAR,
            stress_high INTEGER, recovery_high INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_stress (day, day_summary, stress_high, recovery_high) VALUES
        ('2026-02-20', 'restored', 120, 480),
        ('2026-02-21', 'stressed', 360, 240),
        ('2026-02-22', 'normal', 200, 400),
        ('2026-03-01', 'restored', 100, 500)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.daily_spo2 (
            sk_date INTEGER, day DATE,
            spo2_avg_pct DOUBLE, breathing_disturbance_index DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_spo2 (day, spo2_avg_pct, breathing_disturbance_index) VALUES
        ('2026-02-20', 97.5, 0.8), ('2026-02-21', 96.2, 1.2),
        ('2026-02-22', 98.1, 0.5), ('2026-03-01', 97.8, 0.6)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.workout (
            sk_date INTEGER, day DATE, workout_id VARCHAR, activity VARCHAR,
            intensity VARCHAR, calories INTEGER, distance_meters INTEGER,
            start_datetime TIMESTAMP, end_datetime TIMESTAMP,
            duration_seconds INTEGER, label VARCHAR, source VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.workout (day, activity, intensity, calories, distance_meters,
            start_datetime, duration_seconds) VALUES
        ('2026-02-20', 'running', 'high', 450, 5000, '2026-02-20 07:00:00', 1800),
        ('2026-02-22', 'cycling', 'medium', 300, 15000, '2026-02-22 17:00:00', 3600),
        ('2026-02-25', 'running', 'high', 500, 6000, '2026-02-25 07:00:00', 2100)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.heart_rate (
            sk_date INTEGER, sk_time VARCHAR,
            timestamp TIMESTAMP, bpm INTEGER, source_name VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.heart_rate (timestamp, bpm, source_name) VALUES
        ('2026-02-20 03:00:00', 52, 'Oura Ring'),
        ('2026-02-20 12:00:00', 78, 'Apple Watch'),
        ('2026-02-21 03:00:00', 55, 'Oura Ring')
    """
    )

    con.execute(
        """
        CREATE TABLE silver.weight (
            sk_date INTEGER, sk_time VARCHAR,
            datetime TIMESTAMP, weight_kg DOUBLE,
            fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
            muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.weight (datetime, weight_kg, fat_mass_kg, muscle_mass_kg) VALUES
        ('2026-02-20 07:00:00', 82.5, 15.2, 38.1),
        ('2026-03-01 07:00:00', 82.3, 15.0, 38.3)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.personal_info (
            age INTEGER, weight_kg DOUBLE, height_m DOUBLE,
            biological_sex VARCHAR, email VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.personal_info (age, weight_kg, height_m, biological_sex, email)
        VALUES (45, 75.0, 1.78, 'male', 'test@example.com')
    """
    )

    # --- Agent tables (matching real DB schema) ---

    con.execute(
        """
        CREATE TABLE agent.patient_profile (
            profile_key VARCHAR, profile_value VARCHAR,
            numeric_value DOUBLE, category VARCHAR,
            description VARCHAR, computed_from VARCHAR,
            last_updated_at TIMESTAMP, update_frequency VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO agent.patient_profile (category, profile_key, profile_value) VALUES
        ('demographics', 'age', '40'),
        ('demographics', 'biological_sex', 'male'),
        ('demographics', 'height_m', '1.9'),
        ('baselines', 'sleep_score_baseline', '86'),
        ('baselines', 'resting_hr_baseline', '49 bpm')
    """
    )

    con.execute(
        """
        CREATE TABLE agent.daily_summaries (
            day DATE, sleep_score INTEGER, readiness_score INTEGER,
            steps INTEGER, resting_hr DOUBLE, stress_level VARCHAR,
            has_anomaly BOOLEAN, anomaly_metrics VARCHAR,
            summary_text VARCHAR, embedding FLOAT[384],
            data_completeness DOUBLE, created_at TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO agent.daily_summaries (day, summary_text, sleep_score, readiness_score, steps) VALUES
        ('2026-02-20', 'Good sleep with score 82. Active day with 12450 steps and a morning run.', 82, 79, 12450),
        ('2026-02-21', 'Poor sleep at 75. Low activity with 5200 steps. Stress elevated.', 75, 72, 5200),
        ('2026-02-22', 'Excellent sleep score 91. Moderate activity with evening cycling.', 91, 88, 9800)
    """
    )

    con.execute(
        """
        CREATE TABLE agent.knowledge_base (
            insight_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            insight_type VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            content VARCHAR NOT NULL,
            evidence_query VARCHAR,
            confidence DOUBLE NOT NULL,
            tags VARCHAR[],
            embedding FLOAT[384],
            is_active BOOLEAN DEFAULT true,
            superseded_by VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO agent.knowledge_base
            (insight_id, insight_type, title, content, confidence, tags, is_active)
        VALUES
            ('test-001', 'pattern', 'Sleep consistency', 'Sleep scores are stable around 80-85', 0.8, ['sleep', 'baseline'], true),
            ('test-002', 'correlation', 'Sleep-readiness link', 'Sleep score predicts next-day readiness', 0.9, ['sleep', 'readiness'], true)
    """
    )

    yield con
    con.close()


@pytest.fixture
def tools(smoke_db):
    """HealthTools instance backed by in-memory DuckDB."""
    return HealthTools(smoke_db)


# ===================================================================
# Tool 1: query_health — core metric queries via semantic contracts
# ===================================================================


class TestQueryHealthSmoke:
    def test_sleep_score_daily(self, tools):
        result = tools.query_health("sleep_score", "2026-02-20:2026-03-06")
        assert "sleep_score" in result
        assert "82" in result
        assert "91" in result

    def test_sleep_score_period_average(self, tools):
        result = tools.query_health(
            "sleep_score", "2026-02-20:2026-03-06", "period_average"
        )
        assert "avg_sleep_score" in result

    def test_sleep_score_trend(self, tools):
        result = tools.query_health("sleep_score", "2026-02-20:2026-03-06", "trend")
        assert "rolling_7d" in result

    def test_sleep_score_distribution(self, tools):
        result = tools.query_health(
            "sleep_score", "2026-02-20:2026-03-06", "distribution"
        )
        assert "excellent" in result or "good" in result or "fair" in result

    def test_readiness_score(self, tools):
        result = tools.query_health("readiness_score", "2026-02-20:2026-03-06")
        assert "readiness_score" in result
        assert "88" in result

    def test_activity_score(self, tools):
        result = tools.query_health("activity_score", "2026-02-20:2026-02-28")
        assert "activity_score" in result
        assert "91" in result

    def test_steps(self, tools):
        result = tools.query_health("steps", "2026-02-20:2026-02-28")
        assert "12450" in result

    def test_blood_oxygen(self, tools):
        result = tools.query_health("blood_oxygen", "2026-02-20:2026-02-22")
        assert "97.5" in result or "98.1" in result

    def test_daily_stress(self, tools):
        result = tools.query_health("daily_stress", "2026-02-20:2026-02-22")
        assert "restored" in result

    def test_workout(self, tools):
        result = tools.query_health("workout", "2026-02-20:2026-02-28")
        assert "running" in result
        assert "cycling" in result

    def test_invalid_metric_returns_error(self, tools):
        result = tools.query_health("nonexistent_metric", "last_7_days")
        assert "Error" in result
        assert "Unknown metric" in result

    def test_invalid_computation_returns_error(self, tools):
        result = tools.query_health("sleep_score", "last_7_days", "nonexistent_comp")
        assert "Error" in result
        assert "Unknown computation" in result

    def test_no_data_range(self, tools):
        result = tools.query_health("sleep_score", "2099-01-01:2099-01-07")
        assert "No data found" in result


# ===================================================================
# Tool 2: search_memory — text search over agent memory
# ===================================================================


class TestSearchMemorySmoke:
    def test_finds_daily_summaries(self, tools):
        result = tools.search_memory("sleep quality")
        assert "Daily Summaries" in result

    def test_finds_knowledge_base(self, tools):
        result = tools.search_memory("readiness")
        assert "Knowledge Base" in result

    def test_no_results_for_nonsense(self, tools):
        result = tools.search_memory("xyznonexistent")
        assert "No results" in result

    def test_finds_running_in_summaries(self, tools):
        result = tools.search_memory("morning run")
        assert "Daily Summaries" in result


# ===================================================================
# Tool 3: get_profile — patient core memory
# ===================================================================


class TestGetProfileSmoke:
    def test_returns_all_categories(self, tools):
        result = tools.get_profile()
        assert "Patient Profile" in result
        assert "demographics" in result
        assert "baselines" in result

    def test_filter_demographics(self, tools):
        result = tools.get_profile(["demographics"])
        assert "male" in result
        assert "40" in result

    def test_filter_baselines(self, tools):
        result = tools.get_profile(["baselines"])
        assert "86" in result

    def test_empty_category(self, tools):
        result = tools.get_profile(["nonexistent_category"])
        assert "No profile data" in result


# ===================================================================
# Tool 4: discover_correlations — lag correlation analysis
# ===================================================================


class TestDiscoverCorrelationsSmoke:
    def test_sleep_vs_readiness(self, tools):
        result = tools.discover_correlations(
            "daily_sleep.sleep_score", "daily_readiness.readiness_score", 2
        )
        assert "Correlation" in result
        assert "Lag (days)" in result
        assert "Strongest correlation" in result

    def test_invalid_metrics_returns_error(self, tools):
        result = tools.discover_correlations("fake_table.fake_col", "also_fake.col")
        assert "Error" in result or "Could not compute" in result


# ===================================================================
# Tool 5: get_metric_definition — YAML contract lookup
# ===================================================================


class TestGetMetricDefinitionSmoke:
    def test_sleep_score_definition(self, tools):
        result = tools.get_metric_definition("sleep_score")
        assert "Sleep Score" in result
        assert "Computations" in result

    def test_has_thresholds(self, tools):
        result = tools.get_metric_definition("sleep_score")
        assert "Threshold" in result

    def test_has_examples(self, tools):
        result = tools.get_metric_definition("sleep_score")
        assert "Example" in result

    def test_invalid_metric_returns_error(self, tools):
        result = tools.get_metric_definition("nonexistent")
        assert "Error" in result


# ===================================================================
# Tool 6: record_insight — write to knowledge base
# ===================================================================


class TestRecordInsightSmoke:
    def test_record_pattern(self, tools):
        result = tools.record_insight(
            "Test insight", "Automated smoke test finding", "pattern", 0.8, ["test"]
        )
        assert "recorded successfully" in result
        assert "Test insight" in result

    def test_record_anomaly(self, tools):
        result = tools.record_insight("Anomaly found", "Details here", "anomaly", 0.9)
        assert "recorded successfully" in result

    def test_invalid_type(self, tools):
        result = tools.record_insight("Bad", "Content", "invalid_type", 0.5)
        assert "Error" in result

    def test_invalid_confidence_high(self, tools):
        result = tools.record_insight("Bad", "Content", "pattern", 1.5)
        assert "Error" in result

    def test_invalid_confidence_negative(self, tools):
        result = tools.record_insight("Bad", "Content", "pattern", -0.1)
        assert "Error" in result

    def test_persists_to_db(self, smoke_db):
        tools = HealthTools(smoke_db)
        tools.record_insight(
            "Persist test", "Should be in DB", "pattern", 0.7, ["persist"]
        )
        row = smoke_db.execute(
            "SELECT title FROM agent.knowledge_base WHERE title = 'Persist test'"
        ).fetchone()
        assert row is not None
        assert row[0] == "Persist test"


# ===================================================================
# Tool 7: get_schema_context — schema pruning by category
# ===================================================================


class TestGetSchemaContextSmoke:
    def test_sleep_category(self, tools):
        result = tools.get_schema_context("sleep")
        assert "daily_sleep" in result
        assert "daily_value" in result

    def test_activity_category(self, tools):
        result = tools.get_schema_context("activity")
        assert "daily_activity" in result or "workout" in result

    def test_vitals_category(self, tools):
        result = tools.get_schema_context("vitals")
        assert "heart_rate" in result or "spo2" in result

    def test_recovery_category(self, tools):
        result = tools.get_schema_context("recovery")
        assert "daily_readiness" in result or "daily_stress" in result

    def test_nutrition_category(self, tools):
        result = tools.get_schema_context("nutrition")
        assert "daily_meal" in result or "water_intake" in result

    def test_invalid_category_returns_error(self, tools):
        result = tools.get_schema_context("nonexistent")
        assert "Error" in result or "Unknown" in result


# ===================================================================
# Tool 8: run_custom_query — read-only escape hatch
# ===================================================================


class TestRunCustomQuerySmoke:
    def test_valid_select(self, tools):
        result = tools.run_custom_query(
            "SELECT COUNT(*) AS n FROM silver.daily_sleep", "smoke test"
        )
        assert "15" in result  # 15 rows seeded

    def test_aggregation(self, tools):
        result = tools.run_custom_query(
            "SELECT ROUND(AVG(sleep_score), 1) AS avg FROM silver.daily_sleep",
            "average check",
        )
        assert "Error" not in result

    def test_blocks_delete(self, tools):
        result = tools.run_custom_query("DELETE FROM silver.daily_sleep", "should fail")
        assert "Error" in result

    def test_blocks_drop(self, tools):
        result = tools.run_custom_query("DROP TABLE silver.daily_sleep", "should fail")
        assert "Error" in result

    def test_blocks_insert(self, tools):
        result = tools.run_custom_query(
            "INSERT INTO silver.daily_sleep VALUES (1, '2026-01-01', 80, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            "should fail",
        )
        assert "Error" in result

    def test_empty_result(self, tools):
        result = tools.run_custom_query(
            "SELECT * FROM silver.daily_sleep WHERE sleep_score > 999",
            "no results expected",
        )
        assert "no results" in result.lower()


# ===================================================================
# 10 Standard Health Questions — end-to-end acceptance
# ===================================================================


class TestStandardHealthQuestions:
    """10 standard health questions the AI agent should be able to answer."""

    def test_q1_how_did_i_sleep(self, tools):
        """Q1: How did I sleep last night?"""
        result = tools.query_health("sleep_score", "2026-03-05")
        assert "86" in result

    def test_q2_average_sleep_this_month(self, tools):
        """Q2: What is my average sleep score this month?"""
        result = tools.query_health(
            "sleep_score", "2026-03-01:2026-03-06", "period_average"
        )
        assert "avg_sleep_score" in result

    def test_q3_readiness_today(self, tools):
        """Q3: How is my readiness today?"""
        result = tools.query_health("readiness_score", "2026-03-06")
        assert "76" in result

    def test_q4_step_count_this_week(self, tools):
        """Q4: How many steps this week?"""
        result = tools.query_health("steps", "2026-02-24:2026-02-28")
        assert "steps" in result or "day" in result

    def test_q5_sleep_trend(self, tools):
        """Q5: Is my sleep getting better or worse?"""
        result = tools.query_health("sleep_score", "2026-02-20:2026-03-06", "trend")
        assert "rolling_7d" in result

    def test_q6_stress_levels(self, tools):
        """Q6: What are my stress levels?"""
        result = tools.query_health("daily_stress", "2026-02-20:2026-03-01")
        assert "restored" in result or "stressed" in result or "normal" in result

    def test_q7_workout_summary(self, tools):
        """Q7: How many workouts did I do?"""
        result = tools.query_health("workout", "2026-02-20:2026-02-28")
        assert "running" in result

    def test_q8_sleep_readiness_correlation(self, tools):
        """Q8: Does my sleep affect my readiness?"""
        result = tools.discover_correlations(
            "daily_sleep.sleep_score", "daily_readiness.readiness_score", 1
        )
        assert "Correlation" in result

    def test_q9_who_am_i(self, tools):
        """Q9: What do you know about me?"""
        result = tools.get_profile()
        assert "Patient Profile" in result
        assert "male" in result

    def test_q10_remember_sleep_pattern(self, tools):
        """Q10: Remember that my sleep is best on weekends."""
        result = tools.record_insight(
            "Weekend sleep pattern",
            "User reports best sleep quality on weekends (Friday-Sunday)",
            "pattern",
            0.6,
            ["sleep", "weekend"],
        )
        assert "recorded successfully" in result
