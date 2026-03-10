"""Tests for B4 (expanded correlations) and C2 (trend forecaster + recommendations).

All data is synthetic. Tests use seeded_db and memory_db fixtures from conftest.py.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

# ---------------------------------------------------------------------------
# Helper: create the correct metric_relationships table
# ---------------------------------------------------------------------------


def _setup_relationships_table(con):
    """Create silver.metric_relationships with the real schema."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.metric_relationships (
            source_metric VARCHAR NOT NULL,
            target_metric VARCHAR NOT NULL,
            relationship_type VARCHAR NOT NULL,
            strength DOUBLE,
            lag_days INTEGER DEFAULT 0,
            direction VARCHAR,
            evidence_type VARCHAR,
            confidence DOUBLE,
            description VARCHAR,
            last_computed_at TIMESTAMP,
            PRIMARY KEY (source_metric, target_metric, relationship_type, lag_days)
        )
    """
    )


# ---------------------------------------------------------------------------
# Fixture: seeded_db with extended data (30+ days for trend forecasting)
# ---------------------------------------------------------------------------


@pytest.fixture
def extended_db(memory_db):
    """In-memory DuckDB with 30+ days of synthetic data for trends/forecasts."""
    con = memory_db

    # Create daily_sleep with 30 days of data
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
    # Insert 30 days of gradually declining sleep scores
    for i in range(30):
        d = date.today() - timedelta(days=30 - i)
        score = 85 - i * 0.5  # Declining from 85 to ~70
        con.execute(
            "INSERT INTO silver.daily_sleep (day, sleep_score) VALUES (?, ?)",
            [d, int(score)],
        )

    # Create daily_readiness with 30 days
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
    for i in range(30):
        d = date.today() - timedelta(days=30 - i)
        score = 55 + i * 0.3  # Low readiness, from 55 to ~64
        con.execute(
            "INSERT INTO silver.daily_readiness (day, readiness_score) VALUES (?, ?)",
            [d, int(score)],
        )

    # Create daily_activity with 30 days, low steps
    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER,
            timestamp TIMESTAMP, steps INTEGER,
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
    for i in range(30):
        d = date.today() - timedelta(days=30 - i)
        steps = 4500 + i * 50  # Low steps, gradually increasing
        active_cal = 200 + i * 2
        con.execute(
            "INSERT INTO silver.daily_activity "
            "(day, activity_score, steps, active_calories) "
            "VALUES (?, ?, ?, ?)",
            [d, 70 + i % 10, steps, active_cal],
        )

    # Create daily_stress with high stress ratio
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
    for i in range(30):
        d = date.today() - timedelta(days=30 - i)
        stress = 350  # High stress
        recovery = 200  # Low recovery
        con.execute(
            "INSERT INTO silver.daily_stress (day, stress_high, recovery_high) VALUES (?, ?, ?)",
            [d, stress, recovery],
        )

    # Create daily_spo2
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
    for i in range(30):
        d = date.today() - timedelta(days=30 - i)
        con.execute(
            "INSERT INTO silver.daily_spo2 "
            "(day, spo2_avg_pct, breathing_disturbance_index) "
            "VALUES (?, ?, ?)",
            [d, 97.0 + (i % 3) * 0.5, 0.5 + (i % 5) * 0.2],
        )

    # Create heart_rate
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
    for i in range(30):
        d = date.today() - timedelta(days=30 - i)
        con.execute(
            "INSERT INTO silver.heart_rate (timestamp, bpm) VALUES (?, ?)",
            [d.isoformat() + " 08:00:00", 60 + i % 10],
        )

    # Create weight table with upward trend
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
    for i in range(10):
        d = date.today() - timedelta(days=30 - i * 3)
        weight = 80.0 + i * 0.3  # Increasing weight
        con.execute(
            "INSERT INTO silver.weight (datetime, weight_kg) VALUES (?, ?)",
            [d.isoformat() + " 07:00:00", weight],
        )

    yield con


# ===========================================================================
# B4: Correlation Engine Tests
# ===========================================================================


class TestCorrelationExpansion:
    """Test that the correlation list has been expanded to 30+ pairs."""

    def test_standard_correlations_has_30_plus_pairs(self):
        """Verify _STANDARD_CORRELATIONS contains 30+ pairs."""
        from health_platform.ai.correlation_engine import _STANDARD_CORRELATIONS

        assert (
            len(_STANDARD_CORRELATIONS) >= 30
        ), f"Expected 30+ correlation pairs, got {len(_STANDARD_CORRELATIONS)}"

    def test_original_9_pairs_preserved(self):
        """Original 9 pairs should still be present."""
        from health_platform.ai.correlation_engine import _STANDARD_CORRELATIONS

        original = [
            ("daily_sleep.sleep_score", "daily_readiness.readiness_score", 0),
            ("daily_sleep.sleep_score", "daily_readiness.readiness_score", 1),
            ("daily_sleep.sleep_score", "daily_activity.activity_score", 0),
            ("daily_sleep.sleep_score", "daily_activity.activity_score", 1),
            ("daily_readiness.readiness_score", "daily_activity.activity_score", 0),
            ("daily_sleep.sleep_score", "heart_rate.bpm", 0),
            ("daily_activity.steps", "daily_activity.active_calories", 0),
            ("daily_stress.stress_high", "daily_sleep.sleep_score", 0),
            ("daily_stress.stress_high", "daily_sleep.sleep_score", 1),
        ]
        for pair in original:
            assert pair in _STANDARD_CORRELATIONS, f"Missing original pair: {pair}"

    def test_weight_pairs_included(self):
        """Weight-related correlation pairs should be present."""
        from health_platform.ai.correlation_engine import _STANDARD_CORRELATIONS

        weight_pairs = [
            p for p in _STANDARD_CORRELATIONS if "weight" in p[0] or "weight" in p[1]
        ]
        assert (
            len(weight_pairs) >= 3
        ), f"Expected 3+ weight pairs, got {len(weight_pairs)}"

    def test_spo2_pairs_included(self):
        """SpO2-related correlation pairs should be present."""
        from health_platform.ai.correlation_engine import _STANDARD_CORRELATIONS

        spo2_pairs = [
            p for p in _STANDARD_CORRELATIONS if "spo2" in p[0] or "spo2" in p[1]
        ]
        assert len(spo2_pairs) >= 3, f"Expected 3+ SpO2 pairs, got {len(spo2_pairs)}"

    def test_cross_domain_delayed_pairs_included(self):
        """Cross-domain delayed effect pairs should be present."""
        from health_platform.ai.correlation_engine import _STANDARD_CORRELATIONS

        delayed = [p for p in _STANDARD_CORRELATIONS if p[2] == 1]
        assert len(delayed) >= 10, f"Expected 10+ lag-1 pairs, got {len(delayed)}"

    def test_weight_date_columns_mapping(self):
        """Weight table should use CAST(datetime AS DATE) for date column."""
        from health_platform.ai.correlation_engine import _DATE_COLUMNS

        assert "weight" in _DATE_COLUMNS
        assert "datetime" in _DATE_COLUMNS["weight"].lower()

    def test_compute_correlation_with_synthetic_data(self, seeded_db):
        """Compute correlation returns valid result with synthetic data."""
        from health_platform.ai.correlation_engine import compute_correlation

        result = compute_correlation(
            seeded_db,
            "daily_sleep.sleep_score",
            "daily_readiness.readiness_score",
            lag_days=0,
        )

        assert result["pearson_r"] is not None
        assert -1.0 <= result["pearson_r"] <= 1.0
        assert result["strength"] in ("strong", "moderate", "weak", "negligible")
        assert result["direction"] in ("positive", "negative", "none")
        assert result["sample_size"] >= 2

    def test_compute_all_correlations_stores_results(self, seeded_db):
        """compute_all_correlations stores results in metric_relationships."""
        _setup_relationships_table(seeded_db)

        from health_platform.ai.correlation_engine import compute_all_correlations

        count = compute_all_correlations(seeded_db)
        assert count >= 1

        stored = seeded_db.execute(
            "SELECT COUNT(*) FROM silver.metric_relationships"
        ).fetchone()[0]
        assert stored >= 1

    def test_compute_correlation_missing_table(self, memory_db):
        """Correlation with non-existent table returns error."""
        from health_platform.ai.correlation_engine import compute_correlation

        result = compute_correlation(
            memory_db,
            "nonexistent.col",
            "daily_sleep.sleep_score",
            lag_days=0,
        )

        assert result["pearson_r"] is None
        assert result["strength"] == "error"


# ===========================================================================
# C2: Trend Forecaster Tests
# ===========================================================================


class TestTrendForecaster:
    """Test trend forecasting with linear regression."""

    def test_forecast_with_sufficient_data(self, extended_db):
        """Forecast returns valid result with 30+ days of data."""
        from health_platform.ai.trend_forecaster import TrendForecaster

        forecaster = TrendForecaster(extended_db)
        forecast = forecaster.forecast_metric(
            "silver.daily_sleep",
            "sleep_score",
            lookback_days=90,
            forecast_days=7,
        )

        assert forecast.metric == "silver.daily_sleep.sleep_score"
        assert forecast.current_value > 0
        assert len(forecast.forecast_values) == 7
        assert forecast.trend_direction in ("improving", "declining", "stable")
        assert forecast.method == "linear_regression"
        assert 0.0 <= forecast.confidence <= 1.0

    def test_forecast_with_insufficient_data(self, memory_db):
        """Forecast with too few data points returns empty forecast."""
        from health_platform.ai.trend_forecaster import TrendForecaster

        # Create table with only 3 data points (need 7)
        memory_db.execute(
            """
            CREATE TABLE silver.test_metric (day DATE, value DOUBLE)
        """
        )
        memory_db.execute(
            """
            INSERT INTO silver.test_metric VALUES
                ('2026-03-01', 10.0),
                ('2026-03-02', 11.0),
                ('2026-03-03', 12.0)
        """
        )

        forecaster = TrendForecaster(memory_db)
        forecast = forecaster.forecast_metric(
            "silver.test_metric",
            "value",
            lookback_days=90,
            forecast_days=7,
        )

        assert forecast.trend_direction == "unknown"
        assert forecast.confidence == 0.0
        assert forecast.method == "insufficient_data"
        assert len(forecast.forecast_values) == 0

    def test_trend_direction_declining(self, extended_db):
        """Declining data should be classified as 'declining'."""
        from health_platform.ai.trend_forecaster import TrendForecaster

        forecaster = TrendForecaster(extended_db)
        # extended_db has declining sleep scores (85 -> ~70)
        forecast = forecaster.forecast_metric(
            "silver.daily_sleep",
            "sleep_score",
            lookback_days=90,
            forecast_days=7,
        )

        assert forecast.trend_direction == "declining"

    def test_trend_direction_stable(self, memory_db):
        """Flat data should be classified as 'stable'."""
        from health_platform.ai.trend_forecaster import TrendForecaster

        memory_db.execute("CREATE TABLE silver.stable_metric (day DATE, value DOUBLE)")
        for i in range(15):
            d = date.today() - timedelta(days=15 - i)
            memory_db.execute(
                "INSERT INTO silver.stable_metric VALUES (?, ?)",
                [d, 50.0 + (i % 2) * 0.1],  # Essentially flat
            )

        forecaster = TrendForecaster(memory_db)
        forecast = forecaster.forecast_metric(
            "silver.stable_metric",
            "value",
            lookback_days=90,
            forecast_days=7,
        )

        assert forecast.trend_direction == "stable"

    def test_r_squared_is_valid(self, extended_db):
        """R-squared should be a valid number between 0 and 1."""
        from health_platform.ai.trend_forecaster import TrendForecaster

        forecaster = TrendForecaster(extended_db)
        forecast = forecaster.forecast_metric(
            "silver.daily_sleep",
            "sleep_score",
            lookback_days=90,
            forecast_days=7,
        )

        assert 0.0 <= forecast.r_squared <= 1.0

    def test_forecast_values_have_future_dates(self, extended_db):
        """Forecast dates should be in the future."""
        from health_platform.ai.trend_forecaster import TrendForecaster

        forecaster = TrendForecaster(extended_db)
        forecast = forecaster.forecast_metric(
            "silver.daily_sleep",
            "sleep_score",
            lookback_days=90,
            forecast_days=7,
        )

        today = date.today()
        for forecast_date, value in forecast.forecast_values:
            assert forecast_date > today

    def test_format_forecast_produces_markdown(self, extended_db):
        """format_forecast should produce valid markdown."""
        from health_platform.ai.trend_forecaster import TrendForecaster, format_forecast

        forecaster = TrendForecaster(extended_db)
        forecast = forecaster.forecast_metric(
            "silver.daily_sleep",
            "sleep_score",
            lookback_days=90,
            forecast_days=7,
        )

        md = format_forecast(forecast)
        assert "## Forecast:" in md
        assert "**Direction:**" in md
        assert "**Current:**" in md
        assert "| Date | Predicted |" in md

    def test_format_forecast_empty(self):
        """format_forecast with empty forecast still produces valid markdown."""
        from health_platform.ai.trend_forecaster import Forecast, format_forecast

        empty = Forecast(
            metric="test",
            current_value=0.0,
            forecast_values=[],
            trend_direction="unknown",
            confidence=0.0,
            method="insufficient_data",
            r_squared=0.0,
            description="test: insufficient data",
        )
        md = format_forecast(empty)
        assert "## Forecast:" in md
        assert "unknown" in md

    def test_forecast_nonexistent_table(self, memory_db):
        """Forecasting a nonexistent table returns empty forecast."""
        from health_platform.ai.trend_forecaster import TrendForecaster

        forecaster = TrendForecaster(memory_db)
        forecast = forecaster.forecast_metric(
            "silver.does_not_exist",
            "value",
            lookback_days=90,
            forecast_days=7,
        )

        assert forecast.trend_direction == "unknown"
        assert forecast.method == "insufficient_data"


# ===========================================================================
# C2: Recommendation Engine Tests
# ===========================================================================


class TestRecommendationEngine:
    """Test personalized health recommendations."""

    def test_sleep_declining_triggers_recommendation(self, extended_db):
        """Declining sleep score should trigger a high-priority recommendation."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=20)

        sleep_recs = [
            r for r in recs if r.category == "sleep" and "declining" in r.title.lower()
        ]
        assert len(sleep_recs) >= 1
        assert sleep_recs[0].priority == "high"

    def test_low_steps_triggers_recommendation(self, extended_db):
        """Low step count should trigger an activity recommendation."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=20)

        activity_recs = [
            r
            for r in recs
            if r.category == "activity" and "movement" in r.title.lower()
        ]
        assert len(activity_recs) >= 1

    def test_recovery_low_triggers_recommendation(self, extended_db):
        """Low readiness should trigger a recovery recommendation."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=20)

        recovery_recs = [r for r in recs if r.category == "recovery"]
        assert len(recovery_recs) >= 1
        assert recovery_recs[0].priority == "high"

    def test_stress_imbalance_triggers_recommendation(self, extended_db):
        """High stress/recovery ratio should trigger a stress recommendation."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=20)

        stress_recs = [r for r in recs if r.category == "stress"]
        assert len(stress_recs) >= 1
        assert stress_recs[0].priority == "high"

    def test_weight_trend_triggers_recommendation(self, extended_db):
        """Upward weight trend should trigger a nutrition recommendation."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=20)

        weight_recs = [r for r in recs if r.category == "nutrition"]
        assert len(weight_recs) >= 1
        assert weight_recs[0].priority == "low"

    def test_recommendations_sorted_by_priority(self, extended_db):
        """Recommendations should be sorted: high > medium > low."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=20)

        if len(recs) >= 2:
            priority_order = {"high": 0, "medium": 1, "low": 2}
            for i in range(len(recs) - 1):
                assert priority_order.get(recs[i].priority, 3) <= priority_order.get(
                    recs[i + 1].priority, 3
                ), f"Priority order violated: {recs[i].priority} before {recs[i + 1].priority}"

    def test_max_results_limit(self, extended_db):
        """max_results should cap the number of recommendations."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=2)

        assert len(recs) <= 2

    def test_empty_database_returns_no_recommendations(self, memory_db):
        """Empty database should return no recommendations."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        # Create minimal empty tables
        memory_db.execute(
            """
            CREATE TABLE silver.daily_sleep (day DATE, sleep_score INTEGER)
        """
        )
        memory_db.execute(
            """
            CREATE TABLE silver.daily_activity (
                day DATE, steps INTEGER,
                active_calories INTEGER, activity_score INTEGER
            )
        """
        )
        memory_db.execute(
            """
            CREATE TABLE silver.daily_readiness (day DATE, readiness_score INTEGER)
        """
        )
        memory_db.execute(
            """
            CREATE TABLE silver.daily_stress (day DATE, stress_high INTEGER, recovery_high INTEGER)
        """
        )
        memory_db.execute(
            """
            CREATE TABLE silver.weight (datetime TIMESTAMP, weight_kg DOUBLE)
        """
        )

        engine = RecommendationEngine(memory_db)
        recs = engine.get_recommendations()

        assert len(recs) == 0

    def test_format_recommendations_produces_markdown(self, extended_db):
        """format_recommendations should produce valid markdown."""
        from health_platform.ai.recommendation_engine import (
            RecommendationEngine,
            format_recommendations,
        )

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=5)
        md = format_recommendations(recs)

        assert "# Health Recommendations" in md
        assert "**Category:**" in md
        assert "**Priority:**" in md

    def test_format_recommendations_empty(self):
        """format_recommendations with empty list returns healthy message."""
        from health_platform.ai.recommendation_engine import format_recommendations

        md = format_recommendations([])
        assert "All metrics look healthy" in md

    def test_recommendation_has_required_fields(self, extended_db):
        """Each recommendation should have all required fields populated."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=10)

        for r in recs:
            assert r.title, "Recommendation missing title"
            assert r.category, "Recommendation missing category"
            assert r.priority in (
                "high",
                "medium",
                "low",
            ), f"Invalid priority: {r.priority}"
            assert r.recommendation, "Recommendation missing recommendation text"
            assert r.rationale, "Recommendation missing rationale"
            assert r.evidence_type, "Recommendation missing evidence_type"
            assert len(r.metrics_used) >= 1, "Recommendation missing metrics_used"


# ===========================================================================
# MCP Tool Integration Tests
# ===========================================================================


class TestMCPToolIntegration:
    """Test MCP tool functions that wrap the engines."""

    def test_forecast_metric_tool_invalid_format(self):
        """forecast_metric with invalid metric format returns error."""
        # Import the tool directly (it uses get_tools internally,
        # so we test the validation path only)
        from health_platform.mcp.server import forecast_metric

        result = forecast_metric("invalid_no_dot")
        assert "Error" in result

    def test_health_tools_forecast(self, extended_db):
        """HealthTools.forecast_metric returns valid markdown."""
        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(extended_db)
        result = tools.forecast_metric(
            "daily_sleep",
            "sleep_score",
            lookback_days=90,
            forecast_days=7,
        )

        assert "## Forecast:" in result
        assert "**Direction:**" in result

    def test_health_tools_get_recommendations(self, extended_db):
        """HealthTools.get_recommendations returns valid markdown."""
        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(extended_db)
        result = tools.get_recommendations(max_results=5)

        assert "Health Recommendations" in result

    def test_health_tools_explain_recommendation(self, extended_db):
        """HealthTools.explain_recommendation returns detail for a known recommendation."""
        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(extended_db)

        # First get recommendations to know a title
        from health_platform.ai.recommendation_engine import RecommendationEngine

        engine = RecommendationEngine(extended_db)
        recs = engine.get_recommendations(max_results=1)

        if recs:
            result = tools.explain_recommendation(recs[0].title)
            assert "## Recommendation" in result
            assert "## Data Rationale" in result
            assert "## Evidence Type" in result

    def test_health_tools_explain_recommendation_not_found(self, extended_db):
        """explain_recommendation with unknown title returns not-found message."""
        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(extended_db)
        result = tools.explain_recommendation("Nonexistent recommendation xyz")

        assert "not found" in result.lower()

    def test_health_tools_get_cross_source_insights(self, seeded_db):
        """HealthTools.get_cross_source_insights computes correlations."""
        _setup_relationships_table(seeded_db)

        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(seeded_db)
        result = tools.get_cross_source_insights()

        assert "correlations" in result.lower()
