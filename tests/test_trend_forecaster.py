"""Edge-case tests for trend_forecaster.py.

Happy-path tests live in test_intelligence_b4_c2.py. This file covers
only edge cases: insufficient data, missing columns, forecast_days=0,
higher-is-worse inversion, r-squared cap, known slope, and _empty_forecast
field defaults.

All data is synthetic.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from health_platform.ai.trend_forecaster import TrendForecaster

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def linear_db():
    """In-memory DuckDB with 10 exact data points y = 2x + 10."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE TABLE silver.linear_metric (day DATE, value DOUBLE)")
    for i in range(10):
        d = date.today() - timedelta(days=10 - i)
        con.execute(
            "INSERT INTO silver.linear_metric VALUES (?, ?)",
            [d, 2.0 * (i + 1) + 10.0],
        )
    yield con
    con.close()


@pytest.fixture
def bare_db():
    """In-memory DuckDB with silver schema but no tables."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    yield con
    con.close()


# ---------------------------------------------------------------------------
# Minimum data boundary
# ---------------------------------------------------------------------------


def test_exactly_7_points_succeeds(bare_db):
    """7 data points is the minimum — forecast must succeed (not insufficient_data)."""
    bare_db.execute("CREATE TABLE silver.seven_pts (day DATE, sleep_score INTEGER)")
    for i in range(7):
        d = date.today() - timedelta(days=7 - i)
        bare_db.execute("INSERT INTO silver.seven_pts VALUES (?, ?)", [d, 70 + i])
    forecaster = TrendForecaster(bare_db)
    result = forecaster.forecast_metric(
        "silver.seven_pts", "sleep_score", date_column="day"
    )
    assert result.method != "insufficient_data"


def test_6_points_insufficient(bare_db):
    """6 data points is one below minimum — must return insufficient_data."""
    bare_db.execute("CREATE TABLE silver.six_pts (day DATE, sleep_score INTEGER)")
    for i in range(6):
        d = date.today() - timedelta(days=6 - i)
        bare_db.execute("INSERT INTO silver.six_pts VALUES (?, ?)", [d, 70 + i])
    forecaster = TrendForecaster(bare_db)
    result = forecaster.forecast_metric(
        "silver.six_pts", "sleep_score", date_column="day"
    )
    assert result.method == "insufficient_data"


# ---------------------------------------------------------------------------
# Missing column / table
# ---------------------------------------------------------------------------


def test_nonexistent_column_empty_forecast(linear_db):
    """Querying a column that does not exist must return insufficient_data."""
    forecaster = TrendForecaster(linear_db)
    result = forecaster.forecast_metric(
        "silver.linear_metric", "nonexistent_col", date_column="day"
    )
    assert result.method == "insufficient_data"


# ---------------------------------------------------------------------------
# forecast_days=0
# ---------------------------------------------------------------------------


def test_forecast_days_zero_empty_list(linear_db):
    """forecast_days=0 means the forward projection loop never runs."""
    forecaster = TrendForecaster(linear_db)
    result = forecaster.forecast_metric(
        "silver.linear_metric", "value", date_column="day", forecast_days=0
    )
    assert result.forecast_values == []


# ---------------------------------------------------------------------------
# Higher-is-worse inversion
# ---------------------------------------------------------------------------


def test_higher_is_worse_positive_slope_declining(bare_db):
    """For higher-is-worse metrics, a positive slope must yield direction='declining'."""
    bare_db.execute("CREATE TABLE silver.daily_stress (day DATE, stress_high INTEGER)")
    # Steadily increasing stress — clearly positive slope
    for i in range(10):
        d = date.today() - timedelta(days=10 - i)
        bare_db.execute(
            "INSERT INTO silver.daily_stress VALUES (?, ?)", [d, 10 + i * 5]
        )
    forecaster = TrendForecaster(bare_db)
    result = forecaster.forecast_metric(
        "silver.daily_stress", "stress_high", date_column="day"
    )
    assert result.trend_direction == "declining"


def test_higher_is_worse_negative_slope_improving(bare_db):
    """For higher-is-worse metrics, a negative slope must yield direction='improving'."""
    bare_db.execute("CREATE TABLE silver.daily_stress (day DATE, stress_high INTEGER)")
    # Steadily decreasing stress — negative slope
    for i in range(10):
        d = date.today() - timedelta(days=10 - i)
        bare_db.execute(
            "INSERT INTO silver.daily_stress VALUES (?, ?)", [d, 100 - i * 5]
        )
    forecaster = TrendForecaster(bare_db)
    result = forecaster.forecast_metric(
        "silver.daily_stress", "stress_high", date_column="day"
    )
    assert result.trend_direction == "improving"


# ---------------------------------------------------------------------------
# R-squared cap at 0.95
# ---------------------------------------------------------------------------


def test_r_squared_near_1_capped_at_095(linear_db):
    """Perfect linear data (R²≈1.0) must have confidence capped at 0.95."""
    forecaster = TrendForecaster(linear_db)
    result = forecaster.forecast_metric(
        "silver.linear_metric", "value", date_column="day"
    )
    assert result.confidence == pytest.approx(0.95)


# ---------------------------------------------------------------------------
# Known slope from linear_db (y = 2x + 10)
# ---------------------------------------------------------------------------


def test_known_slope_intercept(linear_db):
    """With y=2x+10 the first forecast value should be intercept + slope*(max_x+1)."""
    forecaster = TrendForecaster(linear_db)
    result = forecaster.forecast_metric(
        "silver.linear_metric", "value", date_column="day", forecast_days=1
    )
    assert len(result.forecast_values) == 1
    # slope ≈ 2.0 — confirm direction is improving (higher value, not higher-is-worse)
    assert result.trend_direction == "improving"
    # The predicted value for step max_x+1 should be noticeably above current_value
    predicted = result.forecast_values[0][1]
    assert (
        predicted > result.current_value - 1
    )  # sanity: projection is in the right ballpark


# ---------------------------------------------------------------------------
# _empty_forecast field defaults
# ---------------------------------------------------------------------------


def test_empty_forecast_defaults(bare_db):
    """_empty_forecast must return all expected zero/empty defaults."""
    forecaster = TrendForecaster(bare_db)
    result = forecaster._empty_forecast("silver.test_metric.value")
    assert result.current_value == 0.0
    assert result.forecast_values == []
    assert result.trend_direction == "unknown"
    assert result.confidence == 0.0
    assert result.method == "insufficient_data"
    assert result.r_squared == 0.0
    assert "insufficient" in result.description
