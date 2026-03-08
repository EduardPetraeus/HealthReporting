"""Tests for the DesktopAPI class.

Uses an in-memory DuckDB database with synthetic data.
"""

from __future__ import annotations

import pytest

from health_platform.desktop.api import DesktopAPI, _sanitize


@pytest.fixture
def dev_db(tmp_path):
    """Create a temporary DuckDB with synthetic data and return path."""
    import duckdb

    db_path = str(tmp_path / "test_health.db")
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    # Sleep data
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
        INSERT INTO silver.daily_sleep (sk_date, day, sleep_score)
        VALUES
            (20260301, '2026-03-01', 82),
            (20260302, '2026-03-02', 75),
            (20260303, '2026-03-03', 91),
            (20260304, '2026-03-04', 58),
            (20260305, '2026-03-05', 88)
    """
    )

    # Readiness data
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
        INSERT INTO silver.daily_readiness (sk_date, day, readiness_score)
        VALUES
            (20260301, '2026-03-01', 79),
            (20260302, '2026-03-02', 72),
            (20260303, '2026-03-03', 88),
            (20260304, '2026-03-04', 55),
            (20260305, '2026-03-05', 85)
    """
    )

    # Activity data
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
    con.execute(
        """
        INSERT INTO silver.daily_activity (sk_date, day, activity_score, steps, active_calories, total_calories)
        VALUES
            (20260301, '2026-03-01', 91, 12450, 650, 2400),
            (20260302, '2026-03-02', 68, 5200, 320, 2100),
            (20260303, '2026-03-03', 85, 9800, 520, 2300),
            (20260304, '2026-03-04', 45, 3100, 180, 1900),
            (20260305, '2026-03-05', 78, 8500, 450, 2200)
    """
    )

    # Heart rate
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
        INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name)
        VALUES
            (20260301, '2026-03-01 03:00:00', 52, 'Oura Ring'),
            (20260301, '2026-03-01 12:00:00', 78, 'Apple Watch'),
            (20260302, '2026-03-02 03:00:00', 55, 'Oura Ring'),
            (20260303, '2026-03-03 03:00:00', 50, 'Oura Ring'),
            (20260304, '2026-03-04 03:00:00', 72, 'Oura Ring'),
            (20260305, '2026-03-05 03:00:00', 51, 'Oura Ring')
    """
    )

    con.close()
    return db_path


@pytest.fixture
def api(dev_db):
    """Create a DesktopAPI instance with test database."""
    return DesktopAPI(dev_db)


class TestSanitize:
    def test_decimal_converted(self):
        from decimal import Decimal

        assert _sanitize(Decimal("42.5")) == 42.5

    def test_date_converted(self):
        from datetime import date

        assert _sanitize(date(2026, 3, 1)) == "2026-03-01"

    def test_nested_dict(self):
        from decimal import Decimal

        result = _sanitize({"score": Decimal("85.3"), "name": "test"})
        assert result == {"score": 85.3, "name": "test"}

    def test_list_sanitized(self):
        from decimal import Decimal

        result = _sanitize([Decimal("1.0"), Decimal("2.0")])
        assert result == [1.0, 2.0]

    def test_passthrough(self):
        assert _sanitize(42) == 42
        assert _sanitize("hello") == "hello"
        assert _sanitize(None) is None


class TestDashboardData:
    def test_returns_all_keys(self, api):
        data = api.get_dashboard_data()
        assert "health_score" in data
        assert "kpis" in data
        assert "sparklines" in data
        assert "trends" in data
        assert "alerts" in data
        assert "timestamp" in data

    def test_health_score(self, api):
        hs = api.get_dashboard_data()["health_score"]
        assert hs["score"] is not None
        assert hs["status"] in ("excellent", "good", "fair", "poor")
        assert hs["day"] is not None
        assert "sleep" in hs["components"]
        assert "readiness" in hs["components"]
        assert "activity" in hs["components"]

    def test_health_score_computation(self, api):
        hs = api.get_dashboard_data()["health_score"]
        comp = hs["components"]
        expected = round(
            (comp["sleep"] or 0) * 0.35
            + (comp["readiness"] or 0) * 0.35
            + (comp["activity"] or 0) * 0.30,
            1,
        )
        assert abs(hs["score"] - expected) < 0.2

    def test_kpis(self, api):
        kpis = api.get_dashboard_data()["kpis"]
        assert "sleep_score" in kpis
        assert "readiness_score" in kpis
        assert "steps" in kpis
        assert "resting_hr" in kpis

        for key, kpi in kpis.items():
            assert "label" in kpi
            assert "value" in kpi
            assert "day" in kpi
            assert "unit" in kpi

    def test_kpi_values(self, api):
        kpis = api.get_dashboard_data()["kpis"]
        assert kpis["sleep_score"]["value"] == 88  # Latest (2026-03-05)
        assert kpis["readiness_score"]["value"] == 85
        assert kpis["steps"]["value"] == 8500
        assert kpis["resting_hr"]["value"] == 51

    def test_sparklines(self, api):
        sparklines = api.get_dashboard_data()["sparklines"]
        assert "sleep_score" in sparklines
        assert "readiness_score" in sparklines
        assert "steps" in sparklines
        assert "resting_hr" in sparklines

        for key, series in sparklines.items():
            assert isinstance(series, list)
            for point in series:
                assert "day" in point
                assert "value" in point

    def test_trends(self, api):
        trends = api.get_dashboard_data()["trends"]
        assert "days" in trends
        assert "sleep" in trends
        assert "readiness" in trends
        assert len(trends["days"]) == len(trends["sleep"])
        assert len(trends["days"]) == len(trends["readiness"])

    def test_alerts_low_scores(self, api):
        alerts = api.get_dashboard_data()["alerts"]
        assert isinstance(alerts, list)
        # We have sleep_score=58 and readiness=55, so alerts should fire
        alert_metrics = [a["metric"] for a in alerts]
        assert "sleep_score" in alert_metrics
        assert "readiness_score" in alert_metrics

    def test_alerts_elevated_hr(self, api):
        alerts = api.get_dashboard_data()["alerts"]
        hr_alerts = [a for a in alerts if a["metric"] == "resting_hr"]
        assert len(hr_alerts) == 1
        assert "72" in hr_alerts[0]["message"]


class TestNoDatabase:
    def test_missing_db(self, tmp_path):
        api = DesktopAPI(str(tmp_path / "nonexistent.db"))
        data = api.get_dashboard_data()
        assert data["error"] == "no_database"


class TestQueryMetric:
    def test_query_returns_result(self, api):
        result = api.query_metric("sleep_score", "daily_value", "last_7_days")
        assert result["error"] is None
        assert result["result"] is not None


class TestListMetrics:
    def test_returns_metrics(self, api):
        metrics = api.list_metrics()
        assert isinstance(metrics, list)
        assert "sleep_score" in metrics
        assert "readiness_score" in metrics
        assert "steps" in metrics


class TestGetStatus:
    def test_status_with_db(self, api):
        status = api.get_status()
        assert status["db_exists"] is True
        assert status["db_size_mb"] > 0

    def test_status_without_db(self, tmp_path):
        api = DesktopAPI(str(tmp_path / "nonexistent.db"))
        status = api.get_status()
        assert status["db_exists"] is False
        assert status["db_size_mb"] == 0
