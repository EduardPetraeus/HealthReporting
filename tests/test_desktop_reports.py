"""Tests for the ReportGenerator class.

Uses a temporary DuckDB database with synthetic data.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import duckdb
import pytest

from health_platform.desktop.reports.generator import (
    VALID_SECTIONS,
    ReportGenerator,
    _to_float,
)


@pytest.fixture
def report_db(tmp_path):
    """Create a temporary DuckDB with synthetic data for reports."""
    db_path = str(tmp_path / "test_report.db")
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # Generate 10 days of data
    random.seed(99)
    days = [(date(2026, 3, 1) + timedelta(days=i)) for i in range(10)]

    # Sleep
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
    for d in days:
        score = random.randint(55, 95)
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            """INSERT INTO silver.daily_sleep
            (sk_date, day, sleep_score, contributor_deep_sleep,
             contributor_efficiency, contributor_rem_sleep, contributor_total_sleep)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                sk,
                d,
                score,
                random.randint(50, 90),
                random.randint(60, 95),
                random.randint(50, 85),
                random.randint(60, 95),
            ],
        )

    # Readiness
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
    for d in days:
        score = random.randint(50, 90)
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            "INSERT INTO silver.daily_readiness (sk_date, day, readiness_score) VALUES (?, ?, ?)",
            [sk, d, score],
        )

    # Activity
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
    for d in days:
        steps = random.randint(4000, 14000)
        score = min(100, max(30, int(steps / 120)))
        active_cal = random.randint(200, 700)
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            """INSERT INTO silver.daily_activity
            (sk_date, day, activity_score, steps, active_calories, total_calories)
            VALUES (?, ?, ?, ?, ?, ?)""",
            [sk, d, score, steps, active_cal, active_cal + 1500],
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
    for d in days:
        sk = int(d.strftime("%Y%m%d"))
        resting = random.randint(48, 65)
        con.execute(
            "INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name) VALUES (?, ?, ?, ?)",
            [sk, f"{d} 03:00:00", resting, "Oura Ring"],
        )
        for hour in [10, 14, 18]:
            bpm = random.randint(62, 90)
            con.execute(
                "INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name) VALUES (?, ?, ?, ?)",
                [sk, f"{d} {hour:02d}:00:00", bpm, "Apple Watch"],
            )

    # Weight
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
    weight = 80.5
    for i, d in enumerate(days):
        if i % 3 == 0:
            sk = int(d.strftime("%Y%m%d"))
            con.execute(
                """INSERT INTO silver.weight (sk_date, datetime, weight_kg, fat_mass_kg, muscle_mass_kg)
                VALUES (?, ?, ?, ?, ?)""",
                [sk, f"{d} 07:00:00", round(weight, 1), 14.5, 38.2],
            )
            weight += random.uniform(-0.2, 0.2)

    con.close()
    return db_path


@pytest.fixture
def generator(report_db):
    """Create a ReportGenerator with test data."""
    gen = ReportGenerator(report_db)
    yield gen
    gen.close()


class TestReportGeneratorInit:
    def test_creates_instance(self, report_db):
        gen = ReportGenerator(report_db)
        assert gen._db_path == report_db
        gen.close()

    def test_connection_lazy(self, report_db):
        gen = ReportGenerator(report_db)
        assert gen._con is None
        gen.close()


class TestValidSections:
    def test_all_sections_listed(self):
        assert "summary" in VALID_SECTIONS
        assert "vitals" in VALID_SECTIONS
        assert "sleep" in VALID_SECTIONS
        assert "activity" in VALID_SECTIONS
        assert "nutrition" in VALID_SECTIONS
        assert "trends" in VALID_SECTIONS
        assert "alerts" in VALID_SECTIONS
        assert len(VALID_SECTIONS) == 7


class TestToFloat:
    def test_none_returns_none(self):
        assert _to_float(None) is None

    def test_int_returns_float(self):
        assert _to_float(42) == 42.0

    def test_string_returns_none(self):
        assert _to_float("abc") is None

    def test_decimal(self):
        from decimal import Decimal

        assert _to_float(Decimal("3.14")) == 3.14


class TestGenerateHTML:
    def test_returns_html_string(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10")
        assert isinstance(html, str)
        assert "<html" in html
        assert "Health Report" in html

    def test_includes_date_range(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10")
        assert "2026-03-01" in html
        assert "2026-03-10" in html

    def test_includes_summary_section(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10", ["summary"])
        assert "Summary" in html

    def test_includes_sleep_section(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10", ["sleep"])
        assert "Sleep" in html

    def test_includes_vitals_section(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10", ["vitals"])
        assert "Vitals" in html

    def test_includes_activity_section(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10", ["activity"])
        assert "Activity" in html

    def test_includes_trends_section(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10", ["trends"])
        assert "Trends" in html

    def test_includes_alerts_section(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10", ["alerts"])
        assert "Alerts" in html

    def test_excludes_unselected_sections(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10", ["summary"])
        # Sleep section header should not appear
        assert "Nights Tracked" not in html

    def test_all_sections_by_default(self, generator):
        html = generator.generate_html("2026-03-01", "2026-03-10")
        assert "Summary" in html
        assert "Vitals" in html
        assert "Sleep" in html
        assert "Activity" in html
        assert "Trends" in html
        assert "Alerts" in html

    def test_invalid_sections_filtered(self, generator):
        html = generator.generate_html(
            "2026-03-01", "2026-03-10", ["summary", "invalid_section"]
        )
        assert "Summary" in html
        assert isinstance(html, str)


class TestBuildContext:
    def test_summary_has_expected_keys(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["summary"])
        summary = ctx["summary"]
        assert "avg_health_score" in summary
        assert "days_with_data" in summary
        assert "avg_sleep" in summary
        assert "avg_readiness" in summary
        assert "avg_activity" in summary
        assert "total_steps" in summary
        assert "avg_resting_hr" in summary

    def test_summary_values_reasonable(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["summary"])
        summary = ctx["summary"]
        assert summary["days_with_data"] > 0
        assert summary["total_steps"] > 0
        if summary["avg_sleep"] is not None:
            assert 0 < summary["avg_sleep"] <= 100
        if summary["avg_resting_hr"] is not None:
            assert 30 < summary["avg_resting_hr"] < 120

    def test_sleep_section(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["sleep"])
        sleep = ctx["sleep"]
        assert "daily" in sleep
        assert "avg_score" in sleep
        assert len(sleep["daily"]) > 0
        assert sleep["avg_score"] is not None

    def test_activity_section(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["activity"])
        activity = ctx["activity"]
        assert "daily" in activity
        assert "avg_steps" in activity
        assert "total_steps" in activity
        assert activity["total_steps"] > 0

    def test_vitals_section(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["vitals"])
        vitals = ctx["vitals"]
        assert "heart_rate_daily" in vitals
        assert "weight_entries" in vitals
        assert len(vitals["heart_rate_daily"]) > 0
        assert len(vitals["weight_entries"]) > 0

    def test_trends_section(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["trends"])
        trends = ctx["trends"]
        assert "days" in trends
        assert "sleep" in trends
        assert "readiness" in trends
        assert len(trends["days"]) > 0

    def test_alerts_section(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["alerts"])
        alerts = ctx["alerts"]
        assert isinstance(alerts, list)

    def test_nutrition_graceful_when_missing(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(con, "2026-03-01", "2026-03-10", ["nutrition"])
        nutrition = ctx["nutrition"]
        assert nutrition["available"] is False
        assert nutrition["daily"] == []

    def test_context_has_metadata(self, generator):
        con = generator._get_connection()
        ctx = generator._build_context(
            con, "2026-03-01", "2026-03-10", list(VALID_SECTIONS)
        )
        assert ctx["start_date"] == "2026-03-01"
        assert ctx["end_date"] == "2026-03-10"
        assert "generated_at" in ctx
        assert ctx["sections"] == list(VALID_SECTIONS)


class TestGenerateReport:
    def test_returns_bytes(self, generator):
        """Test PDF generation returns bytes (requires WeasyPrint)."""
        try:
            pdf = generator.generate_report("2026-03-01", "2026-03-10")
            assert isinstance(pdf, bytes)
            assert len(pdf) > 0
            # PDF magic bytes
            assert pdf[:4] == b"%PDF"
        except ImportError:
            pytest.skip("WeasyPrint not installed")

    def test_single_section(self, generator):
        """Test generating with only one section."""
        try:
            pdf = generator.generate_report("2026-03-01", "2026-03-10", ["summary"])
            assert isinstance(pdf, bytes)
            assert pdf[:4] == b"%PDF"
        except ImportError:
            pytest.skip("WeasyPrint not installed")

    def test_empty_sections_defaults_to_all(self, generator):
        """Empty sections list defaults to all sections."""
        try:
            pdf = generator.generate_report("2026-03-01", "2026-03-10", [])
            assert isinstance(pdf, bytes)
            assert len(pdf) > 0
        except ImportError:
            pytest.skip("WeasyPrint not installed")


class TestClose:
    def test_close_cleans_up(self, report_db):
        gen = ReportGenerator(report_db)
        _ = gen._get_connection()
        assert gen._con is not None
        gen.close()
        assert gen._con is None

    def test_close_idempotent(self, report_db):
        gen = ReportGenerator(report_db)
        gen.close()
        gen.close()  # Should not raise
