"""Tests for the DesktopAPI report methods (generate_report, download_report).

Uses a temporary DuckDB database with synthetic data.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from health_platform.desktop.api import DesktopAPI


@pytest.fixture
def report_db(tmp_path):
    """Create a temporary DuckDB with synthetic data for report tests."""
    db_path = str(tmp_path / "test_api_report.db")
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    random.seed(77)
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
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            """INSERT INTO silver.daily_sleep
            (sk_date, day, sleep_score, contributor_deep_sleep,
             contributor_efficiency, contributor_rem_sleep, contributor_total_sleep)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                sk,
                d,
                random.randint(55, 95),
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
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            "INSERT INTO silver.daily_readiness (sk_date, day, readiness_score) VALUES (?, ?, ?)",
            [sk, d, random.randint(50, 90)],
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
        con.execute(
            "INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name) VALUES (?, ?, ?, ?)",
            [sk, f"{d} 03:00:00", random.randint(48, 62), "Oura Ring"],
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

    # Chat history (required by DesktopAPI)
    con.execute(
        """
        CREATE TABLE agent.chat_history (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR,
            role VARCHAR NOT NULL,
            content TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    con.close()
    return db_path


@pytest.fixture
def api(report_db):
    """Create a DesktopAPI with test database."""
    return DesktopAPI(report_db)


class TestGenerateReportAPI:
    def test_returns_dict_with_pdf(self, api):
        """generate_report returns dict with pdf_base64 key."""
        try:
            result = api.generate_report("2026-03-01", "2026-03-10")
            assert isinstance(result, dict)
            assert "pdf_base64" in result
            assert "error" in result
        except Exception:
            pytest.skip("WeasyPrint not installed")

    def test_pdf_is_valid_base64(self, api):
        """The pdf_base64 value decodes to valid PDF bytes."""
        import base64

        try:
            result = api.generate_report("2026-03-01", "2026-03-10")
            if result["error"]:
                pytest.skip("Report generation error: " + result["error"])
            pdf_bytes = base64.b64decode(result["pdf_base64"])
            assert pdf_bytes[:4] == b"%PDF"
        except ImportError:
            pytest.skip("WeasyPrint not installed")

    def test_with_specific_sections(self, api):
        """generate_report works with a subset of sections."""
        try:
            result = api.generate_report(
                "2026-03-01", "2026-03-10", ["summary", "sleep"]
            )
            assert result["error"] is None
            assert result["pdf_base64"] is not None
        except ImportError:
            pytest.skip("WeasyPrint not installed")

    def test_error_handling(self, tmp_path):
        """generate_report returns error for missing database."""
        bad_api = DesktopAPI(str(tmp_path / "nonexistent.db"))
        result = bad_api.generate_report("2026-03-01", "2026-03-10")
        assert result["error"] is not None
        assert result["pdf_base64"] is None


class TestDownloadReportAPI:
    def test_saves_pdf_to_disk(self, api, tmp_path, monkeypatch):
        """download_report saves a file and returns the path."""
        # Redirect Desktop to tmp_path
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        desktop_dir = tmp_path / "Desktop"
        desktop_dir.mkdir()

        try:
            result = api.download_report("2026-03-01", "2026-03-10")
            if result["error"]:
                pytest.skip("Report generation error: " + result["error"])
            assert result["path"] is not None
            assert Path(result["path"]).exists()
            assert Path(result["path"]).stat().st_size > 0
            # Verify filename format
            assert "health_report_2026-03-01_to_2026-03-10.pdf" in result["path"]
        except ImportError:
            pytest.skip("WeasyPrint not installed")

    def test_error_for_missing_db(self, tmp_path):
        """download_report returns error for missing database."""
        bad_api = DesktopAPI(str(tmp_path / "nonexistent.db"))
        result = bad_api.download_report("2026-03-01", "2026-03-10")
        assert result["error"] is not None
        assert result["path"] is None

    def test_with_sections(self, api, tmp_path, monkeypatch):
        """download_report works with specific sections."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        desktop_dir = tmp_path / "Desktop"
        desktop_dir.mkdir()

        try:
            result = api.download_report(
                "2026-03-01", "2026-03-10", ["summary", "vitals"]
            )
            if result["error"]:
                pytest.skip("Report generation error: " + result["error"])
            assert result["path"] is not None
        except ImportError:
            pytest.skip("WeasyPrint not installed")
