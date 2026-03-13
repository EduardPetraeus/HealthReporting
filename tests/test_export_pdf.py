"""Tests for clinical PDF report generation.

All data is synthetic — no real health measurements used.
"""

from __future__ import annotations

import os
import subprocess
from datetime import date
from unittest.mock import patch

import duckdb
import pytest
from health_platform.export.pdf.clinical_report import (
    ALL_SECTIONS,
    ClinicalReportGenerator,
)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _fake_keychain_run(*args, **kwargs):
    """Mock subprocess.run that simulates errSecItemNotFound (code 44)."""
    return subprocess.CompletedProcess(
        args=args[0] if args else [],
        returncode=44,
        stdout="",
        stderr="The specified item could not be found in the keychain.",
    )


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch):
    """Set test environment variables."""
    monkeypatch.setenv("HEALTH_API_TOKEN", "test-token-12345")
    monkeypatch.setenv("HEALTH_ENV", "dev")
    monkeypatch.setattr(
        "health_platform.utils.keychain.subprocess.run",
        _fake_keychain_run,
    )


@pytest.fixture
def pdf_db(tmp_path):
    """Create a temporary DuckDB with test data for PDF report tests."""
    db_file = tmp_path / "test_pdf.db"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    # Patient profile
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
        ('demographics', 'name', 'Test Patient'),
        ('demographics', 'biological_sex', 'male'),
        ('demographics', 'age', '45'),
        ('demographics', 'date_of_birth', '1981-03-15')
    """
    )

    # Daily sleep
    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_sleep (day, sleep_score) VALUES
        ('2026-03-01', 82), ('2026-03-02', 75), ('2026-03-03', 91),
        ('2026-03-04', 68), ('2026-03-05', 85)
    """
    )

    # Daily readiness
    con.execute(
        """
        CREATE TABLE silver.daily_readiness (
            sk_date INTEGER, day DATE, readiness_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_readiness (day, readiness_score) VALUES
        ('2026-03-01', 79), ('2026-03-02', 72), ('2026-03-03', 88),
        ('2026-03-04', 65), ('2026-03-05', 81)
    """
    )

    # Daily activity
    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER, steps INTEGER,
            active_calories INTEGER, total_calories INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_activity (day, activity_score, steps) VALUES
        ('2026-03-01', 91, 12450), ('2026-03-02', 68, 5200),
        ('2026-03-03', 85, 9800), ('2026-03-04', 72, 7300),
        ('2026-03-05', 88, 11200)
    """
    )

    # Daily SpO2
    con.execute(
        """
        CREATE TABLE silver.daily_spo2 (
            sk_date INTEGER, day DATE, spo2_avg_pct DOUBLE,
            breathing_disturbance_index DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_spo2 (day, spo2_avg_pct) VALUES
        ('2026-03-01', 97.5), ('2026-03-02', 96.2), ('2026-03-03', 98.1)
    """
    )

    # Weight
    con.execute(
        """
        CREATE TABLE silver.weight (
            sk_date INTEGER, sk_time VARCHAR, datetime TIMESTAMP,
            weight_kg DOUBLE, fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
            muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.weight (datetime, weight_kg) VALUES
        ('2026-03-01 07:00:00', 82.5), ('2026-03-03 07:00:00', 82.3)
    """
    )

    con.close()

    with patch.dict(os.environ, {"HEALTH_DB_PATH": str(db_file)}):
        yield str(db_file)


@pytest.fixture
def empty_db(tmp_path):
    """Create a temporary DuckDB with empty tables for edge case tests."""
    db_file = tmp_path / "test_empty.db"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

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
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        CREATE TABLE silver.daily_readiness (
            sk_date INTEGER, day DATE, readiness_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER, steps INTEGER,
            active_calories INTEGER, total_calories INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        CREATE TABLE silver.daily_spo2 (
            sk_date INTEGER, day DATE, spo2_avg_pct DOUBLE,
            breathing_disturbance_index DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        CREATE TABLE silver.weight (
            sk_date INTEGER, sk_time VARCHAR, datetime TIMESTAMP,
            weight_kg DOUBLE, fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
            muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )

    con.close()

    with patch.dict(os.environ, {"HEALTH_DB_PATH": str(db_file)}):
        yield str(db_file)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestClinicalReportGeneratesPdf:
    def test_clinical_report_generates_pdf(self, pdf_db):
        """Generate a PDF report and verify it is valid PDF bytes."""
        gen = ClinicalReportGenerator(db_path=pdf_db)
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
        )

        assert isinstance(pdf_bytes, bytes)
        assert len(pdf_bytes) > 0
        # PDF files start with %PDF
        assert pdf_bytes[:5] == b"%PDF-"

    def test_clinical_report_size_reasonable(self, pdf_db):
        """Report should be non-trivial size (has content)."""
        gen = ClinicalReportGenerator(db_path=pdf_db)
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
        )
        # A clinical report with data should be at least a few KB
        assert len(pdf_bytes) > 1000


class TestClinicalReportSections:
    def test_clinical_report_sections(self, pdf_db):
        """Report with specific sections should generate successfully."""
        gen = ClinicalReportGenerator(db_path=pdf_db)

        # Only patient info
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
            sections=["patient_info"],
        )
        assert pdf_bytes[:5] == b"%PDF-"

        # Only vitals
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
            sections=["vitals_summary"],
        )
        assert pdf_bytes[:5] == b"%PDF-"

    def test_clinical_report_all_sections(self, pdf_db):
        """Report with all sections."""
        gen = ClinicalReportGenerator(db_path=pdf_db)
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
            sections=ALL_SECTIONS,
        )
        assert pdf_bytes[:5] == b"%PDF-"
        assert len(pdf_bytes) > 1000

    def test_invalid_sections_fall_back_to_all(self, pdf_db):
        """Invalid section names should fall back to all sections."""
        gen = ClinicalReportGenerator(db_path=pdf_db)
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
            sections=["nonexistent_section"],
        )
        assert pdf_bytes[:5] == b"%PDF-"


class TestClinicalReportWithEmptyData:
    def test_clinical_report_with_empty_data(self, empty_db):
        """Report with no data should still generate a valid PDF."""
        gen = ClinicalReportGenerator(db_path=empty_db)
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
        )

        assert isinstance(pdf_bytes, bytes)
        assert len(pdf_bytes) > 0
        assert pdf_bytes[:5] == b"%PDF-"

    def test_empty_report_has_reasonable_size(self, empty_db):
        """Even an empty report should have some content (headers, disclaimer)."""
        gen = ClinicalReportGenerator(db_path=empty_db)
        pdf_bytes = gen.generate_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 5),
        )
        # Empty report should still have the base template rendered
        assert len(pdf_bytes) > 500

    def test_empty_report_future_dates(self, empty_db):
        """Report for future dates should also work cleanly."""
        gen = ClinicalReportGenerator(db_path=empty_db)
        pdf_bytes = gen.generate_report(
            start_date=date(2030, 1, 1),
            end_date=date(2030, 1, 31),
        )
        assert pdf_bytes[:5] == b"%PDF-"
