"""Shared fixtures for health platform tests."""

from __future__ import annotations

import sys

import duckdb
import pytest

# Skip tests with deep macOS-specific import chains on non-macOS CI.
# These files have pytestmark = pytest.mark.integration but crash at import
# time before pytest can read the marker (missing transitive deps on Linux).
if sys.platform != "darwin":
    collect_ignore_glob = [
        "test_*api*.py",
        "test_desktop_*.py",
        "test_export_*.py",
        "test_hae_*.py",
        "test_intelligence_*.py",
        "test_mcp_*.py",
        "test_sundhed_*.py",
    ]


@pytest.fixture
def memory_db():
    """In-memory DuckDB connection with silver and agent schemas."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    yield con
    con.close()


@pytest.fixture
def quality_db(memory_db):
    """In-memory DuckDB with intentional DQ issues for quality testing."""
    con = memory_db

    # Table with good data (all checks should pass)
    con.execute(
        """
        CREATE TABLE silver.dq_good (
            business_key_hash VARCHAR NOT NULL,
            sk_date INTEGER NOT NULL,
            day DATE NOT NULL,
            score INTEGER,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.dq_good VALUES
            ('hash_1', 20260307, CURRENT_DATE, 85, CURRENT_TIMESTAMP),
            ('hash_2', 20260306, CURRENT_DATE - INTERVAL '1 day', 72, CURRENT_TIMESTAMP)
    """
    )

    # Table with NULL violations
    con.execute(
        """
        CREATE TABLE silver.dq_nulls (
            business_key_hash VARCHAR,
            sk_date INTEGER,
            day DATE,
            score INTEGER,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.dq_nulls VALUES
            ('hash_1', 20260307, '2026-03-07', 85, CURRENT_TIMESTAMP),
            (NULL, 20260306, '2026-03-06', NULL, CURRENT_TIMESTAMP),
            ('hash_3', NULL, NULL, 70, CURRENT_TIMESTAMP)
    """
    )

    # Table with duplicate business keys
    con.execute(
        """
        CREATE TABLE silver.dq_dupes (
            business_key_hash VARCHAR NOT NULL,
            sk_date INTEGER NOT NULL,
            day DATE NOT NULL,
            score INTEGER,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.dq_dupes VALUES
            ('hash_1', 20260307, '2026-03-07', 85, CURRENT_TIMESTAMP),
            ('hash_1', 20260306, '2026-03-06', 72, CURRENT_TIMESTAMP),
            ('hash_2', 20260305, '2026-03-05', 90, CURRENT_TIMESTAMP)
    """
    )

    # Table with stale data (48 hours old)
    con.execute(
        """
        CREATE TABLE silver.dq_stale (
            business_key_hash VARCHAR NOT NULL,
            sk_date INTEGER NOT NULL,
            day DATE NOT NULL,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.dq_stale VALUES
            ('hash_1', 20260305, CURRENT_DATE - INTERVAL '2 days',
             CURRENT_TIMESTAMP - INTERVAL '48 hours')
    """
    )

    # Table with out-of-range values
    con.execute(
        """
        CREATE TABLE silver.dq_range (
            business_key_hash VARCHAR NOT NULL,
            sk_date INTEGER NOT NULL,
            score INTEGER,
            bpm INTEGER,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.dq_range VALUES
            ('hash_1', 20260307, 85, 72, CURRENT_TIMESTAMP),
            ('hash_2', 20260306, 150, 300, CURRENT_TIMESTAMP),
            ('hash_3', 20260305, -5, 15, CURRENT_TIMESTAMP)
    """
    )

    # Empty table
    con.execute(
        """
        CREATE TABLE silver.dq_empty (
            business_key_hash VARCHAR NOT NULL,
            sk_date INTEGER NOT NULL,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Table with complete daily series (7 days, no gaps)
    con.execute(
        """
        CREATE TABLE silver.dq_complete (
            business_key_hash VARCHAR NOT NULL,
            sk_date INTEGER NOT NULL,
            day DATE NOT NULL,
            score INTEGER,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.dq_complete VALUES
            ('hash_1', 20260301, '2026-03-01', 80, CURRENT_TIMESTAMP),
            ('hash_2', 20260302, '2026-03-02', 82, CURRENT_TIMESTAMP),
            ('hash_3', 20260303, '2026-03-03', 78, CURRENT_TIMESTAMP),
            ('hash_4', 20260304, '2026-03-04', 85, CURRENT_TIMESTAMP),
            ('hash_5', 20260305, '2026-03-05', 79, CURRENT_TIMESTAMP),
            ('hash_6', 20260306, '2026-03-06', 81, CURRENT_TIMESTAMP),
            ('hash_7', 20260307, '2026-03-07', 83, CURRENT_TIMESTAMP)
    """
    )

    # Table with gaps: 8 dates present, 3 missing → 2 gap periods
    # Present: 03-01, 03-02, 03-04, 03-05, 03-08, 03-09, 03-10, 03-11
    # Missing: 03-03, 03-06, 03-07 → gaps: [03-03] and [03-06 to 03-07]
    con.execute(
        """
        CREATE TABLE silver.dq_gaps (
            business_key_hash VARCHAR NOT NULL,
            sk_date INTEGER NOT NULL,
            day DATE NOT NULL,
            score INTEGER,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.dq_gaps VALUES
            ('hash_1', 20260301, '2026-03-01', 80, CURRENT_TIMESTAMP),
            ('hash_2', 20260302, '2026-03-02', 82, CURRENT_TIMESTAMP),
            ('hash_4', 20260304, '2026-03-04', 85, CURRENT_TIMESTAMP),
            ('hash_5', 20260305, '2026-03-05', 79, CURRENT_TIMESTAMP),
            ('hash_8', 20260308, '2026-03-08', 77, CURRENT_TIMESTAMP),
            ('hash_9', 20260309, '2026-03-09', 84, CURRENT_TIMESTAMP),
            ('hash_10', 20260310, '2026-03-10', 86, CURRENT_TIMESTAMP),
            ('hash_11', 20260311, '2026-03-11', 81, CURRENT_TIMESTAMP)
    """
    )

    yield con


@pytest.fixture
def seeded_db(memory_db):
    """In-memory DuckDB with silver tables containing synthetic test data."""
    con = memory_db

    # Create silver.daily_sleep
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
        INSERT INTO silver.daily_sleep (sk_date, day, sleep_score, contributor_deep_sleep,
            contributor_efficiency, contributor_latency, contributor_rem_sleep,
            contributor_restfulness, contributor_timing, contributor_total_sleep)
        VALUES
            (20260301, '2026-03-01', 82, 88, 90, 65, 78, 85, 72, 80),
            (20260302, '2026-03-02', 75, 70, 85, 72, 80, 75, 68, 77),
            (20260303, '2026-03-03', 91, 95, 92, 88, 85, 90, 85, 93)
    """
    )

    # Create silver.daily_readiness
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
            (20260303, '2026-03-03', 88)
    """
    )

    # Create silver.daily_activity
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
            (20260303, '2026-03-03', 85, 9800, 520, 2300)
    """
    )

    # Create silver.daily_stress
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
        INSERT INTO silver.daily_stress (sk_date, day, day_summary, stress_high, recovery_high)
        VALUES
            (20260301, '2026-03-01', 'restored', 120, 480),
            (20260302, '2026-03-02', 'stressed', 360, 240),
            (20260303, '2026-03-03', 'normal', 200, 400)
    """
    )

    # Create silver.daily_spo2
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
        INSERT INTO silver.daily_spo2 (sk_date, day, spo2_avg_pct, breathing_disturbance_index)
        VALUES
            (20260301, '2026-03-01', 97.5, 0.8),
            (20260302, '2026-03-02', 96.2, 1.2),
            (20260303, '2026-03-03', 98.1, 0.5)
    """
    )

    # Create silver.heart_rate
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
            (20260302, '2026-03-02 14:00:00', 85, 'Apple Watch'),
            (20260303, '2026-03-03 03:00:00', 50, 'Oura Ring')
    """
    )

    # Create silver.weight
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
        INSERT INTO silver.weight (sk_date, datetime, weight_kg, fat_mass_kg, muscle_mass_kg)
        VALUES
            (20260301, '2026-03-01 07:00:00', 82.5, 15.2, 38.1),
            (20260303, '2026-03-03 07:00:00', 82.3, 15.0, 38.3)
    """
    )

    # Create silver.personal_info
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

    yield con
