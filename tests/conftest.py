"""Shared fixtures for health platform tests."""
from __future__ import annotations

import pytest
import duckdb


@pytest.fixture
def memory_db():
    """In-memory DuckDB connection with silver and agent schemas."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    yield con
    con.close()


@pytest.fixture
def seeded_db(memory_db):
    """In-memory DuckDB with silver tables containing synthetic test data."""
    con = memory_db

    # Create silver.daily_sleep
    con.execute("""
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
    """)
    con.execute("""
        INSERT INTO silver.daily_sleep (sk_date, day, sleep_score, contributor_deep_sleep,
            contributor_efficiency, contributor_latency, contributor_rem_sleep,
            contributor_restfulness, contributor_timing, contributor_total_sleep)
        VALUES
            (20260301, '2026-03-01', 82, 88, 90, 65, 78, 85, 72, 80),
            (20260302, '2026-03-02', 75, 70, 85, 72, 80, 75, 68, 77),
            (20260303, '2026-03-03', 91, 95, 92, 88, 85, 90, 85, 93)
    """)

    # Create silver.daily_readiness
    con.execute("""
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
    """)
    con.execute("""
        INSERT INTO silver.daily_readiness (sk_date, day, readiness_score)
        VALUES
            (20260301, '2026-03-01', 79),
            (20260302, '2026-03-02', 72),
            (20260303, '2026-03-03', 88)
    """)

    # Create silver.daily_activity
    con.execute("""
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
    """)
    con.execute("""
        INSERT INTO silver.daily_activity (sk_date, day, activity_score, steps, active_calories, total_calories)
        VALUES
            (20260301, '2026-03-01', 91, 12450, 650, 2400),
            (20260302, '2026-03-02', 68, 5200, 320, 2100),
            (20260303, '2026-03-03', 85, 9800, 520, 2300)
    """)

    # Create silver.daily_stress
    con.execute("""
        CREATE TABLE silver.daily_stress (
            sk_date INTEGER, day DATE, day_summary VARCHAR,
            stress_high INTEGER, recovery_high INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.daily_stress (sk_date, day, day_summary, stress_high, recovery_high)
        VALUES
            (20260301, '2026-03-01', 'restored', 120, 480),
            (20260302, '2026-03-02', 'stressed', 360, 240),
            (20260303, '2026-03-03', 'normal', 200, 400)
    """)

    # Create silver.daily_spo2
    con.execute("""
        CREATE TABLE silver.daily_spo2 (
            sk_date INTEGER, day DATE,
            spo2_avg_pct DOUBLE, breathing_disturbance_index DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.daily_spo2 (sk_date, day, spo2_avg_pct, breathing_disturbance_index)
        VALUES
            (20260301, '2026-03-01', 97.5, 0.8),
            (20260302, '2026-03-02', 96.2, 1.2),
            (20260303, '2026-03-03', 98.1, 0.5)
    """)

    # Create silver.heart_rate
    con.execute("""
        CREATE TABLE silver.heart_rate (
            sk_date INTEGER, sk_time VARCHAR,
            timestamp TIMESTAMP, bpm INTEGER, source_name VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name)
        VALUES
            (20260301, '2026-03-01 03:00:00', 52, 'Oura Ring'),
            (20260301, '2026-03-01 12:00:00', 78, 'Apple Watch'),
            (20260302, '2026-03-02 03:00:00', 55, 'Oura Ring'),
            (20260302, '2026-03-02 14:00:00', 85, 'Apple Watch'),
            (20260303, '2026-03-03 03:00:00', 50, 'Oura Ring')
    """)

    # Create silver.weight
    con.execute("""
        CREATE TABLE silver.weight (
            sk_date INTEGER, sk_time VARCHAR,
            datetime TIMESTAMP, weight_kg DOUBLE,
            fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
            muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.weight (sk_date, datetime, weight_kg, fat_mass_kg, muscle_mass_kg)
        VALUES
            (20260301, '2026-03-01 07:00:00', 82.5, 15.2, 38.1),
            (20260303, '2026-03-03 07:00:00', 82.3, 15.0, 38.3)
    """)

    # Create silver.personal_info
    con.execute("""
        CREATE TABLE silver.personal_info (
            age INTEGER, weight_kg DOUBLE, height_m DOUBLE,
            biological_sex VARCHAR, email VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO silver.personal_info (age, weight_kg, height_m, biological_sex, email)
        VALUES (45, 75.0, 1.78, 'male', 'test@example.com')
    """)

    yield con
