"""Seed a development DuckDB database with synthetic health data.

Creates ~/health_dw/health_dw_dev.db with 30 days of realistic
synthetic data for local development and testing.
"""

from __future__ import annotations

import os
import random
from datetime import date, timedelta
from pathlib import Path

import duckdb


def _get_dev_db_path() -> str:
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    return str(Path.home() / "health_dw" / "health_dw_dev.db")


def seed_dev_database() -> None:
    """Create and seed a development database with 30 days of synthetic data."""
    db_path = _get_dev_db_path()
    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    print(f"Seeding dev database: {db_path}")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    today = date.today()
    days = [(today - timedelta(days=i)) for i in range(30, -1, -1)]

    _create_daily_sleep(con, days)
    _create_daily_readiness(con, days)
    _create_daily_activity(con, days)
    _create_heart_rate(con, days)
    _create_daily_stress(con, days)
    _create_weight(con, days)
    _create_patient_profile(con)

    con.close()
    print(f"Dev database seeded: {len(days)} days of synthetic data")


def _create_daily_sleep(con: duckdb.DuckDBPyConnection, days: list[date]) -> None:
    con.execute("DROP TABLE IF EXISTS silver.daily_sleep")
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
    random.seed(42)
    for d in days:
        score = random.randint(60, 95)
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            """INSERT INTO silver.daily_sleep (sk_date, day, sleep_score,
               contributor_deep_sleep, contributor_efficiency, contributor_latency,
               contributor_rem_sleep, contributor_restfulness, contributor_timing,
               contributor_total_sleep)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                sk,
                d,
                score,
                random.randint(50, 95),
                random.randint(60, 98),
                random.randint(40, 90),
                random.randint(50, 90),
                random.randint(55, 95),
                random.randint(45, 90),
                random.randint(60, 95),
            ],
        )


def _create_daily_readiness(con: duckdb.DuckDBPyConnection, days: list[date]) -> None:
    con.execute("DROP TABLE IF EXISTS silver.daily_readiness")
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
    random.seed(43)
    for d in days:
        score = random.randint(55, 92)
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            "INSERT INTO silver.daily_readiness (sk_date, day, readiness_score) VALUES (?, ?, ?)",
            [sk, d, score],
        )


def _create_daily_activity(con: duckdb.DuckDBPyConnection, days: list[date]) -> None:
    con.execute("DROP TABLE IF EXISTS silver.daily_activity")
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
    random.seed(44)
    for d in days:
        steps = random.randint(3000, 15000)
        score = min(100, max(30, int(steps / 120)))
        active_cal = random.randint(200, 800)
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            """INSERT INTO silver.daily_activity (sk_date, day, activity_score, steps,
               active_calories, total_calories)
            VALUES (?, ?, ?, ?, ?, ?)""",
            [sk, d, score, steps, active_cal, active_cal + 1600],
        )


def _create_heart_rate(con: duckdb.DuckDBPyConnection, days: list[date]) -> None:
    con.execute("DROP TABLE IF EXISTS silver.heart_rate")
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
    random.seed(45)
    for d in days:
        sk = int(d.strftime("%Y%m%d"))
        # Resting HR (3am reading)
        resting = random.randint(48, 62)
        con.execute(
            "INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name) VALUES (?, ?, ?, ?)",
            [sk, f"{d} 03:00:00", resting, "Oura Ring"],
        )
        # Daytime readings
        for hour in [8, 12, 15, 18]:
            bpm = random.randint(60, 95)
            con.execute(
                "INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name) VALUES (?, ?, ?, ?)",
                [sk, f"{d} {hour:02d}:00:00", bpm, "Apple Watch"],
            )


def _create_daily_stress(con: duckdb.DuckDBPyConnection, days: list[date]) -> None:
    con.execute("DROP TABLE IF EXISTS silver.daily_stress")
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
    random.seed(46)
    statuses = ["restored", "normal", "stressed"]
    for d in days:
        sk = int(d.strftime("%Y%m%d"))
        summary = random.choice(statuses)
        stress = random.randint(60, 420)
        recovery = random.randint(180, 540)
        con.execute(
            "INSERT INTO silver.daily_stress (sk_date, day, day_summary, stress_high, recovery_high) VALUES (?, ?, ?, ?, ?)",
            [sk, d, summary, stress, recovery],
        )


def _create_weight(con: duckdb.DuckDBPyConnection, days: list[date]) -> None:
    con.execute("DROP TABLE IF EXISTS silver.weight")
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
    random.seed(47)
    weight = 82.0
    for d in days:
        # Only weigh every 2-3 days
        if random.random() < 0.4:
            weight += random.uniform(-0.3, 0.2)
            sk = int(d.strftime("%Y%m%d"))
            con.execute(
                """INSERT INTO silver.weight (sk_date, datetime, weight_kg, fat_mass_kg, muscle_mass_kg)
                VALUES (?, ?, ?, ?, ?)""",
                [
                    sk,
                    f"{d} 07:00:00",
                    round(weight, 1),
                    round(random.uniform(14, 16), 1),
                    round(random.uniform(37, 39), 1),
                ],
            )


def _create_patient_profile(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DROP TABLE IF EXISTS agent.patient_profile")
    con.execute(
        """
        CREATE TABLE agent.patient_profile (
            category VARCHAR, profile_key VARCHAR, profile_value VARCHAR,
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    entries = [
        ("demographics", "age", "45"),
        ("demographics", "sex", "male"),
        ("demographics", "height_cm", "178"),
        ("body", "weight_kg", "82"),
        ("goals", "target_steps", "8000"),
        ("goals", "target_sleep_score", "80"),
        ("devices", "primary_wearable", "Oura Ring Gen 3"),
        ("devices", "secondary", "Apple Watch"),
    ]
    for cat, key, val in entries:
        con.execute(
            "INSERT INTO agent.patient_profile (category, profile_key, profile_value) VALUES (?, ?, ?)",
            [cat, key, val],
        )


if __name__ == "__main__":
    seed_dev_database()
