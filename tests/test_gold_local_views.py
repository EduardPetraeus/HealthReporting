"""Tests for gold layer local DuckDB views.

Uses in-memory DuckDB with synthetic silver data to verify all gold SQL
files execute correctly and produce expected results.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest

GOLD_SQL_DIR = (
    Path(__file__).resolve().parent.parent
    / "health_unified_platform"
    / "health_platform"
    / "transformation_logic"
    / "gold_local"
)

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load_script(module_name: str):
    """Load a module from the scripts/ directory (not a package)."""
    script_path = SCRIPTS_DIR / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def gold_db():
    """In-memory DuckDB with silver and gold schemas plus synthetic silver data."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # --- Silver tables with synthetic data ---

    # daily_sleep (used by fct_daily_health_score, fct_sleep_session)
    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            contributor_deep_sleep INTEGER, contributor_rem_sleep INTEGER,
            contributor_efficiency INTEGER, contributor_latency INTEGER,
            contributor_restfulness INTEGER, contributor_timing INTEGER,
            contributor_total_sleep INTEGER
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_sleep VALUES
            (20260301, '2026-03-01', 82, 88, 78, 90, 65, 85, 72, 80),
            (20260302, '2026-03-02', 75, 70, 80, 85, 72, 75, 68, 77),
            (20260303, '2026-03-03', 91, 95, 85, 92, 88, 90, 85, 93)
    """
    )

    # daily_readiness (used by fct_daily_health_score)
    con.execute(
        """
        CREATE TABLE silver.daily_readiness (
            sk_date INTEGER, day DATE, readiness_score INTEGER,
            temperature_deviation DOUBLE
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_readiness VALUES
            (20260301, '2026-03-01', 79, 0.1),
            (20260302, '2026-03-02', 72, -0.2),
            (20260303, '2026-03-03', 88, 0.0)
    """
    )

    # daily_activity (used by fct_daily_health_score)
    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER,
            steps INTEGER, total_calories INTEGER, active_calories INTEGER
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_activity VALUES
            (20260301, '2026-03-01', 91, 12450, 2400, 650),
            (20260302, '2026-03-02', 68, 5200, 2100, 320),
            (20260303, '2026-03-03', 85, 9800, 2300, 520)
    """
    )

    # daily_stress (used by fct_daily_health_score)
    con.execute(
        """
        CREATE TABLE silver.daily_stress (
            sk_date INTEGER, day DATE, day_summary VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_stress VALUES
            (20260301, '2026-03-01', 'restored'),
            (20260302, '2026-03-02', 'stressed'),
            (20260303, '2026-03-03', 'normal')
    """
    )

    # workout (used by fct_daily_health_score, fct_workout, dim_workout_type)
    con.execute(
        """
        CREATE TABLE silver.workout (
            sk_date INTEGER, day DATE, workout_id VARCHAR,
            activity VARCHAR, intensity VARCHAR,
            duration_seconds INTEGER, calories INTEGER,
            distance_meters DOUBLE,
            start_datetime TIMESTAMP, end_datetime TIMESTAMP,
            source VARCHAR, label VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.workout VALUES
            (20260301, '2026-03-01', 'w001', 'running', 'moderate',
             1800, 350, 5200.0,
             '2026-03-01 07:00:00', '2026-03-01 07:30:00', 'apple_health', NULL),
            (20260303, '2026-03-03', 'w002', 'strength_training', 'high',
             3600, 280, NULL,
             '2026-03-03 17:00:00', '2026-03-03 18:00:00', 'apple_health', NULL)
    """
    )

    # weight (used by fct_body_measurement)
    con.execute(
        """
        CREATE TABLE silver.weight (
            sk_date INTEGER, sk_time VARCHAR,
            datetime TIMESTAMP, weight_kg DOUBLE,
            fat_mass_kg DOUBLE, muscle_mass_kg DOUBLE,
            bone_mass_kg DOUBLE, hydration_kg DOUBLE
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.weight VALUES
            (20260301, '0700', '2026-03-01 07:00:00', 82.5, 15.2, 38.1, 3.1, 42.0),
            (20260303, '0700', '2026-03-03 07:00:00', 82.3, 15.0, 38.3, 3.1, 42.2)
    """
    )

    # daily_meal (used by fct_daily_nutrition)
    con.execute(
        """
        CREATE TABLE silver.daily_meal (
            sk_date INTEGER, date DATE, meal_type VARCHAR,
            calories DOUBLE, protein DOUBLE, carbs DOUBLE, fat DOUBLE,
            carbs_fiber DOUBLE, carbs_sugar DOUBLE
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_meal VALUES
            (20260301, '2026-03-01', 'breakfast', 450, 25, 55, 15, 5, 10),
            (20260301, '2026-03-01', 'lunch', 650, 40, 65, 22, 8, 12),
            (20260301, '2026-03-01', 'dinner', 700, 45, 60, 28, 6, 8),
            (20260301, '2026-03-01', 'snack', 200, 10, 30, 8, 2, 15),
            (20260302, '2026-03-02', 'breakfast', 400, 20, 50, 14, 4, 12)
    """
    )

    # resting_heart_rate (used by fct_daily_vitals_summary)
    con.execute(
        """
        CREATE TABLE silver.resting_heart_rate (
            sk_date INTEGER, date DATE, resting_hr_bpm INTEGER,
            source_name VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.resting_heart_rate VALUES
            (20260301, '2026-03-01', 58, 'oura'),
            (20260302, '2026-03-02', 61, 'oura'),
            (20260303, '2026-03-03', 56, 'oura')
    """
    )

    # daily_spo2 (used by fct_daily_vitals_summary)
    con.execute(
        """
        CREATE TABLE silver.daily_spo2 (
            sk_date INTEGER, day DATE, spo2_avg_pct DOUBLE
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_spo2 VALUES
            (20260301, '2026-03-01', 97.5),
            (20260302, '2026-03-02', 96.2),
            (20260303, '2026-03-03', 98.1)
    """
    )

    # blood_pressure (used by fct_daily_vitals_summary)
    con.execute(
        """
        CREATE TABLE silver.blood_pressure (
            sk_date INTEGER, datetime TIMESTAMP,
            systolic INTEGER, diastolic INTEGER, pulse INTEGER
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.blood_pressure VALUES
            (20260301, '2026-03-01 08:00:00', 125, 82, 68),
            (20260301, '2026-03-01 20:00:00', 118, 76, 72)
    """
    )

    # water_intake (used by fct_daily_vitals_summary)
    con.execute(
        """
        CREATE TABLE silver.water_intake (
            sk_date INTEGER, timestamp TIMESTAMP,
            water_ml DOUBLE, source_name VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.water_intake VALUES
            (20260301, '2026-03-01 09:00:00', 500, 'apple_health'),
            (20260301, '2026-03-01 12:00:00', 300, 'apple_health'),
            (20260301, '2026-03-01 15:00:00', 400, 'apple_health')
    """
    )

    # daily_walking_gait (used by fct_daily_vitals_summary)
    con.execute(
        """
        CREATE TABLE silver.daily_walking_gait (
            sk_date INTEGER, date DATE,
            walking_speed_avg_km_hr DOUBLE,
            walking_step_length_avg_cm DOUBLE,
            walking_asymmetry_avg_pct DOUBLE,
            walking_double_support_avg_pct DOUBLE,
            walking_steadiness_pct DOUBLE
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_walking_gait VALUES
            (20260301, '2026-03-01', 5.2, 72.5, 4.1, 28.3, 95.0)
    """
    )

    # mindful_session (used by fct_daily_vitals_summary)
    con.execute(
        """
        CREATE TABLE silver.mindful_session (
            sk_date INTEGER, timestamp TIMESTAMP, duration_seconds INTEGER
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.mindful_session VALUES
            (20260301, '2026-03-01 06:30:00', 600),
            (20260301, '2026-03-01 21:00:00', 300)
    """
    )

    # lab_results (used by fct_lab_result, dim_lab_marker)
    con.execute(
        """
        CREATE TABLE silver.lab_results (
            test_date DATE, test_id VARCHAR, test_type VARCHAR,
            test_name VARCHAR, lab_name VARCHAR,
            marker_name VARCHAR, marker_category VARCHAR,
            value_numeric DOUBLE, value_text VARCHAR,
            unit VARCHAR, reference_min DOUBLE, reference_max DOUBLE,
            status VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.lab_results VALUES
            ('2026-02-15', 'T001', 'blood', 'CBC', 'LabCorp',
             'hemoglobin', 'vitamins', 14.5, NULL, 'g/dL', 13.5, 17.5, 'normal'),
            ('2026-02-15', 'T001', 'blood', 'CBC', 'LabCorp',
             'white_blood_cells', 'immune_markers', 7.2, NULL, '10^3/uL', 4.5, 11.0, 'normal'),
            ('2026-02-15', 'T001', 'stool', 'GI Panel', 'LabCorp',
             'calprotectin', 'gut_inflammation', 85.0, NULL, 'ug/g', NULL, 50.0, 'high')
    """
    )

    # heart_rate (used by daily_heart_rate_summary, vw_heart_rate_avg_per_day)
    con.execute(
        """
        CREATE TABLE silver.heart_rate (
            sk_date INTEGER, sk_time VARCHAR,
            timestamp TIMESTAMP, bpm INTEGER,
            source_name VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.heart_rate VALUES
            (20260301, '0300', '2026-03-01 03:00:00', 52, 'oura'),
            (20260301, '1200', '2026-03-01 12:00:00', 78, 'apple_health'),
            (20260301, '1500', '2026-03-01 15:00:00', 85, 'apple_health'),
            (20260302, '0300', '2026-03-02 03:00:00', 55, 'oura')
    """
    )

    # supplement_log (used by dim_supplement)
    con.execute(
        """
        CREATE TABLE silver.supplement_log (
            supplement_name VARCHAR, dose DOUBLE, unit VARCHAR, target VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.supplement_log VALUES
            ('Vitamin D3', 4000, 'IU', 'bone_health'),
            ('Omega-3 Fish Oil', 1000, 'mg', 'cardiovascular'),
            ('Magnesium Citrate', 400, 'mg', 'sleep'),
            ('Creon Enzyme', 25000, 'IU', 'digestion')
    """
    )

    # step_count (used by dim_source)
    con.execute(
        """
        CREATE TABLE silver.step_count (
            sk_date INTEGER, source_name VARCHAR, steps INTEGER
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.step_count VALUES
            (20260301, 'apple_health', 12450)
    """
    )

    # blood_oxygen (used by dim_source)
    con.execute(
        """
        CREATE TABLE silver.blood_oxygen (
            sk_date INTEGER, source VARCHAR, spo2_pct DOUBLE
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.blood_oxygen VALUES
            (20260301, 'apple_health', 97.5)
    """
    )

    # daily_annotations (used by vw_daily_annotations)
    con.execute(
        """
        CREATE TABLE silver.daily_annotations (
            sk_date INTEGER, annotation_type VARCHAR,
            annotation VARCHAR, is_valid BOOLEAN
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_annotations VALUES
            (20260301, 'training', 'Morning run 5k', true),
            (20260301, 'note', 'Invalid entry', false),
            (20260302, 'travel', 'Flight to Berlin', true)
    """
    )

    yield con
    con.close()


def _read_sql(filename: str) -> str:
    """Read a gold SQL file by name."""
    return (GOLD_SQL_DIR / filename).read_text()


def _execute_sql(con: duckdb.DuckDBPyConnection, filename: str) -> None:
    """Execute a gold SQL file, handling multi-statement files."""
    sql = _read_sql(filename)
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        con.execute(stmt)


# ---------------------------------------------------------------------------
# Test: All SQL files exist
# ---------------------------------------------------------------------------


class TestGoldSqlFilesExist:
    """Verify all expected gold SQL files are present."""

    EXPECTED_FILES = [
        "dim_body_system.sql",
        "dim_date.sql",
        "dim_health_zone.sql",
        "dim_lab_marker.sql",
        "dim_meal_type.sql",
        "dim_metric.sql",
        "dim_source.sql",
        "dim_supplement.sql",
        "dim_time_of_day.sql",
        "dim_workout_type.sql",
        "fct_body_measurement.sql",
        "fct_daily_health_score.sql",
        "fct_daily_nutrition.sql",
        "fct_daily_vitals_summary.sql",
        "fct_lab_result.sql",
        "fct_sleep_session.sql",
        "fct_workout.sql",
        "daily_heart_rate_summary.sql",
        "vw_daily_annotations.sql",
        "vw_heart_rate_avg_per_day.sql",
    ]

    def test_all_files_exist(self):
        """All 20 gold SQL files must exist."""
        for filename in self.EXPECTED_FILES:
            assert (GOLD_SQL_DIR / filename).exists(), f"Missing: {filename}"

    def test_file_count(self):
        """Exactly 20 SQL files in gold_local."""
        sql_files = list(GOLD_SQL_DIR.glob("*.sql"))
        assert len(sql_files) == 20


# ---------------------------------------------------------------------------
# Test: Static dimension tables create successfully
# ---------------------------------------------------------------------------


class TestStaticDimensions:
    """Test static dimension tables that don't depend on silver data."""

    def test_dim_body_system(self, gold_db):
        _execute_sql(gold_db, "dim_body_system.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_body_system").fetchone()
        assert result[0] == 12

    def test_dim_body_system_columns(self, gold_db):
        _execute_sql(gold_db, "dim_body_system.sql")
        cols = [
            row[0]
            for row in gold_db.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='gold' AND table_name='dim_body_system' "
                "ORDER BY ordinal_position"
            ).fetchall()
        ]
        assert cols == [
            "sk_body_system",
            "body_system_code",
            "body_system_name",
            "metric_category",
        ]

    def test_dim_date(self, gold_db):
        _execute_sql(gold_db, "dim_date.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_date").fetchone()
        # 2020-01-01 to 2030-12-31 = 4018 days
        assert result[0] > 4000

    def test_dim_date_columns(self, gold_db):
        _execute_sql(gold_db, "dim_date.sql")
        cols = [
            row[0]
            for row in gold_db.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='gold' AND table_name='dim_date' "
                "ORDER BY ordinal_position"
            ).fetchall()
        ]
        expected = [
            "sk_date",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "iso_week",
            "day_of_week",
            "day_name",
            "day_of_month",
            "day_of_year",
            "is_weekend",
            "day_type",
            "season",
            "year_month",
            "days_ago",
        ]
        assert cols == expected

    def test_dim_date_specific_date(self, gold_db):
        """Verify a known date has correct attributes."""
        _execute_sql(gold_db, "dim_date.sql")
        row = gold_db.execute(
            "SELECT sk_date, year, month, month_name, day_name, season "
            "FROM gold.dim_date WHERE date = '2026-03-01'"
        ).fetchone()
        assert row[0] == 20260301  # sk_date
        assert row[1] == 2026  # year
        assert row[2] == 3  # month
        assert row[3] == "March"  # month_name
        assert row[4] == "Sunday"  # day_name
        assert row[5] == "spring"  # season

    def test_dim_health_zone(self, gold_db):
        _execute_sql(gold_db, "dim_health_zone.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_health_zone").fetchone()
        assert result[0] == 42

    def test_dim_meal_type(self, gold_db):
        _execute_sql(gold_db, "dim_meal_type.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_meal_type").fetchone()
        assert result[0] == 4

    def test_dim_metric(self, gold_db):
        _execute_sql(gold_db, "dim_metric.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_metric").fetchone()
        assert result[0] == 28

    def test_dim_time_of_day(self, gold_db):
        _execute_sql(gold_db, "dim_time_of_day.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_time_of_day").fetchone()
        assert result[0] == 1440  # 24 * 60

    def test_dim_time_of_day_midnight(self, gold_db):
        """Verify midnight entry."""
        _execute_sql(gold_db, "dim_time_of_day.sql")
        row = gold_db.execute(
            "SELECT sk_time, hour, minute, time_label, time_bucket "
            "FROM gold.dim_time_of_day WHERE hour = 0 AND minute = 0"
        ).fetchone()
        assert row[0] == "0000"
        assert row[3] == "00:00"
        assert row[4] == "night"


# ---------------------------------------------------------------------------
# Test: Data-driven dimension tables
# ---------------------------------------------------------------------------


class TestDataDrivenDimensions:
    """Test dimensions that derive from silver data."""

    def test_dim_lab_marker(self, gold_db):
        _execute_sql(gold_db, "dim_lab_marker.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_lab_marker").fetchone()
        assert result[0] == 3  # 3 distinct markers in test data

    def test_dim_lab_marker_body_system(self, gold_db):
        _execute_sql(gold_db, "dim_lab_marker.sql")
        row = gold_db.execute(
            "SELECT body_system FROM gold.dim_lab_marker WHERE marker_name = 'calprotectin'"
        ).fetchone()
        assert row[0] == "gastrointestinal"

    def test_dim_source(self, gold_db):
        _execute_sql(gold_db, "dim_source.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_source").fetchone()
        assert result[0] >= 1

    def test_dim_supplement(self, gold_db):
        _execute_sql(gold_db, "dim_supplement.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.dim_supplement").fetchone()
        assert result[0] == 4

    def test_dim_supplement_categories(self, gold_db):
        _execute_sql(gold_db, "dim_supplement.sql")
        categories = [
            row[0]
            for row in gold_db.execute(
                "SELECT category FROM gold.dim_supplement ORDER BY supplement_name"
            ).fetchall()
        ]
        assert "enzyme" in categories  # Creon
        assert "mineral" in categories  # Magnesium
        assert "fatty_acid" in categories  # Omega-3
        assert "vitamin" in categories  # Vitamin D3

    def test_dim_workout_type(self, gold_db):
        _execute_sql(gold_db, "dim_workout_type.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.dim_workout_type"
        ).fetchone()
        assert result[0] == 2  # running, strength_training

    def test_dim_workout_type_categories(self, gold_db):
        _execute_sql(gold_db, "dim_workout_type.sql")
        row = gold_db.execute(
            "SELECT activity_category, is_cardio, estimated_energy_system "
            "FROM gold.dim_workout_type WHERE activity_raw = 'running'"
        ).fetchone()
        assert row[0] == "cardio"
        assert row[1] is True
        assert row[2] == "aerobic"


# ---------------------------------------------------------------------------
# Test: Fact views
# ---------------------------------------------------------------------------


class TestFactViews:
    """Test fact views that join across silver tables."""

    def test_fct_body_measurement(self, gold_db):
        _execute_sql(gold_db, "fct_body_measurement.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.fct_body_measurement"
        ).fetchone()
        assert result[0] == 2

    def test_fct_body_measurement_computed_cols(self, gold_db):
        _execute_sql(gold_db, "fct_body_measurement.sql")
        row = gold_db.execute(
            "SELECT fat_pct, muscle_pct FROM gold.fct_body_measurement WHERE sk_date = 20260301"
        ).fetchone()
        # fat_pct = 15.2 / 82.5 * 100 = 18.4
        assert abs(row[0] - 18.4) < 0.2
        # muscle_pct = 38.1 / 82.5 * 100 = 46.2
        assert abs(row[1] - 46.2) < 0.2

    def test_fct_daily_health_score(self, gold_db):
        _execute_sql(gold_db, "fct_daily_health_score.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.fct_daily_health_score"
        ).fetchone()
        assert result[0] == 3

    def test_fct_daily_health_score_composite(self, gold_db):
        """Verify composite score calculation: sleep*0.35 + readiness*0.35 + activity*0.30."""
        _execute_sql(gold_db, "fct_daily_health_score.sql")
        row = gold_db.execute(
            "SELECT composite_health_score, health_status "
            "FROM gold.fct_daily_health_score WHERE sk_date = 20260301"
        ).fetchone()
        # 82*0.35 + 79*0.35 + 91*0.30 = 28.7 + 27.65 + 27.3 = 83.65
        expected = 82 * 0.35 + 79 * 0.35 + 91 * 0.30
        assert abs(float(row[0]) - expected) < 0.2
        assert row[1] == "good"  # 83.65 >= 70 but < 85

    def test_fct_daily_health_score_excellent(self, gold_db):
        """Day 3: high scores should produce 'excellent' status."""
        _execute_sql(gold_db, "fct_daily_health_score.sql")
        row = gold_db.execute(
            "SELECT composite_health_score, health_status "
            "FROM gold.fct_daily_health_score WHERE sk_date = 20260303"
        ).fetchone()
        # 91*0.35 + 88*0.35 + 85*0.30 = 31.85 + 30.8 + 25.5 = 88.15
        assert row[1] == "excellent"

    def test_fct_daily_health_score_workout_flag(self, gold_db):
        """Day with workout should have is_workout_day=True."""
        _execute_sql(gold_db, "fct_daily_health_score.sql")
        row = gold_db.execute(
            "SELECT is_workout_day, workout_count "
            "FROM gold.fct_daily_health_score WHERE sk_date = 20260301"
        ).fetchone()
        assert row[0] is True
        assert row[1] == 1

    def test_fct_daily_health_score_no_workout(self, gold_db):
        """Day without workout should have is_workout_day=False."""
        _execute_sql(gold_db, "fct_daily_health_score.sql")
        row = gold_db.execute(
            "SELECT is_workout_day, workout_count "
            "FROM gold.fct_daily_health_score WHERE sk_date = 20260302"
        ).fetchone()
        assert row[0] is False
        assert row[1] == 0

    def test_fct_daily_nutrition(self, gold_db):
        _execute_sql(gold_db, "fct_daily_nutrition.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.fct_daily_nutrition"
        ).fetchone()
        assert result[0] == 2  # 2 days with meal data

    def test_fct_daily_nutrition_aggregation(self, gold_db):
        _execute_sql(gold_db, "fct_daily_nutrition.sql")
        row = gold_db.execute(
            "SELECT total_calories, total_protein_g, meal_count, item_count "
            "FROM gold.fct_daily_nutrition WHERE sk_date = 20260301"
        ).fetchone()
        assert row[0] == 2000  # 450 + 650 + 700 + 200
        assert row[1] == 120.0  # 25 + 40 + 45 + 10
        assert row[2] == 4  # 4 distinct meal types
        assert row[3] == 4  # 4 items

    def test_fct_daily_nutrition_meal_breakdown(self, gold_db):
        _execute_sql(gold_db, "fct_daily_nutrition.sql")
        row = gold_db.execute(
            "SELECT breakfast_calories, lunch_calories, dinner_calories, snack_calories "
            "FROM gold.fct_daily_nutrition WHERE sk_date = 20260301"
        ).fetchone()
        assert row[0] == 450
        assert row[1] == 650
        assert row[2] == 700
        assert row[3] == 200

    def test_fct_daily_vitals_summary(self, gold_db):
        """Vitals summary joins across rhr, spo2, bp, water, gait, mindful."""
        _execute_sql(gold_db, "fct_daily_vitals_summary.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.fct_daily_vitals_summary"
        ).fetchone()
        assert result[0] >= 3  # At least 3 distinct days

    def test_fct_daily_vitals_summary_day1(self, gold_db):
        """Day 1 should have all vitals populated."""
        _execute_sql(gold_db, "fct_daily_vitals_summary.sql")
        row = gold_db.execute(
            "SELECT resting_hr_bpm, avg_spo2_pct, avg_systolic, "
            "total_water_ml, avg_walking_speed, total_mindful_minutes "
            "FROM gold.fct_daily_vitals_summary WHERE sk_date = 20260301"
        ).fetchone()
        assert row[0] == 58  # resting_hr
        assert abs(row[1] - 97.5) < 0.1  # spo2
        assert row[2] in (121, 122)  # avg of 125 and 118
        assert row[3] == 1200  # 500 + 300 + 400 water
        assert abs(row[4] - 5.2) < 0.1  # walking speed
        assert abs(row[5] - 15.0) < 0.1  # (600+300)/60 = 15 mindful minutes

    def test_fct_lab_result(self, gold_db):
        _execute_sql(gold_db, "fct_lab_result.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.fct_lab_result").fetchone()
        assert result[0] == 3

    def test_fct_lab_result_deviation(self, gold_db):
        """Calprotectin above range should have positive deviation."""
        _execute_sql(gold_db, "fct_lab_result.sql")
        row = gold_db.execute(
            "SELECT deviation_from_ref_pct, body_system "
            "FROM gold.fct_lab_result WHERE marker_name = 'calprotectin'"
        ).fetchone()
        assert row[0] > 0  # Above reference max
        assert row[1] == "gastrointestinal"

    def test_fct_sleep_session(self, gold_db):
        _execute_sql(gold_db, "fct_sleep_session.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.fct_sleep_session"
        ).fetchone()
        assert result[0] == 3

    def test_fct_sleep_session_weakest(self, gold_db):
        _execute_sql(gold_db, "fct_sleep_session.sql")
        row = gold_db.execute(
            "SELECT weakest_contributor, weakest_score "
            "FROM gold.fct_sleep_session WHERE sk_date = 20260301"
        ).fetchone()
        # Day 1: latency=65 is the lowest
        assert row[0] == "latency"
        assert row[1] == 65

    def test_fct_workout(self, gold_db):
        _execute_sql(gold_db, "fct_workout.sql")
        result = gold_db.execute("SELECT COUNT(*) FROM gold.fct_workout").fetchone()
        assert result[0] == 2

    def test_fct_workout_computed_cols(self, gold_db):
        _execute_sql(gold_db, "fct_workout.sql")
        row = gold_db.execute(
            "SELECT duration_minutes, distance_km, sk_time "
            "FROM gold.fct_workout WHERE workout_id = 'w001'"
        ).fetchone()
        assert row[0] == 30.0  # 1800/60
        assert row[1] == 5.2  # 5200/1000
        assert row[2] == "0700"


# ---------------------------------------------------------------------------
# Test: Legacy views
# ---------------------------------------------------------------------------


class TestLegacyViews:
    """Test legacy gold views."""

    def test_daily_heart_rate_summary(self, gold_db):
        _execute_sql(gold_db, "daily_heart_rate_summary.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.daily_heart_rate_summary"
        ).fetchone()
        # Per-source rows + cross-source 'all' rows
        assert result[0] >= 3

    def test_daily_heart_rate_summary_cross_source(self, gold_db):
        _execute_sql(gold_db, "daily_heart_rate_summary.sql")
        row = gold_db.execute(
            "SELECT reading_count, avg_bpm "
            "FROM gold.daily_heart_rate_summary "
            "WHERE day = '2026-03-01' AND source_name = 'all'"
        ).fetchone()
        assert row[0] == 3  # 3 readings on day 1
        assert row[1] is not None

    def test_vw_daily_annotations(self, gold_db):
        _execute_sql(gold_db, "vw_daily_annotations.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.vw_daily_annotations_valid"
        ).fetchone()
        assert result[0] == 2  # Only valid=true rows

    def test_vw_heart_rate_avg_per_day(self, gold_db):
        _execute_sql(gold_db, "vw_heart_rate_avg_per_day.sql")
        result = gold_db.execute(
            "SELECT COUNT(*) FROM gold.vw_heart_rate_avg_per_day"
        ).fetchone()
        assert result[0] == 2  # 2 distinct sk_dates


# ---------------------------------------------------------------------------
# Test: Runner script
# ---------------------------------------------------------------------------


class TestRunnerScript:
    """Test the create_gold_views.py runner module."""

    def test_get_sql_files_ordering(self):
        """Dimensions should come before facts in execution order."""
        gold_views = _load_script("create_gold_views")

        files = gold_views.get_sql_files(GOLD_SQL_DIR)
        names = [f.stem for f in files]

        # Find first fct_ index
        first_fct = next(i for i, n in enumerate(names) if n.startswith("fct_"))
        # All dim_ files should come before first fct_
        for i, n in enumerate(names):
            if n.startswith("dim_"):
                assert i < first_fct, f"{n} should come before facts"

    def test_dry_run_does_not_modify_db(self, gold_db, capsys):
        """Dry run should print SQL but not create tables/views."""
        gold_views = _load_script("create_gold_views")

        # Run in dry_run mode — this should NOT create any gold tables/views
        results = gold_views.run_gold_views(db_path=":memory:", dry_run=True)

        # Verify all files reported success
        assert all(results.values())

        # Verify SQL was printed to stdout
        captured = capsys.readouterr()
        assert "CREATE OR REPLACE" in captured.out

        # Verify no gold tables/views were created in the test database
        tables = gold_db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'"
        ).fetchall()
        assert (
            len(tables) == 0
        ), f"Dry run should not create gold objects, but found: {tables}"
