"""Tests for A3 Apple Health silver tables.

Validates all 15 new silver tables created in A3:
- Table exists and has rows
- No null business_key_hash
- No duplicate business_key_hash
- sk_date within valid range (20100101-20261231)
- source_name populated (shared tables only)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))
from health_platform.utils.paths import get_db_path  # noqa: E402

# Simple event tables: (table_name, value_column)
SIMPLE_EVENT_TABLES = [
    ("hrv", "hrv_ms"),
    ("hr_recovery", "recovery_bpm"),
    ("six_min_walk", "distance_m"),
    ("flights_climbed", "flights"),
    ("running_speed", "speed_m_per_s"),
    ("physical_effort", "effort_kj_per_hr_kg"),
    ("exercise_time", "exercise_minutes"),
]

# Event tables with duration
DURATION_EVENT_TABLES = [
    ("hand_washing", "duration_seconds", "DOUBLE"),
]

# Daily grain tables: (table_name,)
DAILY_TABLES = [
    "vo2_max",
    "resting_heart_rate",
    "stand_time",
    "audio_exposure",
    "body_measurement",
]

# Multi-source tables with distance_type or exposure_type
TYPED_TABLES = [
    ("distance", "distance_type"),
    ("audio_exposure", "exposure_type"),
]

# All 15 tables
ALL_TABLES = [
    "hrv",
    "hr_recovery",
    "vo2_max",
    "resting_heart_rate",
    "six_min_walk",
    "flights_climbed",
    "running_speed",
    "physical_effort",
    "hand_washing",
    "exercise_time",
    "stand_time",
    "distance",
    "audio_exposure",
    "body_measurement",
    "blood_pressure_v2",
]


@pytest.fixture(scope="module")
def db():
    """Read-only connection to the dev DuckDB database."""
    con = duckdb.connect(str(get_db_path()), read_only=True)
    yield con
    con.close()


class TestSilverTableExists:
    """Verify all 15 tables exist and have data."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_table_has_rows(self, db, table):
        count = db.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
        assert count > 0, f"silver.{table} is empty"


class TestBusinessKeyIntegrity:
    """Verify business key hash is populated and unique."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_no_null_business_key(self, db, table):
        null_count = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE business_key_hash IS NULL"
        ).fetchone()[0]
        assert (
            null_count == 0
        ), f"silver.{table} has {null_count} null business_key_hash"

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_no_duplicate_business_key(self, db, table):
        dup_count = db.execute(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT business_key_hash FROM silver.{table} "
            f"  GROUP BY 1 HAVING COUNT(*) > 1"
            f")"
        ).fetchone()[0]
        assert (
            dup_count == 0
        ), f"silver.{table} has {dup_count} duplicate business_key_hash groups"


class TestSkDateRange:
    """Verify sk_date values are within valid range."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_sk_date_in_range(self, db, table):
        out_of_range = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} "
            f"WHERE sk_date < 20100101 OR sk_date > 20261231"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"silver.{table} has {out_of_range} sk_date values out of range"


class TestSourceNamePopulated:
    """Verify source_name is populated in all tables."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_source_name_not_null(self, db, table):
        null_count = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE source_name IS NULL"
        ).fetchone()[0]
        assert null_count == 0, f"silver.{table} has {null_count} null source_name"


class TestSimpleEventColumns:
    """Verify value columns exist and have correct types in simple event tables."""

    @pytest.mark.parametrize("table,col", SIMPLE_EVENT_TABLES)
    def test_value_column_not_null(self, db, table, col):
        null_count = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE {col} IS NULL"
        ).fetchone()[0]
        total = db.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
        assert (
            null_count == 0
        ), f"silver.{table}.{col} has {null_count}/{total} null values"


class TestTypedTableDistinctTypes:
    """Verify multi-source tables have expected type columns."""

    @pytest.mark.parametrize("table,type_col", TYPED_TABLES)
    def test_has_multiple_types(self, db, table, type_col):
        types = db.execute(
            f"SELECT DISTINCT {type_col} FROM silver.{table} ORDER BY 1"
        ).fetchall()
        type_values = [t[0] for t in types]
        assert (
            len(type_values) >= 2
        ), f"silver.{table} expected multiple {type_col} values, got {type_values}"


class TestBloodPressureV2:
    """Verify blood_pressure_v2 has both systolic and diastolic."""

    def test_systolic_not_null(self, db):
        null_count = db.execute(
            "SELECT COUNT(*) FROM silver.blood_pressure_v2 WHERE systolic_mmhg IS NULL"
        ).fetchone()[0]
        assert null_count == 0

    def test_diastolic_not_null(self, db):
        null_count = db.execute(
            "SELECT COUNT(*) FROM silver.blood_pressure_v2 WHERE diastolic_mmhg IS NULL"
        ).fetchone()[0]
        assert null_count == 0

    def test_systolic_range(self, db):
        out_of_range = db.execute(
            "SELECT COUNT(*) FROM silver.blood_pressure_v2 "
            "WHERE systolic_mmhg < 60 OR systolic_mmhg > 250"
        ).fetchone()[0]
        assert out_of_range == 0, f"{out_of_range} systolic readings out of range"

    def test_diastolic_range(self, db):
        out_of_range = db.execute(
            "SELECT COUNT(*) FROM silver.blood_pressure_v2 "
            "WHERE diastolic_mmhg < 30 OR diastolic_mmhg > 150"
        ).fetchone()[0]
        assert out_of_range == 0, f"{out_of_range} diastolic readings out of range"


class TestBodyMeasurement:
    """Verify body_measurement has expected composition columns."""

    def test_has_weight_data(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.body_measurement WHERE weight_kg IS NOT NULL"
        ).fetchone()[0]
        assert count > 0, "No weight data in body_measurement"

    def test_has_multiple_sources(self, db):
        sources = db.execute(
            "SELECT COUNT(DISTINCT source_name) FROM silver.body_measurement"
        ).fetchone()[0]
        assert sources >= 2, f"Expected multiple sources, got {sources}"


class TestStandTime:
    """Verify stand_time daily aggregation."""

    def test_has_stand_minutes(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.stand_time WHERE stand_minutes IS NOT NULL"
        ).fetchone()[0]
        assert count > 0

    def test_has_stand_hours(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.stand_time WHERE stand_hours > 0"
        ).fetchone()[0]
        assert count > 0


class TestAudioExposure:
    """Verify audio_exposure daily aggregation."""

    def test_has_environmental(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.audio_exposure WHERE exposure_type = 'environmental'"
        ).fetchone()[0]
        assert count > 0

    def test_has_headphone(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.audio_exposure WHERE exposure_type = 'headphone'"
        ).fetchone()[0]
        assert count > 0

    def test_avg_db_positive(self, db):
        invalid = db.execute(
            "SELECT COUNT(*) FROM silver.audio_exposure WHERE avg_db <= 0"
        ).fetchone()[0]
        assert invalid == 0
