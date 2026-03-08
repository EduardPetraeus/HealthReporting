"""Tests for shared silver tables with multi-source data (A6+A7).

Validates tables that receive data from both Oura and Withings:
- sleep_session (Oura CSV + Withings)
- blood_pressure_v2 (Apple Health + Withings)
- body_measurement (Apple Health + Withings)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))
from health_platform.utils.paths import get_db_path  # noqa: E402


@pytest.fixture(scope="module")
def db():
    """Read-only connection to the dev DuckDB database."""
    con = duckdb.connect(str(get_db_path()), read_only=True)
    yield con
    con.close()


class TestSleepSession:
    """Validate the shared sleep_session table (Oura + Withings)."""

    def test_has_two_sources(self, db):
        sources = db.execute(
            "SELECT DISTINCT source_name FROM silver.sleep_session ORDER BY 1"
        ).fetchall()
        source_list = [s[0] for s in sources]
        assert (
            "oura" in source_list
        ), f"Missing oura in sleep_session sources: {source_list}"
        assert (
            "withings" in source_list
        ), f"Missing withings in sleep_session sources: {source_list}"

    def test_no_dup_bk(self, db):
        dup_count = db.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT business_key_hash FROM silver.sleep_session "
            "  GROUP BY 1 HAVING COUNT(*) > 1"
            ")"
        ).fetchone()[0]
        assert (
            dup_count == 0
        ), f"sleep_session has {dup_count} duplicate business_key_hash groups"

    def test_total_sleep_range(self, db):
        out_of_range = db.execute(
            "SELECT COUNT(*) FROM silver.sleep_session "
            "WHERE total_sleep_min < 0 OR total_sleep_min > 1440"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"{out_of_range} total_sleep_min values outside 0-1440 range"

    def test_composition_check(self, db):
        """Verify sleep composition sums are reasonable (not exceeding total)."""
        invalid = db.execute(
            "SELECT COUNT(*) FROM silver.sleep_session "
            "WHERE total_sleep_min IS NOT NULL "
            "  AND deep_sleep_min IS NOT NULL "
            "  AND rem_sleep_min IS NOT NULL "
            "  AND light_sleep_min IS NOT NULL "
            "  AND (deep_sleep_min + rem_sleep_min + light_sleep_min) > total_sleep_min * 1.1"
        ).fetchone()[0]
        assert (
            invalid == 0
        ), f"{invalid} rows where sleep stage sum exceeds total by >10%"

    def test_oura_has_readiness(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.sleep_session "
            "WHERE source_name = 'oura' AND readiness_score IS NOT NULL"
        ).fetchone()[0]
        assert count > 0, "No Oura rows with readiness_score"

    def test_withings_has_snoring(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.sleep_session "
            "WHERE source_name = 'withings' AND snoring_min IS NOT NULL"
        ).fetchone()[0]
        assert count > 0, "No Withings rows with snoring_min"

    def test_no_cross_source_bk_overlap(self, db):
        """Business keys should not overlap between sources."""
        overlap = db.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT business_key_hash FROM silver.sleep_session "
            "  GROUP BY business_key_hash "
            "  HAVING COUNT(DISTINCT source_name) > 1"
            ")"
        ).fetchone()[0]
        assert overlap == 0, f"{overlap} business keys shared across sources"


class TestBloodPressureV2:
    """Validate the shared blood_pressure_v2 table (Apple Health + Withings)."""

    def test_has_two_sources(self, db):
        sources = db.execute(
            "SELECT DISTINCT source_name FROM silver.blood_pressure_v2 ORDER BY 1"
        ).fetchall()
        source_list = [s[0] for s in sources]
        assert len(source_list) >= 2, f"Expected 2+ sources, got {source_list}"

    def test_withings_has_pulse(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.blood_pressure_v2 "
            "WHERE source_name = 'withings' AND pulse_bpm IS NOT NULL"
        ).fetchone()[0]
        assert count > 0, "No Withings rows with pulse_bpm"

    def test_apple_pulse_null(self, db):
        non_null = db.execute(
            "SELECT COUNT(*) FROM silver.blood_pressure_v2 "
            "WHERE source_name = 'apple_health' AND pulse_bpm IS NOT NULL"
        ).fetchone()[0]
        assert (
            non_null == 0
        ), f"Apple Health has {non_null} rows with non-null pulse_bpm (expected all NULL)"

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
    """Validate the shared body_measurement table (Apple Health + Withings)."""

    def test_has_rows(self, db):
        count = db.execute("SELECT COUNT(*) FROM silver.body_measurement").fetchone()[0]
        assert count > 0, "silver.body_measurement is empty"

    def test_withings_has_fat_mass(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.body_measurement "
            "WHERE source_name = 'withings' AND fat_mass_kg IS NOT NULL"
        ).fetchone()[0]
        assert count > 0, "No Withings rows with fat_mass_kg"

    def test_withings_has_bone_mass(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.body_measurement "
            "WHERE source_name = 'withings' AND bone_mass_kg IS NOT NULL"
        ).fetchone()[0]
        assert count > 0, "No Withings rows with bone_mass_kg"

    def test_weight_range(self, db):
        out_of_range = db.execute(
            "SELECT COUNT(*) FROM silver.body_measurement "
            "WHERE weight_kg IS NOT NULL AND (weight_kg < 30 OR weight_kg > 200)"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"{out_of_range} weight values outside 30-200 kg range"

    def test_body_fat_pct_range(self, db):
        invalid = db.execute(
            "SELECT COUNT(*) FROM silver.body_measurement "
            "WHERE body_fat_pct IS NOT NULL AND (body_fat_pct <= 0 OR body_fat_pct > 60)"
        ).fetchone()[0]
        assert (
            invalid == 0
        ), f"{invalid} body_fat_pct values outside valid range (0-60%)"
