"""Tests for A7 Withings silver tables.

Validates Withings-only silver tables:
- ecg_session (from Withings signal/ECG CSV)
- pulse_wave_velocity (from Withings PWV CSV)
- body_temperature feed check (Withings rows in shared table)

These are integration tests that require exclusive access to the DuckDB database.
Run with: pytest -m integration tests/test_silver_withings.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))
from health_platform.utils.paths import get_db_path  # noqa: E402

pytestmark = pytest.mark.integration

WITHINGS_TABLES = [
    "ecg_session",
    "pulse_wave_velocity",
]


@pytest.fixture(scope="module")
def db():
    """Read-only connection to the dev DuckDB database."""
    con = duckdb.connect(str(get_db_path()), read_only=True)
    yield con
    con.close()


class TestWithingsTableExists:
    """Verify Withings tables exist and have data."""

    @pytest.mark.parametrize("table", WITHINGS_TABLES)
    def test_table_has_rows(self, db, table):
        count = db.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
        assert count > 0, f"silver.{table} is empty"


class TestWithingsBusinessKeyIntegrity:
    """Verify business key hash is populated and unique."""

    @pytest.mark.parametrize("table", WITHINGS_TABLES)
    def test_no_null_bk(self, db, table):
        null_count = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE business_key_hash IS NULL"
        ).fetchone()[0]
        assert (
            null_count == 0
        ), f"silver.{table} has {null_count} null business_key_hash"

    @pytest.mark.parametrize("table", WITHINGS_TABLES)
    def test_no_dup_bk(self, db, table):
        dup_count = db.execute(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT business_key_hash FROM silver.{table} "
            f"  GROUP BY 1 HAVING COUNT(*) > 1"
            f")"
        ).fetchone()[0]
        assert (
            dup_count == 0
        ), f"silver.{table} has {dup_count} duplicate business_key_hash groups"


class TestWithingsSkDateRange:
    """Verify sk_date values are within valid range."""

    @pytest.mark.parametrize("table", WITHINGS_TABLES)
    def test_sk_date_in_range(self, db, table):
        out_of_range = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE sk_date < 20100101 OR sk_date > 20261231"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"silver.{table} has {out_of_range} sk_date values out of range"


class TestPulseWaveVelocity:
    """Domain-specific validation for pulse_wave_velocity."""

    def test_pwv_range(self, db):
        out_of_range = db.execute(
            "SELECT COUNT(*) FROM silver.pulse_wave_velocity "
            "WHERE pwv_m_per_s < 0.1 OR pwv_m_per_s > 30"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"{out_of_range} pwv_m_per_s values outside 0.1-30 range"


class TestEcgSession:
    """Domain-specific validation for ecg_session."""

    def test_no_signal_column(self, db):
        """Verify the raw signal column is not in the silver table."""
        columns = db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'silver' AND table_name = 'ecg_session'"
        ).fetchall()
        col_names = [c[0] for c in columns]
        assert (
            "signal" not in col_names
        ), "Raw signal column should not be in silver.ecg_session"


class TestWithingsBodyTemperatureFeed:
    """Verify Withings data flows into the shared body_temperature table."""

    def test_withings_body_temp_rows(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM silver.body_temperature WHERE source_name = 'withings'"
        ).fetchone()[0]
        assert count > 0, "No Withings rows in silver.body_temperature"
