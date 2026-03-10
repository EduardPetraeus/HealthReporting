"""Tests for A6 Oura CSV silver tables.

Validates 6 new silver tables created from Oura CSV exports:
- Table exists and has rows
- No null business_key_hash
- No duplicate business_key_hash
- sk_date within valid range (20100101-20261231)
- source_name is 'oura'
- Domain-specific validations (age range, temp range, sample counts)
"""

from __future__ import annotations

import duckdb
import pytest
from health_platform.utils.paths import get_db_path

pytestmark = pytest.mark.integration

OURA_CSV_TABLES = [
    "daily_resilience",
    "cardiovascular_age",
    "daytime_stress",
    "skin_temperature",
    "sleep_recommendation",
    "enhanced_tag",
]


@pytest.fixture(scope="module")
def db():
    """Read-only connection to the dev DuckDB database."""
    con = duckdb.connect(str(get_db_path()), read_only=True)
    yield con
    con.close()


class TestOuraCsvTableExists:
    """Verify all 6 Oura CSV tables exist and have data."""

    @pytest.mark.parametrize("table", OURA_CSV_TABLES)
    def test_table_has_rows(self, db, table):
        count = db.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
        assert count > 0, f"silver.{table} is empty"


class TestOuraCsvBusinessKeyIntegrity:
    """Verify business key hash is populated and unique."""

    @pytest.mark.parametrize("table", OURA_CSV_TABLES)
    def test_no_null_bk(self, db, table):
        null_count = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE business_key_hash IS NULL"
        ).fetchone()[0]
        assert (
            null_count == 0
        ), f"silver.{table} has {null_count} null business_key_hash"

    @pytest.mark.parametrize("table", OURA_CSV_TABLES)
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


class TestOuraCsvSkDateRange:
    """Verify sk_date values are within valid range."""

    @pytest.mark.parametrize("table", OURA_CSV_TABLES)
    def test_sk_date_in_range(self, db, table):
        out_of_range = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE sk_date < 20100101 OR sk_date > 20261231"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"silver.{table} has {out_of_range} sk_date values out of range"


class TestOuraCsvSourceName:
    """Verify source_name is 'oura' for all Oura CSV tables."""

    @pytest.mark.parametrize("table", OURA_CSV_TABLES)
    def test_source_is_oura(self, db, table):
        non_oura = db.execute(
            f"SELECT COUNT(*) FROM silver.{table} WHERE source_name != 'oura'"
        ).fetchone()[0]
        assert (
            non_oura == 0
        ), f"silver.{table} has {non_oura} rows with source_name != 'oura'"


class TestCardiovascularAge:
    """Domain-specific validation for cardiovascular_age."""

    def test_age_range(self, db):
        out_of_range = db.execute(
            "SELECT COUNT(*) FROM silver.cardiovascular_age "
            "WHERE vascular_age < 15 OR vascular_age > 100"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"{out_of_range} vascular_age values outside 15-100 range"


class TestSkinTemperature:
    """Domain-specific validation for skin_temperature."""

    def test_temp_range(self, db):
        out_of_range = db.execute(
            "SELECT COUNT(*) FROM silver.skin_temperature "
            "WHERE avg_skin_temp < 25 OR avg_skin_temp > 45"
        ).fetchone()[0]
        assert (
            out_of_range == 0
        ), f"{out_of_range} avg_skin_temp values outside 25-45 range"

    def test_sample_count_positive(self, db):
        invalid = db.execute(
            "SELECT COUNT(*) FROM silver.skin_temperature WHERE sample_count <= 0"
        ).fetchone()[0]
        assert invalid == 0, f"{invalid} rows with non-positive sample_count"


class TestDaytimeStress:
    """Domain-specific validation for daytime_stress."""

    def test_sample_count_positive(self, db):
        invalid = db.execute(
            "SELECT COUNT(*) FROM silver.daytime_stress WHERE sample_count <= 0"
        ).fetchone()[0]
        assert invalid == 0, f"{invalid} rows with non-positive sample_count"


class TestDailyResilience:
    """Domain-specific validation for daily_resilience."""

    def test_level_values(self, db):
        levels = db.execute(
            "SELECT DISTINCT level FROM silver.daily_resilience WHERE level IS NOT NULL ORDER BY 1"
        ).fetchall()
        level_values = [row[0] for row in levels]
        assert len(level_values) > 0, "No level values found in daily_resilience"
