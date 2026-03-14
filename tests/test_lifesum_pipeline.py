"""Tests for Lifesum data pipeline: CSV -> parquet -> bronze -> silver -> gold.

Validates that the Lifesum pipeline correctly processes food, exercise,
weighins, and bodyfat data through all layers.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from health_platform.utils.paths import get_db_path

# Mark all tests as integration (require DuckDB with data)
pytestmark = pytest.mark.integration

PARQUET_BASE = Path("/Users/Shared/data_lake/lifesum/parquet")


@pytest.fixture(scope="module")
def con():
    """Read-only DuckDB connection for verification queries."""
    db_path = get_db_path()
    if not db_path.exists():
        pytest.skip(f"Database not found: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)
    yield conn
    conn.close()


class TestLifesumParquet:
    """Verify parquet files exist for all active Lifesum sources."""

    @pytest.mark.parametrize("source", ["exercise", "weighins", "bodyfat"])
    def test_parquet_exists(self, source: str):
        parquet_path = PARQUET_BASE / source / "data.parquet"
        assert parquet_path.exists(), f"Missing parquet: {parquet_path}"
        assert parquet_path.stat().st_size > 0, f"Empty parquet: {parquet_path}"

    def test_food_parquet_exists(self):
        """Food comes from PDF pipeline — filename is pdf_{stem}.parquet."""
        food_dir = PARQUET_BASE / "food"
        assert food_dir.exists(), f"Missing directory: {food_dir}"
        parquet_files = list(food_dir.glob("pdf_*.parquet"))
        assert len(parquet_files) > 0, f"No pdf_*.parquet files in {food_dir}"
        for pf in parquet_files:
            assert pf.stat().st_size > 0, f"Empty parquet: {pf}"

    def test_bodymeasures_parquet_not_created(self):
        """bodymeasures was dropped from pipeline — parquet should not exist."""
        path = PARQUET_BASE / "bodymeasures" / "data.parquet"
        assert not path.exists(), "bodymeasures parquet should not exist (dropped)"


class TestLifesumBronze:
    """Verify bronze staging tables have rows for all active Lifesum sources."""

    @pytest.mark.parametrize(
        "table",
        [
            "bronze.stg_lifesum_food",
            "bronze.stg_lifesum_exercise",
            "bronze.stg_lifesum_weighins",
            "bronze.stg_lifesum_bodyfat",
        ],
    )
    def test_bronze_table_has_rows(self, con, table: str):
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count > 0, f"{table} is empty"

    def test_bronze_bodymeasures_not_exists(self, con):
        """stg_lifesum_bodymeasures should not exist (dropped from config)."""
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'bronze' AND table_name = 'stg_lifesum_bodymeasures'"
        ).fetchall()
        assert len(tables) == 0, "stg_lifesum_bodymeasures should not exist"


class TestLifesumSilver:
    """Verify silver tables contain Lifesum-sourced rows."""

    def test_daily_meal_has_rows(self, con):
        count = con.execute("SELECT COUNT(*) FROM silver.daily_meal").fetchone()[0]
        assert count > 0, "silver.daily_meal is empty"

    def test_workout_has_lifesum_rows(self, con):
        count = con.execute(
            "SELECT COUNT(*) FROM silver.workout WHERE source = 'lifesum'"
        ).fetchone()[0]
        assert count > 0, "silver.workout has no Lifesum rows"

    def test_weight_has_rows(self, con):
        count = con.execute("SELECT COUNT(*) FROM silver.weight").fetchone()[0]
        assert count > 0, "silver.weight is empty"

    def test_body_fat_has_rows(self, con):
        count = con.execute("SELECT COUNT(*) FROM silver.body_fat").fetchone()[0]
        assert count > 0, "silver.body_fat is empty"


class TestLifesumSilverNulls:
    """Verify no NULL business keys in Lifesum silver data."""

    def test_workout_no_null_keys(self, con):
        nulls = con.execute(
            "SELECT COUNT(*) FROM silver.workout "
            "WHERE source = 'lifesum' AND (day IS NULL OR business_key_hash IS NULL)"
        ).fetchone()[0]
        assert nulls == 0, f"Found {nulls} NULL business keys in silver.workout"

    def test_weight_no_null_keys(self, con):
        nulls = con.execute(
            "SELECT COUNT(*) FROM silver.weight WHERE datetime IS NULL OR business_key_hash IS NULL"
        ).fetchone()[0]
        assert nulls == 0, f"Found {nulls} NULL business keys in silver.weight"

    def test_body_fat_no_null_keys(self, con):
        nulls = con.execute(
            "SELECT COUNT(*) FROM silver.body_fat WHERE day IS NULL OR business_key_hash IS NULL"
        ).fetchone()[0]
        assert nulls == 0, f"Found {nulls} NULL business keys in silver.body_fat"


class TestLifesumGold:
    """Verify gold layer has nutrition data (sourced from Lifesum food)."""

    def test_fct_daily_nutrition_has_data(self, con):
        try:
            count = con.execute(
                "SELECT COUNT(*) FROM gold.fct_daily_nutrition"
            ).fetchone()[0]
            assert count > 0, "gold.fct_daily_nutrition is empty"
        except duckdb.CatalogException:
            pytest.skip("gold.fct_daily_nutrition does not exist yet")
