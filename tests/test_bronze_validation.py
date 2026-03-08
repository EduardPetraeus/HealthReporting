"""Bronze layer validation tests.

Tests that verify bronze (staging) table creation, null rate calculations,
and schema checks using in-memory DuckDB with synthetic data only.
"""

from __future__ import annotations

import duckdb
import pytest


@pytest.fixture
def bronze_db():
    """In-memory DuckDB with bronze schema and synthetic staging tables."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    # Staging table for Oura sleep data (synthetic)
    con.execute(
        """
        CREATE TABLE bronze.stg_oura_sleep (
            id VARCHAR,
            day DATE,
            score INTEGER,
            deep_sleep_duration INTEGER,
            rem_sleep_duration INTEGER,
            light_sleep_duration INTEGER,
            total_sleep_duration INTEGER,
            bedtime_start TIMESTAMP,
            bedtime_end TIMESTAMP,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO bronze.stg_oura_sleep VALUES
            ('sleep_001', '2026-03-01', 82, 5400, 3600, 7200, 16200,
             '2026-03-01 23:00:00', '2026-03-02 06:30:00', CURRENT_TIMESTAMP,
             'oura_export_2026_03.json'),
            ('sleep_002', '2026-03-02', 75, 4800, 3200, 6800, 14800,
             '2026-03-02 23:30:00', '2026-03-03 07:00:00', CURRENT_TIMESTAMP,
             'oura_export_2026_03.json'),
            ('sleep_003', '2026-03-03', NULL, NULL, 3000, 7000, NULL,
             '2026-03-03 22:45:00', NULL, CURRENT_TIMESTAMP,
             'oura_export_2026_03.json')
        """
    )

    # Staging table for Apple Health heart rate (synthetic)
    con.execute(
        """
        CREATE TABLE bronze.stg_apple_health_heart_rate (
            value DOUBLE,
            unit VARCHAR,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            source_name VARCHAR,
            device VARCHAR,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO bronze.stg_apple_health_heart_rate VALUES
            (72.0, 'count/min', '2026-03-01 08:00:00', '2026-03-01 08:00:00',
             'Apple Watch', 'Watch7,2', CURRENT_TIMESTAMP, 'export.xml'),
            (NULL, 'count/min', '2026-03-01 09:00:00', '2026-03-01 09:00:00',
             'Apple Watch', NULL, CURRENT_TIMESTAMP, 'export.xml'),
            (85.0, 'count/min', '2026-03-01 12:00:00', '2026-03-01 12:00:00',
             'Apple Watch', 'Watch7,2', CURRENT_TIMESTAMP, 'export.xml'),
            (65.0, NULL, '2026-03-01 22:00:00', '2026-03-01 22:00:00',
             NULL, 'Watch7,2', CURRENT_TIMESTAMP, 'export.xml')
        """
    )

    # Empty staging table (for edge case testing)
    con.execute(
        """
        CREATE TABLE bronze.stg_empty_source (
            id VARCHAR,
            value DOUBLE,
            load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    yield con
    con.close()


class TestBronzeTableCreation:
    """Test that bronze staging tables can be created and queried."""

    def test_stg_oura_sleep_exists(self, bronze_db):
        result = bronze_db.execute(
            "SELECT COUNT(*) FROM bronze.stg_oura_sleep"
        ).fetchone()
        assert result[0] == 3

    def test_stg_apple_health_hr_exists(self, bronze_db):
        result = bronze_db.execute(
            "SELECT COUNT(*) FROM bronze.stg_apple_health_heart_rate"
        ).fetchone()
        assert result[0] == 4

    def test_stg_empty_source_queryable(self, bronze_db):
        result = bronze_db.execute(
            "SELECT COUNT(*) FROM bronze.stg_empty_source"
        ).fetchone()
        assert result[0] == 0

    def test_column_types_preserved(self, bronze_db):
        """Verify that column data types match expected bronze schema."""
        rows = bronze_db.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'stg_oura_sleep'
            ORDER BY ordinal_position
            """
        ).fetchall()
        col_map = {row[0]: row[1] for row in rows}
        assert col_map["id"] == "VARCHAR"
        assert col_map["day"] == "DATE"
        assert col_map["score"] == "INTEGER"
        assert col_map["load_datetime"] == "TIMESTAMP"


class TestNullRateCalculations:
    """Test null rate calculations on synthetic bronze data."""

    def test_null_rate_oura_sleep(self, bronze_db):
        """Calculate null rates for each column in stg_oura_sleep."""
        result = bronze_db.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN score IS NULL THEN 1 ELSE 0 END)
                    AS score_nulls,
                SUM(CASE WHEN deep_sleep_duration IS NULL THEN 1 ELSE 0 END)
                    AS deep_sleep_nulls,
                SUM(CASE WHEN bedtime_end IS NULL THEN 1 ELSE 0 END)
                    AS bedtime_end_nulls
            FROM bronze.stg_oura_sleep
            """
        ).fetchone()
        total_rows, score_nulls, deep_nulls, end_nulls = result
        assert total_rows == 3
        # One row has NULL score and deep_sleep_duration
        assert score_nulls == 1
        assert deep_nulls == 1
        assert end_nulls == 1

    def test_null_rate_apple_hr(self, bronze_db):
        """Calculate null rates for heart rate staging table."""
        result = bronze_db.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                ROUND(
                    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END)
                    * 100.0 / COUNT(*), 1
                ) AS value_null_pct,
                ROUND(
                    SUM(CASE WHEN source_name IS NULL THEN 1 ELSE 0 END)
                    * 100.0 / COUNT(*), 1
                ) AS source_null_pct
            FROM bronze.stg_apple_health_heart_rate
            """
        ).fetchone()
        total_rows, value_null_pct, source_null_pct = result
        assert total_rows == 4
        assert value_null_pct == 25.0  # 1 out of 4 rows
        assert source_null_pct == 25.0  # 1 out of 4 rows

    def test_null_rate_empty_table(self, bronze_db):
        """Null rate calculation on empty table should not fail."""
        result = bronze_db.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COALESCE(SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END), 0) AS null_count
            FROM bronze.stg_empty_source
            """
        ).fetchone()
        assert result[0] == 0
        assert result[1] == 0


class TestBronzeSchemaChecks:
    """Test schema validation on bronze staging tables."""

    def test_expected_columns_present(self, bronze_db):
        """Verify stg_oura_sleep has all expected columns."""
        rows = bronze_db.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'stg_oura_sleep'
            """
        ).fetchall()
        actual_columns = {row[0] for row in rows}
        expected_columns = {
            "id",
            "day",
            "score",
            "deep_sleep_duration",
            "rem_sleep_duration",
            "light_sleep_duration",
            "total_sleep_duration",
            "bedtime_start",
            "bedtime_end",
            "load_datetime",
            "source_file",
        }
        assert expected_columns.issubset(actual_columns)

    def test_no_unexpected_columns(self, bronze_db):
        """Verify stg_oura_sleep has no extra unknown columns."""
        rows = bronze_db.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'stg_oura_sleep'
            """
        ).fetchall()
        actual_columns = {row[0] for row in rows}
        known_columns = {
            "id",
            "day",
            "score",
            "deep_sleep_duration",
            "rem_sleep_duration",
            "light_sleep_duration",
            "total_sleep_duration",
            "bedtime_start",
            "bedtime_end",
            "load_datetime",
            "source_file",
        }
        extra = actual_columns - known_columns
        assert not extra, f"Unexpected columns: {extra}"

    def test_load_datetime_always_populated(self, bronze_db):
        """load_datetime must never be NULL in bronze tables."""
        for table in ["stg_oura_sleep", "stg_apple_health_heart_rate"]:
            result = bronze_db.execute(
                f"""
                SELECT COUNT(*)
                FROM bronze.{table}
                WHERE load_datetime IS NULL
                """
            ).fetchone()
            assert result[0] == 0, f"NULL load_datetime in bronze.{table}"

    def test_schema_drift_detection(self, bronze_db):
        """Simulate schema drift by checking for a column that should not exist."""
        rows = bronze_db.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'stg_oura_sleep'
              AND column_name = 'nonexistent_column'
            """
        ).fetchall()
        assert len(rows) == 0, "Unexpected column 'nonexistent_column' detected"
