"""
Tests for DB-22 / DB-23 fixes:
  - state.clean_state()
  - state.update_state() (now persists immediately)
  - run_merge._extract_bronze_table()
  - run_merge main() guard: skip when bronze table missing or empty
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import duckdb

# -- path setup ----------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.source_connectors.oura.state import (  # noqa: E402
    clean_state,
    update_state,
)
from health_platform.transformation_logic.dbt.merge.run_merge import (  # noqa: E402
    _extract_bronze_table,
)

# ==============================================================================
# clean_state
# ==============================================================================


class TestCleanState:
    """clean_state(valid_endpoints, state) removes ghost keys and returns state."""

    def test_removes_ghost_key(self):
        state = {"daily_sleep": "2024-01-01", "old_endpoint": "2023-06-01"}
        result = clean_state(["daily_sleep"], state)
        assert "old_endpoint" not in result

    def test_keeps_valid_keys(self):
        state = {"daily_sleep": "2024-01-01", "heartrate": "2024-01-02"}
        result = clean_state(["daily_sleep", "heartrate"], state)
        assert result == {"daily_sleep": "2024-01-01", "heartrate": "2024-01-02"}

    def test_returns_same_dict_object(self):
        """clean_state mutates and returns the same dict, not a copy."""
        state = {"ghost": "2023-01-01"}
        result = clean_state([], state)
        assert result is state

    def test_empty_state_no_crash(self):
        result = clean_state(["daily_sleep"], {})
        assert result == {}

    def test_empty_valid_endpoints_removes_all(self):
        state = {"a": "2024-01-01", "b": "2024-01-02"}
        result = clean_state([], state)
        assert result == {}

    def test_multiple_ghosts_all_removed(self):
        state = {
            "daily_sleep": "2024-01-01",
            "ghost1": "2023-01-01",
            "ghost2": "2022-01-01",
        }
        result = clean_state(["daily_sleep"], state)
        assert result == {"daily_sleep": "2024-01-01"}

    def test_no_ghosts_state_unchanged(self):
        state = {"daily_sleep": "2024-01-01"}
        result = clean_state(["daily_sleep", "heartrate"], state)
        assert result == {"daily_sleep": "2024-01-01"}

    def test_preserves_value_strings(self):
        state = {"heartrate": "2024-03-14"}
        result = clean_state(["heartrate"], state)
        assert result["heartrate"] == "2024-03-14"


# ==============================================================================
# update_state
# ==============================================================================


class TestUpdateState:
    """update_state(endpoint, fetched_through, state) updates dict and persists."""

    def test_updates_in_memory_state(self, tmp_path):
        from datetime import date

        state = {}
        with patch(
            "health_platform.source_connectors.oura.state.STATE_FILE",
            tmp_path / "oura_state.json",
        ):
            update_state("daily_sleep", date(2024, 3, 14), state)

        assert state["daily_sleep"] == "2024-03-14"

    def test_persists_to_disk(self, tmp_path):
        from datetime import date

        state_file = tmp_path / "oura_state.json"
        state = {}
        with patch(
            "health_platform.source_connectors.oura.state.STATE_FILE",
            state_file,
        ):
            update_state("heartrate", date(2024, 3, 14), state)

        assert state_file.exists()
        persisted = json.loads(state_file.read_text())
        assert persisted["heartrate"] == "2024-03-14"

    def test_overwrites_existing_entry(self, tmp_path):
        from datetime import date

        state = {"daily_sleep": "2024-01-01"}
        with patch(
            "health_platform.source_connectors.oura.state.STATE_FILE",
            tmp_path / "oura_state.json",
        ):
            update_state("daily_sleep", date(2024, 3, 14), state)

        assert state["daily_sleep"] == "2024-03-14"

    def test_multiple_endpoints_persisted(self, tmp_path):
        from datetime import date

        state_file = tmp_path / "oura_state.json"
        state = {}
        with patch(
            "health_platform.source_connectors.oura.state.STATE_FILE",
            state_file,
        ):
            update_state("daily_sleep", date(2024, 3, 1), state)
            update_state("heartrate", date(2024, 3, 14), state)

        persisted = json.loads(state_file.read_text())
        assert persisted["daily_sleep"] == "2024-03-01"
        assert persisted["heartrate"] == "2024-03-14"

    def test_date_iso_format_stored(self, tmp_path):
        """Verify date is stored as YYYY-MM-DD string, not datetime object."""
        from datetime import date

        state = {}
        with patch(
            "health_platform.source_connectors.oura.state.STATE_FILE",
            tmp_path / "oura_state.json",
        ):
            update_state("workout", date(2024, 3, 14), state)

        assert isinstance(state["workout"], str)
        assert state["workout"] == "2024-03-14"


# ==============================================================================
# _extract_bronze_table
# ==============================================================================


class TestExtractBronzeTable:
    """_extract_bronze_table(sql) parses the FROM clause for bronze.stg_* tables."""

    def test_standard_merge_sql(self):
        sql = "SELECT * FROM bronze.stg_oura_daily_sleep WHERE day > '2024-01-01'"
        assert _extract_bronze_table(sql) == "bronze.stg_oura_daily_sleep"

    def test_case_insensitive_from(self):
        sql = "select * from bronze.stg_oura_heartrate limit 10"
        assert _extract_bronze_table(sql) == "bronze.stg_oura_heartrate"

    def test_returns_none_when_no_bronze_table(self):
        sql = "SELECT * FROM silver.oura_daily_sleep"
        assert _extract_bronze_table(sql) is None

    def test_returns_none_for_empty_string(self):
        assert _extract_bronze_table("") is None

    def test_returns_none_for_whitespace(self):
        assert _extract_bronze_table("   ") is None

    def test_multiline_sql(self):
        sql = """
        MERGE INTO silver.oura_daily_sleep AS target
        USING (
            SELECT * FROM bronze.stg_oura_daily_sleep
            WHERE day >= '2024-01-01'
        ) AS source ON target.day = source.day
        """
        assert _extract_bronze_table(sql) == "bronze.stg_oura_daily_sleep"

    def test_returns_first_match_only(self):
        """Regex returns first match — typically the source table."""
        sql = "SELECT a.* FROM bronze.stg_oura_daily_sleep a JOIN bronze.stg_oura_heartrate b ON 1=1"
        result = _extract_bronze_table(sql)
        assert result == "bronze.stg_oura_daily_sleep"

    def test_underscore_in_table_name(self):
        sql = "SELECT * FROM bronze.stg_apple_health_heart_rate"
        assert _extract_bronze_table(sql) == "bronze.stg_apple_health_heart_rate"

    def test_no_match_on_silver_table(self):
        sql = "SELECT * FROM silver.stg_oura_daily_sleep"
        assert _extract_bronze_table(sql) is None

    def test_no_false_positive_on_from_in_column_name(self):
        """FROM must be a word boundary — not inside identifiers."""
        sql = "SELECT from_date FROM silver.some_table"
        assert _extract_bronze_table(sql) is None


# ==============================================================================
# Guard logic: skip merge when bronze table missing or empty
# ==============================================================================


class TestMergeGuardLogic:
    """
    Tests the guard block in run_merge.main() that skips execution when the
    bronze source table does not exist or is empty.

    We test the guard behaviour directly via DuckDB + the extracted table name,
    mirroring exactly what main() does — without invoking main() itself
    (which requires a real SQL file and config on disk).
    """

    def _run_guard(self, con: duckdb.DuckDBPyConnection, bronze_table: str) -> str:
        """
        Reproduce the guard block from run_merge.main() and return a status string.
        Returns: "skipped_no_bronze" | "skipped_empty_bronze" | "would_run"
        """
        try:
            row_count = con.execute(f"SELECT count(*) FROM {bronze_table}").fetchone()[
                0
            ]
        except duckdb.CatalogException:
            return "skipped_no_bronze"
        if row_count == 0:
            return "skipped_empty_bronze"
        return "would_run"

    def test_missing_table_returns_skipped_no_bronze(self):
        con = duckdb.connect()
        result = self._run_guard(con, "bronze.stg_oura_daily_sleep")
        assert result == "skipped_no_bronze"

    def test_empty_table_returns_skipped_empty_bronze(self):
        con = duckdb.connect()
        con.execute("CREATE SCHEMA bronze")
        con.execute("CREATE TABLE bronze.stg_oura_daily_sleep (day DATE)")
        result = self._run_guard(con, "bronze.stg_oura_daily_sleep")
        assert result == "skipped_empty_bronze"

    def test_populated_table_returns_would_run(self):
        con = duckdb.connect()
        con.execute("CREATE SCHEMA bronze")
        con.execute("CREATE TABLE bronze.stg_oura_daily_sleep (day DATE)")
        con.execute("INSERT INTO bronze.stg_oura_daily_sleep VALUES ('2024-03-14')")
        result = self._run_guard(con, "bronze.stg_oura_daily_sleep")
        assert result == "would_run"

    def test_guard_not_triggered_when_no_bronze_table_in_sql(self):
        """
        When _extract_bronze_table returns None, main() skips the guard entirely.
        Simulate: bronze_table is None => guard block not entered => merge proceeds.
        """
        bronze_table = _extract_bronze_table("SELECT * FROM silver.some_table")
        assert bronze_table is None
        # No guard entry means merge would proceed — verified by None return value

    def test_multiple_rows_populate_check(self):
        con = duckdb.connect()
        con.execute("CREATE SCHEMA bronze")
        con.execute(
            "CREATE TABLE bronze.stg_oura_heartrate (ts TIMESTAMP, bpm INTEGER)"
        )
        con.execute(
            "INSERT INTO bronze.stg_oura_heartrate VALUES "
            "('2024-03-14 10:00:00', 65), ('2024-03-14 10:01:00', 66)"
        )
        result = self._run_guard(con, "bronze.stg_oura_heartrate")
        assert result == "would_run"
