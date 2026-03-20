"""Tests for the Daily Stoic source connector.

Covers _extract_table and _write_parquet with synthetic data only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)

from health_platform.source_connectors.daily_stoic.run_daily_stoic import (  # noqa: E402
    _add_metadata,
    _extract_table,
    _write_parquet,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_con_with_table(ddl: str, rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with one table seeded from rows."""
    con = duckdb.connect(":memory:")
    con.execute(ddl)
    for row in rows:
        placeholders = ", ".join(["?"] * len(row))
        table_name = ddl.split("TABLE")[1].split("(")[0].strip()
        con.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", list(row))
    return con


# ---------------------------------------------------------------------------
# _extract_table
# ---------------------------------------------------------------------------


class TestExtractTable:
    """Unit tests for _extract_table."""

    def test_happy_path_returns_all_rows(self):
        """Extract returns the correct number of rows."""
        con = _make_con_with_table(
            "CREATE TABLE habits (id INTEGER, name VARCHAR, done BOOLEAN)",
            [(1, "exercise", True), (2, "meditate", False), (3, "read", True)],
        )
        df = _extract_table(con, "habits")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        con.close()

    def test_happy_path_columns_preserved(self):
        """Extract preserves column names exactly."""
        con = _make_con_with_table(
            "CREATE TABLE focus (id INTEGER, focus_text VARCHAR, day DATE)",
            [(1, "Stoic resilience", "2026-03-01")],
        )
        df = _extract_table(con, "focus")
        assert list(df.columns) == ["id", "focus_text", "day"]
        con.close()

    def test_empty_table_returns_empty_dataframe(self):
        """Extract from empty table returns an empty DataFrame with correct schema."""
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE quotes (id INTEGER, text VARCHAR, author VARCHAR)")
        df = _extract_table(con, "quotes")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["id", "text", "author"]
        con.close()

    def test_none_values_in_nullable_columns(self):
        """Extract preserves NULL values as None/NaN in the DataFrame."""
        con = _make_con_with_table(
            "CREATE TABLE reflections (id INTEGER, text VARCHAR)",
            [(1, None), (2, "Today was productive")],
        )
        df = _extract_table(con, "reflections")
        assert len(df) == 2
        # First row text should be None/NaN
        assert df.iloc[0]["text"] is None or pd.isna(df.iloc[0]["text"])
        con.close()

    def test_single_row_table(self):
        """Extract from a single-row table returns one-row DataFrame."""
        con = _make_con_with_table(
            "CREATE TABLE habit_definitions (id INTEGER, name VARCHAR)",
            [(1, "daily_walk")],
        )
        df = _extract_table(con, "habit_definitions")
        assert len(df) == 1
        assert df.iloc[0]["name"] == "daily_walk"
        con.close()

    def test_missing_table_raises(self):
        """Extract raises when the table does not exist."""
        con = duckdb.connect(":memory:")
        with pytest.raises(Exception):
            _extract_table(con, "nonexistent_table")
        con.close()

    def test_numeric_types_preserved(self):
        """Extract keeps integer and float column types intact."""
        con = _make_con_with_table(
            "CREATE TABLE habits (id INTEGER, streak INTEGER, score DOUBLE)",
            [(1, 7, 98.5), (2, 3, 72.0)],
        )
        df = _extract_table(con, "habits")
        assert df["id"].dtype.kind in ("i", "u")  # integer family
        assert df["score"].dtype.kind == "f"  # float
        con.close()


# ---------------------------------------------------------------------------
# _add_metadata
# ---------------------------------------------------------------------------


class TestAddMetadata:
    """Unit tests for _add_metadata."""

    def test_adds_ingested_at_column(self):
        """_add_metadata adds _ingested_at column to DataFrame."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        result = _add_metadata(df)
        assert "_ingested_at" in result.columns

    def test_adds_source_system_column(self):
        """_add_metadata adds _source_system column with correct value."""
        df = pd.DataFrame({"id": [1]})
        result = _add_metadata(df)
        assert "_source_system" in result.columns
        assert result.iloc[0]["_source_system"] == "daily_stoic"

    def test_preserves_existing_columns(self):
        """_add_metadata does not drop original columns."""
        df = pd.DataFrame({"id": [1], "focus_text": ["Discipline"]})
        result = _add_metadata(df)
        assert "id" in result.columns
        assert "focus_text" in result.columns

    def test_empty_dataframe_still_gets_metadata_columns(self):
        """_add_metadata works on empty DataFrames."""
        df = pd.DataFrame({"id": pd.Series([], dtype=int)})
        result = _add_metadata(df)
        assert "_ingested_at" in result.columns
        assert "_source_system" in result.columns


# ---------------------------------------------------------------------------
# _write_parquet
# ---------------------------------------------------------------------------


class TestWriteParquet:
    """Unit tests for _write_parquet — uses tmp_path for filesystem isolation."""

    def test_writes_file_to_expected_location(self, tmp_path, monkeypatch):
        """_write_parquet creates a parquet file at the computed output path."""
        import pyarrow.parquet as pq
        from health_platform.source_connectors.daily_stoic import run_daily_stoic

        monkeypatch.setattr(
            run_daily_stoic,
            "_output_path",
            lambda table_name: tmp_path / table_name / "data.parquet",
        )

        df = pd.DataFrame({"id": [1, 2], "text": ["a", "b"]})
        _write_parquet(df, "focus")

        out = tmp_path / "focus" / "data.parquet"
        assert out.exists()
        loaded = pq.read_table(out).to_pandas()
        assert len(loaded) == 2

    def test_empty_dataframe_writes_valid_parquet(self, tmp_path, monkeypatch):
        """_write_parquet handles an empty DataFrame without raising."""
        import pyarrow.parquet as pq
        from health_platform.source_connectors.daily_stoic import run_daily_stoic

        monkeypatch.setattr(
            run_daily_stoic,
            "_output_path",
            lambda table_name: tmp_path / table_name / "data.parquet",
        )

        df = pd.DataFrame(
            {"id": pd.Series([], dtype=int), "text": pd.Series([], dtype=str)}
        )
        _write_parquet(df, "quotes")

        out = tmp_path / "quotes" / "data.parquet"
        assert out.exists()
        loaded = pq.read_table(out).to_pandas()
        assert len(loaded) == 0

    def test_none_values_round_trip(self, tmp_path, monkeypatch):
        """Parquet round-trip preserves None values in string columns."""
        import pyarrow.parquet as pq
        from health_platform.source_connectors.daily_stoic import run_daily_stoic

        monkeypatch.setattr(
            run_daily_stoic,
            "_output_path",
            lambda table_name: tmp_path / table_name / "data.parquet",
        )

        df = pd.DataFrame({"id": [1, 2], "text": [None, "stoic"]})
        _write_parquet(df, "reflections")

        out = tmp_path / "reflections" / "data.parquet"
        loaded = pq.read_table(out).to_pandas()
        assert loaded.iloc[0]["text"] is None or pd.isna(loaded.iloc[0]["text"])
        assert loaded.iloc[1]["text"] == "stoic"

    def test_creates_parent_directories(self, tmp_path, monkeypatch):
        """_write_parquet creates missing parent directories."""
        from health_platform.source_connectors.daily_stoic import run_daily_stoic

        nested = tmp_path / "deep" / "nested" / "path"
        monkeypatch.setattr(
            run_daily_stoic,
            "_output_path",
            lambda table_name: nested / table_name / "data.parquet",
        )

        df = pd.DataFrame({"id": [1]})
        _write_parquet(df, "habits")

        assert (nested / "habits" / "data.parquet").exists()
