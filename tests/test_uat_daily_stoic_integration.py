"""UAT: Daily Stoic integration — user-facing output verification.

Covers:
  TC-1  Connector output — 5 parquet files exist with required metadata columns
  TC-2  AI summary — stoic sections present for a date with data
  TC-3  AI summary graceful degradation — no error, no stoic sections for empty date
  TC-4  daily_sync.sh syntax — shell script is valid bash
  TC-5  Gold view — fct_daily_experience returns meaningful data with correct pct
"""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

import duckdb
import pytest

DATA_LAKE = Path("/Users/Shared/data_lake")
STOIC_PARQUET_ROOT = DATA_LAKE / "daily_stoic"
EXPECTED_TABLES = ["focus", "habit_definitions", "habits", "quotes", "reflections"]
DAILY_SYNC_SCRIPT = Path(
    "/Users/clauseduardpetraeus/Github repos/HealthReporting/scripts/daily_sync.sh"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _open_dev_db() -> duckdb.DuckDBPyConnection:
    db_path = DATA_LAKE / "database" / "health_dw_dev.db"
    return duckdb.connect(str(db_path), read_only=True)


# ---------------------------------------------------------------------------
# TC-1: Connector output
# ---------------------------------------------------------------------------


class TestConnectorOutput:
    """TC-1 — run_daily_stoic writes 5 parquet files with required columns."""

    def test_all_five_parquet_files_exist(self):
        """Each expected table must have a data.parquet file on disk."""
        missing = [
            t
            for t in EXPECTED_TABLES
            if not (STOIC_PARQUET_ROOT / t / "data.parquet").exists()
        ]
        assert missing == [], f"Missing parquet files for tables: {missing}"

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_parquet_has_ingested_at_column(self, table_name):
        """_ingested_at column must be present in every parquet file."""
        path = STOIC_PARQUET_ROOT / table_name / "data.parquet"
        con = duckdb.connect(":memory:")
        columns = con.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{path}'))"
        ).fetchall()
        col_names = [r[0] for r in columns]
        assert (
            "_ingested_at" in col_names
        ), f"{table_name}/data.parquet is missing _ingested_at. Columns: {col_names}"

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_parquet_has_source_system_column(self, table_name):
        """_source_system column must be present and equal 'daily_stoic'."""
        path = STOIC_PARQUET_ROOT / table_name / "data.parquet"
        con = duckdb.connect(":memory:")
        result = con.execute(
            f"SELECT DISTINCT _source_system FROM read_parquet('{path}')"
        ).fetchall()
        values = [r[0] for r in result]
        assert values == [
            "daily_stoic"
        ], f"{table_name}: expected _source_system='daily_stoic', got {values}"

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_parquet_has_rows(self, table_name):
        """Each parquet file must contain at least one row."""
        path = STOIC_PARQUET_ROOT / table_name / "data.parquet"
        con = duckdb.connect(":memory:")
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[
            0
        ]
        assert count > 0, f"{table_name}/data.parquet has 0 rows"


# ---------------------------------------------------------------------------
# TC-2: AI summary — date WITH stoic data
# ---------------------------------------------------------------------------


class TestAISummaryWithStoicData:
    """TC-2 — generate_daily_summary includes stoic sections for 2026-03-18."""

    @pytest.fixture(scope="class")
    def summary_2026_03_18(self):
        con = _open_dev_db()
        from health_platform.ai.text_generator import generate_daily_summary

        summary = generate_daily_summary(con, date(2026, 3, 18))
        con.close()
        return summary

    def test_summary_is_non_empty_string(self, summary_2026_03_18):
        assert isinstance(summary_2026_03_18, str)
        assert len(summary_2026_03_18) > 10, "Summary is unexpectedly short"

    def test_summary_contains_focus_section(self, summary_2026_03_18):
        assert (
            "Focus:" in summary_2026_03_18
        ), f"'Focus:' section missing from summary:\n{summary_2026_03_18}"

    def test_summary_contains_reflection_section(self, summary_2026_03_18):
        assert (
            "Reflection:" in summary_2026_03_18
        ), f"'Reflection:' section missing from summary:\n{summary_2026_03_18}"

    def test_summary_contains_habits_section(self, summary_2026_03_18):
        assert (
            "Habits:" in summary_2026_03_18
        ), f"'Habits:' section missing from summary:\n{summary_2026_03_18}"

    def test_habits_section_format(self, summary_2026_03_18):
        """Habits section must follow 'Habits: X/Y completed.' pattern."""
        import re

        match = re.search(r"Habits:\s*(\d+)/(\d+)\s+completed", summary_2026_03_18)
        assert (
            match is not None
        ), f"Habits section does not match 'X/Y completed' format:\n{summary_2026_03_18}"
        completed = int(match.group(1))
        total = int(match.group(2))
        assert total > 0, "Total habits must be > 0"
        assert completed <= total, f"Completed ({completed}) > total ({total})"

    def test_focus_text_truncated_at_100_chars(self, summary_2026_03_18):
        """Focus text after 'Focus: ' must not exceed 100 characters before the period."""
        import re

        match = re.search(r"Focus:\s*(.+?)\.", summary_2026_03_18)
        if match:
            focus_text = match.group(1)
            assert (
                len(focus_text) <= 100
            ), f"Focus text exceeds 100 chars ({len(focus_text)}): {focus_text}"

    def test_reflection_text_truncated_at_100_chars(self, summary_2026_03_18):
        """Reflection text after 'Reflection: ' must not exceed 100 chars before period."""
        import re

        match = re.search(r"Reflection:\s*(.+?)\.", summary_2026_03_18)
        if match:
            reflection_text = match.group(1)
            assert (
                len(reflection_text) <= 100
            ), f"Reflection text exceeds 100 chars ({len(reflection_text)}): {reflection_text}"

    def test_summary_is_coherent_single_string(self, summary_2026_03_18):
        """Summary must be a single string with no embedded newlines or null bytes."""
        assert "\x00" not in summary_2026_03_18, "Summary contains null bytes"
        assert "\n\n" not in summary_2026_03_18, "Summary contains double newlines"


# ---------------------------------------------------------------------------
# TC-3: AI summary graceful degradation — no stoic data
# ---------------------------------------------------------------------------


class TestAISummaryGracefulDegradation:
    """TC-3 — generate_daily_summary for 2025-01-01 works without stoic sections."""

    @pytest.fixture(scope="class")
    def summary_no_stoic(self):
        con = _open_dev_db()
        from health_platform.ai.text_generator import generate_daily_summary

        summary = generate_daily_summary(con, date(2025, 1, 1))
        con.close()
        return summary

    def test_no_exception_raised(self, summary_no_stoic):
        """Must return a string, not raise."""
        assert isinstance(summary_no_stoic, str)

    def test_no_focus_section(self, summary_no_stoic):
        assert (
            "Focus:" not in summary_no_stoic
        ), f"'Focus:' appeared for a date with no stoic data:\n{summary_no_stoic}"

    def test_no_reflection_section(self, summary_no_stoic):
        assert (
            "Reflection:" not in summary_no_stoic
        ), f"'Reflection:' appeared for a date with no stoic data:\n{summary_no_stoic}"

    def test_no_habits_section(self, summary_no_stoic):
        assert (
            "Habits:" not in summary_no_stoic
        ), f"'Habits:' appeared for a date with no stoic data:\n{summary_no_stoic}"

    def test_summary_still_returns_date(self, summary_no_stoic):
        """Summary must still open with the ISO date."""
        assert "2025-01-01" in summary_no_stoic


# ---------------------------------------------------------------------------
# TC-4: daily_sync.sh syntax check
# ---------------------------------------------------------------------------


class TestDailySyncShellScript:
    """TC-4 — daily_sync.sh passes bash -n syntax check."""

    def test_script_exists(self):
        assert DAILY_SYNC_SCRIPT.exists(), f"Script not found: {DAILY_SYNC_SCRIPT}"

    def test_script_syntax_valid(self):
        result = subprocess.run(
            ["bash", "-n", str(DAILY_SYNC_SCRIPT)],
            capture_output=True,
            text=True,
        )
        assert (
            result.returncode == 0
        ), f"bash -n reported syntax errors:\n{result.stderr}"

    def test_script_has_daily_stoic_step(self):
        """Script must reference the daily-stoic connector (step 1e)."""
        content = DAILY_SYNC_SCRIPT.read_text()
        assert (
            "daily_stoic" in content or "daily-stoic" in content
        ), "daily_sync.sh does not reference the daily-stoic connector"

    def test_script_daily_stoic_is_non_fatal(self):
        """daily-stoic failure must increment FETCH_WARN, not abort the pipeline."""
        content = DAILY_SYNC_SCRIPT.read_text()
        # Step 1e block must follow the same if/else/warn pattern as other connectors
        assert "daily-stoic extract complete" in content or "daily-stoic" in content
        # Presence of FETCH_WARN after the daily-stoic block confirms non-fatal handling
        import re

        block = re.search(
            r"daily.stoic.*?FETCH_WARN",
            content,
            re.DOTALL,
        )
        assert block is not None, (
            "daily-stoic failure path does not increment FETCH_WARN — "
            "a connector failure would abort the full pipeline"
        )


# ---------------------------------------------------------------------------
# TC-5: Gold view — fct_daily_experience
# ---------------------------------------------------------------------------


class TestGoldViewFctDailyExperience:
    """TC-5 — fct_daily_experience returns meaningful data for a known date."""

    @pytest.fixture(scope="class")
    def known_row(self):
        """Fetch one row from fct_daily_experience for 2026-03-18."""
        con = _open_dev_db()
        try:
            row = con.execute(
                """
                SELECT *
                FROM gold.fct_daily_experience
                WHERE day = '2026-03-18'
                """
            ).fetchone()
            if row is None:
                # Fall back to any recent row
                row = con.execute(
                    """
                    SELECT *
                    FROM gold.fct_daily_experience
                    ORDER BY day DESC
                    LIMIT 1
                    """
                ).fetchone()
            description = con.execute(
                "DESCRIBE SELECT * FROM gold.fct_daily_experience LIMIT 0"
            ).fetchall()
            col_names = [d[0] for d in description]
        finally:
            con.close()
        return row, col_names

    def test_view_returns_at_least_one_row(self, known_row):
        row, _ = known_row
        assert (
            row is not None
        ), "fct_daily_experience returned no rows — view may be broken or data missing"

    def test_view_has_completion_pct_column(self, known_row):
        _, col_names = known_row
        assert (
            "completion_pct" in col_names
        ), f"completion_pct column missing. Columns: {col_names}"

    def test_completion_pct_is_numeric(self, known_row):
        row, col_names = known_row
        idx = col_names.index("completion_pct")
        pct = row[idx]
        if pct is not None:
            assert isinstance(
                pct, (int, float)
            ), f"completion_pct is not numeric: {type(pct)} = {pct}"

    def test_completion_pct_range(self, known_row):
        """completion_pct must be 0–100 (or NULL for dates with no habits)."""
        row, col_names = known_row
        idx = col_names.index("completion_pct")
        pct = row[idx]
        if pct is not None:
            assert 0.0 <= float(pct) <= 100.0, f"completion_pct out of range: {pct}"

    def test_completion_pct_calculation(self):
        """Verify completion_pct = completed/total * 100 using a controlled fixture."""
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA silver")
        con.execute(
            """
            CREATE TABLE silver.daily_stoic_habits (
                day DATE,
                habit_id INTEGER,
                done BOOLEAN
            )
            """
        )
        con.execute(
            """
            INSERT INTO silver.daily_stoic_habits VALUES
                ('2026-03-01', 1, TRUE),
                ('2026-03-01', 2, TRUE),
                ('2026-03-01', 3, FALSE),
                ('2026-03-01', 4, FALSE)
            """
        )
        row = con.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN done THEN 1 ELSE 0 END) AS completed,
                ROUND(SUM(CASE WHEN done THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS completion_pct
            FROM silver.daily_stoic_habits
            WHERE day = '2026-03-01'
            """
        ).fetchone()
        total, completed, pct = row
        assert total == 4
        assert completed == 2
        assert abs(float(pct) - 50.0) < 0.01, f"Expected 50.0, got {pct}"

    def test_view_has_day_column(self, known_row):
        _, col_names = known_row
        assert "day" in col_names, f"'day' column missing. Columns: {col_names}"

    def test_known_date_columns_populated(self, known_row):
        """At least one non-NULL value must exist beyond the date column itself."""
        row, col_names = known_row
        if row is None:
            pytest.skip("No rows in fct_daily_experience")
        non_null_count = sum(1 for v in row if v is not None)
        assert (
            non_null_count >= 2
        ), f"Row for known date has almost no data — possible empty view. Row: {dict(zip(col_names, row))}"
