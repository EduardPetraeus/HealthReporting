"""Tests for daily summary text generation."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)


# ---------------------------------------------------------------------------
# Fixture: minimal DB with only the three daily-stoic silver tables
# ---------------------------------------------------------------------------


@pytest.fixture
def stoic_db():
    """In-memory DuckDB with silver schema and daily-stoic tables only."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    con.execute(
        """
        CREATE TABLE silver.daily_stoic_habits (
            day DATE,
            habit_id INTEGER,
            name VARCHAR,
            done BOOLEAN
        )
    """
    )
    con.execute(
        """
        CREATE TABLE silver.daily_stoic_focus (
            day DATE,
            focus_text VARCHAR
        )
    """
    )
    con.execute(
        """
        CREATE TABLE silver.daily_stoic_reflections (
            day DATE,
            reflection_text VARCHAR
        )
    """
    )
    yield con
    con.close()


class TestTextGenerator:
    """Test natural language summary generation."""

    def test_generate_daily_summary(self, seeded_db):
        """Generate summary for a day with complete data."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        summary = generate_daily_summary(seeded_db, date(2026, 3, 1))

        assert summary is not None
        assert len(summary) > 50
        assert "82" in summary or "sleep" in summary.lower()

    def test_generate_summary_missing_data(self, seeded_db):
        """Generate summary for a day with partial data."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        # March 2 has data but not weight
        summary = generate_daily_summary(seeded_db, date(2026, 3, 2))

        assert summary is not None
        assert len(summary) > 30

    def test_generate_summary_sparse_data(self, seeded_db):
        """Generate summary for a day with minimal/no data is still valid."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        summary = generate_daily_summary(seeded_db, date(2020, 1, 1))

        # Even with no data, the generator produces a minimal summary
        assert summary is not None
        assert isinstance(summary, str)

    def test_backfill_summaries(self, seeded_db):
        """Backfill creates rows in agent.daily_summaries."""
        from datetime import date

        # Create agent schema first
        seeded_db.execute(
            """
            CREATE TABLE IF NOT EXISTS agent.daily_summaries (
                day DATE PRIMARY KEY,
                sleep_score INTEGER, readiness_score INTEGER,
                steps INTEGER, resting_hr DOUBLE,
                stress_level VARCHAR,
                has_anomaly BOOLEAN DEFAULT FALSE,
                anomaly_metrics VARCHAR,
                summary_text VARCHAR NOT NULL,
                embedding FLOAT[384],
                data_completeness DOUBLE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        from health_platform.ai.text_generator import backfill_summaries

        backfill_summaries(
            seeded_db,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 3),
        )

        count = seeded_db.execute(
            "SELECT COUNT(*) FROM agent.daily_summaries"
        ).fetchone()[0]
        assert count >= 2  # At least days with data


class TestSummaryContent:
    """Test summary content quality."""

    def test_summary_contains_scores(self, seeded_db):
        """Summary mentions key scores."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        summary = generate_daily_summary(seeded_db, date(2026, 3, 1))

        # Should mention at least one score
        has_score_ref = any(
            word in summary.lower()
            for word in ["sleep", "readiness", "activity", "steps"]
        )
        assert has_score_ref, f"Summary lacks score references: {summary}"

    def test_summary_date_format(self, seeded_db):
        """Summary starts with or contains the date."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        summary = generate_daily_summary(seeded_db, date(2026, 3, 1))

        assert "2026-03-01" in summary or "2026" in summary


# ---------------------------------------------------------------------------
# _query_habits_summary — isolated unit tests
# ---------------------------------------------------------------------------


class TestQueryHabitsSummary:
    """Unit tests for _query_habits_summary."""

    def test_normal_case_some_habits_done(self, stoic_db):
        """Returns 'Habits: N/M completed.' when some habits are done."""
        from datetime import date

        from health_platform.ai.text_generator import _query_habits_summary

        stoic_db.execute(
            """
            INSERT INTO silver.daily_stoic_habits VALUES
                ('2026-03-01', 1, 'exercise', TRUE),
                ('2026-03-01', 2, 'meditate', TRUE),
                ('2026-03-01', 3, 'cold_shower', FALSE)
        """
        )
        result = _query_habits_summary(stoic_db, date(2026, 3, 1))
        assert result == "Habits: 2/3 completed."

    def test_all_habits_done(self, stoic_db):
        """Returns 'Habits: N/N completed.' when all habits are completed."""
        from datetime import date

        from health_platform.ai.text_generator import _query_habits_summary

        stoic_db.execute(
            """
            INSERT INTO silver.daily_stoic_habits VALUES
                ('2026-03-02', 1, 'read', TRUE),
                ('2026-03-02', 2, 'walk', TRUE)
        """
        )
        result = _query_habits_summary(stoic_db, date(2026, 3, 2))
        assert result == "Habits: 2/2 completed."

    def test_zero_habits_done(self, stoic_db):
        """Returns 'Habits: 0/N completed.' when no habits are done."""
        from datetime import date

        from health_platform.ai.text_generator import _query_habits_summary

        stoic_db.execute(
            """
            INSERT INTO silver.daily_stoic_habits VALUES
                ('2026-03-03', 1, 'exercise', FALSE),
                ('2026-03-03', 2, 'meditate', FALSE)
        """
        )
        result = _query_habits_summary(stoic_db, date(2026, 3, 3))
        assert result == "Habits: 0/2 completed."

    def test_no_rows_for_day_returns_none(self, stoic_db):
        """Returns None when there are no habit rows for the given day."""
        from datetime import date

        from health_platform.ai.text_generator import _query_habits_summary

        result = _query_habits_summary(stoic_db, date(2026, 3, 15))
        assert result is None

    def test_table_missing_returns_none(self):
        """Returns None gracefully when the habits table does not exist."""
        from datetime import date

        from health_platform.ai.text_generator import _query_habits_summary

        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        result = _query_habits_summary(con, date(2026, 3, 1))
        assert result is None
        con.close()

    def test_return_type_is_str_or_none(self, stoic_db):
        """Return type is always str or None, never raises."""
        from datetime import date

        from health_platform.ai.text_generator import _query_habits_summary

        stoic_db.execute(
            "INSERT INTO silver.daily_stoic_habits VALUES ('2026-03-04', 1, 'read', TRUE)"
        )
        result = _query_habits_summary(stoic_db, date(2026, 3, 4))
        assert isinstance(result, str)

        result_empty = _query_habits_summary(stoic_db, date(2020, 1, 1))
        assert result_empty is None


# ---------------------------------------------------------------------------
# Daily-stoic sections in generate_daily_summary
# ---------------------------------------------------------------------------


class TestDailyStoicSectionsInSummary:
    """Test the three new daily-stoic sections in generate_daily_summary."""

    def _base_db(self):
        """Minimal DB with just the three daily-stoic tables (no other silver data)."""
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        con.execute(
            "CREATE TABLE silver.daily_stoic_focus (day DATE, focus_text VARCHAR)"
        )
        con.execute(
            "CREATE TABLE silver.daily_stoic_reflections (day DATE, reflection_text VARCHAR)"
        )
        con.execute(
            """
            CREATE TABLE silver.daily_stoic_habits (
                day DATE, habit_id INTEGER, name VARCHAR, done BOOLEAN
            )
        """
        )
        return con

    def test_focus_text_appears_in_summary(self):
        """Summary includes focus text when present."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        con.execute(
            "INSERT INTO silver.daily_stoic_focus VALUES ('2026-03-01', 'Memento mori')"
        )
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Memento mori" in summary
        con.close()

    def test_focus_text_truncated_at_100_chars(self):
        """Focus text longer than 100 characters is truncated in the summary."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        # Use two distinct halves so truncation is unambiguous
        long_text = "X" * 100 + "Z" * 50
        con.execute(
            f"INSERT INTO silver.daily_stoic_focus VALUES ('2026-03-01', '{long_text}')"
        )
        summary = generate_daily_summary(con, date(2026, 3, 1))
        # First 100 chars (all X) must be present; the Z suffix must be absent
        assert "X" * 100 in summary
        assert "Z" not in summary
        con.close()

    def test_no_focus_row_omits_focus_section(self):
        """Summary does not contain 'Focus:' when there is no focus row."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Focus:" not in summary
        con.close()

    def test_empty_focus_text_omits_focus_section(self):
        """Empty string focus_text does not produce a Focus: section."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        con.execute("INSERT INTO silver.daily_stoic_focus VALUES ('2026-03-01', '')")
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Focus:" not in summary
        con.close()

    def test_null_focus_text_omits_focus_section(self):
        """NULL focus_text does not produce a Focus: section."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        con.execute("INSERT INTO silver.daily_stoic_focus VALUES ('2026-03-01', NULL)")
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Focus:" not in summary
        con.close()

    def test_reflection_text_appears_in_summary(self):
        """Summary includes reflection text when present."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        con.execute(
            "INSERT INTO silver.daily_stoic_reflections VALUES "
            "('2026-03-01', 'Today I practiced patience')"
        )
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Today I practiced patience" in summary
        con.close()

    def test_null_reflection_omits_reflection_section(self):
        """NULL reflection_text does not produce a Reflection: section."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        con.execute(
            "INSERT INTO silver.daily_stoic_reflections VALUES ('2026-03-01', NULL)"
        )
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Reflection:" not in summary
        con.close()

    def test_habits_section_appears_in_summary(self):
        """Summary includes Habits: section when habit rows exist."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        con.execute(
            """
            INSERT INTO silver.daily_stoic_habits VALUES
                ('2026-03-01', 1, 'exercise', TRUE),
                ('2026-03-01', 2, 'journal', FALSE)
        """
        )
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Habits: 1/2 completed." in summary
        con.close()

    def test_no_habits_omits_habits_section(self):
        """Summary omits Habits: when there are no habit rows for the day."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Habits:" not in summary
        con.close()

    def test_all_three_sections_present_together(self):
        """Summary includes all three daily-stoic sections when all data exists."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = self._base_db()
        con.execute(
            "INSERT INTO silver.daily_stoic_focus VALUES ('2026-03-01', 'Amor fati')"
        )
        con.execute(
            "INSERT INTO silver.daily_stoic_reflections VALUES "
            "('2026-03-01', 'Reflected on impermanence')"
        )
        con.execute(
            """
            INSERT INTO silver.daily_stoic_habits VALUES
                ('2026-03-01', 1, 'meditate', TRUE),
                ('2026-03-01', 2, 'read', TRUE),
                ('2026-03-01', 3, 'exercise', FALSE)
        """
        )
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert "Amor fati" in summary
        assert "Reflected on impermanence" in summary
        assert "Habits: 2/3 completed." in summary
        con.close()

    def test_stoic_sections_do_not_raise_when_tables_missing(self):
        """generate_daily_summary does not raise if stoic tables are absent."""
        from datetime import date

        from health_platform.ai.text_generator import generate_daily_summary

        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        # No stoic tables created
        summary = generate_daily_summary(con, date(2026, 3, 1))
        assert isinstance(summary, str)
        assert "Focus:" not in summary
        assert "Reflection:" not in summary
        assert "Habits:" not in summary
        con.close()
