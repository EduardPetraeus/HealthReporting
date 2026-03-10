"""Tests for daily summary text generation."""

from __future__ import annotations


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
