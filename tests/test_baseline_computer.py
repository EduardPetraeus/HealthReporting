"""Tests for baseline computer."""

from __future__ import annotations


class TestBaselineComputer:
    """Test baseline computation and profile management."""

    def _setup_agent_tables(self, con):
        """Create agent.patient_profile table."""
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS agent.patient_profile (
                profile_key VARCHAR PRIMARY KEY,
                profile_value VARCHAR NOT NULL,
                numeric_value DOUBLE,
                category VARCHAR NOT NULL,
                description VARCHAR NOT NULL,
                computed_from VARCHAR,
                last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                update_frequency VARCHAR
            )
        """
        )

    def test_compute_demographics(self, seeded_db):
        """Demographics are extracted from personal_info."""
        self._setup_agent_tables(seeded_db)

        from health_platform.ai.baseline_computer import compute_demographics

        compute_demographics(seeded_db)

        rows = seeded_db.execute(
            "SELECT profile_key, profile_value FROM agent.patient_profile "
            "WHERE category = 'demographics' ORDER BY profile_key"
        ).fetchall()

        keys = [r[0] for r in rows]
        assert "age" in keys
        assert "biological_sex" in keys

    def test_compute_baselines(self, seeded_db):
        """Baselines are computed from silver data."""
        self._setup_agent_tables(seeded_db)

        from health_platform.ai.baseline_computer import compute_all_baselines

        compute_all_baselines(seeded_db)

        rows = seeded_db.execute(
            "SELECT profile_key, numeric_value FROM agent.patient_profile "
            "WHERE category = 'baselines'"
        ).fetchall()

        assert len(rows) >= 1  # At least some baselines computed

    def test_get_profile(self, seeded_db):
        """Profile retrieval works."""
        self._setup_agent_tables(seeded_db)

        from health_platform.ai.baseline_computer import (
            compute_demographics,
            get_profile,
        )

        compute_demographics(seeded_db)
        profile = get_profile(seeded_db)

        assert isinstance(profile, dict)
        assert len(profile) > 0

    def test_get_profile_filtered(self, seeded_db):
        """Profile retrieval with category filter."""
        self._setup_agent_tables(seeded_db)

        from health_platform.ai.baseline_computer import (
            compute_demographics,
            get_profile,
        )

        compute_demographics(seeded_db)
        profile = get_profile(seeded_db, categories=["demographics"])

        assert isinstance(profile, dict)
        assert all(
            v.get("category") == "demographics"
            for v in profile.values()
            if isinstance(v, dict)
        )

    def test_update_profile_entry(self, seeded_db):
        """Individual profile entries can be upserted."""
        self._setup_agent_tables(seeded_db)

        from health_platform.ai.baseline_computer import update_profile_entry

        update_profile_entry(
            seeded_db,
            key="test_metric",
            value="42",
            numeric=42.0,
            category="test",
            description="Test metric for unit testing",
            computed_from="test",
            frequency="manual",
        )

        row = seeded_db.execute(
            "SELECT profile_value, numeric_value FROM agent.patient_profile "
            "WHERE profile_key = 'test_metric'"
        ).fetchone()

        assert row is not None
        assert row[0] == "42"
        assert row[1] == 42.0
