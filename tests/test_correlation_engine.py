"""Tests for correlation engine."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)


class TestCorrelationEngine:
    """Test metric correlation computation."""

    def _setup_relationships_table(self, con):
        """Create silver.metric_relationships table."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS silver.metric_relationships (
                source_metric VARCHAR NOT NULL,
                target_metric VARCHAR NOT NULL,
                relationship_type VARCHAR NOT NULL,
                strength DOUBLE,
                lag_days INTEGER DEFAULT 0,
                direction VARCHAR,
                evidence_type VARCHAR,
                confidence DOUBLE,
                description VARCHAR,
                last_computed_at TIMESTAMP,
                PRIMARY KEY (source_metric, target_metric, relationship_type, lag_days)
            )
        """)

    def test_compute_correlation(self, seeded_db):
        """Compute Pearson correlation between two metrics."""
        from health_platform.ai.correlation_engine import compute_correlation

        result = compute_correlation(
            seeded_db,
            "daily_sleep.sleep_score",
            "daily_readiness.readiness_score",
            lag_days=0,
        )

        assert "strength" in result
        assert "direction" in result
        assert "sample_size" in result
        assert result["sample_size"] >= 2

    def test_correlation_with_lag(self, seeded_db):
        """Compute correlation with lag."""
        from health_platform.ai.correlation_engine import compute_correlation

        result = compute_correlation(
            seeded_db,
            "daily_sleep.sleep_score",
            "daily_readiness.readiness_score",
            lag_days=1,
        )

        assert "strength" in result

    def test_discover_correlations(self, seeded_db):
        """Discover correlations across multiple lags."""
        from health_platform.ai.correlation_engine import discover_correlations

        results = discover_correlations(
            seeded_db,
            "daily_sleep.sleep_score",
            "daily_readiness.readiness_score",
            max_lag=2,
        )

        assert isinstance(results, list)
        assert len(results) >= 1

    def test_compute_all_correlations(self, seeded_db):
        """Compute all predefined correlations."""
        self._setup_relationships_table(seeded_db)

        from health_platform.ai.correlation_engine import compute_all_correlations

        compute_all_correlations(seeded_db)

        count = seeded_db.execute(
            "SELECT COUNT(*) FROM silver.metric_relationships"
        ).fetchone()[0]
        assert count >= 1
