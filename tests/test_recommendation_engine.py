"""Edge-case tests for recommendation_engine.py.

Happy-path tests live in test_intelligence_b4_c2.py. This file covers
only edge cases: null inputs, missing tables, boundary thresholds,
division-by-zero guards, and dataclass defaults.

All data is synthetic.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)

from health_platform.ai.recommendation_engine import (
    Recommendation,
    RecommendationEngine,
    format_recommendations,
)

# ---------------------------------------------------------------------------
# _get_avg edge cases
# ---------------------------------------------------------------------------


def test_get_avg_missing_table_returns_none(memory_db):
    """Query against a non-existent table must return None, not raise."""
    engine = RecommendationEngine(memory_db)
    result = engine._get_avg("silver.nonexistent_table", "col", 7)
    assert result is None


def test_get_avg_all_null_returns_none(memory_db):
    """AVG of a column containing only NULLs must return None."""
    memory_db.execute(
        "CREATE TABLE silver.all_null_sleep (day DATE, sleep_score INTEGER)"
    )
    for i in range(5):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.all_null_sleep VALUES (?, NULL)", [d])
    engine = RecommendationEngine(memory_db)
    # The WHERE clause filters NULLs, so AVG returns NULL → None
    result = engine._get_avg("silver.all_null_sleep", "sleep_score", 7)
    assert result is None


# ---------------------------------------------------------------------------
# _get_stddev edge cases
# ---------------------------------------------------------------------------


def test_get_stddev_missing_table_returns_none(memory_db):
    """Query against a non-existent table must return None, not raise."""
    engine = RecommendationEngine(memory_db)
    result = engine._get_stddev("silver.nonexistent_table", "col", 14)
    assert result is None


def test_stddev_zero_identical_values(memory_db):
    """STDDEV of identical values should be 0 (or effectively 0)."""
    memory_db.execute("CREATE TABLE silver.flat_sleep (day DATE, sleep_score INTEGER)")
    for i in range(10):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.flat_sleep VALUES (?, 80)", [d])
    engine = RecommendationEngine(memory_db)
    result = engine._get_stddev("silver.flat_sleep", "sleep_score", 14)
    # DuckDB STDDEV on identical values may return 0.0 or None (population vs sample)
    # Either is acceptable — the key contract is: no exception, result is numeric or None
    assert result is None or abs(result) < 1e-9


# ---------------------------------------------------------------------------
# _check_sleep_patterns boundary tests
# ---------------------------------------------------------------------------


def test_sleep_no_decline_avg7_equals_avg30(memory_db):
    """When 7-day avg == 30-day avg the threshold (>5 diff) is not crossed."""
    memory_db.execute("CREATE TABLE silver.daily_sleep (day DATE, sleep_score INTEGER)")
    # Insert 30 days of identical score — avg_7 == avg_30 == 75
    for i in range(30):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_sleep VALUES (?, 75)", [d])
    engine = RecommendationEngine(memory_db)
    recs = engine._check_sleep_patterns()
    sleep_declining = [r for r in recs if r.title == "Sleep quality declining"]
    assert len(sleep_declining) == 0


def test_sleep_no_decline_within_5_margin(memory_db):
    """avg_7 = avg_30 - 4 must NOT trigger the declining rec (threshold is >5)."""
    memory_db.execute("CREATE TABLE silver.daily_sleep (day DATE, sleep_score INTEGER)")
    # 23 days at 80, then 7 days at 76 → avg_30 ≈ 79.07, avg_7 = 76, diff = 3.07
    for i in range(7, 30):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_sleep VALUES (?, 80)", [d])
    for i in range(7):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_sleep VALUES (?, 76)", [d])
    engine = RecommendationEngine(memory_db)
    recs = engine._check_sleep_patterns()
    sleep_declining = [r for r in recs if r.title == "Sleep quality declining"]
    assert len(sleep_declining) == 0


# ---------------------------------------------------------------------------
# _check_stress_patterns division-by-zero guard
# ---------------------------------------------------------------------------


def test_stress_division_by_zero_protected(memory_db):
    """recovery_high=0 must not cause ZeroDivisionError due to max(avg_recovery, 1)."""
    memory_db.execute(
        """
        CREATE TABLE silver.daily_stress (
            day DATE, stress_high INTEGER, recovery_high INTEGER
        )
        """
    )
    for i in range(7):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_stress VALUES (?, 300, 0)", [d])
    engine = RecommendationEngine(memory_db)
    # Must not raise — ratio = 300 / max(0, 1) = 300, which is > 0.6
    recs = engine._check_stress_patterns()
    # Rec is generated (high ratio) — confirm no crash and category is correct
    assert all(r.category == "stress" for r in recs)


# ---------------------------------------------------------------------------
# _check_weight_patterns edge cases
# ---------------------------------------------------------------------------


def test_weight_fewer_than_3_measurements(memory_db):
    """With only 2 weight rows COUNT(*) < 3, so no weight rec is generated."""
    memory_db.execute(
        """
        CREATE TABLE silver.weight (
            datetime TIMESTAMP, weight_kg DOUBLE
        )
        """
    )
    for i in range(2):
        dt = f"2026-03-0{i + 1} 07:00:00"
        memory_db.execute(
            "INSERT INTO silver.weight VALUES (?, ?)", [dt, 82.0 + i * 0.1]
        )
    engine = RecommendationEngine(memory_db)
    recs = engine._check_weight_patterns()
    assert recs == []


def test_weight_missing_table_returns_empty(memory_db):
    """No silver.weight table → bare except catches, returns empty list."""
    # memory_db has silver schema but NO weight table
    engine = RecommendationEngine(memory_db)
    recs = engine._check_weight_patterns()
    assert recs == []


# ---------------------------------------------------------------------------
# Confidence default
# ---------------------------------------------------------------------------


def test_confidence_always_0_7(memory_db):
    """Every generated Recommendation must have confidence == 0.7 (the dataclass default)."""
    memory_db.execute("CREATE TABLE silver.daily_sleep (day DATE, sleep_score INTEGER)")
    # avg_7 will be 60, avg_30 will be 80 → diff = 20 > 5 → triggers declining rec
    for i in range(7, 30):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_sleep VALUES (?, 80)", [d])
    for i in range(7):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_sleep VALUES (?, 60)", [d])
    engine = RecommendationEngine(memory_db)
    recs = engine._check_sleep_patterns()
    assert len(recs) >= 1
    for r in recs:
        assert r.confidence == 0.7


# ---------------------------------------------------------------------------
# Multiple recs from same category
# ---------------------------------------------------------------------------


def test_multiple_recs_same_category(memory_db):
    """Both sleep triggers (declining + high variability) should each fire independently."""
    memory_db.execute("CREATE TABLE silver.daily_sleep (day DATE, sleep_score INTEGER)")
    # 23 days at 85 for avg_30 baseline, last 7 days alternating 50/90 (avg≈70, high variability)
    scores_recent = [50, 90, 50, 90, 50, 90, 50]  # avg=65.7, stddev≈22.4
    for i, score in enumerate(scores_recent):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_sleep VALUES (?, ?)", [d, score])
    for i in range(7, 30):
        d = date.today() - timedelta(days=i)
        memory_db.execute("INSERT INTO silver.daily_sleep VALUES (?, 85)", [d])
    engine = RecommendationEngine(memory_db)
    recs = engine._check_sleep_patterns()
    # avg_7 ≈ 65.7, avg_30 ≈ 79.5 → diff > 5 → declining fires
    # stddev over 14 days spans both recent alternating and some 85s → > 15 → variability fires
    sleep_titles = [r.title for r in recs]
    assert "Sleep quality declining" in sleep_titles
    assert "Inconsistent sleep quality" in sleep_titles


# ---------------------------------------------------------------------------
# format_recommendations
# ---------------------------------------------------------------------------


def test_format_empty_list():
    """format_recommendations([]) must return the healthy message."""
    result = format_recommendations([])
    assert "healthy" in result.lower()


# ---------------------------------------------------------------------------
# Recommendation dataclass defaults
# ---------------------------------------------------------------------------


def test_recommendation_dataclass_defaults():
    """Recommendation created with only required fields uses correct defaults."""
    rec = Recommendation(
        title="Test",
        category="sleep",
        priority="low",
        recommendation="Do something.",
        rationale="Because data says so.",
        evidence_type="data_pattern",
    )
    assert rec.confidence == 0.7
    assert rec.metrics_used == []
