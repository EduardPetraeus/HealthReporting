"""Evidence-backed health recommendation engine.

Generates personalized, actionable recommendations based on the user's
actual health data patterns. All recommendations are:
- Evidence-based (linked to data patterns, not assumptions)
- CDS-safe (no clinical diagnosis or treatment recommendations)
- FDA-safe (no drug/supplement dosage recommendations)
- Actionable (specific behavior suggestions)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from health_platform.utils.logging_config import get_logger

logger = get_logger("recommendation_engine")


@dataclass
class Recommendation:
    """A health recommendation based on data patterns."""

    title: str
    category: str  # "sleep", "activity", "recovery", "nutrition", "stress"
    priority: str  # "high", "medium", "low"
    recommendation: str  # The actual recommendation text
    rationale: str  # Data-backed reasoning
    evidence_type: str  # "data_pattern", "baseline_deviation", "trend", "correlation"
    metrics_used: list[str] = field(default_factory=list)
    confidence: float = 0.7


class RecommendationEngine:
    """Generate personalized health recommendations from data patterns."""

    def __init__(self, con) -> None:
        self.con = con

    def get_recommendations(self, max_results: int = 5) -> list[Recommendation]:
        """Generate recommendations based on recent data patterns."""
        recommendations: list[Recommendation] = []

        # Check each recommendation rule
        recommendations.extend(self._check_sleep_patterns())
        recommendations.extend(self._check_activity_patterns())
        recommendations.extend(self._check_recovery_patterns())
        recommendations.extend(self._check_stress_patterns())
        recommendations.extend(self._check_weight_patterns())

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda r: priority_order.get(r.priority, 3))

        return recommendations[:max_results]

    def _check_sleep_patterns(self) -> list[Recommendation]:
        """Check sleep data for recommendation triggers."""
        recs: list[Recommendation] = []

        # Pattern 1: Declining sleep score trend
        avg_7 = self._get_avg("silver.daily_sleep", "sleep_score", 7)
        avg_30 = self._get_avg("silver.daily_sleep", "sleep_score", 30)

        if avg_7 is not None and avg_30 is not None and avg_7 < avg_30 - 5:
            recs.append(
                Recommendation(
                    title="Sleep quality declining",
                    category="sleep",
                    priority="high",
                    recommendation=(
                        "Your sleep score has dropped compared to your monthly average. "
                        "Consider establishing a consistent bedtime routine and "
                        "limiting screen time 1 hour before bed."
                    ),
                    rationale=(
                        f"7-day avg: {avg_7:.0f} vs 30-day avg: {avg_30:.0f} "
                        f"(decline of {avg_30 - avg_7:.0f} points)"
                    ),
                    evidence_type="trend",
                    metrics_used=["daily_sleep.sleep_score"],
                )
            )

        # Pattern 2: Low sleep consistency
        variability = self._get_stddev("silver.daily_sleep", "sleep_score", 14)
        if variability is not None and variability > 15:
            recs.append(
                Recommendation(
                    title="Inconsistent sleep quality",
                    category="sleep",
                    priority="medium",
                    recommendation=(
                        "Your sleep quality varies significantly night to night. "
                        "Try to maintain consistent sleep and wake times, even on weekends."
                    ),
                    rationale=f"14-day sleep score std dev: {variability:.1f} (target: <10)",
                    evidence_type="data_pattern",
                    metrics_used=["daily_sleep.sleep_score"],
                )
            )

        return recs

    def _check_activity_patterns(self) -> list[Recommendation]:
        """Check activity data for recommendation triggers."""
        recs: list[Recommendation] = []

        avg_steps_7 = self._get_avg("silver.daily_activity", "steps", 7)

        if avg_steps_7 is not None and avg_steps_7 < 6000:
            recs.append(
                Recommendation(
                    title="Low daily movement",
                    category="activity",
                    priority="high",
                    recommendation=(
                        "Your step count is below 6,000. Try adding a 20-minute walk "
                        "after meals -- this also helps with blood sugar regulation."
                    ),
                    rationale=f"7-day avg steps: {avg_steps_7:,.0f} (recommended: >7,500)",
                    evidence_type="baseline_deviation",
                    metrics_used=["daily_activity.steps"],
                )
            )

        # Check for sedentary pattern
        avg_active_cal = self._get_avg("silver.daily_activity", "active_calories", 7)
        if avg_active_cal is not None and avg_active_cal < 250:
            recs.append(
                Recommendation(
                    title="Low active energy expenditure",
                    category="activity",
                    priority="medium",
                    recommendation=(
                        "Your active calorie burn is low. Consider adding 2-3 "
                        "moderate-intensity sessions per week "
                        "(brisk walking, cycling, swimming)."
                    ),
                    rationale=(
                        f"7-day avg active calories: {avg_active_cal:.0f} "
                        f"(recommended: >300)"
                    ),
                    evidence_type="data_pattern",
                    metrics_used=["daily_activity.active_calories"],
                )
            )

        return recs

    def _check_recovery_patterns(self) -> list[Recommendation]:
        """Check readiness and recovery patterns."""
        recs: list[Recommendation] = []

        avg_readiness = self._get_avg("silver.daily_readiness", "readiness_score", 7)

        if avg_readiness is not None and avg_readiness < 65:
            recs.append(
                Recommendation(
                    title="Recovery needs attention",
                    category="recovery",
                    priority="high",
                    recommendation=(
                        "Your readiness has been consistently low. Prioritize rest days, "
                        "reduce training intensity, and ensure adequate sleep and hydration."
                    ),
                    rationale=f"7-day avg readiness: {avg_readiness:.0f} (target: >70)",
                    evidence_type="baseline_deviation",
                    metrics_used=["daily_readiness.readiness_score"],
                )
            )

        return recs

    def _check_stress_patterns(self) -> list[Recommendation]:
        """Check stress data for recommendation triggers."""
        recs: list[Recommendation] = []

        avg_stress = self._get_avg("silver.daily_stress", "stress_high", 7)
        avg_recovery = self._get_avg("silver.daily_stress", "recovery_high", 7)

        if avg_stress is not None and avg_recovery is not None:
            ratio = avg_stress / max(avg_recovery, 1)
            if ratio > 0.6:
                recs.append(
                    Recommendation(
                        title="Stress-recovery imbalance",
                        category="stress",
                        priority="high",
                        recommendation=(
                            "Your stress-to-recovery ratio is elevated. "
                            "Consider breathing exercises, meditation, or reducing workload. "
                            "Even 5 minutes of box breathing can help."
                        ),
                        rationale=(
                            f"Stress/recovery ratio: {ratio:.2f} "
                            f"(7-day: {avg_stress:.0f} stress min vs "
                            f"{avg_recovery:.0f} recovery min)"
                        ),
                        evidence_type="data_pattern",
                        metrics_used=[
                            "daily_stress.stress_high",
                            "daily_stress.recovery_high",
                        ],
                    )
                )

        return recs

    def _check_weight_patterns(self) -> list[Recommendation]:
        """Check weight trends."""
        recs: list[Recommendation] = []

        # Check 30-day weight trend
        try:
            row = self.con.execute(
                """
                WITH numbered AS (
                    SELECT ROW_NUMBER() OVER (ORDER BY datetime) AS x,
                           weight_kg AS y
                    FROM silver.weight
                    WHERE CAST(datetime AS DATE) >= CURRENT_DATE - 30
                      AND weight_kg IS NOT NULL
                )
                SELECT REGR_SLOPE(y, x), COUNT(*) FROM numbered
            """
            ).fetchone()

            if row and row[0] is not None and row[1] >= 3:
                slope = row[0]
                if slope > 0.1:  # Gaining >100g per measurement
                    recs.append(
                        Recommendation(
                            title="Upward weight trend",
                            category="nutrition",
                            priority="low",
                            recommendation=(
                                "Your weight is trending upward over the past month. "
                                "This is informational -- review your calorie intake "
                                "and activity levels if this is not intentional."
                            ),
                            rationale=f"30-day weight trend: +{slope:.2f} kg/measurement",
                            evidence_type="trend",
                            metrics_used=["weight.weight_kg"],
                        )
                    )
        except Exception:
            pass

        return recs

    # --- Helpers ---

    def _get_avg(self, table: str, column: str, days: int) -> float | None:
        """Get the average of a metric over the last N days."""
        try:
            row = self.con.execute(
                f"""
                SELECT AVG({column})
                FROM {table}
                WHERE day >= CURRENT_DATE - {days}
                  AND {column} IS NOT NULL
            """
            ).fetchone()
            return row[0] if row and row[0] is not None else None
        except Exception:
            return None

    def _get_stddev(self, table: str, column: str, days: int) -> float | None:
        """Get the standard deviation of a metric over the last N days."""
        try:
            row = self.con.execute(
                f"""
                SELECT STDDEV({column})
                FROM {table}
                WHERE day >= CURRENT_DATE - {days}
                  AND {column} IS NOT NULL
            """
            ).fetchone()
            return row[0] if row and row[0] is not None else None
        except Exception:
            return None


def format_recommendations(recs: list[Recommendation]) -> str:
    """Format recommendations as markdown."""
    if not recs:
        return "No recommendations at this time. All metrics look healthy!"

    parts = ["# Health Recommendations\n"]
    for r in recs:
        icon = {"high": "[HIGH]", "medium": "[MED]", "low": "[LOW]"}.get(
            r.priority, "[--]"
        )
        parts.append(f"## {icon} {r.title}\n")
        parts.append(f"**Category:** {r.category} | **Priority:** {r.priority}\n")
        parts.append(f"{r.recommendation}\n")
        parts.append(f"*Rationale: {r.rationale}*\n")

    parts.append(
        "\n---\n*These recommendations are based on your data patterns, "
        "not clinical advice. Consult a healthcare provider for medical decisions.*"
    )
    return "\n".join(parts)
