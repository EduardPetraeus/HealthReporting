"""Multi-stream anomaly detection engine.

Detects statistical anomalies across all health metrics using z-score analysis,
constellation patterns (multiple simultaneous anomalies), and temporal
degradation tracking (gradually worsening trends).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

import duckdb
from health_platform.utils.logging_config import get_logger

logger = get_logger("anomaly_detector")


@dataclass
class Anomaly:
    """A detected anomaly in health data."""

    metric: str  # e.g., "daily_sleep.sleep_score"
    table: str  # e.g., "silver.daily_sleep"
    column: str  # e.g., "sleep_score"
    day: date
    value: float
    mean: float
    std_dev: float
    z_score: float
    severity: str  # "anomaly" (>2.0), "unusual" (>1.5), "warning" (>1.0)
    direction: str  # "above" or "below"
    description: str


@dataclass
class ConstellationPattern:
    """Multiple simultaneous anomalies suggesting a systemic issue."""

    day: date
    anomalies: list[Anomaly]
    pattern_type: str  # "recovery_stress", "overtraining", "illness_onset", "general"
    confidence: float  # 0.0-1.0
    description: str


@dataclass
class TemporalDegradation:
    """A metric that's been gradually declining/worsening over time."""

    metric: str
    start_day: date
    end_day: date
    duration_days: int
    slope: float  # negative = declining
    r_squared: float  # goodness of fit
    description: str


@dataclass
class AnomalyReport:
    """Complete anomaly detection report."""

    generated_at: date
    lookback_days: int
    anomalies: list[Anomaly] = field(default_factory=list)
    constellations: list[ConstellationPattern] = field(default_factory=list)
    degradations: list[TemporalDegradation] = field(default_factory=list)

    @property
    def has_critical(self) -> bool:
        return (
            any(a.severity == "anomaly" for a in self.anomalies)
            or len(self.constellations) > 0
        )


# Metrics to monitor -- table, column, date_column, display_name
MONITORED_METRICS = [
    ("silver.daily_sleep", "sleep_score", "day", "Sleep Score"),
    ("silver.daily_readiness", "readiness_score", "day", "Readiness Score"),
    ("silver.daily_activity", "activity_score", "day", "Activity Score"),
    ("silver.daily_activity", "steps", "day", "Steps"),
    ("silver.daily_stress", "stress_high", "day", "High-Stress Minutes"),
    ("silver.daily_spo2", "spo2_avg_pct", "day", "SpO2 Average"),
    ("silver.heart_rate", "bpm", "CAST(timestamp AS DATE)", "Heart Rate"),
    ("silver.weight", "weight_kg", "CAST(datetime AS DATE)", "Weight"),
]

# Constellation patterns: groups of metrics that together signal a condition
CONSTELLATION_RULES: dict[str, dict] = {
    "recovery_stress": {
        "description": "Poor recovery combined with elevated stress indicators",
        "requires": ["Readiness Score", "High-Stress Minutes"],
        "optional": ["Sleep Score"],
        "direction_map": {
            "Readiness Score": "below",
            "High-Stress Minutes": "above",
            "Sleep Score": "below",
        },
    },
    "overtraining": {
        "description": "Signs of overtraining: low readiness + elevated resting HR",
        "requires": ["Readiness Score", "Heart Rate"],
        "optional": ["Activity Score", "Sleep Score"],
        "direction_map": {
            "Readiness Score": "below",
            "Heart Rate": "above",
        },
    },
    "illness_onset": {
        "description": "Possible illness: multiple metrics degraded simultaneously",
        "requires": ["Sleep Score", "Readiness Score"],
        "optional": ["SpO2 Average", "Heart Rate"],
        "direction_map": {
            "Sleep Score": "below",
            "Readiness Score": "below",
            "SpO2 Average": "below",
            "Heart Rate": "above",
        },
    },
}

# Severity thresholds for z-score classification
_SEVERITY_THRESHOLDS = [
    (2.0, "anomaly"),
    (1.5, "unusual"),
    (1.0, "warning"),
]


def _classify_severity(abs_z: float) -> str | None:
    """Classify severity from absolute z-score. Returns None if below all thresholds."""
    for threshold, label in _SEVERITY_THRESHOLDS:
        if abs_z >= threshold:
            return label
    return None


class AnomalyDetector:
    """Multi-stream health anomaly detection engine."""

    def __init__(self, con: duckdb.DuckDBPyConnection, window_days: int = 90):
        self.con = con
        self.window_days = window_days

    def detect(self, lookback_days: int = 7) -> AnomalyReport:
        """Run full anomaly detection pipeline.

        1. Z-score analysis on all monitored metrics
        2. Constellation pattern matching
        3. Temporal degradation detection (30-day trends)
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)

        report = AnomalyReport(generated_at=end_date, lookback_days=lookback_days)

        # Step 1: Z-score anomalies
        for table, column, date_col, display_name in MONITORED_METRICS:
            anomalies = self._detect_zscore_anomalies(
                table, column, date_col, display_name, start_date, end_date
            )
            report.anomalies.extend(anomalies)

        # Step 2: Constellation patterns
        report.constellations = self._detect_constellations(report.anomalies)

        # Step 3: Temporal degradation
        report.degradations = self._detect_degradations(end_date)

        logger.info(
            "Anomaly report: %d anomalies, %d constellations, %d degradations",
            len(report.anomalies),
            len(report.constellations),
            len(report.degradations),
        )
        return report

    def _detect_zscore_anomalies(
        self,
        table: str,
        column: str,
        date_col: str,
        display_name: str,
        start_date: date,
        end_date: date,
    ) -> list[Anomaly]:
        """Detect z-score anomalies for a single metric.

        Computes mean and std_dev from the baseline window (window_days before
        the start_date), then flags values within the lookback period that
        exceed the severity thresholds.
        """
        baseline_start = start_date - timedelta(days=self.window_days)

        sql = f"""
            WITH baseline AS (
                SELECT
                    AVG({column}) AS mean_val,
                    STDDEV({column}) AS sd_val
                FROM {table}
                WHERE {date_col} >= ?
                  AND {date_col} < ?
                  AND {column} IS NOT NULL
            ),
            recent AS (
                SELECT
                    {date_col} AS metric_day,
                    {column} AS val
                FROM {table}
                WHERE {date_col} >= ?
                  AND {date_col} <= ?
                  AND {column} IS NOT NULL
            )
            SELECT
                r.metric_day,
                r.val,
                b.mean_val,
                b.sd_val,
                CASE
                    WHEN b.sd_val > 0
                    THEN (r.val - b.mean_val) / b.sd_val
                    ELSE 0
                END AS z_score
            FROM recent r, baseline b
            WHERE b.sd_val IS NOT NULL AND b.sd_val > 0
            ORDER BY ABS(z_score) DESC
        """

        try:
            rows = self.con.execute(
                sql, [baseline_start, start_date, start_date, end_date]
            ).fetchall()
        except Exception as exc:
            logger.debug("Z-score query failed for %s.%s: %s", table, column, exc)
            return []

        anomalies: list[Anomaly] = []
        for row in rows:
            metric_day, val, mean_val, sd_val, z_score = row
            abs_z = abs(z_score)
            severity = _classify_severity(abs_z)
            if severity is None:
                continue

            direction = "above" if val > mean_val else "below"

            # Convert metric_day to date if it isn't already
            if isinstance(metric_day, str):
                metric_day = date.fromisoformat(metric_day)

            description = (
                f"{display_name} = {val:.1f} on {metric_day} "
                f"({abs_z:.1f} SD {direction} 90-day mean of {mean_val:.1f})"
            )

            anomalies.append(
                Anomaly(
                    metric=f"{table.replace('silver.', '')}.{column}",
                    table=table,
                    column=column,
                    day=metric_day,
                    value=float(val),
                    mean=float(mean_val),
                    std_dev=float(sd_val),
                    z_score=float(z_score),
                    severity=severity,
                    direction=direction,
                    description=description,
                )
            )

        return anomalies

    def _detect_constellations(
        self, anomalies: list[Anomaly]
    ) -> list[ConstellationPattern]:
        """Find constellation patterns where multiple metrics are anomalous on the same day.

        Groups anomalies by day, then checks each day against CONSTELLATION_RULES
        to see if the required metrics are present with correct directions.
        """
        # Group anomalies by day
        by_day: dict[date, list[Anomaly]] = {}
        for a in anomalies:
            by_day.setdefault(a.day, []).append(a)

        patterns: list[ConstellationPattern] = []

        for day, day_anomalies in by_day.items():
            # Build a lookup: display_name -> anomaly (keep most severe per metric)
            display_lookup: dict[str, Anomaly] = {}
            for a in day_anomalies:
                # Map metric back to display name
                for _table, _col, _dcol, dname in MONITORED_METRICS:
                    if a.table == _table and a.column == _col:
                        existing = display_lookup.get(dname)
                        if existing is None or abs(a.z_score) > abs(existing.z_score):
                            display_lookup[dname] = a
                        break

            # Check each constellation rule
            for pattern_name, rule in CONSTELLATION_RULES.items():
                required = rule["requires"]
                optional = rule.get("optional", [])
                direction_map = rule["direction_map"]

                # All required metrics must be present and in correct direction
                matched_required = []
                all_required_match = True
                for req_metric in required:
                    if req_metric not in display_lookup:
                        all_required_match = False
                        break
                    anomaly = display_lookup[req_metric]
                    expected_dir = direction_map.get(req_metric)
                    if expected_dir and anomaly.direction != expected_dir:
                        all_required_match = False
                        break
                    matched_required.append(anomaly)

                if not all_required_match:
                    continue

                # Count optional matches for confidence
                matched_optional = []
                for opt_metric in optional:
                    if opt_metric in display_lookup:
                        anomaly = display_lookup[opt_metric]
                        expected_dir = direction_map.get(opt_metric)
                        if not expected_dir or anomaly.direction == expected_dir:
                            matched_optional.append(anomaly)

                # Confidence: base 0.6 for required match + up to 0.4 for optionals
                total_optional = len(optional) if optional else 1
                optional_ratio = len(matched_optional) / total_optional
                confidence = 0.6 + 0.4 * optional_ratio

                all_matched = matched_required + matched_optional

                patterns.append(
                    ConstellationPattern(
                        day=day,
                        anomalies=all_matched,
                        pattern_type=pattern_name,
                        confidence=confidence,
                        description=rule["description"],
                    )
                )

        return patterns

    def _detect_degradations(
        self, end_date: date, min_days: int = 14
    ) -> list[TemporalDegradation]:
        """Detect metrics with sustained negative trends using linear regression.

        For each metric, computes a 30-day linear regression. Flags if:
        - The slope is significantly negative (or positive for metrics where
          higher = worse, like stress_high)
        - R-squared > 0.3 (the trend explains at least 30% of variance)
        """
        trend_window = 30
        start_date = end_date - timedelta(days=trend_window)

        degradations: list[TemporalDegradation] = []

        # Metrics where higher = worse (trend up is bad)
        higher_is_worse = {"stress_high", "bpm"}

        for table, column, date_col, display_name in MONITORED_METRICS:
            sql = f"""
                SELECT
                    {date_col} AS metric_day,
                    {column} AS val
                FROM {table}
                WHERE {date_col} >= ?
                  AND {date_col} <= ?
                  AND {column} IS NOT NULL
                ORDER BY {date_col}
            """

            try:
                rows = self.con.execute(sql, [start_date, end_date]).fetchall()
            except Exception as exc:
                logger.debug(
                    "Degradation query failed for %s.%s: %s", table, column, exc
                )
                continue

            if len(rows) < min_days:
                continue

            # Simple linear regression: y = slope * x + intercept
            # x = day index (0, 1, 2, ...)
            n = len(rows)
            x_vals = list(range(n))
            y_vals = [float(row[1]) for row in rows]

            x_mean = sum(x_vals) / n
            y_mean = sum(y_vals) / n

            ss_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, y_vals))
            ss_xx = sum((x - x_mean) ** 2 for x in x_vals)
            ss_yy = sum((y - y_mean) ** 2 for y in y_vals)

            if ss_xx == 0 or ss_yy == 0:
                continue

            slope = ss_xy / ss_xx
            r_squared = (ss_xy**2) / (ss_xx * ss_yy)

            # Determine if trend is degrading
            is_bad_trend = False
            if column in higher_is_worse:
                # Higher values are worse, so positive slope = degradation
                is_bad_trend = slope > 0
            else:
                # Lower values are worse, so negative slope = degradation
                is_bad_trend = slope < 0

            if not is_bad_trend or r_squared < 0.3:
                continue

            # Convert first and last day
            first_day = rows[0][0]
            last_day = rows[-1][0]
            if isinstance(first_day, str):
                first_day = date.fromisoformat(first_day)
            if isinstance(last_day, str):
                last_day = date.fromisoformat(last_day)

            duration = (last_day - first_day).days + 1

            direction_word = "increasing" if slope > 0 else "declining"
            description = (
                f"{display_name} has been {direction_word} over {duration} days "
                f"(slope: {slope:+.2f}/day, R²: {r_squared:.2f})"
            )

            degradations.append(
                TemporalDegradation(
                    metric=f"{table.replace('silver.', '')}.{column}",
                    start_day=first_day,
                    end_day=last_day,
                    duration_days=duration,
                    slope=slope,
                    r_squared=r_squared,
                    description=description,
                )
            )

        return degradations


def format_anomaly_report(report: AnomalyReport) -> str:
    """Format an anomaly report as markdown for MCP output."""
    parts = [f"# Anomaly Report ({report.generated_at})\n"]
    parts.append(f"Lookback: {report.lookback_days} days\n")

    if report.constellations:
        parts.append("## Constellation Patterns\n")
        for c in report.constellations:
            parts.append(
                f"**{c.pattern_type}** ({c.day}) -- confidence: {c.confidence:.0%}"
            )
            parts.append(f"  {c.description}")
            for a in c.anomalies:
                parts.append(f"  - {a.description}")

    if report.degradations:
        parts.append("\n## Trend Degradations\n")
        for d in report.degradations:
            parts.append(f"- {d.description}")

    if report.anomalies:
        parts.append("\n## Individual Anomalies\n")
        for a in sorted(report.anomalies, key=lambda x: abs(x.z_score), reverse=True):
            parts.append(f"- [{a.severity.upper()}] {a.description}")

    if not report.anomalies and not report.constellations and not report.degradations:
        parts.append("\nNo anomalies detected. All metrics within normal range.")

    return "\n".join(parts)
