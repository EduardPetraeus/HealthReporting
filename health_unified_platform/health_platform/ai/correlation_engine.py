"""Compute correlations between health metrics.

Supports Pearson correlation with optional time-lag analysis. Results
are stored in silver.metric_relationships for downstream consumption
by the agent memory layer and gold-layer views.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from health_platform.utils.logging_config import get_logger

logger = get_logger("correlation_engine")


# ---------------------------------------------------------------------------
# Table metadata: maps table names to their date column
# ---------------------------------------------------------------------------

# Daily-grain tables use 'day'; per-measurement tables use CAST(timestamp AS DATE).
_DATE_COLUMNS = {
    "daily_sleep": "day",
    "daily_readiness": "day",
    "daily_activity": "day",
    "daily_stress": "day",
    "daily_spo2": "day",
    "weight": "CAST(datetime AS DATE)",
    "heart_rate": "CAST(timestamp AS DATE)",
    "heart_rate_variability": "CAST(timestamp AS DATE)",
}


def _parse_metric(metric: str) -> tuple[str, str]:
    """Parse a metric string like 'daily_sleep.sleep_score' into (table, column).

    Parameters
    ----------
    metric : str
        Dot-separated metric identifier: ``table_name.column_name``.

    Returns
    -------
    tuple[str, str]
        (table_name, column_name)

    Raises
    ------
    ValueError
        If the metric string is not in the expected format.
    """
    parts = metric.split(".", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Invalid metric format '{metric}'. "
            "Expected 'table_name.column_name' (e.g. 'daily_sleep.sleep_score')."
        )
    return parts[0], parts[1]


def _get_date_expr(table: str) -> str:
    """Return the date expression for a given table name."""
    expr = _DATE_COLUMNS.get(table)
    if expr is None:
        # Default assumption: the table has a 'day' column
        logger.debug(
            "No date column mapping for table '%s' — defaulting to 'day'",
            table,
        )
        return "day"
    return expr


def _interpret_strength(pearson_r: float) -> str:
    """Return a human-readable interpretation of correlation strength."""
    abs_r = abs(pearson_r)
    if abs_r >= 0.7:
        return "strong"
    elif abs_r >= 0.4:
        return "moderate"
    elif abs_r >= 0.2:
        return "weak"
    else:
        return "negligible"


def _interpret_direction(pearson_r: float) -> str:
    """Return 'positive', 'negative', or 'none' based on sign."""
    if pearson_r > 0.01:
        return "positive"
    elif pearson_r < -0.01:
        return "negative"
    return "none"


# ---------------------------------------------------------------------------
# Core correlation computation
# ---------------------------------------------------------------------------


def compute_correlation(
    con,
    metric_a: str,
    metric_b: str,
    lag_days: int = 0,
) -> dict:
    """Compute Pearson correlation between two health metrics.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection with silver schema available.
    metric_a : str
        First metric in ``table.column`` format (e.g. ``daily_sleep.sleep_score``).
    metric_b : str
        Second metric in ``table.column`` format.
    lag_days : int
        Number of days to lag metric_b relative to metric_a.
        A lag of 1 means metric_b from the *previous* day is compared
        to metric_a on the current day (i.e. does yesterday's B predict
        today's A?).

    Returns
    -------
    dict
        Keys: ``pearson_r``, ``strength``, ``direction``, ``sample_size``,
        ``metric_a``, ``metric_b``, ``lag_days``.
    """
    table_a, col_a = _parse_metric(metric_a)
    table_b, col_b = _parse_metric(metric_b)

    date_a = _get_date_expr(table_a)
    date_b = _get_date_expr(table_b)

    # For per-measurement tables, we need to aggregate to daily grain first
    subquery_a = _build_daily_subquery(table_a, col_a, date_a, "a")
    subquery_b = _build_daily_subquery(table_b, col_b, date_b, "b")

    lag_clause = ""
    if lag_days > 0:
        lag_clause = f"+ INTERVAL '{lag_days}' DAY"

    sql = f"""
        SELECT CORR(a_val, b_val) AS pearson_r, COUNT(*) AS n
        FROM (
            SELECT a.a_val, b.b_val
            FROM ({subquery_a}) a
            JOIN ({subquery_b}) b
                ON a.a_day = b.b_day {lag_clause}
            WHERE a.a_val IS NOT NULL
              AND b.b_val IS NOT NULL
        )
    """

    try:
        row = con.execute(sql).fetchone()
    except Exception as exc:
        logger.error(
            "Correlation query failed for %s vs %s (lag %d): %s",
            metric_a,
            metric_b,
            lag_days,
            exc,
        )
        return {
            "pearson_r": None,
            "strength": "error",
            "direction": "error",
            "sample_size": 0,
            "metric_a": metric_a,
            "metric_b": metric_b,
            "lag_days": lag_days,
        }

    if row is None or row[0] is None:
        logger.warning(
            "No correlation data for %s vs %s (lag %d)",
            metric_a,
            metric_b,
            lag_days,
        )
        return {
            "pearson_r": None,
            "strength": "insufficient_data",
            "direction": "unknown",
            "sample_size": int(row[1]) if row and row[1] else 0,
            "metric_a": metric_a,
            "metric_b": metric_b,
            "lag_days": lag_days,
        }

    pearson_r = float(row[0])
    sample_size = int(row[1])

    result = {
        "pearson_r": round(pearson_r, 4),
        "strength": _interpret_strength(pearson_r),
        "direction": _interpret_direction(pearson_r),
        "sample_size": sample_size,
        "metric_a": metric_a,
        "metric_b": metric_b,
        "lag_days": lag_days,
    }

    logger.debug(
        "Correlation %s vs %s (lag %d): r=%.4f (%s %s), n=%d",
        metric_a,
        metric_b,
        lag_days,
        pearson_r,
        result["direction"],
        result["strength"],
        sample_size,
    )
    return result


def _build_daily_subquery(
    table: str,
    column: str,
    date_expr: str,
    alias: str,
) -> str:
    """Build a subquery that returns (day, value) at daily grain.

    For tables that already have daily grain (date_expr == 'day'),
    returns a simple SELECT. For per-measurement tables, aggregates
    by date using AVG.
    """
    if date_expr == "day":
        return f"""
            SELECT day AS {alias}_day, {column} AS {alias}_val
            FROM silver.{table}
        """
    else:
        # Per-measurement table: aggregate to daily grain
        return f"""
            SELECT {date_expr} AS {alias}_day, AVG({column}) AS {alias}_val
            FROM silver.{table}
            GROUP BY {date_expr}
        """


# ---------------------------------------------------------------------------
# Predefined correlation pairs
# ---------------------------------------------------------------------------

_STANDARD_CORRELATIONS = [
    # --- Sleep ↔ Readiness/Activity (existing 9) ---
    ("daily_sleep.sleep_score", "daily_readiness.readiness_score", 0),
    ("daily_sleep.sleep_score", "daily_readiness.readiness_score", 1),
    ("daily_sleep.sleep_score", "daily_activity.activity_score", 0),
    ("daily_sleep.sleep_score", "daily_activity.activity_score", 1),
    ("daily_readiness.readiness_score", "daily_activity.activity_score", 0),
    ("daily_sleep.sleep_score", "heart_rate.bpm", 0),
    ("daily_activity.steps", "daily_activity.active_calories", 0),
    ("daily_stress.stress_high", "daily_sleep.sleep_score", 0),
    ("daily_stress.stress_high", "daily_sleep.sleep_score", 1),
    # --- Sleep ↔ Recovery (5) ---
    ("daily_sleep.sleep_score", "daily_stress.recovery_high", 0),
    ("daily_sleep.sleep_score", "daily_stress.recovery_high", 1),
    ("daily_sleep.sleep_score", "daily_spo2.spo2_avg_pct", 0),
    ("daily_sleep.sleep_score", "heart_rate.bpm", 1),  # sleep → next day HR
    ("daily_readiness.readiness_score", "daily_stress.stress_high", 0),
    # --- Activity ↔ Recovery (5) ---
    ("daily_activity.active_calories", "daily_readiness.readiness_score", 1),
    ("daily_activity.steps", "daily_readiness.readiness_score", 1),
    (
        "daily_activity.steps",
        "daily_sleep.sleep_score",
        1,
    ),  # exercise → next night sleep
    ("daily_activity.activity_score", "daily_stress.stress_high", 0),
    ("daily_activity.active_calories", "daily_stress.recovery_high", 1),
    # --- Stress ↔ Others (4) ---
    ("daily_stress.stress_high", "daily_readiness.readiness_score", 1),
    ("daily_stress.stress_high", "daily_activity.activity_score", 0),
    ("daily_stress.recovery_high", "daily_readiness.readiness_score", 0),
    ("daily_stress.recovery_high", "daily_readiness.readiness_score", 1),
    # --- Weight ↔ Activity (3) ---
    ("weight.weight_kg", "daily_activity.steps", 0),
    ("weight.weight_kg", "daily_activity.active_calories", 0),
    ("weight.weight_kg", "daily_sleep.sleep_score", 0),
    # --- SpO2 ↔ Others (3) ---
    ("daily_spo2.spo2_avg_pct", "daily_readiness.readiness_score", 0),
    ("daily_spo2.spo2_avg_pct", "daily_stress.stress_high", 0),
    ("daily_spo2.breathing_disturbance_index", "daily_sleep.sleep_score", 0),
    # --- Cross-domain delayed effects (3) ---
    (
        "daily_activity.active_calories",
        "daily_sleep.sleep_score",
        1,
    ),  # Does exercise help sleep?
    ("daily_stress.stress_high", "daily_spo2.spo2_avg_pct", 1),
    ("heart_rate.bpm", "daily_readiness.readiness_score", 1),
]


def _ensure_relationships_table(con) -> None:
    """Create silver.metric_relationships if it does not exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.metric_relationships (
            metric_a VARCHAR NOT NULL,
            metric_b VARCHAR NOT NULL,
            lag_days INTEGER NOT NULL DEFAULT 0,
            pearson_r DOUBLE,
            strength VARCHAR,
            direction VARCHAR,
            sample_size INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (metric_a, metric_b, lag_days)
        )
    """
    )


def compute_all_correlations(con) -> int:
    """Run all predefined correlation pairs and store results.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection with silver schema available.

    Returns
    -------
    int
        Number of correlations successfully computed and stored.
    """
    _ensure_relationships_table(con)
    count = 0

    for metric_a, metric_b, lag_days in _STANDARD_CORRELATIONS:
        result = compute_correlation(con, metric_a, metric_b, lag_days)
        if result["pearson_r"] is not None:
            _upsert_relationship(con, result)
            count += 1
            logger.info(
                "Correlation: %s vs %s (lag %d) = %.4f (%s %s, n=%d)",
                metric_a,
                metric_b,
                lag_days,
                result["pearson_r"],
                result["direction"],
                result["strength"],
                result["sample_size"],
            )
        else:
            logger.warning(
                "Skipped %s vs %s (lag %d): %s",
                metric_a,
                metric_b,
                lag_days,
                result["strength"],
            )

    logger.info(
        "Computed %d / %d standard correlations",
        count,
        len(_STANDARD_CORRELATIONS),
    )
    return count


def _upsert_relationship(con, result: dict) -> None:
    """Insert or replace a row in silver.metric_relationships."""
    con.execute(
        """
        INSERT OR REPLACE INTO silver.metric_relationships
            (source_metric, target_metric, relationship_type, strength,
             lag_days, direction, evidence_type, confidence,
             description, last_computed_at)
        VALUES (?, ?, 'correlates_with', ?, ?, ?, 'statistical', ?,
                ?, CURRENT_TIMESTAMP)
    """,
        [
            result["metric_a"],
            result["metric_b"],
            result.get("pearson_r"),
            result.get("lag_days", 0),
            result.get("direction"),
            abs(result.get("pearson_r", 0)) if result.get("pearson_r") else None,
            f"{result.get('strength', '')} correlation (r={result.get('pearson_r', 0):.4f}, n={result.get('sample_size', 0)})",
        ],
    )


# ---------------------------------------------------------------------------
# Lag discovery
# ---------------------------------------------------------------------------


def discover_correlations(
    con,
    metric_a: str,
    metric_b: str,
    max_lag: int = 3,
) -> list[dict]:
    """Try multiple lag values and return correlations sorted by strength.

    Useful for discovering delayed effects (e.g. does poor sleep on day N
    affect activity on day N+1 or N+2?).

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    metric_a : str
        First metric in ``table.column`` format.
    metric_b : str
        Second metric in ``table.column`` format.
    max_lag : int
        Maximum lag (in days) to test. Tests lag 0 through max_lag.

    Returns
    -------
    list[dict]
        Correlation results sorted by absolute Pearson r (strongest first).
    """
    results = []
    for lag in range(0, max_lag + 1):
        result = compute_correlation(con, metric_a, metric_b, lag)
        results.append(result)

    # Sort by absolute pearson_r, strongest first; None values go last
    results.sort(
        key=lambda r: abs(r["pearson_r"]) if r["pearson_r"] is not None else -1,
        reverse=True,
    )

    logger.info(
        "Discovered correlations for %s vs %s (lag 0-%d): strongest at lag %d (r=%.4f)",
        metric_a,
        metric_b,
        max_lag,
        (
            results[0]["lag_days"]
            if results and results[0]["pearson_r"] is not None
            else -1
        ),
        (
            results[0]["pearson_r"]
            if results and results[0]["pearson_r"] is not None
            else 0
        ),
    )

    return results
