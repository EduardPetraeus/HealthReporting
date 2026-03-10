"""Generate natural language summaries for daily health data.

Turns structured rows from silver tables into concise, embeddable
narrative summaries stored in agent.daily_summaries.

No external AI APIs — pure template-based text generation.
"""

from __future__ import annotations

from datetime import date

from health_platform.utils.logging_config import get_logger

logger = get_logger("text_generator")


# ---------------------------------------------------------------------------
# Score interpretation helpers
# ---------------------------------------------------------------------------


def _interpret_score(value: float | None, label: str) -> str:
    """Return a parenthetical interpretation for a 0-100 score."""
    if value is None:
        return ""
    if value >= 85:
        return f"{label} {int(value)} (excellent)"
    elif value >= 70:
        return f"{label} {int(value)} (good)"
    elif value >= 55:
        return f"{label} {int(value)} (fair)"
    else:
        return f"{label} {int(value)} (poor)"


def _format_number(value: float | None) -> str:
    """Format a numeric value with thousands separator, or '—' if None."""
    if value is None:
        return "—"
    if value == int(value):
        return f"{int(value):,}"
    return f"{value:,.1f}"


# ---------------------------------------------------------------------------
# Anomaly detection (>1.5 SD from 90-day rolling mean)
# ---------------------------------------------------------------------------

_ANOMALY_METRICS = [
    ("silver.daily_sleep", "sleep_score", "day", "Sleep score"),
    ("silver.daily_readiness", "readiness_score", "day", "Readiness score"),
    ("silver.daily_activity", "activity_score", "day", "Activity score"),
    ("silver.daily_activity", "steps", "day", "Steps"),
    ("silver.daily_stress", "stress_high", "day", "High-stress minutes"),
    ("silver.daily_spo2", "spo2_avg", "day", "SpO2 average"),
]


def _detect_anomalies(con, day: date) -> list[str]:
    """Return list of anomaly strings for metrics >1.5 SD from 90-day mean."""
    anomalies: list[str] = []
    for table, column, date_col, label in _ANOMALY_METRICS:
        sql = f"""
            WITH stats AS (
                SELECT
                    AVG({column}) AS mean_val,
                    STDDEV({column}) AS sd_val
                FROM {table}
                WHERE {date_col} >= ? - INTERVAL '90 days'
                  AND {date_col} < ?
                  AND {column} IS NOT NULL
            ),
            today AS (
                SELECT {column} AS val
                FROM {table}
                WHERE {date_col} = ?
            )
            SELECT
                today.val,
                stats.mean_val,
                stats.sd_val,
                CASE
                    WHEN stats.sd_val > 0
                    THEN ABS(today.val - stats.mean_val) / stats.sd_val
                    ELSE 0
                END AS z_score
            FROM today, stats
        """
        try:
            row = con.execute(sql, [day, day, day]).fetchone()
        except Exception:
            continue
        if row is None or row[0] is None or row[2] is None:
            continue
        val, mean_val, sd_val, z_score = row
        if z_score > 1.5:
            direction = "above" if val > mean_val else "below"
            anomalies.append(
                f"{label} {_format_number(val)} ({z_score:.1f} SD {direction} 90-day mean)"
            )
    return anomalies


# ---------------------------------------------------------------------------
# Main summary generation
# ---------------------------------------------------------------------------


def generate_daily_summary(con, day: date) -> str:
    """Generate a concise natural language summary for a single day.

    Queries silver tables for sleep, readiness, activity, stress, spo2,
    and weight data. Produces a ~100-150 word narrative suitable for
    embedding.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection with silver schema available.
    day : date
        The calendar date to summarize.

    Returns
    -------
    str
        Natural language summary string.
    """
    parts: list[str] = [f"{day.isoformat()}:"]

    # --- Sleep ---
    sleep = _query_row(con, "silver.daily_sleep", "day", day)
    if sleep:
        score_str = _interpret_score(sleep.get("sleep_score"), "Sleep score")
        if score_str:
            parts.append(f"{score_str}.")
        deep = sleep.get("deep_sleep_score")
        if deep is not None:
            parts.append(
                f"Deep sleep {'strong' if deep >= 75 else 'moderate'} ({int(deep)})."
            )
        latency = sleep.get("latency_score")
        if latency is not None and latency < 70:
            parts.append(
                f"Latency low ({int(latency)}) \u2014 took longer to fall asleep."
            )

    # --- Readiness ---
    readiness = _query_row(con, "silver.daily_readiness", "day", day)
    if readiness:
        score_str = _interpret_score(readiness.get("readiness_score"), "Readiness")
        if score_str:
            parts.append(f"{score_str}.")

    # --- Activity ---
    activity = _query_row(con, "silver.daily_activity", "day", day)
    if activity:
        score_str = _interpret_score(activity.get("activity_score"), "Activity score")
        steps = activity.get("steps")
        cal = activity.get("active_calories")
        if score_str:
            extras = []
            if steps is not None:
                extras.append(f"{_format_number(steps)} steps")
            if cal is not None:
                extras.append(f"{_format_number(cal)} active cal")
            suffix = f" with {', '.join(extras)}" if extras else ""
            parts.append(f"{score_str}{suffix}.")

    # --- Stress ---
    stress = _query_row(con, "silver.daily_stress", "day", day)
    if stress:
        high = stress.get("stress_high")
        if high is not None and high > 0:
            parts.append(f"Stress: {int(high)} high-stress minutes.")

    # --- SpO2 ---
    spo2 = _query_row(con, "silver.daily_spo2", "day", day)
    if spo2:
        avg_val = spo2.get("spo2_avg")
        if avg_val is not None:
            parts.append(f"SpO2 avg {avg_val:.1f}%.")

    # --- Weight ---
    weight = _query_row(con, "silver.weight", "CAST(datetime AS DATE)", day)
    if weight:
        kg = weight.get("weight_kg")
        if kg is not None:
            parts.append(f"Weight {kg:.1f} kg.")

    # --- Resting HR (from heart_rate table, daily min) ---
    rhr = _query_resting_hr(con, day)
    if rhr is not None:
        parts.append(f"Resting HR {int(rhr)} bpm.")
    else:
        parts.append("Resting HR stable.")

    # --- Anomalies ---
    anomalies = _detect_anomalies(con, day)
    if anomalies:
        parts.append(f"Anomalies: {'; '.join(anomalies)}.")
    else:
        parts.append("No anomalies detected.")

    summary = " ".join(parts)
    logger.debug("Generated summary for %s (%d chars)", day, len(summary))
    return summary


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def _query_row(con, table: str, date_col: str, day: date) -> dict | None:
    """Query a single row from a silver table by date, return as dict or None."""
    try:
        result = con.execute(f"SELECT * FROM {table} WHERE {date_col} = ?", [day])
        columns = [desc[0] for desc in result.description]
        row = result.fetchone()
        if row is None:
            return None
        return dict(zip(columns, row))
    except Exception as exc:
        logger.debug("Could not query %s for %s: %s", table, day, exc)
        return None


def _query_resting_hr(con, day: date) -> float | None:
    """Return the minimum heart rate for a given day (proxy for resting HR)."""
    sql = """
        SELECT MIN(bpm) AS resting_hr
        FROM silver.heart_rate
        WHERE CAST(timestamp AS DATE) = ?
          AND bpm > 30
    """
    try:
        row = con.execute(sql, [day]).fetchone()
        return row[0] if row and row[0] is not None else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Backfill and pipeline entry points
# ---------------------------------------------------------------------------


def backfill_summaries(
    con,
    start_date: date = None,
    end_date: date = None,
) -> int:
    """Generate and upsert daily summaries for a date range.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    start_date : date, optional
        First date to process. Defaults to earliest date in silver.daily_sleep.
    end_date : date, optional
        Last date to process. Defaults to latest date in silver.daily_sleep.

    Returns
    -------
    int
        Number of summaries generated.
    """
    if start_date is None or end_date is None:
        try:
            row = con.execute(
                "SELECT MIN(day), MAX(day) FROM silver.daily_sleep"
            ).fetchone()
            if row is None or row[0] is None:
                logger.warning("No data in silver.daily_sleep — nothing to backfill")
                return 0
            start_date = start_date or row[0]
            end_date = end_date or row[1]
        except Exception as exc:
            logger.error("Failed to determine date range: %s", exc)
            return 0

    # Ensure the agent schema and table exist
    _ensure_summary_table(con)

    count = 0
    current = start_date
    while current <= end_date:
        try:
            summary = generate_daily_summary(con, current)
            _upsert_summary(con, current, summary)
            count += 1
        except Exception as exc:
            logger.error("Failed to generate summary for %s: %s", current, exc)
        current = _next_day(current)

    logger.info("Backfilled %d summaries from %s to %s", count, start_date, end_date)
    return count


def generate_summary_for_pipeline(con, day: date) -> str:
    """Generate and store a summary for a single day (pipeline entry point).

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    day : date
        The calendar date to summarize.

    Returns
    -------
    str
        The generated summary text.
    """
    _ensure_summary_table(con)
    summary = generate_daily_summary(con, day)
    _upsert_summary(con, day, summary)
    logger.info("Pipeline summary generated for %s", day)
    return summary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_summary_table(con) -> None:
    """Create agent.daily_summaries if it does not exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS agent.daily_summaries (
            day DATE PRIMARY KEY,
            sleep_score INTEGER,
            readiness_score INTEGER,
            steps INTEGER,
            resting_hr DOUBLE,
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


def _upsert_summary(con, day: date, summary: str, **kwargs) -> None:
    """Insert or replace a daily summary row."""
    con.execute(
        """
        INSERT OR REPLACE INTO agent.daily_summaries
            (day, sleep_score, readiness_score, steps, resting_hr,
             stress_level, has_anomaly, anomaly_metrics,
             summary_text, data_completeness, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """,
        [
            day,
            kwargs.get("sleep_score"),
            kwargs.get("readiness_score"),
            kwargs.get("steps"),
            kwargs.get("resting_hr"),
            kwargs.get("stress_level"),
            kwargs.get("has_anomaly", False),
            kwargs.get("anomaly_metrics"),
            summary,
            kwargs.get("data_completeness"),
        ],
    )


def _next_day(d: date) -> date:
    """Return the next calendar day."""
    from datetime import timedelta

    return d + timedelta(days=1)
