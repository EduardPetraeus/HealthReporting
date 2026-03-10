"""Compute and update patient profile baselines and demographics.

Baselines are rolling medians computed from silver-layer tables and
stored in agent.patient_profile. They support anomaly detection and
longitudinal trend analysis.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from health_platform.utils.logging_config import get_logger

logger = get_logger("baseline_computer")


# ---------------------------------------------------------------------------
# Profile table management
# ---------------------------------------------------------------------------


def _ensure_profile_table(con) -> None:
    """Create agent.patient_profile if it does not exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
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


def update_profile_entry(
    con,
    key: str,
    value: str,
    numeric: float | None,
    category: str,
    description: str,
    computed_from: str,
    frequency: str = "daily",
) -> None:
    """Insert or replace a single entry in agent.patient_profile.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    key : str
        Unique key for the profile entry (e.g. ``sleep_score_baseline``).
    value : str
        Human-readable value string.
    numeric : float or None
        Numeric value for programmatic use.
    category : str
        Category grouping (e.g. ``baselines``, ``demographics``).
    description : str
        Brief description of what this entry represents.
    computed_from : str
        Source table/query description.
    frequency : str
        How often this should be recomputed (e.g. ``daily``, ``weekly``).
    """
    _ensure_profile_table(con)
    con.execute(
        """
        INSERT OR REPLACE INTO agent.patient_profile
            (profile_key, profile_value, numeric_value, category, description,
             computed_from, update_frequency, last_updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """,
        [key, value, numeric, category, description, computed_from, frequency],
    )
    logger.debug("Updated profile entry: %s = %s", key, value)


# ---------------------------------------------------------------------------
# Baseline computations
# ---------------------------------------------------------------------------

_BASELINE_QUERIES = [
    {
        "key": "sleep_score_baseline",
        "sql": """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sleep_score)
            FROM silver.daily_sleep
            WHERE day >= CURRENT_DATE - 90
              AND sleep_score IS NOT NULL
        """,
        "description": "90-day median sleep score",
        "computed_from": "silver.daily_sleep (90-day window)",
        "format_fn": lambda v: f"{v:.0f}",
    },
    {
        "key": "readiness_score_baseline",
        "sql": """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY readiness_score)
            FROM silver.daily_readiness
            WHERE day >= CURRENT_DATE - 90
              AND readiness_score IS NOT NULL
        """,
        "description": "90-day median readiness score",
        "computed_from": "silver.daily_readiness (90-day window)",
        "format_fn": lambda v: f"{v:.0f}",
    },
    {
        "key": "resting_hr_baseline",
        "sql": """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_min)
            FROM (
                SELECT CAST(timestamp AS DATE) AS day,
                       MIN(bpm) AS daily_min
                FROM silver.heart_rate
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '90 days'
                  AND bpm > 30
                GROUP BY CAST(timestamp AS DATE)
            )
        """,
        "description": "90-day median of daily minimum resting heart rate",
        "computed_from": "silver.heart_rate (90-day window, daily min, bpm > 30)",
        "format_fn": lambda v: f"{v:.0f} bpm",
    },
    {
        "key": "steps_baseline",
        "sql": """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY steps)
            FROM silver.daily_activity
            WHERE day >= CURRENT_DATE - 90
              AND steps IS NOT NULL
        """,
        "description": "90-day median daily step count",
        "computed_from": "silver.daily_activity (90-day window)",
        "format_fn": lambda v: f"{v:,.0f}",
    },
    {
        "key": "weight_baseline",
        "sql": """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY weight_kg)
            FROM silver.weight
            WHERE CAST(datetime AS DATE) >= CURRENT_DATE - 30
              AND weight_kg IS NOT NULL
        """,
        "description": "30-day median weight",
        "computed_from": "silver.weight (30-day window)",
        "format_fn": lambda v: f"{v:.1f} kg",
    },
    {
        "key": "activity_score_baseline",
        "sql": """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY activity_score)
            FROM silver.daily_activity
            WHERE day >= CURRENT_DATE - 90
              AND activity_score IS NOT NULL
        """,
        "description": "90-day median activity score",
        "computed_from": "silver.daily_activity (90-day window)",
        "format_fn": lambda v: f"{v:.0f}",
    },
]


def compute_all_baselines(con) -> int:
    """Compute all rolling baseline metrics and upsert into patient_profile.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection with silver schema available.

    Returns
    -------
    int
        Number of baselines successfully computed and stored.
    """
    _ensure_profile_table(con)
    count = 0

    for spec in _BASELINE_QUERIES:
        try:
            row = con.execute(spec["sql"]).fetchone()
            if row is None or row[0] is None:
                logger.warning("No data for baseline %s — skipping", spec["key"])
                continue

            numeric_value = float(row[0])
            display_value = spec["format_fn"](numeric_value)

            update_profile_entry(
                con,
                key=spec["key"],
                value=display_value,
                numeric=numeric_value,
                category="baselines",
                description=spec["description"],
                computed_from=spec["computed_from"],
                frequency="daily",
            )
            count += 1
            logger.info("Baseline %s = %s", spec["key"], display_value)

        except Exception as exc:
            logger.error("Failed to compute baseline %s: %s", spec["key"], exc)

    logger.info("Computed %d / %d baselines", count, len(_BASELINE_QUERIES))
    return count


# ---------------------------------------------------------------------------
# Demographics
# ---------------------------------------------------------------------------

_DEMOGRAPHICS_FIELDS = [
    ("age", "Age in years", lambda r: r.get("age")),
    ("weight_kg", "Body weight in kilograms", lambda r: r.get("weight_kg")),
    ("height_m", "Height in meters", lambda r: r.get("height_m")),
    ("biological_sex", "Biological sex", lambda r: r.get("biological_sex")),
]


def compute_demographics(con) -> int:
    """Read demographics from silver.personal_info and upsert into patient_profile.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection with silver schema available.

    Returns
    -------
    int
        Number of demographic entries upserted.
    """
    _ensure_profile_table(con)

    try:
        result = con.execute("SELECT * FROM silver.personal_info LIMIT 1")
        columns = [desc[0] for desc in result.description]
        row = result.fetchone()
        if row is None:
            logger.warning("No data in silver.personal_info — skipping demographics")
            return 0
        data = dict(zip(columns, row))
    except Exception as exc:
        logger.error("Failed to read silver.personal_info: %s", exc)
        return 0

    count = 0
    for key, description, extractor in _DEMOGRAPHICS_FIELDS:
        value = extractor(data)
        if value is None:
            continue

        numeric = None
        display = str(value)
        if isinstance(value, (int, float)):
            numeric = float(value)
            display = f"{value}"

        update_profile_entry(
            con,
            key=key,
            value=display,
            numeric=numeric,
            category="demographics",
            description=description,
            computed_from="silver.personal_info",
            frequency="weekly",
        )
        count += 1

    logger.info("Updated %d demographic entries", count)
    return count


# ---------------------------------------------------------------------------
# Profile reader
# ---------------------------------------------------------------------------


def get_profile(
    con,
    categories: list[str] | None = None,
) -> dict:
    """Read patient_profile entries, optionally filtered by categories.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    categories : list[str], optional
        If provided, only return entries matching these categories.

    Returns
    -------
    dict
        Dictionary mapping keys to their profile data dicts.
        Each value contains: value, numeric_value, category, description,
        computed_from, update_frequency, updated_at.
    """
    _ensure_profile_table(con)

    if categories:
        placeholders = ", ".join(["?"] * len(categories))
        sql = f"""
            SELECT * FROM agent.patient_profile
            WHERE category IN ({placeholders})
            ORDER BY category, profile_key
        """
        result = con.execute(sql, categories)
    else:
        result = con.execute(
            """
            SELECT * FROM agent.patient_profile
            ORDER BY category, profile_key
        """
        )

    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    profile = {}
    for row in rows:
        entry = dict(zip(columns, row))
        profile[entry["profile_key"]] = {
            "value": entry.get("profile_value"),
            "numeric_value": entry.get("numeric_value"),
            "category": entry.get("category"),
            "description": entry.get("description"),
            "computed_from": entry.get("computed_from"),
            "update_frequency": entry.get("update_frequency"),
            "last_updated_at": entry.get("last_updated_at"),
        }

    return profile
