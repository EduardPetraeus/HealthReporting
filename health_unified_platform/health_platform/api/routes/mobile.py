"""Mobile-optimized endpoints for the Laege i Lommen iOS app.

Provides bulk data sync and metric thresholds for efficient mobile access.
Minimizes round-trips: one call replaces multiple endpoint calls.

Routes:
    GET /v1/mobile/sync?since=<ISO timestamp>  — bulk data sync
    GET /v1/mobile/thresholds                   — metric threshold definitions
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
from fastapi import APIRouter, Depends, Query

from health_platform.api.auth import verify_token
from health_platform.mcp.query_builder import QueryBuilder
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.mobile")

router = APIRouter(prefix="/v1/mobile", tags=["mobile"])

# Contracts directory for threshold parsing
_CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts" / "metrics"

# Metrics to include in bulk sync (core dashboard metrics)
_SYNC_METRICS = {
    "sleep_score": {
        "table": "silver.daily_sleep",
        "columns": ["day", "sleep_score"],
    },
    "readiness_score": {
        "table": "silver.daily_readiness",
        "columns": ["day", "readiness_score"],
    },
    "steps": {
        "table": "silver.daily_activity",
        "columns": ["day", "steps"],
    },
    "activity_score": {
        "table": "silver.daily_activity",
        "columns": ["day", "activity_score"],
    },
    "resting_heart_rate": {
        "table": "silver.heart_rate",
        "columns": ["day", "resting_heart_rate"],
    },
    "weight": {
        "table": "silver.weight",
        "columns": ["datetime", "weight_kg"],
    },
    "daily_stress": {
        "table": "silver.daily_stress",
        "columns": ["day", "day_summary", "stress_high", "recovery_high"],
    },
}


def _get_db_path() -> str:
    """Resolve DuckDB database path from environment."""
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    env = os.environ.get("HEALTH_ENV", "dev")
    return str(Path.home() / "health_dw" / f"health_dw_{env}.db")


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Create a read-only DuckDB connection."""
    return duckdb.connect(_get_db_path(), read_only=True)


def _parse_since(since: Optional[str]) -> date:
    """Parse the 'since' parameter into a date. Defaults to 30 days ago."""
    if not since:
        return date.today() - timedelta(days=30)
    try:
        return datetime.fromisoformat(since).date()
    except ValueError:
        return date.today() - timedelta(days=30)


def _query_metric(
    con: duckdb.DuckDBPyConnection, table: str, columns: list[str], since: date
) -> list[dict]:
    """Query a single metric table and return rows as dicts."""
    col_list = ", ".join(columns)
    # Determine the date column (first column)
    date_col = columns[0]
    sql = f"SELECT {col_list} FROM {table} WHERE {date_col} >= ? ORDER BY {date_col}"
    try:
        result = con.execute(sql, [since]).fetchall()
        col_names = [c[0] for c in con.description]
        rows = []
        for row in result:
            row_dict = {}
            for i, col in enumerate(col_names):
                val = row[i]
                # Convert date/datetime to ISO string
                if isinstance(val, (date, datetime)):
                    val = val.isoformat()
                row_dict[col] = val
            rows.append(row_dict)
        return rows
    except duckdb.CatalogException:
        logger.debug("Table %s not found, skipping", table)
        return []
    except Exception:
        logger.debug("Failed to query %s", table, exc_info=True)
        return []


def _get_profile(con: duckdb.DuckDBPyConnection) -> dict:
    """Load patient profile as structured dict."""
    try:
        result = con.execute(
            "SELECT category, profile_key, profile_value "
            "FROM agent.patient_profile ORDER BY category, profile_key"
        ).fetchall()
        profile = {}
        for category, key, value in result:
            if category not in profile:
                profile[category] = {}
            profile[category][key] = value
        return profile
    except Exception:
        logger.debug("Failed to load patient profile", exc_info=True)
        return {}


def _get_daily_summaries(con: duckdb.DuckDBPyConnection, since: date) -> list[dict]:
    """Load daily summaries since a given date."""
    try:
        result = con.execute(
            "SELECT day, summary_text FROM agent.daily_summaries "
            "WHERE day >= ? ORDER BY day",
            [since],
        ).fetchall()
        return [
            {
                "day": row[0].isoformat() if isinstance(row[0], date) else str(row[0]),
                "summary_text": row[1],
            }
            for row in result
        ]
    except Exception:
        logger.debug("Failed to load daily summaries", exc_info=True)
        return []


def _get_alerts(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Get recent anomaly alerts from daily summaries."""
    try:
        result = con.execute(
            "SELECT day, anomaly_metrics FROM agent.daily_summaries "
            "WHERE has_anomaly = true AND day >= CURRENT_DATE - 7 "
            "ORDER BY day DESC"
        ).fetchall()
        return [
            {
                "day": row[0].isoformat() if isinstance(row[0], date) else str(row[0]),
                "anomaly_metrics": row[1],
            }
            for row in result
        ]
    except Exception:
        logger.debug("Failed to load alerts", exc_info=True)
        return []


@router.get("/sync")
async def sync(
    since: Optional[str] = Query(
        None,
        description="ISO timestamp — sync data since this point. Defaults to 30 days ago.",
    ),
    _token: str = Depends(verify_token),
):
    """Bulk data sync for mobile app.

    Returns all dashboard metrics, patient profile, alerts, and daily
    summaries in a single response. Replaces 4+ separate API calls.
    """
    since_date = _parse_since(since)
    con = _get_connection()

    try:
        # Fetch all metrics
        metrics = {}
        for metric_name, config in _SYNC_METRICS.items():
            rows = _query_metric(con, config["table"], config["columns"], since_date)
            if rows:
                metrics[metric_name] = rows

        # Fetch profile, summaries, alerts
        profile = _get_profile(con)
        summaries = _get_daily_summaries(con, since_date)
        alerts = _get_alerts(con)
    finally:
        con.close()

    return {
        "metrics": metrics,
        "profile": profile,
        "alerts": alerts,
        "daily_summaries": summaries,
        "sync_timestamp": datetime.now().isoformat(),
    }


@router.get("/thresholds")
async def thresholds(
    _token: str = Depends(verify_token),
):
    """Metric threshold definitions for UI coloring.

    Parses all YAML semantic contracts and returns the thresholds
    section for each metric. Loaded once by the mobile app, cached
    indefinitely on-device.
    """
    qb = QueryBuilder(_CONTRACTS_DIR)
    metric_names = qb.list_metrics()

    result = {}
    for name in metric_names:
        try:
            contract = qb.load_contract(name)
            metric_def = contract.get("metric", contract)
            thresh = metric_def.get("thresholds")
            if thresh:
                result[name] = thresh
        except Exception:
            logger.debug("Failed to load thresholds for %s", name, exc_info=True)

    return {
        "thresholds": result,
        "metric_count": len(result),
        "timestamp": datetime.now().isoformat(),
    }
