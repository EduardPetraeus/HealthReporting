"""DesktopAPI — Python methods exposed to JavaScript via pywebview.

All public methods are callable from the frontend as:
    window.pywebview.api.method_name(args)

Returns JSON-serializable dicts/lists for JavaScript consumption.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

import duckdb

from health_platform.mcp.health_tools import HealthTools
from health_platform.mcp.query_builder import QueryBuilder
from health_platform.utils.logging_config import get_logger

logger = get_logger("desktop.api")

_CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts" / "metrics"


def _sanitize(value: Any) -> Any:
    """Convert DuckDB types to JSON-serializable Python types."""
    import decimal  # noqa: F811 — imported here to survive ruff auto-removal

    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    return value


class DesktopAPI:
    """API exposed to the pywebview JavaScript bridge.

    All public methods return JSON-serializable values.
    The DuckDB connection uses read_only=True to allow parallel
    access with the daily sync process.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._window = None
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._tools: Optional[HealthTools] = None
        self._query_builder = QueryBuilder(_CONTRACTS_DIR)

    def set_window(self, window) -> None:
        self._window = window

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a read-only DuckDB connection."""
        if self._con is None:
            if not Path(self._db_path).exists():
                raise FileNotFoundError(f"Database not found: {self._db_path}")
            self._con = duckdb.connect(self._db_path, read_only=True)
        return self._con

    def _get_tools(self) -> HealthTools:
        """Get or create a HealthTools instance."""
        if self._tools is None:
            self._tools = HealthTools(self._get_connection())
        return self._tools

    # ------------------------------------------------------------------
    # Dashboard
    # ------------------------------------------------------------------

    def get_dashboard_data(self) -> dict[str, Any]:
        """Return all dashboard data in a single call.

        Returns a dict with:
        - health_score: today's composite score + status
        - kpis: latest values for sleep, readiness, steps, resting HR
        - sparklines: 7-day data series for each KPI
        - trends: 30-day sleep + readiness overlay data
        - alerts: any active alerts/anomalies
        """
        try:
            con = self._get_connection()
        except FileNotFoundError:
            return {
                "error": "no_database",
                "message": f"Database not found: {self._db_path}",
            }

        today = date.today()
        start_7d = (today - timedelta(days=7)).isoformat()
        start_30d = (today - timedelta(days=30)).isoformat()
        end = today.isoformat()

        result: dict[str, Any] = {
            "timestamp": today.isoformat(),
            "health_score": self._get_health_score(con, end),
            "kpis": self._get_kpis(con, end),
            "sparklines": self._get_sparklines(con, start_7d, end),
            "trends": self._get_trends(con, start_30d, end),
            "alerts": self._get_alerts(con, start_7d, end),
        }
        return _sanitize(result)

    def _get_health_score(self, con: duckdb.DuckDBPyConnection, end: str) -> dict:
        """Compute composite health score for the latest day with data."""
        try:
            row = con.execute(
                """
                SELECT
                    COALESCE(s.day, r.day, a.day) AS day,
                    s.sleep_score,
                    r.readiness_score,
                    a.activity_score,
                    ROUND(
                        (COALESCE(s.sleep_score, 0) * 0.35 +
                         COALESCE(r.readiness_score, 0) * 0.35 +
                         COALESCE(a.activity_score, 0) * 0.30), 1
                    ) AS health_score
                FROM silver.daily_sleep s
                FULL OUTER JOIN silver.daily_readiness r ON s.day = r.day
                FULL OUTER JOIN silver.daily_activity a ON COALESCE(s.day, r.day) = a.day
                WHERE COALESCE(s.day, r.day, a.day) <= ?
                ORDER BY COALESCE(s.day, r.day, a.day) DESC
                LIMIT 1
                """,
                [end],
            ).fetchone()
        except Exception as exc:
            logger.debug("Health score query failed: %s", exc)
            return {"score": None, "status": "unknown", "day": None}

        if not row:
            return {"score": None, "status": "unknown", "day": None}

        score = row[4]
        if score is None:
            status = "unknown"
        elif score >= 85:
            status = "excellent"
        elif score >= 70:
            status = "good"
        elif score >= 55:
            status = "fair"
        else:
            status = "poor"

        return {
            "score": score,
            "status": status,
            "day": str(row[0]),
            "components": {
                "sleep": row[1],
                "readiness": row[2],
                "activity": row[3],
            },
        }

    def _get_kpis(self, con: duckdb.DuckDBPyConnection, end: str) -> dict:
        """Get latest values for the 4 main KPIs."""
        kpis = {}

        # Sleep Score
        kpis["sleep_score"] = self._latest_value(
            con,
            "SELECT day, sleep_score FROM silver.daily_sleep WHERE day <= ? ORDER BY day DESC LIMIT 1",
            end,
            "Sleep Score",
            "score",
        )

        # Readiness Score
        kpis["readiness_score"] = self._latest_value(
            con,
            "SELECT day, readiness_score FROM silver.daily_readiness WHERE day <= ? ORDER BY day DESC LIMIT 1",
            end,
            "Readiness",
            "score",
        )

        # Steps
        kpis["steps"] = self._latest_value(
            con,
            "SELECT day, steps FROM silver.daily_activity WHERE day <= ? ORDER BY day DESC LIMIT 1",
            end,
            "Steps",
            "steps",
        )

        # Resting Heart Rate
        kpis["resting_hr"] = self._latest_hr(con, end)

        return kpis

    def _latest_value(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        end: str,
        label: str,
        unit: str,
    ) -> dict:
        """Execute a latest-value query and return structured result."""
        try:
            row = con.execute(sql, [end]).fetchone()
        except Exception as exc:
            logger.debug("KPI query failed for %s: %s", label, exc)
            return {"label": label, "value": None, "day": None, "unit": unit}

        if not row:
            return {"label": label, "value": None, "day": None, "unit": unit}

        return {
            "label": label,
            "value": row[1],
            "day": str(row[0]),
            "unit": unit,
        }

    def _latest_hr(self, con: duckdb.DuckDBPyConnection, end: str) -> dict:
        """Get latest resting heart rate (min bpm per day)."""
        try:
            row = con.execute(
                """
                SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr
                FROM silver.heart_rate
                WHERE DATE(timestamp) <= ?
                GROUP BY DATE(timestamp)
                ORDER BY day DESC
                LIMIT 1
                """,
                [end],
            ).fetchone()
        except Exception as exc:
            logger.debug("Resting HR query failed: %s", exc)
            return {"label": "Resting HR", "value": None, "day": None, "unit": "bpm"}

        if not row:
            return {"label": "Resting HR", "value": None, "day": None, "unit": "bpm"}

        return {
            "label": "Resting HR",
            "value": row[1],
            "day": str(row[0]),
            "unit": "bpm",
        }

    def _get_sparklines(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict:
        """Get 7-day data series for sparkline charts."""
        sparklines = {}

        # Sleep scores
        sparklines["sleep_score"] = self._series(
            con,
            "SELECT day, sleep_score FROM silver.daily_sleep WHERE day BETWEEN ? AND ? ORDER BY day",
            start,
            end,
        )

        # Readiness scores
        sparklines["readiness_score"] = self._series(
            con,
            "SELECT day, readiness_score FROM silver.daily_readiness WHERE day BETWEEN ? AND ? ORDER BY day",
            start,
            end,
        )

        # Steps
        sparklines["steps"] = self._series(
            con,
            "SELECT day, steps FROM silver.daily_activity WHERE day BETWEEN ? AND ? ORDER BY day",
            start,
            end,
        )

        # Resting HR
        sparklines["resting_hr"] = self._series(
            con,
            """
            SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr
            FROM silver.heart_rate
            WHERE DATE(timestamp) BETWEEN ? AND ?
            GROUP BY DATE(timestamp)
            ORDER BY day
            """,
            start,
            end,
        )

        return sparklines

    def _series(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        start: str,
        end: str,
    ) -> list[dict]:
        """Execute a time series query and return [{day, value}, ...]."""
        try:
            rows = con.execute(sql, [start, end]).fetchall()
        except Exception as exc:
            logger.debug("Series query failed: %s", exc)
            return []

        return [{"day": str(r[0]), "value": r[1]} for r in rows if r[1] is not None]

    def _get_trends(self, con: duckdb.DuckDBPyConnection, start: str, end: str) -> dict:
        """Get 30-day trend data for sleep + readiness overlay chart."""
        try:
            rows = con.execute(
                """
                SELECT
                    COALESCE(s.day, r.day) AS day,
                    s.sleep_score,
                    r.readiness_score
                FROM silver.daily_sleep s
                FULL OUTER JOIN silver.daily_readiness r ON s.day = r.day
                WHERE COALESCE(s.day, r.day) BETWEEN ? AND ?
                ORDER BY COALESCE(s.day, r.day)
                """,
                [start, end],
            ).fetchall()
        except Exception as exc:
            logger.debug("Trends query failed: %s", exc)
            return {"days": [], "sleep": [], "readiness": []}

        days = []
        sleep = []
        readiness = []
        for row in rows:
            days.append(str(row[0]))
            sleep.append(row[1])
            readiness.append(row[2])

        return {"days": days, "sleep": sleep, "readiness": readiness}

    def _get_alerts(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> list[dict]:
        """Check for notable patterns in recent data."""
        alerts = []

        # Low sleep score
        try:
            row = con.execute(
                """
                SELECT day, sleep_score
                FROM silver.daily_sleep
                WHERE day BETWEEN ? AND ? AND sleep_score < 60
                ORDER BY day DESC
                LIMIT 1
                """,
                [start, end],
            ).fetchone()
            if row:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "sleep_score",
                        "message": f"Low sleep score ({row[1]}) on {row[0]}",
                    }
                )
        except Exception:
            pass

        # Low readiness
        try:
            row = con.execute(
                """
                SELECT day, readiness_score
                FROM silver.daily_readiness
                WHERE day BETWEEN ? AND ? AND readiness_score < 60
                ORDER BY day DESC
                LIMIT 1
                """,
                [start, end],
            ).fetchone()
            if row:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "readiness_score",
                        "message": f"Low readiness ({row[1]}) on {row[0]}",
                    }
                )
        except Exception:
            pass

        # Elevated resting HR (above 70 bpm)
        try:
            row = con.execute(
                """
                SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr
                FROM silver.heart_rate
                WHERE DATE(timestamp) BETWEEN ? AND ?
                GROUP BY DATE(timestamp)
                HAVING MIN(bpm) > 70
                ORDER BY day DESC
                LIMIT 1
                """,
                [start, end],
            ).fetchone()
            if row:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "resting_hr",
                        "message": f"Elevated resting HR ({row[1]} bpm) on {row[0]}",
                    }
                )
        except Exception:
            pass

        return alerts

    # ------------------------------------------------------------------
    # Metric query (for Data Explorer)
    # ------------------------------------------------------------------

    def query_metric(
        self,
        metric: str,
        computation: str = "daily_value",
        date_range: str = "last_7_days",
    ) -> dict[str, Any]:
        """Query a metric via its semantic contract.

        Returns structured data for the frontend.
        """
        try:
            tools = self._get_tools()
            result = tools.query_health(metric, date_range, computation)
            return {"result": result, "error": None}
        except Exception as exc:
            return {"result": None, "error": str(exc)}

    def list_metrics(self) -> list[str]:
        """Return list of available metric names."""
        return self._query_builder.list_metrics()

    # ------------------------------------------------------------------
    # System
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Return system status for the frontend."""
        db_exists = Path(self._db_path).exists()
        return {
            "db_path": self._db_path,
            "db_exists": db_exists,
            "db_size_mb": (
                round(Path(self._db_path).stat().st_size / 1024 / 1024, 1)
                if db_exists
                else 0
            ),
        }
