"""PDF report generator for HealthReporting.

Generates PDF health reports from silver layer data in DuckDB.
Uses WeasyPrint (HTML -> PDF) for rendering with Jinja2 templates.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Optional

import duckdb
from jinja2 import Environment, FileSystemLoader

from health_platform.utils.logging_config import get_logger

logger = get_logger("desktop.reports.generator")

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

# All valid report sections
VALID_SECTIONS = (
    "summary",
    "vitals",
    "sleep",
    "activity",
    "nutrition",
    "trends",
    "alerts",
)


class ReportGenerator:
    """Generate PDF health reports from DuckDB silver layer data.

    Uses Jinja2 templates rendered to HTML and converted to PDF
    via WeasyPrint.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._env = Environment(
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            autoescape=True,
        )

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a read-only DuckDB connection."""
        if self._con is None:
            self._con = duckdb.connect(self._db_path, read_only=True)
        return self._con

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._con is not None:
            self._con.close()
            self._con = None

    def generate_report(
        self,
        start_date: str,
        end_date: str,
        sections: list[str] | None = None,
    ) -> bytes:
        """Generate a PDF health report for the given date range.

        Args:
            start_date: ISO date string (YYYY-MM-DD).
            end_date: ISO date string (YYYY-MM-DD).
            sections: List of section names to include.
                      Defaults to all sections.

        Returns:
            PDF file contents as bytes.
        """
        from weasyprint import HTML

        if sections is None:
            sections = list(VALID_SECTIONS)

        # Validate sections
        sections = [s for s in sections if s in VALID_SECTIONS]
        if not sections:
            sections = list(VALID_SECTIONS)

        logger.info(
            "Generating report: %s to %s, sections: %s",
            start_date,
            end_date,
            sections,
        )

        con = self._get_connection()
        context = self._build_context(con, start_date, end_date, sections)

        template = self._env.get_template("report.html")
        html_content = template.render(**context)

        pdf_bytes = HTML(string=html_content).write_pdf()
        logger.info("Report generated: %d bytes", len(pdf_bytes))
        return pdf_bytes

    def generate_html(
        self,
        start_date: str,
        end_date: str,
        sections: list[str] | None = None,
    ) -> str:
        """Generate HTML report (useful for preview).

        Same arguments as generate_report, returns HTML string.
        """
        if sections is None:
            sections = list(VALID_SECTIONS)
        sections = [s for s in sections if s in VALID_SECTIONS]
        if not sections:
            sections = list(VALID_SECTIONS)

        con = self._get_connection()
        context = self._build_context(con, start_date, end_date, sections)

        template = self._env.get_template("report.html")
        return template.render(**context)

    def _build_context(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: str,
        end_date: str,
        sections: list[str],
    ) -> dict[str, Any]:
        """Build the full template context by querying all requested sections."""
        context: dict[str, Any] = {
            "start_date": start_date,
            "end_date": end_date,
            "generated_at": date.today().isoformat(),
            "sections": sections,
        }

        if "summary" in sections:
            context["summary"] = self._query_summary(con, start_date, end_date)

        if "vitals" in sections:
            context["vitals"] = self._query_vitals(con, start_date, end_date)

        if "sleep" in sections:
            context["sleep"] = self._query_sleep(con, start_date, end_date)

        if "activity" in sections:
            context["activity"] = self._query_activity(con, start_date, end_date)

        if "nutrition" in sections:
            context["nutrition"] = self._query_nutrition(con, start_date, end_date)

        if "trends" in sections:
            context["trends"] = self._query_trends(con, start_date, end_date)

        if "alerts" in sections:
            context["alerts"] = self._query_alerts(con, start_date, end_date)

        return context

    # ------------------------------------------------------------------
    # Section queries
    # ------------------------------------------------------------------

    def _query_summary(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict[str, Any]:
        """Query summary data: overall health score and period stats."""
        result: dict[str, Any] = {
            "avg_health_score": None,
            "days_with_data": 0,
            "avg_sleep": None,
            "avg_readiness": None,
            "avg_activity": None,
            "total_steps": 0,
            "avg_resting_hr": None,
        }

        try:
            row = con.execute(
                """
                SELECT
                    COUNT(DISTINCT COALESCE(s.day, r.day, a.day)) AS days,
                    ROUND(AVG(s.sleep_score), 1) AS avg_sleep,
                    ROUND(AVG(r.readiness_score), 1) AS avg_readiness,
                    ROUND(AVG(a.activity_score), 1) AS avg_activity,
                    COALESCE(SUM(a.steps), 0) AS total_steps
                FROM silver.daily_sleep s
                FULL OUTER JOIN silver.daily_readiness r ON s.day = r.day
                FULL OUTER JOIN silver.daily_activity a ON COALESCE(s.day, r.day) = a.day
                WHERE COALESCE(s.day, r.day, a.day) BETWEEN ? AND ?
                """,
                [start, end],
            ).fetchone()
            if row:
                result["days_with_data"] = row[0] or 0
                result["avg_sleep"] = _to_float(row[1])
                result["avg_readiness"] = _to_float(row[2])
                result["avg_activity"] = _to_float(row[3])
                result["total_steps"] = int(row[4]) if row[4] else 0

                # Compute average health score
                scores = [
                    s
                    for s in [
                        result["avg_sleep"],
                        result["avg_readiness"],
                        result["avg_activity"],
                    ]
                    if s is not None
                ]
                if scores:
                    weights = [0.35, 0.35, 0.30]
                    vals = [
                        result["avg_sleep"],
                        result["avg_readiness"],
                        result["avg_activity"],
                    ]
                    weighted = sum((v or 0) * w for v, w in zip(vals, weights))
                    result["avg_health_score"] = round(weighted, 1)
        except Exception as exc:
            logger.debug("Summary query failed: %s", exc)

        # Average resting HR
        try:
            hr_row = con.execute(
                """
                SELECT ROUND(AVG(min_bpm), 1) FROM (
                    SELECT DATE(timestamp) AS day, MIN(bpm) AS min_bpm
                    FROM silver.heart_rate
                    WHERE DATE(timestamp) BETWEEN ? AND ?
                    GROUP BY DATE(timestamp)
                )
                """,
                [start, end],
            ).fetchone()
            if hr_row and hr_row[0] is not None:
                result["avg_resting_hr"] = _to_float(hr_row[0])
        except Exception as exc:
            logger.debug("Summary HR query failed: %s", exc)

        return result

    def _query_vitals(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict[str, Any]:
        """Query vitals data: heart rate, weight."""
        result: dict[str, Any] = {
            "heart_rate_daily": [],
            "weight_entries": [],
        }

        # Daily resting HR
        try:
            rows = con.execute(
                """
                SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr,
                       MAX(bpm) AS max_hr, ROUND(AVG(bpm), 0) AS avg_hr
                FROM silver.heart_rate
                WHERE DATE(timestamp) BETWEEN ? AND ?
                GROUP BY DATE(timestamp)
                ORDER BY day
                """,
                [start, end],
            ).fetchall()
            result["heart_rate_daily"] = [
                {
                    "day": str(r[0]),
                    "resting_hr": int(r[1]) if r[1] else None,
                    "max_hr": int(r[2]) if r[2] else None,
                    "avg_hr": int(r[3]) if r[3] else None,
                }
                for r in rows
            ]
        except Exception as exc:
            logger.debug("Vitals HR query failed: %s", exc)

        # Weight
        try:
            rows = con.execute(
                """
                SELECT datetime, weight_kg, fat_mass_kg, muscle_mass_kg
                FROM silver.weight
                WHERE DATE(datetime) BETWEEN ? AND ?
                ORDER BY datetime
                """,
                [start, end],
            ).fetchall()
            result["weight_entries"] = [
                {
                    "date": str(r[0]),
                    "weight_kg": _to_float(r[1]),
                    "fat_mass_kg": _to_float(r[2]),
                    "muscle_mass_kg": _to_float(r[3]),
                }
                for r in rows
            ]
        except Exception as exc:
            logger.debug("Vitals weight query failed: %s", exc)

        return result

    def _query_sleep(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict[str, Any]:
        """Query sleep data."""
        result: dict[str, Any] = {"daily": [], "avg_score": None}

        try:
            rows = con.execute(
                """
                SELECT day, sleep_score,
                       contributor_deep_sleep, contributor_efficiency,
                       contributor_rem_sleep, contributor_total_sleep
                FROM silver.daily_sleep
                WHERE day BETWEEN ? AND ?
                ORDER BY day
                """,
                [start, end],
            ).fetchall()
            result["daily"] = [
                {
                    "day": str(r[0]),
                    "score": r[1],
                    "deep_sleep": r[2],
                    "efficiency": r[3],
                    "rem_sleep": r[4],
                    "total_sleep": r[5],
                }
                for r in rows
            ]
            scores = [r[1] for r in rows if r[1] is not None]
            if scores:
                result["avg_score"] = round(sum(scores) / len(scores), 1)
        except Exception as exc:
            logger.debug("Sleep query failed: %s", exc)

        return result

    def _query_activity(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict[str, Any]:
        """Query activity data."""
        result: dict[str, Any] = {
            "daily": [],
            "avg_steps": None,
            "total_steps": 0,
            "avg_score": None,
        }

        try:
            rows = con.execute(
                """
                SELECT day, activity_score, steps,
                       active_calories, total_calories
                FROM silver.daily_activity
                WHERE day BETWEEN ? AND ?
                ORDER BY day
                """,
                [start, end],
            ).fetchall()
            result["daily"] = [
                {
                    "day": str(r[0]),
                    "score": r[1],
                    "steps": r[2],
                    "active_calories": r[3],
                    "total_calories": r[4],
                }
                for r in rows
            ]
            steps_list = [r[2] for r in rows if r[2] is not None]
            scores = [r[1] for r in rows if r[1] is not None]
            if steps_list:
                result["avg_steps"] = round(sum(steps_list) / len(steps_list))
                result["total_steps"] = sum(steps_list)
            if scores:
                result["avg_score"] = round(sum(scores) / len(scores), 1)
        except Exception as exc:
            logger.debug("Activity query failed: %s", exc)

        return result

    def _query_nutrition(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict[str, Any]:
        """Query nutrition data. Returns empty if tables don't exist."""
        result: dict[str, Any] = {"daily": [], "available": False}

        try:
            rows = con.execute(
                """
                SELECT day, SUM(calories) AS calories,
                       SUM(protein_g) AS protein,
                       SUM(carbs_g) AS carbs,
                       SUM(fat_g) AS fat
                FROM silver.daily_meal
                WHERE day BETWEEN ? AND ?
                GROUP BY day
                ORDER BY day
                """,
                [start, end],
            ).fetchall()
            result["daily"] = [
                {
                    "day": str(r[0]),
                    "calories": _to_float(r[1]),
                    "protein": _to_float(r[2]),
                    "carbs": _to_float(r[3]),
                    "fat": _to_float(r[4]),
                }
                for r in rows
            ]
            result["available"] = len(result["daily"]) > 0
        except Exception as exc:
            logger.debug("Nutrition query failed (table may not exist): %s", exc)

        return result

    def _query_trends(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict[str, Any]:
        """Query trend data: sleep + readiness overlay."""
        result: dict[str, Any] = {"days": [], "sleep": [], "readiness": []}

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
            for row in rows:
                result["days"].append(str(row[0]))
                result["sleep"].append(row[1])
                result["readiness"].append(row[2])
        except Exception as exc:
            logger.debug("Trends query failed: %s", exc)

        return result

    def _query_alerts(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> list[dict[str, Any]]:
        """Check for notable patterns in the report period."""
        alerts: list[dict[str, Any]] = []

        # Low sleep scores
        try:
            rows = con.execute(
                """
                SELECT day, sleep_score FROM silver.daily_sleep
                WHERE day BETWEEN ? AND ? AND sleep_score < 60
                ORDER BY day
                """,
                [start, end],
            ).fetchall()
            for r in rows:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "Sleep",
                        "message": f"Low sleep score ({r[1]}) on {r[0]}",
                    }
                )
        except Exception:
            pass

        # Low readiness
        try:
            rows = con.execute(
                """
                SELECT day, readiness_score FROM silver.daily_readiness
                WHERE day BETWEEN ? AND ? AND readiness_score < 60
                ORDER BY day
                """,
                [start, end],
            ).fetchall()
            for r in rows:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "Readiness",
                        "message": f"Low readiness ({r[1]}) on {r[0]}",
                    }
                )
        except Exception:
            pass

        # Elevated resting HR
        try:
            rows = con.execute(
                """
                SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr
                FROM silver.heart_rate
                WHERE DATE(timestamp) BETWEEN ? AND ?
                GROUP BY DATE(timestamp)
                HAVING MIN(bpm) > 70
                ORDER BY day
                """,
                [start, end],
            ).fetchall()
            for r in rows:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "Heart Rate",
                        "message": f"Elevated resting HR ({r[1]} bpm) on {r[0]}",
                    }
                )
        except Exception:
            pass

        return alerts


def _to_float(value: Any) -> float | None:
    """Convert a value to float, returning None if not possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
