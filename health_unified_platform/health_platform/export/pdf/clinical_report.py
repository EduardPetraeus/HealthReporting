"""Clinical PDF report generator using WeasyPrint.

Queries DuckDB for health data and renders a professional clinical report
suitable for sharing with healthcare providers.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import duckdb
from health_platform.utils.logging_config import get_logger
from jinja2 import Environment, FileSystemLoader

logger = get_logger("export.pdf.clinical_report")

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

# All available report sections
ALL_SECTIONS = [
    "patient_info",
    "vitals_summary",
    "lab_results",
    "trend_analysis",
    "medication_list",
]


def _get_db_path() -> str:
    """Resolve DuckDB database path from environment."""
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    env = os.environ.get("HEALTH_ENV", "dev")
    return str(Path.home() / "health_dw" / f"health_dw_{env}.db")


class ClinicalReportGenerator:
    """Generates clinical PDF reports from health platform data."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize with optional database path override.

        Args:
            db_path: Path to DuckDB file. Uses env-based default if not provided.
        """
        self.db_path = db_path or _get_db_path()
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            autoescape=True,
        )

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a read-only DuckDB connection."""
        return duckdb.connect(self.db_path, read_only=True)

    def _safe_query(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        params: Optional[list[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Execute a query safely, returning list of dicts or empty list on error."""
        try:
            result = con.execute(sql, params or []).fetchall()
            col_names = [c[0] for c in con.description]
            rows = []
            for row in result:
                row_dict = {}
                for i, col in enumerate(col_names):
                    val = row[i]
                    if isinstance(val, (date, datetime)):
                        val = val.isoformat()
                    row_dict[col] = val
                rows.append(row_dict)
            return rows
        except duckdb.CatalogException:
            logger.debug("Table not found for query: %s", sql[:80])
            return []
        except Exception:
            logger.debug("Query failed: %s", sql[:80], exc_info=True)
            return []

    def _load_patient_info(self, con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        """Load patient demographics from agent.patient_profile."""
        try:
            result = con.execute(
                "SELECT category, profile_key, profile_value "
                "FROM agent.patient_profile ORDER BY category, profile_key"
            ).fetchall()
            all_data: dict[str, dict[str, str]] = {}
            for category, key, value in result:
                if category not in all_data:
                    all_data[category] = {}
                all_data[category][key] = value

            demographics = all_data.get("demographics", {})
            return {
                "patient_name": demographics.get(
                    "name",
                    f"{demographics.get('first_name', '')} {demographics.get('last_name', '')}".strip()
                    or "Patient",
                ),
                "date_of_birth": demographics.get("date_of_birth", "N/A"),
                "biological_sex": demographics.get("biological_sex", "N/A"),
                "age": demographics.get("age", "N/A"),
                "demographics": demographics,
            }
        except Exception:
            logger.debug("Failed to load patient info", exc_info=True)
            return {
                "patient_name": "Patient",
                "date_of_birth": "N/A",
                "biological_sex": "N/A",
                "age": "N/A",
                "demographics": {},
            }

    def _load_vitals_data(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        """Load vitals summary and daily breakdowns."""
        # Summary aggregates
        summary: dict[str, Any] = {}

        # Sleep
        sleep_rows = self._safe_query(
            con,
            "SELECT AVG(sleep_score) AS avg_val, MIN(sleep_score) AS min_val, "
            "MAX(sleep_score) AS max_val FROM silver.daily_sleep "
            "WHERE day >= ? AND day <= ?",
            [start_date, end_date],
        )
        if sleep_rows and sleep_rows[0].get("avg_val") is not None:
            summary["avg_sleep_score"] = sleep_rows[0]["avg_val"]

        # Readiness
        readiness_rows = self._safe_query(
            con,
            "SELECT AVG(readiness_score) AS avg_val FROM silver.daily_readiness "
            "WHERE day >= ? AND day <= ?",
            [start_date, end_date],
        )
        if readiness_rows and readiness_rows[0].get("avg_val") is not None:
            summary["avg_readiness_score"] = readiness_rows[0]["avg_val"]

        # Steps
        steps_rows = self._safe_query(
            con,
            "SELECT AVG(steps) AS avg_val FROM silver.daily_activity "
            "WHERE day >= ? AND day <= ?",
            [start_date, end_date],
        )
        if steps_rows and steps_rows[0].get("avg_val") is not None:
            summary["avg_steps"] = steps_rows[0]["avg_val"]

        # SpO2
        spo2_rows = self._safe_query(
            con,
            "SELECT AVG(spo2_avg_pct) AS avg_val FROM silver.daily_spo2 "
            "WHERE day >= ? AND day <= ?",
            [start_date, end_date],
        )
        if spo2_rows and spo2_rows[0].get("avg_val") is not None:
            summary["avg_spo2"] = spo2_rows[0]["avg_val"]

        # Latest weight
        weight_rows = self._safe_query(
            con,
            "SELECT weight_kg FROM silver.weight "
            "WHERE datetime >= ? AND datetime <= ? "
            "ORDER BY datetime DESC LIMIT 1",
            [start_date, end_date],
        )
        if weight_rows:
            summary["latest_weight"] = weight_rows[0]["weight_kg"]

        # Daily breakdown: join sleep + readiness + activity + spo2
        daily_rows = self._safe_query(
            con,
            "SELECT s.day, s.sleep_score, r.readiness_score, "
            "a.steps, sp.spo2_avg_pct AS spo2 "
            "FROM silver.daily_sleep s "
            "LEFT JOIN silver.daily_readiness r ON s.day = r.day "
            "LEFT JOIN silver.daily_activity a ON s.day = a.day "
            "LEFT JOIN silver.daily_spo2 sp ON s.day = sp.day "
            "WHERE s.day >= ? AND s.day <= ? ORDER BY s.day",
            [start_date, end_date],
        )

        return {
            "vitals_summary": summary if summary else None,
            "vitals_daily": daily_rows if daily_rows else None,
        }

    def _load_lab_results(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Load lab biomarker results."""
        # Try gold layer first
        gold = self._safe_query(
            con,
            "SELECT * FROM gold.fct_lab_result "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        if gold:
            return gold

        # Fallback to silver
        return self._safe_query(
            con,
            "SELECT * FROM silver.lab_biomarkers "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )

    def _compute_trends(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Compute simple trend analysis for key metrics."""
        trends: list[dict[str, Any]] = []

        metric_queries = {
            "sleep_score": (
                "SELECT day, sleep_score AS val FROM silver.daily_sleep "
                "WHERE day >= ? AND day <= ? ORDER BY day"
            ),
            "readiness_score": (
                "SELECT day, readiness_score AS val FROM silver.daily_readiness "
                "WHERE day >= ? AND day <= ? ORDER BY day"
            ),
            "steps": (
                "SELECT day, steps AS val FROM silver.daily_activity "
                "WHERE day >= ? AND day <= ? ORDER BY day"
            ),
        }

        for metric, sql in metric_queries.items():
            rows = self._safe_query(con, sql, [start_date, end_date])
            if len(rows) < 2:
                continue

            values = [r["val"] for r in rows if r["val"] is not None]
            if not values:
                continue

            avg_val = sum(values) / len(values)
            min_val = min(values)
            max_val = max(values)

            # Simple trend: compare first half average to second half average
            mid = len(values) // 2
            first_half = values[:mid] if mid > 0 else values
            second_half = values[mid:] if mid > 0 else values
            first_avg = sum(first_half) / len(first_half)
            second_avg = sum(second_half) / len(second_half)

            if second_avg > first_avg * 1.05:
                direction = "up"
            elif second_avg < first_avg * 0.95:
                direction = "down"
            else:
                direction = "stable"

            trends.append(
                {
                    "metric": metric,
                    "avg": avg_val,
                    "min": min_val,
                    "max": max_val,
                    "direction": direction,
                    "note": "",
                }
            )

        return trends

    def _load_medications(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Load medication/supplement list from silver.supplement_log."""
        return self._safe_query(
            con,
            "SELECT supplement_name AS name, dosage, frequency, "
            "MAX(day) AS last_date "
            "FROM silver.supplement_log "
            "WHERE day >= ? AND day <= ? "
            "GROUP BY supplement_name, dosage, frequency "
            "ORDER BY supplement_name",
            [start_date, end_date],
        )

    def generate_report(
        self,
        start_date: date,
        end_date: date,
        sections: Optional[list[str]] = None,
    ) -> bytes:
        """Generate a clinical PDF report for the given date range.

        Args:
            start_date: Start of report period (inclusive).
            end_date: End of report period (inclusive).
            sections: List of sections to include. Default: all sections.

        Returns:
            PDF file content as bytes.
        """
        if sections is None:
            sections = ALL_SECTIONS

        # Validate sections
        sections = [s for s in sections if s in ALL_SECTIONS]
        if not sections:
            sections = ALL_SECTIONS

        template_context: dict[str, Any] = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "report_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "sections": sections,
        }

        con = self._get_connection()
        try:
            # Load patient info
            if "patient_info" in sections:
                patient_info = self._load_patient_info(con)
                template_context.update(patient_info)
            else:
                template_context["patient_name"] = "Patient"

            # Load vitals
            if "vitals_summary" in sections:
                vitals_data = self._load_vitals_data(con, start_date, end_date)
                template_context.update(vitals_data)

            # Load labs
            if "lab_results" in sections:
                template_context["lab_results"] = self._load_lab_results(
                    con, start_date, end_date
                )

            # Compute trends
            if "trend_analysis" in sections:
                template_context["trends"] = self._compute_trends(
                    con, start_date, end_date
                )

            # Load medications
            if "medication_list" in sections:
                template_context["medications"] = self._load_medications(
                    con, start_date, end_date
                )
        finally:
            con.close()

        # Render HTML
        template = self.jinja_env.get_template("clinical_report.html")
        html_content = template.render(**template_context)

        # Convert to PDF using WeasyPrint
        from weasyprint import HTML

        pdf_bytes = HTML(string=html_content).write_pdf()

        logger.info(
            "Generated clinical PDF report: %d bytes, %s to %s, sections=%s",
            len(pdf_bytes),
            start_date,
            end_date,
            sections,
        )

        return pdf_bytes
