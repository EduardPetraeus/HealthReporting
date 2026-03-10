"""FHIR R4 Bundle generator for clinician export.

Queries DuckDB gold/silver layer, maps rows to FHIR resources via FhirMapper,
and assembles a complete FHIR R4 collection Bundle.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import duckdb
from health_platform.export.fhir.mapper import FhirMapper
from health_platform.utils.logging_config import get_logger

logger = get_logger("export.fhir.bundle")


def _get_db_path() -> str:
    """Resolve DuckDB database path from environment."""
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    env = os.environ.get("HEALTH_ENV", "dev")
    return str(Path.home() / "health_dw" / f"health_dw_{env}.db")


class FhirBundleGenerator:
    """Generates FHIR R4 Bundles from health platform data."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize with optional database path override.

        Args:
            db_path: Path to DuckDB file. Uses env-based default if not provided.
        """
        self.db_path = db_path or _get_db_path()
        self.mapper = FhirMapper()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a read-only DuckDB connection."""
        return duckdb.connect(self.db_path, read_only=True)

    def _query_to_dicts(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        params: list[Any],
    ) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        try:
            result = con.execute(sql, params).fetchall()
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

    def _load_patient_profile(
        self, con: duckdb.DuckDBPyConnection
    ) -> dict[str, dict[str, str]]:
        """Load patient profile as nested dict {category: {key: value}}."""
        try:
            result = con.execute(
                "SELECT category, profile_key, profile_value "
                "FROM agent.patient_profile ORDER BY category, profile_key"
            ).fetchall()
            profile: dict[str, dict[str, str]] = {}
            for category, key, value in result:
                if category not in profile:
                    profile[category] = {}
                profile[category][key] = value
            return profile
        except Exception:
            logger.debug("Failed to load patient profile", exc_info=True)
            return {}

    def _load_vitals(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Load daily vitals from gold or silver layer.

        Tries gold.fct_daily_vitals_summary first, then falls back to
        joining silver tables.
        """
        # Try gold layer first
        gold_rows = self._query_to_dicts(
            con,
            "SELECT * FROM gold.fct_daily_vitals_summary "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        if gold_rows:
            return gold_rows

        # Fallback: query silver tables individually
        logger.debug("Gold layer not available, falling back to silver tables")
        vitals: list[dict[str, Any]] = []

        # Sleep scores
        sleep_rows = self._query_to_dicts(
            con,
            "SELECT day, sleep_score FROM silver.daily_sleep "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        for row in sleep_rows:
            vitals.append(row)

        # Readiness scores
        readiness_rows = self._query_to_dicts(
            con,
            "SELECT day, readiness_score FROM silver.daily_readiness "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        for row in readiness_rows:
            vitals.append(row)

        # Activity / steps
        activity_rows = self._query_to_dicts(
            con,
            "SELECT day, activity_score, steps FROM silver.daily_activity "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        for row in activity_rows:
            vitals.append(row)

        # SpO2
        spo2_rows = self._query_to_dicts(
            con,
            "SELECT day, spo2_avg_pct FROM silver.daily_spo2 "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        for row in spo2_rows:
            vitals.append(row)

        # Weight
        weight_rows = self._query_to_dicts(
            con,
            "SELECT datetime AS day, weight_kg FROM silver.weight "
            "WHERE datetime >= ? AND datetime <= ? ORDER BY datetime",
            [start_date, end_date],
        )
        for row in weight_rows:
            vitals.append(row)

        return vitals

    def _load_health_scores(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Load health scores from gold or silver layer."""
        gold_rows = self._query_to_dicts(
            con,
            "SELECT * FROM gold.fct_daily_health_score "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        if gold_rows:
            return gold_rows

        # Fallback to readiness scores from silver
        return self._query_to_dicts(
            con,
            "SELECT day, readiness_score FROM silver.daily_readiness "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )

    def _load_lab_results(
        self,
        con: duckdb.DuckDBPyConnection,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Load lab results from gold or silver layer."""
        gold_rows = self._query_to_dicts(
            con,
            "SELECT * FROM gold.fct_lab_result "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )
        if gold_rows:
            return gold_rows

        # Fallback to silver lab_biomarkers if it exists
        return self._query_to_dicts(
            con,
            "SELECT * FROM silver.lab_biomarkers "
            "WHERE day >= ? AND day <= ? ORDER BY day",
            [start_date, end_date],
        )

    def generate_bundle(
        self,
        start_date: date,
        end_date: date,
        resources: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """Generate a complete FHIR R4 Bundle for the given date range.

        Args:
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
            resources: List of resource types to include.
                       Default: ['Patient', 'Observation'].

        Returns:
            FHIR R4 Bundle dict with resourceType, type, id, timestamp, entry[].
        """
        if resources is None:
            resources = ["Patient", "Observation"]

        entries: list[dict[str, Any]] = []
        con = self._get_connection()

        try:
            # Patient resource
            if "Patient" in resources:
                profile = self._load_patient_profile(con)
                if profile:
                    patient = self.mapper.map_patient(profile)
                    entries.append({"resource": patient})

            # Observation resources (vitals, scores, labs)
            if "Observation" in resources:
                # Vitals
                vitals_rows = self._load_vitals(con, start_date, end_date)
                for row in vitals_rows:
                    observations = self.mapper.map_vital_signs(row)
                    for obs in observations:
                        entries.append({"resource": obs})

                # Health scores
                score_rows = self._load_health_scores(con, start_date, end_date)
                for row in score_rows:
                    obs = self.mapper.map_health_score(row)
                    entries.append({"resource": obs})

                # Lab results
                lab_rows = self._load_lab_results(con, start_date, end_date)
                for row in lab_rows:
                    obs = self.mapper.map_lab_result(row)
                    entries.append({"resource": obs})
        finally:
            con.close()

        bundle: dict[str, Any] = {
            "resourceType": "Bundle",
            "id": str(uuid.uuid4()),
            "type": "collection",
            "timestamp": datetime.now().isoformat(),
            "total": len(entries),
            "entry": entries,
        }

        logger.info(
            "Generated FHIR Bundle: %d entries for %s to %s",
            len(entries),
            start_date,
            end_date,
        )

        return bundle
