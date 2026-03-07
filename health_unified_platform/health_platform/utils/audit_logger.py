"""
audit_logger.py — Platform-agnostic audit logging for the health platform.

Persists structured job/table run records to DuckDB (local) or Delta (Databricks).
Auto-detects backend: if `spark` is in builtins, uses Databricks; otherwise DuckDB.

Usage (context manager — preferred):
    with AuditLogger("ingestion_engine", "bronze", "apple_health") as audit:
        audit.log_table("bronze.stg_heart_rate", "CREATE_OR_REPLACE", rows_after=1234)
        # finish(success) called automatically on clean exit

Usage (explicit):
    audit = AuditLogger("silver_runner", "silver", "oura")
    audit.start()
    audit.log_table("silver.heart_rate", "MERGE", rows_before=100, rows_after=150)
    audit.finish(status="success", rows_processed=50)

Non-fatal: all audit INSERT/UPDATE errors are logged as WARNING — never stop the pipeline.
"""

from __future__ import annotations

import builtins
import os
import uuid
import warnings
from datetime import datetime, timezone
from typing import Any, Optional

from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_db_path

logger = get_logger("audit_logger")

# Unity Catalog table references (Databricks)
_UC_JOB_RUNS_TABLE = "{catalog}.audit.job_runs"
_UC_TABLE_RUNS_TABLE = "{catalog}.audit.table_runs"


def _is_databricks() -> bool:
    return hasattr(builtins, "spark") or "DATABRICKS_RUNTIME_VERSION" in os.environ


def _get_catalog() -> str:
    env = os.environ.get("HEALTH_ENV", "dev")
    return "health-platform-prd" if env == "prd" else "health-platform-dev"


def _get_duckdb_path() -> str:
    return str(get_db_path())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AuditLogger:
    """
    Structured audit logger for health platform jobs.

    Records job-level and table-level run metadata to audit.job_runs
    and audit.table_runs, using the appropriate backend.
    """

    def __init__(self, job_name: str, job_type: str, source_system: str):
        """
        Args:
            job_name:      Name of the script/notebook (e.g. 'ingestion_engine')
            job_type:      Layer being processed: 'bronze', 'silver', 'gold', 'extract'
            source_system: Source identifier (e.g. 'apple_health', 'oura')
        """
        self.job_name = job_name
        self.job_type = job_type
        self.source_system = source_system
        self.env = os.environ.get("HEALTH_ENV", "dev")

        self.job_id = str(uuid.uuid4())
        self.start_time: Optional[datetime] = None
        self._con: Any = None  # duckdb connection (local only)

        self._databricks = _is_databricks()

    # ------------------------------------------------------------------
    # Context manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "AuditLogger":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is None:
            self.finish(status="success")
        else:
            self.finish(status="error", error_message=str(exc_val))
        return False  # do not suppress exceptions

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Insert a 'running' record into audit.job_runs."""
        self.start_time = _now()
        try:
            if self._databricks:
                self._start_databricks()
            else:
                self._start_duckdb()
        except Exception as exc:
            warnings.warn(
                f"[AuditLogger] start() failed (non-fatal): {exc}", stacklevel=2
            )

    def log_table(
        self,
        table_name: str,
        operation: str,
        rows_before: Optional[int] = None,
        rows_after: Optional[int] = None,
        rows_changed: Optional[int] = None,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> str:
        """
        Insert a record into audit.table_runs.

        Returns the run_id (UUID) for the table run.
        """
        run_id = str(uuid.uuid4())
        start_time = _now()
        layer = self.job_type

        try:
            if self._databricks:
                self._log_table_databricks(
                    run_id,
                    table_name,
                    layer,
                    operation,
                    rows_before,
                    rows_after,
                    rows_changed,
                    start_time,
                    status,
                    error_message,
                )
            else:
                self._log_table_duckdb(
                    run_id,
                    table_name,
                    layer,
                    operation,
                    rows_before,
                    rows_after,
                    rows_changed,
                    start_time,
                    status,
                    error_message,
                )
        except Exception as exc:
            warnings.warn(
                f"[AuditLogger] log_table() failed (non-fatal): {exc}", stacklevel=2
            )

        return run_id

    def finish(
        self,
        status: str = "success",
        error_message: str | None = None,
        rows_processed: int | None = None,
        rows_inserted: int | None = None,
        rows_updated: int | None = None,
        rows_deleted: int | None = None,
    ) -> None:
        """Update audit.job_runs with final status and timing."""
        end_time = _now()
        duration = (
            round((end_time - self.start_time).total_seconds(), 3)
            if self.start_time
            else None
        )

        try:
            if self._databricks:
                self._finish_databricks(
                    status,
                    error_message,
                    rows_processed,
                    rows_inserted,
                    rows_updated,
                    rows_deleted,
                    end_time,
                    duration,
                )
            else:
                self._finish_duckdb(
                    status,
                    error_message,
                    rows_processed,
                    rows_inserted,
                    rows_updated,
                    rows_deleted,
                    end_time,
                    duration,
                )
        except Exception as exc:
            warnings.warn(
                f"[AuditLogger] finish() failed (non-fatal): {exc}", stacklevel=2
            )
        finally:
            if self._con:
                try:
                    self._con.close()
                except Exception:
                    pass
                self._con = None

    # ------------------------------------------------------------------
    # DuckDB backend
    # ------------------------------------------------------------------

    def _get_con(self):
        if self._con is None:
            import duckdb

            self._con = duckdb.connect(_get_duckdb_path())
        return self._con

    def _start_duckdb(self) -> None:
        con = self._get_con()
        con.execute(
            """
            INSERT INTO audit.job_runs (
                job_id, job_name, job_type, source_system, env, start_time, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'running')
            """,
            [
                self.job_id,
                self.job_name,
                self.job_type,
                self.source_system,
                self.env,
                self.start_time,
            ],
        )

    def _log_table_duckdb(
        self,
        run_id,
        table_name,
        layer,
        operation,
        rows_before,
        rows_after,
        rows_changed,
        start_time,
        status,
        error_message,
    ) -> None:
        con = self._get_con()
        end_time = _now()
        con.execute(
            """
            INSERT INTO audit.table_runs (
                run_id, job_id, table_name, layer, operation,
                rows_before, rows_after, rows_changed,
                start_time, end_time, status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                self.job_id,
                table_name,
                layer,
                operation,
                rows_before,
                rows_after,
                rows_changed,
                start_time,
                end_time,
                status,
                error_message,
            ],
        )

    def _finish_duckdb(
        self,
        status,
        error_message,
        rows_processed,
        rows_inserted,
        rows_updated,
        rows_deleted,
        end_time,
        duration,
    ) -> None:
        con = self._get_con()
        con.execute(
            """
            UPDATE audit.job_runs
            SET end_time = ?, duration_seconds = ?, status = ?,
                error_message = ?, rows_processed = ?,
                rows_inserted = ?, rows_updated = ?, rows_deleted = ?
            WHERE job_id = ?
            """,
            [
                end_time,
                duration,
                status,
                error_message,
                rows_processed,
                rows_inserted,
                rows_updated,
                rows_deleted,
                self.job_id,
            ],
        )

    # ------------------------------------------------------------------
    # Databricks / Spark backend
    # ------------------------------------------------------------------

    def _spark(self):
        return builtins.spark  # type: ignore[attr-defined]

    def _start_databricks(self) -> None:
        catalog = _get_catalog()
        job_runs_table = _UC_JOB_RUNS_TABLE.format(catalog=catalog)
        sql = f"""
            INSERT INTO `{job_runs_table}` (
                job_id, job_name, job_type, source_system, env, start_time, status
            ) VALUES (
                '{self.job_id}', '{self.job_name}', '{self.job_type}',
                '{self.source_system}', '{self.env}',
                TIMESTAMP '{self.start_time.isoformat()}', 'running'
            )
        """
        self._spark().sql(sql)

    def _log_table_databricks(
        self,
        run_id,
        table_name,
        layer,
        operation,
        rows_before,
        rows_after,
        rows_changed,
        start_time,
        status,
        error_message,
    ) -> None:
        catalog = _get_catalog()
        table_runs_table = _UC_TABLE_RUNS_TABLE.format(catalog=catalog)
        end_time = _now()

        def _val(v):
            return "NULL" if v is None else str(v)

        def _str_val(v):
            return "NULL" if v is None else f"'{v}'"

        sql = f"""
            INSERT INTO `{table_runs_table}` (
                run_id, job_id, table_name, layer, operation,
                rows_before, rows_after, rows_changed,
                start_time, end_time, status, error_message
            ) VALUES (
                '{run_id}', '{self.job_id}', '{table_name}', '{layer}', '{operation}',
                {_val(rows_before)}, {_val(rows_after)}, {_val(rows_changed)},
                TIMESTAMP '{start_time.isoformat()}', TIMESTAMP '{end_time.isoformat()}',
                '{status}', {_str_val(error_message)}
            )
        """
        self._spark().sql(sql)

    def _finish_databricks(
        self,
        status,
        error_message,
        rows_processed,
        rows_inserted,
        rows_updated,
        rows_deleted,
        end_time,
        duration,
    ) -> None:
        catalog = _get_catalog()
        job_runs_table = _UC_JOB_RUNS_TABLE.format(catalog=catalog)

        def _val(v):
            return "NULL" if v is None else str(v)

        def _str_val(v):
            return "NULL" if v is None else f"'{v}'"

        sql = f"""
            UPDATE `{job_runs_table}`
            SET end_time = TIMESTAMP '{end_time.isoformat()}',
                duration_seconds = {_val(duration)},
                status = '{status}',
                error_message = {_str_val(error_message)},
                rows_processed = {_val(rows_processed)},
                rows_inserted = {_val(rows_inserted)},
                rows_updated = {_val(rows_updated)},
                rows_deleted = {_val(rows_deleted)}
            WHERE job_id = '{self.job_id}'
        """
        self._spark().sql(sql)
