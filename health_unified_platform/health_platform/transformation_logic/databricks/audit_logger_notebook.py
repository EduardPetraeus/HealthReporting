# Databricks notebook source
# audit_logger_notebook.py
#
# Inline copy of AuditLogger for Databricks notebooks.
# Import with: %run ../../utils/audit_logger_notebook
#
# This file is intentionally self-contained — Databricks notebooks cannot
# import from local utils/ packages via normal Python import.
#
# PoC scale-up note:
#   In production, package audit_logger as a wheel and install on the cluster.
#   %run is acceptable for PoC but creates maintenance burden at scale.

# COMMAND ----------

import builtins
import os
import uuid
import warnings
from datetime import datetime, timezone


def _is_databricks() -> bool:
    return hasattr(builtins, "spark") or "DATABRICKS_RUNTIME_VERSION" in os.environ


def _get_catalog() -> str:
    env = os.environ.get("HEALTH_ENV", "dev")
    return "health-platform-prd" if env == "prd" else "health-platform-dev"


def _now() -> datetime:
    return datetime.now(timezone.utc)


_UC_JOB_RUNS_TABLE = "{catalog}.audit.job_runs"
_UC_TABLE_RUNS_TABLE = "{catalog}.audit.table_runs"


class AuditLogger:
    """
    Structured audit logger for health platform Databricks notebooks.
    Writes to Unity Catalog audit tables via spark.sql().
    """

    def __init__(self, job_name: str, job_type: str, source_system: str):
        self.job_name = job_name
        self.job_type = job_type
        self.source_system = source_system
        self.env = os.environ.get("HEALTH_ENV", "dev")
        self.job_id = str(uuid.uuid4())
        self.start_time = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.finish(status="success")
        else:
            self.finish(status="error", error_message=str(exc_val))
        return False

    def start(self) -> None:
        self.start_time = _now()
        try:
            catalog = _get_catalog()
            job_runs_table = _UC_JOB_RUNS_TABLE.format(catalog=catalog)
            spark.sql(
                f"""
                INSERT INTO `{job_runs_table}` (
                    job_id, job_name, job_type, source_system, env, start_time, status
                ) VALUES (
                    '{self.job_id}', '{self.job_name}', '{self.job_type}',
                    '{self.source_system}', '{self.env}',
                    TIMESTAMP '{self.start_time.isoformat()}', 'running'
                )
            """
            )
        except Exception as exc:
            warnings.warn(f"[AuditLogger] start() failed (non-fatal): {exc}")

    def log_table(
        self,
        table_name: str,
        operation: str,
        rows_before=None,
        rows_after=None,
        rows_changed=None,
        status: str = "success",
        error_message=None,
    ) -> str:
        run_id = str(uuid.uuid4())
        start_time = _now()
        end_time = _now()

        def _val(v):
            return "NULL" if v is None else str(v)

        def _str_val(v):
            return "NULL" if v is None else f"'{v}'"

        try:
            catalog = _get_catalog()
            table_runs_table = _UC_TABLE_RUNS_TABLE.format(catalog=catalog)
            spark.sql(
                f"""
                INSERT INTO `{table_runs_table}` (
                    run_id, job_id, table_name, layer, operation,
                    rows_before, rows_after, rows_changed,
                    start_time, end_time, status, error_message
                ) VALUES (
                    '{run_id}', '{self.job_id}', '{table_name}', '{self.job_type}', '{operation}',
                    {_val(rows_before)}, {_val(rows_after)}, {_val(rows_changed)},
                    TIMESTAMP '{start_time.isoformat()}', TIMESTAMP '{end_time.isoformat()}',
                    '{status}', {_str_val(error_message)}
                )
            """
            )
        except Exception as exc:
            warnings.warn(f"[AuditLogger] log_table() failed (non-fatal): {exc}")

        return run_id

    def finish(
        self,
        status: str = "success",
        error_message=None,
        rows_processed=None,
        rows_inserted=None,
        rows_updated=None,
        rows_deleted=None,
    ) -> None:
        end_time = _now()
        duration = (
            round((end_time - self.start_time).total_seconds(), 3)
            if self.start_time
            else None
        )

        def _val(v):
            return "NULL" if v is None else str(v)

        def _str_val(v):
            return "NULL" if v is None else f"'{v}'"

        try:
            catalog = _get_catalog()
            job_runs_table = _UC_JOB_RUNS_TABLE.format(catalog=catalog)
            spark.sql(
                f"""
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
            )
        except Exception as exc:
            warnings.warn(f"[AuditLogger] finish() failed (non-fatal): {exc}")


print("AuditLogger loaded.")
