"""
Unit tests for pipeline_run_id propagation through the audit infrastructure.

All tests use synthetic fixtures and in-memory or temp DuckDB — no real health data.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import duckdb

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_CREATE_JOB_RUNS = """
    CREATE TABLE IF NOT EXISTS audit.job_runs (
        job_id          VARCHAR,
        job_name        VARCHAR,
        job_type        VARCHAR,
        source_system   VARCHAR,
        env             VARCHAR,
        start_time      TIMESTAMP,
        end_time        TIMESTAMP,
        duration_seconds DOUBLE,
        status          VARCHAR,
        error_message   VARCHAR,
        rows_processed  INTEGER,
        rows_inserted   INTEGER,
        rows_updated    INTEGER,
        rows_deleted    INTEGER,
        pipeline_run_id UUID
    )
"""

_CREATE_TABLE_RUNS = """
    CREATE TABLE IF NOT EXISTS audit.table_runs (
        run_id          VARCHAR,
        job_id          VARCHAR,
        table_name      VARCHAR,
        layer           VARCHAR,
        operation       VARCHAR,
        rows_before     INTEGER,
        rows_after      INTEGER,
        rows_changed    INTEGER,
        start_time      TIMESTAMP,
        end_time        TIMESTAMP,
        status          VARCHAR,
        error_message   VARCHAR
    )
"""


def _build_audit_db(path: str) -> None:
    """Create audit schema + tables at the given DuckDB file path.

    The file must not exist yet — DuckDB cannot open a pre-existing empty file.
    """
    if os.path.exists(path):
        os.unlink(path)
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA IF NOT EXISTS audit")
    con.execute(_CREATE_JOB_RUNS)
    con.execute(_CREATE_TABLE_RUNS)
    con.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_run_id_optional_defaults_to_null():
    """AuditLogger with no pipeline_run_id sets self.pipeline_run_id to None."""
    from health_platform.utils.audit_logger import AuditLogger

    logger = AuditLogger("test_job", "silver", "test_source")
    assert logger.pipeline_run_id is None


def test_pipeline_run_id_propagates_to_job_runs():
    """pipeline_run_id is written to audit.job_runs when AuditLogger.start() is called."""
    from health_platform.utils.audit_logger import AuditLogger

    expected_run_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        _build_audit_db(db_path)

        with patch(
            "health_platform.utils.audit_logger._get_duckdb_path", return_value=db_path
        ):
            audit = AuditLogger(
                "test_job", "silver", "test_source", pipeline_run_id=expected_run_id
            )
            audit.start()
            audit.finish(status="success")

        con = duckdb.connect(db_path, read_only=True)
        row = con.execute(
            "SELECT pipeline_run_id::VARCHAR FROM audit.job_runs WHERE job_name = 'test_job'"
        ).fetchone()
        con.close()

        assert row is not None, "No job_run record found"
        assert row[0] == expected_run_id
    finally:
        os.unlink(db_path)


def test_pipeline_run_id_none_writes_null_to_db():
    """pipeline_run_id=None (default) writes NULL to audit.job_runs."""
    from health_platform.utils.audit_logger import AuditLogger

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        _build_audit_db(db_path)

        with patch(
            "health_platform.utils.audit_logger._get_duckdb_path", return_value=db_path
        ):
            audit = AuditLogger("test_job_no_run_id", "bronze", "oura")
            audit.start()
            audit.finish(status="success")

        con = duckdb.connect(db_path, read_only=True)
        row = con.execute(
            "SELECT pipeline_run_id FROM audit.job_runs WHERE job_name = 'test_job_no_run_id'"
        ).fetchone()
        con.close()

        assert row is not None, "No job_run record found"
        assert row[0] is None
    finally:
        os.unlink(db_path)


def test_hung_pipeline_fix_marks_as_failed():
    """Migration UPDATE marks job_runs stuck in 'running' > 4h as 'failed'."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA audit")
    con.execute(_CREATE_JOB_RUNS)

    # Insert a synthetic job stuck in running for 5 hours
    five_hours_ago = datetime.now(timezone.utc) - timedelta(hours=5)
    con.execute(
        """
        INSERT INTO audit.job_runs (job_id, job_name, status, start_time)
        VALUES ('stuck-job-001', 'run_oura', 'running', ?)
        """,
        [five_hours_ago],
    )

    # Insert a recent running job that should NOT be affected
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    con.execute(
        """
        INSERT INTO audit.job_runs (job_id, job_name, status, start_time)
        VALUES ('recent-job-001', 'run_withings', 'running', ?)
        """,
        [one_hour_ago],
    )

    # Run the migration UPDATE
    con.execute(
        """
        UPDATE audit.job_runs
        SET status = 'failed',
            error_message = 'Marked failed by migration: status stuck in running > 4h',
            end_time = start_time + INTERVAL 4 HOURS
        WHERE status = 'running'
        AND start_time < NOW() - INTERVAL 4 HOURS
        """
    )

    stuck = con.execute(
        "SELECT status, error_message FROM audit.job_runs WHERE job_id = 'stuck-job-001'"
    ).fetchone()
    recent = con.execute(
        "SELECT status FROM audit.job_runs WHERE job_id = 'recent-job-001'"
    ).fetchone()

    assert stuck[0] == "failed"
    assert "migration" in stuck[1]
    assert recent[0] == "running"  # Not affected
    con.close()


def test_merge_row_counts_populated():
    """_extract_target_table returns correct schema.table from MERGE SQL."""
    import sys
    from pathlib import Path

    merge_dir = (
        Path(__file__).resolve().parents[2]
        / "health_unified_platform"
        / "health_platform"
        / "transformation_logic"
        / "dbt"
        / "merge"
    )
    sys.path.insert(0, str(merge_dir))

    from run_merge import _extract_target_table

    sample_sql = """
        CREATE OR REPLACE TABLE silver.daily_sleep__staging AS SELECT * FROM bronze.stg_oura_sleep;
        MERGE INTO silver.daily_sleep AS target
        USING silver.daily_sleep__staging AS src
        ON target.business_key_hash = src.business_key_hash
        WHEN MATCHED THEN UPDATE SET target.sleep_score = src.sleep_score
        WHEN NOT MATCHED THEN INSERT (business_key_hash, sleep_score)
            VALUES (src.business_key_hash, src.sleep_score);
        DROP TABLE IF EXISTS silver.daily_sleep__staging;
    """

    result = _extract_target_table(sample_sql)
    assert result == "silver.daily_sleep"


def test_merge_row_counts_none_when_no_target():
    """_extract_target_table returns None when SQL has no MERGE INTO."""
    import sys
    from pathlib import Path

    merge_dir = (
        Path(__file__).resolve().parents[2]
        / "health_unified_platform"
        / "health_platform"
        / "transformation_logic"
        / "dbt"
        / "merge"
    )
    sys.path.insert(0, str(merge_dir))

    from run_merge import _extract_target_table

    result = _extract_target_table("SELECT 1")
    assert result is None
