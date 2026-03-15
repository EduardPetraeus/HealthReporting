"""Post-ingest pipeline: bronze -> silver -> gold -> AI layer.

Triggered after HAE data lands via the /v1/ingest/apple-health endpoint.
Runs the same steps as daily_sync.sh steps 2-9, but without API fetches
(data already arrived as parquet).

Concurrency control:
- Lock directory (/tmp/health_pipeline.lock.d) prevents concurrent runs
  (shared with daily_sync.sh via mkdir-based locking, macOS-compatible)
- Throttle: max once per 10 minutes to absorb HAE bursts
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from health_platform.utils.logging_config import get_logger

logger = get_logger("pipeline.post_ingest")

LOCK_DIR = Path("/tmp/health_pipeline.lock.d")
_STALE_THRESHOLD = 3600  # 1 hour — assume stale if lock is older
THROTTLE_FILE = Path("/tmp/health_pipeline_last_run")
THROTTLE_SECONDS = 600  # 10 minutes

# Paths — resolved relative to this file's location in the repo
_THIS_DIR = Path(__file__).resolve().parent
PLATFORM_ROOT = _THIS_DIR.parents[1]  # health_unified_platform/
REPO_ROOT = PLATFORM_ROOT.parent  # HealthReporting/
VENV_PYTHON = str(REPO_ROOT / ".venv" / "bin" / "python3")
VENV_AI_PYTHON = str(REPO_ROOT / ".venv-ai" / "bin" / "python3")


def _acquire_lock() -> bool:
    """Try to acquire the pipeline lock (mkdir-based, macOS-compatible).

    Returns True if lock acquired, False if another run holds it.
    Removes stale locks older than 1 hour.
    """
    try:
        LOCK_DIR.mkdir()
        return True
    except FileExistsError:
        if LOCK_DIR.exists():
            age = time.time() - LOCK_DIR.stat().st_mtime
            if age > _STALE_THRESHOLD:
                logger.warning("Removing stale lock (%.0fs old)", age)
                try:
                    LOCK_DIR.rmdir()
                    LOCK_DIR.mkdir()
                    return True
                except OSError:
                    pass
        return False


def _release_lock() -> None:
    """Release the pipeline lock."""
    try:
        LOCK_DIR.rmdir()
    except OSError:
        pass


def _should_run() -> bool:
    """Check throttle — skip if last run was < 10 min ago."""
    if THROTTLE_FILE.exists():
        elapsed = time.time() - THROTTLE_FILE.stat().st_mtime
        if elapsed < THROTTLE_SECONDS:
            logger.info(
                "Throttled — last run %.0fs ago (min %ds)",
                elapsed,
                THROTTLE_SECONDS,
            )
            return False
    return True


def _build_env() -> dict[str, str]:
    """Build environment dict with PYTHONPATH set for subprocess calls."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PLATFORM_ROOT)
    return env


def _run_step(
    cmd: list[str],
    step_name: str,
    *,
    cwd: str | None = None,
    timeout: int = 300,
) -> bool:
    """Run a subprocess step. Returns True on success."""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd or str(PLATFORM_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=_build_env(),
        )
        if result.returncode == 0:
            logger.info("%s: OK", step_name)
            return True
        logger.warning(
            "%s: FAILED (rc=%d): %s",
            step_name,
            result.returncode,
            result.stderr[:500],
        )
        return False
    except subprocess.TimeoutExpired:
        logger.warning("%s: TIMEOUT after %ds", step_name, timeout)
        return False
    except Exception as exc:
        logger.warning("%s: ERROR: %s", step_name, exc)
        return False


def run_downstream_pipeline() -> dict[str, object]:
    """Run bronze -> silver -> gold -> AI after HAE data lands.

    Returns a dict with per-step results and overall status.
    Status values: 'completed', 'throttled', 'locked'.
    """
    if not _should_run():
        return {"status": "throttled"}

    # Non-blocking lock — if daily_sync.sh holds it, skip
    if not _acquire_lock():
        logger.info("Pipeline locked — daily_sync or another run in progress")
        return {"status": "locked"}

    try:
        THROTTLE_FILE.touch()
        results: dict[str, object] = {}

        # --- Bronze ingestion ---
        results["bronze"] = _run_step(
            [
                VENV_PYTHON,
                "-m",
                "health_platform.transformation_logic.ingestion_engine",
            ],
            "Bronze ingestion",
        )

        # --- Silver merges ---
        merge_dir = (
            PLATFORM_ROOT / "health_platform" / "transformation_logic" / "dbt" / "merge"
        )
        sql_files = sorted((merge_dir / "silver").glob("merge_*.sql"))
        merge_ok = sum(
            1
            for sf in sql_files
            if _run_step(
                [VENV_PYTHON, "run_merge.py", f"silver/{sf.name}"],
                f"Merge {sf.stem}",
                cwd=str(merge_dir),
            )
        )
        results["silver"] = f"{merge_ok}/{len(sql_files)}"

        # --- Gold views ---
        results["gold"] = _run_step(
            [VENV_PYTHON, str(REPO_ROOT / "scripts" / "create_gold_views.py")],
            "Gold views",
        )

        # --- AI layer (needs .venv-ai for sentence-transformers) ---

        # Daily summary
        results["summary"] = _run_step(
            [
                VENV_AI_PYTHON,
                "-c",
                _AI_SUMMARY_SCRIPT,
            ],
            "Daily summary",
            timeout=120,
        )

        # Embeddings
        results["embeddings"] = _run_step(
            [
                VENV_AI_PYTHON,
                "-c",
                _AI_EMBEDDING_SCRIPT,
            ],
            "Embeddings",
            timeout=120,
        )

        # Correlations
        results["correlations"] = _run_step(
            [
                VENV_AI_PYTHON,
                "-c",
                _AI_CORRELATION_SCRIPT,
            ],
            "Correlations",
            timeout=120,
        )

        # Patient profile (baselines + demographics)
        results["baselines"] = _run_step(
            [
                VENV_AI_PYTHON,
                "-c",
                _AI_BASELINE_SCRIPT,
            ],
            "Patient profile",
            timeout=120,
        )

        # Anomaly detection
        results["anomalies"] = _run_step(
            [
                VENV_AI_PYTHON,
                "-c",
                _AI_ANOMALY_SCRIPT,
            ],
            "Anomaly detection",
            timeout=120,
        )

        results["status"] = "completed"
        logger.info("Post-ingest pipeline complete: %s", results)
        return results

    finally:
        _release_lock()


# ---------------------------------------------------------------------------
# Inline Python scripts for AI layer steps (executed via .venv-ai subprocess)
# ---------------------------------------------------------------------------

_AI_SUMMARY_SCRIPT = """\
import duckdb, os
from datetime import date, timedelta
from health_platform.ai.text_generator import generate_summary_for_pipeline

yesterday = date.today() - timedelta(days=1)
con = duckdb.connect(os.environ["HEALTH_DB_PATH"])
try:
    s = generate_summary_for_pipeline(con, yesterday)
    print(f"Summary: {len(s) if s else 0} chars")
finally:
    con.close()
"""

_AI_EMBEDDING_SCRIPT = """\
import duckdb, os
from health_platform.ai.embedding_engine import EmbeddingEngine

con = duckdb.connect(os.environ["HEALTH_DB_PATH"])
try:
    count = EmbeddingEngine().backfill_daily_summaries(con)
    print(f"Embedded {count} summaries")
finally:
    con.close()
"""

_AI_CORRELATION_SCRIPT = """\
import duckdb, os
from health_platform.ai.correlation_engine import compute_all_correlations

con = duckdb.connect(os.environ["HEALTH_DB_PATH"])
try:
    count = compute_all_correlations(con)
    print(f"Correlations: {count}")
finally:
    con.close()
"""

_AI_BASELINE_SCRIPT = """\
import duckdb, os
from health_platform.ai.baseline_computer import compute_all_baselines, compute_demographics

con = duckdb.connect(os.environ["HEALTH_DB_PATH"])
try:
    b = compute_all_baselines(con)
    d = compute_demographics(con)
    print(f"Baselines: {b}, Demographics: {d}")
finally:
    con.close()
"""

_AI_ANOMALY_SCRIPT = """\
import duckdb, os
from health_platform.ai.anomaly_detector import AnomalyDetector, format_anomaly_report

con = duckdb.connect(os.environ["HEALTH_DB_PATH"], read_only=True)
try:
    report = AnomalyDetector(con).detect(lookback_days=1)
    print(format_anomaly_report(report))
finally:
    con.close()
"""
