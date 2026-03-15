"""Ingest endpoint for Health Auto Export (HAE) JSON payloads.

Receives automated daily exports from the HAE iOS app and writes them
to the same Parquet data lake used by process_health_data.py (XML).
The downstream pipeline (ingestion_engine → Bronze → Silver) is unchanged.

After a successful write, the downstream pipeline (bronze → silver → gold → AI)
is triggered as a background task to minimize data latency.

Route: POST /v1/ingest/apple-health
Auth:  Bearer token (same as all other /v1 endpoints)
"""

from __future__ import annotations

import csv
import json as _json
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from health_platform.api.auth import verify_token
from health_platform.pipeline.post_ingest import run_downstream_pipeline
from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
    parse_hae_payload,
    write_hae_records,
)
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.ingest")

_DEFAULT_DATA_LAKE = "/Users/Shared/data_lake"


def _get_audit_log() -> Path:
    """Resolve audit log path from DATA_LAKE_ROOT env var."""
    lake = os.environ.get("DATA_LAKE_ROOT", _DEFAULT_DATA_LAKE)
    return Path(lake) / "audit" / "hae_ingest_log.csv"


_AUDIT_FIELDS = [
    "timestamp",
    "status",
    "records_written",
    "files_created",
    "metrics_received",
    "payload_bytes",
    "error",
]


def _write_audit_entry(
    *,
    audit_status: str,
    records_written: int = 0,
    files_created: int = 0,
    metrics_received: int = 0,
    payload_bytes: int = 0,
    error: str = "",
) -> None:
    """Append one row to the HAE ingest audit log."""
    audit_log = _get_audit_log()
    audit_log.parent.mkdir(parents=True, exist_ok=True)
    write_header = not audit_log.exists()
    # Sanitise error string: strip control chars, truncate, prevent CSV injection
    error = error[:200].replace("\r", "").replace("\n", " ")
    if error and error[0] in ("=", "+", "-", "@"):
        error = "'" + error
    with audit_log.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_AUDIT_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": audit_status,
                "records_written": records_written,
                "files_created": files_created,
                "metrics_received": metrics_received,
                "payload_bytes": payload_bytes,
                "error": error,
            }
        )


router = APIRouter(prefix="/v1/ingest", tags=["ingest"])

# 50 MB request body limit — enforced on actual body bytes, not Content-Length header
MAX_BODY_BYTES = 50 * 1024 * 1024


def _get_apple_health_root() -> Path:
    """Resolve the apple_health_data output directory."""
    lake = os.environ.get("DATA_LAKE_ROOT", _DEFAULT_DATA_LAKE)
    return Path(lake) / "apple_health_data"


@router.post("/apple-health")
async def ingest_apple_health(
    request: Request,
    background_tasks: BackgroundTasks,
    _token: str = Depends(verify_token),
):
    """Receive and persist a Health Auto Export JSON payload.

    Expected JSON structure:
        {"data": {"metrics": [{"name": "...", "units": "...", "data": [...]}]}}

    Returns:
        {"status": "ok", "records_written": N, "files_created": N}
    """
    # Read full body and enforce size limit (works with chunked transfer too)
    body = await request.body()
    body_len = len(body)
    if body_len > MAX_BODY_BYTES:
        _write_audit_entry(
            audit_status="FAIL",
            payload_bytes=body_len,
            error=f"Payload too large ({body_len} bytes)",
        )
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Request body exceeds {MAX_BODY_BYTES // (1024 * 1024)} MB limit",
        )

    try:
        payload = _json.loads(body)
    except _json.JSONDecodeError as exc:
        _write_audit_entry(
            audit_status="FAIL",
            payload_bytes=body_len,
            error=f"Invalid JSON: {exc}",
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Invalid JSON payload",
        ) from exc

    # Validate top-level structure
    metrics = payload.get("data", {}).get("metrics")
    if not isinstance(metrics, list):
        _write_audit_entry(
            audit_status="FAIL",
            payload_bytes=body_len,
            error="Missing data.metrics list",
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Invalid payload: expected data.metrics to be a list",
        )

    num_metrics = len(metrics)
    records = parse_hae_payload(payload)
    if not records:
        logger.warning("HAE payload contained no mappable metrics")
        _write_audit_entry(
            audit_status="OK",
            metrics_received=num_metrics,
            payload_bytes=body_len,
        )
        return {"status": "ok", "records_written": 0, "files_created": 0}

    output_root = _get_apple_health_root()
    try:
        result = write_hae_records(records, output_root)
    except Exception as exc:
        logger.exception("HAE write failed")
        _write_audit_entry(
            audit_status="FAIL",
            metrics_received=num_metrics,
            payload_bytes=body_len,
            error=str(exc),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ingest processing failed",
        ) from exc

    logger.info(
        "HAE ingest complete: %d records, %d files",
        result["records_written"],
        result["files_created"],
    )
    _write_audit_entry(
        audit_status="OK",
        records_written=result["records_written"],
        files_created=result["files_created"],
        metrics_received=num_metrics,
        payload_bytes=body_len,
    )

    # Trigger downstream pipeline as background task (bronze → silver → gold → AI).
    # Throttled (max once per 10 min) and lock-guarded (skips if daily_sync running).
    background_tasks.add_task(run_downstream_pipeline)

    return {
        "status": "ok",
        "records_written": result["records_written"],
        "files_created": result["files_created"],
    }
