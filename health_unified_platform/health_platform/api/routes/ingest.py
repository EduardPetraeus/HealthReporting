"""Ingest endpoint for Health Auto Export (HAE) JSON payloads.

Receives automated daily exports from the HAE iOS app and writes them
to the same Parquet data lake used by process_health_data.py (XML).
The downstream pipeline (ingestion_engine → Bronze → Silver) is unchanged.

Route: POST /v1/ingest/apple-health
Auth:  Bearer token (same as all other /v1 endpoints)
"""

from __future__ import annotations

import json as _json
import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status

from health_platform.api.auth import verify_token
from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
    parse_hae_payload,
    write_hae_records,
)
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.ingest")

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])

# 50 MB request body limit — enforced on actual body bytes, not Content-Length header
MAX_BODY_BYTES = 50 * 1024 * 1024

_DEFAULT_DATA_LAKE = "/Users/Shared/data_lake"


def _get_apple_health_root() -> Path:
    """Resolve the apple_health_data output directory."""
    lake = os.environ.get("DATA_LAKE_ROOT", _DEFAULT_DATA_LAKE)
    return Path(lake) / "apple_health_data"


@router.post("/apple-health")
async def ingest_apple_health(
    request: Request,
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
    if len(body) > MAX_BODY_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Request body exceeds {MAX_BODY_BYTES // (1024 * 1024)} MB limit",
        )

    payload = _json.loads(body)

    # Validate top-level structure
    metrics = payload.get("data", {}).get("metrics")
    if not isinstance(metrics, list):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Invalid payload: expected data.metrics to be a list",
        )

    records = parse_hae_payload(payload)
    if not records:
        logger.warning("HAE payload contained no mappable metrics")
        return {"status": "ok", "records_written": 0, "files_created": 0}

    output_root = _get_apple_health_root()
    try:
        result = write_hae_records(records, output_root)
    except Exception:
        logger.exception("HAE write failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ingest processing failed",
        )

    logger.info(
        "HAE ingest complete: %d records, %d files",
        result["records_written"],
        result["files_created"],
    )

    return {
        "status": "ok",
        "records_written": result["records_written"],
        "files_created": result["files_created"],
    }
