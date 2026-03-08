"""FastAPI server for remote health data access.

Provides REST endpoints for querying health data from anywhere via Tailscale.
Uses the same MCP tool implementations as the Claude Code MCP server.

Usage:
    uvicorn health_platform.api.server:app --host 0.0.0.0 --port 8000

Architecture:
    iPhone/laptop → Tailscale VPN → Mac Mini (100.x.x.x:8000) → FastAPI → DuckDB
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Literal

import duckdb
from fastapi import Depends, FastAPI, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from health_platform.api.auth import verify_token
from health_platform.mcp.health_tools import HealthTools
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.server")


def _get_db_path() -> str:
    """Resolve DuckDB database path from environment."""
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    env = os.environ.get("HEALTH_ENV", "dev")
    return str(Path.home() / "health_dw" / f"health_dw_{env}.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify DB exists on startup."""
    db_path = _get_db_path()
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
    else:
        logger.info("Health API starting. DB: %s", db_path)
    yield
    logger.info("Health API shutting down.")


app = FastAPI(
    title="Health Data API",
    description="Personal health data access via Tailscale VPN",
    version="0.1.0",
    lifespan=lifespan,
)

# Mount chat UI at root
from health_platform.api.chat_ui import router as chat_ui_router  # noqa: E402
from health_platform.api.routes.ingest import router as ingest_router  # noqa: E402

app.include_router(chat_ui_router)
app.include_router(ingest_router)

# Mount mobile endpoints (bulk sync, thresholds)
from health_platform.api.routes.mobile import router as mobile_router  # noqa: E402

app.include_router(mobile_router)

# Mount clinician export endpoints (FHIR, PDF)
from health_platform.export.routes import router as export_router  # noqa: E402

app.include_router(export_router)


def _get_tools(read_only: bool = True) -> HealthTools:
    """Create a fresh HealthTools instance."""
    db_path = _get_db_path()
    con = duckdb.connect(db_path, read_only=read_only)
    return HealthTools(con)


# --- Request/Response models ---


class ChatRequest(BaseModel):
    question: str = Field(
        ..., description="Natural language health question", max_length=2000
    )
    format: Literal["markdown", "plain"] = Field(
        "markdown", description="Output format: markdown or plain"
    )
    session_id: str | None = Field(
        None, description="Session ID for multi-turn conversations"
    )


class ChatResponse(BaseModel):
    answer: str
    timestamp: str


class QueryResponse(BaseModel):
    result: str
    metric: str
    date_range: str
    computation: str
    timestamp: str


class AlertsResponse(BaseModel):
    alerts: list[dict]
    timestamp: str


# --- Endpoints ---


@app.get("/health", tags=["system"])
async def health_check():
    """Health check endpoint (no auth required)."""
    db_path = _get_db_path()
    db_exists = Path(db_path).exists()
    return {
        "status": "healthy" if db_exists else "degraded",
        "database": "connected" if db_exists else "missing",
        "timestamp": datetime.now().isoformat(),
    }


@app.post("/v1/chat", response_model=ChatResponse, tags=["ai"])
async def chat(
    request: ChatRequest,
    _token: str = Depends(verify_token),
):
    """Ask a natural language health question.

    Uses Claude AI to interpret the question and generate insights
    from the user's real health data.
    """
    from health_platform.api.chat_engine import generate_response  # noqa: E402

    tools = _get_tools()
    try:
        answer = generate_response(tools, request.question, request.session_id)
    finally:
        tools.close()

    if request.format == "plain":
        answer = _markdown_table_to_plain(answer)

    return ChatResponse(
        answer=answer,
        timestamp=datetime.now().isoformat(),
    )


@app.post("/v1/chat/stream", tags=["ai"])
async def chat_stream(
    request: ChatRequest,
    _token: str = Depends(verify_token),
):
    """Stream a chat response using Server-Sent Events.

    Uses Claude tool-use to gather health data, then streams the
    synthesized response as SSE text chunks.
    """
    from health_platform.api.chat_engine import generate_response_stream  # noqa: E402

    tools = _get_tools()

    async def event_generator():
        try:
            for chunk in generate_response_stream(
                tools, request.question, request.session_id
            ):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        finally:
            tools.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/v1/query", response_model=QueryResponse, tags=["data"])
async def query(
    metric: str = Query(..., description="Metric name (e.g., sleep_score, steps)"),
    date_range: str = Query(
        "last_7_days", description="Date range (last_7_days, last_30_days, etc.)"
    ),
    computation: str = Query(
        "daily_value",
        description="Computation type (daily_value, period_average, trend)",
    ),
    _token: str = Depends(verify_token),
):
    """Query a specific health metric directly."""
    tools = _get_tools()
    try:
        result = tools.query_health(metric, date_range, computation)
    finally:
        tools.close()

    return QueryResponse(
        result=result,
        metric=metric,
        date_range=date_range,
        computation=computation,
        timestamp=datetime.now().isoformat(),
    )


@app.get("/v1/profile", tags=["data"])
async def profile(
    categories: str = Query("", description="Comma-separated categories to filter"),
    _token: str = Depends(verify_token),
):
    """Load patient profile (core memory)."""
    tools = _get_tools()
    try:
        cat_list = (
            [c.strip() for c in categories.split(",") if c.strip()]
            if categories
            else None
        )
        result = tools.get_profile(cat_list)
    finally:
        tools.close()

    return {"profile": result, "timestamp": datetime.now().isoformat()}


@app.get("/v1/alerts", response_model=AlertsResponse, tags=["intelligence"])
async def alerts(
    _token: str = Depends(verify_token),
):
    """Get proactive health alerts.

    Checks for anomalies in recent data based on business rules.
    """
    tools = _get_tools()
    alert_list = []
    try:
        # Check key metrics against baselines
        for metric in ["sleep_score", "readiness_score", "steps"]:
            result = tools.query_health(metric, "last_7_days", "period_average")
            if "Error" not in result:
                alert_list.append(
                    {
                        "metric": metric,
                        "type": "summary",
                        "data": result,
                    }
                )
    finally:
        tools.close()

    return AlertsResponse(
        alerts=alert_list,
        timestamp=datetime.now().isoformat(),
    )


# --- Formatting helpers ---


def _markdown_table_to_plain(text: str) -> str:
    """Convert markdown table output to plain text for mobile display."""
    lines = text.strip().split("\n")
    result = []
    for line in lines:
        # Skip separator lines (|---|---|)
        if line.strip().startswith("|") and set(line.replace("|", "").strip()) <= {
            "-",
            " ",
        }:
            continue
        if "|" in line:
            cells = [c.strip() for c in line.split("|") if c.strip()]
            result.append("  ".join(cells))
        else:
            result.append(line)
    return "\n".join(result)


# --- Question routing ---


def _route_question(tools: HealthTools, question_lower: str, original: str) -> str:
    """Route a natural language question to the appropriate tool.

    Simple keyword-based routing. For full AI understanding, use Claude + MCP.
    """
    # Sleep questions
    if any(kw in question_lower for kw in ["sleep", "sov", "søvn", "slept"]):
        if "trend" in question_lower or "getting" in question_lower:
            return tools.query_health("sleep_score", "last_30_days", "trend")
        if "average" in question_lower or "gennemsnit" in question_lower:
            return tools.query_health("sleep_score", "last_30_days", "period_average")
        return tools.query_health("sleep_score", "last_7_days")

    # Readiness questions
    if any(kw in question_lower for kw in ["readiness", "ready", "klar"]):
        return tools.query_health("readiness_score", "last_7_days")

    # Activity/steps questions
    if any(kw in question_lower for kw in ["step", "skridt", "walk", "activity"]):
        return tools.query_health("steps", "last_7_days")

    # Workout questions
    if any(kw in question_lower for kw in ["workout", "exercise", "træning", "run"]):
        return tools.query_health("workout", "last_30_days")

    # Stress questions
    if any(kw in question_lower for kw in ["stress", "recover"]):
        return tools.query_health("daily_stress", "last_7_days")

    # Weight questions
    if any(kw in question_lower for kw in ["weight", "vægt", "body"]):
        return tools.query_health("weight", "last_30_days")

    # Profile questions
    if any(
        kw in question_lower for kw in ["profile", "who am i", "about me", "profil"]
    ):
        return tools.get_profile()

    # Memory search as fallback
    return tools.search_memory(original)
