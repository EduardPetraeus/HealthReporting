"""MCP server for health data access.

Exposes 10 tools for AI agents to interact with health data through
semantic contracts instead of raw SQL.

Usage:
    python -m health_platform.mcp.server
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
from mcp.server.fastmcp import FastMCP

from health_platform.mcp.health_tools import HealthTools
from health_platform.utils.logging_config import get_logger

logger = get_logger("mcp_server")

mcp = FastMCP("health")


def get_db_path() -> str:
    """Resolve DuckDB database path from environment."""
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    env = os.environ.get("HEALTH_ENV", "dev")
    return str(Path.home() / "health_dw" / f"health_dw_{env}.db")


def get_tools(read_only: bool = True) -> HealthTools:
    """Get or create HealthTools instance."""
    db_path = get_db_path()
    con = duckdb.connect(db_path, read_only=read_only)
    return HealthTools(con)


@mcp.tool()
def query_health(metric: str, date_range: str, computation: str = "daily_value") -> str:
    """Query a health metric using semantic contracts.

    Args:
        metric: Metric name (e.g., 'sleep_score', 'readiness_score', 'steps')
        date_range: Date range (e.g., 'today', 'yesterday', 'last_7_days', 'last_30_days', 'last_90_days', '2026-01-01:2026-01-31')
        computation: Computation type: 'daily_value', 'period_average', 'trend', 'anomaly'
    """
    tools = get_tools()
    try:
        return tools.query_health(metric, date_range, computation)
    finally:
        tools.close()


@mcp.tool()
def search_memory(query: str, top_k: int = 5) -> str:
    """Search agent memory using vector similarity.

    Args:
        query: Natural language search query
        top_k: Number of results to return (default 5)
    """
    tools = get_tools()
    try:
        return tools.search_memory(query, top_k)
    finally:
        tools.close()


@mcp.tool()
def get_profile(categories: str = "") -> str:
    """Load patient profile (core memory).

    Args:
        categories: Comma-separated categories to filter (e.g., 'baselines,demographics'). Empty = all.
    """
    tools = get_tools()
    try:
        cat_list = (
            [c.strip() for c in categories.split(",") if c.strip()]
            if categories
            else None
        )
        return tools.get_profile(cat_list)
    finally:
        tools.close()


@mcp.tool()
def discover_correlations(metric_a: str, metric_b: str, max_lag: int = 3) -> str:
    """Discover correlations between two health metrics with lag analysis.

    Args:
        metric_a: First metric (e.g., 'daily_sleep.sleep_score')
        metric_b: Second metric (e.g., 'daily_readiness.readiness_score')
        max_lag: Maximum lag in days to test (default 3)
    """
    tools = get_tools()
    try:
        return tools.discover_correlations(metric_a, metric_b, max_lag)
    finally:
        tools.close()


@mcp.tool()
def get_metric_definition(metric_name: str) -> str:
    """Read a metric's semantic contract definition.

    Args:
        metric_name: Metric name (e.g., 'sleep_score', 'weight')
    """
    tools = get_tools()
    try:
        return tools.get_metric_definition(metric_name)
    finally:
        tools.close()


@mcp.tool()
def record_insight(
    title: str,
    content: str,
    insight_type: str = "pattern",
    confidence: float = 0.7,
    tags: str = "",
) -> str:
    """Record an insight to the knowledge base.

    Args:
        title: Short title for the insight
        content: Detailed description of the insight
        insight_type: Type: 'pattern', 'anomaly', 'correlation', 'recommendation'
        confidence: Confidence score 0-1 (default 0.7)
        tags: Comma-separated tags
    """
    tools = get_tools(read_only=False)
    try:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        return tools.record_insight(title, content, insight_type, confidence, tag_list)
    finally:
        tools.close()


@mcp.tool()
def get_schema_context(category: str) -> str:
    """Get schema context for a question category (schema pruning).

    Args:
        category: Category from index: 'sleep', 'vitals', 'activity', 'recovery', 'body', 'nutrition', 'cross_domain'
    """
    tools = get_tools()
    try:
        return tools.get_schema_context(category)
    finally:
        tools.close()


@mcp.tool()
def run_custom_query(sql: str, explanation: str) -> str:
    """Execute a custom SQL query (escape hatch for uncovered queries).

    Args:
        sql: The SQL query to execute (read-only)
        explanation: Why this query is needed (for audit trail)
    """
    tools = get_tools()
    try:
        return tools.run_custom_query(sql, explanation)
    finally:
        tools.close()


@mcp.tool()
def check_data_quality(table: str = "", check_type: str = "") -> str:
    """Run data quality checks on silver tables.

    Args:
        table: Single table to check (e.g., 'daily_sleep'). Empty = all tables.
        check_type: Single check type to run: 'not_null', 'unique', 'freshness', 'row_count', 'value_range'. Empty = all types.
    """
    tools = get_tools()
    try:
        return tools.check_data_quality(
            table=table or None,
            check_type=check_type or None,
        )
    finally:
        tools.close()


@mcp.tool()
def search_evidence(query: str, max_results: int = 5, min_year: str = "") -> str:
    """Search PubMed for evidence supporting health claims.

    Returns peer-reviewed articles sorted by evidence quality (GRADE hierarchy).

    Args:
        query: Search query (e.g., 'HRV sleep quality', 'magnesium supplementation sleep')
        max_results: Number of articles to return (default 5, max 20)
        min_year: Minimum publication year filter (e.g., '2020')
    """
    tools = get_tools(read_only=False)
    try:
        return tools.search_evidence(
            query, max_results=min(int(max_results), 20), min_year=min_year
        )
    finally:
        tools.close()


@mcp.tool()
def detect_anomalies(lookback_days: int = 7) -> str:
    """Detect anomalies across all health metrics.

    Performs multi-stream z-score analysis, constellation pattern matching,
    and temporal degradation detection.

    Args:
        lookback_days: Number of days to check for anomalies (default 7, max 90)
    """
    from health_platform.ai.anomaly_detector import (
        AnomalyDetector,
        format_anomaly_report,
    )

    lookback_days = min(max(int(lookback_days), 1), 90)
    tools = get_tools()
    try:
        detector = AnomalyDetector(tools.con)
        report = detector.detect(lookback_days)
        return format_anomaly_report(report)
    finally:
        tools.close()


if __name__ == "__main__":
    mcp.run()
