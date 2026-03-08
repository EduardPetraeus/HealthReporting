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
from health_platform.mcp.health_tools import HealthTools
from health_platform.utils.logging_config import get_logger
from mcp.server.fastmcp import FastMCP

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
        date_range: Date range (e.g., 'today', 'last_7_days',
            'last_30_days', '2026-01-01:2026-01-31')
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
        categories: Comma-separated categories to filter
            (e.g., 'baselines,demographics'). Empty = all.
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
        category: Category: 'sleep', 'vitals', 'activity',
            'recovery', 'body', 'nutrition', 'cross_domain'
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
        check_type: Check type: 'not_null', 'unique', 'freshness',
            'row_count', 'value_range'. Empty = all types.
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


@mcp.tool()
def forecast_metric(
    metric: str, lookback_days: int = 90, forecast_days: int = 7
) -> str:
    """Forecast a health metric's trajectory using linear regression.

    Args:
        metric: Metric in table.column format (e.g., 'daily_sleep.sleep_score')
        lookback_days: Days of historical data to use (default 90)
        forecast_days: Days to forecast forward (default 7, max 14)
    """
    from health_platform.ai.trend_forecaster import TrendForecaster, format_forecast

    parts = metric.split(".", 1)
    if len(parts) != 2:
        return "Error: metric must be in table.column format (e.g., 'daily_sleep.sleep_score')"

    table, column = f"silver.{parts[0]}", parts[1]
    forecast_days = min(max(int(forecast_days), 1), 14)

    tools = get_tools()
    try:
        forecaster = TrendForecaster(tools.con)
        forecast = forecaster.forecast_metric(
            table, column, lookback_days=lookback_days, forecast_days=forecast_days
        )
        return format_forecast(forecast)
    finally:
        tools.close()


@mcp.tool()
def get_cross_source_insights() -> str:
    """Get cross-source correlation insights -- which metrics influence each other."""
    from health_platform.ai.correlation_engine import compute_all_correlations

    tools = get_tools(read_only=False)
    try:
        count = compute_all_correlations(tools.con)

        # Fetch strongest correlations
        result = tools.con.execute(
            """
            SELECT source_metric, target_metric, strength, lag_days,
                   direction, confidence, description
            FROM silver.metric_relationships
            WHERE ABS(strength) > 0.3
            ORDER BY ABS(strength) DESC
            LIMIT 15
        """
        )
        rows = result.fetchall()
        columns = [d[0] for d in result.description]

        if not rows:
            return f"Computed {count} correlations. No strong relationships found (|r| > 0.3)."

        from health_platform.mcp.formatter import format_as_markdown_table

        header = f"# Cross-Source Insights\n\nComputed {count} correlations. Showing strongest:\n\n"
        return header + format_as_markdown_table(rows, columns)
    finally:
        tools.close()


@mcp.tool()
def get_recommendations(max_results: int = 5) -> str:
    """Get personalized health recommendations based on recent data patterns.

    Evidence-backed, actionable suggestions. Safe -- no clinical diagnosis or treatment advice.

    Args:
        max_results: Maximum recommendations to return (default 5)
    """
    from health_platform.ai.recommendation_engine import (
        RecommendationEngine,
        format_recommendations,
    )

    tools = get_tools()
    try:
        engine = RecommendationEngine(tools.con)
        recs = engine.get_recommendations(max_results=min(int(max_results), 10))
        return format_recommendations(recs)
    finally:
        tools.close()


@mcp.tool()
def explain_recommendation(recommendation_title: str) -> str:
    """Explain the data behind a recommendation in detail.

    Args:
        recommendation_title: Title of the recommendation to explain
    """
    from health_platform.ai.recommendation_engine import RecommendationEngine

    tools = get_tools()
    try:
        engine = RecommendationEngine(tools.con)
        recs = engine.get_recommendations(max_results=20)

        for r in recs:
            if recommendation_title.lower() in r.title.lower():
                parts = [
                    f"# {r.title}\n",
                    f"**Category:** {r.category}",
                    f"**Priority:** {r.priority}",
                    f"**Confidence:** {r.confidence:.0%}\n",
                    f"## Recommendation\n{r.recommendation}\n",
                    f"## Data Rationale\n{r.rationale}\n",
                    f"## Evidence Type\n{r.evidence_type}\n",
                    "## Metrics Used\n" + "\n".join(f"- `{m}`" for m in r.metrics_used),
                ]
                return "\n".join(parts)

        titles = [r.title for r in recs]
        return f"Recommendation not found: '{recommendation_title}'. Available: {', '.join(titles)}"
    finally:
        tools.close()


@mcp.tool()
def query_lab_results(
    marker_name: str = "", date_range: str = "", status: str = ""
) -> str:
    """Query lab test results (blood panels, biomarkers) with optional filters.

    Args:
        marker_name: Filter by marker name (partial match,
            e.g., 'cholesterol', 'vitamin'). Empty = all.
        date_range: Date range (e.g., 'last_30_days', '2026-01-01:2026-03-01'). Empty = all dates.
        status: Filter by result status: 'normal', 'low', 'high'. Empty = all statuses.
    """
    tools = get_tools()
    try:
        return tools.query_lab_results(
            marker_name=marker_name or None,
            date_range=date_range or None,
            status=status or None,
        )
    finally:
        tools.close()


@mcp.tool()
def query_genetics(
    query_type: str = "", category: str = "", report_name: str = ""
) -> str:
    """Query genetics data — health findings, ancestry composition, and family tree.

    Args:
        query_type: Data type to query: 'health', 'ancestry', 'family'. Empty = dashboard overview.
        category: Filter by category (e.g., 'Scandinavian'
            for ancestry, 'health_risk' for findings). Empty = all.
        report_name: Filter by report name (partial match,
            e.g., 'alzheimer'). Empty = all.
    """
    tools = get_tools()
    try:
        return tools.query_genetics(
            query_type=query_type or None,
            category=category or None,
            report_name=report_name or None,
        )
    finally:
        tools.close()


if __name__ == "__main__":
    mcp.run()
