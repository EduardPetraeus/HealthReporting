"""Core tool implementations for the MCP health server.

Each method corresponds to an MCP tool registered in server.py.
Delegates to QueryBuilder for YAML-based queries, Formatter for output,
and SchemaPruner for schema context.
"""

from __future__ import annotations

import re
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
from health_platform.mcp.formatter import (
    format_as_markdown_kv,
    format_as_markdown_table,
    format_empty,
    format_error,
    format_result,
)
from health_platform.mcp.query_builder import QueryBuilder
from health_platform.mcp.schema_pruner import SchemaPruner
from health_platform.utils.logging_config import get_logger
from health_platform.utils.sql_safety import is_safe_identifier, validate_sql_identifier

logger = get_logger("health_tools")

# Path to semantic contracts directory
_CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts" / "metrics"


class HealthTools:
    """Implements all MCP tool operations against a DuckDB connection."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.query_builder = QueryBuilder(_CONTRACTS_DIR)
        self.schema_pruner = SchemaPruner(con, _CONTRACTS_DIR)

    def close(self) -> None:
        """Close the DuckDB connection."""
        try:
            self.con.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Tool 1: query_health
    # ------------------------------------------------------------------

    def query_health(
        self, metric: str, date_range: str, computation: str = "daily_value"
    ) -> str:
        """Query a metric via its semantic contract.

        Steps:
        1. Load YAML contract for the metric.
        2. Parse date_range into start/end dates.
        3. Get the computation SQL from the contract.
        4. Substitute parameters and execute.
        5. Format result as markdown table.
        """
        try:
            contract = self.query_builder.load_contract(metric)
        except FileNotFoundError:
            available = self.query_builder.list_metrics()
            return format_error(
                f"Unknown metric '{metric}'. Available: {', '.join(available)}"
            )

        metric_def = contract.get("metric", contract)
        computations = metric_def.get("computations", {})

        if computation not in computations:
            available_comps = list(computations.keys())
            return format_error(
                f"Unknown computation '{computation}' for metric '{metric}'. "
                f"Available: {', '.join(available_comps)}"
            )

        start, end = self._parse_date_range(date_range)
        params = {
            "date": end.isoformat(),
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

        sql_template = computations[computation].get("sql", "")
        if not sql_template:
            return format_error(
                f"No SQL defined for computation '{computation}' on metric '{metric}'."
            )

        sql = self.query_builder.substitute_params(sql_template.strip(), params)

        try:
            result = self.con.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Query failed for %s/%s: %s", metric, computation, exc)
            return format_error(
                "Query execution failed. Check server logs for details."
            )

        if not rows:
            return format_empty(metric, date_range)

        return format_result(rows, columns, "markdown-table")

    # ------------------------------------------------------------------
    # Tool 2: search_memory
    # ------------------------------------------------------------------

    def search_memory(self, query: str, top_k: int = 5) -> str:
        """Search agent memory — daily summaries and knowledge base.

        Uses text search with LIKE as the default fallback.
        Vector search can be added when an embedding engine is available.
        """
        results_parts: list[str] = []

        # Search daily summaries
        summary_results = self._search_daily_summaries(query, top_k)
        if summary_results:
            results_parts.append("## Daily Summaries\n")
            results_parts.append(summary_results)

        # Search knowledge base
        kb_results = self._search_knowledge_base(query, top_k)
        if kb_results:
            results_parts.append("\n## Knowledge Base\n")
            results_parts.append(kb_results)

        if not results_parts:
            return f"No results found for query: '{query}'"

        return "\n".join(results_parts)

    def _search_daily_summaries(self, query: str, top_k: int) -> str:
        """Search daily summaries using text matching."""
        keywords = [kw.strip() for kw in query.lower().split() if len(kw.strip()) > 2]
        if not keywords:
            return ""

        # Build LIKE conditions — match any keyword
        like_conditions = " OR ".join(
            ["LOWER(summary_text) LIKE '%' || ? || '%'" for _ in keywords]
        )
        sql = f"""
            SELECT day, summary_text
            FROM agent.daily_summaries
            WHERE {like_conditions}
            ORDER BY day DESC
            LIMIT ?
        """
        try:
            result = self.con.execute(sql, keywords + [top_k])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            if not rows:
                return ""
            return format_as_markdown_table(rows, columns)
        except Exception as exc:
            logger.debug("Daily summaries search failed: %s", exc)
            return ""

    def _search_knowledge_base(self, query: str, top_k: int) -> str:
        """Search knowledge base using text matching."""
        keywords = [kw.strip() for kw in query.lower().split() if len(kw.strip()) > 2]
        if not keywords:
            return ""

        like_conditions = " OR ".join(
            [
                "LOWER(title) LIKE '%' || ? || '%' OR LOWER(content) LIKE '%' || ? || '%'"
                for _ in keywords
            ]
        )
        # Each keyword needs to appear twice (title + content)
        params: list = []
        for kw in keywords:
            params.extend([kw, kw])

        sql = f"""
            SELECT insight_id, title, insight_type, confidence, created_at
            FROM agent.knowledge_base
            WHERE is_active = true AND ({like_conditions})
            ORDER BY created_at DESC
            LIMIT ?
        """
        params.append(top_k)

        try:
            result = self.con.execute(sql, params)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            if not rows:
                return ""
            return format_as_markdown_table(rows, columns)
        except Exception as exc:
            logger.debug("Knowledge base search failed: %s", exc)
            return ""

    # ------------------------------------------------------------------
    # Tool 3: get_profile
    # ------------------------------------------------------------------

    def get_profile(self, categories: Optional[list[str]] = None) -> str:
        """Load patient profile (core memory) as formatted text."""
        try:
            if categories:
                placeholders = ", ".join(["?" for _ in categories])
                sql = f"""
                    SELECT category, profile_key, profile_value, last_updated_at
                    FROM agent.patient_profile
                    WHERE category IN ({placeholders})
                    ORDER BY category, profile_key
                """
                result = self.con.execute(sql, categories)
            else:
                sql = """
                    SELECT category, profile_key, profile_value, last_updated_at
                    FROM agent.patient_profile
                    ORDER BY category, profile_key
                """
                result = self.con.execute(sql)

            rows = result.fetchall()
        except Exception as exc:
            logger.error("Failed to load profile: %s", exc)
            return format_error(
                "Failed to load profile. Check server logs for details."
            )

        if not rows:
            cat_str = ", ".join(categories) if categories else "all"
            return f"No profile data found for categories: {cat_str}"

        # Group by category for readable output
        grouped: dict[str, dict[str, str]] = {}
        for row in rows:
            cat = row[0]
            key = row[1]
            value = row[2]
            if cat not in grouped:
                grouped[cat] = {}
            grouped[cat][key] = value

        parts: list[str] = ["# Patient Profile\n"]
        for category, entries in grouped.items():
            parts.append(f"## {category}\n")
            parts.append(format_as_markdown_kv(entries))
            parts.append("")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Tool 4: discover_correlations
    # ------------------------------------------------------------------

    def discover_correlations(
        self, metric_a: str, metric_b: str, max_lag: int = 3
    ) -> str:
        """Compute correlations between two metrics with lag analysis.

        Supports dotted notation: 'table.column' (e.g., 'daily_sleep.sleep_score').
        """
        max_lag = min(max_lag, 30)
        table_a, col_a = self._parse_metric_ref(metric_a)
        table_b, col_b = self._parse_metric_ref(metric_b)

        results: list[list] = []

        for lag in range(-max_lag, max_lag + 1):
            try:
                if lag == 0:
                    sql = f"""
                        SELECT ROUND(CORR(a.{col_a}, b.{col_b}), 4) AS correlation,
                               COUNT(*) AS sample_size
                        FROM {table_a} a
                        JOIN {table_b} b ON a.day = b.day
                        WHERE a.{col_a} IS NOT NULL AND b.{col_b} IS NOT NULL
                    """
                elif lag > 0:
                    sql = f"""
                        SELECT ROUND(CORR(a.{col_a}, b.{col_b}), 4) AS correlation,
                               COUNT(*) AS sample_size
                        FROM {table_a} a
                        JOIN {table_b} b ON a.day = b.day - INTERVAL '{lag} days'
                        WHERE a.{col_a} IS NOT NULL AND b.{col_b} IS NOT NULL
                    """
                else:
                    abs_lag = abs(lag)
                    sql = f"""
                        SELECT ROUND(CORR(a.{col_a}, b.{col_b}), 4) AS correlation,
                               COUNT(*) AS sample_size
                        FROM {table_a} a
                        JOIN {table_b} b ON a.day = b.day + INTERVAL '{abs_lag} days'
                        WHERE a.{col_a} IS NOT NULL AND b.{col_b} IS NOT NULL
                    """

                row = self.con.execute(sql).fetchone()
                if row and row[0] is not None:
                    corr_val = row[0]
                    sample_size = row[1]
                    strength = self._interpret_correlation(abs(corr_val))
                    results.append([lag, corr_val, sample_size, strength])
            except Exception as exc:
                logger.debug("Correlation computation failed at lag %d: %s", lag, exc)
                continue

        if not results:
            return format_error(
                f"Could not compute correlations between {metric_a} and {metric_b}. "
                "Check that both metrics exist and have overlapping date ranges."
            )

        header = f"## Correlation: {metric_a} vs {metric_b}\n\n"
        columns = ["Lag (days)", "Correlation", "Sample Size", "Strength"]
        table = format_as_markdown_table(results, columns)

        # Find strongest correlation
        best = max(results, key=lambda r: abs(r[1]))
        interpretation = (
            f"\n**Strongest correlation:** r={best[1]} at lag={best[0]} days "
            f"({best[3]}, n={best[2]})"
        )
        if best[0] > 0:
            interpretation += f"\n*{metric_a} leads {metric_b} by {best[0]} day(s).*"
        elif best[0] < 0:
            interpretation += (
                f"\n*{metric_b} leads {metric_a} by {abs(best[0])} day(s).*"
            )
        else:
            interpretation += "\n*Same-day relationship.*"

        return header + table + interpretation

    # ------------------------------------------------------------------
    # Tool 5: get_metric_definition
    # ------------------------------------------------------------------

    def get_metric_definition(self, metric_name: str) -> str:
        """Read a YAML contract and return as formatted text."""
        try:
            contract = self.query_builder.load_contract(metric_name)
        except FileNotFoundError:
            available = self.query_builder.list_metrics()
            return format_error(
                f"Unknown metric '{metric_name}'. Available: {', '.join(available)}"
            )

        metric_def = contract.get("metric", contract)

        info: dict[str, str] = {
            "Name": metric_def.get("name", metric_name),
            "Display Name": metric_def.get("display_name", ""),
            "Description": metric_def.get("description", "").strip(),
            "Category": metric_def.get("category", ""),
            "Source Table": metric_def.get("source_table", ""),
            "Source Column": metric_def.get("source_column", ""),
            "Grain": metric_def.get("grain", ""),
            "Unit": metric_def.get("unit", ""),
        }

        parts: list[str] = [f"# Metric: {info['Display Name'] or info['Name']}\n"]
        parts.append(format_as_markdown_kv(info))

        # Computations
        computations = metric_def.get("computations", {})
        if computations:
            parts.append("\n## Available Computations\n")
            for comp_name in computations:
                parts.append(f"- `{comp_name}`")

        # Thresholds
        thresholds = metric_def.get("thresholds", {})
        if thresholds:
            parts.append("\n## Thresholds\n")
            threshold_rows = []
            for level, spec in thresholds.items():
                label = spec.get("label", level)
                min_val = spec.get("min", "")
                max_val = spec.get("max", "")
                range_str = ""
                if min_val != "" and max_val != "":
                    range_str = f"{min_val}-{max_val}"
                elif min_val != "":
                    range_str = f">= {min_val}"
                elif max_val != "":
                    range_str = f"<= {max_val}"
                threshold_rows.append([level, range_str, label])
            parts.append(
                format_as_markdown_table(threshold_rows, ["Level", "Range", "Label"])
            )

        # Related metrics
        related = metric_def.get("related_metrics", [])
        if related:
            parts.append("\n## Related Metrics\n")
            for rel in related:
                parts.append(
                    f"- **{rel.get('metric', '')}** (lag {rel.get('lag', 0)}): "
                    f"{rel.get('relationship', '')}"
                )

        # Examples
        examples = metric_def.get("examples", [])
        if examples:
            parts.append("\n## Example Queries\n")
            for ex in examples:
                parts.append(
                    f'- *"{ex.get("question", "")}"* -> `{ex.get("tool", "")}`'
                )

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Tool 6: record_insight
    # ------------------------------------------------------------------

    def record_insight(
        self,
        title: str,
        content: str,
        insight_type: str = "pattern",
        confidence: float = 0.7,
        tags: Optional[list[str]] = None,
    ) -> str:
        """Save an insight to the knowledge base."""
        valid_types = {"pattern", "anomaly", "correlation", "recommendation"}
        if insight_type not in valid_types:
            return format_error(
                f"Invalid insight_type '{insight_type}'. "
                f"Valid types: {', '.join(sorted(valid_types))}"
            )

        if not 0.0 <= confidence <= 1.0:
            return format_error("Confidence must be between 0.0 and 1.0.")

        insight_id = str(uuid.uuid4())
        tags_list = tags if tags else []

        try:
            self._ensure_knowledge_base_table()
            self.con.execute(
                """
                INSERT INTO agent.knowledge_base
                    (insight_id, title, content, insight_type, confidence, tags, created_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [insight_id, title, content, insight_type, confidence, tags_list],
            )
            logger.info("Recorded insight id=%s type=%s", insight_id, insight_type)
            tags_display = ", ".join(tags_list) if tags_list else "(none)"
            return (
                f"Insight recorded successfully.\n\n"
                f"- **ID:** {insight_id}\n"
                f"- **Title:** {title}\n"
                f"- **Type:** {insight_type}\n"
                f"- **Confidence:** {confidence}\n"
                f"- **Tags:** {tags_display}"
            )
        except Exception as exc:
            logger.error("Failed to record insight: %s", exc)
            return format_error(
                "Failed to record insight. Check server logs for details."
            )

    # ------------------------------------------------------------------
    # Tool 7: get_schema_context
    # ------------------------------------------------------------------

    def get_schema_context(self, category: str) -> str:
        """Return pruned schema for a category (delegates to SchemaPruner)."""
        return self.schema_pruner.get_relevant_schema(category)

    # ------------------------------------------------------------------
    # Tool 8: run_custom_query
    # ------------------------------------------------------------------

    # Allowed schemas for custom queries — restrict to health data only
    _ALLOWED_SCHEMAS = {"silver", "agent", "gold"}

    def run_custom_query(self, sql: str, explanation: str) -> str:
        """Execute a custom read-only SQL query with audit logging.

        Security: Only SELECT queries against allowed schemas (silver, agent, gold).
        System tables (information_schema, pg_catalog, duckdb_*) are blocked.
        """
        # Must start with SELECT
        sql_upper = sql.upper().strip()
        if not sql_upper.startswith("SELECT"):
            return format_error(
                "Only SELECT queries are allowed. Use the dedicated tools for data modification."
            )

        # Block write/DDL keywords
        forbidden_keywords = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "CREATE",
            "ALTER",
            "TRUNCATE",
            "MERGE",
            "REPLACE",
            "GRANT",
            "REVOKE",
            "ATTACH",
            "DETACH",
            "COPY",
            "CALL",
            "PRAGMA",
            "EXPORT",
            "LOAD",
            "INSTALL",
        ]
        for keyword in forbidden_keywords:
            if re.search(rf"\b{keyword}\b", sql_upper):
                return format_error(
                    f"Query contains forbidden keyword '{keyword}'. "
                    "Only read-only (SELECT) queries are allowed."
                )

        # Block schema/system exploration
        blocked_patterns = [
            r"\bINFORMATION_SCHEMA\b",
            r"\bDUCKDB_\w+\(",
            r"\bPG_CATALOG\b",
            r"\bSQLITE_MASTER\b",
            r"\bGLOB\s*\(",
            r"\bREAD_CSV\b",
            r"\bREAD_PARQUET\b",
            r"\bREAD_JSON\b",
        ]
        for pattern in blocked_patterns:
            if re.search(pattern, sql_upper):
                return format_error(
                    "Query accesses system tables or file functions. "
                    "Use get_schema_context() for schema information."
                )

        # Validate table references are in allowed schemas
        # Catches FROM/JOIN and comma-joined tables (e.g. FROM silver.x, bronze.y)
        table_refs = re.findall(r"(?:FROM|JOIN)\s+(\w+)\.(\w+)", sql_upper)
        table_refs += re.findall(r",\s*(\w+)\.(\w+)", sql_upper)
        for schema, _table in table_refs:
            if schema.lower() not in self._ALLOWED_SCHEMAS:
                return format_error(
                    f"Schema '{schema}' is not allowed. "
                    f"Allowed schemas: {', '.join(sorted(self._ALLOWED_SCHEMAS))}."
                )

        # Limit result size to prevent DoS
        if "LIMIT" not in sql_upper:
            sql = sql.rstrip().rstrip(";") + " LIMIT 1000"

        logger.info("Custom query executed (reason length=%d)", len(explanation))

        try:
            result = self.con.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Custom query failed: %s", exc)
            return format_error(
                "Query execution failed. Check server logs for details."
            )

        if not rows:
            return "Query returned no results."

        return format_result(rows, columns, "markdown-table")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_date_range(self, date_range: str) -> tuple[date, date]:
        """Parse a date range string into (start_date, end_date).

        Supported formats:
        - 'today', 'yesterday'
        - 'last_7_days', 'last_14_days', 'last_30_days', 'last_90_days'
        - 'YYYY-MM-DD:YYYY-MM-DD' (explicit range)
        """
        today = date.today()

        range_map = {
            "today": (today, today),
            "yesterday": (today - timedelta(days=1), today - timedelta(days=1)),
            "last_7_days": (today - timedelta(days=7), today),
            "last_14_days": (today - timedelta(days=14), today),
            "last_30_days": (today - timedelta(days=30), today),
            "last_90_days": (today - timedelta(days=90), today),
        }

        if date_range in range_map:
            return range_map[date_range]

        # Try explicit range: YYYY-MM-DD:YYYY-MM-DD
        if ":" in date_range:
            parts = date_range.split(":")
            if len(parts) == 2:
                try:
                    start = date.fromisoformat(parts[0].strip())
                    end = date.fromisoformat(parts[1].strip())
                    return (start, end)
                except ValueError:
                    pass

        # Fallback: try single date
        try:
            single = date.fromisoformat(date_range.strip())
            return (single, single)
        except ValueError:
            pass

        # Default: last 7 days
        logger.warning(
            "Could not parse date_range '%s', defaulting to last_7_days", date_range
        )
        return (today - timedelta(days=7), today)

    def _parse_metric_ref(self, metric_ref: str) -> tuple[str, str]:
        """Parse a metric reference like 'daily_sleep.sleep_score' into (table, column).

        If no dot, tries to look up the metric contract for source_table and source_column.
        Validates all identifiers to prevent SQL injection.
        """
        if "." in metric_ref:
            parts = metric_ref.split(".", 1)
            table = validate_sql_identifier(parts[0])
            column = validate_sql_identifier(parts[1])
            # Ensure table has schema prefix
            if not table.startswith("silver.") and not table.startswith("agent."):
                table = f"silver.{table}"
            return (table, column)

        # Lookup from contract — validate even YAML-sourced identifiers
        try:
            contract = self.query_builder.load_contract(metric_ref)
            metric_def = contract.get("metric", contract)
            table = metric_def.get("source_table", f"silver.{metric_ref}")
            column = metric_def.get("source_column", metric_ref)
            # Validate table parts (schema.table)
            for part in table.split("."):
                validate_sql_identifier(part)
            validate_sql_identifier(column)
            return (table, column)
        except FileNotFoundError:
            table = validate_sql_identifier(metric_ref)
            return (f"silver.{table}", validate_sql_identifier(metric_ref))

    @staticmethod
    def _interpret_correlation(abs_r: float) -> str:
        """Interpret the strength of a correlation coefficient."""
        if abs_r >= 0.7:
            return "strong"
        elif abs_r >= 0.4:
            return "moderate"
        elif abs_r >= 0.2:
            return "weak"
        else:
            return "negligible"

    # ------------------------------------------------------------------
    # Tool 10: search_evidence
    # ------------------------------------------------------------------

    def search_evidence(
        self, query: str, max_results: int = 5, min_year: str = ""
    ) -> str:
        """Search PubMed for evidence supporting health claims."""
        from health_platform.knowledge.evidence_store import EvidenceStore

        try:
            store = EvidenceStore(self.con)
            articles = store.search(query, max_results=max_results, min_year=min_year)
            return store.format_for_mcp(articles, query)
        except Exception as exc:
            logger.error("Evidence search failed: %s", exc)
            return format_error(
                "Evidence search failed. Check server logs for details."
            )

    # ------------------------------------------------------------------
    # Tool 9: check_data_quality
    # ------------------------------------------------------------------

    def check_data_quality(
        self, table: Optional[str] = None, check_type: Optional[str] = None
    ) -> str:
        """Run data quality checks and return a formatted report."""
        from health_platform.quality.data_quality_checker import DataQualityChecker
        from health_platform.quality.models import QualityReport
        from health_platform.quality.reporters import format_for_mcp

        if table and not is_safe_identifier(table):
            return format_error(f"Invalid table name: {table!r}")
        if check_type:
            valid_types = {
                "not_null",
                "unique",
                "freshness",
                "row_count",
                "value_range",
            }
            if check_type not in valid_types:
                return format_error(
                    f"Unknown check type '{check_type}'. Valid: {', '.join(sorted(valid_types))}"
                )

        try:
            checker = DataQualityChecker(self.con)
        except Exception as exc:
            logger.error("Failed to initialize DQ checker: %s", exc)
            return format_error(
                "Failed to load quality rules. Check server logs for details."
            )

        try:
            if table:
                results = checker.run_table_checks(table)
                report = QualityReport(results=results)
                report.finish()
            elif check_type:
                results = checker.run_check_type(check_type)
                report = QualityReport(results=results)
                report.finish()
            else:
                report = checker.run_all_checks()
        except Exception as exc:
            logger.error("DQ check failed: %s", exc)
            return format_error("Quality check failed. Check server logs for details.")

        return format_for_mcp(report)

    # ------------------------------------------------------------------
    # Tool 11: forecast_metric
    # ------------------------------------------------------------------

    def forecast_metric(
        self,
        table: str,
        column: str,
        date_column: str = "day",
        lookback_days: int = 90,
        forecast_days: int = 7,
    ) -> str:
        """Forecast a metric using linear regression."""
        from health_platform.ai.trend_forecaster import TrendForecaster, format_forecast

        # Validate all user-supplied identifiers to prevent SQL injection
        try:
            validate_sql_identifier(table)
            validate_sql_identifier(column)
            validate_sql_identifier(date_column)
        except ValueError as exc:
            return format_error(f"Invalid identifier: {exc}")

        try:
            forecaster = TrendForecaster(self.con)
            forecast = forecaster.forecast_metric(
                table, column, date_column, lookback_days, forecast_days
            )
            return format_forecast(forecast)
        except Exception as exc:
            logger.error("Forecast failed: %s", exc)
            return format_error(f"Forecast failed: {exc}")

    # ------------------------------------------------------------------
    # Tool 12: get_cross_source_insights
    # ------------------------------------------------------------------

    def get_cross_source_insights(self) -> str:
        """Run all correlations and return strongest relationships."""
        from health_platform.ai.correlation_engine import compute_all_correlations

        try:
            count = compute_all_correlations(self.con)

            result = self.con.execute(
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

            title = "# Cross-Source Insights\n\n"
            header = title + f"Computed {count} correlations. Showing strongest:\n\n"
            return header + format_as_markdown_table(rows, columns)
        except Exception as exc:
            logger.error("Cross-source insights failed: %s", exc)
            return format_error(f"Cross-source insights failed: {exc}")

    # ------------------------------------------------------------------
    # Tool 13: get_recommendations
    # ------------------------------------------------------------------

    def get_recommendations(self, max_results: int = 5) -> str:
        """Generate personalized health recommendations."""
        from health_platform.ai.recommendation_engine import (
            RecommendationEngine,
            format_recommendations,
        )

        try:
            engine = RecommendationEngine(self.con)
            recs = engine.get_recommendations(max_results=max_results)
            return format_recommendations(recs)
        except Exception as exc:
            logger.error("Recommendations failed: %s", exc)
            return format_error(f"Recommendations failed: {exc}")

    # ------------------------------------------------------------------
    # Tool 14: explain_recommendation
    # ------------------------------------------------------------------

    def explain_recommendation(self, recommendation_title: str) -> str:
        """Explain the data behind a recommendation in detail."""
        from health_platform.ai.recommendation_engine import RecommendationEngine

        try:
            engine = RecommendationEngine(self.con)
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
                        "## Metrics Used\n"
                        + "\n".join(f"- `{m}`" for m in r.metrics_used),
                    ]
                    return "\n".join(parts)

            titles = [r.title for r in recs]
            return (
                f"Recommendation not found: '{recommendation_title}'. "
                f"Available: {', '.join(titles)}"
            )
        except Exception as exc:
            logger.error("Explain recommendation failed: %s", exc)
            return format_error(f"Explain recommendation failed: {exc}")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Tool 11: detect_anomalies
    # ------------------------------------------------------------------

    def detect_anomalies(self, lookback_days: int = 7) -> str:
        """Run anomaly detection and return formatted report."""
        from health_platform.ai.anomaly_detector import (
            AnomalyDetector,
            format_anomaly_report,
        )

        detector = AnomalyDetector(self.con)
        report = detector.detect(lookback_days)
        return format_anomaly_report(report)

    # ------------------------------------------------------------------
    # Tool 15: query_lab_results
    # ------------------------------------------------------------------

    def query_lab_results(
        self,
        marker_name: Optional[str] = None,
        date_range: Optional[str] = None,
        status: Optional[str] = None,
    ) -> str:
        """Query lab test results with optional filters.

        Args:
            marker_name: Filter by marker name (case-insensitive partial match).
            date_range: Date range string (see _parse_date_range).
            status: Filter by status ('normal', 'low', 'high').

        Returns:
            Formatted markdown table of matching lab results.
        """
        conditions: list[str] = []
        params: list = []

        if marker_name:
            conditions.append("LOWER(marker_name) LIKE '%' || LOWER(?) || '%'")
            params.append(marker_name)

        if date_range:
            start, end = self._parse_date_range(date_range)
            conditions.append("test_date BETWEEN ? AND ?")
            params.extend([start.isoformat(), end.isoformat()])

        if status:
            valid_statuses = {"normal", "low", "high", "unknown"}
            if status.lower() not in valid_statuses:
                return format_error(
                    f"Invalid status '{status}'. Valid: {', '.join(sorted(valid_statuses))}"
                )
            conditions.append("LOWER(status) = LOWER(?)")
            params.append(status)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
            SELECT test_date, marker_name, marker_category, value_numeric,
                   value_text, unit, reference_min, reference_max, status
            FROM silver.lab_results
            WHERE {where_clause}
            ORDER BY test_date DESC, marker_category, marker_name
            LIMIT 100
        """

        try:
            result = self.con.execute(sql, params)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Lab results query failed: %s", exc)
            return format_error(
                "Lab results query failed. Check server logs for details."
            )

        if not rows:
            filter_desc = []
            if marker_name:
                filter_desc.append(f"marker='{marker_name}'")
            if date_range:
                filter_desc.append(f"dates='{date_range}'")
            if status:
                filter_desc.append(f"status='{status}'")
            filter_str = ", ".join(filter_desc) if filter_desc else "no filters"
            return f"No lab results found ({filter_str})."

        return format_as_markdown_table(rows, columns)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Tool 16: query_genetics
    # ------------------------------------------------------------------

    def query_genetics(
        self,
        query_type: Optional[str] = None,
        category: Optional[str] = None,
        report_name: Optional[str] = None,
    ) -> str:
        """Query genetics data across health findings, ancestry, and family tree.

        Args:
            query_type: 'health', 'ancestry', 'family', or None (= dashboard overview).
            category: Filter by category (e.g., 'Scandinavian', 'health_risk', 'maternal').
            report_name: Filter by report name (partial match).

        Returns:
            Formatted markdown output.
        """
        if query_type == "health" or (query_type is None and report_name):
            return self._query_genetics_health(category, report_name)
        elif query_type == "ancestry":
            return self._query_genetics_ancestry(category)
        elif query_type == "family":
            return self._query_genetics_family(category)
        else:
            return self._query_genetics_dashboard()

    def _query_genetics_health(
        self,
        category: Optional[str] = None,
        report_name: Optional[str] = None,
    ) -> str:
        """Query genetic health findings."""
        conditions: list[str] = []
        params: list = []

        if category:
            conditions.append("LOWER(category) = LOWER(?)")
            params.append(category)
        if report_name:
            conditions.append("LOWER(report_name) LIKE '%' || LOWER(?) || '%'")
            # Strip LIKE wildcards to prevent unintended pattern matching
            params.append(report_name.replace("%", "").replace("_", ""))

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT report_name, category, gene, snp_id, genotype,
                   risk_level, result_summary, variant_detected
            FROM silver.genetic_health_findings
            WHERE {where_clause}
            ORDER BY
                CASE risk_level
                    WHEN 'high' THEN 1 WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3 ELSE 4
                END,
                category, report_name
        """
        try:
            result = self.con.execute(sql, params)
            columns = [d[0] for d in result.description]
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Genetics health query failed: %s", exc)
            return format_error("Genetics health query failed.")

        if not rows:
            return "No genetic health findings found."
        return "# Genetic Health Findings\n\n" + format_as_markdown_table(rows, columns)

    def _query_genetics_ancestry(self, category: Optional[str] = None) -> str:
        """Query ancestry segments."""
        if category:
            sql = """
                SELECT ancestry_category, chromosome, copy,
                       segment_length_bp, pct_of_chromosome
                FROM silver.ancestry_segments
                WHERE LOWER(ancestry_category) LIKE '%' || LOWER(?) || '%'
                ORDER BY chromosome, copy, start_bp
            """
            params: list = [category]
        else:
            sql = """
                SELECT ancestry_category,
                       COUNT(*) AS segment_count,
                       SUM(segment_length_bp) AS total_bp,
                       ROUND(SUM(segment_length_bp) * 100.0 / 6400000000, 2) AS pct_of_genome
                FROM silver.ancestry_segments
                WHERE copy = 1
                GROUP BY ancestry_category
                ORDER BY total_bp DESC
            """
            params = []

        try:
            result = self.con.execute(sql, params)
            columns = [d[0] for d in result.description]
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Genetics ancestry query failed: %s", exc)
            return format_error("Genetics ancestry query failed.")

        if not rows:
            return "No ancestry data found."
        return "# Ancestry Composition\n\n" + format_as_markdown_table(rows, columns)

    def _query_genetics_family(self, side: Optional[str] = None) -> str:
        """Query family tree."""
        conditions: list[str] = []
        params: list = []

        if side:
            conditions.append("(LOWER(side) = LOWER(?) OR side = 'both')")
            params.append(side)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT relationship_to_user, generation, side,
                   num_shared_segments, has_dna_match,
                   first_name, last_name
            FROM silver.family_tree
            WHERE {where_clause}
            ORDER BY generation, relationship_to_user
        """
        try:
            result = self.con.execute(sql, params)
            columns = [d[0] for d in result.description]
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Genetics family query failed: %s", exc)
            return format_error("Genetics family query failed.")

        if not rows:
            return "No family tree data found."
        return "# Family Tree\n\n" + format_as_markdown_table(rows, columns)

    def _query_genetics_dashboard(self) -> str:
        """Return a dashboard overview of all genetics data."""
        parts: list[str] = ["# Genetics Dashboard\n"]

        # Health findings summary
        try:
            result = self.con.execute(
                """
                SELECT category, COUNT(*) AS findings,
                       SUM(CASE WHEN variant_detected THEN 1 ELSE 0 END) AS variants_found
                FROM silver.genetic_health_findings
                GROUP BY category
                ORDER BY category
                """
            )
            rows = result.fetchall()
            if rows:
                columns = [d[0] for d in result.description]
                parts.append("## Health Findings by Category\n")
                parts.append(format_as_markdown_table(rows, columns))
        except Exception:
            pass

        # Ancestry summary
        try:
            result = self.con.execute(
                """
                SELECT ancestry_category,
                       ROUND(SUM(segment_length_bp) * 100.0 / 6400000000, 2) AS pct_of_genome
                FROM silver.ancestry_segments
                WHERE copy = 1
                GROUP BY ancestry_category
                HAVING pct_of_genome > 0.5
                ORDER BY pct_of_genome DESC
                """
            )
            rows = result.fetchall()
            if rows:
                columns = [d[0] for d in result.description]
                parts.append("\n## Ancestry Overview\n")
                parts.append(format_as_markdown_table(rows, columns))
        except Exception:
            pass

        # Family tree summary
        try:
            result = self.con.execute(
                """
                SELECT COUNT(*) AS total_members,
                       SUM(CASE WHEN has_dna_match THEN 1 ELSE 0 END) AS dna_matches,
                       MIN(generation) AS oldest_generation,
                       MAX(generation) AS youngest_generation
                FROM silver.family_tree
                """
            )
            row = result.fetchone()
            if row and row[0] > 0:
                parts.append("\n## Family Tree Summary\n")
                parts.append(f"- **Total members:** {row[0]}")
                parts.append(f"- **DNA matches:** {row[1]}")
                parts.append(f"- **Generations:** {row[2]} to {row[3]}")
        except Exception:
            pass

        return "\n".join(parts) if len(parts) > 1 else "No genetics data available."

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _ensure_knowledge_base_table(self) -> None:
        """Create agent.knowledge_base if it does not exist."""
        self.con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS agent.knowledge_base (
                insight_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                insight_type VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                content VARCHAR NOT NULL,
                evidence_query VARCHAR,
                confidence DOUBLE NOT NULL,
                tags VARCHAR[],
                embedding FLOAT[384],
                is_active BOOLEAN DEFAULT true,
                superseded_by VARCHAR
            )
        """
        )
