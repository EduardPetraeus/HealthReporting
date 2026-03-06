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

logger = get_logger("health_tools")

# Path to semantic contracts directory
_CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts" / "metrics"


class HealthTools:
    """Implements all 8 MCP tool operations against a DuckDB connection."""

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
            return format_error(f"Query execution failed: {exc}")

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
            return format_error(f"Failed to load profile: {exc}")

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
            logger.info("Recorded insight: %s (id=%s)", title, insight_id)
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
            return format_error(f"Failed to record insight: {exc}")

    # ------------------------------------------------------------------
    # Tool 7: get_schema_context
    # ------------------------------------------------------------------

    def get_schema_context(self, category: str) -> str:
        """Return pruned schema for a category (delegates to SchemaPruner)."""
        return self.schema_pruner.get_relevant_schema(category)

    # ------------------------------------------------------------------
    # Tool 8: run_custom_query
    # ------------------------------------------------------------------

    def run_custom_query(self, sql: str, explanation: str) -> str:
        """Execute a custom read-only SQL query with audit logging."""
        # Validate read-only
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
        ]
        sql_upper = sql.upper().strip()
        for keyword in forbidden_keywords:
            # Match keyword as a whole word to avoid false positives
            pattern = rf"\b{keyword}\b"
            if re.search(pattern, sql_upper):
                return format_error(
                    f"Query contains forbidden keyword '{keyword}'. "
                    "Only read-only (SELECT) queries are allowed."
                )

        logger.info("Custom query executed. Reason: %s", explanation)

        try:
            result = self.con.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Custom query failed: %s", exc)
            return format_error(f"Query execution failed: {exc}")

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

    _SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    def _validate_identifier(self, name: str) -> str:
        """Validate that a string is a safe SQL identifier (prevents injection)."""
        if not self._SAFE_IDENTIFIER.match(name):
            raise ValueError(f"Invalid SQL identifier: {name!r}")
        return name

    def _parse_metric_ref(self, metric_ref: str) -> tuple[str, str]:
        """Parse a metric reference like 'daily_sleep.sleep_score' into (table, column).

        If no dot, tries to look up the metric contract for source_table and source_column.
        Validates all identifiers to prevent SQL injection.
        """
        if "." in metric_ref:
            parts = metric_ref.split(".", 1)
            table = self._validate_identifier(parts[0])
            column = self._validate_identifier(parts[1])
            # Ensure table has schema prefix
            if not table.startswith("silver.") and not table.startswith("agent."):
                table = f"silver.{table}"
            return (table, column)

        # Lookup from contract
        try:
            contract = self.query_builder.load_contract(metric_ref)
            metric_def = contract.get("metric", contract)
            table = metric_def.get("source_table", f"silver.{metric_ref}")
            column = metric_def.get("source_column", metric_ref)
            return (table, column)
        except FileNotFoundError:
            return (f"silver.{metric_ref}", metric_ref)

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
