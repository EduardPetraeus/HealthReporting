"""Schema pruner for MCP tool context.

Provides only the relevant tables and columns for a given question
category, reducing token usage and improving query accuracy.

Reads the _index.yml contract to determine which tables are relevant,
then queries DuckDB information_schema for column details.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from health_platform.utils.logging_config import get_logger

logger = get_logger("schema_pruner")


class SchemaPruner:
    """Provides pruned schema context based on question category.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection for querying information_schema.
    contracts_dir : Path
        Path to contracts/metrics/ directory containing _index.yml.
    """

    def __init__(self, con, contracts_dir: Path) -> None:
        self.con = con
        self.contracts_dir = contracts_dir
        self._index: dict | None = None

    def _load_index(self) -> dict:
        """Load and cache _index.yml."""
        if self._index is not None:
            return self._index

        index_path = self.contracts_dir / "_index.yml"
        if not index_path.exists():
            logger.warning("_index.yml not found at %s", index_path)
            self._index = {}
            return self._index

        with open(index_path, "r", encoding="utf-8") as f:
            self._index = yaml.safe_load(f) or {}

        return self._index

    def get_relevant_schema(self, category: str) -> str:
        """Return a markdown description of relevant tables and columns.

        Parameters
        ----------
        category : str
            Category from the index: 'sleep', 'vitals', 'activity',
            'recovery', 'body', 'nutrition', 'cross_domain', 'hygiene',
            'gait', 'mindfulness'.

        Returns
        -------
        str
            Markdown-formatted schema context including table descriptions,
            column types, and available computations.
        """
        index = self._load_index()

        # Resolve tables from the metrics section
        metrics_section = index.get("metrics", {})
        query_routing = index.get("query_routing", {})

        source_tables: list[str] = []
        available_metrics: list[str] = []

        # Check metrics section first
        if category in metrics_section:
            cat_info = metrics_section[category]
            source_tables = cat_info.get("source_tables", [])
            available_metrics = cat_info.get("metrics", [])
        # Fall back to query_routing section
        elif f"{category}_questions" in query_routing:
            routing_info = query_routing[f"{category}_questions"]
            source_tables = routing_info.get("tables", [])
        elif category in query_routing:
            routing_info = query_routing[category]
            source_tables = routing_info.get("tables", [])
        else:
            available_categories = sorted(
                set(list(metrics_section.keys()) + [
                    k.replace("_questions", "")
                    for k in query_routing.keys()
                ])
            )
            return (
                f"**Error:** Unknown category '{category}'.\n\n"
                f"Available categories: {', '.join(available_categories)}"
            )

        if not source_tables:
            return f"No source tables defined for category '{category}'."

        # Build schema context
        parts: list[str] = [f"## Relevant tables for: {category}\n"]

        for table_name in source_tables:
            columns = self.get_table_columns(table_name)
            if columns:
                parts.append(f"### {table_name}")
                parts.append("| Column | Type | Description |")
                parts.append("|---|---|---|")
                for col in columns:
                    parts.append(
                        f"| {col['name']} | {col['type']} | {col['description']} |"
                    )
                parts.append("")
            else:
                parts.append(f"### {table_name}")
                parts.append("*(Table not found or no columns available)*\n")

        # Add available metrics
        if available_metrics:
            parts.append("### Available metrics")
            for m in available_metrics:
                parts.append(f"- `{m}`")
            parts.append("")

        # Add standard computations
        parts.append("### Available computations")
        parts.append("- `daily_value`: Get value for a specific date")
        parts.append("- `period_average`: Average over date range")
        parts.append("- `trend`: 7-day rolling average")
        parts.append("- `distribution`: Distribution across thresholds")
        parts.append("")

        # Add output format recommendation
        output_config = index.get("output_format", {})
        if output_config:
            default_fmt = output_config.get("default", "markdown-table")
            parts.append(f"### Output format")
            parts.append(f"Default: `{default_fmt}`")
            alternatives = output_config.get("alternatives", [])
            if alternatives:
                parts.append(
                    f"Alternatives: {', '.join(f'`{a}`' for a in alternatives)}"
                )

        return "\n".join(parts)

    def get_table_columns(self, table_name: str) -> list[dict]:
        """Query DuckDB information_schema for column names, types, and comments.

        Parameters
        ----------
        table_name : str
            Fully qualified table name (e.g., 'silver.daily_sleep').

        Returns
        -------
        list[dict]
            List of dicts with keys: 'name', 'type', 'description'.
        """
        # Parse schema and table from dotted name
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema = "main"
            table = table_name

        # Query duckdb_columns() for column details including COMMENT ON descriptions
        sql = """
            SELECT
                column_name,
                data_type,
                COALESCE(comment, '') AS description
            FROM duckdb_columns()
            WHERE schema_name = ?
              AND table_name = ?
            ORDER BY column_index
        """
        try:
            result = self.con.execute(sql, [schema, table])
            rows = result.fetchall()
        except Exception as exc:
            logger.debug(
                "Could not query duckdb_columns for %s: %s", table_name, exc
            )
            # Fallback: try PRAGMA table_info
            return self._get_columns_via_pragma(table_name)

        if not rows:
            # Fallback to PRAGMA if information_schema returned nothing
            return self._get_columns_via_pragma(table_name)

        columns = []
        for row in rows:
            columns.append({
                "name": row[0],
                "type": row[1],
                "description": row[2] if row[2] else "--",
            })

        return columns

    def _get_columns_via_pragma(self, table_name: str) -> list[dict]:
        """Fallback column discovery using PRAGMA table_info.

        Parameters
        ----------
        table_name : str
            Fully qualified table name.

        Returns
        -------
        list[dict]
            List of dicts with keys: 'name', 'type', 'description'.
        """
        try:
            result = self.con.execute(f"PRAGMA table_info('{table_name}')")
            rows = result.fetchall()
        except Exception as exc:
            logger.debug("PRAGMA table_info failed for %s: %s", table_name, exc)
            return []

        columns = []
        for row in rows:
            # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            columns.append({
                "name": row[1],
                "type": row[2],
                "description": "--",
            })

        return columns
