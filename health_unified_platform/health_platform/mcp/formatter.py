"""Output formatting for MCP tool responses.

Provides functions to format query results as markdown tables,
key-value pairs, YAML, and error messages. All output is designed
to be consumed by AI agents via the MCP protocol.
"""
from __future__ import annotations

from typing import Any

import yaml


def format_as_markdown_table(rows: list, columns: list[str]) -> str:
    """Format rows and columns as a standard markdown table.

    Parameters
    ----------
    rows : list
        List of row tuples or lists.
    columns : list[str]
        Column header names.

    Returns
    -------
    str
        Markdown-formatted table string.
    """
    if not rows or not columns:
        return ""

    # Convert all values to strings, handle None
    str_rows = []
    for row in rows:
        str_row = []
        for val in row:
            if val is None:
                str_row.append("--")
            elif isinstance(val, float):
                # Format floats cleanly
                if val == int(val):
                    str_row.append(str(int(val)))
                else:
                    str_row.append(f"{val:.4f}".rstrip("0").rstrip("."))
            else:
                str_row.append(str(val))
        str_rows.append(str_row)

    # Calculate column widths for alignment
    widths = [len(col) for col in columns]
    for row in str_rows:
        for i, val in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(val))

    # Build header
    header = "| " + " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns)) + " |"
    separator = "|" + "|".join("-" * (w + 2) for w in widths) + "|"

    # Build rows
    lines = [header, separator]
    for row in str_rows:
        padded = []
        for i, val in enumerate(row):
            if i < len(widths):
                padded.append(val.ljust(widths[i]))
            else:
                padded.append(val)
        lines.append("| " + " | ".join(padded) + " |")

    return "\n".join(lines)


def format_as_markdown_kv(data: dict[str, Any]) -> str:
    """Format a dictionary as markdown key-value pairs.

    Parameters
    ----------
    data : dict
        Key-value pairs to format.

    Returns
    -------
    str
        Formatted string with one key-value pair per line.
    """
    if not data:
        return ""

    lines = []
    for key, value in data.items():
        if value is None or value == "":
            continue
        # Truncate very long values
        str_value = str(value)
        if len(str_value) > 200:
            str_value = str_value[:200] + "..."
        lines.append(f"- **{key}:** {str_value}")

    return "\n".join(lines)


def format_as_yaml(data: dict) -> str:
    """Format a dictionary as YAML output.

    Parameters
    ----------
    data : dict
        Data to format as YAML.

    Returns
    -------
    str
        YAML-formatted string wrapped in a code block.
    """
    if not data:
        return "```yaml\n{}\n```"

    yaml_str = yaml.dump(
        data,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )
    return f"```yaml\n{yaml_str}```"


def format_result(
    rows: list,
    columns: list[str],
    format_type: str = "markdown-table",
) -> str:
    """Dispatch to the appropriate formatter based on format_type.

    Parameters
    ----------
    rows : list
        Query result rows.
    columns : list[str]
        Column names.
    format_type : str
        One of 'markdown-table', 'markdown-kv', 'yaml'.

    Returns
    -------
    str
        Formatted output string.
    """
    if format_type == "markdown-kv":
        if not rows:
            return ""
        # Convert first row to dict
        data = dict(zip(columns, rows[0]))
        return format_as_markdown_kv(data)

    if format_type == "yaml":
        if not rows:
            return format_as_yaml({})
        result_list = [dict(zip(columns, row)) for row in rows]
        if len(result_list) == 1:
            return format_as_yaml(result_list[0])
        return format_as_yaml({"results": result_list})

    # Default: markdown-table
    return format_as_markdown_table(rows, columns)


def format_error(error: str) -> str:
    """Format an error message for MCP tool output.

    Parameters
    ----------
    error : str
        Error description.

    Returns
    -------
    str
        Formatted error string with a clear error marker.
    """
    return f"**Error:** {error}"


def format_empty(metric: str, date_range: str) -> str:
    """Format a 'no data' message for MCP tool output.

    Parameters
    ----------
    metric : str
        The metric that was queried.
    date_range : str
        The date range that was queried.

    Returns
    -------
    str
        User-friendly message indicating no data was found.
    """
    return (
        f"No data found for metric **{metric}** in range **{date_range}**.\n\n"
        "Possible reasons:\n"
        "- The date range has no recorded data\n"
        "- The metric name may be incorrect (use `get_metric_definition` to check)\n"
        "- Data ingestion may not have run for this period"
    )
