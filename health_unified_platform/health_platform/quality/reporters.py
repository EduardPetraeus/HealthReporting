"""Output formatters for data quality reports."""

from __future__ import annotations

from health_platform.quality.models import QualityReport


def format_for_log(report: QualityReport) -> str:
    """Structured log output for daily_sync.sh."""
    lines = [f"DQ Report: {report.passed}/{report.total} passed ({report.pass_rate}%)"]
    for r in report.results:
        if not r.passed:
            prefix = "FAIL"
            table_col = f"{r.table_name}.{r.column}" if r.column else r.table_name
            lines.append(f"  [{prefix}] {r.check_type} — {table_col}: {r.message}")
    return "\n".join(lines)


def format_for_ntfy(report: QualityReport) -> str:
    """Short summary for push notification via ntfy.sh."""
    if report.failed == 0:
        return f"DQ OK: {report.total} checks passed"

    fail_summary = []
    for r in report.results:
        if not r.passed:
            table_col = f"{r.table_name}.{r.column}" if r.column else r.table_name
            fail_summary.append(f"{r.check_type}:{table_col}")

    top_failures = fail_summary[:5]
    extra = f" (+{len(fail_summary) - 5} more)" if len(fail_summary) > 5 else ""
    return (
        f"DQ: {report.failed}/{report.total} FAILED — "
        + ", ".join(top_failures)
        + extra
    )


def format_for_mcp(report: QualityReport) -> str:
    """Markdown table output for MCP tool responses."""
    lines = [
        "# Data Quality Report\n",
        f"**{report.passed}/{report.total} checks passed ({report.pass_rate}%)**\n",
    ]

    if report.failed > 0:
        lines.append("## Failed Checks\n")
        lines.append("| Table | Check | Column | Message |")
        lines.append("|-------|-------|--------|---------|")
        for r in report.results:
            if not r.passed:
                col = r.column or "—"
                lines.append(
                    f"| {r.table_name} | {r.check_type} | {col} | {r.message} |"
                )

    if report.passed > 0 and report.failed > 0:
        lines.append("\n## Passed Checks\n")
        lines.append(f"{report.passed} checks passed across all tables.")

    return "\n".join(lines)
