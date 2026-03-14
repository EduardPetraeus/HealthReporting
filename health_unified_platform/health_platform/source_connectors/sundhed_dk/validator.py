"""Validation framework for sundhed.dk scraper pipeline.

Three-layer validation ensuring zero data loss:
1. HTML archive — forensic audit trail of raw HTML
2. Row count — expected rows in HTML vs parsed count
3. Source hash — row-level tracing via MD5 of raw cell content

Usage:
    report = validate_parse_completeness(html, records, "lab_results")
    print(generate_validation_report([report]))
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from bs4 import BeautifulSoup
from health_platform.utils.logging_config import get_logger

logger = get_logger("sundhed_dk.validator")

# Row count selectors per section — maps section name to CSS selectors
# that identify data rows in the raw HTML
ROW_SELECTORS: dict[str, list[str]] = {
    "lab_results": [
        "div.ui-grid-pinned-container-left div.ui-grid-viewport div.ui-grid-row",
    ],
    "medications": [
        "tbody tr.sdk-table-row",
    ],
    "vaccinations": [
        "tbody tr.sdk-table-row",
    ],
    "ejournal": [
        ".journal-entry",
        ".ejournal-item",
        "article.note",
    ],
    "appointments": [
        "datatable-body-row",
    ],
}

# Selectors for group headers that should be excluded from row counts
GROUP_HEADER_SELECTORS: dict[str, list[str]] = {
    "lab_results": ["span.group-header"],
}


@dataclass
class ValidationReport:
    """Validation result for a single section."""

    section: str
    html_rows: int
    parsed_rows: int
    match: bool
    status: str
    errors: list[str] = field(default_factory=list)

    @property
    def is_ok(self) -> bool:
        return self.match and self.status == "OK"


def count_expected_rows(html: str, section: str) -> int:
    """Count expected data rows in raw HTML for a given section.

    Uses section-specific CSS selectors to find data rows,
    excluding group headers and empty rows.
    """
    if not html or not html.strip():
        return 0

    soup = BeautifulSoup(html, "lxml")
    selectors = ROW_SELECTORS.get(section, [])
    group_selectors = GROUP_HEADER_SELECTORS.get(section, [])

    total = 0
    for selector in selectors:
        rows = soup.select(selector)
        for row in rows:
            # Skip group header rows
            is_group = False
            for gs in group_selectors:
                if row.select_one(gs):
                    is_group = True
                    break
            if is_group:
                continue

            # Skip rows with no text content
            text = row.get_text(strip=True)
            if not text:
                continue

            total += 1

    return total


def compute_row_source_hash(raw_cell_content: str) -> str:
    """Compute MD5 hash of raw HTML cell content for row-level tracing."""
    normalized = re.sub(r"\s+", " ", raw_cell_content.strip())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def validate_parse_completeness(
    html: str,
    records: list[dict],
    section: str,
) -> ValidationReport:
    """Validate that parsing extracted all rows from the HTML.

    Compares expected row count from raw HTML against actual parsed records.
    A mismatch indicates data loss — the HTML should be inspected.
    """
    html_rows = count_expected_rows(html, section)
    parsed_rows = len(records)
    errors: list[str] = []

    if html_rows == 0 and parsed_rows == 0:
        return ValidationReport(
            section=section,
            html_rows=0,
            parsed_rows=0,
            match=True,
            status="EMPTY",
        )

    match = html_rows == parsed_rows
    status = "OK" if match else "MISMATCH"

    if not match:
        delta = html_rows - parsed_rows
        direction = "fewer" if delta > 0 else "more"
        errors.append(
            f"Row count mismatch: {parsed_rows} parsed vs {html_rows} in HTML "
            f"({abs(delta)} {direction} records parsed)"
        )
        logger.warning(
            "Validation MISMATCH for %s: HTML=%d, parsed=%d",
            section,
            html_rows,
            parsed_rows,
        )

    return ValidationReport(
        section=section,
        html_rows=html_rows,
        parsed_rows=parsed_rows,
        match=match,
        status=status,
        errors=errors,
    )


def generate_validation_report(reports: list[ValidationReport]) -> str:
    """Generate a formatted validation report table.

    Returns a human-readable string:
        Section          | HTML Rows | Parsed | Match | Status
        lab_results      | 42        | 42     | YES   | OK
        medications      | 15        | 15     | YES   | OK
    """
    lines = [
        f"{'Section':<20} | {'HTML Rows':>9} | {'Parsed':>6} | {'Match':>5} | Status",
        "-" * 70,
    ]

    all_ok = True
    for r in reports:
        match_str = "YES" if r.match else "NO"
        lines.append(
            f"{r.section:<20} | {r.html_rows:>9} | {r.parsed_rows:>6} | {match_str:>5} | {r.status}"
        )
        if not r.is_ok:
            all_ok = False
            for err in r.errors:
                lines.append(f"  ⚠ {err}")

    lines.append("-" * 70)
    summary = "ALL SECTIONS OK" if all_ok else "VALIDATION ERRORS DETECTED"
    lines.append(f"Result: {summary}")

    return "\n".join(lines)
