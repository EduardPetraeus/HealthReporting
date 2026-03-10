"""Pure HTML-to-dict parsers for sundhed.dk data sections.

Each function takes raw HTML (string) and returns a list of dicts.
No browser or network dependency — fully testable with fixtures.

sundhed.dk uses AngularJS 1.x with sdk-table components for medications
and vaccinations, Angular UI Grid for lab results, and ngx-datatable
for appointments. CSS selectors are based on the actual HTML structure
observed in March 2026.

Reuses Danish number parsing from the existing lab PDF parser.
"""

from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup, Tag
from health_platform.source_connectors.lab.pdf_parser import (
    _parse_danish_number,
)
from health_platform.utils.logging_config import get_logger

logger = get_logger("sundhed_dk.parsers")

# ---------------------------------------------------------------------------
# CSS selectors — based on actual sundhed.dk HTML (March 2026)
# ---------------------------------------------------------------------------

# Lab results: Angular UI Grid with pinned left panel (names+units)
# and scrollable body (date columns with values)
SEL_LAB_GRID = "div#labsvargrid"
SEL_LAB_LEFT_ROWS = (
    "div.ui-grid-pinned-container-left div.ui-grid-viewport div.ui-grid-row"
)
SEL_LAB_NAME_CELL = "div.ui-grid-cell[class*='coltestName']"
SEL_LAB_UNIT_CELL = "div.ui-grid-cell[class*='coltestUnit']"
SEL_LAB_BODY_HEADER = "div.ui-grid-render-container-body div.ui-grid-header-cell"
SEL_LAB_BODY_ROWS = (
    "div.ui-grid-render-container-body div.ui-grid-viewport div.ui-grid-row"
)
SEL_LAB_VALUE_CELL = "div.ui-grid-cell"
SEL_LAB_GROUP_HEADER = "span.group-header"
SEL_LAB_CELL_TEXT = "span[bind-html-compile]"

# Medications: sdk-table with standard HTML <table>
SEL_MED_TABLE = "table.sdk-table"
SEL_MED_ROW = "tbody tr.sdk-table-row"
SEL_MED_CELL_VALUE = "span.ng-binding"

# Vaccinations: sdk-table with effectuated-vaccinations class
SEL_VAC_TABLE = "table.sdk-table.effectuated-vaccinations, table.sdk-table"
SEL_VAC_ROW = "tbody tr.sdk-table-row"
SEL_VAC_CELL_VALUE = "span.ng-binding"

# E-journal: needs URL /min-sundhedsjournal/journal-fra-sygehus/
# Structure TBD — requires separate HTML dump from the correct URL
SEL_JOURNAL_ENTRY = ".journal-entry, .ejournal-item, article.note"
SEL_JOURNAL_DATE = ".note-date, .entry-date, time"
SEL_JOURNAL_DEPT = ".department, .afdeling"
SEL_JOURNAL_HOSPITAL = ".hospital, .sygehus"
SEL_JOURNAL_TYPE = ".note-type, .entry-type"
SEL_JOURNAL_TEXT = ".note-text, .entry-content, .journal-text"

# Appointments: ngx-datatable (Angular 6 component)
SEL_APPT_SECTION = "front-page > div.app-dnhf-front, div.app-dnhf-front"
SEL_APPT_TABLE = "ngx-datatable"
SEL_APPT_ROW = "datatable-body-row"
SEL_APPT_CELL = "datatable-body-cell"

# Date patterns common on sundhed.dk: "03.01.2026", "03-01-2026", "3. januar 2026"
_DATE_DMY_RE = re.compile(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})")
_DANISH_MONTHS = {
    "januar": "01",
    "februar": "02",
    "marts": "03",
    "april": "04",
    "maj": "05",
    "juni": "06",
    "juli": "07",
    "august": "08",
    "september": "09",
    "oktober": "10",
    "november": "11",
    "december": "12",
}
_DATE_NAMED_RE = re.compile(
    r"(\d{1,2})\.\s*(" + "|".join(_DANISH_MONTHS.keys()) + r")\s+(\d{4})",
    re.IGNORECASE,
)

# Regex for parsing medication drug column: "BrandName (Ingredient)\nForm, Strength"
_MED_NAME_RE = re.compile(r"^(.+?)\s*\((.+?)\)")


def _parse_danish_date(text: str) -> Optional[str]:
    """Parse a Danish date string into ISO format (YYYY-MM-DD).

    Handles:
        "03-01-2026" -> "2026-01-03"
        "03.01.2026" -> "2026-01-03"
        "3. januar 2026" -> "2026-01-03"
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Try named month first: "3. januar 2026"
    match = _DATE_NAMED_RE.search(text)
    if match:
        day = int(match.group(1))
        month = _DANISH_MONTHS[match.group(2).lower()]
        year = match.group(3)
        return f"{year}-{month}-{day:02d}"

    # Try numeric: "03-01-2026" or "03.01.2026"
    match = _DATE_DMY_RE.search(text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = match.group(3)
        return f"{year}-{month:02d}-{day:02d}"

    return None


def _text(element: Optional[Tag], selector: str) -> str:
    """Extract text from a child element matching selector, or empty string."""
    if element is None:
        return ""
    child = element.select_one(selector)
    if child is None:
        return ""
    return child.get_text(strip=True)


def _cell_text(cells: list, index: int, selector: str = "span.ng-binding") -> str:
    """Extract text from a specific cell by index, using inner selector."""
    if index >= len(cells):
        return ""
    span = cells[index].select_one(selector)
    if span is None:
        # Fallback: use the cell text directly
        return cells[index].get_text(strip=True)
    return span.get_text(strip=True)


def _map_status_label(label: str) -> str:
    """Map Danish status labels to normalized values."""
    lower = label.strip().lower()
    if lower in {"normal", "ok", "inden for reference", "i.r.", "i orden"}:
        return "normal"
    if lower in {"lav", "low", "l", "under", "for lav"}:
        return "low"
    if lower in {"høj", "high", "h", "over", "for høj"}:
        return "high"
    return lower if lower else "unknown"


# ---------------------------------------------------------------------------
# Public parser functions
# ---------------------------------------------------------------------------


def parse_lab_results(html: str) -> list[dict]:
    """Parse lab results HTML from Angular UI Grid.

    The grid has a pinned left panel (marker names + units) and a scrollable
    body with date columns containing values. Due to virtual scrolling, only
    visible rows are rendered — the Playwright scraper must scroll to load all.

    Returns list of dicts with keys:
        test_date, marker_name, value_numeric, value_text, unit,
        reference_min, reference_max, reference_direction, status
    """
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    grid = soup.select_one(SEL_LAB_GRID)
    if not grid:
        logger.warning("No lab results grid found (#labsvargrid)")
        return results

    # Extract marker names and units from pinned left panel
    left_rows = grid.select(SEL_LAB_LEFT_ROWS)
    markers: list[dict] = []

    for row in left_rows:
        name_cell = row.select_one(SEL_LAB_NAME_CELL)
        unit_cell = row.select_one(SEL_LAB_UNIT_CELL)

        # Skip group headers
        group_header = row.select_one(SEL_LAB_GROUP_HEADER)
        if group_header:
            continue

        name_text = ""
        if name_cell:
            text_el = name_cell.select_one(SEL_LAB_CELL_TEXT)
            name_text = (
                text_el.get_text(strip=True)
                if text_el
                else name_cell.get_text(strip=True)
            )

        unit_text = ""
        if unit_cell:
            text_el = unit_cell.select_one(SEL_LAB_CELL_TEXT)
            unit_text = (
                text_el.get_text(strip=True)
                if text_el
                else unit_cell.get_text(strip=True)
            )

        if name_text:
            markers.append({"marker_name": name_text, "unit": unit_text or None})

    # Extract date columns from body header
    date_headers = grid.select(SEL_LAB_BODY_HEADER)
    dates: list[Optional[str]] = []
    for header in date_headers:
        header_text = header.get_text(strip=True)
        dates.append(_parse_danish_date(header_text))

    # Extract values from body rows — skip rows with no value cells
    # (group header rows exist in body too but have no cells)
    body_rows = grid.select(SEL_LAB_BODY_ROWS)
    marker_idx = 0
    for row in body_rows:
        value_cells = row.select(SEL_LAB_VALUE_CELL)
        if not value_cells:
            continue
        if marker_idx >= len(markers):
            break

        marker_info = markers[marker_idx]
        marker_idx += 1

        for col_idx, cell in enumerate(value_cells):
            text_el = cell.select_one(SEL_LAB_CELL_TEXT)
            value_text = (
                text_el.get_text(strip=True) if text_el else cell.get_text(strip=True)
            )
            if not value_text:
                continue

            value_numeric = _parse_danish_number(value_text)
            test_date = dates[col_idx] if col_idx < len(dates) else None

            results.append(
                {
                    "test_date": test_date,
                    "marker_name": marker_info["marker_name"],
                    "value_numeric": value_numeric,
                    "value_text": value_text or None,
                    "unit": marker_info["unit"],
                    "reference_min": None,
                    "reference_max": None,
                    "reference_direction": None,
                    "status": "unknown",
                }
            )

    logger.info("Parsed %d lab results", len(results))
    return results


def parse_medications(html: str) -> list[dict]:
    """Parse medications HTML from sdk-table.

    sundhed.dk medicinkortet uses a standard HTML table with class sdk-table.
    Columns: Startdato | Lægemiddel, form og styrke | Dosering | Årsag | (arrow)

    The drug column contains: "BrandName (ActiveIngredient)\\nForm, Strength"

    Returns list of dicts with keys:
        medication_name, active_ingredient, strength, dosage,
        start_date, end_date, atc_code, reason
    """
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    table = soup.select_one(SEL_MED_TABLE)
    if not table:
        logger.warning("No medication table found (table.sdk-table)")
        return results

    for row in table.select(SEL_MED_ROW):
        cells = row.select("td")
        if len(cells) < 4:
            continue

        # Column 0: Start date (DD.MM.YYYY)
        start_date_text = _cell_text(cells, 0)
        start_date = _parse_danish_date(start_date_text)

        # Column 1: Drug name — "BrandName (ActiveIngredient)\nForm, Strength"
        drug_el = cells[1].select_one(SEL_MED_CELL_VALUE)
        medication_name = ""
        active_ingredient = None
        strength = None

        if drug_el:
            parts = list(drug_el.stripped_strings)
            if parts:
                # First part: "BrandName (ActiveIngredient)"
                name_match = _MED_NAME_RE.match(parts[0])
                if name_match:
                    medication_name = name_match.group(1).strip()
                    active_ingredient = name_match.group(2).strip()
                else:
                    medication_name = parts[0].strip()

                # Second part (after <br>): "Form, Strength"
                if len(parts) >= 2:
                    strength = parts[1].strip()

        if not medication_name:
            continue

        # Column 2: Dosage
        dosage = _cell_text(cells, 2) or None

        # Column 3: Reason/indication
        reason = _cell_text(cells, 3) or None

        results.append(
            {
                "medication_name": medication_name,
                "active_ingredient": active_ingredient,
                "strength": strength,
                "dosage": dosage,
                "start_date": start_date,
                "end_date": None,  # Not in table view — only in detail modal
                "atc_code": None,  # Not in table view — only in detail modal
                "reason": reason,
            }
        )

    logger.info("Parsed %d medications", len(results))
    return results


def parse_vaccinations(html: str) -> list[dict]:
    """Parse vaccinations HTML from sdk-table.

    sundhed.dk uses table.sdk-table.effectuated-vaccinations.
    Columns: Dato | Vaccine | Givet hos | Varighed | (arrow)

    Returns list of dicts with keys:
        vaccine_name, vaccine_date, given_at, duration, batch_number
    """
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    # Try specific vaccination table first, then generic sdk-table
    table = soup.select_one("table.effectuated-vaccinations")
    if not table:
        table = soup.select_one(SEL_VAC_TABLE)
    if not table:
        logger.warning("No vaccination table found")
        return results

    for row in table.select(SEL_VAC_ROW):
        cells = row.select("td")
        if len(cells) < 4:
            continue

        # Column 0: Date (DD.MM.YYYY)
        date_text = _cell_text(cells, 0)
        vaccine_date = _parse_danish_date(date_text)

        # Column 1: Vaccine name (e.g. "ProductName mod DiseaseName")
        vaccine_name = _cell_text(cells, 1)
        if not vaccine_name:
            continue

        # Column 2: Given at (provider/clinic)
        given_at = _cell_text(cells, 2) or None

        # Column 3: Duration/validity
        duration = _cell_text(cells, 3) or None

        results.append(
            {
                "vaccine_name": vaccine_name,
                "vaccine_date": vaccine_date,
                "given_at": given_at,
                "duration": duration,
                "batch_number": None,  # Only in detail modal, not table view
            }
        )

    logger.info("Parsed %d vaccinations", len(results))
    return results


def parse_ejournal(html: str) -> list[dict]:
    """Parse e-journal HTML into structured dicts.

    NOTE: Requires HTML from /min-sundhedsjournal/journal-fra-sygehus/
    (not the dashboard page). Structure TBD — needs live HTML dump.

    Returns list of dicts with keys:
        note_date, department, hospital, note_type, note_text
    """
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    for entry in soup.select(SEL_JOURNAL_ENTRY):
        note_text = _text(entry, SEL_JOURNAL_TEXT)
        if not note_text:
            continue

        results.append(
            {
                "note_date": _parse_danish_date(_text(entry, SEL_JOURNAL_DATE)),
                "department": _text(entry, SEL_JOURNAL_DEPT) or None,
                "hospital": _text(entry, SEL_JOURNAL_HOSPITAL) or None,
                "note_type": _text(entry, SEL_JOURNAL_TYPE) or None,
                "note_text": note_text,
            }
        )

    logger.info("Parsed %d journal entries", len(results))
    return results


def parse_appointments(html: str) -> list[dict]:
    """Parse appointments HTML from ngx-datatable.

    sundhed.dk uses ngx-datatable (Angular 6) for appointments/henvisninger.
    Columns: Henvisningsdato | Udløbsdato | Henvisende klinik |
             Modtagerklinik | Specialenavn | (arrow)

    Two sections: "Aktive henvisninger" and "Tidligere henvisninger".

    Returns list of dicts with keys:
        referral_date, expiry_date, referring_clinic, receiving_clinic,
        specialty, section
    """
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    sections = soup.select(SEL_APPT_SECTION)
    if not sections:
        # Fallback: try to find ngx-datatable directly
        sections = [soup]

    for idx, section in enumerate(sections):
        # Determine section type from heading
        heading = section.select_one("h2")
        section_name = "active" if idx == 0 else "previous"
        if heading:
            heading_text = heading.get_text(strip=True).lower()
            if "aktive" in heading_text:
                section_name = "active"
            elif "tidligere" in heading_text:
                section_name = "previous"

        table = section.select_one(SEL_APPT_TABLE)
        if not table:
            continue

        for row in table.select(SEL_APPT_ROW):
            cells = row.select(SEL_APPT_CELL)
            if len(cells) < 5:
                continue

            referral_date = _parse_danish_date(cells[0].get_text(strip=True))
            expiry_date = _parse_danish_date(cells[1].get_text(strip=True))
            referring_clinic = cells[2].get_text(strip=True) or None
            receiving_clinic = cells[3].get_text(strip=True) or None
            specialty = cells[4].get_text(strip=True) or None

            results.append(
                {
                    "referral_date": referral_date,
                    "expiry_date": expiry_date,
                    "referring_clinic": referring_clinic,
                    "receiving_clinic": receiving_clinic,
                    "specialty": specialty,
                    "section": section_name,
                }
            )

    logger.info("Parsed %d appointments", len(results))
    return results
