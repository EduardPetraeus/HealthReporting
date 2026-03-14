"""PDF parsers for downloaded sundhed.dk health data.

Handles four PDF types from sundhed.dk exports:
  - FMK medications (Faelles Medicinkort)
  - SSI vaccinations (Vaccinationsregister)
  - E-journal (Forloebsoversigt + Epikriser/Notater)
  - Lab results (multi-page date-grid)

Uses pdfplumber for table extraction. All text is PII-stripped before storage.
Reuses Danish number parsing from the existing lab PDF parser.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pdfplumber
from health_platform.utils.logging_config import get_logger

logger = get_logger("sundhed_dk.pdf_parser")

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# CPR number: DDMMYY-DDDD (6 digits, dash, 4 digits)
_CPR_RE = re.compile(r"\b\d{6}-\d{4}\b")

# Danish date patterns
_DATE_DOT_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_DATE_DASH_RE = re.compile(r"(\d{1,2})-(\d{1,2})-(\d{4})")

# Danish number: "9,6" or "1.234,5"
_DANISH_NUMBER_RE = re.compile(r"^-?\d[\d.]*,\d+$")

# Lab result date header: "YYYY\nDD.MM\nHH:MM" (newline-separated in cell)
# or "DD.MM YYYY HH:MM" or "DD.MM.YYYY HH:MM"
_LAB_DATE_HEADER_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.?\s*(\d{4})\s+(\d{1,2}:\d{2})")
# Multiline variant: "YYYY\nDD.MM\nHH:MM" as seen in actual sundhed.dk PDFs
_LAB_DATE_MULTILINE_RE = re.compile(r"(\d{4})\n(\d{1,2})\.(\d{1,2})\n(\d{1,2}:\d{2})")

# Marker name with unit in parentheses: "Haemoglobin (mmol/L)"
_MARKER_UNIT_RE = re.compile(r"^(.+?)\s*\(([^)]+)\)\s*$")

# Text values that are not numeric results
_NON_NUMERIC_VALUES = frozenset(
    {
        "erstattet",
        "se note",
        "ikke påvist",
        "mikro",
        "påvist",
        "negativ",
        "positiv",
        "normal",
        "se kommentar",
        "annulleret",
    }
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _strip_pii(text: str) -> str:
    """Remove CPR numbers from text.

    Replaces DDMMYY-DDDD patterns with '[CPR-REMOVED]'.
    """
    if not text:
        return text
    return _CPR_RE.sub("[CPR-REMOVED]", text)


def _parse_danish_date_pdf(text: str) -> Optional[str]:
    """Parse a Danish date string into ISO format (YYYY-MM-DD).

    Handles:
        "03.01.2026" -> "2026-01-03"  (dot separator)
        "03-01-2026" -> "2026-01-03"  (dash separator)
        "3.1.2026"   -> "2026-01-03"  (single digit)

    Returns None if no valid date pattern found.
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Try dot-separated first: DD.MM.YYYY
    match = _DATE_DOT_RE.search(text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = match.group(3)
        if 1 <= day <= 31 and 1 <= month <= 12:
            return f"{year}-{month:02d}-{day:02d}"

    # Try dash-separated: DD-MM-YYYY
    match = _DATE_DASH_RE.search(text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = match.group(3)
        if 1 <= day <= 31 and 1 <= month <= 12:
            return f"{year}-{month:02d}-{day:02d}"

    return None


def _parse_danish_number_pdf(text: str) -> Optional[float]:
    """Parse a number that may use Danish format (comma as decimal separator).

    Examples:
        "9,6"     -> 9.6
        "1.234,5" -> 1234.5
        "12.5"    -> 12.5   (standard format)
        ""        -> None
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Danish format: dots as thousands separator, comma as decimal
    if _DANISH_NUMBER_RE.match(text):
        text = text.replace(".", "").replace(",", ".")
    elif "," in text and "." not in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def _safe_cell(row: list, index: int) -> str:
    """Extract cell text from a table row safely."""
    if index >= len(row) or row[index] is None:
        return ""
    return str(row[index]).strip()


def _is_empty_row(row: Optional[list]) -> bool:
    """Check if a table row is empty or None."""
    if row is None:
        return True
    return all(cell is None or str(cell).strip() == "" for cell in row)


# ---------------------------------------------------------------------------
# 1. Medications parser (FMK — Faelles Medicinkort)
# ---------------------------------------------------------------------------


def parse_medications_pdf(pdf_path: str) -> list[dict]:
    """Parse FMK medications PDF from sundhed.dk download.

    The sundhed.dk FMK PDF uses a text-based layout (no extractable tables).
    Columns are rendered as fixed-width text:
        Lægemiddel | Styrke | Daglig dosis | Bemærkning | Behandling for/mod | Start | Slut
        Indholdsstof
        Form

    Each medication block starts with a drug name and spans 3 lines
    (name, active ingredient, form).

    Args:
        pdf_path: Path to the FMK PDF file.

    Returns:
        List of dicts with keys: medication_name, active_ingredient,
        strength, dosage, start_date, end_date, atc_code, reason.
    """
    path = Path(pdf_path)
    if not path.exists():
        logger.error("Medications PDF not found: %s", path)
        return []

    results: list[dict] = []

    try:
        with pdfplumber.open(path) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

        full_text = _strip_pii(full_text)

        # Split into lines and find the medication data section
        lines = full_text.split("\n")

        # Find the header line to determine column boundaries
        header_idx = None
        for i, line in enumerate(lines):
            if "Lægemiddel" in line and "Styrke" in line:
                header_idx = i
                break

        if header_idx is None:
            logger.warning("No medication header found in %s", path.name)
            logger.info("Parsed 0 medication records from %s", path.name)
            return []

        # Skip header + subheader lines
        data_start = header_idx + 1
        while data_start < len(lines):
            low = lines[data_start].strip().lower()
            if low in ("indholdsstof", "form") or low.startswith("for/mod"):
                data_start += 1
            else:
                break

        # Regex to detect a medication start line: name followed by
        # strength like "2 %", "100 mg", "500 mg"
        _strength_re = re.compile(
            r"\s(\d[\d,.]*\s*(?:mg|%|g|ml|µg|IE|mcg))\s", re.IGNORECASE
        )
        _footer_re = re.compile(
            r"^(Alle datoer|Se nyeste|Side \d|Medicinkort|FMK)", re.IGNORECASE
        )

        i = data_start
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            if _footer_re.match(line):
                break

            strength_match = _strength_re.search(line)
            if not strength_match:
                i += 1
                continue

            # Found a medication start line
            medication_name = line[: strength_match.start()].strip()
            strength = strength_match.group(1).strip()
            after_strength = line[strength_match.end() :].strip()

            # Extract date if present
            start_date = None
            end_date = None
            date_match = _DATE_DOT_RE.search(after_strength)
            if date_match:
                start_date = _parse_danish_date_pdf(date_match.group(0))
                rest = after_strength[date_match.end() :].strip()
                end_match = _DATE_DOT_RE.search(rest)
                if end_match:
                    end_date = _parse_danish_date_pdf(end_match.group(0))
                after_strength = after_strength[: date_match.start()].strip()

            # Extract reason: "Mod ..." pattern
            reason = None
            reason_re = re.compile(r"(Mod\s+.+?)$", re.IGNORECASE)
            reason_match = reason_re.search(after_strength)
            if reason_match:
                reason = reason_match.group(1).strip()
                after_strength = after_strength[: reason_match.start()].strip()

            dosage = after_strength if after_strength else None

            # Collect continuation + active ingredient + form lines
            active_ingredient = None
            j = i + 1

            # Skip continuation lines (lowercase start or hyphen-continued)
            while j < len(lines):
                nxt = lines[j].strip()
                if not nxt or _footer_re.match(nxt) or _strength_re.search(nxt):
                    break
                if nxt[0].islower() or (dosage and dosage.endswith("-")):
                    if reason and reason.endswith("-"):
                        reason = reason[:-1] + nxt
                    elif dosage and dosage.endswith("-"):
                        dosage = dosage[:-1] + nxt
                    else:
                        dosage = (dosage + " " + nxt) if dosage else nxt
                    j += 1
                else:
                    break

            # Next line = active ingredient
            if j < len(lines):
                nxt = lines[j].strip()
                if nxt and not _footer_re.match(nxt) and not _strength_re.search(nxt):
                    active_ingredient = nxt
                    j += 1
                    # Line after = form — skip it
                    if j < len(lines):
                        frm = lines[j].strip()
                        if (
                            frm
                            and not _footer_re.match(frm)
                            and not _strength_re.search(frm)
                        ):
                            j += 1

            results.append(
                {
                    "medication_name": _strip_pii(medication_name),
                    "active_ingredient": (
                        _strip_pii(active_ingredient) if active_ingredient else None
                    ),
                    "strength": strength,
                    "dosage": dosage if dosage else None,
                    "start_date": start_date,
                    "end_date": end_date,
                    "atc_code": None,
                    "reason": _strip_pii(reason) if reason else None,
                }
            )

            i = j

    except Exception as exc:
        logger.error("Failed to parse medications PDF %s: %s", path.name, exc)
        return []

    logger.info("Parsed %d medication records from %s", len(results), path.name)
    return results


# ---------------------------------------------------------------------------
# 2. Vaccinations parser (SSI Vaccinationsregister)
# ---------------------------------------------------------------------------


def parse_vaccinations_pdf(pdf_path: str) -> list[dict]:
    """Parse SSI Vaccinationsregister PDF from sundhed.dk download.

    The PDF contains up to 3 sections:
      - Bornevaccinationer (childhood — may say "Ingen vaccinationer")
      - Andre vaccinationer (main table)
      - Vaccinationer der kraever yderligere dokumentation

    pdfplumber collapses all table columns into a single cell per row.
    Each data row looks like:
        "DD-MM-YYYY NN år/yrs VaccineName DiseaseTarget GivenAt"
    so we parse the text content rather than relying on column structure.

    Args:
        pdf_path: Path to the vaccinations PDF file.

    Returns:
        List of dicts with keys: vaccine_name, vaccine_date, given_at,
        duration, batch_number, disease_target.
    """
    path = Path(pdf_path)
    if not path.exists():
        logger.error("Vaccinations PDF not found: %s", path)
        return []

    results: list[dict] = []

    try:
        with pdfplumber.open(path) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

        full_text = _strip_pii(full_text)

        # Parse vaccination entries from full text.
        # Each entry starts with a date pattern: DD-MM-YYYY
        # followed by age, vaccine name, disease target, and given_at.
        # Format: "DD-MM-YYYY NN år/yrs VaccineName\nDiseaseTarget GivenAt"
        #
        # The text is continuous, so we split on date patterns.
        lines = full_text.split("\n")

        # Regex for vaccination entry start: date + age + vaccine name
        _vac_entry_re = re.compile(r"(\d{2}-\d{2}-\d{4})\s+(\d+)\s+år/yrs\s+(.+)")

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            match = _vac_entry_re.match(line)
            if match:
                date_str = match.group(1)
                vaccine_date = _parse_danish_date_pdf(date_str)

                # Rest of line after age contains vaccine name + possibly more
                rest = match.group(3).strip()

                # The rest may continue on the next line(s) before the next entry
                # Collect continuation lines
                continuation = [rest]
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    # Stop at next vaccination entry, section header, or empty
                    if not next_line:
                        break
                    if _vac_entry_re.match(next_line):
                        break
                    if next_line.startswith("Dato") or next_line.startswith("Date"):
                        break
                    if (
                        "vaccinationer" in next_line.lower()
                        and "ingen" not in next_line.lower()
                    ):
                        break
                    if next_line.startswith("Vaccinationskort"):
                        break
                    if next_line.startswith("Borgeroprettede"):
                        break
                    continuation.append(next_line)
                    j += 1

                # Join all continuation text
                full_entry = " ".join(continuation)

                # Parse the entry: VaccineName DiseaseTarget GivenAtPerson, GivenAtPlace
                # The disease target and given_at are typically separated by the
                # provider name containing a comma
                vaccine_name, disease_target, given_at = _parse_vac_entry_text(
                    full_entry
                )

                if vaccine_name:
                    results.append(
                        {
                            "vaccine_name": _strip_pii(vaccine_name),
                            "vaccine_date": vaccine_date,
                            "given_at": _strip_pii(given_at) if given_at else None,
                            "duration": None,
                            "batch_number": None,
                            "disease_target": (
                                _strip_pii(disease_target) if disease_target else None
                            ),
                        }
                    )

                i = j
            else:
                i += 1

    except Exception as exc:
        logger.error("Failed to parse vaccinations PDF %s: %s", path.name, exc)
        return []

    logger.info("Parsed %d vaccination records from %s", len(results), path.name)
    return results


def _parse_vac_entry_text(
    text: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse a vaccination entry text into (vaccine_name, disease_target, given_at).

    The text follows the pattern:
        "VaccineName DiseaseTarget ProviderName, ProviderOrg"
    or with English translation on next line:
        "VaccineName DiseaseTarget ProviderName,\\nEnglishDisease ProviderOrg"

    The disease target line often contains the English translation which
    we can use as a separator.
    """
    if not text:
        return None, None, None

    # Known patterns: the text has vaccine name first, then disease,
    # then provider info (often containing a comma + organization name).
    #
    # Strategy: find the provider part (typically ends with "Service" or
    # contains "Lægers" or a proper name with comma) and work backwards.

    # Try to find provider: look for common provider suffixes
    provider_re = re.compile(
        r"([\w\s-]+(?:Lægers Vaccinations\s*Service|Vaccinations\s*Service|ApotekVaccination))",
        re.IGNORECASE,
    )
    provider_match = provider_re.search(text)

    given_at = None
    remaining = text

    if provider_match:
        given_at = provider_match.group(0).strip()
        # Remove the provider name and the person's name before it
        # The person's name is right before the org name, separated by comma
        provider_start = provider_match.start()
        remaining = text[:provider_start].strip()

        # Remove trailing comma and person name
        # Pattern: "..., PersonName, OrgName" — we want to remove ", PersonName"
        if remaining.endswith(","):
            remaining = remaining[:-1].strip()
        # Check if there's a person name (capitalized words) at the end
        person_re = re.compile(r",\s*([A-ZÆØÅ][\w\s-]+)$")
        person_match = person_re.search(remaining)
        if person_match:
            given_at = person_match.group(1).strip() + ", " + given_at
            remaining = remaining[: person_match.start()].strip()

    # Now remaining should be "VaccineName DiseaseTarget"
    # or "VaccineName DiseaseTarget\nEnglishDisease"
    # Split: vaccine name is typically one word, disease follows
    # Common vaccine names: Influvac, Abrysvo, diTeBooster, Stamaril, etc.

    # Split on first occurrence of disease-related words
    disease_keywords_list = [
        "influenza",
        "rsv",
        "respiratory",
        "stivkrampe",
        "tetanus",
        "difteri",
        "diphteria",
        "gul feber",
        "yellow fever",
        "hepatitis",
        "covid",
        "pneumokok",
        "meningokok",
        "mæslinger",
        "measles",
        "kighoste",
        "pertussis",
    ]

    vaccine_name = remaining
    disease_target = None

    remaining_lower = remaining.lower()
    best_pos = len(remaining)
    for kw in disease_keywords_list:
        pos = remaining_lower.find(kw)
        if pos != -1 and pos < best_pos:
            best_pos = pos

    if best_pos < len(remaining) and best_pos > 0:
        vaccine_name = remaining[:best_pos].strip()
        disease_target = remaining[best_pos:].strip()
    else:
        # Fallback: first word is vaccine name
        parts = remaining.split(None, 1)
        if len(parts) == 2:
            vaccine_name = parts[0]
            disease_target = parts[1]
        else:
            vaccine_name = remaining

    return vaccine_name or None, disease_target or None, given_at or None


# ---------------------------------------------------------------------------
# 3. E-journal parser (Forloebsoversigt + Epikriser/Notater)
# ---------------------------------------------------------------------------

# Header signatures for auto-detection
_EJOURNAL_OVERVIEW_KEYWORDS = {"forløbsoversigt", "forloebsoversigt"}
_EJOURNAL_NOTE_KEYWORDS = {"epikriser til forløb", "notater til forløb"}


def parse_ejournal_pdf(pdf_path: str) -> list[dict]:
    """Parse e-journal PDF from sundhed.dk download.

    Auto-detects two PDF types:
      - Type A: Forloebsoversigt (overview table with multiple encounters)
      - Type B: Epikriser/Notater (detailed clinical notes for one encounter)

    Args:
        pdf_path: Path to the e-journal PDF file.

    Returns:
        List of dicts with keys: note_date, department, hospital,
        note_type, note_text.
    """
    path = Path(pdf_path)
    if not path.exists():
        logger.error("E-journal PDF not found: %s", path)
        return []

    try:
        with pdfplumber.open(path) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            full_text_lower = full_text.lower()

            # Auto-detect type — always fall back to notes if overview
            # returns nothing (some "Forloebsoversigt" PDFs have data in
            # text rather than extractable tables).
            if any(kw in full_text_lower for kw in _EJOURNAL_OVERVIEW_KEYWORDS):
                results = _parse_ejournal_overview(pdf)
                if not results:
                    results = _parse_ejournal_notes(pdf, full_text)
            elif any(kw in full_text_lower for kw in _EJOURNAL_NOTE_KEYWORDS):
                results = _parse_ejournal_notes(pdf, full_text)
            else:
                results = _parse_ejournal_overview(pdf)
                if not results:
                    results = _parse_ejournal_notes(pdf, full_text)

    except Exception as exc:
        logger.error("Failed to parse e-journal PDF %s: %s", path.name, exc)
        return []

    logger.info("Parsed %d journal entries from %s", len(results), path.name)
    return results


def _parse_ejournal_overview(pdf: pdfplumber.PDF) -> list[dict]:
    """Parse Type A: Forloebsoversigt table.

    pdfplumber often cannot extract the overview table as a structured
    table (it renders as text).  Strategy:
      1. Try pdfplumber table extraction first.
      2. If no data rows found, fall back to text-based parsing where
         each data row starts with a DD.MM.YYYY date pair.

    Expected text layout per row:
        DD.MM.YYYY DD.MM.YYYY [DD.MM.YYYY]  Hospital  Department  [Diagnosis]  [Epikrise marker]  [Notat marker]
    """
    results: list[dict] = []

    # --- Attempt 1: pdfplumber structured tables ---
    for page in pdf.pages:
        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            if not table or len(table) < 2:
                continue

            header = table[0]
            if header is None:
                continue

            header_text = " ".join(str(h).lower() for h in header if h)
            is_overview_header = (
                "senest" in header_text
                or "opdateret" in header_text
                or "behandlingssted" in header_text
            )
            if not is_overview_header:
                continue

            col_map = _map_ejournal_overview_columns(header)

            for row in table[1:]:
                if _is_empty_row(row):
                    continue

                note_date_text = _safe_cell(row, col_map.get("updated", 0))
                hospital = _safe_cell(row, col_map.get("hospital", 3)) or None
                department = _safe_cell(row, col_map.get("department", 4)) or None
                note_type = _safe_cell(row, col_map.get("diagnosis", 5)) or None

                epikrise = _safe_cell(row, col_map.get("epikrise", 6))
                notat = _safe_cell(row, col_map.get("notat", 7))
                note_parts = [p for p in [epikrise, notat] if p]
                note_text = " ".join(note_parts) if note_parts else None

                note_date = _parse_danish_date_pdf(note_date_text)

                results.append(
                    {
                        "note_date": note_date,
                        "department": _strip_pii(department) if department else None,
                        "hospital": _strip_pii(hospital) if hospital else None,
                        "note_type": _strip_pii(note_type) if note_type else None,
                        "note_text": _strip_pii(note_text) if note_text else None,
                    }
                )

    if results:
        return results

    # --- Attempt 2: text-based parsing ---
    # Each data line starts with two DD.MM.YYYY dates (updated + start).
    # An optional third date may follow (end date).
    # Then hospital name, department/lokation, optional diagnosis.
    full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    full_text = _strip_pii(full_text)
    lines = full_text.split("\n")

    # Regex: two mandatory dates at start, optional third date, rest is text
    _overview_row_re = re.compile(
        r"^(\d{1,2}\.\d{1,2}\.\d{4})\s+"  # date 1: senest opdateret
        r"(\d{1,2}\.\d{1,2}\.\d{4})\s+"  # date 2: forloeb startdato
        r"(?:(\d{1,2}\.\d{1,2}\.\d{4})\s+)?"  # date 3: afsluttet (optional)
        r"(.+)"  # rest: hospital + department + diagnosis
    )

    # Lines to skip
    _overview_skip_re = re.compile(
        r"^(Forløbsoversigt|Senest|opdateret|Udskrift|Hvis udskriften|"
        r"https?://|Side \d|Cpr-nummer|Udskriftsdato|Printet af|"
        r"Notater for|Claus)",
        re.IGNORECASE,
    )

    for line in lines:
        line = line.strip()
        if not line or _overview_skip_re.match(line):
            continue

        match = _overview_row_re.match(line)
        if not match:
            continue

        note_date = _parse_danish_date_pdf(match.group(1))
        rest = match.group(4).strip()

        # The rest contains: Hospital  Department  [Diagnosis]  [markers]
        # Markers like "(1)", "(3)", "-" at the end indicate epikrise/notat counts
        # Remove trailing markers
        rest = re.sub(r"\s*[-–]\s*(?:\(\d+\))?\s*$", "", rest)
        rest = re.sub(r"\s*\(\d+\)\s*$", "", rest)
        rest = re.sub(r"\s*[-–]\s*$", "", rest)
        rest = rest.strip()

        # Split on 2+ spaces to separate hospital from department/diagnosis
        parts = re.split(r"\s{2,}", rest)

        hospital = parts[0] if len(parts) >= 1 else None
        department = parts[1] if len(parts) >= 2 else None
        note_type = parts[2] if len(parts) >= 3 else None

        # Sometimes diagnosis wraps across what looks like 2 columns
        # If we have 4+ parts, join the extras as diagnosis
        if len(parts) >= 4:
            note_type = " ".join(parts[2:])

        results.append(
            {
                "note_date": note_date,
                "department": _strip_pii(department) if department else None,
                "hospital": _strip_pii(hospital) if hospital else None,
                "note_type": _strip_pii(note_type) if note_type else None,
                "note_text": None,
            }
        )

    return results


def _map_ejournal_overview_columns(header: list) -> dict[str, int]:
    """Map overview table header to column indices."""
    col_map: dict[str, int] = {}
    header_lower = [str(h).strip().lower() if h else "" for h in header]

    for i, col_text in enumerate(header_lower):
        if "senest" in col_text or "opdateret" in col_text:
            col_map["updated"] = i
        elif "startdato" in col_text:
            col_map["start_date"] = i
        elif "afsluttet" in col_text:
            col_map["end_date"] = i
        elif "behandlingssted" in col_text:
            col_map["hospital"] = i
        elif "lokation" in col_text:
            col_map["department"] = i
        elif "diagnose" in col_text:
            col_map["diagnosis"] = i
        elif "epikrise" in col_text:
            col_map["epikrise"] = i
        elif "notat" in col_text:
            col_map["notat"] = i

    return col_map


def _parse_ejournal_notes(pdf: pdfplumber.PDF, full_text: str) -> list[dict]:
    """Parse Type B: Epikriser/Notater (detailed clinical notes).

    These PDFs contain metadata (hospital, department, dates) followed
    by free-text clinical notes. Each note is preceded by a
    "Dato  Titel" header line followed by a date + title line like:
        "11.06.2020 Amb.notat til praktiserende læge"
    The note body follows until the next structural marker.

    Structure per note block:
        Dato  Titel
        DD.MM.YYYY <title>
        <note body lines...>
        Behandlers navn: ...
        Behandlingssted: ...
        Lokation: ...
    """
    results: list[dict] = []
    full_text_lower = full_text.lower()

    # Determine note type from header
    if "epikriser" in full_text_lower:
        note_type = "Epikrise"
    elif "notater" in full_text_lower:
        note_type = "Notat"
    else:
        note_type = "Unknown"

    # Extract metadata from the "oprettet af" line on page 1
    hospital = None
    department = None

    oprettet_re = re.compile(r"(?:oprettet\s+af|fra)\s+(.+?)(?:\n|$)", re.IGNORECASE)
    oprettet_match = oprettet_re.search(full_text)
    if oprettet_match:
        hospital = oprettet_match.group(1).strip()

    afd_re = re.compile(r"(?:afdeling|lokation)[:\s]+(.+?)(?:\n|$)", re.IGNORECASE)
    afd_match = afd_re.search(full_text)
    if afd_match:
        department = afd_match.group(1).strip()

    # ---------------------------------------------------------------------------
    # Strategy: find "Dato  Titel" section markers, then parse the note block
    # that follows each marker.  Each block looks like:
    #   Dato  Titel
    #   DD.MM.YYYY  <title>
    #   <body lines>
    #   ...
    #   Behandlers navn: <name>
    #   Behandlingssted: <hospital>
    #   Lokation: <dept>
    # The block ends at: next "Dato" header, page footer, or end-of-text.
    # ---------------------------------------------------------------------------

    lines = full_text.split("\n")

    # Lines that signal note-block boundaries (stop collecting body text)
    _boundary_re = re.compile(
        r"^(Dato\s+Titel|Enhedskode:|Behandlers?\s+navn:|Behandlingssted:|Lokation:|"
        r"Hvis udskriften|https?://|Side \d|Notater for:|Cpr-nummer:|Udskriftsdato:|"
        r"Printet af:)",
        re.IGNORECASE,
    )
    # Date + title pattern at the start of a note
    _note_start_re = re.compile(
        r"^(\d{1,2}\.\d{1,2}\.\d{4})\s+(.+)",
    )

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Look for "Dato  Titel" section header
        if re.match(r"^Dato\s+Titel", line, re.IGNORECASE):
            i += 1
            if i >= len(lines):
                break

            # Next line should be "DD.MM.YYYY <title>"
            note_line = lines[i].strip()
            note_start_match = _note_start_re.match(note_line)
            if not note_start_match:
                continue

            note_date_str = note_start_match.group(1)
            note_date = _parse_danish_date_pdf(note_date_str)
            i += 1

            # Collect body lines until next boundary
            body_lines: list[str] = []
            while i < len(lines):
                bline = lines[i].strip()
                if _boundary_re.match(bline):
                    break
                body_lines.append(bline)
                i += 1

            body_text = "\n".join(body_lines).strip()
            if body_text:
                # Extract per-note hospital/dept from trailing metadata
                note_hospital = hospital
                note_department = department
                for bl in body_lines:
                    bl_stripped = bl.strip()
                    if bl_stripped.startswith("Behandlingssted:"):
                        note_hospital = bl_stripped.split(":", 1)[1].strip()
                    elif bl_stripped.startswith("Lokation:"):
                        note_department = bl_stripped.split(":", 1)[1].strip()

                results.append(
                    {
                        "note_date": note_date,
                        "department": (
                            _strip_pii(note_department) if note_department else None
                        ),
                        "hospital": (
                            _strip_pii(note_hospital) if note_hospital else None
                        ),
                        "note_type": note_type,
                        "note_text": _strip_pii(body_text),
                    }
                )
        else:
            i += 1

    # Fallback: if structured parsing found nothing, treat full text as one note
    if not results:
        any_date_match = _DATE_DOT_RE.search(full_text) or _DATE_DASH_RE.search(
            full_text
        )
        note_date = None
        if any_date_match:
            note_date = _parse_danish_date_pdf(any_date_match.group(0))

        body_lines_fb = lines[5:] if len(lines) > 5 else lines
        body_text_fb = "\n".join(body_lines_fb).strip()

        if body_text_fb:
            results.append(
                {
                    "note_date": note_date,
                    "department": _strip_pii(department) if department else None,
                    "hospital": _strip_pii(hospital) if hospital else None,
                    "note_type": note_type,
                    "note_text": _strip_pii(body_text_fb),
                }
            )

    return results


# ---------------------------------------------------------------------------
# 4. Lab results parser (multi-page date-grid)
# ---------------------------------------------------------------------------


def parse_lab_results_pdf(pdf_path: str) -> list[dict]:
    """Parse lab results PDF from sundhed.dk download.

    This is the most complex parser. The PDF is a multi-page grid where:
      - The header row contains test dates across columns (DD.MM YYYY HH:MM)
      - The left column contains marker names with units, grouped by category
      - Values use Danish decimal format (comma = decimal point)
      - Some cells contain text values like "Erstattet", "Se note", etc.

    Args:
        pdf_path: Path to the lab results PDF file.

    Returns:
        List of dicts with keys: test_date, marker_name, value_numeric,
        value_text, unit, reference_min, reference_max,
        reference_direction, status.
    """
    path = Path(pdf_path)
    if not path.exists():
        logger.error("Lab results PDF not found: %s", path)
        return []

    results: list[dict] = []

    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                page_results = _parse_lab_page(page)
                results.extend(page_results)

    except Exception as exc:
        logger.error("Failed to parse lab results PDF %s: %s", path.name, exc)
        return []

    logger.info("Parsed %d lab result records from %s", len(results), path.name)
    return results


def _parse_lab_page(page: pdfplumber.Page) -> list[dict]:
    """Parse a single page of the lab results grid."""
    results: list[dict] = []

    tables = page.extract_tables()
    if not tables:
        return results

    for table in tables:
        if not table or len(table) < 2:
            continue

        # First row should contain date headers
        header = table[0]
        if header is None:
            continue

        # Parse date columns from header
        dates = _extract_lab_dates(header)
        if not dates:
            # Not a lab result table — skip
            continue

        # Find which column index corresponds to the first date column
        # Typically column 0 is the marker name, and dates start at column 1
        first_date_col = _find_first_date_column(header)

        for row in table[1:]:
            if _is_empty_row(row):
                continue

            # Extract marker name and unit from first column(s)
            marker_name, unit = _extract_marker_info(row, first_date_col)
            if not marker_name:
                continue

            # Skip category header rows (e.g., "Haematologi", "Metabolisme")
            if _is_category_header(marker_name):
                continue

            # Extract values from date columns
            for col_offset, date_str in enumerate(dates):
                col_idx = first_date_col + col_offset
                if col_idx >= len(row):
                    break

                cell_value = _safe_cell(row, col_idx)
                if not cell_value:
                    continue

                value_numeric = None
                value_text = cell_value
                status = None

                # Check if value is a known text result
                if cell_value.lower() in _NON_NUMERIC_VALUES:
                    value_text = cell_value
                else:
                    value_numeric = _parse_danish_number_pdf(cell_value)
                    if value_numeric is None and cell_value:
                        # Not a number and not a known text — store as text
                        value_text = cell_value
                    elif value_numeric is not None:
                        value_text = cell_value

                results.append(
                    {
                        "test_date": date_str,
                        "marker_name": _strip_pii(marker_name),
                        "value_numeric": value_numeric,
                        "value_text": value_text if value_text else None,
                        "unit": unit,
                        "reference_min": None,
                        "reference_max": None,
                        "reference_direction": None,
                        "status": status,
                    }
                )

    return results


def _extract_lab_dates(header: list) -> list[str]:
    """Extract test dates from the header row.

    Date formats in header:
      - "YYYY\\nDD.MM\\nHH:MM" (multiline, actual sundhed.dk format)
      - "DD.MM YYYY HH:MM" or "DD.MM.YYYY HH:MM" (single line)
    Returns list of ISO date strings (YYYY-MM-DD).
    """
    dates: list[str] = []

    for cell in header:
        if cell is None:
            continue
        cell_text = str(cell).strip()

        # Try multiline format first: "2024\n19.04\n08:26"
        match = _LAB_DATE_MULTILINE_RE.search(cell_text)
        if match:
            year = match.group(1)
            day = int(match.group(2))
            month = int(match.group(3))
            if 1 <= day <= 31 and 1 <= month <= 12:
                dates.append(f"{year}-{month:02d}-{day:02d}")
            continue

        # Fallback: single-line format "DD.MM YYYY HH:MM"
        match = _LAB_DATE_HEADER_RE.search(cell_text)
        if match:
            day = int(match.group(1))
            month = int(match.group(2))
            year = match.group(3)
            if 1 <= day <= 31 and 1 <= month <= 12:
                dates.append(f"{year}-{month:02d}-{day:02d}")

    return dates


def _find_first_date_column(header: list) -> int:
    """Find the index of the first date column in the header."""
    for i, cell in enumerate(header):
        if cell is None:
            continue
        cell_text = str(cell).strip()
        if _LAB_DATE_MULTILINE_RE.search(cell_text):
            return i
        if _LAB_DATE_HEADER_RE.search(cell_text):
            return i

    # Default: assume dates start at column 2 (col 0=name, col 1=unit)
    return 2


def _extract_marker_info(
    row: list, first_date_col: int
) -> tuple[Optional[str], Optional[str]]:
    """Extract marker name and unit from the row's label columns.

    The marker info is in columns before the first date column.
    Common patterns:
      - Single column: "Haemoglobin (mmol/L)"
      - Two columns: col 0 = name, col 1 = unit
    """
    if not row or first_date_col < 1:
        return None, None

    marker_name = None
    unit = None

    if first_date_col == 1:
        # Single label column — may contain "Name (Unit)"
        label = _safe_cell(row, 0)
        if not label:
            return None, None

        unit_match = _MARKER_UNIT_RE.match(label)
        if unit_match:
            marker_name = unit_match.group(1).strip()
            unit = unit_match.group(2).strip()
        else:
            marker_name = label

    elif first_date_col >= 2:
        # Multiple label columns — try name in col 0, unit in col 1
        marker_name = _safe_cell(row, 0) or None
        unit = _safe_cell(row, 1) or None

        # If col 0 is empty, try col 1 as name
        if not marker_name and first_date_col > 1:
            marker_name = _safe_cell(row, 1) or None
            unit = _safe_cell(row, 2) if first_date_col > 2 else None

    return marker_name, unit


def _is_category_header(text: str) -> bool:
    """Check if a marker name is actually a category header.

    Category headers are section labels like "Haematologi", "Metabolisme",
    "Klinisk biokemi og immunologi", etc. They have no associated values.
    """
    categories = {
        "patologi",
        "mikrobiologi",
        "klinisk biokemi",
        "klinisk biokemi og immunologi",
        "hæmatologi",
        "haematologi",
        "væske- og elektrolytbalance",
        "vaeske- og elektrolytbalance",
        "organmarkører",
        "organmarkoerer",
        "metabolisme",
        "endokrinologi",
        "immunologi og inflammation",
        "immunologi",
        "farmakologi",
        "sporstoffer og vitaminer",
        "molekylær genetik",
        "molekylaer genetik",
        "urin og fæces",
        "urin og faeces",
        "koagulation",
        "blodtype og forlig",
        "infektionsserologi",
        "allergi",
        "autoimmunitet",
        "tumormarkører",
        "tumormarkoerer",
    }
    return text.strip().lower() in categories


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

# Subdirectory names mapped to their parser functions
_SECTION_PARSERS = {
    "medications": parse_medications_pdf,
    "vaccinations": parse_vaccinations_pdf,
    "ejournal": parse_ejournal_pdf,
    "lab_results": parse_lab_results_pdf,
}


def parse_all_sundhed_dk_pdfs(
    downloads_dir: str,
) -> dict[str, list[dict]]:
    """Parse all sundhed.dk PDFs from the downloads directory.

    Expects subdirectories: lab_results/, medications/, vaccinations/, ejournal/
    Each subdirectory is scanned for .pdf files and parsed with the
    appropriate parser.

    Deduplicates records within each section using business keys.

    Args:
        downloads_dir: Path to the root directory containing PDF subdirectories.

    Returns:
        Dict mapping section name to list of parsed records.
        Sections: 'medications', 'vaccinations', 'ejournal', 'lab_results'.
    """
    base = Path(downloads_dir)
    if not base.exists():
        logger.error("Downloads directory not found: %s", base)
        return {}

    combined: dict[str, list[dict]] = {}

    for section, parser_fn in _SECTION_PARSERS.items():
        section_dir = base / section
        if not section_dir.is_dir():
            logger.info("No '%s' subdirectory found — skipping", section)
            continue

        all_records: list[dict] = []
        pdf_files = sorted(section_dir.glob("*.pdf"))

        if not pdf_files:
            logger.info("No PDF files in %s — skipping", section_dir)
            continue

        for pdf_file in pdf_files:
            logger.info("Parsing %s/%s", section, pdf_file.name)
            records = parser_fn(str(pdf_file))
            all_records.extend(records)

        # Deduplicate
        deduped = _deduplicate_records(all_records, section)
        combined[section] = deduped

        logger.info(
            "Section '%s': %d records (%d before dedup)",
            section,
            len(deduped),
            len(all_records),
        )

    return combined


def _deduplicate_records(records: list[dict], section: str) -> list[dict]:
    """Deduplicate records based on business keys per section.

    Business keys:
      - medications: (medication_name, start_date, dosage)
      - vaccinations: (vaccine_name, vaccine_date)
      - ejournal: (note_date, hospital, note_type)
      - lab_results: (test_date, marker_name, value_text)
    """
    if not records:
        return records

    key_fields: dict[str, tuple[str, ...]] = {
        "medications": ("medication_name", "start_date", "dosage"),
        "vaccinations": ("vaccine_name", "vaccine_date"),
        "ejournal": ("note_date", "hospital", "note_type"),
        "lab_results": ("test_date", "marker_name", "value_text"),
    }

    fields = key_fields.get(section)
    if fields is None:
        return records

    seen: set[tuple] = set()
    unique: list[dict] = []

    for record in records:
        key = tuple(record.get(f) for f in fields)
        if key not in seen:
            seen.add(key)
            unique.append(record)

    return unique
