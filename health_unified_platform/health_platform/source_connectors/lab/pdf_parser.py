"""PDF parser for blood test lab results.

Supports two formats:
  - GetTested (gettested.dk / gettested.se): tabular blood panel PDFs
  - sundhed.dk: Danish national health portal lab result PDFs

Extracts per-marker rows with value, unit, reference range, and status.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pdfplumber

from health_platform.utils.logging_config import get_logger

logger = get_logger("lab_pdf_parser")


# Regex for Danish number format: "4,5" or "1.234,5"
_DANISH_NUMBER_RE = re.compile(r"^-?\d[\d.]*,\d+$")

# Regex for reference range patterns: "3.5-5.0", "3,5 - 5,0", "< 50", "> 200"
_RANGE_DASH_RE = re.compile(r"(?P<min>[\d.,]+)\s*[-–—]\s*(?P<max>[\d.,]+)")
_RANGE_LT_RE = re.compile(r"<\s*(?P<max>[\d.,]+)")
_RANGE_GT_RE = re.compile(r">\s*(?P<min>[\d.,]+)")


def _parse_danish_number(text: str) -> Optional[float]:
    """Parse a number that may use Danish format (comma as decimal separator).

    Examples:
        "4,5"     -> 4.5
        "1.234,5" -> 1234.5
        "12.5"    -> 12.5  (standard format)
        ""        -> None
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Danish format: dots as thousands separator, comma as decimal
    if _DANISH_NUMBER_RE.match(text):
        text = text.replace(".", "").replace(",", ".")

    # Also handle comma-only (no thousands dots)
    elif "," in text and "." not in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def _parse_reference_range(
    range_text: str,
) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """Parse a reference range string into (min, max, direction).

    Returns:
        (reference_min, reference_max, reference_direction)
        direction is None for bounded ranges, 'below' for < X, 'above' for > X.

    Examples:
        "3.5-5.0"    -> (3.5, 5.0, None)
        "3,5 - 5,0"  -> (3.5, 5.0, None)
        "< 50"       -> (None, 50.0, "below")
        "> 200"      -> (200.0, None, "above")
    """
    if not range_text or not range_text.strip():
        return None, None, None

    text = range_text.strip()

    # Try "min - max" pattern
    match = _RANGE_DASH_RE.search(text)
    if match:
        ref_min = _parse_danish_number(match.group("min"))
        ref_max = _parse_danish_number(match.group("max"))
        return ref_min, ref_max, None

    # Try "< max" pattern
    match = _RANGE_LT_RE.search(text)
    if match:
        ref_max = _parse_danish_number(match.group("max"))
        return None, ref_max, "below"

    # Try "> min" pattern
    match = _RANGE_GT_RE.search(text)
    if match:
        ref_min = _parse_danish_number(match.group("min"))
        return ref_min, None, "above"

    return None, None, None


def _determine_status(
    value: Optional[float],
    ref_min: Optional[float],
    ref_max: Optional[float],
    ref_direction: Optional[str],
) -> str:
    """Determine status based on value and reference range.

    Returns one of: 'normal', 'low', 'high', 'unknown'.
    """
    if value is None:
        return "unknown"

    if ref_min is not None and ref_max is not None:
        if value < ref_min:
            return "low"
        elif value > ref_max:
            return "high"
        return "normal"

    if ref_direction == "below" and ref_max is not None:
        return "normal" if value <= ref_max else "high"

    if ref_direction == "above" and ref_min is not None:
        return "normal" if value >= ref_min else "low"

    if ref_min is not None:
        return "low" if value < ref_min else "normal"

    if ref_max is not None:
        return "high" if value > ref_max else "normal"

    return "unknown"


class LabPdfParser:
    """Parses blood test PDF files from supported lab formats.

    Supported formats:
      - GetTested: tabular PDFs with marker | value | unit | reference | status
      - sundhed.dk: Danish health portal lab PDFs

    Usage:
        parser = LabPdfParser()
        markers = parser.parse_pdf(Path("my_blood_test.pdf"))
        fmt = parser.last_detected_format  # format from most recent parse_pdf call
    """

    # Known format detection keywords
    # sundhed.dk checked first — more specific than GetTested generic terms
    _SUNDHED_DK_KEYWORDS = ["sundhed.dk", "sundhed dk", "min sundhed", "e-journal"]
    _GETTESTED_KEYWORDS = ["gettested", "bodypanel", "analysesvar"]

    def __init__(self) -> None:
        self.last_detected_format: str = "unknown"

    def detect_format(self, text: str) -> str:
        """Detect the lab PDF format from extracted text.

        Checks sundhed.dk first (more specific), then GetTested.

        Args:
            text: Full text extracted from the PDF.

        Returns:
            'gettested', 'sundhed_dk', or 'unknown'.
        """
        lower = text.lower()

        for keyword in self._SUNDHED_DK_KEYWORDS:
            if keyword in lower:
                return "sundhed_dk"

        for keyword in self._GETTESTED_KEYWORDS:
            if keyword in lower:
                return "gettested"

        return "unknown"

    def parse_pdf(self, path: Path) -> list[dict]:
        """Parse a lab result PDF and extract per-marker data.

        Args:
            path: Path to the PDF file.

        Returns:
            List of dicts with keys: marker_name, value_numeric, value_text,
            unit, reference_min, reference_max, reference_direction, status.
        """
        if not path.exists():
            logger.error("PDF file not found: %s", path)
            return []

        try:
            with pdfplumber.open(path) as pdf:
                full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                tables = []
                for page in pdf.pages:
                    page_tables = page.extract_tables()
                    if page_tables:
                        tables.extend(page_tables)
        except Exception as exc:
            logger.error("Failed to read PDF %s: %s", path, exc)
            return []

        fmt = self.detect_format(full_text)
        self.last_detected_format = fmt
        logger.info("Detected format '%s' for %s", fmt, path.name)

        if fmt == "gettested":
            return self._parse_gettested(tables, full_text)
        elif fmt == "sundhed_dk":
            return self._parse_sundhed_dk(tables, full_text)
        else:
            # Attempt generic table parsing
            logger.warning(
                "Unknown format for %s — attempting generic table parse", path.name
            )
            return self._parse_generic_tables(tables)

    def _parse_gettested(self, tables: list[list], full_text: str) -> list[dict]:
        """Parse GetTested-style PDF tables.

        Expected columns (flexible order):
          Analys/Analyse | Resultat/Result | Enhed/Unit | Reference | Status
        """
        markers: list[dict] = []

        for table in tables:
            if not table or len(table) < 2:
                continue

            header = table[0]
            if header is None:
                continue

            col_map = self._map_columns_gettested(header)
            if col_map.get("marker") is None:
                continue

            for row in table[1:]:
                if row is None or all(
                    cell is None or str(cell).strip() == "" for cell in row
                ):
                    continue

                marker = self._extract_marker_from_row(row, col_map)
                if marker and marker.get("marker_name"):
                    markers.append(marker)

        return markers

    def _parse_sundhed_dk(self, tables: list[list], full_text: str) -> list[dict]:
        """Parse sundhed.dk-style PDF tables.

        Similar structure but with Danish column names:
          Analyse | Svar | Enhed | Referenceinterval
        """
        markers: list[dict] = []

        for table in tables:
            if not table or len(table) < 2:
                continue

            header = table[0]
            if header is None:
                continue

            col_map = self._map_columns_sundhed_dk(header)
            if col_map.get("marker") is None:
                continue

            for row in table[1:]:
                if row is None or all(
                    cell is None or str(cell).strip() == "" for cell in row
                ):
                    continue

                marker = self._extract_marker_from_row(row, col_map)
                if marker and marker.get("marker_name"):
                    markers.append(marker)

        return markers

    def _parse_generic_tables(self, tables: list[list]) -> list[dict]:
        """Best-effort parsing of unrecognized table formats.

        Looks for tables with at least 3 columns where the first column
        contains text (marker names) and another column contains numbers.
        """
        markers: list[dict] = []

        for table in tables:
            if not table or len(table) < 2:
                continue

            header = table[0]
            if header is None or len(header) < 3:
                continue

            # Try to guess column mapping
            col_map = self._guess_column_mapping(header)
            if col_map.get("marker") is None:
                continue

            for row in table[1:]:
                if row is None or all(
                    cell is None or str(cell).strip() == "" for cell in row
                ):
                    continue

                marker = self._extract_marker_from_row(row, col_map)
                if marker and marker.get("marker_name"):
                    markers.append(marker)

        return markers

    @staticmethod
    def _map_columns_gettested(header: list) -> dict[str, Optional[int]]:
        """Map GetTested column headers to indices."""
        col_map: dict[str, Optional[int]] = {
            "marker": None,
            "value": None,
            "unit": None,
            "reference": None,
            "status": None,
        }

        marker_names = {"analys", "analyse", "test", "marker", "biomarker", "name"}
        value_names = {"resultat", "result", "value", "svar", "værdi"}
        unit_names = {"enhed", "unit", "enhet"}
        ref_names = {
            "reference",
            "referens",
            "ref",
            "referenceinterval",
            "interval",
            "normalområde",
        }
        status_names = {"status", "flag", "bedømmelse", "vurdering"}

        for i, col in enumerate(header):
            if col is None:
                continue
            lower = col.strip().lower()
            if any(n in lower for n in marker_names):
                col_map["marker"] = i
            elif any(n in lower for n in value_names):
                col_map["value"] = i
            elif any(n in lower for n in unit_names):
                col_map["unit"] = i
            elif any(n in lower for n in ref_names):
                col_map["reference"] = i
            elif any(n in lower for n in status_names):
                col_map["status"] = i

        return col_map

    @staticmethod
    def _map_columns_sundhed_dk(header: list) -> dict[str, Optional[int]]:
        """Map sundhed.dk column headers to indices."""
        col_map: dict[str, Optional[int]] = {
            "marker": None,
            "value": None,
            "unit": None,
            "reference": None,
            "status": None,
        }

        for i, col in enumerate(header):
            if col is None:
                continue
            lower = col.strip().lower()
            if lower in {"analyse", "komponent", "undersøgelse", "test"}:
                col_map["marker"] = i
            elif lower in {"svar", "resultat", "værdi", "value"}:
                col_map["value"] = i
            elif lower in {"enhed", "unit"}:
                col_map["unit"] = i
            elif lower in {
                "referenceinterval",
                "ref.interval",
                "reference",
                "normalområde",
            }:
                col_map["reference"] = i
            elif lower in {"status", "flag", "vurdering"}:
                col_map["status"] = i

        return col_map

    @staticmethod
    def _guess_column_mapping(header: list) -> dict[str, Optional[int]]:
        """Best-effort column mapping for unknown formats."""
        col_map: dict[str, Optional[int]] = {
            "marker": None,
            "value": None,
            "unit": None,
            "reference": None,
            "status": None,
        }

        if len(header) >= 3:
            # Assume first column is marker name
            col_map["marker"] = 0
            col_map["value"] = 1
            if len(header) >= 4:
                col_map["unit"] = 2
                col_map["reference"] = 3
            else:
                col_map["reference"] = 2
            if len(header) >= 5:
                col_map["status"] = 4

        return col_map

    @staticmethod
    def _extract_marker_from_row(
        row: list, col_map: dict[str, Optional[int]]
    ) -> Optional[dict]:
        """Extract a single marker dict from a table row using column mapping."""

        def _cell(idx: Optional[int]) -> str:
            if idx is None or idx >= len(row) or row[idx] is None:
                return ""
            return str(row[idx]).strip()

        marker_name = _cell(col_map["marker"])
        if not marker_name:
            return None

        value_text = _cell(col_map["value"])
        value_numeric = _parse_danish_number(value_text) if value_text else None

        unit = _cell(col_map["unit"])
        reference_text = _cell(col_map["reference"])
        ref_min, ref_max, ref_direction = _parse_reference_range(reference_text)

        status_text = _cell(col_map["status"]).lower()

        # Map common status labels
        if status_text in {"normal", "ok", "inden for reference", "i.r."}:
            status = "normal"
        elif status_text in {"lav", "low", "l", "under"}:
            status = "low"
        elif status_text in {"høj", "high", "h", "over"}:
            status = "high"
        elif status_text:
            status = status_text
        else:
            # Compute from reference range
            status = _determine_status(value_numeric, ref_min, ref_max, ref_direction)

        return {
            "marker_name": marker_name,
            "value_numeric": value_numeric,
            "value_text": value_text if value_text else None,
            "unit": unit if unit else None,
            "reference_min": ref_min,
            "reference_max": ref_max,
            "reference_direction": ref_direction,
            "status": status,
        }
