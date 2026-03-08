"""Lab PDF ingestion pipeline.

Scans a directory for blood test PDFs, parses each using LabPdfParser,
generates business keys and hashes, and writes results to parquet.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pdfplumber  # noqa: F401 — used in ingest_lab_pdfs

from health_platform.source_connectors.lab.pdf_parser import LabPdfParser
from health_platform.utils.logging_config import get_logger

logger = get_logger("lab_ingestion")

# Pattern for extracting date from filenames: 2026-01-15, 20260115, etc.
_DATE_FROM_FILENAME_RE = re.compile(r"(\d{4})-?(\d{2})-?(\d{2})")


def _extract_date_from_filename(filename: str) -> Optional[str]:
    """Try to extract a date from the PDF filename.

    Supports formats like:
      - blood_test_2026-01-15.pdf
      - 20260115_gettested.pdf
      - lab_results_2026_01_15.pdf

    Returns ISO date string or None.
    """
    match = _DATE_FROM_FILENAME_RE.search(filename)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return None


def _compute_hash(*parts: str) -> str:
    """Compute MD5 hash of concatenated parts."""
    combined = "||".join(str(p) for p in parts)
    return hashlib.md5(combined.encode("utf-8")).hexdigest()


def ingest_lab_pdfs(pdf_dir: Path, output_dir: Path) -> Path:
    """Scan directory for PDF files, parse each, and write results to parquet.

    Args:
        pdf_dir: Directory containing lab PDF files.
        output_dir: Directory to write the output parquet file.

    Returns:
        Path to the output parquet file.
    """
    parser = LabPdfParser()
    all_rows: list[dict] = []

    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in %s", pdf_dir)

    for pdf_path in pdf_files:
        logger.info("Processing %s", pdf_path.name)
        test_date = _extract_date_from_filename(pdf_path.stem)
        if not test_date:
            test_date = datetime.now().strftime("%Y-%m-%d")
            logger.warning(
                "Could not extract date from %s — using today: %s",
                pdf_path.name,
                test_date,
            )

        test_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"lab:{pdf_path.name}:{test_date}")
        )

        markers = parser.parse_pdf(pdf_path)
        if not markers:
            logger.warning("No markers extracted from %s", pdf_path.name)
            continue

        # Detect format for metadata
        try:
            with pdfplumber.open(pdf_path) as pdf:
                full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            fmt = parser.detect_format(full_text)
        except Exception:
            fmt = "unknown"

        lab_name = _format_to_lab_name(fmt)

        for marker in markers:
            business_key = _compute_hash(
                test_id,
                marker["marker_name"],
            )
            row_hash = _compute_hash(
                str(marker.get("value_numeric", "")),
                str(marker.get("value_text", "")),
                str(marker.get("unit", "")),
                str(marker.get("reference_min", "")),
                str(marker.get("reference_max", "")),
                marker["status"],
            )

            all_rows.append(
                {
                    "test_id": test_id,
                    "test_date": test_date,
                    "test_type": "blood_panel",
                    "test_name": pdf_path.stem,
                    "lab_name": lab_name,
                    "lab_accreditation": None,
                    "marker_name": marker["marker_name"],
                    "marker_category": _categorize_marker(marker["marker_name"]),
                    "value_numeric": marker.get("value_numeric"),
                    "value_text": marker.get("value_text"),
                    "unit": marker.get("unit"),
                    "reference_min": marker.get("reference_min"),
                    "reference_max": marker.get("reference_max"),
                    "reference_direction": marker.get("reference_direction"),
                    "status": marker["status"],
                    "business_key_hash": business_key,
                    "row_hash": row_hash,
                    "load_datetime": datetime.now().isoformat(),
                    "update_datetime": None,
                }
            )

    if not all_rows:
        logger.warning("No lab results extracted from any PDFs in %s", pdf_dir)
        # Write empty parquet with correct schema
        df = pd.DataFrame(
            columns=[
                "test_id",
                "test_date",
                "test_type",
                "test_name",
                "lab_name",
                "lab_accreditation",
                "marker_name",
                "marker_category",
                "value_numeric",
                "value_text",
                "unit",
                "reference_min",
                "reference_max",
                "reference_direction",
                "status",
                "business_key_hash",
                "row_hash",
                "load_datetime",
                "update_datetime",
            ]
        )
    else:
        df = pd.DataFrame(all_rows)

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"lab_pdf_results_{timestamp}.parquet"
    df.to_parquet(output_path, index=False)

    logger.info(
        "Wrote %d markers from %d PDFs to %s",
        len(all_rows),
        len(pdf_files),
        output_path,
    )
    return output_path


def _format_to_lab_name(fmt: str) -> str:
    """Map format identifier to human-readable lab name."""
    mapping = {
        "gettested": "GetTested",
        "sundhed_dk": "sundhed.dk",
        "unknown": "Unknown Lab",
    }
    return mapping.get(fmt, "Unknown Lab")


# Basic marker categorization based on common blood panel groupings.
# Order matters: more specific categories (gut_health with "elastase") must
# come before broader ones (liver with "ast") to avoid false matches.
_CATEGORY_PATTERNS: list[tuple[str, list[str]]] = [
    ("lipids", ["cholesterol", "ldl", "hdl", "triglycerid", "apolipoprotein"]),
    ("gut_health", ["elastase", "zonulin", "iga", "microbiome", "flora"]),
    (
        "liver",
        [
            "alanin",
            "aspartat",
            "ggt",
            "bilirubin",
            "albumin",
            "alkalisk",
            "alat",
            "asat",
        ],
    ),
    ("kidney", ["kreatinin", "creatinin", "gfr", "urea", "cystatin"]),
    ("thyroid", ["tsh", "thyroxin", " t3", " t4", "free t3", "free t4"]),
    (
        "blood_count",
        [
            "hæmoglobin",
            "hemoglobin",
            "hgb",
            "leukocyt",
            "trombocyt",
            "erytrocyt",
            "mcv",
            "mch",
        ],
    ),
    ("inflammation", ["crp", "calprotectin", "ferritin"]),
    ("vitamins", ["vitamin", "folat", "b12", "d3", "cobalamin"]),
    (
        "minerals",
        [
            "jern",
            "iron",
            "zink",
            "zinc",
            "selen",
            "selenium",
            "magnesium",
            "calcium",
            "kalium",
            "natrium",
        ],
    ),
    (
        "hormones",
        ["testosteron", "cortisol", "insulin", "shbg", "estradiol", "progesteron"],
    ),
    ("fatty_acids", ["epa", "dha", "omega", "arachidonic"]),
    ("glucose", ["glucose", "hba1c", "blodsukker"]),
]


def _categorize_marker(marker_name: str) -> str:
    """Categorize a marker based on its name using pattern matching."""
    lower = marker_name.lower()
    for category, patterns in _CATEGORY_PATTERNS:
        for pattern in patterns:
            if pattern in lower:
                return category
    return "other"
