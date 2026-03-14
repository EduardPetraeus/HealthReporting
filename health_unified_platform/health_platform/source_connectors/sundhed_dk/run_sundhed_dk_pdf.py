"""Ingest sundhed.dk PDF downloads into the data lake.

Reads PDFs from <data_lake_root>/min sundhed/downloads/
and writes Parquet files to <data_lake_root>/min sundhed/raw/

Usage:
    HEALTH_ENV=dev python run_sundhed_dk_pdf.py
    HEALTH_ENV=dev python run_sundhed_dk_pdf.py --sections medications,vaccinations
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from health_platform.source_connectors.oura.writer import write_records
from health_platform.source_connectors.sundhed_dk.pdf_parser import (
    parse_ejournal_pdf,
    parse_lab_results_pdf,
    parse_medications_pdf,
    parse_vaccinations_pdf,
)
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger

logger = get_logger("run_sundhed_dk_pdf")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")

# Section definitions: (section_name, subdirectory, parser_function, parquet_endpoint, date_field)
SECTIONS: list[tuple[str, str, callable, str, str]] = [
    ("lab_results", "lab_results", parse_lab_results_pdf, "lab_results", "test_date"),
    ("medications", "medications", parse_medications_pdf, "medications", "start_date"),
    (
        "vaccinations",
        "vaccinations",
        parse_vaccinations_pdf,
        "vaccinations",
        "vaccine_date",
    ),
    ("ejournal", "ejournal", parse_ejournal_pdf, "ejournal", "note_date"),
]

# Resolve data lake root (same logic as run_sundhed_dk.py)
_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "health_environment"
    / "config"
    / "environment_config.yaml"
)


def _resolve_sundhed_dk_root() -> Path:
    """Resolve data lake root for sundhed.dk."""
    try:
        import yaml

        with open(_CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return Path(cfg["paths"]["data_lake_root"]) / "min sundhed" / "raw"
    except Exception:
        fallback = os.getenv("SUNDHED_DK_DATA_ROOT")
        if fallback:
            return Path(fallback)
        raise RuntimeError(
            "Cannot resolve data lake root: environment_config.yaml not found "
            "and SUNDHED_DK_DATA_ROOT env var not set."
        )


def _resolve_downloads_root() -> Path:
    """Resolve the PDF downloads directory."""
    try:
        import yaml

        with open(_CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return Path(cfg["paths"]["data_lake_root"]) / "min sundhed" / "downloads"
    except Exception:
        fallback = os.getenv("SUNDHED_DK_DOWNLOADS_ROOT")
        if fallback:
            return Path(fallback)
        # Default path via utility
        from health_platform.utils.paths import get_data_lake_root

        return get_data_lake_root() / "min sundhed" / "downloads"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest sundhed.dk PDF downloads into the data lake"
    )
    parser.add_argument(
        "--sections",
        type=str,
        default=None,
        help="Comma-separated list of sections to ingest (default: all). "
        "Options: lab_results, medications, vaccinations, ejournal",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    sundhed_dk_data_root = _resolve_sundhed_dk_root()
    downloads_root = _resolve_downloads_root()

    # Filter sections if specified
    if args.sections:
        requested = {s.strip() for s in args.sections.split(",")}
        valid_names = {name for name, _, _, _, _ in SECTIONS}
        invalid = requested - valid_names
        if invalid:
            logger.error("Unknown sections: %s. Valid: %s", invalid, valid_names)
            sys.exit(1)
        sections = [(n, d, p, e, df) for n, d, p, e, df in SECTIONS if n in requested]
    else:
        sections = SECTIONS

    logger.info(
        "sundhed.dk PDF pipeline starting [env: %s, sections: %s, downloads: %s]",
        SOURCE_ENV,
        [s[0] for s in sections],
        downloads_root,
    )

    if not downloads_root.exists():
        logger.error("Downloads directory does not exist: %s", downloads_root)
        sys.exit(1)

    # Ensure output directory exists
    sundhed_dk_data_root.mkdir(parents=True, exist_ok=True)

    # Summary counters
    summary: list[dict] = []

    with AuditLogger("run_sundhed_dk_pdf", "extract", "sundhed_dk") as audit:
        for section_name, subdir, parser_fn, endpoint, date_field in sections:
            section_dir = downloads_root / subdir
            logger.info("Processing section: %s (dir: %s)", section_name, section_dir)

            if not section_dir.exists():
                logger.warning(
                    "Section directory does not exist: %s — skipping",
                    section_dir,
                )
                audit.log_table(
                    f"sundhed_dk.{section_name}",
                    "WRITE_PARQUET",
                    rows_after=0,
                    status="directory_missing",
                )
                summary.append(
                    {
                        "section": section_name,
                        "pdfs": 0,
                        "records": 0,
                        "status": "directory_missing",
                    }
                )
                continue

            pdf_files = sorted(section_dir.glob("*.pdf"))
            if not pdf_files:
                logger.warning("No PDF files found in %s", section_dir)
                audit.log_table(
                    f"sundhed_dk.{section_name}",
                    "WRITE_PARQUET",
                    rows_after=0,
                    status="no_pdfs",
                )
                summary.append(
                    {
                        "section": section_name,
                        "pdfs": 0,
                        "records": 0,
                        "status": "no_pdfs",
                    }
                )
                continue

            # Parse all PDFs for this section and combine records
            all_records: list[dict] = []
            for pdf_path in pdf_files:
                logger.info("  Parsing: %s", pdf_path.name)
                try:
                    records = parser_fn(pdf_path)
                    logger.info(
                        "  Extracted %d records from %s", len(records), pdf_path.name
                    )
                    all_records.extend(records)
                except Exception:
                    logger.exception("  Failed to parse %s — skipping", pdf_path.name)

            logger.info(
                "  Total: %d records from %d PDFs for '%s'",
                len(all_records),
                len(pdf_files),
                section_name,
            )

            # Write to Parquet using the shared writer
            if all_records:
                write_records(
                    all_records,
                    endpoint,
                    date_field,
                    source_env=SOURCE_ENV,
                    data_lake_root=sundhed_dk_data_root,
                )

            audit.log_table(
                f"sundhed_dk.{section_name}",
                "WRITE_PARQUET",
                rows_after=len(all_records),
                status="success" if all_records else "empty",
            )

            summary.append(
                {
                    "section": section_name,
                    "pdfs": len(pdf_files),
                    "records": len(all_records),
                    "status": "success" if all_records else "empty",
                }
            )

    # Print summary report
    _print_summary(summary)


def _print_summary(summary: list[dict]) -> None:
    """Print a human-readable summary of the ingestion run."""
    total_pdfs = sum(s["pdfs"] for s in summary)
    total_records = sum(s["records"] for s in summary)

    logger.info("=" * 60)
    logger.info("sundhed.dk PDF ingestion complete")
    logger.info("=" * 60)
    logger.info("%-15s  %6s  %8s  %s", "Section", "PDFs", "Records", "Status")
    logger.info("-" * 50)

    for s in summary:
        logger.info(
            "%-15s  %6d  %8d  %s",
            s["section"],
            s["pdfs"],
            s["records"],
            s["status"],
        )

    logger.info("-" * 50)
    logger.info("%-15s  %6d  %8d", "TOTAL", total_pdfs, total_records)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
