"""Entry point for the sundhed.dk scraper pipeline.

Full load only — no incremental sync (data changes quarterly at most).

Pipeline: authenticate -> scrape HTML -> archive HTML -> parse -> validate -> write Parquet.

Usage:
    HEALTH_ENV=dev python run_sundhed_dk.py
    HEALTH_ENV=dev python run_sundhed_dk.py --sections lab_results,medications
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
from pathlib import Path

from health_platform.source_connectors.oura.writer import write_records
from health_platform.source_connectors.sundhed_dk.browser import SundhedDkBrowser
from health_platform.source_connectors.sundhed_dk.parsers import (
    parse_appointments,
    parse_ejournal,
    parse_lab_results,
    parse_medications,
    parse_vaccinations,
)
from health_platform.source_connectors.sundhed_dk.scraper import (
    SessionExpiredError,
    SundhedDkScraper,
    save_html,
)
from health_platform.source_connectors.sundhed_dk.validator import (
    ValidationReport,
    generate_validation_report,
    validate_parse_completeness,
)
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger

logger = get_logger("run_sundhed_dk")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")

# Section definitions: (section_name, parser_function, date_field_in_output)
SECTIONS: list[tuple[str, callable, str]] = [
    ("lab_results", parse_lab_results, "test_date"),
    ("medications", parse_medications, "start_date"),
    ("vaccinations", parse_vaccinations, "vaccine_date"),
    ("ejournal", parse_ejournal, "note_date"),
    ("appointments", parse_appointments, "referral_date"),
]

# Resolve data lake root
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


SUNDHED_DK_DATA_ROOT = _resolve_sundhed_dk_root()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape sundhed.dk health data")
    parser.add_argument(
        "--sections",
        type=str,
        default=None,
        help="Comma-separated list of sections to scrape (default: all). "
        "Options: lab_results, medications, vaccinations, ejournal, appointments",
    )
    return parser.parse_args()


def _save_validation_report(reports: list[ValidationReport], data_root: Path) -> None:
    """Save validation report to the validation directory."""
    report_text = generate_validation_report(reports)
    validation_dir = data_root.parent / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = validation_dir / f"validation_{timestamp}.txt"
    report_path.write_text(report_text, encoding="utf-8")
    logger.info("Validation report saved: %s", report_path)


def main() -> None:
    args = _parse_args()

    # Filter sections if specified
    if args.sections:
        requested = {s.strip() for s in args.sections.split(",")}
        valid_names = {name for name, _, _ in SECTIONS}
        invalid = requested - valid_names
        if invalid:
            logger.error("Unknown sections: %s. Valid: %s", invalid, valid_names)
            sys.exit(1)
        sections = [(n, p, d) for n, p, d in SECTIONS if n in requested]
    else:
        sections = SECTIONS

    logger.info(
        "sundhed.dk pipeline starting [env: %s, sections: %s]",
        SOURCE_ENV,
        [s[0] for s in sections],
    )

    # Ensure data lake directories exist
    SUNDHED_DK_DATA_ROOT.mkdir(parents=True, exist_ok=True)
    archive_root = str(SUNDHED_DK_DATA_ROOT.parent)  # "min sundhed/" level

    browser = SundhedDkBrowser()
    validation_reports: list[ValidationReport] = []

    try:
        page = browser.launch_and_authenticate()
        scraper = SundhedDkScraper(page)

        with AuditLogger("run_sundhed_dk", "extract", "sundhed_dk") as audit:
            for section_name, parser_fn, date_field in sections:
                logger.info("Processing section: %s", section_name)

                try:
                    html = scraper.scrape_section(section_name)
                except SessionExpiredError:
                    logger.error(
                        "Session expired during '%s'. Re-run the script to re-authenticate.",
                        section_name,
                    )
                    audit.log_table(
                        f"sundhed_dk.{section_name}",
                        "WRITE_PARQUET",
                        rows_after=0,
                        status="session_expired",
                    )
                    break

                if not html:
                    logger.warning("No HTML content for '%s', skipping.", section_name)
                    audit.log_table(
                        f"sundhed_dk.{section_name}",
                        "WRITE_PARQUET",
                        rows_after=0,
                        status="empty",
                    )
                    continue

                # Archive raw HTML before parsing (forensic audit trail)
                save_html(html, section_name, archive_root)

                # Parse HTML to records
                records = parser_fn(html)
                logger.info("  Parsed %d records from '%s'", len(records), section_name)

                # Validate parse completeness (zero data loss check)
                report = validate_parse_completeness(html, records, section_name)
                validation_reports.append(report)

                if not report.match:
                    logger.error(
                        "VALIDATION MISMATCH for '%s': HTML=%d rows, parsed=%d. "
                        "HTML archived for inspection.",
                        section_name,
                        report.html_rows,
                        report.parsed_rows,
                    )

                # Write Parquet regardless of validation (data is still useful)
                write_records(
                    records,
                    section_name,
                    date_field,
                    source_env=SOURCE_ENV,
                    data_lake_root=SUNDHED_DK_DATA_ROOT,
                )

                audit.log_table(
                    f"sundhed_dk.{section_name}",
                    "WRITE_PARQUET",
                    rows_after=len(records),
                    status="success" if report.match else "mismatch",
                )

    finally:
        browser.close()

    # Print and save validation report
    if validation_reports:
        report_text = generate_validation_report(validation_reports)
        logger.info("Validation report:\n%s", report_text)
        _save_validation_report(validation_reports, SUNDHED_DK_DATA_ROOT)

    logger.info("sundhed.dk pipeline complete")


if __name__ == "__main__":
    main()
