"""Entry point for the sundhed.dk scraper pipeline.

Full load only — no incremental sync (data changes quarterly at most).

Usage:
    HEALTH_ENV=dev python run_sundhed_dk.py
    HEALTH_ENV=dev python run_sundhed_dk.py --sections lab_results,medications
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger

# Reuse the Oura writer — same parquet pattern
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "oura"))
from browser import SundhedDkBrowser
from parsers import (
    parse_appointments,
    parse_ejournal,
    parse_lab_results,
    parse_medications,
    parse_vaccinations,
)
from scraper import SessionExpiredError, SundhedDkScraper
from writer import write_records

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

    browser = SundhedDkBrowser()

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

                records = parser_fn(html)
                logger.info("  Parsed %d records from '%s'", len(records), section_name)

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
                    status="success",
                )

    finally:
        browser.close()

    logger.info("sundhed.dk pipeline complete")


if __name__ == "__main__":
    main()
