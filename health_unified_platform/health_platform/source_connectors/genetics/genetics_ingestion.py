"""Orchestrate 23andMe genetics data ingestion.

Scans a directory for PDF, CSV, and JSON files, routes each to the
appropriate parser, computes hashes, and writes bronze parquet files.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from health_platform.source_connectors.genetics.csv_parser import (
    parse_ancestry_csv,
    parse_family_tree_json,
)
from health_platform.source_connectors.genetics.pdf_parser import GeneticsPdfParser
from health_platform.utils.logging_config import get_logger

logger = get_logger("genetics_ingestion")


def _compute_hash(*parts: str) -> str:
    """Compute MD5 hash of concatenated parts."""
    combined = "||".join(str(p) for p in parts)
    return hashlib.md5(combined.encode("utf-8")).hexdigest()


def ingest_genetics_data(input_dir: Path, output_dir: Path) -> dict[str, Path]:
    """Scan directory for 23andMe files, parse, and write bronze parquet.

    Args:
        input_dir: Directory containing 23andMe PDF, CSV, JSON files.
        output_dir: Directory to write bronze parquet files.

    Returns:
        Dict mapping bronze table name to output parquet path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    now_str = datetime.now(timezone.utc).isoformat()

    pdf_parser = GeneticsPdfParser()
    health_findings: list[dict] = []
    ancestry_segments: list[dict] = []
    family_members: list[dict] = []

    # Scan for PDF files
    pdf_files = sorted(input_dir.glob("*.pdf"))
    for pdf_path in pdf_files:
        logger.info("Parsing PDF: %s", pdf_path.name)
        findings = pdf_parser.parse_pdf(pdf_path)
        for finding in findings:
            finding["source_file"] = pdf_path.name
            finding["business_key_hash"] = _compute_hash(
                finding.get("category", ""),
                finding.get("report_name", ""),
            )
            finding["row_hash"] = _compute_hash(
                finding.get("result_summary", ""),
                finding.get("gene", "") or "",
                finding.get("snp_id", "") or "",
                finding.get("genotype", "") or "",
                str(finding.get("variant_detected", "")),
            )
            finding["load_datetime"] = now_str
            finding["update_datetime"] = now_str
            health_findings.append(finding)

    # Scan for ancestry CSV files
    csv_files = sorted(input_dir.glob("*.csv"))
    for csv_path in csv_files:
        if "ancestry" in csv_path.name.lower():
            logger.info("Parsing ancestry CSV: %s", csv_path.name)
            segments = parse_ancestry_csv(csv_path)
            for seg in segments:
                seg["source_file"] = csv_path.name
                seg["business_key_hash"] = _compute_hash(
                    seg["ancestry_category"],
                    str(seg["copy"]),
                    seg["chromosome"],
                    str(seg["start_bp"]),
                )
                seg["row_hash"] = _compute_hash(
                    str(seg["end_bp"]),
                    str(seg["segment_length_bp"]),
                )
                seg["load_datetime"] = now_str
                seg["update_datetime"] = now_str
                ancestry_segments.extend([seg])

    # Scan for family tree JSON files
    json_files = sorted(input_dir.glob("*.json"))
    for json_path in json_files:
        if "family" in json_path.name.lower():
            logger.info("Parsing family tree JSON: %s", json_path.name)
            members = parse_family_tree_json(json_path)
            for member in members:
                member["source_file"] = json_path.name
                member["business_key_hash"] = _compute_hash(
                    member["person_id"],
                )
                member["row_hash"] = _compute_hash(
                    member["relationship_to_user"],
                    str(member["generation"]),
                    member["side"],
                    str(member["num_shared_segments"]),
                )
                member["load_datetime"] = now_str
                member["update_datetime"] = now_str
                family_members.append(member)

    outputs: dict[str, Path] = {}

    # Write health findings parquet
    if health_findings:
        path = output_dir / f"stg_23andme_health_reports_{timestamp}.parquet"
        df = pd.DataFrame(health_findings)
        df.to_parquet(path, index=False)
        outputs["stg_23andme_health_reports"] = path
        logger.info("Wrote %d health findings to %s", len(health_findings), path.name)

    # Write ancestry segments parquet
    if ancestry_segments:
        path = output_dir / f"stg_23andme_ancestry_{timestamp}.parquet"
        df = pd.DataFrame(ancestry_segments)
        df.to_parquet(path, index=False)
        outputs["stg_23andme_ancestry"] = path
        logger.info(
            "Wrote %d ancestry segments to %s", len(ancestry_segments), path.name
        )

    # Write family tree parquet
    if family_members:
        path = output_dir / f"stg_23andme_family_tree_{timestamp}.parquet"
        df = pd.DataFrame(family_members)
        df.to_parquet(path, index=False)
        outputs["stg_23andme_family_tree"] = path
        logger.info("Wrote %d family members to %s", len(family_members), path.name)

    logger.info(
        "Genetics ingestion complete: %d findings, %d ancestry segments, %d family members",
        len(health_findings),
        len(ancestry_segments),
        len(family_members),
    )
    return outputs
