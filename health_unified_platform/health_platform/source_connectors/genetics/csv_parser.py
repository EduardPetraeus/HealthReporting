"""Parse 23andMe ancestry CSV and family tree JSON files.

Ancestry CSV format:
    Ancestry,Copy,Chromosome,Start Point,End Point

Family tree JSON format:
    {"trees": [{"nodes": [...]}], ...}
    Each node: id, parent_ids, partner_ids, relationship_id, up, down,
               first_name, last_name, num_shared, ...
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from health_platform.utils.logging_config import get_logger

logger = get_logger("genetics_csv_parser")

# Human chromosome lengths (GRCh37/hg19, approximate bp)
_CHROMOSOME_LENGTHS: dict[str, int] = {
    "chr1": 249_250_621,
    "chr2": 243_199_373,
    "chr3": 198_022_430,
    "chr4": 191_154_276,
    "chr5": 180_915_260,
    "chr6": 171_115_067,
    "chr7": 159_138_663,
    "chr8": 146_364_022,
    "chr9": 141_213_431,
    "chr10": 135_534_747,
    "chr11": 135_006_516,
    "chr12": 133_851_895,
    "chr13": 115_169_878,
    "chr14": 107_349_540,
    "chr15": 102_531_392,
    "chr16": 90_354_753,
    "chr17": 81_195_210,
    "chr18": 78_077_248,
    "chr19": 59_128_983,
    "chr20": 63_025_520,
    "chr21": 48_129_895,
    "chr22": 51_304_566,
    "chrX": 155_270_560,
    "chrY": 59_373_566,
}


def parse_ancestry_csv(path: Path) -> list[dict]:
    """Parse a 23andMe ancestry composition CSV.

    Adds computed segment_length_bp field.

    Returns:
        List of dicts with: ancestry_category, copy, chromosome,
        start_bp, end_bp, segment_length_bp
    """
    rows: list[dict] = []

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ancestry = row.get("Ancestry", "").strip()
            if not ancestry:
                continue

            try:
                copy_num = int(row.get("Copy", "0"))
                chromosome = row.get("Chromosome", "").strip()
                start_bp = int(row.get("Start Point", "0"))
                end_bp = int(row.get("End Point", "0"))
            except (ValueError, TypeError) as exc:
                logger.warning("Skipping malformed row in %s: %s", path.name, exc)
                continue

            segment_length_bp = end_bp - start_bp if end_bp > start_bp else 0

            rows.append(
                {
                    "ancestry_category": ancestry,
                    "copy": copy_num,
                    "chromosome": chromosome,
                    "start_bp": start_bp,
                    "end_bp": end_bp,
                    "segment_length_bp": segment_length_bp,
                }
            )

    logger.info("Parsed %d ancestry segments from %s", len(rows), path.name)
    return rows


def parse_family_tree_json(path: Path) -> list[dict]:
    """Parse a 23andMe family tree JSON export.

    Flattens the tree nodes and derives side (maternal/paternal)
    from relationship strings.

    Returns:
        List of dicts with: person_id, first_name, last_name,
        relationship_to_user, generation, side, num_shared_segments,
        has_dna_match, profile_url
    """
    max_size = 50 * 1024 * 1024  # 50 MB
    if path.stat().st_size > max_size:
        raise ValueError(f"Family tree JSON exceeds {max_size} bytes: {path.name}")

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    trees = data.get("trees", [])
    if not trees:
        logger.warning("No trees found in %s", path.name)
        return []

    nodes = trees[0].get("nodes", [])
    rows: list[dict] = []

    for node in nodes:
        relationship = node.get("relationship_id") or "unknown"
        generation_up = node.get("up", 0)
        generation_down = node.get("down", 0)

        # Derive generation relative to user: negative = ancestor, positive = descendant
        if relationship == "you":
            generation = 0
        elif generation_down > 0 and generation_up > 0:
            # Collateral relative (cousin, uncle, etc.)
            generation = generation_down - generation_up
        elif generation_up > 0:
            generation = -generation_up
        else:
            generation = generation_down

        side = _derive_side(relationship)
        first_name = node.get("first_name") or ""
        last_name = node.get("last_name") or ""
        num_shared = node.get("num_shared", 0)
        profile_url = node.get("url") or ""

        rows.append(
            {
                "person_id": node.get("id", ""),
                "first_name": first_name,
                "last_name": last_name,
                "relationship_to_user": relationship,
                "generation": generation,
                "side": side,
                "num_shared_segments": num_shared,
                "has_dna_match": num_shared > 0,
                "profile_url": profile_url,
            }
        )

    logger.info("Parsed %d family tree nodes from %s", len(rows), path.name)
    return rows


def _derive_side(relationship: str) -> str:
    """Derive maternal/paternal side from relationship string.

    23andMe doesn't explicitly label sides, but we can infer
    from relationship patterns in the tree structure.
    Returns 'maternal', 'paternal', 'both', or 'unknown'.
    """
    if not relationship or relationship == "you":
        return "both"

    rel_lower = relationship.lower()

    # Partners of relatives have no genetic side
    if relationship is None or rel_lower == "unknown":
        return "unknown"

    # Direct ancestors could be either side — we'd need parent_ids
    # to resolve. For now, return 'unknown' for ambiguous cases.
    return "unknown"
