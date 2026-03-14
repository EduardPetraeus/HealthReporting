"""Zero-loss validation for lab and genetics data.

Compares source YAML files against DuckDB silver tables to ensure
all values were ingested correctly with no data loss.

Usage:
    python -m health_platform.quality.validate_lab_data
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import duckdb
import yaml
from health_platform.utils.logging_config import get_logger

logger = get_logger("validate_lab_data")


# Paths
DATA_LAKE = Path("/Users/Shared/data_lake")
DB_PATH = DATA_LAKE / "database" / "health_dw_dev.db"
BLOOD_YAML = DATA_LAKE / "manual" / "lab_results" / "blood_panel_2026-02-24.yaml"
MICROBIOME_YAML = DATA_LAKE / "manual" / "lab_results" / "microbiome_2026-02-23.yaml"
GENETICS_YAML = DATA_LAKE / "manual" / "genetic_profile.yaml"


class ValidationResult:
    """Tracks validation results for a single data source."""

    def __init__(self, source_name: str) -> None:
        self.source_name = source_name
        self.checks: list[dict] = []

    def add(
        self,
        field: str,
        status: str,
        expected: str,
        actual: str,
        notes: str = "",
    ) -> None:
        self.checks.append(
            {
                "field": field,
                "status": status,  # PASS, WARN, FAIL
                "expected": expected,
                "actual": actual,
                "notes": notes,
            }
        )

    @property
    def pass_count(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "PASS")

    @property
    def warn_count(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "WARN")

    @property
    def fail_count(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "FAIL")

    @property
    def total(self) -> int:
        return len(self.checks)

    def summary(self) -> str:
        lines = [
            f"\n{'=' * 60}",
            f"Validation: {self.source_name}",
            f"{'=' * 60}",
        ]
        for check in self.checks:
            icon = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]"}.get(
                check["status"], "?"
            )
            line = f"  {icon} {check['field']}: {check['status']}"
            if check["status"] != "PASS":
                line += f" (expected={check['expected']}, actual={check['actual']})"
            if check["notes"]:
                line += f" -- {check['notes']}"
            lines.append(line)
        lines.append(
            f"\nTotal: {self.total} checks -- "
            f"{self.pass_count} PASS, {self.warn_count} WARN, {self.fail_count} FAIL"
        )
        return "\n".join(lines)


def _approx_equal(
    a: Optional[float], b: Optional[float], tolerance: float = 0.01
) -> bool:
    """Check if two floats are approximately equal."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) < tolerance


def _normalize_name(name: str) -> str:
    """Normalize marker name for comparison (lowercase, strip whitespace)."""
    return name.strip().lower()


def validate_blood_panel(con: duckdb.DuckDBPyConnection) -> ValidationResult:
    """Validate blood panel data: YAML source vs silver.lab_results.

    YAML structure: flat list under 'markers' key, each with:
      name, category, value, unit, ref_min, ref_max, status,
      optional ref_direction.
    """
    result = ValidationResult("Blood Panel")

    # Load source YAML
    if not BLOOD_YAML.exists():
        result.add("source_file", "FAIL", str(BLOOD_YAML), "NOT FOUND")
        return result

    with open(BLOOD_YAML, errors="ignore") as f:
        source = yaml.safe_load(f)

    # Extract markers from flat list
    yaml_markers = source.get("markers", [])
    yaml_count = len(yaml_markers)

    result.add(
        "source_marker_count",
        "PASS" if yaml_count > 0 else "FAIL",
        ">0",
        str(yaml_count),
    )

    # Query silver table
    try:
        rows = con.execute(
            """
            SELECT marker_name, value_numeric, value_text, unit,
                   reference_min, reference_max, status
            FROM silver.lab_results
            WHERE test_type = 'blood_panel'
            ORDER BY marker_name
        """
        ).fetchall()
    except duckdb.CatalogException:
        result.add("silver_table", "FAIL", "silver.lab_results", "TABLE NOT FOUND")
        return result

    silver_by_name: dict[str, tuple] = {_normalize_name(row[0]): row for row in rows}

    # Count check
    result.add(
        "ingested_marker_count",
        "PASS" if len(silver_by_name) >= yaml_count else "FAIL",
        str(yaml_count),
        str(len(silver_by_name)),
        "Missing markers in silver" if len(silver_by_name) < yaml_count else "",
    )

    # Per-marker field-by-field validation
    for marker in yaml_markers:
        name = marker.get("name", "")
        name_key = _normalize_name(name)

        if name_key not in silver_by_name:
            result.add(f"{name}/presence", "FAIL", "present", "NOT FOUND")
            continue

        row = silver_by_name[name_key]
        # row: (marker_name, value_numeric, value_text, unit,
        #        reference_min, reference_max, status)

        # --- Value check ---
        yaml_val = marker.get("value")
        if yaml_val is not None:
            try:
                yaml_numeric = float(str(yaml_val).replace(",", "."))
            except (ValueError, TypeError):
                yaml_numeric = None

            if yaml_numeric is not None:
                if _approx_equal(yaml_numeric, row[1]):
                    status = "PASS"
                elif row[1] is not None and _approx_equal(
                    yaml_numeric, row[1], tolerance=0.1
                ):
                    status = "WARN"
                else:
                    status = "FAIL"
                result.add(
                    f"{name}/value",
                    status,
                    str(yaml_numeric),
                    str(row[1]),
                    "Rounding diff" if status == "WARN" else "",
                )
            else:
                # Text value comparison
                yaml_text = str(yaml_val)
                silver_text = str(row[2]) if row[2] is not None else ""
                status = "PASS" if yaml_text == silver_text else "WARN"
                result.add(
                    f"{name}/value_text",
                    status,
                    yaml_text,
                    silver_text,
                    "Formatting diff" if status == "WARN" else "",
                )

        # --- Unit check ---
        yaml_unit = marker.get("unit", "")
        if yaml_unit:
            silver_unit = row[3] if row[3] is not None else ""
            if yaml_unit == silver_unit:
                status = "PASS"
            elif _normalize_name(yaml_unit) == _normalize_name(silver_unit):
                status = "WARN"
            else:
                status = "FAIL"
            result.add(
                f"{name}/unit",
                status,
                str(yaml_unit),
                str(silver_unit),
                "Case/format diff" if status == "WARN" else "",
            )

        # --- Reference range check ---
        yaml_ref_min = marker.get("ref_min")
        yaml_ref_max = marker.get("ref_max")

        if yaml_ref_min is not None:
            try:
                yaml_ref_min_f = float(yaml_ref_min)
            except (ValueError, TypeError):
                yaml_ref_min_f = None
            if yaml_ref_min_f is not None:
                status = "PASS" if _approx_equal(yaml_ref_min_f, row[4]) else "FAIL"
                result.add(f"{name}/ref_min", status, str(yaml_ref_min_f), str(row[4]))

        if yaml_ref_max is not None:
            try:
                yaml_ref_max_f = float(yaml_ref_max)
            except (ValueError, TypeError):
                yaml_ref_max_f = None
            if yaml_ref_max_f is not None:
                status = "PASS" if _approx_equal(yaml_ref_max_f, row[5]) else "FAIL"
                result.add(f"{name}/ref_max", status, str(yaml_ref_max_f), str(row[5]))

    return result


def validate_microbiome(con: duckdb.DuckDBPyConnection) -> ValidationResult:
    """Validate microbiome data: YAML source vs silver.lab_results.

    YAML structure: flat list under 'markers' key, each with:
      name, category, value, value_text, unit, ref_min, ref_max, status,
      optional ref_direction. Below-detection markers have value_text
      starting with '<'.
    """
    result = ValidationResult("Microbiome")

    if not MICROBIOME_YAML.exists():
        result.add("source_file", "FAIL", str(MICROBIOME_YAML), "NOT FOUND")
        return result

    with open(MICROBIOME_YAML, errors="ignore") as f:
        source = yaml.safe_load(f)

    yaml_markers = source.get("markers", [])
    yaml_count = len(yaml_markers)

    result.add(
        "source_marker_count",
        "PASS" if yaml_count > 0 else "FAIL",
        ">0",
        str(yaml_count),
    )

    try:
        rows = con.execute(
            """
            SELECT marker_name, value_numeric, value_text, unit,
                   reference_min, reference_max, status
            FROM silver.lab_results
            WHERE test_type = 'microbiome'
            ORDER BY marker_name
        """
        ).fetchall()
    except duckdb.CatalogException:
        result.add("silver_table", "FAIL", "silver.lab_results", "TABLE NOT FOUND")
        return result

    silver_by_name: dict[str, tuple] = {_normalize_name(row[0]): row for row in rows}

    # Count check
    not_ingested = len(silver_by_name) == 0
    result.add(
        "ingested_marker_count",
        "PASS" if len(silver_by_name) >= yaml_count else "FAIL",
        str(yaml_count),
        str(len(silver_by_name)),
        "Table empty -- not yet ingested" if not_ingested else "",
    )

    # Below-detection-limit markers (value_text starts with '<')
    below_detection = [m for m in yaml_markers if "<" in str(m.get("value_text", ""))]
    if below_detection and not not_ingested:
        bd_in_silver = sum(
            1 for row in silver_by_name.values() if row[2] and "<" in str(row[2])
        )
        result.add(
            "below_detection_markers",
            "PASS" if bd_in_silver >= len(below_detection) else "WARN",
            str(len(below_detection)),
            str(bd_in_silver),
        )

    # Per-marker validation
    for marker in yaml_markers:
        name = marker.get("name", "")
        name_key = _normalize_name(name)

        if name_key not in silver_by_name:
            result.add(
                f"{name}/presence",
                "WARN" if not_ingested else "FAIL",
                "present",
                "NOT FOUND",
                "Table empty -- not yet ingested" if not_ingested else "",
            )
            continue

        row = silver_by_name[name_key]

        # Value check -- handle below-detection specially
        yaml_val = marker.get("value")
        yaml_value_text = marker.get("value_text", "")
        is_below_detection = "<" in str(yaml_value_text)

        if is_below_detection:
            # For below-detection markers, compare value_text
            silver_text = str(row[2]) if row[2] is not None else ""
            if yaml_value_text == silver_text:
                status = "PASS"
            elif "<" in silver_text:
                status = "WARN"
            else:
                status = "FAIL"
            result.add(
                f"{name}/value_text",
                status,
                str(yaml_value_text),
                silver_text,
                "Below-detection formatting" if status == "WARN" else "",
            )
        elif yaml_val is not None:
            try:
                yaml_numeric = float(str(yaml_val).replace(",", "."))
            except (ValueError, TypeError):
                yaml_numeric = None

            if yaml_numeric is not None:
                # Microbiome values can be large (CFU/g in scientific notation)
                # Use relative tolerance for large values
                if yaml_numeric > 1000:
                    is_match = (
                        row[1] is not None
                        and abs(yaml_numeric - row[1]) / yaml_numeric < 0.001
                    )
                else:
                    is_match = _approx_equal(yaml_numeric, row[1])
                status = "PASS" if is_match else "FAIL"
                result.add(f"{name}/value", status, str(yaml_numeric), str(row[1]))

        # Unit check
        yaml_unit = marker.get("unit")
        if yaml_unit:
            silver_unit = row[3] if row[3] is not None else ""
            status = "PASS" if yaml_unit == silver_unit else "WARN"
            result.add(f"{name}/unit", status, str(yaml_unit), str(silver_unit))

    return result


def validate_genetics(con: duckdb.DuckDBPyConnection) -> ValidationResult:
    """Validate genetics data: YAML source vs silver tables.

    YAML structure:
      health_predispositions: list of dicts with report_name, category,
        result_summary, variant_detected, gene, snp_id, genotype,
        clinical_relevance, platform_relevance, related_metrics
      carrier_status: same structure as health_predispositions
      wellness: same structure (mapped to genetic_traits)
      ancestry:
        composition: dict of region -> percentage
        maternal_haplogroup: str
        paternal_haplogroup: str
    """
    result = ValidationResult("23andMe Genetics")

    if not GENETICS_YAML.exists():
        result.add("source_file", "FAIL", str(GENETICS_YAML), "NOT FOUND")
        return result

    with open(GENETICS_YAML, errors="ignore") as f:
        source = yaml.safe_load(f)

    # --- genetic_health_findings (health_predispositions + carrier_status) ---
    yaml_health = source.get("health_predispositions", [])
    yaml_carrier = source.get("carrier_status", [])
    yaml_all_findings = yaml_health + yaml_carrier
    expected_findings = len(yaml_all_findings)

    try:
        health_rows = con.execute(
            """
            SELECT report_name, category, result_summary, variant_detected,
                   gene, snp_id, genotype, clinical_relevance
            FROM silver.genetic_health_findings
            ORDER BY report_name
        """
        ).fetchall()
    except duckdb.CatalogException:
        health_rows = []
        result.add(
            "genetic_health_findings_table",
            "WARN",
            "silver.genetic_health_findings",
            "TABLE NOT FOUND",
            "Not yet created",
        )

    health_count = len(health_rows)
    not_ingested_health = health_count == 0

    result.add(
        "health_findings_count",
        "PASS" if health_count >= expected_findings else "WARN",
        str(expected_findings),
        str(health_count),
        "Not yet ingested" if not_ingested_health else "",
    )

    # Per-finding validation if data exists
    if health_rows:
        silver_findings = {_normalize_name(row[0]): row for row in health_rows}
        for finding in yaml_all_findings:
            report_name = finding.get("report_name", "")
            name_key = _normalize_name(report_name)
            if name_key in silver_findings:
                row = silver_findings[name_key]
                # Check variant_detected
                yaml_variant = finding.get("variant_detected", False)
                silver_variant = row[3]
                result.add(
                    f"{report_name}/variant_detected",
                    "PASS" if yaml_variant == silver_variant else "FAIL",
                    str(yaml_variant),
                    str(silver_variant),
                )
                # Check gene
                yaml_gene = finding.get("gene")
                silver_gene = row[4]
                if yaml_gene is not None:
                    result.add(
                        f"{report_name}/gene",
                        "PASS" if yaml_gene == silver_gene else "WARN",
                        str(yaml_gene),
                        str(silver_gene),
                    )
            else:
                result.add(
                    f"{report_name}/presence",
                    "FAIL",
                    "present",
                    "NOT FOUND",
                )

    # --- genetic_ancestry ---
    yaml_ancestry_comp = source.get("ancestry", {}).get("composition", {})
    # Haplogroups validated via genetic_traits table, not here
    # composition is a dict: {scandinavian: 84.4, other_european: 15.6}
    expected_ancestry = len(yaml_ancestry_comp)

    try:
        ancestry_rows = con.execute(
            """
            SELECT region, percentage
            FROM silver.genetic_ancestry
            ORDER BY region
        """
        ).fetchall()
    except duckdb.CatalogException:
        ancestry_rows = []
        result.add(
            "genetic_ancestry_table",
            "WARN",
            "silver.genetic_ancestry",
            "TABLE NOT FOUND",
            "Not yet created",
        )

    ancestry_count = len(ancestry_rows)
    not_ingested_ancestry = ancestry_count == 0

    result.add(
        "ancestry_count",
        "PASS" if ancestry_count >= expected_ancestry else "WARN",
        str(expected_ancestry),
        str(ancestry_count),
        "Not yet ingested" if not_ingested_ancestry else "",
    )

    # Check ancestry percentages sum to ~100%
    if ancestry_rows:
        pct_sum = sum(row[1] for row in ancestry_rows if row[1] is not None)
        result.add(
            "ancestry_pct_sum",
            "PASS" if 99.0 <= pct_sum <= 101.0 else "WARN",
            "~100%",
            f"{pct_sum:.1f}%",
        )

        # Per-region validation
        silver_ancestry = {_normalize_name(row[0]): row[1] for row in ancestry_rows}
        for region, pct in yaml_ancestry_comp.items():
            region_key = _normalize_name(region)
            if region_key in silver_ancestry:
                status = (
                    "PASS"
                    if _approx_equal(pct, silver_ancestry[region_key], tolerance=0.1)
                    else "FAIL"
                )
                result.add(
                    f"ancestry/{region}",
                    status,
                    str(pct),
                    str(silver_ancestry[region_key]),
                )
            else:
                result.add(
                    f"ancestry/{region}/presence",
                    "FAIL",
                    "present",
                    "NOT FOUND",
                )

    # --- genetic_traits (wellness) ---
    yaml_wellness = source.get("wellness", [])
    expected_traits = len(yaml_wellness)

    try:
        traits_rows = con.execute(
            """
            SELECT report_name, result_summary, variant_detected,
                   gene, clinical_relevance
            FROM silver.genetic_traits
            ORDER BY report_name
        """
        ).fetchall()
    except duckdb.CatalogException:
        traits_rows = []
        result.add(
            "genetic_traits_table",
            "WARN",
            "silver.genetic_traits",
            "TABLE NOT FOUND",
            "Not yet created",
        )

    traits_count = len(traits_rows)
    not_ingested_traits = traits_count == 0

    result.add(
        "traits_count",
        "PASS" if traits_count >= expected_traits else "WARN",
        str(expected_traits),
        str(traits_count),
        "Not yet ingested" if not_ingested_traits else "",
    )

    # Per-trait validation if data exists
    if traits_rows:
        silver_traits = {_normalize_name(row[0]): row for row in traits_rows}
        for trait in yaml_wellness:
            report_name = trait.get("report_name", "")
            name_key = _normalize_name(report_name)
            if name_key in silver_traits:
                row = silver_traits[name_key]
                # Check result_summary
                yaml_summary = trait.get("result_summary", "")
                silver_summary = row[1] if row[1] is not None else ""
                result.add(
                    f"{report_name}/result_summary",
                    "PASS" if yaml_summary == silver_summary else "WARN",
                    yaml_summary,
                    silver_summary,
                    "Text diff" if yaml_summary != silver_summary else "",
                )
            else:
                result.add(
                    f"{report_name}/presence",
                    "FAIL",
                    "present",
                    "NOT FOUND",
                )

    return result


def validate_dimension_tables(con: duckdb.DuckDBPyConnection) -> ValidationResult:
    """Validate dimension tables are seeded correctly."""
    result = ValidationResult("Dimension Tables")

    # dim_marker_catalog
    try:
        catalog_count = con.execute(
            "SELECT COUNT(*) FROM silver.dim_marker_catalog"
        ).fetchone()[0]
    except (duckdb.CatalogException, Exception):
        catalog_count = 0

    result.add(
        "dim_marker_catalog_rows",
        "PASS" if catalog_count >= 97 else "FAIL",
        ">=97",
        str(catalog_count),
    )

    # dim_reference_range
    try:
        range_count = con.execute(
            "SELECT COUNT(*) FROM silver.dim_reference_range"
        ).fetchone()[0]
    except (duckdb.CatalogException, Exception):
        range_count = 0

    result.add(
        "dim_reference_range_rows",
        "PASS" if range_count >= 96 else "FAIL",
        ">=96",
        str(range_count),
    )

    # log_scale markers
    try:
        log_count = con.execute(
            "SELECT COUNT(*) FROM silver.dim_marker_catalog WHERE is_log_scale = true"
        ).fetchone()[0]
    except (duckdb.CatalogException, Exception):
        log_count = 0

    result.add(
        "log_scale_markers",
        "PASS" if log_count >= 13 else "WARN",
        ">=13",
        str(log_count),
    )

    # body_system coverage
    try:
        systems = con.execute(
            "SELECT COUNT(DISTINCT body_system) FROM silver.dim_marker_catalog"
        ).fetchone()[0]
    except (duckdb.CatalogException, Exception):
        systems = 0

    result.add(
        "body_system_coverage",
        "PASS" if systems >= 5 else "WARN",
        ">=5",
        str(systems),
    )

    return result


def main() -> int:
    """Run all validations and report results."""
    print("=" * 60)
    print("Zero-Loss Data Validation")
    print("=" * 60)

    if not DB_PATH.exists():
        logger.error("Database not found: %s", DB_PATH)
        print(f"\n[FAIL] Database not found: {DB_PATH}")
        print("Run schema setup and ingestion first.")
        return 1

    con = duckdb.connect(str(DB_PATH), read_only=True)

    results = [
        validate_dimension_tables(con),
        validate_blood_panel(con),
        validate_microbiome(con),
        validate_genetics(con),
    ]

    con.close()

    total_pass = sum(r.pass_count for r in results)
    total_warn = sum(r.warn_count for r in results)
    total_fail = sum(r.fail_count for r in results)
    total = sum(r.total for r in results)

    for r in results:
        print(r.summary())
        logger.info(
            "%s: %d PASS, %d WARN, %d FAIL",
            r.source_name,
            r.pass_count,
            r.warn_count,
            r.fail_count,
        )

    print(f"\n{'=' * 60}")
    print(
        f"OVERALL: {total} checks -- {total_pass} PASS, {total_warn} WARN, {total_fail} FAIL"
    )
    print(f"{'=' * 60}")

    if total_fail > 0:
        print("\n[FAIL] VALIDATION FAILED -- data loss detected")
        logger.error("Validation FAILED: %d failures detected", total_fail)
        return 1
    elif total_warn > 0:
        print("\n[WARN] VALIDATION PASSED WITH WARNINGS -- review warnings above")
        logger.warning("Validation passed with %d warnings", total_warn)
        return 0
    else:
        print("\n[PASS] ALL VALIDATIONS PASSED -- zero data loss confirmed")
        logger.info("All validations passed -- zero data loss confirmed")
        return 0


if __name__ == "__main__":
    sys.exit(main())
