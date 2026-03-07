"""
import_manual_data.py — Import manually curated health data into the platform.

Reads YAML files from the data lake manual directory and loads them into
DuckDB (silver + agent schemas). Handles:
  - Lab results (blood panels, microbiome) -> silver.lab_results
  - Supplement protocol -> silver.supplement_log
  - Genetic profile -> agent.genetic_profile
  - Health graph updates (new nodes + edges)
  - Patient profile updates (genetic context)

Idempotent — safe to re-run. Uses MERGE for silver tables, DELETE+INSERT for agent tables.

Usage:
    python import_manual_data.py
    HEALTH_ENV=prd python import_manual_data.py
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from health_platform.utils.logging_config import get_logger
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.paths import get_db_path, get_lab_dir, get_manual_dir

logger = get_logger("import_manual_data")

MANUAL_DIR = get_manual_dir()
LAB_DIR = get_lab_dir()
GENETIC_FILE = MANUAL_DIR / "genetic_profile.yaml"
SUPPLEMENT_FILE = MANUAL_DIR / "supplement_protocol.yaml"


def md5_hash(*parts: str) -> str:
    """Compute MD5 hash from concatenated parts."""
    combined = "||".join(str(p) for p in parts)
    return hashlib.md5(combined.encode()).hexdigest()


def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    """Create schemas if they don't exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create tables if they don't exist."""
    from health_platform.utils.path_resolver import get_project_root

    setup_dir = get_project_root() / "health_platform" / "setup"
    schema_file = setup_dir / "create_lab_and_supplements_schema.sql"
    if schema_file.exists():
        sql = schema_file.read_text()
        for stmt in sql.split(";"):
            # Strip comment-only lines, keep SQL
            lines = [
                line
                for line in stmt.strip().splitlines()
                if not line.strip().startswith("--")
            ]
            cleaned = "\n".join(lines).strip()
            if cleaned:
                con.execute(stmt.strip())
        logger.info("Schema tables ensured.")


# =============================================================================
# Lab Results Import
# =============================================================================


def import_lab_results(con: duckdb.DuckDBPyConnection, audit: AuditLogger) -> int:
    """Import all lab result YAML files into silver.lab_results."""
    if not LAB_DIR.exists():
        logger.warning(f"Lab results directory not found: {LAB_DIR}")
        return 0

    yaml_files = sorted(LAB_DIR.glob("*.yaml"))
    if not yaml_files:
        logger.info("No lab result YAML files found.")
        return 0

    total_rows = 0

    for yaml_file in yaml_files:
        logger.info(f"Processing lab file: {yaml_file.name}")
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        meta = data["test_metadata"]
        test_id = meta["analysis_id"]
        test_date = meta["test_date"]
        test_type = meta["test_type"]
        test_name = meta.get("test_name")
        lab_name = meta.get("lab_name")
        lab_accreditation = meta.get("lab_accreditation")

        rows = []
        for marker in data["markers"]:
            bk_hash = md5_hash(test_id, marker["name"])
            rh_parts = [
                str(marker.get("value", "")),
                str(marker.get("value_text", "")),
                marker.get("unit", ""),
                str(marker.get("ref_min", "")),
                str(marker.get("ref_max", "")),
                marker.get("status", ""),
            ]
            row_hash = md5_hash(*rh_parts)

            rows.append(
                (
                    test_id,
                    test_date,
                    test_type,
                    test_name,
                    lab_name,
                    lab_accreditation,
                    marker["name"],
                    marker["category"],
                    marker.get("value"),
                    marker.get("value_text"),
                    marker.get("unit"),
                    marker.get("ref_min"),
                    marker.get("ref_max"),
                    marker.get("ref_direction"),
                    marker["status"],
                    bk_hash,
                    row_hash,
                )
            )

        if not rows:
            continue

        # Create staging table
        con.execute("DROP TABLE IF EXISTS silver.lab_results__staging")
        con.execute(
            """
            CREATE TABLE silver.lab_results__staging AS
            SELECT * FROM silver.lab_results WHERE false
        """
        )

        # Insert into staging
        con.executemany(
            """
            INSERT INTO silver.lab_results__staging (
                test_id, test_date, test_type, test_name, lab_name, lab_accreditation,
                marker_name, marker_category, value_numeric, value_text, unit,
                reference_min, reference_max, reference_direction, status,
                business_key_hash, row_hash, load_datetime, update_datetime
            ) VALUES (?, ?::DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
            rows,
        )

        # MERGE into target
        con.execute(
            """
            MERGE INTO silver.lab_results AS target
            USING silver.lab_results__staging AS src
            ON target.business_key_hash = src.business_key_hash

            WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
                test_date = src.test_date,
                test_type = src.test_type,
                test_name = src.test_name,
                lab_name = src.lab_name,
                lab_accreditation = src.lab_accreditation,
                marker_name = src.marker_name,
                marker_category = src.marker_category,
                value_numeric = src.value_numeric,
                value_text = src.value_text,
                unit = src.unit,
                reference_min = src.reference_min,
                reference_max = src.reference_max,
                reference_direction = src.reference_direction,
                status = src.status,
                row_hash = src.row_hash,
                update_datetime = CURRENT_TIMESTAMP

            WHEN NOT MATCHED THEN INSERT (
                test_id, test_date, test_type, test_name, lab_name, lab_accreditation,
                marker_name, marker_category, value_numeric, value_text, unit,
                reference_min, reference_max, reference_direction, status,
                business_key_hash, row_hash, load_datetime, update_datetime
            ) VALUES (
                src.test_id, src.test_date, src.test_type, src.test_name,
                src.lab_name, src.lab_accreditation,
                src.marker_name, src.marker_category, src.value_numeric, src.value_text,
                src.unit, src.reference_min, src.reference_max, src.reference_direction,
                src.status, src.business_key_hash, src.row_hash,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
        """
        )

        con.execute("DROP TABLE IF EXISTS silver.lab_results__staging")

        total_rows += len(rows)
        logger.info(f"  Merged {len(rows)} markers from {yaml_file.name}")
        audit.log_table("silver.lab_results", "MERGE", rows_after=len(rows))

    return total_rows


# =============================================================================
# Supplement Protocol Import
# =============================================================================


def import_supplements(con: duckdb.DuckDBPyConnection, audit: AuditLogger) -> int:
    """Import supplement protocol into silver.supplement_log."""
    if not SUPPLEMENT_FILE.exists():
        logger.warning(f"Supplement file not found: {SUPPLEMENT_FILE}")
        return 0

    with open(SUPPLEMENT_FILE) as f:
        data = yaml.safe_load(f)

    supplements = data.get("supplements", [])
    if not supplements:
        logger.info("No supplements found.")
        return 0

    rows = []
    for s in supplements:
        bk_hash = md5_hash(s["name"])
        rh_parts = [
            str(s.get("dose", "")),
            s.get("unit", ""),
            s.get("frequency", ""),
            s.get("timing", ""),
            s.get("product", ""),
            str(s.get("start_date", "")),
            str(s.get("end_date", "")),
            s.get("status", ""),
        ]
        row_hash = md5_hash(*rh_parts)

        rows.append(
            (
                s["name"],
                s.get("dose"),
                s.get("unit"),
                s.get("frequency"),
                s.get("timing"),
                s.get("product"),
                s.get("start_date"),
                s.get("end_date"),
                s["status"],
                s.get("target"),
                s.get("notes"),
                bk_hash,
                row_hash,
            )
        )

    # Create staging
    con.execute("DROP TABLE IF EXISTS silver.supplement_log__staging")
    con.execute(
        """
        CREATE TABLE silver.supplement_log__staging AS
        SELECT * FROM silver.supplement_log WHERE false
    """
    )

    con.executemany(
        """
        INSERT INTO silver.supplement_log__staging (
            supplement_name, dose, unit, frequency, timing, product,
            start_date, end_date, status, target, notes,
            business_key_hash, row_hash, load_datetime, update_datetime
        ) VALUES (?, ?, ?, ?, ?, ?, ?::DATE, ?::DATE, ?, ?, ?, ?, ?,
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """,
        rows,
    )

    # MERGE
    con.execute(
        """
        MERGE INTO silver.supplement_log AS target
        USING silver.supplement_log__staging AS src
        ON target.business_key_hash = src.business_key_hash

        WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
            dose = src.dose,
            unit = src.unit,
            frequency = src.frequency,
            timing = src.timing,
            product = src.product,
            start_date = src.start_date,
            end_date = src.end_date,
            status = src.status,
            target = src.target,
            notes = src.notes,
            row_hash = src.row_hash,
            update_datetime = CURRENT_TIMESTAMP

        WHEN NOT MATCHED THEN INSERT (
            supplement_name, dose, unit, frequency, timing, product,
            start_date, end_date, status, target, notes,
            business_key_hash, row_hash, load_datetime, update_datetime
        ) VALUES (
            src.supplement_name, src.dose, src.unit, src.frequency, src.timing,
            src.product, src.start_date, src.end_date, src.status, src.target,
            src.notes, src.business_key_hash, src.row_hash,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
    """
    )

    con.execute("DROP TABLE IF EXISTS silver.supplement_log__staging")
    logger.info(f"Merged {len(rows)} supplements.")
    audit.log_table("silver.supplement_log", "MERGE", rows_after=len(rows))
    return len(rows)


# =============================================================================
# Genetic Profile Import
# =============================================================================


def import_genetic_profile(con: duckdb.DuckDBPyConnection, audit: AuditLogger) -> int:
    """Import genetic profile into agent.genetic_profile."""
    if not GENETIC_FILE.exists():
        logger.warning(f"Genetic profile not found: {GENETIC_FILE}")
        return 0

    with open(GENETIC_FILE) as f:
        data = yaml.safe_load(f)

    # Collect all findings from all categories
    findings = []
    for section_key in ["health_predispositions", "carrier_status", "wellness"]:
        for item in data.get(section_key, []):
            findings.append(
                (
                    item["category"],
                    item["report_name"],
                    item["result_summary"],
                    item.get("variant_detected"),
                    item.get("gene"),
                    item.get("snp_id"),
                    item.get("genotype"),
                    item["clinical_relevance"],
                    item.get("platform_relevance"),
                    item.get("related_metrics"),
                )
            )

    if not findings:
        return 0

    # Delete and re-insert (static data, simple approach)
    con.execute("DELETE FROM agent.genetic_profile")
    con.executemany(
        """
        INSERT INTO agent.genetic_profile (
            category, report_name, result_summary, variant_detected,
            gene, snp_id, genotype, clinical_relevance,
            platform_relevance, related_metrics, load_datetime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """,
        findings,
    )

    logger.info(f"Loaded {len(findings)} genetic findings.")
    audit.log_table("agent.genetic_profile", "DELETE_INSERT", rows_after=len(findings))
    return len(findings)


# =============================================================================
# Patient Profile Updates
# =============================================================================


def update_patient_profile(con: duckdb.DuckDBPyConnection) -> None:
    """Build patient profile entries from imported genetic and lab data.

    Reads from agent.genetic_profile and silver.lab_results — no hardcoded
    health values. Profile entries are derived from whatever data was imported.
    """
    count = 0

    # --- Genetic findings → profile entries ---
    try:
        genetic_rows = con.execute(
            """
            SELECT report_name, result_summary, platform_relevance
            FROM agent.genetic_profile
            WHERE clinical_relevance IN ('high', 'medium')
            ORDER BY clinical_relevance, category
        """
        ).fetchall()

        for report_name, result_summary, platform_relevance in genetic_rows:
            key = (
                "genetic_"
                + report_name.lower().replace(" ", "_").replace("-", "_")[:40]
            )
            description = platform_relevance or report_name
            _upsert_profile(
                con,
                key,
                result_summary,
                None,
                "genetics",
                description,
                "23andMe",
                "static",
            )
            count += 1
    except Exception as exc:
        logger.warning("Could not read genetic profile for patient_profile: %s", exc)

    # --- Out-of-range lab markers → profile entries ---
    try:
        lab_rows = con.execute(
            """
            SELECT marker_name, value_numeric, value_text, unit,
                   reference_min, reference_max, status, test_date, test_name
            FROM silver.lab_results
            WHERE status IN ('below_range', 'above_range', 'critical')
            ORDER BY test_date DESC, marker_name
        """
        ).fetchall()

        for row in lab_rows:
            (
                marker,
                val_num,
                val_text,
                unit,
                ref_min,
                ref_max,
                status,
                test_date,
                test_name,
            ) = row
            key = "lab_" + marker.lower().replace(" ", "_").replace("-", "_")[:40]
            value_str = f"{val_num} {unit}" if val_num is not None else (val_text or "")
            ref_str = ""
            if ref_min is not None and ref_max is not None:
                ref_str = f" (ref {ref_min}-{ref_max})"
            elif ref_min is not None:
                ref_str = f" (ref >{ref_min})"
            elif ref_max is not None:
                ref_str = f" (ref <{ref_max})"
            display = f"{value_str}{ref_str} — {status}"
            description = f"{marker} from {test_name or 'lab test'}"
            _upsert_profile(
                con,
                key,
                display,
                val_num,
                "lab_findings",
                description,
                "lab test",
                "periodic",
            )
            count += 1
    except Exception as exc:
        logger.warning("Could not read lab results for patient_profile: %s", exc)

    logger.info("Updated %d patient profile entries from imported data.", count)


def _upsert_profile(
    con: duckdb.DuckDBPyConnection,
    key: str,
    value: str,
    numeric: float | None,
    category: str,
    description: str,
    computed_from: str,
    frequency: str,
) -> None:
    """Delete + insert a patient_profile entry (DuckDB ON CONFLICT workaround)."""
    con.execute("DELETE FROM agent.patient_profile WHERE profile_key = ?", [key])
    con.execute(
        """
        INSERT INTO agent.patient_profile (
            profile_key, profile_value, numeric_value, category,
            description, computed_from, update_frequency
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        [key, value, numeric, category, description, computed_from, frequency],
    )


# =============================================================================
# Health Graph Updates
# =============================================================================


def update_health_graph(con: duckdb.DuckDBPyConnection) -> None:
    """Add lab, genetic, and supplement nodes + edges to the health graph.

    Descriptions are medical knowledge (reference ranges, mechanisms) — NOT
    patient-specific values. Current values live in the data tables and are
    queried at runtime by the AI agent.
    """
    # --- Nodes: medical knowledge, no patient values ---
    nodes = [
        # Lab biomarkers
        (
            "biomarker:pancreatic_elastase",
            "biomarker",
            "Pancreatic Elastase",
            "Exocrine pancreas function marker. Normal >200 ug/g. Below 100 = severe insufficiency.",
            "lab_results",
            "value_numeric",
        ),
        (
            "biomarker:secretory_iga",
            "biomarker",
            "Secretory IgA",
            "Mucosal immune defense marker. Normal 510-2040 ug/ml. First-line gut immune defense.",
            "lab_results",
            "value_numeric",
        ),
        (
            "biomarker:epa",
            "biomarker",
            "EPA (Eicosapentaenoic Acid)",
            "Omega-3 fatty acid. Anti-inflammatory. Normal >2%. Affected by fat absorption capacity.",
            "lab_results",
            "value_numeric",
        ),
        (
            "biomarker:vitamin_d3_level",
            "biomarker",
            "Vitamin D3 25-OH Level",
            "Serum vitamin D status. Optimal 40-60 ng/ml. Fat-soluble — affected by malabsorption.",
            "lab_results",
            "value_numeric",
        ),
        (
            "biomarker:choline_level",
            "biomarker",
            "Choline",
            "Essential nutrient for liver, brain, cell membranes. Normal 22-97.5 umol/l.",
            "lab_results",
            "value_numeric",
        ),
        (
            "biomarker:fat_residue",
            "biomarker",
            "Fecal Fat Residue",
            "Undigested fat in stool. Normal <3.5 g/100g. Elevated values confirm fat malabsorption.",
            "lab_results",
            "value_numeric",
        ),
        (
            "biomarker:sugar_residue",
            "biomarker",
            "Fecal Sugar Residue",
            "Undigested sugar in stool. Normal <2.5 g/100g. Elevated values indicate carbohydrate malabsorption.",
            "lab_results",
            "value_numeric",
        ),
        # Genetic nodes
        (
            "genetic:alpha1_piz",
            "genetic",
            "Alpha-1 Antitrypsin PI*Z",
            "SERPINA1 variant. MZ phenotype has mildly reduced AAT. Risk modifier for liver/lung disease.",
            None,
            None,
        ),
        (
            "genetic:actn3_power",
            "genetic",
            "ACTN3 Power Athlete",
            "ACTN3 gene variant affecting muscle fiber composition. Influences optimal training type.",
            None,
            None,
        ),
        (
            "genetic:deep_sleep_predisposition",
            "genetic",
            "Deep Sleep Predisposition",
            "Genetic factor affecting deep sleep duration. Lower deep sleep may be a normal baseline.",
            None,
            None,
        ),
        # Condition nodes
        (
            "condition:fat_malabsorption",
            "condition",
            "Fat Malabsorption",
            "Impaired fat digestion due to pancreatic enzyme insufficiency. Affects EPA, fat-soluble vitamins.",
            "lab_results",
            "value_numeric",
        ),
        (
            "condition:dysbiosis",
            "condition",
            "Gut Dysbiosis",
            "Imbalanced gut microbiome caused by undigested food reaching colon due to enzyme insufficiency.",
            "lab_results",
            "value_numeric",
        ),
    ]

    # --- Supplement/intervention nodes from database ---
    try:
        supp_rows = con.execute(
            """
            SELECT supplement_name, dose, unit, target
            FROM silver.supplement_log
            WHERE status IN ('active', 'planned', 'awaiting_rx')
        """
        ).fetchall()
        for name, dose, unit, target in supp_rows:
            node_id = "intervention:" + name.lower().replace(" ", "_").replace("-", "_")
            dose_str = f"{dose} {unit}" if dose and unit else ""
            desc = f"Supplement targeting {target}." if target else "Active supplement."
            if dose_str:
                desc = f"{dose_str}. {desc}"
            nodes.append(
                (node_id, "supplement", name, desc, "supplement_log", "supplement_name")
            )
    except Exception as exc:
        logger.warning("Could not read supplements for health graph: %s", exc)

    for node in nodes:
        con.execute(
            """
            INSERT INTO agent.health_graph (node_id, node_type, node_label, description, related_tables, related_columns)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (node_id) DO UPDATE SET
                description = EXCLUDED.description,
                related_tables = EXCLUDED.related_tables,
                related_columns = EXCLUDED.related_columns
        """,
            node,
        )

    # --- Edges: medical relationships, no patient values ---
    edges = [
        # Causal chain: elastase → malabsorption → downstream
        (
            "biomarker:pancreatic_elastase",
            "condition:fat_malabsorption",
            "causes",
            0.95,
            "clinical_observation",
            "Low elastase directly causes impaired fat digestion and elevated fecal fat.",
        ),
        (
            "condition:fat_malabsorption",
            "biomarker:epa",
            "worsens",
            0.85,
            "clinical_observation",
            "Fat malabsorption reduces EPA absorption from dietary sources.",
        ),
        (
            "condition:fat_malabsorption",
            "biomarker:vitamin_d3_level",
            "worsens",
            0.80,
            "clinical_observation",
            "Vitamin D3 is fat-soluble. Malabsorption impairs D3 uptake.",
        ),
        (
            "biomarker:pancreatic_elastase",
            "condition:dysbiosis",
            "causes",
            0.85,
            "clinical_observation",
            "Low elastase causes undigested food to reach colon, feeding opportunistic bacteria.",
        ),
        (
            "condition:dysbiosis",
            "biomarker:secretory_iga",
            "worsens",
            0.80,
            "clinical_observation",
            "Dysbiotic microbiome weakens mucosal immune defense and sIgA production.",
        ),
        (
            "biomarker:pancreatic_elastase",
            "biomarker:fat_residue",
            "correlates_with",
            0.90,
            "mechanistic",
            "Low elastase directly causes elevated fecal fat. Both confirm exocrine insufficiency.",
        ),
        (
            "biomarker:pancreatic_elastase",
            "biomarker:sugar_residue",
            "correlates_with",
            0.80,
            "mechanistic",
            "Pancreatic amylase co-secreted with elastase. Low elastase indicates reduced enzyme output.",
        ),
        # Supplement → target edges
        (
            "intervention:omega3_supplement",
            "biomarker:epa",
            "improves",
            0.75,
            "clinical_trial",
            "EPA-rich fish oil directly increases EPA levels. Expected normalization in 8-12 weeks.",
        ),
        (
            "intervention:vitamin_d3_supplement",
            "biomarker:vitamin_d3_level",
            "improves",
            0.70,
            "clinical_trial",
            "D3 supplementation raises serum 25-OH D3. Absorption limited by fat malabsorption.",
        ),
        (
            "intervention:digestive_enzymes",
            "biomarker:pancreatic_elastase",
            "improves",
            0.60,
            "clinical_trial",
            "Exogenous enzymes compensate for endogenous insufficiency. Restores digestive function.",
        ),
        (
            "intervention:digestive_enzymes",
            "condition:fat_malabsorption",
            "improves",
            0.80,
            "clinical_trial",
            "Enzyme replacement (lipase, protease, amylase) restores fat and nutrient absorption.",
        ),
        (
            "intervention:l_glutamine",
            "biomarker:secretory_iga",
            "improves",
            0.55,
            "clinical_trial",
            "L-glutamine supports enterocyte metabolism and may improve mucosal immune function.",
        ),
        # Genetic context edges
        (
            "genetic:alpha1_piz",
            "condition:inflammation",
            "increases_risk_of",
            0.40,
            "meta_analysis",
            "PI*Z carrier has mildly reduced AAT. Risk elevated with alcohol, smoking, or metabolic syndrome.",
        ),
        (
            "genetic:deep_sleep_predisposition",
            "biomarker:deep_sleep",
            "correlates_with",
            0.70,
            "observational_study",
            "Genetic predisposition for less deep sleep. Lower scores may reflect genetics, not pathology.",
        ),
        (
            "genetic:actn3_power",
            "activity:strength_training",
            "improves",
            0.60,
            "meta_analysis",
            "ACTN3 variant favors fast-twitch muscle. May respond better to power/strength training.",
        ),
        # Cross-domain
        (
            "condition:fat_malabsorption",
            "concept:energy_balance",
            "worsens",
            0.50,
            "mechanistic",
            "Fat malabsorption reduces caloric absorption efficiency. May mask true energy intake.",
        ),
        (
            "condition:dysbiosis",
            "concept:gut_brain_axis",
            "worsens",
            0.65,
            "meta_analysis",
            "Depleted beneficial bacteria impairs gut-brain communication. May affect mood and stress.",
        ),
        (
            "biomarker:secretory_iga",
            "condition:inflammation",
            "prevents",
            0.70,
            "mechanistic",
            "sIgA is the first-line mucosal immune defense. Low sIgA increases inflammation vulnerability.",
        ),
        (
            "intervention:omega3_supplement",
            "condition:inflammation",
            "improves",
            0.70,
            "meta_analysis",
            "EPA/DHA are precursors to anti-inflammatory resolvins.",
        ),
    ]

    for edge in edges:
        con.execute(
            """
            INSERT INTO agent.health_graph_edges (
                source_node_id, target_node_id, edge_type, weight, evidence, description
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (source_node_id, target_node_id, edge_type) DO UPDATE SET
                weight = EXCLUDED.weight,
                evidence = EXCLUDED.evidence,
                description = EXCLUDED.description
        """,
            edge,
        )

    logger.info("Updated health graph: %d nodes, %d edges.", len(nodes), len(edges))


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    db_path = str(get_db_path())
    logger.info(f"Database: {db_path}")

    con = duckdb.connect(db_path)

    try:
        ensure_schemas(con)
        ensure_tables(con)

        with AuditLogger("import_manual_data", "silver", "manual") as audit:
            lab_count = import_lab_results(con, audit)
            supp_count = import_supplements(con, audit)
            gen_count = import_genetic_profile(con, audit)
            update_patient_profile(con)
            update_health_graph(con)

            audit.finish(
                rows_processed=lab_count + supp_count + gen_count,
                rows_inserted=lab_count + supp_count + gen_count,
            )

        logger.info(
            f"Import complete: {lab_count} lab markers, "
            f"{supp_count} supplements, {gen_count} genetic findings."
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
