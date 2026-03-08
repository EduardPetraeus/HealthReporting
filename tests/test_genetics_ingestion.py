"""Tests for 23andMe genetics ingestion pipeline.

Covers:
- GeneticsPdfParser: format detection, health/haplogroup parsing
- parse_ancestry_csv: CSV parsing, segment_length computation
- parse_family_tree_json: JSON flattening, side detection
- ingest_genetics_data: orchestration, parquet output
- query_genetics: MCP tool with in-memory DuckDB
"""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
from health_platform.source_connectors.genetics.csv_parser import (
    parse_ancestry_csv,
    parse_family_tree_json,
)
from health_platform.source_connectors.genetics.pdf_parser import GeneticsPdfParser

# ====================================================================
# TestGeneticsPdfParser
# ====================================================================


class TestGeneticsPdfParser:
    """Tests for GeneticsPdfParser format detection and parsing."""

    def setup_method(self):
        self.parser = GeneticsPdfParser()

    # -- Format detection --

    def test_detect_format_health_risk_alzheimer(self):
        text = "Late-Onset Alzheimer's Disease 23andMe variant APOE"
        assert self.parser.detect_format(text) == "23andme_health_risk"

    def test_detect_format_health_risk_parkinson(self):
        text = "Parkinson's Disease 23andMe LRRK2"
        assert self.parser.detect_format(text) == "23andme_health_risk"

    def test_detect_format_carrier_status_hemochromatosis(self):
        text = "Hereditary Hemochromatosis HFE 23andMe"
        assert self.parser.detect_format(text) == "23andme_carrier_status"

    def test_detect_format_carrier_status_thrombophilia(self):
        text = "Hereditary Thrombophilia Factor V 23andMe"
        assert self.parser.detect_format(text) == "23andme_carrier_status"

    def test_detect_format_wellness_muscle(self):
        text = "Muscle Composition ACTN3 23andMe"
        assert self.parser.detect_format(text) == "23andme_wellness"

    def test_detect_format_haplogroup_maternal(self):
        text = "Maternal Haplogroup mtDNA 23andMe"
        assert self.parser.detect_format(text) == "23andme_haplogroup"

    def test_detect_format_haplogroup_paternal(self):
        text = "Paternal Haplogroup Y-DNA 23andMe"
        assert self.parser.detect_format(text) == "23andme_haplogroup"

    def test_detect_format_skip_neanderthal(self):
        text = "Neanderthal Ancestry 23andMe"
        assert self.parser.detect_format(text) == "skip"

    def test_detect_format_skip_reports_summary(self):
        text = "Reports Summary 23andMe overview"
        assert self.parser.detect_format(text) == "skip"

    def test_detect_format_unknown(self):
        text = "Some random text without keywords"
        assert self.parser.detect_format(text) == "unknown"

    # -- Result summary extraction --

    def test_extract_result_summary_no_variant(self):
        text = "Claus Eduard, you do not have the e4 variant we tested."
        result = GeneticsPdfParser._extract_result_summary(text)
        assert "do not have" in result.lower()

    def test_extract_result_summary_variant_count(self):
        text = "Blah blah\n0 variants detected in the APOE gene"
        result = GeneticsPdfParser._extract_result_summary(text)
        assert "0" in result

    def test_extract_result_summary_muscle(self):
        text = "Claus Eduard, your genetic muscle composition is common in elite power athletes"
        result = GeneticsPdfParser._extract_result_summary(text)
        assert "muscle" in result.lower() or "power" in result.lower()

    # -- Variant detection --

    def test_detect_variant_zero_detected(self):
        text = "0 variants detected in the APOE gene"
        assert GeneticsPdfParser._detect_variant(text) is False

    def test_detect_variant_do_not_have(self):
        text = "you do not have the variant we tested"
        assert GeneticsPdfParser._detect_variant(text) is False

    def test_detect_variant_one_detected(self):
        text = "1 variant detected in the HFE gene"
        assert GeneticsPdfParser._detect_variant(text) is True

    def test_detect_variant_you_have_copy(self):
        text = "You have one copy of the C variant and one copy of the T variant"
        assert GeneticsPdfParser._detect_variant(text) is True

    # -- Gene extraction --

    def test_extract_gene_from_text(self):
        text = "The variant in the APOE gene is associated with"
        assert GeneticsPdfParser._extract_gene(text) == "APOE"

    def test_extract_gene_actn3(self):
        text = "A genetic marker in the ACTN3 gene controls"
        assert GeneticsPdfParser._extract_gene(text) == "ACTN3"

    def test_extract_gene_none(self):
        text = "Some text without gene mention"
        assert GeneticsPdfParser._extract_gene(text) is None

    # -- SNP extraction --

    def test_extract_snp_id(self):
        text = "Marker ID (SNP) rs429358 Your Genotype"
        assert GeneticsPdfParser._extract_snp_id(text) == "rs429358"

    def test_extract_snp_id_none(self):
        text = "No SNP identifiers here"
        assert GeneticsPdfParser._extract_snp_id(text) is None

    # -- Genotype extraction --

    def test_extract_genotype_two_alleles(self):
        text = "rs1815739 C T T"
        assert GeneticsPdfParser._extract_genotype(text) == "CT"

    def test_extract_genotype_descriptive(self):
        text = "You have one copy of the C variant and one copy of the T variant."
        assert GeneticsPdfParser._extract_genotype(text) == "CT"

    # -- Haplogroup extraction --

    def test_extract_haplogroup_maternal(self):
        text = "your maternal haplogroup is V."
        assert GeneticsPdfParser._extract_haplogroup(text) == "V"

    def test_extract_haplogroup_paternal(self):
        text = "your paternal haplogroup is R1b."
        assert GeneticsPdfParser._extract_haplogroup(text) == "R1b"

    # -- Report name extraction --

    def test_extract_report_name_strips_prefix(self):
        path = Path(
            "Claus Eduard Petræus Late-Onset Alzheimer's Disease Report - 23andMe.pdf"
        )
        result = GeneticsPdfParser._extract_report_name(path)
        assert result == "Late-Onset Alzheimer's Disease"

    def test_extract_report_name_clean(self):
        path = Path("Muscle Composition Report - 23andMe.pdf")
        result = GeneticsPdfParser._extract_report_name(path)
        assert result == "Muscle Composition"

    # -- Full PDF parse with mock --

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_pdf_health_report(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Claus Eduard Petræus\n"
            "Late-Onset Alzheimer's Disease\n"
            "Claus Eduard, you do not have the e4 variant we tested.\n"
            "0 variants detected in the APOE gene\n"
            "Marker Tested Gene Marker ID (SNP) Your Genotype\n"
            "e4 APOE rs429358 T T C\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        path = Path(
            "Claus Eduard Petræus Late-Onset Alzheimer's Disease Report - 23andMe.pdf"
        )
        results = self.parser.parse_pdf(path)

        assert len(results) == 1
        r = results[0]
        assert r["category"] == "health_risk"
        assert r["gene"] == "APOE"
        assert r["snp_id"] == "rs429358"
        assert r["variant_detected"] is False
        assert r["risk_level"] == "low"

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_pdf_haplogroup(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Claus Eduard Petræus\n"
            "Maternal Haplogroup\n"
            "Claus Eduard, your maternal haplogroup is V.\n"
            "Migrations of Your Maternal Line\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        path = Path("Claus Eduard Petræus Maternal Haplogroup Report - 23andMe.pdf")
        results = self.parser.parse_pdf(path)

        assert len(results) == 1
        r = results[0]
        assert r["category"] == "haplogroup"
        assert r["genotype"] == "V"
        assert r["gene"] == "mtDNA"
        assert r["variant_detected"] is False

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_pdf_skip_neanderthal(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Neanderthal Ancestry 23andMe"
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Neanderthal - 23andMe.pdf"))
        assert len(results) == 0


# ====================================================================
# TestAncestryParser
# ====================================================================


class TestAncestryParser:
    """Tests for parse_ancestry_csv."""

    def test_parse_basic_csv(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Ancestry", "Copy", "Chromosome", "Start Point", "End Point"]
            )
            writer.writerow(["Scandinavian", "1", "chr1", "100000", "200000"])
            writer.writerow(["British & Irish", "2", "chr2", "50000", "150000"])
            path = Path(f.name)

        rows = parse_ancestry_csv(path)
        assert len(rows) == 2

        assert rows[0]["ancestry_category"] == "Scandinavian"
        assert rows[0]["copy"] == 1
        assert rows[0]["chromosome"] == "chr1"
        assert rows[0]["start_bp"] == 100000
        assert rows[0]["end_bp"] == 200000
        assert rows[0]["segment_length_bp"] == 100000

    def test_segment_length_computation(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Ancestry", "Copy", "Chromosome", "Start Point", "End Point"]
            )
            writer.writerow(["European", "1", "chr1", "1000", "250000000"])
            path = Path(f.name)

        rows = parse_ancestry_csv(path)
        assert rows[0]["segment_length_bp"] == 249999000

    def test_empty_rows_skipped(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Ancestry", "Copy", "Chromosome", "Start Point", "End Point"]
            )
            writer.writerow(["Scandinavian", "1", "chr1", "100", "200"])
            writer.writerow(["", "", "", "", ""])
            path = Path(f.name)

        rows = parse_ancestry_csv(path)
        assert len(rows) == 1

    def test_x_chromosome_parsed(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Ancestry", "Copy", "Chromosome", "Start Point", "End Point"]
            )
            writer.writerow(["British & Irish", "1", "chrX", "2700157", "154854338"])
            path = Path(f.name)

        rows = parse_ancestry_csv(path)
        assert rows[0]["chromosome"] == "chrX"
        assert rows[0]["segment_length_bp"] == 154854338 - 2700157


# ====================================================================
# TestFamilyTreeParser
# ====================================================================


class TestFamilyTreeParser:
    """Tests for parse_family_tree_json."""

    def _make_tree_json(self, nodes: list[dict]) -> Path:
        data = {
            "trees": [{"id": "test", "nodes": nodes}],
            "health_trees": [],
            "annotations": [],
            "sharing": {},
        }
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(data, f)
        f.close()
        return Path(f.name)

    def test_parse_basic_tree(self):
        nodes = [
            {
                "id": "user1",
                "parent_ids": [],
                "partner_ids": [],
                "sex": None,
                "first_name": "Claus",
                "last_name": "Petraeus",
                "relationship_id": "you",
                "up": 0,
                "down": 0,
                "num_shared": 2,
                "verified": False,
                "edited": False,
                "added_by_user": False,
                "url": "https://example.com/profile/1",
            },
            {
                "id": "father1",
                "parent_ids": ["gf1", "gf2"],
                "partner_ids": [],
                "sex": None,
                "first_name": None,
                "last_name": None,
                "relationship_id": "father",
                "up": 1,
                "down": 0,
                "num_shared": 1,
                "verified": False,
                "edited": False,
                "added_by_user": False,
                "url": None,
            },
        ]
        path = self._make_tree_json(nodes)
        rows = parse_family_tree_json(path)

        assert len(rows) == 2

        user = next(r for r in rows if r["relationship_to_user"] == "you")
        assert user["first_name"] == "Claus"
        assert user["generation"] == 0
        assert user["num_shared_segments"] == 2
        assert user["has_dna_match"] is True

        father = next(r for r in rows if r["relationship_to_user"] == "father")
        assert father["generation"] == -1
        assert father["num_shared_segments"] == 1

    def test_side_detection_user_is_both(self):
        nodes = [
            {
                "id": "u",
                "parent_ids": [],
                "partner_ids": [],
                "sex": None,
                "first_name": "Test",
                "last_name": "User",
                "relationship_id": "you",
                "up": 0,
                "down": 0,
                "num_shared": 2,
                "verified": False,
                "edited": False,
                "added_by_user": False,
                "url": None,
            }
        ]
        path = self._make_tree_json(nodes)
        rows = parse_family_tree_json(path)
        assert rows[0]["side"] == "both"

    def test_empty_tree(self):
        data = {"trees": [], "health_trees": [], "annotations": [], "sharing": {}}
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(data, f)
        f.close()
        rows = parse_family_tree_json(Path(f.name))
        assert len(rows) == 0

    def test_generation_computation_collateral(self):
        """Collateral relatives (cousins) have both up and down."""
        nodes = [
            {
                "id": "cousin1",
                "parent_ids": [],
                "partner_ids": [],
                "sex": None,
                "first_name": "M",
                "last_name": "M",
                "relationship_id": "half_second_cousin_once_removed",
                "up": 4,
                "down": 3,
                "num_shared": 1,
                "verified": False,
                "edited": False,
                "added_by_user": False,
                "url": None,
            }
        ]
        path = self._make_tree_json(nodes)
        rows = parse_family_tree_json(path)
        # generation = down - up = 3 - 4 = -1
        assert rows[0]["generation"] == -1


# ====================================================================
# TestGeneticsIngestion
# ====================================================================


class TestGeneticsIngestion:
    """Tests for the ingest_genetics_data orchestrator."""

    @patch(
        "health_platform.source_connectors.genetics.genetics_ingestion.GeneticsPdfParser"
    )
    def test_ingestion_writes_parquet(self, mock_parser_class):
        """Test that ingestion produces parquet files."""
        mock_parser = MagicMock()
        mock_parser.parse_pdf.return_value = [
            {
                "report_name": "Test Report",
                "category": "health_risk",
                "condition": "Test Condition",
                "gene": "TEST",
                "snp_id": "rs12345",
                "genotype": "AG",
                "risk_level": "low",
                "result_summary": "No variants detected",
                "variant_detected": False,
            }
        ]
        mock_parser_class.return_value = mock_parser

        with (
            tempfile.TemporaryDirectory() as input_dir,
            tempfile.TemporaryDirectory() as output_dir,
        ):
            input_path = Path(input_dir)
            output_path = Path(output_dir)

            # Create a fake PDF file
            (input_path / "Test Report - 23andMe.pdf").touch()

            # Create ancestry CSV
            csv_path = input_path / "ancestry_composition.csv"
            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["Ancestry", "Copy", "Chromosome", "Start Point", "End Point"]
                )
                writer.writerow(["European", "1", "chr1", "1000", "200000"])

            # Create family tree JSON
            json_path = input_path / "family_tree.json"
            with open(json_path, "w") as f:
                json.dump(
                    {
                        "trees": [
                            {
                                "id": "t1",
                                "nodes": [
                                    {
                                        "id": "u1",
                                        "parent_ids": [],
                                        "partner_ids": [],
                                        "sex": None,
                                        "first_name": "Test",
                                        "last_name": "User",
                                        "relationship_id": "you",
                                        "up": 0,
                                        "down": 0,
                                        "num_shared": 2,
                                        "verified": False,
                                        "edited": False,
                                        "added_by_user": False,
                                        "url": None,
                                    }
                                ],
                            }
                        ],
                        "health_trees": [],
                        "annotations": [],
                        "sharing": {},
                    },
                    f,
                )

            from health_platform.source_connectors.genetics.genetics_ingestion import (
                ingest_genetics_data,
            )

            outputs = ingest_genetics_data(input_path, output_path)

            assert "stg_23andme_health_reports" in outputs
            assert "stg_23andme_ancestry" in outputs
            assert "stg_23andme_family_tree" in outputs
            assert outputs["stg_23andme_health_reports"].exists()
            assert outputs["stg_23andme_ancestry"].exists()
            assert outputs["stg_23andme_family_tree"].exists()


# ====================================================================
# TestQueryGeneticsTool
# ====================================================================


class TestQueryGeneticsTool:
    """Tests for the query_genetics MCP tool with in-memory DuckDB."""

    def setup_method(self):
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS agent")

        # Create silver tables
        self.con.execute(
            """
            CREATE TABLE silver.genetic_health_findings (
                report_name VARCHAR, category VARCHAR, condition VARCHAR,
                gene VARCHAR, snp_id VARCHAR, genotype VARCHAR,
                risk_level VARCHAR, result_summary VARCHAR,
                variant_detected BOOLEAN, source_file VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        self.con.execute(
            """
            CREATE TABLE silver.ancestry_segments (
                ancestry_category VARCHAR, copy INTEGER, chromosome VARCHAR,
                start_bp BIGINT, end_bp BIGINT, segment_length_bp BIGINT,
                pct_of_chromosome DOUBLE, source_file VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        self.con.execute(
            """
            CREATE TABLE silver.family_tree (
                person_id VARCHAR, first_name VARCHAR, last_name VARCHAR,
                relationship_to_user VARCHAR, generation INTEGER,
                side VARCHAR, num_shared_segments INTEGER,
                has_dna_match BOOLEAN, profile_url VARCHAR,
                source_file VARCHAR, business_key_hash VARCHAR,
                row_hash VARCHAR, load_datetime TIMESTAMP,
                update_datetime TIMESTAMP
            )
        """
        )

        # Insert test data
        self.con.execute(
            """
            INSERT INTO silver.genetic_health_findings VALUES
            ('Late-Onset Alzheimer''s Disease', 'health_risk', 'Alzheimer''s',
             'APOE', 'rs429358', 'TT', 'low', 'No e4 variant detected',
             false, 'test.pdf', 'h1', 'r1', CURRENT_TIMESTAMP, NULL),
            ('Muscle Composition', 'wellness', 'Muscle Composition',
             'ACTN3', 'rs1815739', 'CT', 'informational', 'Common in elite power athletes',
             true, 'test2.pdf', 'h2', 'r2', CURRENT_TIMESTAMP, NULL),
            ('Hemochromatosis', 'carrier_status', 'Hereditary Hemochromatosis',
             'HFE', 'rs1800562', 'AG', 'medium', 'Carrier of one variant',
             true, 'test3.pdf', 'h3', 'r3', CURRENT_TIMESTAMP, NULL)
        """
        )
        self.con.execute(
            """
            INSERT INTO silver.ancestry_segments VALUES
            ('Scandinavian', 1, 'chr1', 100, 50000, 49900,
             0.02, 'test.csv', 'a1', 'ar1', CURRENT_TIMESTAMP, NULL),
            ('British & Irish', 1, 'chr2', 200, 30000, 29800,
             0.01, 'test.csv', 'a2', 'ar2', CURRENT_TIMESTAMP, NULL),
            ('European', 1, 'chr1', 100, 249000000, 248999900,
             99.9, 'test.csv', 'a3', 'ar3', CURRENT_TIMESTAMP, NULL)
        """
        )
        self.con.execute(
            """
            INSERT INTO silver.family_tree VALUES
            ('u1', 'Claus', 'Petraeus', 'you', 0, 'both',
             2, true, NULL, 'test.json', 'f1', 'fr1', CURRENT_TIMESTAMP, NULL),
            ('f1', NULL, NULL, 'father', -1, 'unknown',
             1, true, NULL, 'test.json', 'f2', 'fr2', CURRENT_TIMESTAMP, NULL)
        """
        )

        from health_platform.mcp.health_tools import HealthTools

        self.tools = HealthTools(self.con)

    def teardown_method(self):
        try:
            self.con.close()
        except Exception:
            pass

    def test_query_genetics_dashboard(self):
        result = self.tools.query_genetics()
        assert "Genetics Dashboard" in result
        assert "Health Findings" in result

    def test_query_genetics_health_all(self):
        result = self.tools.query_genetics(query_type="health")
        assert "Genetic Health Findings" in result
        assert "Alzheimer" in result
        assert "ACTN3" in result

    def test_query_genetics_health_by_category(self):
        result = self.tools.query_genetics(query_type="health", category="health_risk")
        assert "Alzheimer" in result
        assert "Muscle" not in result

    def test_query_genetics_health_by_report_name(self):
        result = self.tools.query_genetics(query_type="health", report_name="muscle")
        assert "ACTN3" in result

    def test_query_genetics_ancestry_overview(self):
        result = self.tools.query_genetics(query_type="ancestry")
        assert "Ancestry Composition" in result
        assert "European" in result

    def test_query_genetics_ancestry_filtered(self):
        result = self.tools.query_genetics(
            query_type="ancestry", category="Scandinavian"
        )
        assert "Scandinavian" in result

    def test_query_genetics_family(self):
        result = self.tools.query_genetics(query_type="family")
        assert "Family Tree" in result
        assert "father" in result

    def test_query_genetics_empty_tables(self):
        """Test with empty tables returns appropriate message."""
        empty_con = duckdb.connect(":memory:")
        empty_con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        empty_con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        empty_con.execute(
            """CREATE TABLE silver.genetic_health_findings (
                report_name VARCHAR, category VARCHAR, condition VARCHAR,
                gene VARCHAR, snp_id VARCHAR, genotype VARCHAR,
                risk_level VARCHAR, result_summary VARCHAR,
                variant_detected BOOLEAN, source_file VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )"""
        )
        empty_con.execute(
            """CREATE TABLE silver.ancestry_segments (
                ancestry_category VARCHAR, copy INTEGER, chromosome VARCHAR,
                start_bp BIGINT, end_bp BIGINT, segment_length_bp BIGINT,
                pct_of_chromosome DOUBLE, source_file VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )"""
        )
        empty_con.execute(
            """CREATE TABLE silver.family_tree (
                person_id VARCHAR, first_name VARCHAR, last_name VARCHAR,
                relationship_to_user VARCHAR, generation INTEGER,
                side VARCHAR, num_shared_segments INTEGER,
                has_dna_match BOOLEAN, profile_url VARCHAR,
                source_file VARCHAR, business_key_hash VARCHAR,
                row_hash VARCHAR, load_datetime TIMESTAMP,
                update_datetime TIMESTAMP
            )"""
        )

        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(empty_con)
        result = tools.query_genetics(query_type="health")
        assert "No genetic health findings found" in result
        empty_con.close()
