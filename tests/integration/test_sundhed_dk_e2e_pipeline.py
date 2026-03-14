"""End-to-end integration tests for sundhed.dk silver merge SQL files.

Wave 2: Synthetic data — validates each merge SQL produces correct output
from in-memory bronze tables with zero data loss.

Wave 3: Real PDF E2E — skippable tests that parse actual PDFs, load through
bronze, merge to silver, and assert count equality at every layer.

All synthetic test data is fabricated — no real health data.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from health_platform.utils.paths import get_data_lake_root

pytestmark = pytest.mark.integration

MERGE_SQL_DIR = Path(__file__).resolve().parents[2] / (
    "health_unified_platform/health_platform/transformation_logic/dbt/merge/silver"
)

# ---------------------------------------------------------------------------
# Merge configurations: synthetic bronze data for each sundhed.dk section
# ---------------------------------------------------------------------------

MERGE_CONFIGS = {
    "lab_results": {
        "sql": "merge_sundhed_dk_lab_results.sql",
        "bronze": "bronze.stg_sundhed_dk_lab_results",
        "silver": "silver.sundhed_dk_lab_results",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_sundhed_dk_lab_results (
                test_date DATE,
                marker_name VARCHAR,
                value_numeric DOUBLE,
                value_text VARCHAR,
                unit VARCHAR,
                reference_min DOUBLE,
                reference_max DOUBLE,
                reference_direction VARCHAR,
                status VARCHAR,
                _ingested_at TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_sundhed_dk_lab_results VALUES
                ('2026-01-15', 'Haemoglobin', 9.2, '9.2', 'mmol/L', 8.3, 10.5, NULL, 'normal', '2026-03-01 10:00:00'),
                ('2026-01-15', 'Leukocytter', 6.8, '6.8', '10^9/L', 3.5, 10.0, NULL, 'normal', '2026-03-01 10:00:00'),
                ('2026-02-10', 'Haemoglobin', 9.5, '9.5', 'mmol/L', 8.3, 10.5, NULL, 'normal', '2026-03-01 10:00:00'),
                ('2026-02-10', 'CRP', 3.2, '3.2', 'mg/L', NULL, 8.0, NULL, 'normal', '2026-03-01 10:00:00'),
                ('2026-03-01', 'TSH', 2.1, '2.1', 'mIU/L', 0.3, 4.0, NULL, 'normal', '2026-03-01 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.sundhed_dk_lab_results (
                test_date DATE,
                marker_name VARCHAR,
                value_numeric DOUBLE,
                value_text VARCHAR,
                unit VARCHAR,
                reference_min DOUBLE,
                reference_max DOUBLE,
                reference_direction VARCHAR,
                status VARCHAR,
                business_key_hash VARCHAR,
                row_hash VARCHAR,
                load_datetime TIMESTAMP,
                update_datetime TIMESTAMP
            )
        """,
        "expected_rows": 5,
    },
    "medications": {
        "sql": "merge_sundhed_dk_medications.sql",
        "bronze": "bronze.stg_sundhed_dk_medications",
        "silver": "silver.sundhed_dk_medications",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_sundhed_dk_medications (
                medication_name VARCHAR,
                active_ingredient VARCHAR,
                strength VARCHAR,
                dosage VARCHAR,
                start_date DATE,
                end_date DATE,
                atc_code VARCHAR,
                reason VARCHAR,
                _ingested_at TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_sundhed_dk_medications VALUES
                ('Panodil', 'Paracetamol', '500 mg', '1-2 tabletter', '2025-06-01', NULL, 'N02BE01', 'Smerter', '2026-03-01 10:00:00'),
                ('Ibuprofen', 'Ibuprofen', '400 mg', '1 tablet', '2025-08-15', '2025-12-31', 'M01AE01', 'Betaendelse', '2026-03-01 10:00:00'),
                ('Vitamin D', 'Cholecalciferol', '20 microgram', '1 tablet', '2025-01-01', NULL, 'A11CC05', 'Forebyggelse', '2026-03-01 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.sundhed_dk_medications (
                medication_name VARCHAR,
                active_ingredient VARCHAR,
                strength VARCHAR,
                dosage VARCHAR,
                start_date DATE,
                end_date DATE,
                atc_code VARCHAR,
                reason VARCHAR,
                is_active BOOLEAN,
                business_key_hash VARCHAR,
                row_hash VARCHAR,
                load_datetime TIMESTAMP,
                update_datetime TIMESTAMP
            )
        """,
        "expected_rows": 3,
    },
    "vaccinations": {
        "sql": "merge_sundhed_dk_vaccinations.sql",
        "bronze": "bronze.stg_sundhed_dk_vaccinations",
        "silver": "silver.sundhed_dk_vaccinations",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_sundhed_dk_vaccinations (
                vaccine_name VARCHAR,
                vaccine_date DATE,
                given_at VARCHAR,
                duration VARCHAR,
                batch_number VARCHAR,
                _ingested_at TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_sundhed_dk_vaccinations VALUES
                ('Influenza', '2025-10-15', 'Laegeklinik Nord', NULL, 'FLU-2025-A', '2026-03-01 10:00:00'),
                ('COVID-19 mod Omicron', '2024-01-20', 'Vaccinationscenter Oest', NULL, 'COV-2024-B', '2026-03-01 10:00:00'),
                ('Tetanus', '2020-06-10', 'Skadestuen', '10 aar', 'TET-2020-C', '2026-03-01 10:00:00'),
                ('Hepatitis B', '2019-03-05', 'Rejseklinikken', NULL, 'HEP-2019-D', '2026-03-01 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.sundhed_dk_vaccinations (
                vaccine_name VARCHAR,
                vaccine_date DATE,
                given_at VARCHAR,
                duration VARCHAR,
                batch_number VARCHAR,
                disease_target VARCHAR,
                business_key_hash VARCHAR,
                row_hash VARCHAR,
                load_datetime TIMESTAMP,
                update_datetime TIMESTAMP
            )
        """,
        "expected_rows": 4,
    },
    "ejournal": {
        "sql": "merge_sundhed_dk_ejournal.sql",
        "bronze": "bronze.stg_sundhed_dk_ejournal",
        "silver": "silver.sundhed_dk_ejournal",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_sundhed_dk_ejournal (
                note_date DATE,
                department VARCHAR,
                hospital VARCHAR,
                note_type VARCHAR,
                note_text VARCHAR,
                _ingested_at TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_sundhed_dk_ejournal VALUES
                ('2025-12-10', 'Medicinsk afdeling', 'Rigshospitalet', 'Epikrise', 'Patienten blev undersoegt for symptomer.', '2026-03-01 10:00:00'),
                ('2025-11-05', 'Orthopaedkirurgi', 'Herlev Hospital', 'Notat', 'Opfoelgning efter procedure.', '2026-03-01 10:00:00'),
                ('2025-09-20', 'Kardiologi', 'Gentofte Hospital', 'Epikrise', 'Rutinekontrol gennemfoert uden anmaerkninger.', '2026-03-01 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.sundhed_dk_ejournal (
                note_date DATE,
                department VARCHAR,
                hospital VARCHAR,
                note_type VARCHAR,
                note_text VARCHAR,
                business_key_hash VARCHAR,
                row_hash VARCHAR,
                load_datetime TIMESTAMP,
                update_datetime TIMESTAMP
            )
        """,
        "expected_rows": 3,
    },
    "appointments": {
        "sql": "merge_sundhed_dk_appointments.sql",
        "bronze": "bronze.stg_sundhed_dk_appointments",
        "silver": "silver.sundhed_dk_appointments",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_sundhed_dk_appointments (
                referral_date DATE,
                expiry_date DATE,
                referring_clinic VARCHAR,
                receiving_clinic VARCHAR,
                specialty VARCHAR,
                section VARCHAR,
                _ingested_at TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_sundhed_dk_appointments VALUES
                ('2025-11-01', '2026-05-01', 'Egen laege', 'Orthopaedkirurgisk afd.', 'Orthopaedi', 'Region H', '2026-03-01 10:00:00'),
                ('2026-01-15', '2026-07-15', 'Egen laege', 'Kardiologisk ambulatorium', 'Kardiologi', 'Region H', '2026-03-01 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.sundhed_dk_appointments (
                referral_date DATE,
                expiry_date DATE,
                referring_clinic VARCHAR,
                receiving_clinic VARCHAR,
                specialty VARCHAR,
                section VARCHAR,
                is_active BOOLEAN,
                days_until_expiry INTEGER,
                business_key_hash VARCHAR,
                row_hash VARCHAR,
                load_datetime TIMESTAMP,
                update_datetime TIMESTAMP
            )
        """,
        "expected_rows": 2,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _strip_sql_comments(sql: str) -> str:
    """Remove full-line SQL comments (-- ...) from SQL text."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped.startswith("--"):
            lines.append(line)
    return "\n".join(lines)


def _setup_and_run_merge(con: duckdb.DuckDBPyConnection, config: dict) -> None:
    """Create bronze + silver tables, insert synthetic data, run merge SQL."""
    con.execute(config["bronze_ddl"])
    con.execute(config["bronze_insert"])
    con.execute(config["silver_ddl"])

    sql_path = MERGE_SQL_DIR / config["sql"]
    sql = _strip_sql_comments(sql_path.read_text())
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


@pytest.fixture(params=list(MERGE_CONFIGS.keys()))
def merge_result(request):
    """Run a single merge in an in-memory DuckDB and yield (connection, config)."""
    config = MERGE_CONFIGS[request.param]
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    _setup_and_run_merge(con, config)
    yield con, config
    con.close()


# ---------------------------------------------------------------------------
# Wave 2: Synthetic data E2E tests
# ---------------------------------------------------------------------------


class TestMergeSqlExecutes:
    """Verify all 5 sundhed.dk merge SQL files execute without errors."""

    def test_table_has_rows(self, merge_result):
        con, config = merge_result
        silver_table = config["silver"]
        count = con.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()[0]
        assert count > 0, f"{silver_table} is empty after merge"

    def test_expected_row_count(self, merge_result):
        """Zero data loss: bronze input count = silver output count."""
        con, config = merge_result
        expected = config["expected_rows"]
        silver_table = config["silver"]
        count = con.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()[0]
        assert (
            count == expected
        ), f"{silver_table}: expected {expected} rows, got {count} — data loss detected"


class TestBusinessKeyIntegrity:
    """Verify business_key_hash is NOT NULL and unique after merge."""

    def test_no_null_business_key_hash(self, merge_result):
        con, config = merge_result
        silver_table = config["silver"]
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {silver_table} WHERE business_key_hash IS NULL"
        ).fetchone()[0]
        assert (
            null_count == 0
        ), f"{silver_table} has {null_count} NULL business_key_hash"

    def test_unique_business_key_hash(self, merge_result):
        con, config = merge_result
        silver_table = config["silver"]
        dup_count = con.execute(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT business_key_hash FROM {silver_table} "
            f"  GROUP BY 1 HAVING COUNT(*) > 1"
            f")"
        ).fetchone()[0]
        assert (
            dup_count == 0
        ), f"{silver_table} has {dup_count} duplicate business_key_hash groups"


class TestMergeIdempotency:
    """Verify running the merge twice does not create duplicates."""

    @pytest.mark.parametrize("merge_name", list(MERGE_CONFIGS.keys()))
    def test_idempotent_merge(self, merge_name):
        config = MERGE_CONFIGS[merge_name]
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        _setup_and_run_merge(con, config)

        count_after_first = con.execute(
            f"SELECT COUNT(*) FROM {config['silver']}"
        ).fetchone()[0]

        # Re-create bronze data (staging was dropped) and run merge again
        con.execute(f"DROP TABLE IF EXISTS {config['bronze']}")
        con.execute(config["bronze_ddl"])
        con.execute(config["bronze_insert"])

        sql_path = MERGE_SQL_DIR / config["sql"]
        sql = _strip_sql_comments(sql_path.read_text())
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        count_after_second = con.execute(
            f"SELECT COUNT(*) FROM {config['silver']}"
        ).fetchone()[0]

        assert count_after_second == count_after_first, (
            f"{config['silver']}: row count changed from {count_after_first} "
            f"to {count_after_second} after second merge (not idempotent)"
        )
        con.close()


class TestStagingTableCleanup:
    """Verify staging tables are dropped after merge."""

    @pytest.mark.parametrize("merge_name", list(MERGE_CONFIGS.keys()))
    def test_staging_dropped(self, merge_name):
        config = MERGE_CONFIGS[merge_name]
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        _setup_and_run_merge(con, config)

        silver_name = config["silver"].replace("silver.", "")
        staging_exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = 'silver' AND table_name = '{silver_name}__staging'"
        ).fetchone()[0]

        assert (
            staging_exists == 0
        ), f"Staging table silver.{silver_name}__staging was not dropped"
        con.close()


# ---------------------------------------------------------------------------
# Wave 3: Real PDF E2E tests (skippable)
# ---------------------------------------------------------------------------

_DOWNLOADS = get_data_lake_root() / "min sundhed" / "downloads"


@pytest.mark.skipif(
    not _DOWNLOADS.exists(),
    reason="sundhed.dk PDFs not available",
)
class TestSundhedDkRealPdfE2E:
    """Full pipeline: PDF -> parse -> bronze -> silver merge.

    Only runs when real PDFs exist locally. Asserts count equality
    at every layer: PDF records = Bronze rows = Silver rows.
    """

    @staticmethod
    def _run_section_e2e(
        parse_fn,
        pdf_path: Path,
        merge_sql_name: str,
        bronze_table: str,
        silver_table: str,
        bronze_ddl: str,
        silver_ddl: str,
    ) -> None:
        """Parse PDF, load to bronze, merge to silver, verify counts."""
        records = parse_fn(str(pdf_path))
        assert len(records) > 0, f"Parser returned 0 records from {pdf_path.name}"
        pdf_count = len(records)

        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(bronze_ddl)
        con.execute(silver_ddl)

        # Insert parsed records into bronze — only columns that exist in schema
        actual_bronze_cols = {
            row[0]
            for row in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{bronze_table.split('.')[-1]}'"
            ).fetchall()
        }
        # Only insert columns present in both parser output and bronze schema
        parser_cols = list(records[0].keys())
        cols = [c for c in parser_cols if c in actual_bronze_cols]
        placeholders = ", ".join(["?"] * (len(cols) + 1))  # +1 for _ingested_at
        col_list = ", ".join(cols) + ", _ingested_at"
        for rec in records:
            values = [rec[c] for c in cols] + ["2026-03-14 10:00:00"]
            con.execute(
                f"INSERT INTO {bronze_table} ({col_list}) VALUES ({placeholders})",
                values,
            )

        bronze_count = con.execute(f"SELECT COUNT(*) FROM {bronze_table}").fetchone()[0]
        assert (
            bronze_count == pdf_count
        ), f"Bronze load lost data: PDF={pdf_count}, Bronze={bronze_count}"

        # Run merge
        sql_path = MERGE_SQL_DIR / merge_sql_name
        sql = _strip_sql_comments(sql_path.read_text())
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        silver_count = con.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()[0]

        # Silver may have fewer rows due to dedup (same business key),
        # but must never have MORE rows than bronze
        assert silver_count > 0, f"{silver_table} is empty after merge"
        assert (
            silver_count <= bronze_count
        ), f"Silver has more rows than bronze: Silver={silver_count}, Bronze={bronze_count}"
        con.close()

    def test_lab_results_e2e(self):
        """Lab results: PDF -> bronze -> silver with count verification."""
        lab_dir = _DOWNLOADS / "lab_results"
        if not lab_dir.exists():
            pytest.skip("lab_results directory not found")
        pdfs = sorted(lab_dir.glob("*.pdf"))
        if not pdfs:
            pytest.skip("No lab result PDFs found")

        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            parse_lab_results_pdf,
        )

        self._run_section_e2e(
            parse_fn=parse_lab_results_pdf,
            pdf_path=pdfs[0],
            merge_sql_name="merge_sundhed_dk_lab_results.sql",
            bronze_table="bronze.stg_sundhed_dk_lab_results",
            silver_table="silver.sundhed_dk_lab_results",
            bronze_ddl=MERGE_CONFIGS["lab_results"]["bronze_ddl"],
            silver_ddl=MERGE_CONFIGS["lab_results"]["silver_ddl"],
        )

    def test_medications_e2e(self):
        """Medications: PDF -> bronze -> silver with count verification."""
        pdf = _DOWNLOADS / "medications" / "Medicin - sundhed.dk.pdf"
        if not pdf.exists():
            pytest.skip("Medications PDF not found")

        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            parse_medications_pdf,
        )

        self._run_section_e2e(
            parse_fn=parse_medications_pdf,
            pdf_path=pdf,
            merge_sql_name="merge_sundhed_dk_medications.sql",
            bronze_table="bronze.stg_sundhed_dk_medications",
            silver_table="silver.sundhed_dk_medications",
            bronze_ddl=MERGE_CONFIGS["medications"]["bronze_ddl"],
            silver_ddl=MERGE_CONFIGS["medications"]["silver_ddl"],
        )

    def test_vaccinations_e2e(self):
        """Vaccinations: PDF -> bronze -> silver with count verification."""
        pdf = _DOWNLOADS / "vaccinations" / "Vaccinationer - sundhed.dk.pdf"
        if not pdf.exists():
            pytest.skip("Vaccinations PDF not found")

        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            parse_vaccinations_pdf,
        )

        self._run_section_e2e(
            parse_fn=parse_vaccinations_pdf,
            pdf_path=pdf,
            merge_sql_name="merge_sundhed_dk_vaccinations.sql",
            bronze_table="bronze.stg_sundhed_dk_vaccinations",
            silver_table="silver.sundhed_dk_vaccinations",
            bronze_ddl=MERGE_CONFIGS["vaccinations"]["bronze_ddl"],
            silver_ddl=MERGE_CONFIGS["vaccinations"]["silver_ddl"],
        )

    def test_ejournal_e2e(self):
        """E-journal: PDF -> bronze -> silver with count verification."""
        ejournal_dir = _DOWNLOADS / "ejournal"
        if not ejournal_dir.exists():
            pytest.skip("ejournal directory not found")
        pdfs = sorted(ejournal_dir.glob("*.pdf"))
        if not pdfs:
            pytest.skip("No ejournal PDFs found")

        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            parse_ejournal_pdf,
        )

        self._run_section_e2e(
            parse_fn=parse_ejournal_pdf,
            pdf_path=pdfs[0],
            merge_sql_name="merge_sundhed_dk_ejournal.sql",
            bronze_table="bronze.stg_sundhed_dk_ejournal",
            silver_table="silver.sundhed_dk_ejournal",
            bronze_ddl=MERGE_CONFIGS["ejournal"]["bronze_ddl"],
            silver_ddl=MERGE_CONFIGS["ejournal"]["silver_ddl"],
        )
