"""Integration tests for DS1 Oura API silver merge SQL files.

Validates 8 new silver tables created from Oura API bronze sources:
- oura_vo2_max, oura_tag, oura_rest_mode_period, oura_ring_configuration,
  oura_sleep, cardiovascular_age, daily_resilience, enhanced_tag

For each merge:
- Table exists after merge and has rows
- business_key_hash is NOT NULL
- business_key_hash is unique
- sk_date is NOT NULL

All test data is synthetic — no real health data.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

pytestmark = pytest.mark.integration

MERGE_SQL_DIR = Path(__file__).resolve().parents[2] / (
    "health_unified_platform/health_platform/transformation_logic/dbt/merge/silver"
)

# ── Merge definitions ──────────────────────────────────────────────────
# Each entry: (sql_filename, bronze_table, silver_table, bronze_ddl, bronze_insert)

MERGE_CONFIGS = {
    "oura_vo2_max": {
        "sql": "merge_oura_vo2_max.sql",
        "bronze": "bronze.stg_oura_vo2_max",
        "silver": "silver.oura_vo2_max",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_vo2_max (
                year INTEGER, month VARCHAR, day VARCHAR,
                id VARCHAR, vo2_max VARCHAR,
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_vo2_max VALUES
                (2026, '3', '1', 'vm-001', '42.5', '2026-03-01 10:00:00'),
                (2026, '3', '2', 'vm-002', '43.1', '2026-03-02 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.oura_vo2_max (
                sk_date INTEGER, day DATE, id VARCHAR,
                vo2_max DOUBLE,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
    "oura_tag": {
        "sql": "merge_oura_tag.sql",
        "bronze": "bronze.stg_oura_tag",
        "silver": "silver.oura_tag",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_tag (
                year INTEGER, month VARCHAR, day VARCHAR,
                id VARCHAR, tag_type_code VARCHAR,
                start_day DATE, end_day DATE,
                start_time VARCHAR, end_time VARCHAR,
                comment VARCHAR,
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_tag VALUES
                (2026, '3', '1', 'tag-001', 'generic',
                 '2026-03-01', '2026-03-01', '08:00', '09:00', 'morning walk',
                 '2026-03-01 10:00:00'),
                (2026, '3', '2', 'tag-002', 'medication',
                 '2026-03-02', '2026-03-02', '20:00', '20:05', 'vitamin d',
                 '2026-03-02 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.oura_tag (
                sk_date INTEGER, day DATE, id VARCHAR,
                tag_type_code VARCHAR,
                start_day DATE, end_day DATE,
                start_time VARCHAR, end_time VARCHAR,
                comment VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
    "oura_rest_mode_period": {
        "sql": "merge_oura_rest_mode_period.sql",
        "bronze": "bronze.stg_oura_rest_mode_period",
        "silver": "silver.oura_rest_mode_period",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_rest_mode_period (
                year INTEGER, month VARCHAR, day VARCHAR,
                id VARCHAR,
                start_day DATE, end_day DATE,
                start_time TIMESTAMP, end_time TIMESTAMP,
                episodes VARCHAR,
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_rest_mode_period VALUES
                (2026, '3', '1', 'rmp-001',
                 '2026-03-01', '2026-03-02',
                 '2026-03-01 22:00:00', '2026-03-02 06:00:00',
                 '[]',
                 '2026-03-01 10:00:00'),
                (2026, '3', '5', 'rmp-002',
                 '2026-03-05', '2026-03-06',
                 '2026-03-05 21:00:00', '2026-03-06 07:00:00',
                 '[]',
                 '2026-03-05 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.oura_rest_mode_period (
                sk_date INTEGER, day DATE, id VARCHAR,
                start_day DATE, end_day DATE,
                start_time TIMESTAMP, end_time TIMESTAMP,
                episodes VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
    "oura_ring_configuration": {
        "sql": "merge_oura_ring_configuration.sql",
        "bronze": "bronze.stg_oura_ring_configuration",
        "silver": "silver.oura_ring_configuration",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_ring_configuration (
                year INTEGER, month VARCHAR, day VARCHAR,
                id VARCHAR, color VARCHAR, design VARCHAR,
                firmware_version VARCHAR, hardware_type VARCHAR,
                set_up_at TIMESTAMP, size INTEGER,
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_ring_configuration VALUES
                (2026, '1', '15', 'rc-001', 'silver', 'heritage',
                 '5.6.0', 'gen3', '2025-01-15 12:00:00', 9,
                 '2026-01-15 10:00:00'),
                (2026, '2', '10', 'rc-002', 'black', 'horizon',
                 '5.7.1', 'gen3', '2026-02-10 12:00:00', 10,
                 '2026-02-10 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.oura_ring_configuration (
                sk_date INTEGER, day DATE, id VARCHAR,
                color VARCHAR, design VARCHAR,
                firmware_version VARCHAR, hardware_type VARCHAR,
                set_up_at TIMESTAMP, size INTEGER,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
    "oura_sleep": {
        "sql": "merge_oura_sleep.sql",
        "bronze": "bronze.stg_oura_sleep",
        "silver": "silver.oura_sleep",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_sleep (
                year INTEGER, month VARCHAR, day VARCHAR,
                id VARCHAR,
                average_breath VARCHAR, average_heart_rate VARCHAR,
                average_hrv VARCHAR, awake_time VARCHAR,
                bedtime_end TIMESTAMP, bedtime_start TIMESTAMP,
                deep_sleep_duration VARCHAR, efficiency VARCHAR,
                latency VARCHAR, light_sleep_duration VARCHAR,
                low_battery_alert BOOLEAN, lowest_heart_rate VARCHAR,
                period VARCHAR, readiness_score_delta VARCHAR,
                rem_sleep_duration VARCHAR, restless_periods VARCHAR,
                sleep_score_delta VARCHAR, time_in_bed VARCHAR,
                total_sleep_duration VARCHAR, type VARCHAR,
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_sleep VALUES
                (2026, '3', '1', 'slp-001',
                 '15.2', '54.0', '45', '1800',
                 '2026-03-01 07:00:00', '2026-03-01 23:00:00',
                 '5400', '88', '300', '14400',
                 false, '48', '0', '1.5',
                 '7200', '3', '-0.5', '28800',
                 '27000', 'long_sleep',
                 '2026-03-01 10:00:00'),
                (2026, '3', '2', 'slp-002',
                 '14.8', '56.0', '40', '2100',
                 '2026-03-02 06:30:00', '2026-03-01 23:30:00',
                 '4800', '82', '600', '13200',
                 false, '50', '0', '0.8',
                 '6600', '5', '0.2', '27000',
                 '24900', 'long_sleep',
                 '2026-03-02 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.oura_sleep (
                sk_date INTEGER, day DATE, id VARCHAR,
                average_breath DOUBLE, average_heart_rate DOUBLE,
                average_hrv INTEGER, awake_time INTEGER,
                bedtime_end TIMESTAMP, bedtime_start TIMESTAMP,
                deep_sleep_duration INTEGER, efficiency INTEGER,
                latency INTEGER, light_sleep_duration INTEGER,
                low_battery_alert BOOLEAN, lowest_heart_rate INTEGER,
                period INTEGER, readiness_score_delta DOUBLE,
                rem_sleep_duration INTEGER, restless_periods INTEGER,
                sleep_score_delta DOUBLE, time_in_bed INTEGER,
                total_sleep_duration INTEGER, type VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
    "cardiovascular_age": {
        "sql": "merge_oura_cardiovascular_age.sql",
        "bronze": "bronze.stg_oura_daily_cardiovascular_age",
        "silver": "silver.cardiovascular_age",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_daily_cardiovascular_age (
                year INTEGER, month VARCHAR, day VARCHAR,
                vascular_age VARCHAR,
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_daily_cardiovascular_age VALUES
                (2026, '3', '1', '38', '2026-03-01 10:00:00'),
                (2026, '3', '2', '37', '2026-03-02 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.cardiovascular_age (
                sk_date INTEGER, day DATE,
                vascular_age INTEGER,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
    "daily_resilience": {
        "sql": "merge_oura_daily_resilience.sql",
        "bronze": "bronze.stg_oura_daily_resilience",
        "silver": "silver.daily_resilience",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_daily_resilience (
                year INTEGER, month VARCHAR, day VARCHAR,
                level VARCHAR,
                contributors STRUCT(
                    sleep_recovery DOUBLE,
                    daytime_recovery DOUBLE,
                    stress DOUBLE
                ),
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_daily_resilience VALUES
                (2026, '3', '1', 'solid',
                 {'sleep_recovery': 82.0, 'daytime_recovery': 75.0, 'stress': 68.0},
                 '2026-03-01 10:00:00'),
                (2026, '3', '2', 'strong',
                 {'sleep_recovery': 90.0, 'daytime_recovery': 85.0, 'stress': 72.0},
                 '2026-03-02 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.daily_resilience (
                sk_date INTEGER, day DATE,
                level VARCHAR,
                contributor_sleep_recovery DOUBLE,
                contributor_daytime_recovery DOUBLE,
                contributor_stress DOUBLE,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
    "enhanced_tag": {
        "sql": "merge_oura_enhanced_tag.sql",
        "bronze": "bronze.stg_oura_enhanced_tag",
        "silver": "silver.enhanced_tag",
        "bronze_ddl": """
            CREATE TABLE bronze.stg_oura_enhanced_tag (
                year INTEGER, month VARCHAR, day VARCHAR,
                id VARCHAR,
                start_day DATE, start_time VARCHAR,
                end_day DATE, end_time VARCHAR,
                tag_type_code VARCHAR, custom_tag_name VARCHAR,
                comment VARCHAR,
                _ingested_at_1 TIMESTAMP
            )
        """,
        "bronze_insert": """
            INSERT INTO bronze.stg_oura_enhanced_tag VALUES
                (2026, '3', '1', 'et-001',
                 '2026-03-01', '08:00', '2026-03-01', '09:00',
                 'tag_generic', 'morning_run', 'easy 5k',
                 '2026-03-01 10:00:00'),
                (2026, '3', '2', 'et-002',
                 '2026-03-02', '19:00', '2026-03-02', '19:30',
                 'tag_generic', 'meditation', 'guided session',
                 '2026-03-02 10:00:00')
        """,
        "silver_ddl": """
            CREATE TABLE silver.enhanced_tag (
                sk_date INTEGER, day DATE, id VARCHAR,
                start_day DATE, start_time VARCHAR,
                end_day DATE, end_time VARCHAR,
                tag_type_code VARCHAR, custom_tag_name VARCHAR,
                comment VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """,
    },
}


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


# ── Tests ──────────────────────────────────────────────────────────────


class TestMergeSqlExecutes:
    """Verify all 8 merge SQL files execute without errors."""

    def test_table_has_rows(self, merge_result):
        con, config = merge_result
        silver_table = config["silver"]
        count = con.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()[0]
        assert count > 0, f"{silver_table} is empty after merge"


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


class TestSkDateIntegrity:
    """Verify sk_date is NOT NULL after merge."""

    def test_no_null_sk_date(self, merge_result):
        con, config = merge_result
        silver_table = config["silver"]
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {silver_table} WHERE sk_date IS NULL"
        ).fetchone()[0]
        assert null_count == 0, f"{silver_table} has {null_count} NULL sk_date"


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

        # Extract staging table name from silver table name
        silver_name = config["silver"].replace("silver.", "")
        staging_exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = 'silver' AND table_name = '{silver_name}__staging'"
        ).fetchone()[0]

        assert (
            staging_exists == 0
        ), f"Staging table silver.{silver_name}__staging was not dropped"
        con.close()
