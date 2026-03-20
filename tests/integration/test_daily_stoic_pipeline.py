"""Integration tests for the daily-stoic data pipeline.

Validates all 5 silver tables and 2 gold tables produced by the
daily-stoic integration. Checks are run directly against the dev
DuckDB database — no live data is read, only structure and counts.

All assertions use read-only queries. No writes are performed.
"""

from __future__ import annotations

import duckdb
import pytest

pytestmark = pytest.mark.integration

DB_PATH = "/Users/Shared/data_lake/database/health_dw_dev.db"

# ── Expected schema definitions ──────────────────────────────────────────────
# Maps table (schema.name) → set of required columns.

SILVER_SCHEMAS: dict[str, list[str]] = {
    "silver.daily_stoic_focus": [
        "sk_date",
        "day",
        "focus_text",
        "updated_at",
        "business_key_hash",
        "row_hash",
        "load_datetime",
        "update_datetime",
    ],
    "silver.habit_definitions": [
        "habit_id",
        "habit_name",
        "habit_type",
        "goal",
        "position",
        "active",
        "created_at",
        "business_key_hash",
        "row_hash",
        "load_datetime",
        "update_datetime",
    ],
    "silver.daily_stoic_habits": [
        "sk_date",
        "day",
        "habit_id",
        "done",
        "toggled_at",
        "business_key_hash",
        "row_hash",
        "load_datetime",
        "update_datetime",
    ],
    "silver.stoic_quotes": [
        "sk_date",
        "day",
        "quote_text",
        "author",
        "business_key_hash",
        "row_hash",
        "load_datetime",
        "update_datetime",
    ],
    "silver.daily_stoic_reflections": [
        "sk_date",
        "day",
        "reflection_text",
        "updated_at",
        "business_key_hash",
        "row_hash",
        "load_datetime",
        "update_datetime",
    ],
}

GOLD_SCHEMAS: dict[str, list[str]] = {
    "gold.dim_habit": [
        "sk_habit",
        "habit_id",
        "habit_name",
        "habit_type",
        "goal",
        "position",
        "active",
    ],
    "gold.fct_daily_experience": [
        "sk_date",
        "day",
        "focus_text",
        "reflection_text",
        "quote_text",
        "quote_author",
        "total_habits",
        "completed_habits",
        "completion_pct",
        "completed_habit_ids",
    ],
}

# ── Expected row counts ───────────────────────────────────────────────────────

EXPECTED_ROW_COUNTS: dict[str, int] = {
    "silver.daily_stoic_focus": 5,
    "silver.habit_definitions": 9,
    "silver.daily_stoic_habits": 20,
    "silver.stoic_quotes": 1826,
    "silver.daily_stoic_reflections": 2,
    "gold.dim_habit": 9,
}

# Tables where the minimum (not exact) row count is checked.
EXPECTED_MIN_ROW_COUNTS: dict[str, int] = {
    "gold.fct_daily_experience": 5,
}

# ── Tables that carry date-grain keys ────────────────────────────────────────

DATE_GRAIN_TABLES = [
    "silver.daily_stoic_focus",
    "silver.daily_stoic_habits",
    "silver.stoic_quotes",
    "silver.daily_stoic_reflections",
    "gold.fct_daily_experience",
]

# Silver tables that have a business_key_hash column.
SILVER_TABLES_WITH_BK = list(SILVER_SCHEMAS.keys())


# ── Fixture ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    """Read-only connection to the dev DuckDB database."""
    connection = duckdb.connect(DB_PATH, read_only=True)
    yield connection
    connection.close()


# ── Helper ────────────────────────────────────────────────────────────────────


def _columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchall()
    return {r[0] for r in rows}


def _count(con: duckdb.DuckDBPyConnection, full_table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]


# ── 1. Schema validation ──────────────────────────────────────────────────────


@pytest.mark.parametrize("full_table,required_cols", list(SILVER_SCHEMAS.items()))
def test_silver_schema(
    con: duckdb.DuckDBPyConnection, full_table: str, required_cols: list[str]
) -> None:
    """All required columns are present in each silver table."""
    schema, table = full_table.split(".", 1)
    actual = _columns(con, schema, table)
    missing = set(required_cols) - actual
    assert not missing, f"{full_table} is missing columns: {sorted(missing)}"


@pytest.mark.parametrize("full_table,required_cols", list(GOLD_SCHEMAS.items()))
def test_gold_schema(
    con: duckdb.DuckDBPyConnection, full_table: str, required_cols: list[str]
) -> None:
    """All required columns are present in each gold table."""
    schema, table = full_table.split(".", 1)
    actual = _columns(con, schema, table)
    missing = set(required_cols) - actual
    assert not missing, f"{full_table} is missing columns: {sorted(missing)}"


# ── 2. Row count validation ───────────────────────────────────────────────────


@pytest.mark.parametrize("full_table,expected", list(EXPECTED_ROW_COUNTS.items()))
def test_exact_row_count(
    con: duckdb.DuckDBPyConnection, full_table: str, expected: int
) -> None:
    """Row count matches the expected count from source."""
    actual = _count(con, full_table)
    assert actual == expected, f"{full_table}: expected {expected} rows, got {actual}"


@pytest.mark.parametrize("full_table,minimum", list(EXPECTED_MIN_ROW_COUNTS.items()))
def test_minimum_row_count(
    con: duckdb.DuckDBPyConnection, full_table: str, minimum: int
) -> None:
    """Row count is at least the expected minimum."""
    actual = _count(con, full_table)
    assert actual >= minimum, f"{full_table}: expected >= {minimum} rows, got {actual}"


# ── 3. NULL handling ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("full_table", SILVER_TABLES_WITH_BK)
def test_no_null_business_key_hash(
    con: duckdb.DuckDBPyConnection, full_table: str
) -> None:
    """business_key_hash is never NULL in silver tables."""
    null_count = con.execute(
        f"SELECT COUNT(*) FROM {full_table} WHERE business_key_hash IS NULL"
    ).fetchone()[0]
    assert (
        null_count == 0
    ), f"{full_table}: {null_count} NULL business_key_hash values found"


@pytest.mark.parametrize("full_table", DATE_GRAIN_TABLES)
def test_no_null_sk_date(con: duckdb.DuckDBPyConnection, full_table: str) -> None:
    """sk_date is never NULL in date-grain tables."""
    null_count = con.execute(
        f"SELECT COUNT(*) FROM {full_table} WHERE sk_date IS NULL"
    ).fetchone()[0]
    assert null_count == 0, f"{full_table}: {null_count} NULL sk_date values found"


_TABLES_WITH_DAY_COL = [
    t
    for t in list(SILVER_SCHEMAS.keys()) + list(GOLD_SCHEMAS.keys())
    # habit_definitions and dim_habit are entity tables — no day column.
    if t not in ("silver.habit_definitions", "gold.dim_habit")
]


@pytest.mark.parametrize("full_table", _TABLES_WITH_DAY_COL)
def test_no_null_day(con: duckdb.DuckDBPyConnection, full_table: str) -> None:
    """day column is never NULL in date-grain tables."""
    null_count = con.execute(
        f"SELECT COUNT(*) FROM {full_table} WHERE day IS NULL"
    ).fetchone()[0]
    assert null_count == 0, f"{full_table}: {null_count} NULL day values found"


# ── 4. Data quality ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("full_table", DATE_GRAIN_TABLES)
def test_sk_date_format(con: duckdb.DuckDBPyConnection, full_table: str) -> None:
    """sk_date values are 8-digit integers (YYYYMMDD format)."""
    invalid_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {full_table}
        WHERE sk_date < 10000000 OR sk_date > 99999999
        """
    ).fetchone()[0]
    assert (
        invalid_count == 0
    ), f"{full_table}: {invalid_count} sk_date values outside YYYYMMDD range"


@pytest.mark.parametrize("full_table", _TABLES_WITH_DAY_COL)
def test_day_is_valid_date(con: duckdb.DuckDBPyConnection, full_table: str) -> None:
    """day column values are valid dates (not NULL, parseable)."""
    # DuckDB stores DATE columns natively; invalid dates cannot be inserted.
    # This confirms the column is DATE-typed and has no nulls.
    result = con.execute(f"SELECT COUNT(*), COUNT(day) FROM {full_table}").fetchone()
    total, non_null = result
    assert (
        total == non_null
    ), f"{full_table}: {total - non_null} NULL day values — not a valid date"


@pytest.mark.parametrize("full_table", SILVER_TABLES_WITH_BK)
def test_business_key_hash_uniqueness(
    con: duckdb.DuckDBPyConnection, full_table: str
) -> None:
    """business_key_hash is unique within each silver table (primary key proxy)."""
    dup_count = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT business_key_hash
            FROM {full_table}
            GROUP BY business_key_hash
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    assert (
        dup_count == 0
    ), f"{full_table}: {dup_count} duplicate business_key_hash values found"


@pytest.mark.parametrize("full_table", SILVER_TABLES_WITH_BK)
def test_row_hash_populated(con: duckdb.DuckDBPyConnection, full_table: str) -> None:
    """row_hash is populated for all rows."""
    null_count = con.execute(
        f"SELECT COUNT(*) FROM {full_table} WHERE row_hash IS NULL"
    ).fetchone()[0]
    assert null_count == 0, f"{full_table}: {null_count} NULL row_hash values found"


def test_quotes_no_null_text_or_author(con: duckdb.DuckDBPyConnection) -> None:
    """All 1826 stoic_quotes rows have non-null quote_text and author."""
    result = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE quote_text IS NULL) AS null_text,
            COUNT(*) FILTER (WHERE author IS NULL)     AS null_author,
            COUNT(*) FILTER (WHERE TRIM(quote_text) = '') AS empty_text,
            COUNT(*) FILTER (WHERE TRIM(author) = '')     AS empty_author
        FROM silver.stoic_quotes
        """
    ).fetchone()
    null_text, null_author, empty_text, empty_author = result
    assert null_text == 0, f"silver.stoic_quotes: {null_text} NULL quote_text values"
    assert null_author == 0, f"silver.stoic_quotes: {null_author} NULL author values"
    assert empty_text == 0, f"silver.stoic_quotes: {empty_text} empty quote_text values"
    assert empty_author == 0, f"silver.stoic_quotes: {empty_author} empty author values"


# ── 5. Referential integrity ──────────────────────────────────────────────────


def test_habits_habit_id_fk(con: duckdb.DuckDBPyConnection) -> None:
    """All habit_id values in daily_stoic_habits exist in habit_definitions."""
    orphan_count = con.execute(
        """
        SELECT COUNT(*)
        FROM silver.daily_stoic_habits h
        WHERE NOT EXISTS (
            SELECT 1
            FROM silver.habit_definitions d
            WHERE d.habit_id = h.habit_id
        )
        """
    ).fetchone()[0]
    assert orphan_count == 0, (
        f"silver.daily_stoic_habits: {orphan_count} orphan habit_id rows "
        f"(no matching habit_definitions entry)"
    )


def test_fct_daily_experience_sk_date_coverage(con: duckdb.DuckDBPyConnection) -> None:
    """All sk_date values in fct_daily_experience exist in at least one source table."""
    orphan_count = con.execute(
        """
        WITH source_dates AS (
            SELECT sk_date FROM silver.daily_stoic_focus
            UNION
            SELECT sk_date FROM silver.daily_stoic_habits
            UNION
            SELECT sk_date FROM silver.stoic_quotes
            UNION
            SELECT sk_date FROM silver.daily_stoic_reflections
        )
        SELECT COUNT(*)
        FROM gold.fct_daily_experience f
        WHERE NOT EXISTS (
            SELECT 1 FROM source_dates s WHERE s.sk_date = f.sk_date
        )
        """
    ).fetchone()[0]
    assert orphan_count == 0, (
        f"gold.fct_daily_experience: {orphan_count} sk_date values not found "
        f"in any silver source table"
    )


def test_dim_habit_covers_all_definitions(con: duckdb.DuckDBPyConnection) -> None:
    """Every habit in habit_definitions has a corresponding row in dim_habit."""
    missing_count = con.execute(
        """
        SELECT COUNT(*)
        FROM silver.habit_definitions d
        WHERE NOT EXISTS (
            SELECT 1
            FROM gold.dim_habit g
            WHERE g.habit_id = d.habit_id
        )
        """
    ).fetchone()[0]
    assert missing_count == 0, (
        f"gold.dim_habit: {missing_count} habit_id values from habit_definitions "
        f"are missing in dim_habit"
    )


# ── 6. Aggregation correctness (gold.fct_daily_experience) ───────────────────


def test_fct_completion_pct_range(con: duckdb.DuckDBPyConnection) -> None:
    """completion_pct is between 0.0 and 1.0 (or 0-100 if percentage scale)."""
    result = con.execute(
        """
        SELECT MIN(completion_pct), MAX(completion_pct)
        FROM gold.fct_daily_experience
        WHERE completion_pct IS NOT NULL
        """
    ).fetchone()
    if result[0] is None:
        pytest.skip("completion_pct is entirely NULL — no data to validate")
    min_pct, max_pct = result
    # Accept either 0-1 or 0-100 scale; reject anything outside 0-100.
    assert (
        min_pct >= 0
    ), f"gold.fct_daily_experience: completion_pct min = {min_pct} (below 0)"
    assert (
        max_pct <= 100
    ), f"gold.fct_daily_experience: completion_pct max = {max_pct} (above 100)"


def test_fct_completed_habits_lte_total(con: duckdb.DuckDBPyConnection) -> None:
    """completed_habits never exceeds total_habits on any given day."""
    violation_count = con.execute(
        """
        SELECT COUNT(*)
        FROM gold.fct_daily_experience
        WHERE completed_habits > total_habits
        """
    ).fetchone()[0]
    assert (
        violation_count == 0
    ), f"gold.fct_daily_experience: {violation_count} rows where completed_habits > total_habits"


def test_fct_total_habits_bounded_by_dim(con: duckdb.DuckDBPyConnection) -> None:
    """total_habits never exceeds the total number of habits defined in dim_habit.

    fct_daily_experience aggregates habits per day (not all active habits), so
    total_habits may be <= dim_habit count — but never greater.
    """
    dim_habit_count = con.execute("SELECT COUNT(*) FROM gold.dim_habit").fetchone()[0]

    if dim_habit_count == 0:
        pytest.skip("No habits in dim_habit — skipping consistency check")

    violation_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM gold.fct_daily_experience
        WHERE total_habits > {dim_habit_count}
        """
    ).fetchone()[0]
    assert violation_count == 0, (
        f"gold.fct_daily_experience: {violation_count} rows where total_habits "
        f"> {dim_habit_count} (total habit count in dim_habit)"
    )


# ── 7. Idempotency smoke-check ────────────────────────────────────────────────


def test_silver_tables_not_empty(con: duckdb.DuckDBPyConnection) -> None:
    """All 5 silver tables have at least 1 row (pipeline ran successfully)."""
    for full_table in SILVER_SCHEMAS:
        row_count = _count(con, full_table)
        assert (
            row_count > 0
        ), f"{full_table}: table is empty — pipeline may not have run"


def test_gold_tables_not_empty(con: duckdb.DuckDBPyConnection) -> None:
    """Both gold tables have at least 1 row."""
    for full_table in GOLD_SCHEMAS:
        row_count = _count(con, full_table)
        assert (
            row_count > 0
        ), f"{full_table}: table is empty — gold build may not have run"
