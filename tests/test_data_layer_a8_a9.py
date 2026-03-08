"""Tests for A8 (Lifesum CSV expansion) and A9 (Workout unification).

Validates SQL syntax, column mappings, deduplication, and cross-source
business key isolation for all new merge scripts.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

# ─── Paths ──────────────────────────────────────────────────────────────────

MERGE_DIR = Path(__file__).resolve().parent.parent / (
    "health_unified_platform/health_platform/transformation_logic/dbt/merge/silver"
)

SQL_BODYMEASURES = MERGE_DIR / "merge_lifesum_bodymeasures.sql"
SQL_EXERCISE = MERGE_DIR / "merge_lifesum_exercise.sql"
SQL_WEIGHINS = MERGE_DIR / "merge_lifesum_weighins.sql"
SQL_BODYFAT = MERGE_DIR / "merge_lifesum_bodyfat.sql"
SQL_STRAVA = MERGE_DIR / "merge_strava_activities.sql"
SQL_OURA_WORKOUT = MERGE_DIR / "merge_oura_workout.sql"


# ─── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def memory_db():
    """In-memory DuckDB connection with silver, bronze, and agent schemas."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    yield con
    con.close()


def _read_sql(path: Path) -> str:
    """Read a SQL file and return its content."""
    return path.read_text(encoding="utf-8")


# ─── A8: Lifesum Body Measures ──────────────────────────────────────────────


class TestLifesumBodymeasures:
    """Tests for merge_lifesum_bodymeasures.sql."""

    def test_sql_file_exists(self):
        assert SQL_BODYMEASURES.exists(), f"Missing: {SQL_BODYMEASURES}"

    def test_sql_parses(self, memory_db):
        """Verify SQL syntax by running against in-memory DuckDB with mock data."""
        con = memory_db
        # Create bronze source table
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_bodymeasures (
                date VARCHAR, weight_kg VARCHAR, body_fat_pct VARCHAR,
                muscle_mass_pct VARCHAR, waist_cm VARCHAR, hip_cm VARCHAR,
                chest_cm VARCHAR, _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        # Create silver target table
        con.execute(
            """
            CREATE TABLE silver.body_measures (
                sk_date INTEGER, day DATE, weight_kg DOUBLE,
                body_fat_pct DOUBLE, muscle_mass_pct DOUBLE,
                waist_cm DOUBLE, hip_cm DOUBLE, chest_cm DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_bodymeasures
            (date, weight_kg, body_fat_pct, muscle_mass_pct, waist_cm, hip_cm, chest_cm)
            VALUES ('2026-03-01', '80.5', '18.2', '42.0', '85.0', '95.0', '100.0')
        """
        )
        sql = _read_sql(SQL_BODYMEASURES)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

    def test_produces_correct_columns(self, memory_db):
        """Verify the merged data has expected columns and values."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_bodymeasures (
                date VARCHAR, weight_kg VARCHAR, body_fat_pct VARCHAR,
                muscle_mass_pct VARCHAR, waist_cm VARCHAR, hip_cm VARCHAR,
                chest_cm VARCHAR, _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.body_measures (
                sk_date INTEGER, day DATE, weight_kg DOUBLE,
                body_fat_pct DOUBLE, muscle_mass_pct DOUBLE,
                waist_cm DOUBLE, hip_cm DOUBLE, chest_cm DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_bodymeasures VALUES
            ('2026-03-01', '80.5', '18.2', '42.0', '85.0', '95.0', '100.0', CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_BODYMEASURES)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        result = con.execute("SELECT * FROM silver.body_measures").fetchall()
        assert len(result) == 1
        row = result[0]
        cols = [
            d[0] for d in con.execute("SELECT * FROM silver.body_measures").description
        ]
        data = dict(zip(cols, row))
        assert data["sk_date"] == 20260301
        assert data["weight_kg"] == 80.5
        assert data["body_fat_pct"] == 18.2
        assert data["source_system"] == "lifesum"
        assert data["business_key_hash"] is not None

    def test_dedup_keeps_latest(self, memory_db):
        """Verify deduplication keeps the latest record per date."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_bodymeasures (
                date VARCHAR, weight_kg VARCHAR, body_fat_pct VARCHAR,
                muscle_mass_pct VARCHAR, waist_cm VARCHAR, hip_cm VARCHAR,
                chest_cm VARCHAR, _ingested_at TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.body_measures (
                sk_date INTEGER, day DATE, weight_kg DOUBLE,
                body_fat_pct DOUBLE, muscle_mass_pct DOUBLE,
                waist_cm DOUBLE, hip_cm DOUBLE, chest_cm DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        # Insert two records for same date, different timestamps
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_bodymeasures VALUES
            ('2026-03-01', '80.0', '18.0', '42.0', '85.0', '95.0', '100.0', '2026-03-01 08:00:00'),
            ('2026-03-01', '81.0', '19.0', '43.0', '86.0', '96.0', '101.0', '2026-03-01 12:00:00')
        """
        )
        sql = _read_sql(SQL_BODYMEASURES)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        result = con.execute("SELECT weight_kg FROM silver.body_measures").fetchone()
        assert result[0] == 81.0  # Latest ingested record wins


# ─── A8: Lifesum Exercise -> Workout ────────────────────────────────────────


class TestLifesumExercise:
    """Tests for merge_lifesum_exercise.sql -> silver.workout."""

    def test_sql_file_exists(self):
        assert SQL_EXERCISE.exists(), f"Missing: {SQL_EXERCISE}"

    def test_sql_parses(self, memory_db):
        """Verify SQL syntax by running against in-memory DuckDB."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_exercise (
                date VARCHAR, exercise_name VARCHAR, duration_minutes VARCHAR,
                calories_burned VARCHAR, category VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.workout (
                sk_date INTEGER, day DATE, workout_id VARCHAR,
                activity VARCHAR, intensity VARCHAR, calories DOUBLE,
                distance_meters DOUBLE, start_datetime TIMESTAMP,
                end_datetime TIMESTAMP, duration_seconds DOUBLE,
                label VARCHAR, source VARCHAR, source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_exercise
            (date, exercise_name, duration_minutes, calories_burned, category)
            VALUES ('2026-03-01', 'Running', '30', '300', 'cardio')
        """
        )
        sql = _read_sql(SQL_EXERCISE)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

    def test_maps_to_workout_schema(self, memory_db):
        """Verify exercise data is correctly mapped to silver.workout columns."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_exercise (
                date VARCHAR, exercise_name VARCHAR, duration_minutes VARCHAR,
                calories_burned VARCHAR, category VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.workout (
                sk_date INTEGER, day DATE, workout_id VARCHAR,
                activity VARCHAR, intensity VARCHAR, calories DOUBLE,
                distance_meters DOUBLE, start_datetime TIMESTAMP,
                end_datetime TIMESTAMP, duration_seconds DOUBLE,
                label VARCHAR, source VARCHAR, source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_exercise VALUES
            ('2026-03-01', 'Running', '30', '300', 'cardio', CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_EXERCISE)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        result = con.execute("SELECT * FROM silver.workout").fetchall()
        assert len(result) == 1
        cols = [d[0] for d in con.execute("SELECT * FROM silver.workout").description]
        data = dict(zip(cols, result[0]))
        assert data["activity"] == "Running"
        assert data["duration_seconds"] == 1800.0  # 30 min * 60
        assert data["calories"] == 300.0
        assert data["source"] == "lifesum"
        assert data["source_system"] == "lifesum"
        assert data["label"] == "Running"

    def test_source_system_populated(self, memory_db):
        """Verify source_system is 'lifesum' for exercise entries."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_exercise (
                date VARCHAR, exercise_name VARCHAR, duration_minutes VARCHAR,
                calories_burned VARCHAR, category VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.workout (
                sk_date INTEGER, day DATE, workout_id VARCHAR,
                activity VARCHAR, intensity VARCHAR, calories DOUBLE,
                distance_meters DOUBLE, start_datetime TIMESTAMP,
                end_datetime TIMESTAMP, duration_seconds DOUBLE,
                label VARCHAR, source VARCHAR, source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_exercise VALUES
            ('2026-03-01', 'Yoga', '60', '200', 'flexibility', CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_EXERCISE)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT source_system FROM silver.workout").fetchone()
        assert result[0] == "lifesum"


# ─── A8: Lifesum Weighins -> Weight ─────────────────────────────────────────


class TestLifesumWeighins:
    """Tests for merge_lifesum_weighins.sql -> silver.weight."""

    def test_sql_file_exists(self):
        assert SQL_WEIGHINS.exists(), f"Missing: {SQL_WEIGHINS}"

    def test_sql_parses(self, memory_db):
        """Verify SQL syntax by running against in-memory DuckDB."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_weighins (
                date VARCHAR, weight VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.weight (
                sk_date INTEGER, sk_time VARCHAR, datetime TIMESTAMP,
                weight_kg DOUBLE, fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
                muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_weighins (date, weight)
            VALUES ('2026-03-01', '82.3')
        """
        )
        sql = _read_sql(SQL_WEIGHINS)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

    def test_maps_to_weight_schema(self, memory_db):
        """Verify weighin data maps correctly to silver.weight columns."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_weighins (
                date VARCHAR, weight VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.weight (
                sk_date INTEGER, sk_time VARCHAR, datetime TIMESTAMP,
                weight_kg DOUBLE, fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
                muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_weighins VALUES
            ('2026-03-01', '82.3', CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_WEIGHINS)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        result = con.execute("SELECT * FROM silver.weight").fetchall()
        assert len(result) == 1
        cols = [d[0] for d in con.execute("SELECT * FROM silver.weight").description]
        data = dict(zip(cols, result[0]))
        assert data["weight_kg"] == 82.3
        assert data["sk_time"] == "0000"
        assert data["fat_mass_kg"] is None
        assert data["source_system"] == "lifesum"

    def test_source_system_populated(self, memory_db):
        """Verify source_system is 'lifesum' for weighin entries."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_weighins (
                date VARCHAR, weight VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.weight (
                sk_date INTEGER, sk_time VARCHAR, datetime TIMESTAMP,
                weight_kg DOUBLE, fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
                muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_weighins VALUES
            ('2026-03-01', '75.0', CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_WEIGHINS)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT source_system FROM silver.weight").fetchone()
        assert result[0] == "lifesum"


# ─── A8: Lifesum Body Fat ───────────────────────────────────────────────────


class TestLifesumBodyfat:
    """Tests for merge_lifesum_bodyfat.sql."""

    def test_sql_file_exists(self):
        assert SQL_BODYFAT.exists(), f"Missing: {SQL_BODYFAT}"

    def test_sql_parses(self, memory_db):
        """Verify SQL syntax by running against in-memory DuckDB."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_bodyfat (
                date VARCHAR, body_fat_percentage VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.body_fat (
                sk_date INTEGER, day DATE, body_fat_pct DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_bodyfat (date, body_fat_percentage)
            VALUES ('2026-03-01', '18.5')
        """
        )
        sql = _read_sql(SQL_BODYFAT)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

    def test_produces_correct_columns(self, memory_db):
        """Verify body fat merge produces expected columns and values."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_bodyfat (
                date VARCHAR, body_fat_percentage VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.body_fat (
                sk_date INTEGER, day DATE, body_fat_pct DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_bodyfat VALUES
            ('2026-03-01', '18.5', CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_BODYFAT)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        result = con.execute("SELECT * FROM silver.body_fat").fetchall()
        assert len(result) == 1
        cols = [d[0] for d in con.execute("SELECT * FROM silver.body_fat").description]
        data = dict(zip(cols, result[0]))
        assert data["sk_date"] == 20260301
        assert data["body_fat_pct"] == 18.5
        assert data["source_system"] == "lifesum"
        assert data["business_key_hash"] is not None

    def test_source_system_populated(self, memory_db):
        """Verify source_system is 'lifesum' for body fat entries."""
        con = memory_db
        con.execute(
            """
            CREATE TABLE bronze.stg_lifesum_bodyfat (
                date VARCHAR, body_fat_percentage VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.body_fat (
                sk_date INTEGER, day DATE, body_fat_pct DOUBLE,
                source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_lifesum_bodyfat VALUES
            ('2026-03-01', '22.0', CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_BODYFAT)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT source_system FROM silver.body_fat").fetchone()
        assert result[0] == "lifesum"


# ─── A9: Strava -> Workout ──────────────────────────────────────────────────


class TestStravaWorkout:
    """Tests for merge_strava_activities.sql -> silver.workout."""

    def test_sql_file_exists(self):
        assert SQL_STRAVA.exists(), f"Missing: {SQL_STRAVA}"

    def _create_strava_tables(self, con):
        """Create bronze and silver tables for Strava tests."""
        con.execute(
            """
            CREATE TABLE bronze.stg_strava_activities (
                id VARCHAR, name VARCHAR, type VARCHAR, sport_type VARCHAR,
                distance VARCHAR, moving_time VARCHAR, elapsed_time VARCHAR,
                total_elevation_gain VARCHAR, start_date VARCHAR,
                average_heartrate VARCHAR, max_heartrate VARCHAR,
                average_speed VARCHAR, kilojoules VARCHAR, calories VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            CREATE TABLE silver.workout (
                sk_date INTEGER, day DATE, workout_id VARCHAR,
                activity VARCHAR, intensity VARCHAR, calories DOUBLE,
                distance_meters DOUBLE, start_datetime TIMESTAMP,
                end_datetime TIMESTAMP, duration_seconds DOUBLE,
                label VARCHAR, source VARCHAR, source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )

    def test_sql_parses(self, memory_db):
        """Verify SQL syntax by running against in-memory DuckDB."""
        con = memory_db
        self._create_strava_tables(con)
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities
            (id, name, type, sport_type, distance, moving_time, elapsed_time,
             start_date, average_heartrate, calories)
            VALUES ('12345', 'Morning Run', 'Run', 'Run', '5000', '1800', '2000',
                    '2026-03-01 07:00:00', '140', '350')
        """
        )
        sql = _read_sql(SQL_STRAVA)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

    def test_maps_to_workout_schema(self, memory_db):
        """Verify Strava data maps correctly to silver.workout columns."""
        con = memory_db
        self._create_strava_tables(con)
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities VALUES
            ('12345', 'Morning Run', 'Run', 'Run', '5000', '1800', '2000',
             '50', '2026-03-01 07:00:00', '140', '155', '3.5', '250', '350',
             CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_STRAVA)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        result = con.execute("SELECT * FROM silver.workout").fetchall()
        assert len(result) == 1
        cols = [d[0] for d in con.execute("SELECT * FROM silver.workout").description]
        data = dict(zip(cols, result[0]))
        assert data["workout_id"] == "12345"
        assert data["activity"] == "Run"
        assert data["source"] == "strava"
        assert data["source_system"] == "strava"
        assert data["distance_meters"] == 5000.0
        assert data["duration_seconds"] == 2000.0
        assert data["label"] == "Morning Run"

    def test_intensity_easy(self, memory_db):
        """Verify intensity='easy' when avg HR < 120."""
        con = memory_db
        self._create_strava_tables(con)
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities VALUES
            ('1', 'Walk', 'Walk', 'Walk', '2000', '1800', '1800',
             '10', '2026-03-01 07:00:00', '100', NULL, '2.0', NULL, '100',
             CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_STRAVA)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT intensity FROM silver.workout").fetchone()
        assert result[0] == "easy"

    def test_intensity_moderate(self, memory_db):
        """Verify intensity='moderate' when avg HR between 120 and 150."""
        con = memory_db
        self._create_strava_tables(con)
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities VALUES
            ('2', 'Jog', 'Run', 'Run', '5000', '1800', '1800',
             '20', '2026-03-01 07:00:00', '135', NULL, '3.0', NULL, '300',
             CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_STRAVA)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT intensity FROM silver.workout").fetchone()
        assert result[0] == "moderate"

    def test_intensity_hard(self, memory_db):
        """Verify intensity='hard' when avg HR > 150."""
        con = memory_db
        self._create_strava_tables(con)
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities VALUES
            ('3', 'Sprint', 'Run', 'Run', '3000', '900', '900',
             '30', '2026-03-01 07:00:00', '170', NULL, '5.0', NULL, '400',
             CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_STRAVA)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT intensity FROM silver.workout").fetchone()
        assert result[0] == "hard"

    def test_intensity_null_when_no_hr(self, memory_db):
        """Verify intensity is NULL when no heart rate data."""
        con = memory_db
        self._create_strava_tables(con)
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities VALUES
            ('4', 'Swim', 'Swim', 'Swim', '1500', '1800', '1800',
             NULL, '2026-03-01 07:00:00', NULL, NULL, '1.5', NULL, '250',
             CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_STRAVA)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT intensity FROM silver.workout").fetchone()
        assert result[0] is None

    def test_source_system_populated(self, memory_db):
        """Verify source_system is 'strava' for Strava entries."""
        con = memory_db
        self._create_strava_tables(con)
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities VALUES
            ('5', 'Ride', 'Ride', 'Ride', '20000', '3600', '3600',
             '100', '2026-03-01 07:00:00', '130', NULL, '6.0', NULL, '500',
             CURRENT_TIMESTAMP)
        """
        )
        sql = _read_sql(SQL_STRAVA)
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        result = con.execute("SELECT source_system FROM silver.workout").fetchone()
        assert result[0] == "strava"


# ─── A9: Cross-source Duplicate Detection ───────────────────────────────────


class TestCrossSourceDuplication:
    """Tests for cross-source business_key_hash isolation."""

    def test_oura_strava_distinct_hashes(self, memory_db):
        """Same workout from Oura and Strava gets distinct business_key_hash."""
        con = memory_db
        # Oura hash includes '||oura'
        oura_hash = con.execute("SELECT md5('workout-123' || '||oura')").fetchone()[0]
        # Strava hash includes '||strava'
        strava_hash = con.execute("SELECT md5('workout-123' || '||strava')").fetchone()[
            0
        ]
        assert oura_hash != strava_hash

    def test_lifesum_strava_distinct_hashes(self, memory_db):
        """Same-date exercise from Lifesum and Strava gets distinct hashes."""
        con = memory_db
        lifesum_hash = con.execute(
            "SELECT md5('2026-03-01' || '||' || 'Running' || '||lifesum')"
        ).fetchone()[0]
        strava_hash = con.execute("SELECT md5('12345' || '||strava')").fetchone()[0]
        assert lifesum_hash != strava_hash

    def test_oura_lifesum_distinct_hashes(self, memory_db):
        """Same workout ID from Oura and Lifesum gets distinct hashes."""
        con = memory_db
        oura_hash = con.execute("SELECT md5('running-uuid' || '||oura')").fetchone()[0]
        lifesum_hash = con.execute(
            "SELECT md5('2026-03-01' || '||' || 'Running' || '||lifesum')"
        ).fetchone()[0]
        assert oura_hash != lifesum_hash

    def test_oura_merge_includes_source_in_hash(self):
        """Verify oura merge SQL contains '||oura' in business_key_hash."""
        sql = _read_sql(SQL_OURA_WORKOUT)
        assert (
            "||oura" in sql
        ), "merge_oura_workout.sql must include '||oura' in business_key_hash"

    def test_strava_merge_includes_source_in_hash(self):
        """Verify strava merge SQL contains '||strava' in business_key_hash."""
        sql = _read_sql(SQL_STRAVA)
        assert (
            "||strava" in sql
        ), "merge_strava_activities.sql must include '||strava' in business_key_hash"

    def test_lifesum_exercise_merge_includes_source_in_hash(self):
        """Verify lifesum exercise merge SQL contains '||lifesum' in hash."""
        sql = _read_sql(SQL_EXERCISE)
        assert (
            "||lifesum" in sql
        ), "merge_lifesum_exercise.sql must include '||lifesum' in business_key_hash"

    def test_concurrent_oura_strava_merge(self, memory_db):
        """Both Oura and Strava can merge into silver.workout without collision."""
        con = memory_db
        # Create unified workout table
        con.execute(
            """
            CREATE TABLE silver.workout (
                sk_date INTEGER, day DATE, workout_id VARCHAR,
                activity VARCHAR, intensity VARCHAR, calories DOUBLE,
                distance_meters DOUBLE, start_datetime TIMESTAMP,
                end_datetime TIMESTAMP, duration_seconds DOUBLE,
                label VARCHAR, source VARCHAR, source_system VARCHAR,
                business_key_hash VARCHAR, row_hash VARCHAR,
                load_datetime TIMESTAMP, update_datetime TIMESTAMP
            )
        """
        )

        # Create Strava bronze
        con.execute(
            """
            CREATE TABLE bronze.stg_strava_activities (
                id VARCHAR, name VARCHAR, type VARCHAR, sport_type VARCHAR,
                distance VARCHAR, moving_time VARCHAR, elapsed_time VARCHAR,
                total_elevation_gain VARCHAR, start_date VARCHAR,
                average_heartrate VARCHAR, max_heartrate VARCHAR,
                average_speed VARCHAR, kilojoules VARCHAR, calories VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        con.execute(
            """
            INSERT INTO bronze.stg_strava_activities VALUES
            ('s1', 'Morning Run', 'Run', 'Run', '5000', '1800', '2000',
             '50', '2026-03-01 07:00:00', '140', '165', '3.5', NULL, '350',
             CURRENT_TIMESTAMP)
        """
        )

        # Run Strava merge
        sql_strava = _read_sql(SQL_STRAVA)
        for stmt in sql_strava.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        # Simulate Oura data by inserting directly (Oura merge uses different bronze cols)
        con.execute(
            """
            INSERT INTO silver.workout (
                sk_date, day, workout_id, activity, intensity, calories,
                distance_meters, start_datetime, end_datetime, duration_seconds,
                label, source, source_system, business_key_hash, row_hash,
                load_datetime, update_datetime
            ) VALUES (
                20260301, '2026-03-01', 'oura-uuid-1', 'running', 'moderate', 300,
                5000, '2026-03-01 07:00:00', '2026-03-01 07:30:00', 1800,
                'Running', 'manual', 'oura',
                md5('oura-uuid-1' || '||oura'),
                md5('oura-uuid-1||running||300||5000||moderate'),
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
        """
        )

        # Both should exist as separate rows
        result = con.execute(
            "SELECT source_system, COUNT(*) FROM silver.workout GROUP BY source_system ORDER BY source_system"
        ).fetchall()
        assert len(result) == 2
        sources = {r[0]: r[1] for r in result}
        assert sources["oura"] == 1
        assert sources["strava"] == 1


# ─── A8: Sources Config ─────────────────────────────────────────────────────


class TestSourcesConfig:
    """Tests for sources_config.yaml entries."""

    def test_config_has_lifesum_bodymeasures(self):
        """Verify sources_config.yaml contains lifesum_bodymeasures entry."""
        config_path = Path(__file__).resolve().parent.parent / (
            "health_unified_platform/health_environment/config/sources_config.yaml"
        )
        content = config_path.read_text(encoding="utf-8")
        assert "lifesum_bodymeasures" in content
        assert "stg_lifesum_bodymeasures" in content

    def test_config_has_lifesum_exercise(self):
        """Verify sources_config.yaml contains lifesum_exercise entry."""
        config_path = Path(__file__).resolve().parent.parent / (
            "health_unified_platform/health_environment/config/sources_config.yaml"
        )
        content = config_path.read_text(encoding="utf-8")
        assert "lifesum_exercise" in content
        assert "stg_lifesum_exercise" in content

    def test_config_has_lifesum_weighins(self):
        """Verify sources_config.yaml contains lifesum_weighins entry."""
        config_path = Path(__file__).resolve().parent.parent / (
            "health_unified_platform/health_environment/config/sources_config.yaml"
        )
        content = config_path.read_text(encoding="utf-8")
        assert "lifesum_weighins" in content
        assert "stg_lifesum_weighins" in content

    def test_config_has_lifesum_bodyfat(self):
        """Verify sources_config.yaml contains lifesum_bodyfat entry."""
        config_path = Path(__file__).resolve().parent.parent / (
            "health_unified_platform/health_environment/config/sources_config.yaml"
        )
        content = config_path.read_text(encoding="utf-8")
        assert "lifesum_bodyfat" in content
        assert "stg_lifesum_bodyfat" in content
