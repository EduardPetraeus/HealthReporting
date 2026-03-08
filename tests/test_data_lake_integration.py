"""
Integration tests: API sources → parquet in data lake → bronze config alignment.

Validates that:
1. All API source writers resolve to /Users/Shared/data_lake (canonical root)
2. Parquet files exist on disk for each active source
3. sources_config.yaml relative_paths match actual data lake structure
4. Parquet files are readable and contain expected metadata columns
5. No data exists in the legacy fallback location (~/health_data_lake/)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

# Ensure platform utils are importable
_REPO_ROOT = Path(__file__).resolve().parents[1]
_PLATFORM_DIR = _REPO_ROOT / "health_unified_platform"
if str(_PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(_PLATFORM_DIR))

from health_platform.utils.paths import get_data_lake_root  # noqa: E402

CANONICAL_ROOT = Path("/Users/Shared/data_lake")
LEGACY_FALLBACK = Path.home() / "health_data_lake"

SOURCES_CONFIG = (
    _REPO_ROOT
    / "health_unified_platform"
    / "health_environment"
    / "config"
    / "sources_config.yaml"
)


@pytest.fixture(scope="module")
def sources_config() -> list[dict]:
    """Load sources_config.yaml once for all tests."""
    with open(SOURCES_CONFIG) as f:
        cfg = yaml.safe_load(f)
    return cfg.get("sources", [])


# ---------------------------------------------------------------------------
# 1. Path resolution — all writers must point to canonical root
# ---------------------------------------------------------------------------


class TestPathResolution:
    """All writers must resolve to /Users/Shared/data_lake."""

    def test_data_lake_root_is_canonical(self):
        root = get_data_lake_root()
        assert (
            root == CANONICAL_ROOT
        ), f"get_data_lake_root() returned {root}, expected {CANONICAL_ROOT}"

    def test_oura_writer_resolves_to_canonical(self):
        from health_platform.source_connectors.oura.writer import DATA_LAKE_ROOT

        assert (
            DATA_LAKE_ROOT == CANONICAL_ROOT / "oura" / "raw"
        ), f"Oura writer root: {DATA_LAKE_ROOT}"

    def test_strava_writer_resolves_to_canonical(self):
        from health_platform.source_connectors.strava.writer import DATA_LAKE_ROOT

        assert (
            DATA_LAKE_ROOT == CANONICAL_ROOT / "strava" / "raw"
        ), f"Strava writer root: {DATA_LAKE_ROOT}"

    def test_no_data_in_legacy_fallback(self):
        """Legacy ~/health_data_lake/ must not contain active parquet data."""
        if not LEGACY_FALLBACK.exists():
            return  # Clean — nothing there
        parquet_files = list(LEGACY_FALLBACK.rglob("*.parquet"))
        assert len(parquet_files) == 0, (
            f"Found {len(parquet_files)} parquet files in legacy fallback "
            f"{LEGACY_FALLBACK}. Data should be in {CANONICAL_ROOT}."
        )


# ---------------------------------------------------------------------------
# 2. Data existence — parquet files on disk for each active API source
# ---------------------------------------------------------------------------


class TestDataExistence:
    """Verify parquet files exist for each API-fetched source."""

    @pytest.mark.parametrize(
        "source_dir,min_files",
        [
            ("oura/raw/daily_sleep", 1),
            ("oura/raw/daily_activity", 1),
            ("oura/raw/daily_readiness", 1),
            ("oura/raw/heartrate", 1),
            ("oura/raw/workout", 1),
            ("oura/raw/daily_spo2", 1),
            ("oura/raw/daily_stress", 1),
            ("strava/raw/activities", 1),
            ("strava/raw/athlete_stats", 1),
        ],
    )
    def test_parquet_files_exist(self, source_dir: str, min_files: int):
        data_path = CANONICAL_ROOT / source_dir
        if not data_path.exists():
            pytest.skip(f"{data_path} not yet populated")
        parquet_files = list(data_path.rglob("*.parquet"))
        assert (
            len(parquet_files) >= min_files
        ), f"Expected >= {min_files} parquet files in {data_path}, found {len(parquet_files)}"

    def test_strava_directory_exists(self):
        strava_path = CANONICAL_ROOT / "strava" / "raw"
        assert strava_path.exists(), f"Strava raw directory missing: {strava_path}"

    def test_oura_directory_exists(self):
        oura_path = CANONICAL_ROOT / "oura" / "raw"
        assert oura_path.exists(), f"Oura raw directory missing: {oura_path}"


# ---------------------------------------------------------------------------
# 3. Config alignment — sources_config.yaml matches disk structure
# ---------------------------------------------------------------------------


class TestSourcesConfigAlignment:
    """sources_config.yaml relative_paths must match data on disk."""

    @pytest.mark.parametrize(
        "source_name,relative_path",
        [
            ("strava_activities", "strava/raw/activities/**/*.parquet"),
            ("strava_athlete_stats", "strava/raw/athlete_stats/**/*.parquet"),
            ("oura_daily_sleep", "oura/raw/daily_sleep/**/*.parquet"),
            ("oura_daily_activity", "oura/raw/daily_activity/**/*.parquet"),
            ("oura_daily_readiness", "oura/raw/daily_readiness/**/*.parquet"),
            ("oura_heartrate", "oura/raw/heartrate/**/*.parquet"),
            ("oura_workout", "oura/raw/workout/**/*.parquet"),
            ("oura_daily_spo2", "oura/raw/daily_spo2/**/*.parquet"),
            ("oura_daily_stress", "oura/raw/daily_stress/**/*.parquet"),
            ("weather_open_meteo", "weather/raw/open_meteo/**/*.parquet"),
        ],
    )
    def test_config_entry_matches_disk(
        self, sources_config, source_name: str, relative_path: str
    ):
        """Verify each config entry exists and its glob resolves to files."""
        # Find entry in config
        entry = next((s for s in sources_config if s["name"] == source_name), None)
        assert (
            entry is not None
        ), f"Source '{source_name}' missing from sources_config.yaml"
        assert entry["relative_path"] == relative_path, (
            f"Path mismatch for {source_name}: "
            f"config={entry['relative_path']}, expected={relative_path}"
        )

        # Check if files exist on disk (skip if source not yet populated)
        base_dir = relative_path.split("/**")[0]
        full_path = CANONICAL_ROOT / base_dir
        if not full_path.exists():
            pytest.skip(f"{full_path} not yet populated")
        files = list(full_path.rglob("*.parquet"))
        assert len(files) > 0, (
            f"Config entry '{source_name}' glob resolves to 0 files at "
            f"{CANONICAL_ROOT / relative_path}"
        )

    def test_all_api_sources_have_config_entries(self, sources_config):
        """Every API source directory must have a matching config entry."""
        config_names = {s["name"] for s in sources_config}
        required = [
            "strava_activities",
            "strava_athlete_stats",
            "oura_daily_sleep",
            "oura_daily_activity",
            "oura_daily_readiness",
            "oura_heartrate",
            "oura_workout",
            "oura_daily_spo2",
            "oura_daily_stress",
        ]
        for name in required:
            assert (
                name in config_names
            ), f"Required source '{name}' missing from sources_config.yaml"

    def test_all_config_entries_have_target_table(self, sources_config):
        """Every source entry must define a target_table."""
        for entry in sources_config:
            assert (
                "target_table" in entry
            ), f"Source '{entry['name']}' missing target_table"
            assert (
                entry["target_table"].startswith("stg_")
                or entry.get("target_schema") == "silver"
            ), (
                f"Bronze table '{entry['target_table']}' for "
                f"'{entry['name']}' should start with 'stg_'"
            )


# ---------------------------------------------------------------------------
# 4. Parquet quality — files are readable with metadata columns
# ---------------------------------------------------------------------------


class TestParquetQuality:
    """Parquet files must be readable and contain metadata columns."""

    def _read_single_parquet(self, source_dir: str) -> pq.ParquetFile | None:
        """Return a ParquetFile for the first parquet in a source dir."""
        data_path = CANONICAL_ROOT / source_dir
        if not data_path.exists():
            return None
        files = list(data_path.rglob("*.parquet"))
        if not files:
            return None
        return pq.ParquetFile(files[0])

    @pytest.mark.parametrize(
        "source_dir",
        [
            "oura/raw/daily_sleep",
            "oura/raw/daily_activity",
            "strava/raw/activities",
            "strava/raw/athlete_stats",
        ],
    )
    def test_parquet_is_readable(self, source_dir: str):
        pf = self._read_single_parquet(source_dir)
        if pf is None:
            pytest.skip(f"No parquet files in {source_dir}")
        table = pf.read()
        assert table.num_rows > 0, f"Empty parquet file in {source_dir}"

    @pytest.mark.parametrize(
        "source_dir",
        [
            "oura/raw/daily_sleep",
            "strava/raw/activities",
        ],
    )
    def test_metadata_columns_present(self, source_dir: str):
        """API-ingested parquet must have _ingested_at and _source_env."""
        pf = self._read_single_parquet(source_dir)
        if pf is None:
            pytest.skip(f"No parquet files in {source_dir}")
        columns = pf.schema_arrow.names
        assert "_ingested_at" in columns, f"Missing _ingested_at in {source_dir}"
        assert "_source_env" in columns, f"Missing _source_env in {source_dir}"

    @pytest.mark.parametrize(
        "source_dir",
        [
            "strava/raw/activities",
        ],
    )
    def test_strava_activity_schema(self, source_dir: str):
        """Strava activities must contain key activity fields."""
        pf = self._read_single_parquet(source_dir)
        if pf is None:
            pytest.skip(f"No parquet files in {source_dir}")
        columns = set(pf.schema_arrow.names)
        required = {
            "activity_id",
            "name",
            "activity_type",
            "start_date",
            "distance_m",
            "moving_time_s",
        }
        missing = required - columns
        assert not missing, f"Missing columns in strava activities: {missing}"


# ---------------------------------------------------------------------------
# 5. Hive partitioning — verify partition structure
# ---------------------------------------------------------------------------


class TestHivePartitioning:
    """Verify Hive-style year=/month=/day= partitioning."""

    @pytest.mark.parametrize(
        "source_dir",
        [
            "strava/raw/activities",
            "oura/raw/daily_sleep",
        ],
    )
    def test_partition_structure(self, source_dir: str):
        data_path = CANONICAL_ROOT / source_dir
        if not data_path.exists():
            pytest.skip(f"{data_path} not populated")
        # Find a parquet file and check its parents
        sample = next(data_path.rglob("*.parquet"), None)
        if sample is None:
            pytest.skip(f"No parquet in {data_path}")
        parts = [p.name for p in sample.parents if "=" in p.name]
        partition_keys = {p.split("=")[0] for p in parts}
        assert {"year", "month", "day"}.issubset(
            partition_keys
        ), f"Expected year/month/day partitions, got {partition_keys} for {sample}"
