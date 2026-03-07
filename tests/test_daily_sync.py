"""Tests for daily_sync.sh pipeline configuration.

Validates that the sync script is correctly wired to all sources,
merge scripts, and AI components without actually running API calls.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "daily_sync.sh"
MERGE_DIR = (
    REPO_ROOT
    / "health_unified_platform"
    / "health_platform"
    / "transformation_logic"
    / "dbt"
    / "merge"
    / "silver"
)


class TestDailySyncStructure:
    """Verify daily_sync.sh is properly configured."""

    @pytest.fixture(autouse=True)
    def _load_script(self):
        self.content = SYNC_SCRIPT.read_text()

    def test_script_exists_and_is_executable(self):
        assert SYNC_SCRIPT.exists()
        assert SYNC_SCRIPT.stat().st_mode & 0o111, "Script must be executable"

    def test_bash_syntax_valid(self):
        result = subprocess.run(
            ["bash", "-n", str(SYNC_SCRIPT)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Syntax error: {result.stderr}"

    def test_has_six_steps(self):
        assert "Step 1/6" in self.content
        assert "Step 2/6" in self.content
        assert "Step 3/6" in self.content
        assert "Step 4/6" in self.content
        assert "Step 5/6" in self.content
        assert "Step 6/6" in self.content

    def test_fetches_oura(self):
        assert "run_oura.py" in self.content

    def test_fetches_withings(self):
        assert "run_withings.py" in self.content

    def test_fetches_strava(self):
        assert "run_strava.py" in self.content

    def test_fetches_weather(self):
        assert "run_weather.py" in self.content

    def test_merge_glob_covers_all_sources(self):
        """Merge loop must use merge_*.sql, not merge_oura_*.sql."""
        assert "merge_*.sql" in self.content
        # Must NOT be restricted to oura only
        assert "merge_oura_*.sql" not in self.content

    def test_runs_correlation_engine(self):
        assert "compute_all_correlations" in self.content

    def test_runs_baseline_computer(self):
        assert "compute_all_baselines" in self.content
        assert "compute_demographics" in self.content

    def test_runs_bronze_ingestion(self):
        assert "ingestion_engine" in self.content

    def test_runs_daily_summary(self):
        assert "generate_summary_for_pipeline" in self.content

    def test_runs_embedding_backfill(self):
        assert "EmbeddingEngine" in self.content

    def test_uses_set_euo_pipefail(self):
        assert "set -euo pipefail" in self.content

    def test_has_error_handler(self):
        assert "trap" in self.content
        assert "on_error" in self.content

    def test_ntfy_topic_validation(self):
        """NTFY_TOPIC is validated before use in URL."""
        assert "^[a-zA-Z0-9_-]+$" in self.content

    def test_ntfy_notifications(self):
        assert "ntfy.sh" in self.content
        assert "NTFY_TOPIC" in self.content

    def test_no_hardcoded_db_path(self):
        """DB path must come from paths.py, not hardcoded."""
        assert "/Users/Shared/data_lake/database" not in self.content

    def test_platform_root_validation(self):
        """PLATFORM_ROOT must be validated before use."""
        assert "__init__.py" in self.content

    def test_db_path_from_paths_module(self):
        """DB path resolved via health_platform.utils.paths."""
        assert "health_platform.utils.paths" in self.content


class TestMergeScriptCoverage:
    """Verify all merge scripts exist for all sources."""

    def _merge_scripts(self):
        return sorted(MERGE_DIR.glob("merge_*.sql"))

    def test_merge_dir_exists(self):
        assert MERGE_DIR.is_dir()

    def test_oura_merge_scripts_exist(self):
        oura = [f for f in self._merge_scripts() if "oura" in f.name]
        assert len(oura) >= 8, f"Expected 8+ Oura merges, found {len(oura)}"

    def test_apple_health_merge_scripts_exist(self):
        ah = [f for f in self._merge_scripts() if "apple_health" in f.name]
        assert len(ah) >= 9, f"Expected 9+ Apple Health merges, found {len(ah)}"

    def test_withings_merge_scripts_exist(self):
        w = [f for f in self._merge_scripts() if "withings" in f.name]
        assert len(w) >= 2, f"Expected 2+ Withings merges, found {len(w)}"

    def test_strava_merge_scripts_exist(self):
        s = [f for f in self._merge_scripts() if "strava" in f.name]
        assert len(s) >= 1, f"Expected 1+ Strava merges, found {len(s)}"

    def test_weather_merge_scripts_exist(self):
        w = [f for f in self._merge_scripts() if "weather" in f.name]
        assert len(w) >= 1, f"Expected 1+ Weather merges, found {len(w)}"

    def test_lifesum_merge_scripts_exist(self):
        lf = [f for f in self._merge_scripts() if "lifesum" in f.name]
        assert len(lf) >= 1, f"Expected 1+ Lifesum merges, found {len(lf)}"

    def test_total_merge_script_count(self):
        total = self._merge_scripts()
        assert len(total) >= 23, f"Expected 23+ merge scripts, found {len(total)}"


class TestConnectorEntryPoints:
    """Verify all source connector entry points exist."""

    CONNECTORS_ROOT = (
        REPO_ROOT / "health_unified_platform" / "health_platform" / "source_connectors"
    )

    @pytest.mark.parametrize(
        "source,script",
        [
            ("oura", "run_oura.py"),
            ("withings", "run_withings.py"),
            ("strava", "run_strava.py"),
            ("weather", "run_weather.py"),
        ],
    )
    def test_connector_entry_point_exists(self, source, script):
        path = self.CONNECTORS_ROOT / source / script
        assert path.exists(), f"Missing connector: {path}"
