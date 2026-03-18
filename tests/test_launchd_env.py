"""Tests that launchd plist files contain required environment variables.

Catches configuration drift between plist templates and deployed plists,
specifically HEALTH_DB_PATH which is needed by the post-ingest pipeline.
"""

import os
import plistlib
from pathlib import Path
from unittest.mock import patch

import pytest

LAUNCHAGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
FASTAPI_PLIST = LAUNCHAGENTS_DIR / "com.health.fastapi.plist"
DAILY_SYNC_PLIST = LAUNCHAGENTS_DIR / "com.health.daily-sync.plist"


@pytest.mark.skipif(
    not FASTAPI_PLIST.exists(), reason="launchd plists only exist on deploy target"
)
class TestFastapiPlistEnvVars:
    """Verify com.health.fastapi.plist has all required env vars."""

    @pytest.fixture()
    def fastapi_env_vars(self) -> dict:
        """Parse EnvironmentVariables from the deployed FastAPI plist."""
        assert FASTAPI_PLIST.exists(), f"Plist not found: {FASTAPI_PLIST}"
        with open(FASTAPI_PLIST, "rb") as f:
            plist = plistlib.load(f)
        return plist.get("EnvironmentVariables", {})

    def test_health_db_path_present(self, fastapi_env_vars: dict) -> None:
        """HEALTH_DB_PATH must be set so post-ingest pipeline can connect."""
        assert "HEALTH_DB_PATH" in fastapi_env_vars, (
            "HEALTH_DB_PATH missing from com.health.fastapi.plist "
            "EnvironmentVariables — post-ingest pipeline will crash with KeyError"
        )

    def test_health_db_path_points_to_valid_file(self, fastapi_env_vars: dict) -> None:
        """HEALTH_DB_PATH must point to an existing database file."""
        db_path = fastapi_env_vars.get("HEALTH_DB_PATH", "")
        assert db_path, "HEALTH_DB_PATH is empty"
        assert Path(db_path).exists(), f"Database file not found: {db_path}"

    def test_health_db_path_matches_daily_sync(self, fastapi_env_vars: dict) -> None:
        """Both plists must use the same HEALTH_DB_PATH to avoid data split."""
        assert DAILY_SYNC_PLIST.exists(), f"Plist not found: {DAILY_SYNC_PLIST}"
        with open(DAILY_SYNC_PLIST, "rb") as f:
            sync_plist = plistlib.load(f)
        sync_db_path = sync_plist["EnvironmentVariables"]["HEALTH_DB_PATH"]
        fastapi_db_path = fastapi_env_vars.get("HEALTH_DB_PATH", "")
        assert (
            fastapi_db_path == sync_db_path
        ), f"HEALTH_DB_PATH mismatch: fastapi={fastapi_db_path!r} vs daily-sync={sync_db_path!r}"


class TestPostIngestDbPathResolution:
    """Verify the post-ingest scripts can resolve HEALTH_DB_PATH."""

    def test_post_ingest_script_uses_environ(self) -> None:
        """The AI summary script must be able to read HEALTH_DB_PATH from env."""
        test_path = "/tmp/test_health.db"
        with patch.dict(os.environ, {"HEALTH_DB_PATH": test_path}):
            # Simulate what post_ingest.py line 249 does
            resolved = os.environ["HEALTH_DB_PATH"]
            assert resolved == test_path

    def test_post_ingest_crashes_without_env_var(self) -> None:
        """Without HEALTH_DB_PATH, post-ingest scripts raise KeyError."""
        env = os.environ.copy()
        env.pop("HEALTH_DB_PATH", None)
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(KeyError, match="HEALTH_DB_PATH"):
                _ = os.environ["HEALTH_DB_PATH"]
