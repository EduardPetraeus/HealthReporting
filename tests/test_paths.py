"""Tests for the cross-platform path resolution module."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)

from health_platform.utils.paths import (
    get_data_lake_root,
    get_db_dir,
    get_db_path,
    get_lab_dir,
    get_log_dir,
    get_manual_dir,
)


class TestPathResolution:
    """Test the path resolution chain."""

    def test_env_var_takes_priority(self):
        with patch.dict(os.environ, {"HEALTH_DATA_LAKE": "/tmp/test_lake"}):
            assert get_data_lake_root() == Path("/tmp/test_lake")

    def test_db_path_env_override(self):
        with patch.dict(os.environ, {"HEALTH_DB_PATH": "/tmp/test.db"}):
            assert get_db_path() == Path("/tmp/test.db")

    def test_db_path_includes_env_name(self):
        with patch.dict(os.environ, {"HEALTH_DB_PATH": ""}, clear=False):
            os.environ.pop("HEALTH_DB_PATH", None)
            path = get_db_path(env="dev")
            assert "health_dw_dev.db" in str(path)

    def test_db_path_prd(self):
        with patch.dict(os.environ, {"HEALTH_DB_PATH": ""}, clear=False):
            os.environ.pop("HEALTH_DB_PATH", None)
            path = get_db_path(env="prd")
            assert "health_dw_prd.db" in str(path)

    def test_derived_paths_under_data_lake(self):
        with patch.dict(os.environ, {"HEALTH_DATA_LAKE": "/tmp/lake"}):
            assert get_db_dir() == Path("/tmp/lake/database")
            assert get_log_dir() == Path("/tmp/lake/logs")
            assert get_manual_dir() == Path("/tmp/lake/manual")
            assert get_lab_dir() == Path("/tmp/lake/manual/lab_results")

    def test_returns_path_objects(self):
        assert isinstance(get_data_lake_root(), Path)
        assert isinstance(get_db_dir(), Path)
        assert isinstance(get_db_path(), Path)
        assert isinstance(get_log_dir(), Path)
        assert isinstance(get_manual_dir(), Path)
        assert isinstance(get_lab_dir(), Path)


class TestPlatformDefaults:
    """Test platform detection for defaults."""

    def test_macos_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HEALTH_DATA_LAKE", None)
            from health_platform.utils.paths import _platform_default_data_lake

            with patch(
                "health_platform.utils.paths.platform.system", return_value="Darwin"
            ):
                assert _platform_default_data_lake() == "/Users/Shared/data_lake"

    def test_linux_default(self):
        from health_platform.utils.paths import _platform_default_data_lake

        with patch("health_platform.utils.paths.platform.system", return_value="Linux"):
            with patch.dict(os.environ, {"XDG_DATA_HOME": "/home/user/.local/share"}):
                result = _platform_default_data_lake()
                assert "health-platform" in result
                assert "data_lake" in result

    def test_windows_default(self):
        from health_platform.utils.paths import _platform_default_data_lake

        with patch(
            "health_platform.utils.paths.platform.system", return_value="Windows"
        ):
            with patch.dict(
                os.environ, {"LOCALAPPDATA": "C:\\Users\\test\\AppData\\Local"}
            ):
                result = _platform_default_data_lake()
                assert "health-platform" in result
