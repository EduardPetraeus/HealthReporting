"""Cross-platform path resolution for the health platform.

Resolution chain (highest priority first):
  1. Environment variables (HEALTH_DATA_LAKE, HEALTH_DB_PATH)
  2. Machine config: ~/.config/health-platform/config.yaml
  3. environment_config.yaml (repo-level, legacy)
  4. Platform defaults (macOS, Linux, Windows)

Usage:
    from health_platform.utils.paths import get_data_lake_root, get_db_path
    lake = get_data_lake_root()
    db = get_db_path()  # uses HEALTH_ENV for dev/prd
"""

from __future__ import annotations

import os
import platform
from pathlib import Path

import yaml


def _platform_default_data_lake() -> str:
    """Return the platform-appropriate default data lake root."""
    system = platform.system()
    if system == "Darwin":
        return "/Users/Shared/data_lake"
    elif system == "Linux":
        xdg = os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))
        return str(Path(xdg) / "health-platform" / "data_lake")
    elif system == "Windows":
        local = os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))
        return str(Path(local) / "health-platform" / "data_lake")
    return str(Path.home() / "health-platform" / "data_lake")


def _machine_config_path() -> Path:
    """Return path to the machine-level config file."""
    system = platform.system()
    if system == "Windows":
        base = os.environ.get("APPDATA", str(Path.home() / "AppData" / "Roaming"))
        return Path(base) / "health-platform" / "config.yaml"
    # macOS + Linux: XDG convention
    xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    return Path(xdg) / "health-platform" / "config.yaml"


def _load_machine_config() -> dict:
    """Load the machine-level config, or return empty dict if missing."""
    path = _machine_config_path()
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


def _load_env_config() -> dict:
    """Load the repo-level environment_config.yaml (legacy fallback)."""
    try:
        from health_platform.utils.path_resolver import get_project_root

        config_path = (
            get_project_root()
            / "health_environment"
            / "config"
            / "environment_config.yaml"
        )
        if config_path.exists():
            with open(config_path) as f:
                return yaml.safe_load(f) or {}
    except Exception:
        pass
    return {}


def get_data_lake_root() -> Path:
    """Resolve the data lake root directory.

    Resolution: HEALTH_DATA_LAKE env > machine config > env_config.yaml > platform default.
    """
    # 1. Environment variable
    env_val = os.environ.get("HEALTH_DATA_LAKE")
    if env_val:
        return Path(env_val)

    # 2. Machine config
    machine = _load_machine_config()
    if machine.get("data_lake_root"):
        return Path(machine["data_lake_root"])

    # 3. Repo-level environment_config.yaml (legacy)
    env_config = _load_env_config()
    repo_val = env_config.get("paths", {}).get("data_lake_root")
    if repo_val:
        return Path(repo_val)

    # 4. Platform default
    return Path(_platform_default_data_lake())


def get_db_dir() -> Path:
    """Return the database directory."""
    return get_data_lake_root() / "database"


def get_db_path(env: str | None = None) -> Path:
    """Resolve the DuckDB file path.

    Parameters
    ----------
    env : str, optional
        Environment name (dev/prd). Falls back to HEALTH_ENV env var,
        then environment_config.yaml default, then 'dev'.
    """
    # Allow explicit override via HEALTH_DB_PATH
    explicit = os.environ.get("HEALTH_DB_PATH")
    if explicit:
        return Path(explicit)

    if env is None:
        env = os.environ.get("HEALTH_ENV")
    if env is None:
        env_config = _load_env_config()
        env = env_config.get("defaults", {}).get("environment", "dev")

    env_config = _load_env_config()
    db_name = env_config.get("defaults", {}).get("database_name", "health_dw")

    return get_db_dir() / f"{db_name}_{env}.db"


def get_log_dir() -> Path:
    """Return the log directory root."""
    return get_data_lake_root() / "logs"


def get_manual_dir() -> Path:
    """Return the manual data import directory."""
    return get_data_lake_root() / "manual"


def get_lab_dir() -> Path:
    """Return the lab results directory."""
    return get_manual_dir() / "lab_results"
