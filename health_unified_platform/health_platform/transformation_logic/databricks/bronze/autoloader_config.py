"""
Bronze Autoloader configuration generator.

Reads sources_config.yaml and generates cloudFiles configuration dicts
for Databricks Auto Loader ingestion. Each source maps to a target
bronze table with schema inference and checkpoint management.

Usage (standalone):
    python autoloader_config.py

Usage (from notebook):
    from autoloader_config import get_autoloader_configs
    configs = get_autoloader_configs()
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# Default paths — overridable via environment variables
_DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "..",
    "..",
    "health_environment",
    "config",
    "sources_config.yaml",
)

_DEFAULT_LANDING_ZONE = "abfss://landing@healthdatalake.dfs.core.windows.net"
_DEFAULT_CHECKPOINT_ROOT = (
    "abfss://checkpoints@healthdatalake.dfs.core.windows.net/bronze"
)
_DEFAULT_SCHEMA_ROOT = (
    "abfss://checkpoints@healthdatalake.dfs.core.windows.net/bronze/schemas"
)


def load_sources_config(config_path: str | None = None) -> list[dict[str, Any]]:
    """Load and return the list of source definitions from sources_config.yaml."""
    path = config_path or os.environ.get("SOURCES_CONFIG_PATH", _DEFAULT_CONFIG_PATH)
    resolved = Path(path).resolve()

    with open(resolved, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data.get("sources", [])


def build_cloud_files_config(
    source: dict[str, Any],
    landing_zone: str | None = None,
    checkpoint_root: str | None = None,
    schema_root: str | None = None,
) -> dict[str, Any]:
    """
    Build a cloudFiles configuration dict for a single source.

    Returns:
        Dict with keys: name, source_path, target_table, schema,
        format, checkpoint_location, schema_location, cloud_files_options.
    """
    landing = landing_zone or os.environ.get("LANDING_ZONE", _DEFAULT_LANDING_ZONE)
    cp_root = checkpoint_root or os.environ.get(
        "CHECKPOINT_ROOT", _DEFAULT_CHECKPOINT_ROOT
    )
    sc_root = schema_root or os.environ.get("SCHEMA_ROOT", _DEFAULT_SCHEMA_ROOT)

    name = source["name"]
    relative_path = source["relative_path"]
    source_type = source.get("source_type", "parquet")
    target_schema = source.get("target_schema", "bronze")
    target_table = source.get("target_table", f"stg_{name}")

    # Skip non-parquet sources (e.g., YAML manual imports go directly to silver)
    if source_type != "parquet":
        return None

    full_source_path = f"{landing}/{relative_path}"
    full_target = f"health_dw.{target_schema}.{target_table}"
    checkpoint_location = f"{cp_root}/{name}"
    schema_location = f"{sc_root}/{name}"

    cloud_files_options = {
        "cloudFiles.format": source_type,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
    }

    return {
        "name": name,
        "source_path": full_source_path,
        "target_table": full_target,
        "schema": target_schema,
        "format": source_type,
        "checkpoint_location": checkpoint_location,
        "schema_location": schema_location,
        "cloud_files_options": cloud_files_options,
    }


def get_autoloader_configs(
    config_path: str | None = None,
    landing_zone: str | None = None,
    checkpoint_root: str | None = None,
    schema_root: str | None = None,
) -> list[dict[str, Any]]:
    """
    Generate cloudFiles configs for all parquet sources.

    Returns:
        List of configuration dicts, one per source. Non-parquet sources
        are excluded (they bypass bronze and go directly to silver).
    """
    sources = load_sources_config(config_path)
    configs = []

    for source in sources:
        cfg = build_cloud_files_config(
            source,
            landing_zone=landing_zone,
            checkpoint_root=checkpoint_root,
            schema_root=schema_root,
        )
        if cfg is not None:
            configs.append(cfg)

    return configs


if __name__ == "__main__":
    configs = get_autoloader_configs()
    print(f"Generated {len(configs)} autoloader configs:\n")
    for cfg in configs:
        print(f"  {cfg['name']:40s} -> {cfg['target_table']}")
