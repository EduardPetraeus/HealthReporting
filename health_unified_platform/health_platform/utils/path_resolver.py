"""
path_resolver.py — Reliable project root resolution for the health platform.

Avoids hardcoded relative paths by anchoring to the repository structure.
The project root is health_unified_platform/ (contains both health_platform/
and health_environment/).
"""

from pathlib import Path


def get_project_root() -> Path:
    """
    Return the absolute path to health_unified_platform/.

    Walks up from this file's location (utils/) to find the project root.
    Layout: health_unified_platform/health_platform/utils/path_resolver.py
    """
    return Path(__file__).resolve().parents[2]
