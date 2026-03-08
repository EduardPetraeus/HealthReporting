"""Root conftest.py — ensures health_platform is importable during tests.

Transition step: once `pip install -e .` is used, the sys.path manipulation
here and in individual test files becomes unnecessary.
The pyproject.toml [tool.pytest.ini_options] pythonpath setting handles this
for pytest invocations, but this file provides a fallback for direct execution.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PLATFORM_DIR = Path(__file__).resolve().parent / "health_unified_platform"
if str(_PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(_PLATFORM_DIR))
