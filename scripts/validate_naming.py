#!/usr/bin/env python3
"""
Naming convention validator — Layer 3 enforcement (Tier 1, deterministic).

Checks changed files for:
  - snake_case filenames (.py, .sql)
  - No hardcoded /Users/ paths in Python files
  - No hardcoded production schema references

Usage:
  CI mode (git diff against main): python scripts/validate_naming.py
  Pre-commit mode (staged files):  python scripts/validate_naming.py --staged
"""

import re
import subprocess
import sys

HARDCODED_PATH_PATTERN = re.compile(r"/Users/\w+", re.IGNORECASE)
HARDCODED_PRD_PATTERN = re.compile(r"health_dw_prd|health-platform-prd", re.IGNORECASE)
SNAKE_CASE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

EXCLUDE_DIRS = {".github", "docs", ".claude", "archive", ".venv", "node_modules"}


def get_changed_files(staged=False):
    if staged:
        result = subprocess.run(
            ["git", "diff", "--cached", "--name-only"], capture_output=True, text=True
        )
    else:
        result = subprocess.run(
            ["git", "diff", "origin/main...HEAD", "--name-only"],
            capture_output=True,
            text=True,
        )
    files = [f for f in result.stdout.strip().split("\n") if f]
    return files


def is_excluded(filepath):
    parts = filepath.split("/")
    return any(part in EXCLUDE_DIRS for part in parts)


def check_snake_case(filepath):
    if is_excluded(filepath):
        return []
    if not filepath.endswith((".py", ".sql")):
        return []
    basename = filepath.split("/")[-1]
    name_without_ext = re.sub(r"\.[^.]+$", "", basename)
    # Skip Python dunder files (__init__.py, __main__.py, etc.)
    if name_without_ext.startswith("__") and name_without_ext.endswith("__"):
        return []
    if not SNAKE_CASE_PATTERN.match(name_without_ext):
        return [f"{filepath}: filename '{name_without_ext}' is not snake_case"]
    return []


def check_python_content(filepath):
    violations = []
    try:
        with open(filepath) as fh:
            for i, line in enumerate(fh, 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if HARDCODED_PATH_PATTERN.search(line):
                    violations.append(
                        f"{filepath}:{i}: hardcoded /Users/ path — use relative path or env var"
                    )
                if (
                    HARDCODED_PRD_PATTERN.search(line)
                    and "HEALTH_ENV" not in line
                    and "os.environ" not in line
                ):
                    violations.append(
                        f"{filepath}:{i}: possible hardcoded prd reference — use HEALTH_ENV"
                    )
    except (OSError, UnicodeDecodeError):
        pass
    return violations


def main():
    staged = "--staged" in sys.argv
    files = get_changed_files(staged=staged)

    if not files:
        print("No changed files to validate.")
        sys.exit(0)

    violations = []
    for filepath in files:
        violations += check_snake_case(filepath)
        if filepath.endswith(".py") and not is_excluded(filepath):
            violations += check_python_content(filepath)

    if violations:
        print("Naming/convention violations found:")
        for v in violations:
            print(f"  {v}")
        sys.exit(1)

    print(f"Naming conventions check passed ({len(files)} files checked).")


if __name__ == "__main__":
    main()
