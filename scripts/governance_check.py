#!/usr/bin/env python3
"""
Governance check — Layer 3 enforcement (Tier 1, deterministic).

Verifies that governance files (CHANGELOG.md, ARCHITECTURE.md) are updated
whenever code files are changed. Enforces the session protocol at CI level.

Exit codes:
  0 — pass (or only warnings)
  1 — fail (critical governance violation)

Usage: python scripts/governance_check.py
"""

import subprocess
import sys

CODE_EXTENSIONS = {".py", ".sql", ".yaml", ".yml", ".json"}
EXCLUDE_DIRS = {".github", ".venv", "archive", "node_modules"}
CONNECTOR_KEYWORDS = {"connector", "bronze", "ingestion", "stg_"}
TRANSFORM_KEYWORDS = {"silver", "gold", "merge", "transform"}
GOVERNANCE_FILES = {"docs/CHANGELOG.md", "docs/ARCHITECTURE.md", "docs/PROJECT_PLAN.md"}


def get_changed_files():
    result = subprocess.run(
        ["git", "diff", "origin/main...HEAD", "--name-only"],
        capture_output=True,
        text=True,
    )
    files = set(f for f in result.stdout.strip().split("\n") if f)
    return files


def is_code_file(filepath):
    if any(filepath.startswith(d + "/") for d in EXCLUDE_DIRS):
        return False
    ext = "." + filepath.rsplit(".", 1)[-1] if "." in filepath else ""
    return ext in CODE_EXTENSIONS


def contains_keyword(filepath, keywords):
    lower = filepath.lower()
    return any(k in lower for k in keywords)


def main():
    changed = get_changed_files()
    code_files = {f for f in changed if is_code_file(f)}
    governance_changed = changed & GOVERNANCE_FILES

    warnings = []
    failures = []

    if not code_files:
        print("No code files changed. Governance check skipped.")
        sys.exit(0)

    # Rule 1: Any code change should update CHANGELOG.md
    if "docs/CHANGELOG.md" not in changed:
        warnings.append(
            f"Code changes detected ({len(code_files)} files) but docs/CHANGELOG.md not updated.\n"
            f"  Changed code: {', '.join(sorted(code_files)[:5])}{'...' if len(code_files) > 5 else ''}"
        )

    # Rule 2: New connectors/bronze should update ARCHITECTURE.md
    connector_files = {f for f in code_files if contains_keyword(f, CONNECTOR_KEYWORDS)}
    if connector_files and "docs/ARCHITECTURE.md" not in changed:
        warnings.append(
            f"Connector/bronze files changed but docs/ARCHITECTURE.md not updated.\n"
            f"  Files: {', '.join(sorted(connector_files))}"
        )

    # Rule 3: New transforms should update ARCHITECTURE.md
    transform_files = {f for f in code_files if contains_keyword(f, TRANSFORM_KEYWORDS)}
    if transform_files and "docs/ARCHITECTURE.md" not in changed:
        warnings.append(
            f"Transform files (silver/gold) changed but docs/ARCHITECTURE.md not updated.\n"
            f"  Files: {', '.join(sorted(transform_files))}"
        )

    # Rule 4: GitHub Actions workflows should never change without review (hard fail not needed, just warn)
    workflow_files = {f for f in changed if f.startswith(".github/workflows/")}
    if workflow_files and "docs/CHANGELOG.md" not in changed:
        failures.append(
            f"CI/CD workflow files changed but CHANGELOG.md not updated — this is a critical change.\n"
            f"  Files: {', '.join(sorted(workflow_files))}"
        )

    if warnings:
        print("GOVERNANCE WARNINGS:")
        for w in warnings:
            print(f"  ⚠️  {w}")
        print()

    if failures:
        print("GOVERNANCE FAILURES:")
        for f in failures:
            print(f"  ❌  {f}")
        sys.exit(1)

    if warnings:
        print("Governance check passed with warnings.")
    else:
        print(
            f"Governance check passed ({len(code_files)} code files, {len(governance_changed)} governance files updated)."
        )


if __name__ == "__main__":
    main()
