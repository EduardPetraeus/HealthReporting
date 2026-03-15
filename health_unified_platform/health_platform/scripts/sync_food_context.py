"""Sync food context between Numbers (human-editable) and YAML (AI-readable).

Two operations:
1. Discover new foods from silver.daily_meal not yet in Numbers → append rows to CSV
2. Generate food_context.yml from Numbers file (Numbers is source of truth)

The human edits food_context.numbers in Apple Numbers.
This script reads it, discovers new foods, exports a CSV for Numbers to re-import,
and generates the YAML that the MCP tool reads.

Usage:
    python sync_food_context.py              # both: discover + generate
    python sync_food_context.py --discover   # only add new foods (writes CSV for import)
    python sync_food_context.py --generate   # only regenerate YAML from Numbers
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import duckdb
import yaml
from numbers_parser import Document

NUMBERS_PATH = Path("/Users/Shared/data_lake/manual/food_context.numbers")
CSV_PATH = Path("/Users/Shared/data_lake/manual/food_context.csv")
YAML_PATH = Path(__file__).resolve().parents[1] / "contracts" / "food_context.yml"
MIN_OCCURRENCES = 5  # only add foods logged at least this many times

COLUMNS = ["lifesum_name", "times_logged", "description", "ingredients", "notes"]


def get_db_path() -> str:
    """Resolve DuckDB path from environment."""
    import os

    env = os.environ.get("HEALTH_ENV", "dev")
    return f"/Users/Shared/data_lake/database/health_dw_{env}.db"


def read_numbers() -> list[dict]:
    """Read food context from Numbers file."""
    if not NUMBERS_PATH.exists():
        print(f"WARNING: {NUMBERS_PATH} not found, falling back to CSV")
        return read_csv()

    doc = Document(str(NUMBERS_PATH))
    table = doc.sheets[0].tables[0]

    rows = []
    for row_idx in range(1, table.num_rows):
        name = table.cell(row_idx, 0).value or ""
        if not name:
            continue
        rows.append(
            {
                "lifesum_name": str(name),
                "times_logged": str(table.cell(row_idx, 1).value or "0"),
                "description": str(table.cell(row_idx, 2).value or ""),
                "ingredients": str(table.cell(row_idx, 3).value or ""),
                "notes": str(table.cell(row_idx, 4).value or ""),
            }
        )
    return rows


def read_csv() -> list[dict]:
    """Read existing CSV rows (fallback if Numbers not available)."""
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(rows: list[dict]) -> None:
    """Write rows to CSV (for re-import into Numbers if new foods discovered)."""
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)


def discover_new_foods(rows: list[dict]) -> tuple[list[dict], int]:
    """Query silver.daily_meal for foods not yet in the spreadsheet."""
    existing_names = {r["lifesum_name"] for r in rows}

    con = duckdb.connect(get_db_path(), read_only=True)
    try:
        db_rows = con.execute(
            """
            SELECT food_item, COUNT(*) AS times_logged
            FROM silver.daily_meal
            WHERE food_item IS NOT NULL AND food_item != ''
            GROUP BY food_item
            HAVING times_logged >= ?
            ORDER BY times_logged DESC
            """,
            [MIN_OCCURRENCES],
        ).fetchall()
    finally:
        con.close()

    new_count = 0
    for name, count in db_rows:
        if name not in existing_names:
            rows.append(
                {
                    "lifesum_name": name,
                    "times_logged": str(count),
                    "description": "",
                    "ingredients": "",
                    "notes": "",
                }
            )
            new_count += 1

    # Update times_logged for existing entries
    count_map = {name: str(count) for name, count in db_rows}
    for row in rows:
        if row["lifesum_name"] in count_map:
            row["times_logged"] = count_map[row["lifesum_name"]]

    # Sort by times_logged descending
    rows.sort(key=lambda r: int(r.get("times_logged", "0")), reverse=True)

    return rows, new_count


def generate_yaml(rows: list[dict]) -> None:
    """Generate food_context.yml from rows."""
    foods = []
    for row in rows:
        desc = row.get("description", "") or ""
        if desc == "None":
            desc = ""

        entry = {
            "lifesum_name": row["lifesum_name"],
            "description": desc,
        }

        # Parse ingredients (semicolon-separated)
        raw_ingredients = row.get("ingredients", "") or ""
        if raw_ingredients.strip() and raw_ingredients != "None":
            entry["ingredients"] = [
                i.strip() for i in raw_ingredients.split(";") if i.strip()
            ]
        else:
            entry["ingredients"] = []

        notes = row.get("notes", "") or ""
        if notes.strip() and notes != "None":
            entry["notes"] = notes.strip()

        foods.append(entry)

    data = {
        "version": "1.0",
        "foods": foods,
    }

    header = (
        "# Food Context — maps Lifesum food item names to descriptions\n"
        "# AI loads this to understand what non-obvious foods actually contain\n"
        "# AUTO-GENERATED from /Users/Shared/data_lake/manual/food_context.numbers\n"
        "# Do not edit manually — edit the Numbers file, then run sync_food_context.py\n"
    )

    with open(YAML_PATH, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(
            data, f, default_flow_style=False, allow_unicode=True, sort_keys=False
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync food context Numbers → YAML")
    parser.add_argument(
        "--discover", action="store_true", help="Only discover new foods"
    )
    parser.add_argument("--generate", action="store_true", help="Only regenerate YAML")
    args = parser.parse_args()

    # Default: both operations
    do_discover = args.discover or (not args.discover and not args.generate)
    do_generate = args.generate or (not args.discover and not args.generate)

    rows = read_numbers()
    print(f"Read {len(rows)} foods from Numbers")

    if do_discover:
        rows, new_count = discover_new_foods(rows)
        write_csv(rows)
        print(f"CSV: {len(rows)} total foods ({new_count} new discovered)")
        if new_count > 0:
            print(
                f"  → Open {CSV_PATH} in Numbers and copy new rows to your .numbers file"
            )

    if do_generate:
        generate_yaml(rows)
        filled = sum(
            1
            for r in rows
            if (r.get("description") or "").strip() and r["description"] != "None"
        )
        print(
            f"YAML: generated {YAML_PATH.name} with {len(rows)} entries ({filled} with descriptions)"
        )


if __name__ == "__main__":
    main()
