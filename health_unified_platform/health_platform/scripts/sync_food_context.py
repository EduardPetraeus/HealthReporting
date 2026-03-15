"""Sync food context between CSV (human-editable) and YAML (AI-readable).

Two operations:
1. Discover new foods from silver.daily_meal not yet in CSV → append rows
2. Generate food_context.yml from CSV (CSV is source of truth)

Usage:
    python sync_food_context.py              # both: discover + generate
    python sync_food_context.py --discover   # only add new foods to CSV
    python sync_food_context.py --generate   # only regenerate YAML from CSV
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import duckdb
import yaml

CSV_PATH = Path("/Users/Shared/data_lake/manual/food_context.csv")
YAML_PATH = Path(__file__).resolve().parents[1] / "contracts" / "food_context.yml"
MIN_OCCURRENCES = 5  # only add foods logged at least this many times


def get_db_path() -> str:
    """Resolve DuckDB path from environment."""
    import os

    env = os.environ.get("HEALTH_ENV", "dev")
    return f"/Users/Shared/data_lake/database/health_dw_{env}.db"


def read_csv() -> list[dict]:
    """Read existing CSV rows."""
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(rows: list[dict]) -> None:
    """Write rows back to CSV."""
    fieldnames = ["lifesum_name", "times_logged", "description", "ingredients", "notes"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def discover_new_foods(rows: list[dict]) -> list[dict]:
    """Query silver.daily_meal for foods not yet in CSV."""
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
    """Generate food_context.yml from CSV rows."""
    foods = []
    for row in rows:
        entry = {
            "lifesum_name": row["lifesum_name"],
            "description": row.get("description", "") or "",
        }

        # Parse ingredients (semicolon-separated in CSV)
        raw_ingredients = row.get("ingredients", "") or ""
        if raw_ingredients.strip():
            entry["ingredients"] = [
                i.strip() for i in raw_ingredients.split(";") if i.strip()
            ]
        else:
            entry["ingredients"] = []

        notes = row.get("notes", "") or ""
        if notes.strip():
            entry["notes"] = notes.strip()

        foods.append(entry)

    data = {
        "version": "1.0",
        "foods": foods,
    }

    # Write with a header comment
    header = (
        "# Food Context — maps Lifesum food item names to descriptions\n"
        "# AI loads this to understand what non-obvious foods actually contain\n"
        "# AUTO-GENERATED from /Users/Shared/data_lake/manual/food_context.csv\n"
        "# Do not edit manually — edit the CSV instead, then run sync_food_context.py\n"
    )

    with open(YAML_PATH, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(
            data, f, default_flow_style=False, allow_unicode=True, sort_keys=False
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync food context CSV ↔ YAML")
    parser.add_argument(
        "--discover", action="store_true", help="Only discover new foods"
    )
    parser.add_argument("--generate", action="store_true", help="Only regenerate YAML")
    args = parser.parse_args()

    # Default: both operations
    do_discover = args.discover or (not args.discover and not args.generate)
    do_generate = args.generate or (not args.discover and not args.generate)

    rows = read_csv()

    if do_discover:
        rows, new_count = discover_new_foods(rows)
        write_csv(rows)
        print(f"CSV: {len(rows)} total foods ({new_count} new discovered)")

    if do_generate:
        if not rows:
            rows = read_csv()
        generate_yaml(rows)
        print(f"YAML: generated {YAML_PATH.name} with {len(rows)} entries")


if __name__ == "__main__":
    main()
