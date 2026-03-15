"""Parse Lifesum nutrition PDF export into a pandas DataFrame.

Uses pdfplumber to extract the detail food item table from the PDF,
skipping the summary section at the top.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pdfplumber
from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_data_lake_root

logger = get_logger("lifesum.pdf_parser")

# Columns in the detail table of the PDF
PDF_DETAIL_COLUMNS = [
    "date",
    "meal_type",
    "title",
    "amount",
    "serving_name",
    "amount_in_grams",
    "calories",
    "carbs",
    "carbs_fiber",
    "carbs_sugar",
    "fat",
    "fat_saturated",
    "fat_unsaturated",
    "cholesterol",
    "protein",
    "potassium",
    "sodium",
]

# Numeric columns that should be cast to float
NUMERIC_COLUMNS = [
    "amount",
    "amount_in_grams",
    "calories",
    "carbs",
    "carbs_fiber",
    "carbs_sugar",
    "fat",
    "fat_saturated",
    "fat_unsaturated",
    "cholesterol",
    "protein",
    "potassium",
    "sodium",
]


def parse_lifesum_pdf(pdf_path: Path) -> pd.DataFrame:
    """Extract food item detail rows from a Lifesum nutrition PDF.

    Args:
        pdf_path: Path to the Lifesum export PDF.

    Returns:
        DataFrame with columns matching the food parquet schema.
    """
    logger.info("Parsing PDF: %s", pdf_path.name)

    all_rows = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if row is None or len(row) < 10:
                        continue
                    # Skip header rows and summary rows
                    first_cell = (row[0] or "").strip()
                    if first_cell in ("Date", ""):
                        continue
                    if (
                        first_cell.startswith("Summary")
                        or first_cell.startswith("Average")
                        or first_cell.startswith("User")
                    ):
                        continue
                    # A detail row starts with a date like "2026-03-08"
                    if len(first_cell) >= 10 and first_cell[4] == "-":
                        all_rows.append(row)

    if not all_rows:
        logger.warning("No detail rows found in PDF")
        return pd.DataFrame()

    logger.info("Extracted %d raw rows from PDF", len(all_rows))

    # Build DataFrame — PDF has 17 columns in the detail table
    # Some rows may have merged cells (multi-line titles), handle gracefully
    cleaned_rows = []
    for row in all_rows:
        # Clean cells: strip whitespace, collapse newlines (multi-line PDF cells)
        cells = [" ".join((c or "").split()) for c in row]
        if len(cells) >= len(PDF_DETAIL_COLUMNS):
            cells = cells[: len(PDF_DETAIL_COLUMNS)]
        else:
            # Pad with empty strings
            cells.extend([""] * (len(PDF_DETAIL_COLUMNS) - len(cells)))
        cleaned_rows.append(cells)

    df = pd.DataFrame(cleaned_rows, columns=PDF_DETAIL_COLUMNS)

    # Add missing columns to match food parquet schema
    df["brand"] = None
    df["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = pdf_path.name

    # Cast numeric columns
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Reorder to match existing parquet schema
    output_columns = [
        "date",
        "meal_type",
        "title",
        "brand",
        "serving_name",
        "amount",
        "amount_in_grams",
        "calories",
        "carbs",
        "carbs_fiber",
        "carbs_sugar",
        "cholesterol",
        "fat",
        "fat_saturated",
        "fat_unsaturated",
        "potassium",
        "protein",
        "sodium",
        "_loaded_at",
        "_source_file",
    ]
    df = df[output_columns]

    logger.info("Parsed %d food items from %s", len(df), pdf_path.name)
    return df


def pdf_to_parquet(pdf_path: Path, output_dir: Path | None = None) -> Path | None:
    """Parse a Lifesum PDF and write to parquet in the food directory.

    Args:
        pdf_path: Path to the Lifesum export PDF.
        output_dir: Override output directory. Defaults to lifesum/parquet/food/.

    Returns:
        Path to the written parquet file, or None if parsing failed.
    """
    output_dir = output_dir or (get_data_lake_root() / "lifesum" / "parquet" / "food")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = parse_lifesum_pdf(pdf_path)
    if df.empty:
        logger.warning("No data parsed from %s", pdf_path.name)
        return None

    # Name parquet file after the PDF source
    parquet_name = f"pdf_{pdf_path.stem}.parquet"
    output_path = output_dir / parquet_name

    df.to_parquet(output_path, index=False, compression="snappy")
    size_kb = output_path.stat().st_size / 1024
    logger.info("Parquet written: %s (%d rows, %.1f KB)", output_path, len(df), size_kb)
    return output_path
