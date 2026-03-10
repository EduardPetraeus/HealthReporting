#!/usr/bin/env python3
"""
Generic CSV to Parquet Converter
Purpose: Convert any CSV file to Parquet format for use in the health data lake.
Usage:   python csv_to_parquet.py --input <csv_path> --output <output_dir> [options]
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger

logger = get_logger("csv_to_parquet")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert a CSV file to Parquet and write it to a target directory."
    )
    parser.add_argument(
        "--input", required=True, help="Full path to the input CSV file."
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Full path to the output base directory. A subdirectory named after the source will be created inside.",
    )
    parser.add_argument(
        "--source-name",
        default=None,
        help="Override the source name (used as subdirectory name). Defaults to the CSV filename without extension.",
    )
    parser.add_argument(
        "--delimiter", default=",", help="CSV column delimiter. Default: ','."
    )
    parser.add_argument(
        "--encoding", default="utf-8", help="CSV file encoding. Default: 'utf-8'."
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "brotli", "zstd", "none"],
        help="Parquet compression codec. Default: 'snappy'.",
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Skip adding _loaded_at metadata column.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    input_path = Path(args.input)
    output_base = Path(args.output)
    source_name = args.source_name or input_path.stem
    compression = None if args.compression == "none" else args.compression

    # Validate input
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    if not input_path.is_file():
        logger.error(f"Input path is not a file: {input_path}")
        sys.exit(1)

    # Validate output path parent exists
    if not output_base.parent.exists():
        logger.error(f"Output directory parent does not exist: {output_base.parent}")
        sys.exit(1)

    output_dir = output_base / source_name
    output_file = output_dir / "data.parquet"

    logger.info(f"csv_to_parquet starting: {input_path.name} -> {output_file}")
    logger.debug(f"Source: {input_path.resolve()}")
    logger.debug(f"Source name: {source_name}")
    logger.debug(f"Compression: {compression or 'none'}")

    with AuditLogger("csv_to_parquet", "extract", source_name) as audit:
        # Read CSV
        try:
            df = pd.read_csv(
                input_path,
                delimiter=args.delimiter,
                encoding=args.encoding,
            )
        except Exception as e:
            logger.error(f"Failed to read CSV: {e}")
            sys.exit(1)

        logger.info(f"Rows read: {len(df):,} | Columns: {list(df.columns)}")

        # Add load metadata
        if not args.no_metadata:
            df["_loaded_at"] = datetime.now(timezone.utc).isoformat()
            df["_source_file"] = input_path.name

        # Write Parquet (full load — always overwrite)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            df.to_parquet(output_file, index=False, compression=compression)
        except Exception as e:
            logger.error(f"Failed to write Parquet: {e}")
            sys.exit(1)

        size_kb = output_file.stat().st_size / 1024
        logger.info(f"Done: {output_file.resolve()} ({size_kb:.1f} KB)")

        audit.log_table(
            str(output_file),
            "WRITE_PARQUET",
            rows_after=len(df),
            status="success",
        )


if __name__ == "__main__":
    main()
