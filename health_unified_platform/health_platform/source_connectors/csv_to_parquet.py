#!/usr/bin/env python3
"""
Generic CSV to Parquet Converter
Purpose: Convert any CSV file to Parquet format for use in the health data lake.
Usage:   python csv_to_parquet.py --input <csv_path> --output <output_dir> [options]
"""

import pandas as pd
import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert a CSV file to Parquet and write it to a target directory."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Full path to the input CSV file."
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Full path to the output base directory. A subdirectory named after the source will be created inside."
    )
    parser.add_argument(
        "--source-name",
        default=None,
        help="Override the source name (used as subdirectory name). Defaults to the CSV filename without extension."
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV column delimiter. Default: ','."
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV file encoding. Default: 'utf-8'."
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "brotli", "zstd", "none"],
        help="Parquet compression codec. Default: 'snappy'."
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Skip adding _loaded_at metadata column."
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
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    if not input_path.is_file():
        print(f"Error: Input path is not a file: {input_path}")
        sys.exit(1)

    output_dir = output_base / source_name
    output_file = output_dir / "data.parquet"

    print(f"Source:      {input_path.resolve()}")
    print(f"Source name: {source_name}")
    print(f"Target:      {output_file.resolve()}")
    print(f"Delimiter:   '{args.delimiter}'")
    print(f"Encoding:    {args.encoding}")
    print(f"Compression: {compression or 'none'}")

    # Read CSV
    try:
        df = pd.read_csv(
            input_path,
            delimiter=args.delimiter,
            encoding=args.encoding,
        )
    except Exception as e:
        print(f"Error: Failed to read CSV: {e}")
        sys.exit(1)

    print(f"Rows read:   {len(df):,}")
    print(f"Columns:     {list(df.columns)}")

    # Add load metadata
    if not args.no_metadata:
        df["_loaded_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = input_path.name

    # Write Parquet (full load — always overwrite)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(output_file, index=False, compression=compression)
    except Exception as e:
        print(f"Error: Failed to write Parquet: {e}")
        sys.exit(1)

    size_kb = output_file.stat().st_size / 1024
    print(f"Done: {output_file.resolve()} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
