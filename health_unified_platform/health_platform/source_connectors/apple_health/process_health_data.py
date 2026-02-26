#!/usr/bin/env python3
"""
Apple Health XML to Partitioned Parquet Ingestor

Stream-parses large Health Export XML files and stores them in a
Hive-partitioned data lake. Processes records in batches to limit
memory usage and writes timestamped parquet files to support
incremental runs without overwriting existing data.

Usage:
    python process_health_data.py --input /path/to/eksport.xml --output /path/to/data_lake
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import xml.etree.ElementTree as ET

BATCH_SIZE = 500_000

AREA_MAPPING = {
    # Vitality & Clinical Metrics
    "HKQuantityTypeIdentifierHeart": "Vitality",
    "HKQuantityTypeIdentifierRespiratory": "Vitality",
    "HKQuantityTypeIdentifierVO2Max": "Vitality",
    "HKQuantityTypeIdentifierBodyTemperature": "Vitality",
    "HKCorrelationTypeIdentifierBloodPressure": "Vitality",
    "HKQuantityTypeIdentifierBloodPressure": "Vitality",
    # Activity & Workout Events
    "HKQuantityTypeIdentifierStep": "Activity",
    "HKQuantityTypeIdentifierDistance": "Activity",
    "HKQuantityTypeIdentifierActiveEnergy": "Activity",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "Activity",
    "HKQuantityTypeIdentifierFlights": "Activity",
    "HKQuantityTypeIdentifierAppleExerciseTime": "Activity",
    "HKQuantityTypeIdentifierAppleStand": "Activity",
    "HKQuantityTypeIdentifierPhysicalEffort": "Activity",
    "HKQuantityTypeIdentifierRunning": "Activity",
    "HKWorkoutEventType": "Activity",
    # Mobility & Gait
    "HKQuantityTypeIdentifierWalking": "Mobility",
    "HKQuantityTypeIdentifierAppleWalking": "Mobility",
    # Body Composition
    "HKQuantityTypeIdentifierBodyMass": "BodyMetrics",
    "HKQuantityTypeIdentifierBodyFat": "BodyMetrics",
    "HKQuantityTypeIdentifierLeanBodyMass": "BodyMetrics",
    "HKQuantityTypeIdentifierHeight": "BodyMetrics",
    "HKQuantityTypeIdentifierBodyMassIndex": "BodyMetrics",
    # Sleep & Mindfulness
    "HKCategoryTypeIdentifierSleep": "Sleep",
    "HKDataTypeSleepDurationGoal": "Sleep",
    "HKCategoryTypeIdentifierMindful": "Mindfulness",
    # Nutrition & Hydration
    "HKQuantityTypeIdentifierDietary": "Nutrition",
    # Hygiene
    "HKCategoryTypeIdentifierToothbrushing": "Hygiene",
    "HKCategoryTypeIdentifierHandwashing": "Hygiene",
    # Environment & Hearing
    "HKQuantityTypeIdentifierEnvironmental": "Environment",
    "HKQuantityTypeIdentifierHeadphone": "Environment",
    "HKCategoryTypeIdentifierAudioExposure": "Environment",
    "HKDataTypeIdentifierAudiogram": "Environment",
}


def get_area(record_type: str) -> str:
    for prefix, area in AREA_MAPPING.items():
        if record_type.startswith(prefix):
            return area
    return "Other"


def clean_name(name: str) -> str:
    return (
        name.replace("HKQuantityTypeIdentifier", "")
            .replace("HKCategoryTypeIdentifier", "")
            .replace("HKCorrelationTypeIdentifier", "")
            .replace("HKDataTypeIdentifier", "")
            .replace("HKWorkoutEventType", "event")
    ).lower()


def write_batch(records: list, output_folder: Path, ingested_at: str, batch_num: int) -> int:
    """Convert a list of records to DataFrame and write partitioned parquet files."""
    df = pd.DataFrame(records)
    df["_ingested_at"] = ingested_at

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if "startDate" in df.columns:
        df["startDate"] = pd.to_datetime(df["startDate"], utc=True)
        df["year"] = df["startDate"].dt.year
    else:
        df["year"] = 0

    # Vectorized duration calculation
    if "startDate" in df.columns and "endDate" in df.columns:
        df["endDate"] = pd.to_datetime(df["endDate"], utc=True)
        df["duration_seconds"] = (df["endDate"] - df["startDate"]).dt.total_seconds()

    files_written = 0
    filename = f"batch_{batch_num}_{ingested_at}.parquet"

    for (domain, d_type, year), group in df.groupby(["data_domain", "data_type", "year"]):
        target_path = output_folder / domain / d_type / f"year={year}"
        target_path.mkdir(parents=True, exist_ok=True)
        group.to_parquet(target_path / filename, index=False, compression="snappy")
        files_written += 1

    return files_written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Apple Health XML into a partitioned Parquet data lake."
    )
    parser.add_argument("--input", required=True, help="Path to eksport.xml")
    parser.add_argument("--output", required=True, help="Path to data lake root directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    xml_path = Path(args.input)
    output_folder = Path(args.output)

    if not xml_path.exists():
        print(f"Error: input file not found at {xml_path}")
        sys.exit(1)

    ingested_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Starting Apple Health ingestion — run id: {ingested_at}")
    print(f"Source: {xml_path.resolve()}")
    print(f"Target: {output_folder.resolve()}")

    records = []
    total_records = 0
    total_files = 0
    batch_num = 0

    try:
        context = ET.iterparse(str(xml_path), events=("end",))
        for event, elem in context:
            if elem.tag == "Record":
                record_data = dict(elem.attrib)
                r_type = record_data.get("type", "Unknown")
                record_data["data_domain"] = get_area(r_type)
                record_data["data_type"] = clean_name(r_type)
                records.append(record_data)
                elem.clear()

                if len(records) >= BATCH_SIZE:
                    total_files += write_batch(records, output_folder, ingested_at, batch_num)
                    total_records += len(records)
                    batch_num += 1
                    print(f"  Processed {total_records:,} records...")
                    records = []

    except Exception as e:
        print(f"Error during XML parsing: {e}")
        sys.exit(1)

    # Write remaining records
    if records:
        total_files += write_batch(records, output_folder, ingested_at, batch_num)
        total_records += len(records)

    if total_records == 0:
        print("No records found in XML file.")
        return

    print(f"Done. {total_records:,} records written to {total_files} parquet files.")


if __name__ == "__main__":
    main()
