#!/usr/bin/env python3
"""
Apple Health XML to Partitioned Parquet Ingestor
Purpose: Stream-parse large Health Export XML files and store them in a Hive-partitioned Data Lake.
Target: Local Mac Mini M4 / Databricks Compatible
Note: This version dynamically captures all XML attributes and calculates record duration.
"""

import pandas as pd
import xml.etree.ElementTree as ET
import argparse
import sys
from pathlib import Path

# --- DOMAIN MAPPING ---
# Categorizes technical Apple Health identifiers into logical business domains
AREA_MAPPING = {
    # Vitality & Clinical Metrics
    'HKQuantityTypeIdentifierHeart': 'Vitality',
    'HKQuantityTypeIdentifierRespiratory': 'Vitality',
    'HKQuantityTypeIdentifierVO2Max': 'Vitality',
    'HKQuantityTypeIdentifierBodyTemperature': 'Vitality',
    'HKCorrelationTypeIdentifierBloodPressure': 'Vitality',
    'HKQuantityTypeIdentifierBloodPressure': 'Vitality',
    
    # Activity & Workout Events
    'HKQuantityTypeIdentifierStep': 'Activity',
    'HKQuantityTypeIdentifierDistance': 'Activity',
    'HKQuantityTypeIdentifierActiveEnergy': 'Activity',
    'HKQuantityTypeIdentifierBasalEnergyBurned': 'Activity',
    'HKQuantityTypeIdentifierFlights': 'Activity',
    'HKQuantityTypeIdentifierAppleExerciseTime': 'Activity',
    'HKQuantityTypeIdentifierAppleStand': 'Activity',
    'HKQuantityTypeIdentifierPhysicalEffort': 'Activity',
    'HKQuantityTypeIdentifierRunning': 'Activity',
    'HKWorkoutEventType': 'Activity',
    
    # Mobility & Gait
    'HKQuantityTypeIdentifierWalking': 'Mobility',
    'HKQuantityTypeIdentifierAppleWalking': 'Mobility',
    
    # Body Composition
    'HKQuantityTypeIdentifierBodyMass': 'BodyMetrics',
    'HKQuantityTypeIdentifierBodyFat': 'BodyMetrics',
    'HKQuantityTypeIdentifierLeanBodyMass': 'BodyMetrics',
    'HKQuantityTypeIdentifierHeight': 'BodyMetrics',
    'HKQuantityTypeIdentifierBodyMassIndex': 'BodyMetrics',
    
    # Sleep & Mindfulness
    'HKCategoryTypeIdentifierSleep': 'Sleep',
    'HKDataTypeSleepDurationGoal': 'Sleep',
    'HKCategoryTypeIdentifierMindful': 'Mindfulness',
    
    # Nutrition & Hydration
    'HKQuantityTypeIdentifierDietary': 'Nutrition',
    
    # Hygiene
    'HKCategoryTypeIdentifierToothbrushing': 'Hygiene',
    'HKCategoryTypeIdentifierHandwashing': 'Hygiene',
    
    # Environment & Hearing
    'HKQuantityTypeIdentifierEnvironmental': 'Environment',
    'HKQuantityTypeIdentifierHeadphone': 'Environment',
    'HKCategoryTypeIdentifierAudioExposure': 'Environment',
    'HKDataTypeIdentifierAudiogram': 'Environment'
}

def get_area(record_type):
    """Assigns the record to a high-level domain (e.g., Vitality, Activity)."""
    for prefix, area in AREA_MAPPING.items():
        if record_type.startswith(prefix):
            return area
    return 'Other'

def clean_name(name):
    """Standardizes table names by removing technical prefixes and using snake_case."""
    return (name.replace('HKQuantityTypeIdentifier', '')
                .replace('HKCategoryTypeIdentifier', '')
                .replace('HKCorrelationTypeIdentifier', '')
                .replace('HKDataTypeIdentifier', '')
                .replace('HKWorkoutEventType', 'Event_')).lower()

def parse_args():
    """Configures command-line arguments for dynamic path injection."""
    parser = argparse.ArgumentParser(description='Ingest Apple Health XML into a partitioned Parquet Data Lake.')
    parser.add_argument('--input', required=True, help='Full path to the Apple Health eksport.xml file')
    parser.add_argument('--output', required=True, help='Full path to the target directory for the Data Lake')
    return parser.parse_args()

def main():
    # 1. Handle CLI Arguments
    args = parse_args()
    xml_path = Path(args.input)
    output_folder = Path(args.output)

    # 2. Safety Check
    if not xml_path.exists():
        print(f"❌ Error: Input file not found at {xml_path}")
        sys.exit(1)

    print(f"🚀 Initializing Dynamic Ingestion...")
    print(f"📄 Source XML: {xml_path.resolve()}")
    print(f"🏗️  Target Lake: {output_folder.resolve()}")

    records = []
    
    # 3. Memory-Efficient Stream Parsing
    try:
        context = ET.iterparse(str(xml_path), events=('end',))
        for event, elem in context:
            if elem.tag == 'Record':
                # Dynamically capture all attributes from the XML element
                record_data = dict(elem.attrib)
                
                # Add transformed metadata
                r_type = record_data.get('type', 'Unknown')
                record_data['data_domain'] = get_area(r_type)
                record_data['data_type'] = clean_name(r_type)
                
                # Calculate duration if timestamps are present
                if 'startDate' in record_data and 'endDate' in record_data:
                    start = pd.to_datetime(record_data['startDate'])
                    end = pd.to_datetime(record_data['endDate'])
                    record_data['duration_seconds'] = (end - start).total_seconds()
                
                records.append(record_data)
                
                # Immediate cleanup to save memory on M4
                elem.clear()
                
                if len(records) % 100000 == 0:
                    print(f"⏳ Parsed {len(records)} records...")

    except Exception as e:
        print(f"❌ Error during XML parsing: {e}")
        sys.exit(1)

    if not records:
        print("⚠️ No records found in the XML file.")
        return

    # 4. Data Transformation with Pandas
    print("📊 Refining data structures and enforcing types...")
    df = pd.DataFrame(records)
    
    # Convert standard columns to appropriate types
    if 'value' in df.columns:
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    if 'startDate' in df.columns:
        df['startDate'] = pd.to_datetime(df['startDate'], utc=True)
        df['year'] = df['startDate'].dt.year
    else:
        df['year'] = 0

    # 5. Partitioned Output (Parquet)
    print(f"📦 Partitioning data by Domain/Type/Year ...")
    
    for (domain, d_type, year), group in df.groupby(['data_domain', 'data_type', 'year']):
        target_path = output_folder / domain / d_type / f"year={year}"
        target_path.mkdir(parents=True, exist_ok=True)
        
        # Save all captured columns to Parquet
        group.to_parquet(target_path / "data_batch.parquet", index=False, compression='snappy')

    print(f"✅ Success! Data Lake updated at: {output_folder.absolute()}")

if __name__ == "__main__":
    main()