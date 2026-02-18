import duckdb
import yaml
import os
import glob
from pathlib import Path

def load_yaml(path):
    """loads_a_yaml_configuration_file"""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def run_ingestion():
    # load_environment_and_source_configurations
    env_path = 'health_unified_platform/health_environment/config/environment_config.yaml'
    src_path = 'health_unified_platform/health_environment/config/sources_config.yaml'
    
    env_cfg = load_yaml(env_path)
    src_cfg = load_yaml(src_path)
    
    # setup_environment_and_database_paths
    active_env = os.getenv("HEALTH_ENV", env_cfg['defaults']['environment'])
    db_name = env_cfg['defaults']['database_name']
    
    db_file = Path(env_cfg['paths']['db_root']) / f"{db_name}_{active_env}.db"
    lake_root = Path(env_cfg['paths']['data_lake_root'])
    
    print(f"--- starting_ingestion_engine [env: {active_env}] ---")
    print(f"target_database: {db_file}")
    
    # establish_connection_to_duckdb
    con = duckdb.connect(str(db_file))
    
    for source in src_cfg['sources']:
        name = source['name']
        schema = source['target_schema']
        table = source['target_table']
        input_glob = str(lake_root / source['relative_path'])
        
        print(f"processing_source: {name} -> {schema}.{table}")
        print(f"  searching_path: {input_glob}")
        
        # verify_file_existence
        found_files = glob.glob(input_glob, recursive=True)
        print(f"  debug_info: found {len(found_files)} files matching the pattern")
        
        if not found_files:
            print(f"  warning: no_files_found_at_path")
            continue
            
        # ensure_target_schema_exists
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
        # ingestion_logic_with_union_by_name_for_schema_evolution
        query = f"""
            CREATE OR REPLACE TABLE {schema}.{table} AS 
            SELECT 
                *, 
                current_timestamp as _ingested_at, 
                '{active_env}' as _source_env
            FROM read_parquet('{input_glob}', hive_partitioning=True, union_by_name=True)
        """
        
        try:
            con.execute(query)
            count = con.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
            print(f"  success: {count}_rows_ingested")
        except Exception as e:
            print(f"  error_during_ingestion: {e}")

    con.close()
    print("--- ingestion_complete ---")

# this_part_is_critical_to_actually_run_the_script
if __name__ == "__main__":
    run_ingestion()