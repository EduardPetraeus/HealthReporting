import glob
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger
from health_platform.utils.path_resolver import get_project_root

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

logger = get_logger("ingestion_engine")

_WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS bronze._load_watermarks (
    source_name VARCHAR PRIMARY KEY,
    last_loaded_at TIMESTAMP WITH TIME ZONE,
    files_loaded INTEGER,
    rows_loaded BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE
)
"""


def load_yaml(path):
    """loads_a_yaml_configuration_file"""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _get_watermark(con, source_name: str) -> datetime:
    """Return last_loaded_at for a source, or epoch 0 if first run."""
    row = con.execute(
        "SELECT last_loaded_at FROM bronze._load_watermarks WHERE source_name = ?",
        [source_name],
    ).fetchone()
    if row and row[0]:
        return row[0]
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _update_watermark(
    con, source_name: str, last_loaded_at: datetime, files: int, rows: int
) -> None:
    """Upsert watermark after successful incremental load."""
    con.execute(
        """
        INSERT INTO bronze._load_watermarks
            (source_name, last_loaded_at, files_loaded, rows_loaded, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (source_name) DO UPDATE SET
            last_loaded_at = EXCLUDED.last_loaded_at,
            files_loaded = EXCLUDED.files_loaded,
            rows_loaded = EXCLUDED.rows_loaded,
            updated_at = EXCLUDED.updated_at
        """,
        [source_name, last_loaded_at, files, rows, datetime.now(timezone.utc)],
    )


def _filter_new_files(
    files: list[str], watermark: datetime
) -> tuple[list[str], datetime]:
    """Return files with mtime > watermark and the max mtime among them."""
    new_files = []
    max_mtime = watermark
    for f in files:
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(f), tz=timezone.utc)
        except OSError:
            continue
        if mtime > watermark:
            new_files.append(f)
            if mtime > max_mtime:
                max_mtime = mtime
    return new_files, max_mtime


def run_ingestion():
    # load_environment_and_source_configurations
    root = get_project_root()
    env_path = root / "health_environment" / "config" / "environment_config.yaml"
    src_path = root / "health_environment" / "config" / "sources_config.yaml"

    env_cfg = load_yaml(env_path)
    src_cfg = load_yaml(src_path)

    # setup_environment_and_database_paths
    active_env = os.getenv("HEALTH_ENV", env_cfg["defaults"]["environment"])
    db_name = env_cfg["defaults"]["database_name"]

    db_file = Path(env_cfg["paths"]["db_root"]) / f"{db_name}_{active_env}.db"
    lake_root = Path(env_cfg["paths"]["data_lake_root"])

    logger.info(f"Starting ingestion engine [env: {active_env}]")
    logger.info(f"Target database: {db_file}")

    with AuditLogger("ingestion_engine", "bronze", "multi_source") as audit:
        # establish_connection_to_duckdb
        con = duckdb.connect(str(db_file))

        # ensure watermark table exists
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute(_WATERMARK_DDL)

        sources_loaded = 0
        sources_skipped = 0

        for source in src_cfg["sources"]:
            name = source["name"]
            schema = source["target_schema"]
            table = source["target_table"]
            input_glob = str(lake_root / source["relative_path"])

            logger.info(f"Processing source: {name} -> {schema}.{table}")

            # validate identifiers before f-string SQL interpolation
            if not _IDENTIFIER_RE.match(schema) or not _IDENTIFIER_RE.match(table):
                logger.error(f"Invalid identifier in source {name}: {schema}.{table}")
                continue

            # containment check: relative_path must resolve inside data lake
            resolved_glob = Path(input_glob).resolve()
            if not str(resolved_glob).startswith(str(lake_root.resolve())):
                logger.error(f"Path escape detected for {name}: {input_glob}")
                continue

            # verify_file_existence
            found_files = glob.glob(input_glob, recursive=True)

            if not found_files:
                logger.warning(f"No files found at path: {input_glob}")
                audit.log_table(
                    f"{schema}.{table}",
                    "INCREMENTAL",
                    status="error",
                    error_message="no_files_found",
                )
                continue

            # ensure_target_schema_exists
            # WARN: schema and table are f-string interpolated into SQL.
            # Safe only because both values come from trusted YAML config files.
            # Never pass user-supplied input into these variables.
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            # check if table exists — cold start requires full load
            table_exists = (
                con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = ? AND table_name = ?",
                    [schema, table],
                ).fetchone()[0]
                > 0
            )

            if not table_exists:
                # cold start: full load to create the table
                logger.info(
                    f"  Cold start: creating {schema}.{table} from {len(found_files)} files"
                )
                query = f"""
                    CREATE TABLE {schema}.{table} AS
                    SELECT
                        *,
                        current_timestamp as _ingested_at,
                        '{active_env}' as _source_env
                    FROM read_parquet('{input_glob}', hive_partitioning=True, union_by_name=True)
                """
                try:
                    con.execute(query)
                    count = con.execute(
                        f"SELECT count(*) FROM {schema}.{table}"
                    ).fetchone()[0]
                    max_mtime = max(
                        datetime.fromtimestamp(os.path.getmtime(f), tz=timezone.utc)
                        for f in found_files
                    )
                    _update_watermark(con, name, max_mtime, len(found_files), count)
                    logger.info(
                        f"  Cold start complete: {count:,} rows -> {schema}.{table}"
                    )
                    audit.log_table(
                        f"{schema}.{table}",
                        "FULL_LOAD",
                        rows_after=count,
                        status="success",
                    )
                    sources_loaded += 1
                except Exception as e:
                    logger.error(f"Error during cold start of {name}: {e}")
                    audit.log_table(
                        f"{schema}.{table}",
                        "FULL_LOAD",
                        status="error",
                        error_message=str(e),
                    )
                continue

            # incremental: filter files newer than watermark
            watermark = _get_watermark(con, name)
            new_files, max_mtime = _filter_new_files(found_files, watermark)

            if not new_files:
                logger.info(
                    f"  No new data for {name} (watermark: {watermark.isoformat()})"
                )
                sources_skipped += 1
                continue

            logger.info(
                f"  Incremental: {len(new_files)} new files (of {len(found_files)} total)"
            )

            # build file list for read_parquet
            file_list = "[" + ", ".join(f"'{f}'" for f in new_files) + "]"

            # INSERT BY NAME handles schema evolution: matches columns by name,
            # missing columns in source get NULL. Works when HAE parquet has
            # fewer columns than XML-origin bronze tables.
            # columns() lambda strips _ingested_at/_source_env if present in
            # parquet (HAE adds them), then we add fresh values. Works regardless
            # of whether the parquet has those columns or not.
            query = f"""
                INSERT INTO {schema}.{table} BY NAME
                SELECT
                    columns(c -> c NOT IN ('_ingested_at', '_source_env')),
                    current_timestamp as _ingested_at,
                    '{active_env}' as _source_env
                FROM read_parquet({file_list}, hive_partitioning=True, union_by_name=True)
            """

            try:
                rows_before = con.execute(
                    f"SELECT count(*) FROM {schema}.{table}"
                ).fetchone()[0]
                con.execute(query)
                rows_after = con.execute(
                    f"SELECT count(*) FROM {schema}.{table}"
                ).fetchone()[0]
                rows_inserted = rows_after - rows_before

                _update_watermark(con, name, max_mtime, len(new_files), rows_inserted)

                logger.info(
                    f"  Success: +{rows_inserted:,} rows ({rows_before:,} -> {rows_after:,}) -> {schema}.{table}"
                )
                audit.log_table(
                    f"{schema}.{table}",
                    "INCREMENTAL",
                    rows_before=rows_before,
                    rows_after=rows_after,
                    status="success",
                )
                sources_loaded += 1
            except Exception as e:
                logger.error(f"Error during incremental load of {name}: {e}")
                audit.log_table(
                    f"{schema}.{table}",
                    "INCREMENTAL",
                    status="error",
                    error_message=str(e),
                )

        con.close()

    logger.info(
        f"Ingestion complete: {sources_loaded} loaded, {sources_skipped} skipped (no new data)"
    )

    # Post-ingestion: trigger daily summary generation for today
    try:
        from datetime import date

        from health_platform.ai.text_generator import generate_summary_for_pipeline

        con = duckdb.connect(str(db_file))
        summary = generate_summary_for_pipeline(con, date.today())
        if summary:
            logger.info(f"Daily summary generated ({len(summary)} chars)")
        con.close()
    except Exception as e:
        logger.warning(f"Daily summary generation skipped: {e}")

    logger.info("Done")


# this_part_is_critical_to_actually_run_the_script
if __name__ == "__main__":
    run_ingestion()
