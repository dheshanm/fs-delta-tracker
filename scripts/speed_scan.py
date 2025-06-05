#!/usr/bin/env python
"""
Scans a directory and tracks changes in files.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "fs-delta-tracker":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
import os
import io
from datetime import datetime
import multiprocessing
from typing import Iterator, Optional, Tuple, Dict, Any
import argparse

import psycopg2
from rich.logging import RichHandler

from pipeline.helpers import utils, db, cli
from pipeline.helpers.timer import Timer
from pipeline.models.scans import ScanRun
from pipeline.models.file_changes import FileChanges
from pipeline.models.staging_files import StagingFile

MODULE_NAME = "speed_scan"
INSTANCE_NAME = MODULE_NAME

console = utils.get_console()


logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

LOG_FREQUENCY = 10000  # Log every N files processed


class IterableToFile(io.TextIOBase):
    """
    Wrap an iterator of strings so we can feed it into cursor.copy_expert().
    Each string in the iterator must end in "\n" (i.e. a CSV row).
    """

    def __init__(self, iterator):
        self._it = iter(iterator)
        self._buf = ""

    def readable(self) -> bool:
        return True

    def read(self, size: Optional[int] = -1) -> str:
        # If user wants 'read all' just concatenate everything
        if size is None or size < 0:
            return "".join(self._it)
        # else return up to size chars, buffering rest
        while len(self._buf) < size:
            try:
                self._buf += next(self._it)
            except StopIteration:
                break
        result, self._buf = self._buf[:size], self._buf[size:]
        return result

    def readline(self, limit=-1) -> str:
        # read until next newline
        line = ""
        # first see if buf has a newline
        if "\n" in self._buf:
            idx = self._buf.index("\n") + 1
            line, self._buf = self._buf[:idx], self._buf[idx:]
            return line
        # otherwise keep pulling from iterator
        while True:
            try:
                chunk = next(self._it)
            except StopIteration:
                # nothing left
                line, self._buf = self._buf, ""
                return line
            if "\n" in chunk:
                idx = chunk.index("\n") + 1
                line += chunk[:idx]
                self._buf = chunk[idx:]
                return line
            else:
                line += chunk


def iter_files(root: Path) -> Iterator[Path]:
    """
    Yield the full path of every file under `root`,
    including in subdirectories.
    """
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            yield Path(os.path.join(dirpath, fn))


def get_file_stat_line(file_info: Tuple[int, Path]) -> str:  # (scan_id, file_path)
    """
    Get a line with file stats for the given file.
    The line is formatted as:
    <scan_id>\t<file_name>\t<file_type>\t<file_path>\t<file_size_bytes>\t<file_mtime>\n
    """
    scan_id, file_path = file_info

    try:
        file_name = file_path.name
        file_type = file_path.suffix if file_path.suffix else "unknown"

        file_stat = os.stat(file_path)
        file_size_bytes = file_stat.st_size
        # Convert Unix timestamp to datetime object, then to ISO 8601 string
        file_mtime_dt = datetime.fromtimestamp(file_stat.st_mtime)
        file_mtime_iso = file_mtime_dt.isoformat()
    except (FileNotFoundError, PermissionError):
        # If the file is not found or we don't have permission, log the error
        # logger.error(f"Error accessing file {file_path}: {e}")
        return ""

    # Format the line as required
    return f"{scan_id}\t{file_name}\t{file_type}\t{file_path.as_posix()}\t{file_size_bytes}\t{file_mtime_iso}\n"


def get_files_count_estimate(
    config_file: Path,
) -> Optional[int]:
    """
    Get the estimated number of files to be processed.
    This is a rough estimate based on the number of files in the data_root.
    """
    sql_query = """
    SELECT COUNT(*) FROM filesystem.files;
    """

    result = db.fetch_record(
        config_file=config_file,
        query=sql_query,
    )

    if result is None:
        return None

    return int(result)


def format_duration(seconds: float) -> str:
    """
    Convert a time duration given in seconds to a string HHH:MM:SS.

    seconds may be an int or float; fractional parts are discarded.
    """
    total_seconds = int(seconds)  # drop any fractional part
    hours, rem = divmod(total_seconds, 3600)  # how many whole hours?
    minutes, secs = divmod(rem, 60)  # the remainder in minutes & seconds
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def bulk_copy_staging(
    config_file: Path,
    scan_id: int,
    data_root: Path,
    n_workers: int = multiprocessing.cpu_count() // 2,
) -> Dict[str, Any]:
    """
    Scans files under a given data root, collects their metadata, and bulk copies
    this information into the 'filesystem.staging_files' table in a PostgreSQL database.

    The function utilizes multiprocessing to parallelize file system scanning and
    metadata collection. Progress is logged periodically, showing the number of
    files processed, processing rate, batch processing time, and total elapsed time.

    The collected metadata for each file includes its scan ID, name, type, path,
    size in bytes, and modification time. This data is formatted as tab-separated
    values (TSV) and streamed directly to the database using the PostgreSQL `COPY`
    command for efficient insertion.

    Args:
        config_file (Path): Path to the configuration file containing database
                            credentials.
        scan_id (int):  The identifier for the current scan operation. This ID
                        is associated with every file record inserted into the
                        database.
        data_root (Path): The root directory path from which to start scanning
                        for files.
        n_workers (int): The number of worker processes to use for parallel file
                        scanning and metadata collection. Defaults to half the
                        number of CPU cores available on the system.

    Returns:
        Dict[str, Any]: A dictionary containing the statistics of the bulk copy operation,
        including the total number of files processed and the time taken for the operation.
    """
    logger.info(
        f"Initiating bulk copy to staging_files for scan_id: {scan_id}, data_root: {data_root}"
    )

    # This start_time is for the overall function and progress calculation
    function_start_time = datetime.now()

    # Initialize counters and timing variables for progress logging
    processed_files_count = 0
    # Stores the total elapsed seconds at the time of the last progress log
    elapsed_seconds_at_last_log = 0.0

    pool = multiprocessing.Pool(n_workers)
    try:
        logger.info("Starting parallel file system scan and metadata collection...")
        raw_iter = pool.imap_unordered(
            get_file_stat_line,
            ((scan_id, p) for p in iter_files(data_root)),
            chunksize=256,  # Default chunksize, consider tuning for large datasets
        )

        def progress_iterator(iterator: Iterator[str]) -> Iterator[str]:
            """
            Wrap the iterator to log progress every LOG_FREQUENCY items.
            Uses nonlocal variables from the parent function for state.
            """
            nonlocal processed_files_count, function_start_time, elapsed_seconds_at_last_log
            for line in iterator:
                processed_files_count += 1

                if processed_files_count % LOG_FREQUENCY == 0:
                    current_time = datetime.now()
                    total_elapsed_seconds = (
                        current_time - function_start_time
                    ).total_seconds()

                    # Time spent processing the current batch of LOG_FREQUENCY files
                    current_batch_duration_seconds = (
                        total_elapsed_seconds - elapsed_seconds_at_last_log
                    )

                    files_per_second_current_batch = (
                        LOG_FREQUENCY / current_batch_duration_seconds
                        if current_batch_duration_seconds > 0
                        else 0  # Avoid division by zero
                    )

                    total_files_per_second = (
                        processed_files_count / total_elapsed_seconds
                        if total_elapsed_seconds > 0
                        else 0  # Avoid division by zero
                    )

                    total_elapsed_formatted = format_duration(total_elapsed_seconds)
                    current_batch_duration_formatted = format_duration(
                        current_batch_duration_seconds
                    )

                    log_message = (
                        f"Processed {processed_files_count} files. "
                        f"Current batch ({LOG_FREQUENCY} files): "
                        f"{files_per_second_current_batch:.2f} f/s, "
                        f"took {current_batch_duration_formatted}. "
                        f"Total: {total_files_per_second:.2f} f/s, "
                        f"elapsed: {total_elapsed_formatted}."
                    )
                    logger.info(log_message)

                    # Update for the next iteration's calculation
                    elapsed_seconds_at_last_log = total_elapsed_seconds
                yield line

        db_credentials = db.get_db_credentials(config_file=config_file)
        logger.info("Connecting to database...")
        conn: psycopg2.extensions.connection = psycopg2.connect(**db_credentials)  # type: ignore
        try:
            with conn.cursor() as cur:
                copy_sql = """
                COPY filesystem.staging_files(
                    scan_id, file_name, file_type, file_path, file_size_bytes, file_mtime
                )
                    FROM STDIN
                    WITH (
                        FORMAT csv,
                        DELIMITER E'\t',
                        NULL '',
                        HEADER FALSE
                    )
                """
                # Wrap the generator in our file-like helper, which includes progress logging
                file_like_iterable = IterableToFile(progress_iterator(raw_iter))

                logger.info(
                    "Starting database COPY operation from stream to 'filesystem.staging_files'..."
                )
                cur.copy_expert(copy_sql, file_like_iterable)  # type: ignore

                # Log completion of COPY and total files processed by it
                logger.info(
                    "COPY operation completed. "
                    f"{processed_files_count} file records prepared for commit."
                )

                conn.commit()
                logger.info("Database transaction committed successfully.")
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
            if conn:  # Try to rollback if an error occurs during DB operations
                try:
                    conn.rollback()
                    logger.info("Database transaction rolled back.")
                except psycopg2.Error as rb_e:
                    logger.error(f"Rollback failed: {rb_e}")
            raise  # Re-raise the exception after attempting rollback
        finally:
            if conn:
                logger.info("Closing database connection.")
                conn.close()

    finally:
        logger.info("Shutting down worker pool...")
        pool.close()
        pool.join()
        logger.info("Worker pool shut down.")

    # Log total time for the bulk_copy_staging function itself
    function_end_time = datetime.now()
    total_function_duration = function_end_time - function_start_time
    logger.info(
        f"Bulk copy to staging_files finished. "
        f"Total time: {format_duration(total_function_duration.total_seconds())}. "
        f"Total files processed: {processed_files_count}. "
    )

    statistics: Dict[str, Any] = {
        "total_files_processed": processed_files_count,
        "timer_duration": total_function_duration.total_seconds(),
    }

    return statistics


def populate_file_changes(
    config_file: Path,
    scan_id: int,
) -> Dict[str, Any]:
    """
    Populate the 'filesystem.file_changes' table with file changes for the given scan_id.

    Args:
        config_file (Path): Path to the configuration file containing database credentials.
        scan_id (int): The identifier for the current scan operation.
    """

    sql_params = utils.config(
        path=config_file,
        section="sql",
    )
    sql_templates_root = sql_params.get("templates_root", None)
    if sql_templates_root is None:
        logger.error("sql.templates_root not found in config file.")
        sys.exit(1)

    repo_root = cli.get_repo_root_from_config(config_file=config_file)
    sql_templates_root = Path(repo_root) / sql_templates_root
    sql_templates_root = sql_templates_root.resolve()

    sql_template = sql_templates_root / "process_staging_v2.sql"

    if not sql_template.exists():
        logger.error(f"SQL template file not found: {sql_template}")
        sys.exit(1)

    logger.info(f"Using SQL template: {sql_template}")

    template_variables: Dict[str, Any] = {
        "scan_id": scan_id,
    }

    sql_query = db.render_sql_template(
        template_path=sql_template,
        variables=template_variables,
    )

    with Timer() as timer:
        logger.info("Populating file changes in the database...")
        db.execute_queries(
            config_file=config_file,
            queries=[sql_query],
            show_commands=False,
        )

    logger.info(
        f"File changes populated successfully. Took {timer.duration:.2f} seconds."
    )

    statistics: Dict[str, Any] = {
        "timer_duration": timer.duration,
    }

    return statistics


def finalize_scan(
    scan_id: int,
    scanned_paths_count: int,
    scan_metadata: Dict[str, Any],
    config_file: Path,
) -> None:
    """
    Finalize the scan by updating the scan run in the database.
    """

    finished_at: datetime = datetime.now()

    # Define a mapping from change_type to the corresponding parameter names
    # for ScanRun.finish_scan (count_param_name, size_param_name)
    change_type_to_param_names = {
        "added": ("added_files_count", "new_data_mb"),
        "modified": ("modified_files_count", "modified_data_mb"),
        "deleted": ("removed_files_count", "removed_data_mb"),
    }

    scan_stats: Dict[str, Any] = {}

    for change_type, (count_param, size_param) in change_type_to_param_names.items():
        scan_stats[count_param] = FileChanges.get_files_count_by_change_type(
            scan_id=scan_id,
            change_type=change_type,
            config_file=config_file,
        )
        size_bytes = FileChanges.get_file_size_by_change_type(
            scan_id=scan_id,
            change_type=change_type,
            config_file=config_file,
        )
        scan_stats[size_param] = (
            size_bytes / 1024**2 if size_bytes is not None else 0.0
        )  # Convert bytes to MB

    logger.info(f"Scan finished at {finished_at.isoformat()}")
    logger.info(f"Total paths scanned: {scanned_paths_count}")
    logger.info(f"Statistics: {scan_stats}")

    ScanRun.finish_scan(
        scan_id=scan_id,
        finished_at=finished_at,
        total_paths_count=scanned_paths_count,
        scan_metadata=scan_metadata,
        config_file=config_file,
        **scan_stats,  # Unpack collected statistics
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan a directory and track changes in files."
    )
    parser.add_argument(
        "-c",
        "--config",
        default=utils.get_config_file_path(),
        help="Path to the configuration file.",
    )
    parser.add_argument(
        "-d",
        "--data-root",
        type=Path,
        help="Path to the root directory to scan. If not provided, will use 'data_root' from config file.",
        required=True,
    )
    args = parser.parse_args()

    config_file: Path = args.config
    data_root: Path = args.data_root
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    if data_root is None:
        logger.error("data_root not specified.")
        sys.exit(1)

    data_root = Path(data_root).resolve()
    started_at = datetime.now()

    logger.info(f"Using data_root: {data_root}")

    scan_id = ScanRun.start_scan(
        started_at=started_at,
        scan_root=data_root,
        config_file=config_file,
    )
    logger.info(f"Started scan with ID: {scan_id}")

    # Add new file handler for logging
    utils.start_scan_log(
        scan_id=scan_id,
        config_file=config_file,
    )

    n_workers = max(1, multiprocessing.cpu_count() // 2)
    logger.info(f"Using {n_workers} worker processes for file scanning.")

    logger.info("Starting bulk copy of staging data...")
    bulk_copy_statistics = bulk_copy_staging(
        config_file=config_file,
        scan_id=scan_id,
        data_root=data_root,
        n_workers=n_workers,
    )

    file_changes_statistics = populate_file_changes(
        config_file=config_file,
        scan_id=scan_id,
    )

    scan_metadata = {
        "data_root": data_root,
        "n_workers": n_workers,
        "crawl_timer_duration_s": bulk_copy_statistics.get("timer_duration", 0),
        "file_changes_timer_duration_s": file_changes_statistics.get(
            "timer_duration", 0
        ),
        "total_files_processed": bulk_copy_statistics.get("total_files_processed", 0),
        "crawler_files_per_second": (
            bulk_copy_statistics.get("total_files_processed", 0)
            / bulk_copy_statistics.get("timer_duration", 1)
        ),
    }

    finalize_scan(
        scan_id=scan_id,
        scanned_paths_count=bulk_copy_statistics.get("total_files_processed", 0),
        scan_metadata=scan_metadata,
        config_file=config_file,
    )

    # Clean up staging files after processing
    logger.info(
        f"Cleaning staging files for scan_id: {scan_id} in table 'filesystem.staging_files'."
    )
    StagingFile.clean_table(
        config_file=config_file,
        scan_id=scan_id,
    )

    logger.info("Done!")
