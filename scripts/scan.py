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
from datetime import datetime
import multiprocessing
from typing import List, Iterator, Optional, Dict, Any

from rich.logging import RichHandler

from pipeline.helpers.timer import Timer
from pipeline.helpers import utils, db
from pipeline.models.scans import ScanRun
from pipeline.models.file_changes import FileChanges
from pipeline.models.files import File

MODULE_NAME = "scan"
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

LOG_FREQUENCY = 1000  # Log every N files processed


def iter_files(root: Path) -> Iterator[Path]:
    """
    Yield the full path of every file under `root`,
    including in subdirectories.
    """
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            yield Path(os.path.join(dirpath, fn))


def process_file(path: Path, scan_id: int) -> None:
    """
    Process a file.

    Hash the file if it is new or modified, and update the database.

    - Look up file in files.
    - If not found
        - INSERT into file_changes(scan_id,…,change_type='new',new_*)
        - INSERT into files with last_seen_scan=scan_id.
    - If found, compare size, mtime; if different, hash the file, then:
    - UPDATE files (...).
    - INSERT into file_changes with change_type='modified', filling old_*/new_* columns.
    - If found and unchanged, just UPDATE last_seen_scan in files_current.
    """
    old_file = File.get(
        file_path=path,
        config_file=utils.get_config_file_path(),
    )
    if old_file is not None:
        new_file = File.update(
            old_data=old_file,
        )
    else:
        new_file = File.from_path(path)

    file_changes = FileChanges.get_file_changes(
        old_file=old_file,
        new_file=new_file,
    )

    new_file.last_seen_scan = scan_id

    sql_queries: List[str] = [
        new_file.to_sql(),
    ]

    if file_changes is not None:
        file_changes.scan_id = scan_id
        sql_queries.append(file_changes.to_sql())

    db.execute_queries(
        config_file=utils.get_config_file_path(),
        queries=sql_queries,
        silent=True,
        show_commands=False,
    )


def handle_deleted_files(
    scan_id: int,
) -> None:
    """
    Handle deleted files.
    """

    logger.info("Handling deleted files...")

    # Get all files in the database
    sql_query = f"""
    SELECT file_path FROM filesystem.files
    WHERE last_seen_scan != {scan_id};
    """
    results_df = db.execute_sql(
        config_file=utils.get_config_file_path(),
        query=sql_query,
    )

    sql_queries: List[str] = []
    for _, row in results_df.iterrows():
        file_path = row["file_path"]
        old_file = File.get(
            file_path=Path(file_path),
            config_file=utils.get_config_file_path(),
        )
        file_changes = FileChanges.get_file_changes(
            old_file=old_file,
            new_file=None,
        )

        if file_changes is None:
            logger.warning(f"No changes found for deleted file: {file_path}")
            continue

        file_changes.scan_id = scan_id
        sql_queries.append(file_changes.to_sql())

    # Delete the files that are not in the current scan
    sql_query = f"""
    DELETE FROM filesystem.files
    WHERE last_seen_scan != {scan_id};
    """

    sql_queries.append(sql_query)

    db.execute_queries(
        config_file=utils.get_config_file_path(),
        queries=sql_queries,
        silent=True,
        show_commands=False,
    )
    deleted_files_count = len(results_df)
    logger.info(f"{deleted_files_count} deleted files handled.")


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
        scan_stats[size_param] = FileChanges.get_file_size_by_change_type(
            scan_id=scan_id,
            change_type=change_type,
            config_file=config_file,
        )

    logger.info(f"Scan finished at {finished_at}")
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
    total_seconds = int(seconds)               # drop any fractional part
    hours, rem = divmod(total_seconds, 3600)   # how many whole hours?
    minutes, secs = divmod(rem, 60)            # the remainder in minutes & seconds
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(
        path=config_file,
        section="crawler",
    )

    data_root = config_params.get("data_root")
    if data_root is None:
        logger.error("data_root not found in config file.")
        sys.exit(1)

    data_root = Path(data_root).resolve()
    n_workers = multiprocessing.cpu_count() // 2
    started_at = datetime.now()

    logger.info(f"Using data_root: {data_root}")
    logger.info(f"Using {n_workers} workers.")

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

    def process_file_wrapper(path: Path) -> None:
        """
        Wrapper for process_file to be used with multiprocessing.
        """
        process_file(path, scan_id)

    last_batch_duration: float = 0.0
    with Timer() as timer:
        with utils.get_progress_bar() as progress:
            task = progress.add_task(
                description="Processing files",
                total=get_files_count_estimate(config_file=config_file),
            )
            with multiprocessing.Pool(processes=n_workers) as pool:
                # chunksize controls how many tasks get sent to each worker at once.
                # Increasing it reduces IPC overhead but raises max queued tasks.
                # hashing is so costly that the tiny IPC overhead is negligible
                processed_count = 0
                for i, _ in enumerate(
                    pool.imap_unordered(
                        process_file_wrapper,
                        iter_files(Path(data_root)),
                        chunksize=2,
                    )
                ):
                    progress.update(task, advance=1)
                    processed_count += 1
                    if processed_count % LOG_FREQUENCY == 0:
                        current_time = timer.duration

                        if current_time is None or current_time <= 0:
                            logger.debug(f"Processed {processed_count} files...")
                        else:
                            overall_fps = processed_count / current_time

                            time_for_batch = current_time - last_batch_duration

                            if time_for_batch > 0:
                                batch_fps = LOG_FREQUENCY / time_for_batch
                                logger.debug(
                                    f"Processed {processed_count}. "
                                    f"Batch: {batch_fps:.2f} f/s. "
                                    f"Overall: {overall_fps:.2f} f/s. "
                                    f"Elapsed time: {format_duration(current_time)}. "
                                )
                            else:
                                logger.debug(
                                    f"Processed {processed_count}. "
                                    f"Overall: {overall_fps:.2f} f/s."
                                )

                            last_batch_duration = current_time

    progress.update(task, completed=task)
    logger.info(f"Processed {processed_count} files in {timer.duration:.2f} seconds.")

    handle_deleted_files(scan_id=scan_id)

    scan_metadata = {
        "data_root": data_root,
        "n_workers": n_workers,
        "timer_duration_s": timer.duration,
        "files_per_second": (
            processed_count / timer.duration
            if timer.duration is not None and timer.duration > 0
            else 0
        ),
    }

    finalize_scan(
        scan_id=scan_id,
        scanned_paths_count=processed_count,
        scan_metadata=scan_metadata,
        config_file=config_file,
    )

    logger.info("Done!")
