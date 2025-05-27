#!/usr/bin/env python
"""
Scan Model

Represents a scan of a directory.
"""
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from pipeline.helpers import db


class ScanRun:
    """
    Represents a scan run.
    """

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to create the scan_runs table.

        Returns:
            str: SQL query to create the scan_runs table.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS filesystem.scan_runs (
                scan_id SERIAL PRIMARY KEY,
                scan_root TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at TIMESTAMPTZ NULL,
                total_paths_count BIGINT NULL,
                added_files_count BIGINT NULL,
                modified_files_count BIGINT NULL,
                removed_files_count BIGINT NULL,
                new_data_mb FLOAT NULL,
                modified_data_mb FLOAT NULL,
                deleted_data_mb FLOAT NULL,
                scan_metadata JSONB NULL
            );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the scan_runs table.

        Returns:
            str: SQL query to drop the scan_runs table.
        """
        sql_query = """
            DROP TABLE IF EXISTS filesystem.scan_runs CASCADE;
        """

        return sql_query

    @staticmethod
    def truncate_table_query() -> str:
        """
        Returns the SQL query to truncate the scan_runs table.

        Returns:
            str: SQL query to truncate the scan_runs table.
        """
        sql_query = """
            TRUNCATE TABLE filesystem.scan_runs RESTART IDENTITY CASCADE;
        """

        return sql_query

    @staticmethod
    def start_scan(started_at: datetime, scan_root: Path, config_file: Path) -> int:
        """
        Starts a new scan and returns the scan ID.

        Args:
            started_at (datetime): The start time of the scan.

        Returns:
            int: The scan ID.
        """

        sql_query = f"""
            INSERT INTO filesystem.scan_runs (started_at, scan_root)
            VALUES ('{started_at}', '{scan_root}')
            RETURNING scan_id;
        """

        results = db.execute_queries(
            config_file=config_file,
            queries=[sql_query],
        )

        scan_id = results[0][0][0]

        return scan_id

    @staticmethod
    def finish_scan(
        scan_id: int,
        finished_at: datetime,
        total_paths_count: int,
        added_files_count: int,
        modified_files_count: int,
        removed_files_count: int,
        new_data_mb: float,
        modified_data_mb: float,
        removed_data_mb: float,
        scan_metadata: Dict[str, Any],
        config_file: Path,
    ):
        """
        Finishes a scan and updates the scan run in the database.

        Args:
            scan_id (int): The ID of the scan.
            finished_at (datetime): The finish time of the scan.
            total_paths_count (int): The total number of paths scanned.
            added_files_count (int): The number of files added.
            modified_files_count (int): The number of files modified.
            removed_files_count (int): The number of files removed.
            new_data_mb (int): The amount of new data in MB.
            modified_data_mb (int): The amount of modified data in MB.
            removed_data_mb (int): The amount of removed data in MB.
            scan_metadata (Dict[str, Any]): Metadata about the scan.
            config_file (Path): The path to the configuration file.
        """

        scan_metadata_str = db.sanitize_json(scan_metadata)

        sql_query = f"""
            UPDATE filesystem.scan_runs
            SET finished_at = '{finished_at}',
                total_paths_count = {total_paths_count},
                added_files_count = {added_files_count},
                modified_files_count = {modified_files_count},
                removed_files_count = {removed_files_count},
                new_data_mb = {new_data_mb},
                modified_data_mb = {modified_data_mb},
                deleted_data_mb = {removed_data_mb},
                scan_metadata = '{scan_metadata_str}'
            WHERE scan_id = {scan_id};
        """

        db.execute_queries(
            config_file=config_file,
            queries=[sql_query],
        )
        return
