#!/usr/bin/env python
"""
File Model
"""

from datetime import datetime
from pathlib import Path
from typing import List

from pydantic import BaseModel

from pipeline.helpers import db


class StagingFile(BaseModel):
    """
    Represents a file.

    Attributes:
        file_path (Path): The path to the file.
    """

    file_path: Path
    file_size_bytes: int
    file_mtime: datetime

    def __repr__(self):
        """
        Return a string representation of the File object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL query to create the 'files' table.
        """
        sql_query = """
        CREATE UNLOGGED TABLE filesystem.staging_files (
        scan_id         INT         NOT NULL REFERENCES filesystem.scan_runs(scan_id),
        file_path       TEXT        NOT NULL,
        file_name       TEXT        NOT NULL,
        file_type       TEXT        NOT NULL,
        file_size_bytes BIGINT      NOT NULL,
        file_mtime      TIMESTAMPTZ NOT NULL,

        PRIMARY KEY (scan_id,file_path)
        );
        """

        create_index_query = """
        CREATE INDEX ON filesystem.staging_files (scan_id, file_path);
        """


        return [sql_query, create_index_query]

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS filesystem.staging_files CASCADE;
        """

        return sql_query

    @staticmethod
    def clean_table_query(scan_id: int) -> str:
        """
        Return SQL query to delete contents of 'staging_files' table for the current scan.
        """
        sql_query = f"""
        DELETE FROM filesystem.staging_files WHERE scan_id = {scan_id};
        """

        return sql_query

    @staticmethod
    def clean_table(config_file: Path, scan_id: int) -> None:
        """
        Clean the 'staging_files' table for the given scan_id.

        Args:
            config_file (Path): Path to the configuration file.
            scan_id (int): The ID of the scan for which to clean the table.

        This method executes a SQL query to delete all entries in the 'staging_files' table
        for the specified scan_id.

        It is typically used to clean up the staging area after a scan has been processed.
        """

        sql_query = StagingFile.clean_table_query(scan_id)

        db.execute_queries(
            config_file=config_file,
            queries=[sql_query],
        )
