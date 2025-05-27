#!/usr/bin/env python
"""
FileChanges Model
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, List

from pydantic import BaseModel

from pipeline.helpers import db
from pipeline.models.files import File


class FileChanges(BaseModel):
    """
    Represents a file change.
    """

    file_path: Path
    scan_id: Optional[int] = None
    old_size: Optional[int] = None
    new_size: Optional[int] = None
    old_mtime: Optional[datetime] = None
    new_mtime: Optional[datetime] = None
    old_fingerprint: Optional[str] = None
    new_fingerprint: Optional[str] = None
    change_type: Optional[str] = None
    recorded_at: Optional[datetime] = None

    @staticmethod
    def get_file_changes(
        old_file: Optional[File],
        new_file: Optional[File],
    ) -> Optional["FileChanges"]:
        """
        Get the file change between two files.
        """
        if old_file is None and new_file is None:
            raise ValueError("Both old_file and new_file cannot be None")

        changes: Optional[FileChanges] = None

        if old_file is not None and new_file is not None:
            # If both files are present, compare their fingerprints
            if old_file.file_fingerprint != new_file.file_fingerprint:
                changes = FileChanges(
                    scan_id=new_file.last_seen_scan,
                    file_path=new_file.file_path,
                    old_size=old_file.file_size_bytes,
                    new_size=new_file.file_size_bytes,
                    old_mtime=old_file.file_mtime,
                    new_mtime=new_file.file_mtime,
                    old_fingerprint=old_file.file_fingerprint,
                    new_fingerprint=new_file.file_fingerprint,
                    change_type="modified",
                )
            else:
                # If the fingerprints are the same, no changes
                return None

        if changes is None and old_file is not None:
            changes = FileChanges(
                scan_id=old_file.last_seen_scan,
                file_path=old_file.file_path,
                old_size=old_file.file_size_bytes,
                old_mtime=old_file.file_mtime,
                old_fingerprint=old_file.file_fingerprint,
                change_type="deleted",
            )

        if changes is None and new_file is not None:
            changes = FileChanges(
                scan_id=new_file.last_seen_scan,
                file_path=new_file.file_path,
                new_size=new_file.file_size_bytes,
                new_mtime=new_file.file_mtime,
                new_fingerprint=new_file.file_fingerprint,
                change_type="added",
            )

        if changes is None:
            raise ValueError("No changes detected between the files")

        # Set the recorded_at timestamp to the current time
        changes.recorded_at = datetime.now()

        return changes

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Returns the SQL query to create the file_changes table.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS filesystem.file_changes (
                scan_id             INT           NOT NULL
                    REFERENCES filesystem.scan_runs(scan_id) ON DELETE CASCADE,
                file_path           TEXT          NOT NULL,
                change_type         TEXT          NOT NULL,
                old_size_bytes      BIGINT        NULL,
                new_size_bytes      BIGINT        NULL,
                old_mtime           TIMESTAMPTZ   NULL,
                new_mtime           TIMESTAMPTZ   NULL,
                old_fingerprint     TEXT          NULL,
                new_fingerprint     TEXT          NULL,
                recorded_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
                
                PRIMARY KEY (scan_id, file_path)
            );
        """

        create_index_query_1 = """
            CREATE INDEX ON filesystem.file_changes (change_type);
        """
        create_index_query_2 = """
            CREATE INDEX ON filesystem.file_changes (scan_id);
        """

        return [sql_query, create_index_query_1, create_index_query_2]

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the file_changes table.
        """
        sql_query = """
            DROP TABLE IF EXISTS filesystem.file_changes CASCADE;
        """

        return sql_query

    @staticmethod
    def truncate_table_query() -> str:
        """
        Returns the SQL query to truncate the file_changes table.
        """
        sql_query = """
            TRUNCATE TABLE filesystem.file_changes RESTART IDENTITY CASCADE;
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Convert the FileChanges object to an SQL insert statement.
        Handles None values by converting them to SQL NULL.
        Escapes single quotes in string values.
        """

        def sql_value_formatter(value):
            if value is None:
                return "NULL"
            if isinstance(value, (str, Path, datetime)):
                # Convert to string, then escape single quotes for SQL
                str_value = str(value)
                return db.santize_string(str_value)

            # For numbers (int, float) and other types that don't need quotes
            return str(value)

        # Apply the formatter to each attribute
        scan_id_sql = sql_value_formatter(self.scan_id)
        file_path_sql = sql_value_formatter(self.file_path)
        change_type_sql = sql_value_formatter(self.change_type)
        old_size_sql = sql_value_formatter(self.old_size)
        new_size_sql = sql_value_formatter(self.new_size)
        old_mtime_sql = sql_value_formatter(self.old_mtime)
        new_mtime_sql = sql_value_formatter(self.new_mtime)
        old_fingerprint_sql = sql_value_formatter(self.old_fingerprint)
        new_fingerprint_sql = sql_value_formatter(self.new_fingerprint)
        recorded_at_sql = sql_value_formatter(self.recorded_at)

        sql_query = f"""
            INSERT INTO filesystem.file_changes (
                scan_id,
                file_path,
                change_type,
                old_size_bytes,
                new_size_bytes,
                old_mtime,
                new_mtime,
                old_fingerprint,
                new_fingerprint,
                recorded_at
            ) VALUES (
                {scan_id_sql},
                '{file_path_sql}',
                '{change_type_sql}',
                {old_size_sql},
                {new_size_sql},
                '{old_mtime_sql}',
                '{new_mtime_sql}',
                '{old_fingerprint_sql}',
                '{new_fingerprint_sql}',
                '{recorded_at_sql}'
            );
        """

        sql_query = db.handle_null(sql_query)

        return sql_query

    @staticmethod
    def get_files_count_by_change_type(
        scan_id: int,
        change_type: str,  # 'added', 'modified', or 'deleted'
        config_file: Path,
    ) -> Optional[int]:
        """
        Get the count of files by change type for a specific scan.

        Supports change types: 'added', 'modified', 'deleted'.

        Args:
            scan_id (int): The ID of the scan.
            config_file (Path): The path to the configuration file.
        Returns:
            int: The count of files with the specified change type for the given scan ID.
        """

        sql_query = f"""
            SELECT COUNT(*) FROM filesystem.file_changes
            WHERE scan_id = {scan_id} AND change_type = '{change_type}';
        """

        result = db.fetch_record(
            config_file=config_file,
            query=sql_query,
        )

        if result is None:
            return None

        count = int(result)
        return count

    @staticmethod
    def get_file_size_by_change_type(
        scan_id: int,
        change_type: str,  # 'added', 'modified', or 'deleted'
        config_file: Path,
    ) -> float:
        """
        Get the total size of files by change type for a specific scan.

        Supports change types: 'added', 'modified', 'deleted'.

        Args:
            scan_id (int): The ID of the scan.
            config_file (Path): The path to the configuration file.
        Returns:
            float: The total size of files with the specified change type for the given scan ID.
        """

        sql_query = f"""
            SELECT SUM(new_size_mb) FROM filesystem.file_changes
            WHERE scan_id = {scan_id} AND change_type = '{change_type}';
        """

        result = db.fetch_record(
            config_file=config_file,
            query=sql_query,
        )

        if result is None or result == 'None':
            return 0.0

        total_size = float(result)
        return total_size
