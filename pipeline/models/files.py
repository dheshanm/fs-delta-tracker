#!/usr/bin/env python
"""
File Model
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, List

from pydantic import BaseModel

from pipeline.helpers import db
from pipeline.helpers.hash import compute_fingerprint


class File(BaseModel):
    """
    Represents a file.

    Attributes:
        file_path (Path): The path to the file.
    """

    file_path: Path
    file_name: str
    file_type: str
    file_size_bytes: int
    file_mtime: datetime
    file_fingerprint: str
    last_seen_scan: Optional[int] = None

    @classmethod
    def from_path(cls, file_path: Path, fingerprint: Optional[str] = None) -> "File":
        """
        Initialize a File object from a given path.

        Args:
            file_path (Path): The path to the file.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_name = file_path.name
        file_type = file_path.suffix
        if file_type == ".lock" and len(file_path.suffixes) > 1:
            # Use previous suffix for lock files
            file_type = file_path.suffixes[-2]
        elif file_type == ".lock":  # case where .lock is the only suffix
            file_type = ".lock"

        file_size_bytes = file_path.stat().st_size
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        if fingerprint is None:
            try:
                file_fingerprint = compute_fingerprint(file_path=file_path)
            except PermissionError:
                file_fingerprint = "PermissionError"
        else:
            file_fingerprint = fingerprint

        return cls(
            file_path=file_path,
            file_name=file_name,
            file_type=file_type,
            file_size_bytes=file_size_bytes,
            file_mtime=file_mtime,
            file_fingerprint=file_fingerprint,
        )

    @staticmethod
    def update(old_data: "File") -> "File":
        """
        Attempt to reuse an existing File object.
        If the file has changed, update its attributes.

        Args:
            old_data (File): The existing File object.

        Returns:
            File: The updated File object.
        """
        new_file = File.from_path(
            file_path=old_data.file_path,
            fingerprint=old_data.file_fingerprint,
        )

        if (
            new_file.file_mtime != old_data.file_mtime
            or new_file.file_size_bytes != old_data.file_size_bytes
        ):
            new_file.file_fingerprint = compute_fingerprint(
                file_path=new_file.file_path
            )

        return new_file

    @staticmethod
    def get(file_path: Path, config_file: Path) -> Optional["File"]:
        """
        Get a File object from the database.

        Args:
            file_path (Path): The path to the file.

        Returns:
            Optional[File]: The File object if found, None otherwise.
        """
        sql_query = f"""
        SELECT * FROM filesystem.files
        WHERE file_path = '{file_path}';
        """

        result = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        if result.empty:
            return None

        file_name = result.iloc[0]["file_name"]
        file_type = result.iloc[0]["file_type"]
        file_size_bytes = result.iloc[0]["file_size_bytes"]
        file_mtime = result.iloc[0]["file_mtime"]
        file_fingerprint = result.iloc[0]["file_fingerprint"]
        last_seen_scan = result.iloc[0]["last_seen_scan"]

        return File(
            file_path=file_path,
            file_name=file_name,
            file_type=file_type,
            file_size_bytes=file_size_bytes,
            file_mtime=file_mtime,
            file_fingerprint=file_fingerprint,
            last_seen_scan=last_seen_scan,
        )

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
        CREATE TABLE IF NOT EXISTS filesystem.files (
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            file_path TEXT PRIMARY KEY,
            file_mtime TIMESTAMPTZ NOT NULL,
            file_fingerprint TEXT NOT NULL,
            last_seen_scan INT NOT NULL REFERENCES filesystem.scan_runs(scan_id) ON UPDATE CASCADE,
            last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT file_path_unique UNIQUE (file_path)
        );
        """

        create_index_query = """
        CREATE INDEX ON filesystem.files (last_seen_scan);
        """

        return [sql_query, create_index_query]

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS filesystem.files CASCADE;
        """

        return sql_query

    @staticmethod
    def truncate_table_query() -> str:
        """
        Return SQL query to truncate conetnes of 'files' table.
        """
        sql_query = """
        TRUNCATE TABLE files RESTART IDENTITY CASCADE;
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the File object into the 'files' table.
        """
        f_name = db.santize_string(self.file_name)
        f_path = db.santize_string(str(self.file_path))

        sql_query = f"""
        INSERT INTO filesystem.files (
            file_name, file_type, file_size_bytes,
            file_path, file_mtime, file_fingerprint,
            last_seen_scan, last_updated
        ) VALUES (
            '{f_name}', '{self.file_type}', {self.file_size_bytes},
            '{f_path}', '{self.file_mtime}', '{self.file_fingerprint}',
            {self.last_seen_scan}, now()
        )
        ON CONFLICT (file_path) DO UPDATE
        SET file_name = EXCLUDED.file_name,
            file_type = EXCLUDED.file_type,
            file_size_bytes = EXCLUDED.file_size_bytes,
            file_mtime = EXCLUDED.file_mtime,
            file_fingerprint = EXCLUDED.file_fingerprint,
            last_seen_scan = EXCLUDED.last_seen_scan,
            last_updated = now();
        """

        sql_query = db.handle_null(sql_query)

        return sql_query
