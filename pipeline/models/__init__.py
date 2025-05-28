"""
Models used in the pipeline.
"""

from pathlib import Path
from typing import List, Union

from pipeline.helpers import db
from pipeline.models.scans import ScanRun
from pipeline.models.file_changes import FileChanges
from pipeline.models.files import File
from pipeline.models.staging_files import StagingFile


def flatten_list(coll: list) -> list:
    """
    Flattens a list of lists into a single list.

    Args:
        coll (list): List of lists.

    Returns:
        list: Flattened list.
    """
    flat_list = []
    for i in coll:
        if isinstance(i, list):
            flat_list += flatten_list(i)
        else:
            flat_list.append(i)
    return flat_list


def init_schema() -> List[str]:
    """
    Initializes the schema for the pipeline.

    Returns:
        List[str]: List of SQL queries to create the schema.
    """

    create_schema = """
        CREATE SCHEMA IF NOT EXISTS filesystem;
    """

    return [create_schema]


def init_db(config_file: Path):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        config_file (Path): Path to the config file.
    """

    drop_queries_l: List[Union[str, List[str]]] = [
        FileChanges.drop_table_query(),
        File.drop_table_query(),
        ScanRun.drop_table_query(),
        StagingFile.drop_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        ScanRun.init_table_query(),
        File.init_table_query(),
        FileChanges.init_table_query(),
        StagingFile.init_table_query(),
    ]

    schema_queries = init_schema()
    drop_queries = flatten_list(drop_queries_l)
    create_queries = flatten_list(create_queries_l)

    sql_queries: List[str] = schema_queries + drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)
