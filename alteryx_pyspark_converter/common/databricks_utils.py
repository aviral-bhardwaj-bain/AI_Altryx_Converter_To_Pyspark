"""Databricks volume and workspace utilities."""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def is_databricks_environment() -> bool:
    """Check if running in a Databricks environment."""
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or os.path.exists("/databricks")
    )


def get_volume_path(
    catalog: str,
    schema: str,
    volume: str,
    sub_path: str = "",
) -> str:
    """
    Construct a Databricks volume path.

    Args:
        catalog: Unity Catalog name.
        schema: Schema name.
        volume: Volume name.
        sub_path: Optional sub-path within the volume.

    Returns:
        Full volume path.
    """
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    if sub_path:
        return os.path.join(base, sub_path)
    return base


def get_workspace_path(
    user_email: str,
    sub_path: str = "",
) -> str:
    """
    Construct a Databricks workspace path.

    Args:
        user_email: User's email address.
        sub_path: Optional sub-path.

    Returns:
        Full workspace path.
    """
    base = f"/Workspace/Users/{user_email}"
    if sub_path:
        return os.path.join(base, sub_path)
    return base


def resolve_path(path: str) -> str:
    """
    Resolve a path for the current environment.

    In Databricks, /dbfs/ paths are converted to FUSE mount paths.
    In local environments, paths are used as-is.
    """
    if is_databricks_environment():
        # Convert /dbfs/ paths to direct FUSE paths if needed
        if path.startswith("/dbfs/"):
            return path
        if path.startswith("dbfs:/"):
            return path.replace("dbfs:/", "/dbfs/", 1)
    return path


def read_table_schema(
    spark: Optional[object],
    table_name: str,
) -> list[dict]:
    """
    Read the schema of a Databricks table.

    Args:
        spark: SparkSession (only available in Databricks).
        table_name: Fully qualified table name (catalog.schema.table).

    Returns:
        List of field definitions.
    """
    if spark is None:
        logger.warning(
            "SparkSession not available. Cannot read schema for %s",
            table_name,
        )
        return []

    try:
        df = spark.table(table_name)  # type: ignore
        return [
            {
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable,
            }
            for field in df.schema.fields  # type: ignore
        ]
    except Exception as e:
        logger.error("Error reading schema for %s: %s", table_name, e)
        return []
