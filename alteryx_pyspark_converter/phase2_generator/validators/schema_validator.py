"""Validate columns exist in target schemas."""

import logging
from typing import Optional

from ..config.schema_config import SchemaConfig
from ..config.column_mapping import ColumnMapper

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validate that all referenced columns exist in the target schemas.
    """

    def __init__(
        self,
        schema_config: SchemaConfig,
        column_mapper: ColumnMapper,
    ):
        self.schema_config = schema_config
        self.column_mapper = column_mapper

    def validate_table_columns(
        self,
        table_key: str,
        columns: list[str],
    ) -> dict:
        """
        Validate that columns exist in a specific table.

        Args:
            table_key: Config key for the table.
            columns: Alteryx column names to validate.

        Returns:
            Validation result dictionary.
        """
        schema = self.schema_config.get_schema(table_key)
        if schema is None:
            return {
                "valid": True,
                "warning": f"No schema found for {table_key}. Skipping validation.",
                "missing": [],
                "found": [],
            }

        if not schema.fields:
            return {
                "valid": True,
                "warning": f"Empty schema for {table_key}. Skipping validation.",
                "missing": [],
                "found": [],
            }

        available = schema.get_column_names()
        result = self.column_mapper.validate_against_schema(columns, available)

        is_valid = len(result["missing"]) == 0
        if not is_valid:
            logger.warning(
                "Schema validation failed for %s. Missing columns: %s",
                table_key,
                result["missing"],
            )

        return {
            "valid": is_valid,
            "table": table_key,
            "found": result["valid"],
            "missing": result["missing"],
            "unmapped": result["unmapped"],
        }

    def validate_all(
        self,
        columns_by_table: dict[str, list[str]],
    ) -> dict:
        """
        Validate all columns across all tables.

        Args:
            columns_by_table: Maps table config key -> list of Alteryx column names.

        Returns:
            Overall validation result.
        """
        results = {}
        all_valid = True

        for table_key, columns in columns_by_table.items():
            result = self.validate_table_columns(table_key, columns)
            results[table_key] = result
            if not result["valid"]:
                all_valid = False

        return {
            "valid": all_valid,
            "tables": results,
        }
