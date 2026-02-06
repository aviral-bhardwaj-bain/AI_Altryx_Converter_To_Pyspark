"""Column mapping handler for Alteryx to Databricks column name translation."""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class ColumnMapper:
    """
    Map Alteryx column names to Databricks column names.

    Handles bidirectional mapping and expression conversion.
    """

    def __init__(self, mappings: dict[str, str]):
        """
        Args:
            mappings: Dictionary mapping Alteryx column names to Databricks
                     column names. E.g., {"nps_type": "typ_nps"}.
        """
        self.mappings = dict(mappings)  # alteryx -> databricks
        self.reverse_mappings = {v: k for k, v in mappings.items()}

    def to_databricks(self, alteryx_col: str) -> str:
        """
        Convert an Alteryx column name to its Databricks equivalent.

        If no mapping exists, the column name is returned as-is.
        """
        return self.mappings.get(alteryx_col, alteryx_col)

    def to_alteryx(self, databricks_col: str) -> str:
        """
        Convert a Databricks column name to its Alteryx equivalent.

        If no mapping exists, the column name is returned as-is.
        """
        return self.reverse_mappings.get(databricks_col, databricks_col)

    def convert_column_list(self, alteryx_cols: list[str]) -> list[str]:
        """Convert a list of Alteryx column names to Databricks names."""
        return [self.to_databricks(c) for c in alteryx_cols]

    def convert_expression(self, alteryx_expr: str) -> str:
        """
        Convert column references in an Alteryx expression to Databricks names.

        Replaces [column_name] references with the mapped Databricks names.
        """
        def replace_col_ref(match: re.Match) -> str:
            col_name = match.group(1)
            mapped = self.to_databricks(col_name)
            return f"[{mapped}]"

        return re.sub(r'\[([^\]]+)\]', replace_col_ref, alteryx_expr)

    def get_unmapped_columns(self, columns: list[str]) -> list[str]:
        """
        Get columns that have no explicit mapping.

        These columns will pass through with their original names.
        """
        return [c for c in columns if c not in self.mappings]

    def get_all_mappings(self) -> dict[str, str]:
        """Get all current mappings."""
        return dict(self.mappings)

    def add_mapping(self, alteryx_col: str, databricks_col: str) -> None:
        """Add a single column mapping."""
        self.mappings[alteryx_col] = databricks_col
        self.reverse_mappings[databricks_col] = alteryx_col

    def validate_against_schema(
        self,
        columns: list[str],
        available_columns: list[str],
    ) -> dict[str, list[str]]:
        """
        Validate that all mapped columns exist in the target schema.

        Args:
            columns: Alteryx column names used in the workflow.
            available_columns: Column names available in Databricks tables.

        Returns:
            Dictionary with 'valid', 'missing', and 'unmapped' lists.
        """
        valid = []
        missing = []
        unmapped = []

        available_set = set(available_columns)

        for col in columns:
            mapped = self.to_databricks(col)
            if mapped in available_set:
                valid.append(col)
            elif col in self.mappings:
                missing.append(f"{col} -> {mapped} (NOT FOUND in schema)")
            else:
                unmapped.append(col)

        return {
            "valid": valid,
            "missing": missing,
            "unmapped": unmapped,
        }

    def __repr__(self) -> str:
        return f"ColumnMapper({len(self.mappings)} mappings)"
