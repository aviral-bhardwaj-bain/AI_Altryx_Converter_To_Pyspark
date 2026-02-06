"""Validate column mappings are complete."""

import logging
from typing import Optional

from ...phase1_parser.models.workflow import Workflow
from ...phase1_parser.parser.formula_extractor import FormulaExtractor
from ..config.column_mapping import ColumnMapper

logger = logging.getLogger(__name__)


class MappingValidator:
    """
    Validate that all column mappings are complete and consistent.
    """

    def __init__(
        self,
        workflow: Workflow,
        column_mapper: ColumnMapper,
    ):
        self.workflow = workflow
        self.column_mapper = column_mapper
        self.formula_extractor = FormulaExtractor()

    def validate_container_mappings(
        self,
        container_id: int,
    ) -> dict:
        """
        Validate all column mappings needed by a container.

        Args:
            container_id: The container to validate.

        Returns:
            Validation result with mapped, unmapped, and unused mappings.
        """
        container = self.workflow.get_container(container_id)
        if not container:
            return {"valid": False, "error": f"Container {container_id} not found"}

        # Collect all columns referenced in the container
        all_columns: set[str] = set()
        tools = self.workflow.get_container_tools(container_id)

        for tool in tools:
            cols = self.formula_extractor.extract_all_columns_from_tool(
                tool.configuration
            )
            all_columns.update(cols)

        # Check mappings
        mapped = []
        unmapped = []
        all_mappings = self.column_mapper.get_all_mappings()

        for col in sorted(all_columns):
            if col in all_mappings:
                mapped.append(f"{col} -> {all_mappings[col]}")
            else:
                unmapped.append(col)

        # Find unused mappings
        used_keys = all_columns & set(all_mappings.keys())
        unused = [
            f"{k} -> {v}"
            for k, v in all_mappings.items()
            if k not in all_columns
        ]

        return {
            "valid": True,  # Unmapped columns pass through with original names
            "total_columns": len(all_columns),
            "mapped": mapped,
            "unmapped": unmapped,
            "unused_mappings": unused,
            "mapping_coverage": (
                f"{len(mapped)}/{len(all_columns)} "
                f"({100 * len(mapped) / max(len(all_columns), 1):.0f}%)"
            ),
        }

    def get_required_columns(self, container_id: int) -> set[str]:
        """Get all columns that a container references."""
        all_columns: set[str] = set()
        tools = self.workflow.get_container_tools(container_id)

        for tool in tools:
            cols = self.formula_extractor.extract_all_columns_from_tool(
                tool.configuration
            )
            all_columns.update(cols)

        return all_columns

    def suggest_mappings(
        self,
        container_id: int,
        available_columns: list[str],
    ) -> list[dict]:
        """
        Suggest column mappings using fuzzy matching.

        Args:
            container_id: Container to analyze.
            available_columns: Available Databricks column names.

        Returns:
            List of suggested mappings.
        """
        required = self.get_required_columns(container_id)
        available_set = set(available_columns)
        available_lower = {c.lower(): c for c in available_columns}

        suggestions = []

        for col in sorted(required):
            # Exact match
            if col in available_set:
                suggestions.append({
                    "alteryx_col": col,
                    "suggested": col,
                    "confidence": "exact",
                })
                continue

            # Case-insensitive match
            if col.lower() in available_lower:
                suggestions.append({
                    "alteryx_col": col,
                    "suggested": available_lower[col.lower()],
                    "confidence": "case_insensitive",
                })
                continue

            # Substring match
            matches = [
                c for c in available_columns
                if col.lower() in c.lower() or c.lower() in col.lower()
            ]
            if matches:
                suggestions.append({
                    "alteryx_col": col,
                    "suggested": matches[0],
                    "confidence": "partial",
                    "alternatives": matches[1:5],
                })
                continue

            # No match found
            suggestions.append({
                "alteryx_col": col,
                "suggested": None,
                "confidence": "none",
            })

        return suggestions
