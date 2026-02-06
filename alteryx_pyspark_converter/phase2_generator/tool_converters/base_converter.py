"""Abstract base class for all tool converters."""

from abc import ABC, abstractmethod
from typing import Optional

from ..config.column_mapping import ColumnMapper
from ..expression_converter.alteryx_to_pyspark import AlteryxExpressionConverter


class BaseToolConverter(ABC):
    """
    Base class for all Alteryx tool to PySpark converters.

    Each subclass handles the conversion of a specific Alteryx tool type
    into equivalent PySpark code.
    """

    def __init__(
        self,
        tool_config: dict,
        column_mapper: ColumnMapper,
        expression_converter: AlteryxExpressionConverter,
    ):
        """
        Args:
            tool_config: The tool's configuration from intermediate JSON.
            column_mapper: Column name mapper (Alteryx -> Databricks).
            expression_converter: Expression converter instance.
        """
        self.tool_config = tool_config
        self.column_mapper = column_mapper
        self.expr_converter = expression_converter
        self.tool_id = tool_config.get("tool_id", 0)
        self.annotation = tool_config.get("annotation", "")
        self.configuration = tool_config.get("configuration", {})

    @abstractmethod
    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark code for this tool.

        Args:
            input_df_name: Name of the input DataFrame variable.
            output_df_name: Name to use for the output DataFrame variable.
            **kwargs: Additional arguments (e.g., right_df_name for joins).

        Returns:
            PySpark code string.
        """
        pass

    def get_comment(self) -> str:
        """Get a comment describing this tool conversion."""
        tool_type = self.tool_config.get("tool_type", "Unknown")
        comment = f"# {tool_type}"
        if self.annotation:
            comment += f": {self.annotation}"
        comment += f" (Tool ID: {self.tool_id})"
        return comment

    def map_column(self, col_name: str) -> str:
        """Map a single column name from Alteryx to Databricks."""
        return self.column_mapper.to_databricks(col_name)

    def map_columns(self, col_names: list[str]) -> list[str]:
        """Map a list of column names."""
        return [self.column_mapper.to_databricks(c) for c in col_names]
