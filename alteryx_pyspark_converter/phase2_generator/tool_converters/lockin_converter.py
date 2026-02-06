"""Convert Alteryx LockIn* (In-Database) tools to PySpark.

LockIn tools are the in-database equivalents of standard Alteryx tools.
They map to the same PySpark operations as their standard counterparts.
"""

from .filter_converter import FilterConverter
from .join_converter import JoinConverter
from .formula_converter import FormulaConverter
from .select_converter import SelectConverter
from .crosstab_converter import CrossTabConverter
from .summarize_converter import SummarizeConverter
from .union_converter import UnionConverter
from .sort_converter import SortConverter
from .unique_converter import UniqueConverter
from .sample_converter import SampleConverter
from .base_converter import BaseToolConverter


class LockInInputConverter(BaseToolConverter):
    """Convert LockInInput to PySpark spark.table() read."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """Generate code to read from a Databricks table."""
        table_name = kwargs.get("table_name", "")
        conn_string = self.configuration.get("connection_string", "")
        query = self.configuration.get("query", "")

        if table_name:
            return (
                f"{self.get_comment()}\n"
                f'{output_df_name} = spark.table("{table_name}")\n'
            )
        elif query:
            return (
                f"{self.get_comment()}\n"
                f"# Original query: {query[:100]}...\n"
                f'{output_df_name} = spark.sql("""{query}""")\n'
            )
        else:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: No table or query specified for LockInInput\n"
                f"# Original connection: {conn_string[:100]}\n"
                f'{output_df_name} = spark.table("REPLACE_WITH_TABLE_NAME")\n'
            )


class LockInWriteConverter(BaseToolConverter):
    """Convert LockInWrite to PySpark table write."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """Generate code to write to a Databricks table."""
        table_name = kwargs.get(
            "table_name",
            self.configuration.get("table_name", ""),
        )
        output_type = self.configuration.get("output_type", "overwrite")

        mode = "overwrite" if "overwrite" in output_type.lower() else "append"

        if table_name:
            return (
                f"{self.get_comment()}\n"
                f'{input_df_name}.write.mode("{mode}")'
                f'.saveAsTable("{table_name}")\n'
                f"{output_df_name} = {input_df_name}\n"
            )
        else:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: No output table specified\n"
                f'{input_df_name}.write.mode("{mode}")'
                f'.saveAsTable("REPLACE_WITH_TABLE_NAME")\n'
                f"{output_df_name} = {input_df_name}\n"
            )


# Re-export LockIn converters that are identical to standard converters
LockInFilterConverter = FilterConverter
LockInJoinConverter = JoinConverter
LockInFormulaConverter = FormulaConverter
LockInSelectConverter = SelectConverter
LockInCrossTabConverter = CrossTabConverter
LockInSummarizeConverter = SummarizeConverter
LockInUnionConverter = UnionConverter
LockInSortConverter = SortConverter
LockInUniqueConverter = UniqueConverter
LockInSampleConverter = SampleConverter
