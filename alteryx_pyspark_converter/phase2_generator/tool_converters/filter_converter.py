"""Convert Alteryx Filter tool to PySpark."""

from .base_converter import BaseToolConverter


class FilterConverter(BaseToolConverter):
    """Convert Alteryx Filter tool to PySpark .filter() operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark filter code.

        Produces two output DataFrames:
        - {output_df_name}_true: Records matching the filter
        - {output_df_name}_false: Records NOT matching the filter
        """
        expr = self.configuration.get("expression", "")
        pyspark_expr = self.expr_converter.convert(expr)

        if not pyspark_expr:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: Empty filter expression\n"
                f"{output_df_name}_true = {input_df_name}\n"
                f"{output_df_name}_false = {input_df_name}.limit(0)\n"
            )

        return (
            f"{self.get_comment()}\n"
            f"{output_df_name}_true = {input_df_name}.filter({pyspark_expr})\n"
            f"{output_df_name}_false = {input_df_name}.filter(~({pyspark_expr}))\n"
        )
