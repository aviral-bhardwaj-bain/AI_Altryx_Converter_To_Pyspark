"""Convert Alteryx CrossTab tool to PySpark."""

from .base_converter import BaseToolConverter


class CrossTabConverter(BaseToolConverter):
    """Convert Alteryx CrossTab tool to PySpark pivot operations."""

    # Map Alteryx aggregation method to PySpark aggregate function
    METHOD_MAP = {
        "Concatenate": "F.first",
        "Sum": "F.sum",
        "Count": "F.count",
        "CountDistinct": "F.countDistinct",
        "First": "F.first",
        "Last": "F.last",
        "Min": "F.min",
        "Max": "F.max",
        "Avg": "F.avg",
    }

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark pivot/crosstab code.

        Converts Alteryx CrossTab to:
            df.groupBy(group_cols).pivot(header_col).agg(method(data_col))
        """
        group_fields = self.configuration.get("group_fields", [])
        header_field = self.configuration.get("header_field", "")
        data_field = self.configuration.get("data_field", "")
        method = self.configuration.get("method", "Concatenate")
        separator = self.configuration.get("separator", "")

        # Map column names
        mapped_group = self.map_columns(group_fields)
        mapped_header = self.map_column(header_field)
        mapped_data = self.map_column(data_field)

        # Get PySpark aggregate function
        agg_func = self.METHOD_MAP.get(method, "F.first")

        # Handle concatenation with separator
        if method == "Concatenate" and separator:
            agg_expr = (
                f'F.concat_ws("{separator}", '
                f'F.collect_list(F.col("{mapped_data}")))'
            )
        else:
            agg_expr = f'{agg_func}(F.col("{mapped_data}"))'

        # Build group by columns
        if mapped_group:
            group_cols = ", ".join(f'"{c}"' for c in mapped_group)
        else:
            group_cols = ""

        code = (
            f"{self.get_comment()}\n"
            f'{output_df_name} = {input_df_name}'
        )

        if group_cols:
            code += f'.groupBy({group_cols})'
        else:
            code += '.groupBy()'

        code += f' \\\n    .pivot("{mapped_header}")'
        code += f" \\\n    .agg({agg_expr})\n"

        return code
