"""Convert Alteryx Formula tool to PySpark."""

from .base_converter import BaseToolConverter


class FormulaConverter(BaseToolConverter):
    """Convert Alteryx Formula tool to PySpark .withColumn() operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark code for Formula tool.

        Each formula becomes a .withColumn() call.
        """
        formulas = self.configuration.get("formulas", [])

        if not formulas:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: No formulas defined\n"
                f"{output_df_name} = {input_df_name}\n"
            )

        lines = [self.get_comment()]
        current_df = input_df_name

        for i, formula in enumerate(formulas):
            field_name = formula.get("field", "")
            expression = formula.get("expression", "")

            if not field_name or not expression:
                lines.append(f"# WARNING: Skipping empty formula at index {i}")
                continue

            # Map the output field name
            mapped_field = self.map_column(field_name)

            # Convert the expression
            pyspark_expr = self.expr_converter.convert(expression)

            if i == len(formulas) - 1:
                # Last formula - use output name
                target_df = output_df_name
            else:
                target_df = current_df

            lines.append(
                f'{target_df} = {current_df}.withColumn("{mapped_field}", {pyspark_expr})'
            )
            current_df = target_df

        # If no valid formulas were converted, just pass through
        if len(lines) == 1:
            lines.append(f"{output_df_name} = {input_df_name}")

        return "\n".join(lines) + "\n"
