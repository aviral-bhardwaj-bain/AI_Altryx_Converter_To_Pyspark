"""Convert Alteryx Union tool to PySpark."""

from .base_converter import BaseToolConverter


class UnionConverter(BaseToolConverter):
    """Convert Alteryx Union tool to PySpark unionByName operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark union code.

        Args:
            input_df_name: First input DataFrame name.
            output_df_name: Output DataFrame name.
            **kwargs: Must include 'additional_inputs' - list of other DF names.
        """
        additional_inputs = kwargs.get("additional_inputs", [])

        if not additional_inputs:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: Union with single input\n"
                f"{output_df_name} = {input_df_name}\n"
            )

        lines = [self.get_comment()]

        # Use unionByName with allowMissingColumns for compatibility
        current = input_df_name
        for other_df in additional_inputs:
            lines.append(
                f'{current} = {current}.unionByName('
                f'{other_df}, allowMissingColumns=True)'
            )

        # Assign final result
        if current != output_df_name:
            lines.append(f"{output_df_name} = {current}")

        return "\n".join(lines) + "\n"
