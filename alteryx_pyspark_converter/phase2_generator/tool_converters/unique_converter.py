"""Convert Alteryx Unique tool to PySpark."""

from .base_converter import BaseToolConverter


class UniqueConverter(BaseToolConverter):
    """Convert Alteryx Unique tool to PySpark dropDuplicates/distinct operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark unique/distinct code.

        Produces two outputs:
        - {output_df_name}_unique: Unique records
        - {output_df_name}_dupes: Duplicate records
        """
        unique_fields = self.configuration.get("unique_fields", [])

        if not unique_fields:
            return (
                f"{self.get_comment()}\n"
                f"{output_df_name}_unique = {input_df_name}.distinct()\n"
                f"# Duplicates detection without key fields requires window functions\n"
                f"{output_df_name}_dupes = {input_df_name}.exceptAll("
                f"{output_df_name}_unique)\n"
            )

        mapped_fields = self.map_columns(unique_fields)
        fields_str = ", ".join(f'"{f}"' for f in mapped_fields)

        return (
            f"{self.get_comment()}\n"
            f"{output_df_name}_unique = {input_df_name}.dropDuplicates("
            f"[{fields_str}])\n"
            f"# Duplicate records (those removed by dropDuplicates)\n"
            f"{output_df_name}_dupes = {input_df_name}.exceptAll("
            f"{output_df_name}_unique)\n"
        )
