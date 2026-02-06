"""Convert Alteryx Sample tool to PySpark."""

from .base_converter import BaseToolConverter


class SampleConverter(BaseToolConverter):
    """Convert Alteryx Sample tool to PySpark limit/sample operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """Generate PySpark sample/limit code."""
        mode = self.configuration.get("mode", "First")
        n = self.configuration.get("n", 1)
        group_fields = self.configuration.get("group_fields", [])

        if group_fields:
            # Sample per group using window functions
            mapped_fields = self.map_columns(group_fields)
            partition_str = ", ".join(f'F.col("{f}")' for f in mapped_fields)

            return (
                f"{self.get_comment()}\n"
                f"from pyspark.sql.window import Window\n"
                f"_w = Window.partitionBy({partition_str})"
                f".orderBy(F.monotonically_increasing_id())\n"
                f"{output_df_name} = {input_df_name}"
                f'.withColumn("_row_num", F.row_number().over(_w))'
                f' \\\n    .filter(F.col("_row_num") <= {n})'
                f' \\\n    .drop("_row_num")\n'
            )

        if mode == "First":
            return (
                f"{self.get_comment()}\n"
                f"{output_df_name} = {input_df_name}.limit({n})\n"
            )
        elif mode == "Last":
            return (
                f"{self.get_comment()}\n"
                f"# Last N records - reverse sort then limit\n"
                f'{output_df_name} = {input_df_name}'
                f'.orderBy(F.monotonically_increasing_id().desc())'
                f'.limit({n})\n'
            )
        elif mode == "Skip":
            return (
                f"{self.get_comment()}\n"
                f"# Skip first {n} records\n"
                f'{output_df_name} = {input_df_name}'
                f'.withColumn("_row_num", F.monotonically_increasing_id())'
                f' \\\n    .filter(F.col("_row_num") >= {n})'
                f' \\\n    .drop("_row_num")\n'
            )
        elif mode == "Percent":
            fraction = n / 100.0
            return (
                f"{self.get_comment()}\n"
                f"{output_df_name} = {input_df_name}.sample(fraction={fraction})\n"
            )
        elif mode == "Random":
            return (
                f"{self.get_comment()}\n"
                f"{output_df_name} = {input_df_name}"
                f".orderBy(F.rand()).limit({n})\n"
            )
        else:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: Unknown sample mode '{mode}'\n"
                f"{output_df_name} = {input_df_name}.limit({n})\n"
            )
