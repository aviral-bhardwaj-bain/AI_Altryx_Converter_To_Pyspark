"""Convert Alteryx Summarize tool to PySpark."""

from .base_converter import BaseToolConverter


class SummarizeConverter(BaseToolConverter):
    """Convert Alteryx Summarize tool to PySpark groupBy/agg operations."""

    # Map Alteryx action to PySpark aggregate function
    ACTION_MAP = {
        "GroupBy": None,  # Not an aggregation
        "Sum": "F.sum",
        "Count": "F.count",
        "CountDistinct": "F.countDistinct",
        "CountNonNull": "F.count",  # F.count already ignores nulls
        "Min": "F.min",
        "Max": "F.max",
        "Avg": "F.avg",
        "First": "F.first",
        "Last": "F.last",
        "Concat": "F.concat_ws",
        "Concatenate": "F.concat_ws",
        "Median": "F.percentile_approx",
        "Mode": "F.mode",
        "StdDev": "F.stddev",
        "Variance": "F.variance",
    }

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark groupBy/agg code.

        Converts Alteryx Summarize to:
            df.groupBy(group_cols).agg(
                F.sum("col").alias("alias"),
                F.count("col").alias("alias"),
                ...
            )
        """
        fields = self.configuration.get("fields", [])

        if not fields:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: No summarize fields defined\n"
                f"{output_df_name} = {input_df_name}\n"
            )

        group_by_cols = []
        agg_exprs = []

        for field_config in fields:
            field_name = field_config.get("field", "")
            action = field_config.get("action", "GroupBy")
            output_name = field_config.get("output_name", field_name)

            mapped_field = self.map_column(field_name)
            mapped_output = self.map_column(output_name)

            if action == "GroupBy":
                group_by_cols.append(mapped_field)
            else:
                agg_func = self.ACTION_MAP.get(action)
                if agg_func is None:
                    agg_exprs.append(
                        f'# WARNING: Unknown action "{action}" for field "{mapped_field}"'
                    )
                    continue

                if action in ("Concat", "Concatenate"):
                    agg_expr = (
                        f'F.concat_ws(", ", F.collect_list(F.col("{mapped_field}")))'
                        f'.alias("{mapped_output}")'
                    )
                elif action == "Median":
                    agg_expr = (
                        f'F.percentile_approx(F.col("{mapped_field}"), 0.5)'
                        f'.alias("{mapped_output}")'
                    )
                elif action == "CountDistinct":
                    agg_expr = (
                        f'F.countDistinct(F.col("{mapped_field}"))'
                        f'.alias("{mapped_output}")'
                    )
                else:
                    agg_expr = (
                        f'{agg_func}(F.col("{mapped_field}"))'
                        f'.alias("{mapped_output}")'
                    )
                agg_exprs.append(agg_expr)

        # Build the code
        lines = [self.get_comment()]

        if group_by_cols:
            group_str = ", ".join(f'"{c}"' for c in group_by_cols)
            lines.append(f"{output_df_name} = {input_df_name}.groupBy({group_str})")
        else:
            lines.append(f"{output_df_name} = {input_df_name}.groupBy()")

        if agg_exprs:
            # Filter out warning comments
            actual_exprs = [e for e in agg_exprs if not e.startswith("#")]
            warnings = [e for e in agg_exprs if e.startswith("#")]

            for w in warnings:
                lines.append(w)

            if actual_exprs:
                agg_str = ",\n    ".join(actual_exprs)
                lines[-1 if not warnings else -1 - len(warnings)] += f".agg(\n    {agg_str}\n)"
            else:
                lines.append(f"{output_df_name} = {input_df_name}")
        else:
            # No aggregations, just groupBy (unusual but possible)
            lines.append(f"# WARNING: No aggregation expressions")
            lines.append(f"{output_df_name} = {input_df_name}")

        return "\n".join(lines) + "\n"
