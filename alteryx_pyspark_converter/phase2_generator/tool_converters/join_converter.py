"""Convert Alteryx Join tool to PySpark."""

from .base_converter import BaseToolConverter


class JoinConverter(BaseToolConverter):
    """Convert Alteryx Join tool to PySpark join operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark join code.

        Produces three output DataFrames:
        - {output_df_name}_join: Inner join (matched records)
        - {output_df_name}_left: Left unmatched records
        - {output_df_name}_right: Right unmatched records

        Args:
            input_df_name: Left input DataFrame name.
            output_df_name: Prefix for output DataFrame names.
            **kwargs: Must include 'right_df_name'.
        """
        right_df_name = kwargs.get("right_df_name", f"{input_df_name}_right")

        left_keys = self.configuration.get("left_keys", [])
        right_keys = self.configuration.get("right_keys", [])

        # Map column names
        mapped_left_keys = self.map_columns(left_keys)
        mapped_right_keys = self.map_columns(right_keys)

        # Build join condition
        if mapped_left_keys == mapped_right_keys and len(mapped_left_keys) > 0:
            # Same column names - simple join
            if len(mapped_left_keys) == 1:
                join_condition = f'"{mapped_left_keys[0]}"'
            else:
                join_condition = repr(mapped_left_keys)
            join_code = (
                f"{output_df_name}_join = {input_df_name}.join("
                f"{right_df_name}, on={join_condition}, how=\"inner\")\n"
                f"{output_df_name}_left = {input_df_name}.join("
                f"{right_df_name}, on={join_condition}, how=\"left_anti\")\n"
                f"{output_df_name}_right = {right_df_name}.join("
                f"{input_df_name}, on={join_condition}, how=\"left_anti\")\n"
            )
        elif mapped_left_keys and mapped_right_keys:
            # Different column names - build explicit condition
            conditions = []
            for lk, rk in zip(mapped_left_keys, mapped_right_keys):
                conditions.append(
                    f'{input_df_name}["{lk}"] == {right_df_name}["{rk}"]'
                )
            join_expr = " & ".join(conditions)
            join_code = (
                f"_join_cond = {join_expr}\n"
                f"{output_df_name}_join = {input_df_name}.join("
                f"{right_df_name}, on=_join_cond, how=\"inner\")\n"
                f"{output_df_name}_left = {input_df_name}.join("
                f"{right_df_name}, on=_join_cond, how=\"left_anti\")\n"
                f"{output_df_name}_right = {right_df_name}.join("
                f"{input_df_name}, on=_join_cond, how=\"left_anti\")\n"
            )
        else:
            join_code = (
                f"# WARNING: No join keys specified\n"
                f"{output_df_name}_join = {input_df_name}.crossJoin({right_df_name})\n"
                f"{output_df_name}_left = {input_df_name}.limit(0)\n"
                f"{output_df_name}_right = {right_df_name}.limit(0)\n"
            )

        # Handle select configurations for deduplicating columns
        select_code = self._generate_select_code(output_df_name)

        return (
            f"{self.get_comment()}\n"
            f"{join_code}"
            f"{select_code}"
        )

    def _generate_select_code(self, output_df_name: str) -> str:
        """Generate column selection code to handle duplicate columns after join."""
        join_select = self.configuration.get("join_select", [])
        if not join_select:
            return ""

        selected = []
        renames = []
        for field in join_select:
            name = field.get("name", "")
            if name == "*Unknown":
                continue
            if not field.get("selected", True):
                continue
            mapped = self.map_column(name)
            rename = field.get("rename")
            if rename:
                renames.append((mapped, rename))
            else:
                selected.append(mapped)

        if not selected and not renames:
            return ""

        code_parts = []
        if selected:
            cols = ", ".join(f'"{c}"' for c in selected)
            code_parts.append(
                f"{output_df_name}_join = {output_df_name}_join.select({cols})"
            )
        for old_name, new_name in renames:
            code_parts.append(
                f'{output_df_name}_join = {output_df_name}_join'
                f'.withColumnRenamed("{old_name}", "{new_name}")'
            )

        return "\n".join(code_parts) + "\n" if code_parts else ""
