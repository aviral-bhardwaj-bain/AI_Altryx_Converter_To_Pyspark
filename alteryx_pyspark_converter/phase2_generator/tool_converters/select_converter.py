"""Convert Alteryx Select tool to PySpark."""

from .base_converter import BaseToolConverter


class SelectConverter(BaseToolConverter):
    """Convert Alteryx Select tool to PySpark select/drop/rename operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark code for Select tool.

        Handles column selection, deselection, renaming, and reordering.
        """
        fields = self.configuration.get("fields", [])
        unknown_fields = self.configuration.get("unknown_fields", "Select")

        if not fields:
            return (
                f"{self.get_comment()}\n"
                f"{output_df_name} = {input_df_name}\n"
            )

        selected_cols = []
        deselected_cols = []
        renames: list[tuple[str, str]] = []

        for field in fields:
            name = field.get("name", "")
            if name == "*Unknown":
                continue
            if not name:
                continue

            is_selected = field.get("selected", True)
            rename = field.get("rename")
            mapped_name = self.map_column(name)

            if is_selected:
                if rename:
                    mapped_rename = self.map_column(rename)
                    renames.append((mapped_name, mapped_rename))
                    selected_cols.append(mapped_rename)
                else:
                    selected_cols.append(mapped_name)
            else:
                deselected_cols.append(mapped_name)

        lines = [self.get_comment()]
        current_df = input_df_name

        # Apply renames first
        for old_name, new_name in renames:
            lines.append(
                f'{current_df} = {current_df}'
                f'.withColumnRenamed("{old_name}", "{new_name}")'
            )

        # Apply selection or deselection
        if selected_cols and unknown_fields != "Select":
            # Explicit select
            cols_str = ", ".join(f'"{c}"' for c in selected_cols)
            lines.append(f"{output_df_name} = {current_df}.select({cols_str})")
        elif deselected_cols:
            # Drop deselected columns
            cols_str = ", ".join(f'"{c}"' for c in deselected_cols)
            lines.append(f"{output_df_name} = {current_df}.drop({cols_str})")
        else:
            lines.append(f"{output_df_name} = {current_df}")

        return "\n".join(lines) + "\n"
