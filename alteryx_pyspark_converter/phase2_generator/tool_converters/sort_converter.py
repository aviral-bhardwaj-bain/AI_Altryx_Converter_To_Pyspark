"""Convert Alteryx Sort tool to PySpark."""

from .base_converter import BaseToolConverter


class SortConverter(BaseToolConverter):
    """Convert Alteryx Sort tool to PySpark orderBy operations."""

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """Generate PySpark sort/orderBy code."""
        fields = self.configuration.get("fields", [])

        if not fields:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: No sort fields defined\n"
                f"{output_df_name} = {input_df_name}\n"
            )

        sort_exprs = []
        for field in fields:
            field_name = field.get("field", "")
            order = field.get("order", "Ascending")
            mapped = self.map_column(field_name)

            if order.lower() in ("descending", "desc"):
                sort_exprs.append(f'F.col("{mapped}").desc()')
            else:
                sort_exprs.append(f'F.col("{mapped}").asc()')

        sort_str = ", ".join(sort_exprs)

        return (
            f"{self.get_comment()}\n"
            f"{output_df_name} = {input_df_name}.orderBy({sort_str})\n"
        )
