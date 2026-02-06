"""Convert Alteryx TextInput tool to PySpark."""

from .base_converter import BaseToolConverter


class TextInputConverter(BaseToolConverter):
    """Convert Alteryx TextInput (hardcoded data) to spark.createDataFrame()."""

    # Map Alteryx types to PySpark types
    TYPE_MAP = {
        "Bool": "BooleanType()",
        "Byte": "ByteType()",
        "Int16": "ShortType()",
        "Int32": "IntegerType()",
        "Int64": "LongType()",
        "FixedDecimal": "DecimalType()",
        "Float": "FloatType()",
        "Double": "DoubleType()",
        "String": "StringType()",
        "WString": "StringType()",
        "V_String": "StringType()",
        "V_WString": "StringType()",
        "Date": "DateType()",
        "Time": "StringType()",
        "DateTime": "TimestampType()",
    }

    def generate_code(
        self,
        input_df_name: str,
        output_df_name: str,
        **kwargs,
    ) -> str:
        """
        Generate PySpark code to create a DataFrame from hardcoded data.
        """
        fields = self.configuration.get("fields", [])
        data = self.configuration.get("data", [])

        if not fields or not data:
            return (
                f"{self.get_comment()}\n"
                f"# WARNING: Empty TextInput\n"
                f'{output_df_name} = spark.createDataFrame([], schema="id: string")\n'
            )

        # Build schema
        schema_fields = []
        for field in fields:
            name = self.map_column(field.get("name", ""))
            dtype = field.get("type", "V_WString")
            spark_type = self.TYPE_MAP.get(dtype, "StringType()")
            schema_fields.append(f'    StructField("{name}", {spark_type}, True)')

        schema_str = "StructType([\n" + ",\n".join(schema_fields) + "\n])"

        # Build data rows
        field_names = [f.get("name", "") for f in fields]
        rows = []
        for row in data:
            if isinstance(row, dict):
                values = [self._format_value(row.get(fn)) for fn in field_names]
            elif isinstance(row, (list, tuple)):
                values = [self._format_value(v) for v in row]
            else:
                continue
            rows.append(f"    ({', '.join(values)})")

        rows_str = ",\n".join(rows)

        return (
            f"{self.get_comment()}\n"
            f"_schema_{self.tool_id} = {schema_str}\n"
            f"_data_{self.tool_id} = [\n{rows_str}\n]\n"
            f"{output_df_name} = spark.createDataFrame("
            f"_data_{self.tool_id}, schema=_schema_{self.tool_id})\n"
        )

    def _format_value(self, value) -> str:
        """Format a value for Python code."""
        if value is None:
            return "None"
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, (int, float)):
            return str(value)
        # String value
        escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
