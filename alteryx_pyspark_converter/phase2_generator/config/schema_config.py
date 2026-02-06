"""Load and manage table schemas for code generation."""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TableSchema:
    """Represents the schema of a single Databricks table."""

    def __init__(self, table_name: str, fields: list[dict]):
        self.table_name = table_name
        self.fields = fields
        self._field_map = {f["name"]: f for f in fields}

    def has_column(self, name: str) -> bool:
        """Check if a column exists in the schema."""
        return name in self._field_map

    def get_column_type(self, name: str) -> Optional[str]:
        """Get the data type of a column."""
        field = self._field_map.get(name)
        return field["type"] if field else None

    def get_column_names(self) -> list[str]:
        """Get all column names."""
        return list(self._field_map.keys())

    def __repr__(self) -> str:
        return f"TableSchema({self.table_name}: {len(self.fields)} fields)"


class SchemaConfig:
    """
    Load and manage table schemas for the generator.

    Schemas can be provided:
    1. Directly in the config YAML
    2. By reading from Databricks tables (requires SparkSession)
    3. By loading from a schema definition file
    """

    def __init__(self):
        self.schemas: dict[str, TableSchema] = {}

    def load_from_config(self, input_tables: dict) -> None:
        """
        Load schema information from the user configuration.

        Args:
            input_tables: Dictionary from config YAML with table definitions.
        """
        for table_key, table_config in input_tables.items():
            table_name = table_config.get("databricks_table", table_key)
            fields = table_config.get("schema", [])

            if fields:
                self.schemas[table_key] = TableSchema(table_name, fields)
                logger.info(
                    "Loaded schema for %s (%s): %d fields",
                    table_key, table_name, len(fields),
                )
            else:
                # Create empty schema placeholder
                self.schemas[table_key] = TableSchema(table_name, [])
                logger.warning(
                    "No schema provided for %s (%s). "
                    "Column validation will be skipped.",
                    table_key, table_name,
                )

    def load_from_spark(
        self, input_tables: dict, spark: Any
    ) -> None:
        """
        Load schemas by reading from actual Databricks tables.

        Args:
            input_tables: Dictionary from config YAML with table definitions.
            spark: SparkSession instance.
        """
        for table_key, table_config in input_tables.items():
            table_name = table_config.get("databricks_table", table_key)
            try:
                df = spark.table(table_name)
                fields = [
                    {
                        "name": f.name,
                        "type": str(f.dataType),
                        "nullable": f.nullable,
                    }
                    for f in df.schema.fields
                ]
                self.schemas[table_key] = TableSchema(table_name, fields)
                logger.info(
                    "Loaded schema from Spark for %s: %d fields",
                    table_name, len(fields),
                )
            except Exception as e:
                logger.error("Failed to load schema for %s: %s", table_name, e)
                self.schemas[table_key] = TableSchema(table_name, [])

    def get_schema(self, table_key: str) -> Optional[TableSchema]:
        """Get schema for a table by its config key."""
        return self.schemas.get(table_key)

    def get_all_columns(self) -> dict[str, list[str]]:
        """Get all column names grouped by table."""
        return {
            key: schema.get_column_names()
            for key, schema in self.schemas.items()
        }

    def validate_column(self, table_key: str, column_name: str) -> bool:
        """Check if a column exists in a table's schema."""
        schema = self.schemas.get(table_key)
        if schema is None:
            return True  # No schema to validate against
        if not schema.fields:
            return True  # Empty schema = skip validation
        return schema.has_column(column_name)
