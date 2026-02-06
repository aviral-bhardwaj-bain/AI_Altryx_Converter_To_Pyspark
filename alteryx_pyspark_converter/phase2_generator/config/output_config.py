"""Output configuration for notebook generation."""

import logging
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


class OutputConfig:
    """Configuration for the generated notebook output."""

    def __init__(
        self,
        table_name: str = "",
        columns: list[str] | None = None,
        expected_row_count: int | None = None,
        notebook_path: str = "",
        add_validation_cell: bool = True,
        add_schema_print: bool = True,
    ):
        self.table_name = table_name
        self.columns = columns or []
        self.expected_row_count = expected_row_count
        self.notebook_path = notebook_path
        self.add_validation_cell = add_validation_cell
        self.add_schema_print = add_schema_print

    @classmethod
    def from_config(cls, config: dict) -> "OutputConfig":
        """Create OutputConfig from a config dictionary."""
        output_cfg = config.get("output", {})
        notebook_cfg = config.get("notebook", {})

        return cls(
            table_name=output_cfg.get("table_name", ""),
            columns=output_cfg.get("columns", []),
            expected_row_count=output_cfg.get("expected_row_count"),
            notebook_path=notebook_cfg.get("output_path", ""),
            add_validation_cell=notebook_cfg.get("add_validation_cell", True),
            add_schema_print=notebook_cfg.get("add_schema_print", True),
        )

    def __repr__(self) -> str:
        return (
            f"OutputConfig(table={self.table_name}, "
            f"columns={len(self.columns)})"
        )


class GeneratorConfig:
    """
    Complete configuration for Phase 2 generation.

    Loaded from the user-provided YAML configuration file.
    """

    def __init__(self, config_path: str | None = None, config_dict: dict | None = None):
        if config_path:
            with open(config_path, "r", encoding="utf-8") as f:
                self._raw = yaml.safe_load(f)
        elif config_dict:
            self._raw = config_dict
        else:
            self._raw = {}

        self.container_name: str = self._raw.get("container_name", "")
        self.input_tables: dict = self._raw.get("input_tables", {})
        self.column_mappings: dict[str, str] = self._raw.get("column_mappings", {})
        self.output = OutputConfig.from_config(self._raw)

    def get_input_table_names(self) -> dict[str, str]:
        """Get mapping of config key -> Databricks table name."""
        return {
            key: cfg.get("databricks_table", key)
            for key, cfg in self.input_tables.items()
        }

    def get_tool_id_mapping(self) -> dict[int, str]:
        """Get mapping of tool_id -> config key for input tables."""
        result = {}
        for key, cfg in self.input_tables.items():
            tool_id = cfg.get("maps_to_tool_id")
            if tool_id is not None:
                result[int(tool_id)] = key
        return result

    def get_pre_filters(self) -> dict[str, str]:
        """Get pre-filters for input tables."""
        return {
            key: cfg.get("pre_filter", "")
            for key, cfg in self.input_tables.items()
            if cfg.get("pre_filter")
        }

    def to_dict(self) -> dict:
        """Convert back to dictionary."""
        return dict(self._raw)

    def __repr__(self) -> str:
        return (
            f"GeneratorConfig(container={self.container_name}, "
            f"inputs={len(self.input_tables)}, "
            f"mappings={len(self.column_mappings)})"
        )
