"""Tool model representing an Alteryx workflow tool/node."""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Tool:
    """Represents a single Alteryx tool/node in the workflow."""

    tool_id: int
    tool_type: str  # Mapped tool type (e.g., "Filter", "Join")
    plugin_type: str  # Original Alteryx plugin name
    position: dict = field(default_factory=lambda: {"x": 0, "y": 0})
    annotation: str = ""
    container_id: Optional[int] = None
    configuration: dict = field(default_factory=dict)
    raw_xml: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "tool_id": self.tool_id,
            "tool_type": self.tool_type,
            "plugin_type": self.plugin_type,
            "position": self.position,
            "annotation": self.annotation,
            "container_id": self.container_id,
            "configuration": self.configuration,
            "raw_xml": self.raw_xml,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Tool":
        """Create Tool from dictionary."""
        return cls(
            tool_id=int(data["tool_id"]),
            tool_type=data["tool_type"],
            plugin_type=data.get("plugin_type", ""),
            position=data.get("position", {"x": 0, "y": 0}),
            annotation=data.get("annotation", ""),
            container_id=data.get("container_id"),
            configuration=data.get("configuration", {}),
            raw_xml=data.get("raw_xml", ""),
        )

    def is_input_tool(self) -> bool:
        """Check if this tool is a data input tool."""
        return self.tool_type in (
            "TextInput", "DbInput", "LockInInput", "MacroInput"
        )

    def is_output_tool(self) -> bool:
        """Check if this tool is a data output tool."""
        return self.tool_type in ("DbOutput", "Browse", "LockInWrite", "MacroOutput")

    def is_container(self) -> bool:
        """Check if this tool is a container."""
        return self.tool_type == "Container"

    def is_lockin_tool(self) -> bool:
        """Check if this is a LockIn (In-Database) tool."""
        return self.tool_type.startswith("LockIn")

    def __repr__(self) -> str:
        ann = f" ({self.annotation})" if self.annotation else ""
        return f"Tool({self.tool_id}: {self.tool_type}{ann})"
