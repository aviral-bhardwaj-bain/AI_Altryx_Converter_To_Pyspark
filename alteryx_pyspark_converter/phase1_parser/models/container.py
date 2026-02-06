"""Container model representing an Alteryx ToolContainer."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Container:
    """Represents an Alteryx ToolContainer (grouping of tools)."""

    tool_id: int
    name: str
    position: dict = field(default_factory=lambda: {
        "x": 0, "y": 0, "width": 0, "height": 0
    })
    disabled: bool = False
    folded: bool = False
    parent_container_id: Optional[int] = None
    child_tool_ids: list[int] = field(default_factory=list)
    child_container_ids: list[int] = field(default_factory=list)
    style: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "tool_id": self.tool_id,
            "name": self.name,
            "position": self.position,
            "disabled": self.disabled,
            "folded": self.folded,
            "parent_container_id": self.parent_container_id,
            "child_tool_ids": self.child_tool_ids,
            "child_container_ids": self.child_container_ids,
            "style": self.style,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Container":
        """Create Container from dictionary."""
        return cls(
            tool_id=int(data["tool_id"]),
            name=data["name"],
            position=data.get("position", {"x": 0, "y": 0, "width": 0, "height": 0}),
            disabled=data.get("disabled", False),
            folded=data.get("folded", False),
            parent_container_id=data.get("parent_container_id"),
            child_tool_ids=data.get("child_tool_ids", []),
            child_container_ids=data.get("child_container_ids", []),
            style=data.get("style", {}),
        )

    def get_all_tool_ids(self) -> list[int]:
        """Get all tool IDs including the container itself."""
        return [self.tool_id] + self.child_tool_ids

    def __repr__(self) -> str:
        return (
            f"Container({self.tool_id}: {self.name}, "
            f"tools={len(self.child_tool_ids)}, "
            f"sub_containers={len(self.child_container_ids)})"
        )
