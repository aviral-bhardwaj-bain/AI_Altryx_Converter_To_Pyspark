"""Connection model representing data flow between Alteryx tools."""

from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class Connection:
    """Represents a connection (data flow) between two Alteryx tools."""

    origin_tool_id: int
    origin_connection: str  # "Output", "True", "False", "Join", "Left", "Right"
    destination_tool_id: int
    destination_connection: str  # "Input", "Left", "Right"
    wireless: bool = False
    name: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "origin_tool_id": self.origin_tool_id,
            "origin_connection": self.origin_connection,
            "destination_tool_id": self.destination_tool_id,
            "destination_connection": self.destination_connection,
            "wireless": self.wireless,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Connection":
        """Create Connection from dictionary."""
        return cls(
            origin_tool_id=int(data["origin_tool_id"]),
            origin_connection=data["origin_connection"],
            destination_tool_id=int(data["destination_tool_id"]),
            destination_connection=data["destination_connection"],
            wireless=data.get("wireless", False),
            name=data.get("name"),
        )

    def __repr__(self) -> str:
        return (
            f"Connection({self.origin_tool_id}:{self.origin_connection} "
            f"-> {self.destination_tool_id}:{self.destination_connection})"
        )
