"""Workflow model representing a complete parsed Alteryx workflow."""

from dataclasses import dataclass, field
from typing import Optional

from .container import Container
from .tool import Tool
from .connection import Connection


@dataclass
class WorkflowMetadata:
    """Metadata about the Alteryx workflow."""
    alteryx_version: str = ""
    yxmd_version: str = ""
    gallery_version: Optional[str] = None
    layout_type: str = ""


@dataclass
class ExternalInput:
    """Represents an external data flow entering a container."""
    tool_id: int
    source_tool_id: int
    source_connection: str
    enters_container: int
    source_description: str = ""

    def to_dict(self) -> dict:
        return {
            "tool_id": self.tool_id,
            "source_tool_id": self.source_tool_id,
            "source_connection": self.source_connection,
            "enters_container": self.enters_container,
            "source_description": self.source_description,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ExternalInput":
        return cls(
            tool_id=int(data["tool_id"]),
            source_tool_id=int(data.get("source_tool_id", 0)),
            source_connection=data.get("source_connection", "Output"),
            enters_container=int(data["enters_container"]),
            source_description=data.get("source_description", ""),
        )


@dataclass
class Workflow:
    """Complete parsed Alteryx workflow representation."""

    name: str = ""
    file_path: str = ""
    parsed_at: str = ""
    metadata: WorkflowMetadata = field(default_factory=WorkflowMetadata)
    containers: list[Container] = field(default_factory=list)
    tools: list[Tool] = field(default_factory=list)
    connections: list[Connection] = field(default_factory=list)
    external_inputs: list[ExternalInput] = field(default_factory=list)

    # Indexed lookups (built after parsing)
    _tools_by_id: dict[int, Tool] = field(default_factory=dict, repr=False)
    _containers_by_id: dict[int, Container] = field(default_factory=dict, repr=False)
    _containers_by_name: dict[str, Container] = field(default_factory=dict, repr=False)

    def build_indexes(self) -> None:
        """Build lookup indexes after parsing."""
        self._tools_by_id = {t.tool_id: t for t in self.tools}
        self._containers_by_id = {c.tool_id: c for c in self.containers}
        self._containers_by_name = {c.name: c for c in self.containers}

    def get_tool(self, tool_id: int) -> Optional[Tool]:
        """Get tool by ID."""
        return self._tools_by_id.get(tool_id)

    def get_container(self, tool_id: int) -> Optional[Container]:
        """Get container by tool ID."""
        return self._containers_by_id.get(tool_id)

    def get_container_by_name(self, name: str) -> Optional[Container]:
        """Get container by name."""
        return self._containers_by_name.get(name)

    def get_incoming_connections(self, tool_id: int) -> list[Connection]:
        """Get all connections flowing INTO a tool."""
        return [c for c in self.connections if c.destination_tool_id == tool_id]

    def get_outgoing_connections(self, tool_id: int) -> list[Connection]:
        """Get all connections flowing OUT of a tool."""
        return [c for c in self.connections if c.origin_tool_id == tool_id]

    def get_container_tools(self, container_id: int) -> list[Tool]:
        """Get all tools belonging to a container."""
        container = self.get_container(container_id)
        if not container:
            return []
        return [
            t for t in self.tools
            if t.tool_id in container.child_tool_ids
        ]

    def get_container_connections(self, container_id: int) -> list[Connection]:
        """Get all connections within a container (both endpoints inside)."""
        container = self.get_container(container_id)
        if not container:
            return []

        # Gather all tool IDs in this container and its sub-containers
        all_tool_ids = set(container.child_tool_ids)
        for sub_id in container.child_container_ids:
            sub = self.get_container(sub_id)
            if sub:
                all_tool_ids.update(sub.child_tool_ids)
                all_tool_ids.add(sub.tool_id)

        return [
            c for c in self.connections
            if c.origin_tool_id in all_tool_ids
            and c.destination_tool_id in all_tool_ids
        ]

    def get_container_external_inputs(self, container_id: int) -> list[Connection]:
        """Get connections entering a container from outside."""
        container = self.get_container(container_id)
        if not container:
            return []

        all_tool_ids = set(container.child_tool_ids)
        for sub_id in container.child_container_ids:
            sub = self.get_container(sub_id)
            if sub:
                all_tool_ids.update(sub.child_tool_ids)

        return [
            c for c in self.connections
            if c.destination_tool_id in all_tool_ids
            and c.origin_tool_id not in all_tool_ids
        ]

    def get_container_external_outputs(self, container_id: int) -> list[Connection]:
        """Get connections leaving a container to outside."""
        container = self.get_container(container_id)
        if not container:
            return []

        all_tool_ids = set(container.child_tool_ids)
        for sub_id in container.child_container_ids:
            sub = self.get_container(sub_id)
            if sub:
                all_tool_ids.update(sub.child_tool_ids)

        return [
            c for c in self.connections
            if c.origin_tool_id in all_tool_ids
            and c.destination_tool_id not in all_tool_ids
        ]

    def get_top_level_containers(self) -> list[Container]:
        """Get containers that are not nested inside other containers."""
        return [c for c in self.containers if c.parent_container_id is None]

    def get_global_tools(self) -> list[Tool]:
        """Get tools that are not inside any container."""
        return [t for t in self.tools if t.container_id is None]

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "workflow": {
                "name": self.name,
                "file_path": self.file_path,
                "parsed_at": self.parsed_at,
                "alteryx_version": self.metadata.alteryx_version,
                "yxmd_version": self.metadata.yxmd_version,
            },
            "containers": [c.to_dict() for c in self.containers],
            "tools": [t.to_dict() for t in self.tools],
            "connections": [c.to_dict() for c in self.connections],
            "external_inputs": [e.to_dict() for e in self.external_inputs],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Workflow":
        """Create Workflow from dictionary (loaded from intermediate JSON)."""
        wf_data = data.get("workflow", {})
        wf = cls(
            name=wf_data.get("name", ""),
            file_path=wf_data.get("file_path", ""),
            parsed_at=wf_data.get("parsed_at", ""),
            metadata=WorkflowMetadata(
                alteryx_version=wf_data.get("alteryx_version", ""),
                yxmd_version=wf_data.get("yxmd_version", ""),
            ),
            containers=[
                Container.from_dict(c)
                for c in data.get("containers", [])
            ],
            tools=[
                Tool.from_dict(t)
                for t in data.get("tools", [])
            ],
            connections=[
                Connection.from_dict(c)
                for c in data.get("connections", [])
            ],
            external_inputs=[
                ExternalInput.from_dict(e)
                for e in data.get("external_inputs", [])
            ],
        )
        wf.build_indexes()
        return wf

    def __repr__(self) -> str:
        return (
            f"Workflow({self.name}: "
            f"{len(self.containers)} containers, "
            f"{len(self.tools)} tools, "
            f"{len(self.connections)} connections)"
        )
