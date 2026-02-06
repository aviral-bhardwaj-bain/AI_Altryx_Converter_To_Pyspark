"""Main parser orchestrator for Alteryx .yxmd workflows."""

import os
from datetime import datetime, timezone
import logging

from ..models.workflow import Workflow, WorkflowMetadata, ExternalInput
from ..models.container import Container
from ..models.tool import Tool
from ..models.connection import Connection
from .xml_extractor import XMLExtractor
from .container_extractor import ContainerExtractor
from .tool_extractor import ToolExtractor
from .connection_extractor import ConnectionExtractor
from .formula_extractor import FormulaExtractor

logger = logging.getLogger(__name__)


class WorkflowParser:
    """
    Generic parser for ANY Alteryx .yxmd workflow.

    Extracts workflow structure, tools, connections, and formulas
    WITHOUT any knowledge of target schemas.
    """

    def __init__(self):
        self.xml_extractor: XMLExtractor | None = None
        self.container_extractor = ContainerExtractor()
        self.tool_extractor = ToolExtractor()
        self.connection_extractor = ConnectionExtractor()
        self.formula_extractor = FormulaExtractor()

    def parse(self, yxmd_path: str) -> Workflow:
        """
        Parse an Alteryx workflow and return the intermediate representation.

        Args:
            yxmd_path: Path to the .yxmd file.

        Returns:
            Workflow object containing all parsed data.
        """
        logger.info("Parsing workflow: %s", yxmd_path)

        # Parse XML
        self.xml_extractor = XMLExtractor(yxmd_path)
        root = self.xml_extractor.parse()

        # Extract metadata
        metadata = self._extract_metadata()

        # Get all nodes
        nodes = self.xml_extractor.get_nodes()
        logger.info("Found %d nodes in workflow", len(nodes))

        # Extract containers
        containers = self.container_extractor.extract(nodes)
        logger.info("Extracted %d containers", len(containers))

        # Extract tools (non-container nodes)
        tools = self.tool_extractor.extract(nodes)
        logger.info("Extracted %d tools", len(tools))

        # Extract connections
        connections = self.connection_extractor.extract(root)
        logger.info("Extracted %d connections", len(connections))

        # Identify external inputs for each container
        external_inputs = self._identify_external_inputs(
            containers, tools, connections
        )

        # Build workflow object
        workflow = Workflow(
            name=self._extract_workflow_name(yxmd_path),
            file_path=os.path.abspath(yxmd_path),
            parsed_at=datetime.now(timezone.utc).isoformat(),
            metadata=metadata,
            containers=containers,
            tools=tools,
            connections=connections,
            external_inputs=external_inputs,
        )

        # Build indexes for fast lookups
        workflow.build_indexes()

        logger.info(
            "Parsed workflow: %s (%d containers, %d tools, %d connections)",
            workflow.name,
            len(containers),
            len(tools),
            len(connections),
        )

        return workflow

    def _extract_metadata(self) -> WorkflowMetadata:
        """Extract workflow metadata from XML."""
        if self.xml_extractor is None:
            return WorkflowMetadata()

        return WorkflowMetadata(
            alteryx_version=self.xml_extractor.get_workflow_version(),
            yxmd_version=self.xml_extractor.get_yxmd_version(),
        )

    def _extract_workflow_name(self, yxmd_path: str) -> str:
        """Extract workflow name from file path."""
        basename = os.path.basename(yxmd_path)
        name, _ = os.path.splitext(basename)
        return name

    def _identify_external_inputs(
        self,
        containers: list[Container],
        tools: list[Tool],
        connections: list[Connection],
    ) -> list[ExternalInput]:
        """
        Identify external data inputs for each container.

        An external input is a connection that crosses from outside a container
        to inside a container.
        """
        external_inputs: list[ExternalInput] = []

        # Build set of tool IDs per container (including nested)
        container_tool_sets: dict[int, set[int]] = {}
        container_map = {c.tool_id: c for c in containers}

        for container in containers:
            tool_ids = set(container.child_tool_ids)
            # Include tools from sub-containers
            for sub_id in container.child_container_ids:
                sub = container_map.get(sub_id)
                if sub:
                    tool_ids.update(sub.child_tool_ids)
            container_tool_sets[container.tool_id] = tool_ids

        # Find connections crossing container boundaries
        for container in containers:
            tool_ids = container_tool_sets.get(container.tool_id, set())

            for conn in connections:
                if (
                    conn.destination_tool_id in tool_ids
                    and conn.origin_tool_id not in tool_ids
                ):
                    # Build source description
                    source_desc = self._build_source_description(
                        conn.origin_tool_id, tools
                    )

                    external_inputs.append(
                        ExternalInput(
                            tool_id=conn.destination_tool_id,
                            source_tool_id=conn.origin_tool_id,
                            source_connection=conn.origin_connection,
                            enters_container=container.tool_id,
                            source_description=source_desc,
                        )
                    )

        return external_inputs

    def _build_source_description(
        self, tool_id: int, tools: list[Tool]
    ) -> str:
        """Build a human-readable description of a source tool."""
        for tool in tools:
            if tool.tool_id == tool_id:
                desc = tool.tool_type
                if tool.annotation:
                    desc += f": {tool.annotation}"
                return desc
        return f"Tool {tool_id}"

    def get_container_summary(self, workflow: Workflow) -> list[dict]:
        """
        Get a summary of all containers in the workflow.

        Returns:
            List of container summaries with name, tool count, etc.
        """
        summaries = []

        for container in workflow.containers:
            tools = workflow.get_container_tools(container.tool_id)
            internal_conns = workflow.get_container_connections(container.tool_id)
            ext_inputs = workflow.get_container_external_inputs(container.tool_id)
            ext_outputs = workflow.get_container_external_outputs(container.tool_id)

            # Gather tool type counts
            tool_types: dict[str, int] = {}
            for tool in tools:
                tool_types[tool.tool_type] = tool_types.get(tool.tool_type, 0) + 1

            summaries.append({
                "tool_id": container.tool_id,
                "name": container.name,
                "disabled": container.disabled,
                "parent_container": container.parent_container_id,
                "num_tools": len(container.child_tool_ids),
                "num_sub_containers": len(container.child_container_ids),
                "num_internal_connections": len(internal_conns),
                "num_external_inputs": len(ext_inputs),
                "num_external_outputs": len(ext_outputs),
                "tool_types": tool_types,
            })

        return summaries

    def get_container_dependencies(
        self, workflow: Workflow, container_id: int
    ) -> dict:
        """
        Get the dependencies (external inputs) for a container.

        Returns:
            Dictionary describing what data the container needs.
        """
        container = workflow.get_container(container_id)
        if not container:
            return {"error": f"Container {container_id} not found"}

        ext_inputs = workflow.get_container_external_inputs(container_id)

        dependencies = []
        for conn in ext_inputs:
            source_tool = workflow.get_tool(conn.origin_tool_id)
            dest_tool = workflow.get_tool(conn.destination_tool_id)

            dep = {
                "source_tool_id": conn.origin_tool_id,
                "source_type": source_tool.tool_type if source_tool else "Unknown",
                "source_annotation": source_tool.annotation if source_tool else "",
                "source_connection": conn.origin_connection,
                "destination_tool_id": conn.destination_tool_id,
                "destination_type": dest_tool.tool_type if dest_tool else "Unknown",
                "destination_connection": conn.destination_connection,
            }
            dependencies.append(dep)

        # Get all columns referenced in the container
        all_columns: set[str] = set()
        for tool in workflow.get_container_tools(container_id):
            cols = self.formula_extractor.extract_all_columns_from_tool(
                tool.configuration
            )
            all_columns.update(cols)

        return {
            "container_id": container_id,
            "container_name": container.name,
            "dependencies": dependencies,
            "columns_referenced": sorted(all_columns),
        }
