"""Extract connections (data flow) between Alteryx workflow tools."""

import xml.etree.ElementTree as ET
from typing import Optional
import logging

from ..models.connection import Connection

logger = logging.getLogger(__name__)


class ConnectionExtractor:
    """Extract all connections between tools in an Alteryx workflow."""

    def extract(self, xml_root: ET.Element) -> list[Connection]:
        """
        Extract all connections from the workflow XML.

        Args:
            xml_root: Root element of the parsed workflow XML.

        Returns:
            List of Connection objects.
        """
        connections: list[Connection] = []

        # Find all Connection elements
        conn_elements = xml_root.findall(".//Connection")

        for conn_elem in conn_elements:
            connection = self._parse_connection(conn_elem)
            if connection:
                connections.append(connection)

        logger.info("Extracted %d connections", len(connections))
        return connections

    def _parse_connection(self, conn_elem: ET.Element) -> Optional[Connection]:
        """Parse a single connection element."""
        origin = conn_elem.find("Origin")
        dest = conn_elem.find("Destination")

        if origin is None or dest is None:
            return None

        origin_tool_id = origin.get("ToolID")
        dest_tool_id = dest.get("ToolID")

        if not origin_tool_id or not dest_tool_id:
            return None

        try:
            return Connection(
                origin_tool_id=int(origin_tool_id),
                origin_connection=origin.get("Connection", "Output"),
                destination_tool_id=int(dest_tool_id),
                destination_connection=dest.get("Connection", "Input"),
                wireless=conn_elem.get("Wireless", "False").lower() in ("true", "1"),
                name=conn_elem.get("name"),
            )
        except (ValueError, TypeError) as e:
            logger.warning("Error parsing connection: %s", e)
            return None

    def build_data_flow_graph(
        self, connections: list[Connection]
    ) -> dict[int, dict[str, list[tuple[int, str]]]]:
        """
        Build adjacency list for data flow analysis.

        Returns:
            Dictionary mapping tool_id to its inputs and outputs:
            {
                tool_id: {
                    "inputs": [(from_tool_id, connection_type), ...],
                    "outputs": [(to_tool_id, connection_type), ...]
                }
            }
        """
        graph: dict[int, dict[str, list[tuple[int, str]]]] = {}

        for conn in connections:
            # Initialize entries
            if conn.origin_tool_id not in graph:
                graph[conn.origin_tool_id] = {"inputs": [], "outputs": []}
            if conn.destination_tool_id not in graph:
                graph[conn.destination_tool_id] = {"inputs": [], "outputs": []}

            # Add connections
            graph[conn.origin_tool_id]["outputs"].append(
                (conn.destination_tool_id, conn.origin_connection)
            )
            graph[conn.destination_tool_id]["inputs"].append(
                (conn.origin_tool_id, conn.origin_connection)
            )

        return graph

    def find_source_tools(
        self, connections: list[Connection]
    ) -> list[int]:
        """Find tools that have no incoming connections (source tools)."""
        all_dest = {c.destination_tool_id for c in connections}
        all_origin = {c.origin_tool_id for c in connections}
        return sorted(all_origin - all_dest)

    def find_sink_tools(
        self, connections: list[Connection]
    ) -> list[int]:
        """Find tools that have no outgoing connections (sink tools)."""
        all_dest = {c.destination_tool_id for c in connections}
        all_origin = {c.origin_tool_id for c in connections}
        return sorted(all_dest - all_origin)

    def get_upstream_tools(
        self,
        tool_id: int,
        connections: list[Connection],
    ) -> set[int]:
        """Get all tools upstream of the given tool (transitive)."""
        graph = self.build_data_flow_graph(connections)
        visited: set[int] = set()

        def _traverse(tid: int) -> None:
            if tid in visited:
                return
            visited.add(tid)
            if tid in graph:
                for from_id, _ in graph[tid]["inputs"]:
                    _traverse(from_id)

        _traverse(tool_id)
        visited.discard(tool_id)
        return visited

    def get_downstream_tools(
        self,
        tool_id: int,
        connections: list[Connection],
    ) -> set[int]:
        """Get all tools downstream of the given tool (transitive)."""
        graph = self.build_data_flow_graph(connections)
        visited: set[int] = set()

        def _traverse(tid: int) -> None:
            if tid in visited:
                return
            visited.add(tid)
            if tid in graph:
                for to_id, _ in graph[tid]["outputs"]:
                    _traverse(to_id)

        _traverse(tool_id)
        visited.discard(tool_id)
        return visited
