"""Extract containers from Alteryx workflow XML, including nested containers."""

import xml.etree.ElementTree as ET
from typing import Optional
import logging

from ..models.container import Container
from .xml_extractor import XMLExtractor

logger = logging.getLogger(__name__)


class ContainerExtractor:
    """Extract all containers including nested containers from an Alteryx workflow."""

    # The plugin type string for containers
    CONTAINER_PLUGIN = "AlteryxGuiToolkit.ToolContainer.ToolContainer"

    def extract(self, nodes: list[ET.Element]) -> list[Container]:
        """
        Extract all containers from the workflow nodes.

        Args:
            nodes: List of <Node> XML elements from the workflow.

        Returns:
            List of Container objects with parent/child relationships resolved.
        """
        containers: list[Container] = []
        container_map: dict[int, Container] = {}

        # First pass: extract all containers
        for node in nodes:
            plugin = node.get("EngineDll", "") or ""
            gui_settings = node.find("GuiSettings")
            if gui_settings is not None:
                plugin = gui_settings.get("Plugin", plugin)

            if plugin == self.CONTAINER_PLUGIN:
                container = self._parse_container(node)
                if container:
                    containers.append(container)
                    container_map[container.tool_id] = container

        # Second pass: resolve child tools and parent containers
        self._resolve_children(nodes, container_map)

        # Third pass: resolve nested container relationships
        self._resolve_nesting(container_map)

        logger.info("Extracted %d containers", len(containers))
        return containers

    def _parse_container(self, node: ET.Element) -> Optional[Container]:
        """Parse a single container node."""
        tool_id_str = node.get("ToolID")
        if not tool_id_str:
            return None

        tool_id = int(tool_id_str)

        # Extract name from annotation or properties
        name = self._extract_container_name(node)

        # Extract position
        position = self._extract_position(node)

        # Extract disabled/folded state
        disabled = self._is_disabled(node)
        folded = self._is_folded(node)

        # Extract style
        style = self._extract_style(node)

        return Container(
            tool_id=tool_id,
            name=name,
            position=position,
            disabled=disabled,
            folded=folded,
            style=style,
        )

    def _extract_container_name(self, node: ET.Element) -> str:
        """Extract the container name from its annotation or configuration."""
        # Try annotation first
        annotation = node.find(".//Annotation")
        if annotation is not None:
            name_elem = annotation.find("Name")
            if name_elem is not None and name_elem.text:
                return name_elem.text.strip()

        # Try caption in configuration
        config = node.find(".//Configuration")
        if config is not None:
            caption = config.find("Caption")
            if caption is not None and caption.text:
                return caption.text.strip()

        # Try properties
        props = node.find(".//Properties/Configuration")
        if props is not None:
            caption = props.find("Caption")
            if caption is not None and caption.text:
                return caption.text.strip()

        return f"Container_{node.get('ToolID', 'unknown')}"

    def _extract_position(self, node: ET.Element) -> dict:
        """Extract container position and dimensions."""
        gui = node.find("GuiSettings")
        position = {"x": 0, "y": 0, "width": 0, "height": 0}

        if gui is not None:
            pos = gui.find("Position")
            if pos is not None:
                position["x"] = int(float(pos.get("x", "0")))
                position["y"] = int(float(pos.get("y", "0")))
                position["width"] = int(float(pos.get("width", "0")))
                position["height"] = int(float(pos.get("height", "0")))

        return position

    def _is_disabled(self, node: ET.Element) -> bool:
        """Check if container is disabled."""
        config = node.find(".//Configuration")
        if config is not None:
            disabled = config.find("Disabled")
            if disabled is not None and disabled.text:
                return disabled.text.strip().lower() in ("true", "1", "yes")
        return False

    def _is_folded(self, node: ET.Element) -> bool:
        """Check if container is folded/collapsed."""
        config = node.find(".//Configuration")
        if config is not None:
            folded = config.find("Folded")
            if folded is not None and folded.text:
                return folded.text.strip().lower() in ("true", "1", "yes")
        return False

    def _extract_style(self, node: ET.Element) -> dict:
        """Extract container style information."""
        style = {}
        config = node.find(".//Configuration")
        if config is not None:
            for elem in config:
                if elem.tag in ("Fill", "Border", "TextColor", "Opacity"):
                    style[elem.tag] = elem.text or ""
        return style

    def _resolve_children(
        self,
        nodes: list[ET.Element],
        container_map: dict[int, Container],
    ) -> None:
        """Resolve which tools belong to which containers."""
        for node in nodes:
            tool_id_str = node.get("ToolID")
            if not tool_id_str:
                continue
            tool_id = int(tool_id_str)

            # Check if this node has a ChildOf property
            child_of = self._find_parent_container(node)
            if child_of is not None and child_of in container_map:
                container = container_map[child_of]
                if tool_id not in container.child_tool_ids and tool_id != child_of:
                    container.child_tool_ids.append(tool_id)

    def _find_parent_container(self, node: ET.Element) -> Optional[int]:
        """Find which container a node belongs to."""
        # Look in GuiSettings for container membership
        gui = node.find("GuiSettings")
        if gui is not None:
            # Some workflows use a ToolContainer reference
            container_ref = gui.get("ToolContainer")
            if container_ref:
                return int(container_ref)

        # Check in properties
        props = node.find(".//Properties/Configuration")
        if props is not None:
            tc = props.find("ToolContainer")
            if tc is not None and tc.text:
                return int(tc.text)

        # Check annotation for container reference
        annotation = node.find(".//Annotation")
        if annotation is not None:
            # Try as attribute first: <Annotation ChildOf="100">
            child_of = annotation.get("ChildOf")
            if child_of:
                return int(child_of)
            # Try as child element: <Annotation><ChildOf>100</ChildOf></Annotation>
            child_of_elem = annotation.find("ChildOf")
            if child_of_elem is not None and child_of_elem.text:
                return int(child_of_elem.text.strip())

        return None

    def _resolve_nesting(self, container_map: dict[int, Container]) -> None:
        """Resolve parent-child relationships between containers."""
        for container in container_map.values():
            # Check if any of the child tools are actually containers
            child_containers = []
            remaining_tools = []

            for child_id in container.child_tool_ids:
                if child_id in container_map and child_id != container.tool_id:
                    child_containers.append(child_id)
                    container_map[child_id].parent_container_id = container.tool_id
                else:
                    remaining_tools.append(child_id)

            container.child_container_ids = child_containers
            container.child_tool_ids = remaining_tools
