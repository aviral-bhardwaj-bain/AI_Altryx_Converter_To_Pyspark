"""XML parsing utilities for Alteryx .yxmd files."""

import xml.etree.ElementTree as ET
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class XMLExtractor:
    """Low-level XML extraction utilities for Alteryx workflows."""

    def __init__(self, yxmd_path: str):
        self.yxmd_path = yxmd_path
        self.tree: Optional[ET.ElementTree] = None
        self.root: Optional[ET.Element] = None

    def parse(self) -> ET.Element:
        """Parse the .yxmd XML file and return the root element."""
        try:
            self.tree = ET.parse(self.yxmd_path)
            self.root = self.tree.getroot()
            logger.info("Parsed XML from: %s", self.yxmd_path)
            return self.root
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML in {self.yxmd_path}: {e}") from e
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {self.yxmd_path}")

    def get_nodes(self) -> list[ET.Element]:
        """Get all <Node> elements from the workflow."""
        if self.root is None:
            raise RuntimeError("XML not parsed. Call parse() first.")
        nodes = self.root.findall(".//Node")
        logger.debug("Found %d nodes", len(nodes))
        return nodes

    def get_connections(self) -> list[ET.Element]:
        """Get all <Connection> elements from the workflow."""
        if self.root is None:
            raise RuntimeError("XML not parsed. Call parse() first.")
        connections = self.root.findall(".//Connection")
        logger.debug("Found %d connections", len(connections))
        return connections

    def get_properties(self) -> Optional[ET.Element]:
        """Get the <Properties> element containing workflow metadata."""
        if self.root is None:
            raise RuntimeError("XML not parsed. Call parse() first.")
        return self.root.find("Properties")

    def get_workflow_version(self) -> str:
        """Extract Alteryx version from workflow metadata."""
        props = self.get_properties()
        if props is not None:
            meta = props.find(".//MetaInfo")
            if meta is not None:
                ver = meta.find("AlteryxVersion")
                if ver is not None and ver.text:
                    return ver.text
        return ""

    def get_yxmd_version(self) -> str:
        """Extract YXMD format version."""
        if self.root is not None:
            return self.root.get("yxmdVer", "")
        return ""

    @staticmethod
    def get_text(element: Optional[ET.Element], default: str = "") -> str:
        """Safely get text from an XML element."""
        if element is not None and element.text:
            return element.text.strip()
        return default

    @staticmethod
    def get_attr(element: Optional[ET.Element], attr: str, default: str = "") -> str:
        """Safely get an attribute from an XML element."""
        if element is not None:
            return element.get(attr, default)
        return default

    @staticmethod
    def element_to_string(element: ET.Element) -> str:
        """Convert an XML element to a string."""
        return ET.tostring(element, encoding="unicode", method="xml")

    @staticmethod
    def find_child_text(parent: ET.Element, path: str, default: str = "") -> str:
        """Find a child element and return its text."""
        child = parent.find(path)
        if child is not None and child.text:
            return child.text.strip()
        return default
