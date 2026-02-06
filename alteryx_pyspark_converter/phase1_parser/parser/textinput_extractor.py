"""Extract hardcoded TextInput data from Alteryx workflows."""

import xml.etree.ElementTree as ET
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


class TextInputExtractor:
    """
    Extract inline/hardcoded data from TextInput tools.

    TextInput tools in Alteryx contain hardcoded data rows that need to be
    extracted and preserved in the intermediate representation.
    """

    def extract(self, config_node: ET.Element) -> dict:
        """
        Extract TextInput configuration including field definitions and data.

        Args:
            config_node: The <Configuration> XML element of a TextInput tool.

        Returns:
            Dictionary with fields, data, and row_count.
        """
        fields = self._extract_fields(config_node)
        data = self._extract_data(config_node, fields)

        return {
            "fields": fields,
            "data": data,
            "row_count": len(data),
        }

    def _extract_fields(self, config_node: ET.Element) -> list[dict]:
        """Extract field definitions from TextInput configuration."""
        fields = []

        # Try multiple possible XML structures
        field_sources = [
            config_node.findall(".//Field"),
            config_node.findall(".//Fields/Field"),
            config_node.findall(".//RecordInfo/Field"),
        ]

        for source in field_sources:
            if source:
                for f in source:
                    name = f.get("name", "")
                    if not name:
                        continue

                    fields.append({
                        "name": name,
                        "type": f.get("type", "V_WString"),
                        "size": f.get("size", ""),
                        "scale": f.get("scale", ""),
                        "source": f.get("source", "TextInput"),
                    })
                break  # Use the first source that has fields

        return fields

    def _extract_data(
        self,
        config_node: ET.Element,
        fields: list[dict],
    ) -> list[dict]:
        """Extract data rows from TextInput configuration."""
        data = []
        field_names = [f["name"] for f in fields]

        # Try multiple possible data locations
        row_sources = [
            config_node.findall(".//Row"),
            config_node.findall(".//Data/r"),
            config_node.findall(".//Data/Row"),
        ]

        for source in row_sources:
            if source:
                for row_elem in source:
                    row_data = {}
                    for fname in field_names:
                        value = row_elem.get(fname)
                        row_data[fname] = self._convert_value(value)
                    data.append(row_data)
                break

        return data

    def _convert_value(self, value: Optional[str]) -> Any:
        """Convert a string value to appropriate Python type."""
        if value is None:
            return None
        if value == "":
            return ""
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        # Try numeric conversion
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    def extract_as_list(
        self,
        config_node: ET.Element,
    ) -> tuple[list[dict], list[list]]:
        """
        Extract TextInput data as fields + list-of-lists format.

        Returns:
            Tuple of (fields, data_rows) where data_rows is list of lists.
        """
        result = self.extract(config_node)
        fields = result["fields"]
        field_names = [f["name"] for f in fields]

        data_rows = []
        for row in result["data"]:
            row_list = [row.get(fn) for fn in field_names]
            data_rows.append(row_list)

        return fields, data_rows
