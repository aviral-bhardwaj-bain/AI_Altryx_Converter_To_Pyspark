"""Extract tools and their configurations from Alteryx workflow XML."""

import xml.etree.ElementTree as ET
from typing import Optional
import logging

from ..models.tool import Tool
from ..models.enums import PLUGIN_TYPE_MAP, ToolType
from .xml_extractor import XMLExtractor

logger = logging.getLogger(__name__)


class ToolExtractor:
    """Extract all tools and their configurations from an Alteryx workflow."""

    CONTAINER_PLUGIN = "AlteryxGuiToolkit.ToolContainer.ToolContainer"

    def extract(self, nodes: list[ET.Element]) -> list[Tool]:
        """
        Extract all non-container tools from the workflow nodes.

        Args:
            nodes: List of <Node> XML elements.

        Returns:
            List of Tool objects with parsed configurations.
        """
        tools: list[Tool] = []

        for node in nodes:
            plugin = self._get_plugin_type(node)

            # Skip containers - handled by ContainerExtractor
            if plugin == self.CONTAINER_PLUGIN:
                continue

            tool = self._parse_tool(node, plugin)
            if tool:
                tools.append(tool)

        logger.info("Extracted %d tools", len(tools))
        return tools

    def _get_plugin_type(self, node: ET.Element) -> str:
        """Get the plugin type string from a node."""
        gui_settings = node.find("GuiSettings")
        if gui_settings is not None:
            return gui_settings.get("Plugin", "")
        return node.get("EngineDll", "")

    def _parse_tool(self, node: ET.Element, plugin: str) -> Optional[Tool]:
        """Parse a single tool node into a Tool object."""
        tool_id_str = node.get("ToolID")
        if not tool_id_str:
            return None

        tool_id = int(tool_id_str)
        tool_type = PLUGIN_TYPE_MAP.get(plugin, ToolType.UNKNOWN)
        if tool_type == ToolType.UNKNOWN and plugin:
            logger.warning("Unknown plugin type: %s (tool %d)", plugin, tool_id)

        # Extract position
        position = self._extract_position(node)

        # Extract annotation
        annotation = self._extract_annotation(node)

        # Extract container membership
        container_id = self._find_container_id(node)

        # Extract configuration based on tool type
        configuration = self._extract_configuration(node, tool_type)

        # Raw XML for debugging
        raw_xml = ET.tostring(node, encoding="unicode", method="xml")

        return Tool(
            tool_id=tool_id,
            tool_type=str(tool_type.value) if isinstance(tool_type, ToolType) else str(tool_type),
            plugin_type=plugin,
            position=position,
            annotation=annotation,
            container_id=container_id,
            configuration=configuration,
            raw_xml=raw_xml,
        )

    def _extract_position(self, node: ET.Element) -> dict:
        """Extract tool position."""
        gui = node.find("GuiSettings")
        if gui is not None:
            pos = gui.find("Position")
            if pos is not None:
                return {
                    "x": int(float(pos.get("x", "0"))),
                    "y": int(float(pos.get("y", "0"))),
                }
        return {"x": 0, "y": 0}

    def _extract_annotation(self, node: ET.Element) -> str:
        """Extract the human-readable annotation/comment."""
        ann = node.find(".//Annotation")
        if ann is not None:
            # Try Name element first
            name = ann.find("Name")
            if name is not None and name.text:
                return name.text.strip()
            # Then DefaultAnnotationText
            default = ann.find("DefaultAnnotationText")
            if default is not None and default.text:
                return default.text.strip()
        return ""

    def _find_container_id(self, node: ET.Element) -> Optional[int]:
        """Determine which container this tool belongs to."""
        gui = node.find("GuiSettings")
        if gui is not None:
            container_ref = gui.get("ToolContainer")
            if container_ref:
                return int(container_ref)

        annotation = node.find(".//Annotation")
        if annotation is not None:
            # Try as attribute: <Annotation ChildOf="100">
            child_of = annotation.get("ChildOf")
            if child_of:
                return int(child_of)
            # Try as child element: <Annotation><ChildOf>100</ChildOf></Annotation>
            child_of_elem = annotation.find("ChildOf")
            if child_of_elem is not None and child_of_elem.text:
                return int(child_of_elem.text.strip())

        return None

    def _extract_configuration(self, node: ET.Element, tool_type: ToolType) -> dict:
        """Extract tool-specific configuration."""
        config_node = node.find(".//Configuration")
        if config_node is None:
            # Try Properties/Configuration
            config_node = node.find(".//Properties/Configuration")
        if config_node is None:
            return {}

        extractors = {
            ToolType.FILTER: self.extract_filter_config,
            ToolType.JOIN: self.extract_join_config,
            ToolType.FORMULA: self.extract_formula_config,
            ToolType.SELECT: self.extract_select_config,
            ToolType.CROSSTAB: self.extract_crosstab_config,
            ToolType.SUMMARIZE: self.extract_summarize_config,
            ToolType.UNION: self.extract_union_config,
            ToolType.SORT: self.extract_sort_config,
            ToolType.UNIQUE: self.extract_unique_config,
            ToolType.SAMPLE: self.extract_sample_config,
            ToolType.TEXT_INPUT: self.extract_textinput_config,
            ToolType.DB_INPUT: self.extract_dbinput_config,
            ToolType.DB_OUTPUT: self.extract_dboutput_config,
            ToolType.MULTI_ROW_FORMULA: self.extract_multirow_formula_config,
            ToolType.RECORD_ID: self.extract_record_id_config,
            ToolType.TRANSPOSE: self.extract_transpose_config,
            ToolType.APPEND_FIELDS: self.extract_append_fields_config,
            ToolType.FIND_REPLACE: self.extract_find_replace_config,
            ToolType.REGEX: self.extract_regex_config,
            ToolType.UNIQUE: self.extract_unique_config,
            # LockIn tools use the same config as their standard counterparts
            ToolType.LOCKIN_FILTER: self.extract_filter_config,
            ToolType.LOCKIN_SELECT: self.extract_select_config,
            ToolType.LOCKIN_FORMULA: self.extract_formula_config,
            ToolType.LOCKIN_JOIN: self.extract_join_config,
            ToolType.LOCKIN_SUMMARIZE: self.extract_summarize_config,
            ToolType.LOCKIN_UNION: self.extract_union_config,
            ToolType.LOCKIN_CROSSTAB: self.extract_crosstab_config,
            ToolType.LOCKIN_SORT: self.extract_sort_config,
            ToolType.LOCKIN_INPUT: self.extract_dbinput_config,
            ToolType.LOCKIN_WRITE: self.extract_dboutput_config,
        }

        extractor = extractors.get(tool_type)
        if extractor:
            try:
                return extractor(config_node)
            except Exception as e:
                logger.warning(
                    "Error extracting config for tool type %s: %s",
                    tool_type, e,
                )
                return {"raw": ET.tostring(config_node, encoding="unicode")}

        # For unknown/unsupported tools, store raw XML
        return {"raw": ET.tostring(config_node, encoding="unicode")}

    def extract_filter_config(self, config_node: ET.Element) -> dict:
        """Extract Filter tool configuration."""
        result: dict = {"expression": "", "mode": "Custom"}

        # Try Expression element
        expr = config_node.find("Expression")
        if expr is not None and expr.text:
            result["expression"] = expr.text.strip()

        # Try Mode
        mode = config_node.find("Mode")
        if mode is not None and mode.text:
            result["mode"] = mode.text.strip()

        # Simple filter config
        simple = config_node.find("Simple")
        if simple is not None:
            result["mode"] = "Simple"
            field_elem = simple.find("Field")
            operator = simple.find("Operator")
            operands = simple.findall("Operand")

            if field_elem is not None:
                result["simple_field"] = field_elem.get("field", "")
            if operator is not None:
                result["simple_operator"] = operator.text or ""
            if operands:
                result["simple_operands"] = [
                    o.text or "" for o in operands
                ]

        return result

    def extract_join_config(self, config_node: ET.Element) -> dict:
        """Extract Join tool configuration."""
        result: dict = {
            "join_by_position": False,
            "left_keys": [],
            "right_keys": [],
            "left_select": [],
            "right_select": [],
            "join_select": [],
        }

        # Join by position
        jbp = config_node.find("JoinByPosition")
        if jbp is not None and jbp.text:
            result["join_by_position"] = jbp.text.strip().lower() in ("true", "1")

        # Join fields
        join_fields = config_node.findall(".//JoinInfo")
        for jf in join_fields:
            connection = jf.get("connection", "")
            field_elem = jf.find("Field")
            if field_elem is not None:
                field_name = field_elem.get("field", "")
                if connection == "Left":
                    result["left_keys"].append(field_name)
                elif connection == "Right":
                    result["right_keys"].append(field_name)

        # Select configurations for each output
        for select_tag in ("SelectConfiguration", "Select"):
            for sel in config_node.findall(f".//{select_tag}"):
                connection = sel.get("connection", "Join")
                fields = self._extract_select_fields(sel)
                if connection == "Left":
                    result["left_select"] = fields
                elif connection == "Right":
                    result["right_select"] = fields
                else:
                    result["join_select"] = fields

        return result

    def extract_formula_config(self, config_node: ET.Element) -> dict:
        """Extract Formula tool configuration."""
        formulas = []

        for expr_elem in config_node.findall(".//FormulaField"):
            field_name = expr_elem.get("field", expr_elem.get("expression", ""))
            expression = expr_elem.get("expression", "")
            field_type = expr_elem.get("type", "")
            size = expr_elem.get("size", "")

            formulas.append({
                "field": field_name,
                "type": field_type,
                "size": size,
                "expression": expression,
            })

        # Alternative structure: FormulaFields/FormulaField
        if not formulas:
            for expr_elem in config_node.findall(".//FormulaFields/FormulaField"):
                field_name = expr_elem.get("field", "")
                expression = expr_elem.get("expression", "")
                field_type = expr_elem.get("type", "")
                formulas.append({
                    "field": field_name,
                    "type": field_type,
                    "expression": expression,
                })

        return {"formulas": formulas}

    def extract_select_config(self, config_node: ET.Element) -> dict:
        """Extract Select tool configuration."""
        fields = self._extract_select_fields(config_node)

        # Unknown fields handling
        unknown = "Select"
        order_changed = False
        cfg = config_node.find("Configuration")
        target = cfg if cfg is not None else config_node

        unknown_elem = target.find("SelectFields")
        if unknown_elem is not None:
            unknown = unknown_elem.get("UnknownFields", "Select")
            order_changed = unknown_elem.get("OrderChanged", "false").lower() == "true"

        return {
            "fields": fields,
            "unknown_fields": unknown,
            "order_changed": order_changed,
        }

    def extract_crosstab_config(self, config_node: ET.Element) -> dict:
        """Extract CrossTab tool configuration."""
        result: dict = {
            "group_fields": [],
            "header_field": "",
            "data_field": "",
            "method": "Concatenate",
            "separator": "",
            "field_type": "",
            "field_size": "",
        }

        # Group by fields
        for gf in config_node.findall(".//GroupFields/Field"):
            field_name = gf.get("field", gf.text or "")
            if field_name:
                result["group_fields"].append(field_name)

        # Header (pivot column)
        header = config_node.find(".//HeaderField")
        if header is not None:
            result["header_field"] = header.get("field", header.text or "")

        # Data field
        data = config_node.find(".//DataField")
        if data is not None:
            result["data_field"] = data.get("field", data.text or "")

        # Aggregation method
        method = config_node.find(".//Method")
        if method is not None:
            result["method"] = method.text or method.get("value", "Concatenate")

        # Separator
        sep = config_node.find(".//Separator")
        if sep is not None:
            result["separator"] = sep.text or ""

        # Field type and size
        ft = config_node.find(".//FieldType")
        if ft is not None:
            result["field_type"] = ft.text or ""
        fs = config_node.find(".//FieldSize")
        if fs is not None:
            result["field_size"] = fs.text or ""

        return result

    def extract_summarize_config(self, config_node: ET.Element) -> dict:
        """Extract Summarize tool configuration."""
        fields = []

        for sf in config_node.findall(".//SummarizeField"):
            field_name = sf.get("field", "")
            action = sf.get("action", "GroupBy")
            output_name = sf.get("rename", field_name)

            fields.append({
                "field": field_name,
                "action": action,
                "output_name": output_name,
            })

        return {"fields": fields}

    def extract_union_config(self, config_node: ET.Element) -> dict:
        """Extract Union tool configuration."""
        result: dict = {
            "mode": "Auto",  # Auto, Manual, ByPosition
            "field_mappings": [],
        }

        mode = config_node.find("Mode")
        if mode is not None and mode.text:
            result["mode"] = mode.text.strip()

        # Manual field mappings
        for fm in config_node.findall(".//UnionField"):
            result["field_mappings"].append({
                "output": fm.get("output", ""),
                "inputs": [
                    {"connection": inp.get("connection", ""),
                     "field": inp.get("field", "")}
                    for inp in fm.findall("Input")
                ],
            })

        return result

    def extract_sort_config(self, config_node: ET.Element) -> dict:
        """Extract Sort tool configuration."""
        fields = []

        for sf in config_node.findall(".//SortInfo/Field"):
            field_name = sf.get("field", "")
            order = sf.get("order", "Ascending")
            fields.append({
                "field": field_name,
                "order": order,
            })

        # Alternative structure
        if not fields:
            for sf in config_node.findall(".//Field"):
                field_name = sf.get("field", "")
                order = sf.get("order", "Ascending")
                if field_name:
                    fields.append({
                        "field": field_name,
                        "order": order,
                    })

        return {"fields": fields}

    def extract_unique_config(self, config_node: ET.Element) -> dict:
        """Extract Unique tool configuration."""
        fields = []

        for uf in config_node.findall(".//UniqueFields/Field"):
            fields.append(uf.get("field", uf.text or ""))

        # Alternative structure
        if not fields:
            for uf in config_node.findall(".//Field"):
                field_name = uf.get("field", uf.text or "")
                if field_name:
                    fields.append(field_name)

        return {"unique_fields": fields}

    def extract_sample_config(self, config_node: ET.Element) -> dict:
        """Extract Sample tool configuration."""
        result: dict = {
            "mode": "First",  # First, Last, Skip, Percent, Random
            "n": 1,
        }

        # Number of records
        n_elem = config_node.find("N")
        if n_elem is not None and n_elem.text:
            try:
                result["n"] = int(n_elem.text)
            except ValueError:
                result["n"] = 1

        # GroupBy fields
        group_fields = []
        for gf in config_node.findall(".//GroupFields/Field"):
            group_fields.append(gf.get("field", ""))
        if group_fields:
            result["group_fields"] = group_fields

        return result

    def extract_textinput_config(self, config_node: ET.Element) -> dict:
        """Extract TextInput tool configuration (hardcoded data)."""
        fields = []
        data = []

        # Extract field definitions
        for f in config_node.findall(".//Field"):
            fields.append({
                "name": f.get("name", ""),
                "type": f.get("type", "V_WString"),
                "size": f.get("size", ""),
            })

        # Extract data rows
        for row in config_node.findall(".//Row"):
            row_data = {}
            for field_def in fields:
                fname = field_def["name"]
                val = row.get(fname)
                row_data[fname] = val
            data.append(row_data)

        # Alternative: data in text content
        if not data:
            data_elem = config_node.find(".//Data")
            if data_elem is not None:
                for row in data_elem.findall("r"):
                    row_data = {}
                    for field_def in fields:
                        fname = field_def["name"]
                        val = row.get(fname)
                        row_data[fname] = val
                    data.append(row_data)

        return {
            "fields": fields,
            "data": data,
            "row_count": len(data),
        }

    def extract_dbinput_config(self, config_node: ET.Element) -> dict:
        """Extract Database Input tool configuration."""
        result: dict = {
            "connection_string": "",
            "query": "",
            "table_name": "",
        }

        conn = config_node.find(".//ConnectionString")
        if conn is not None and conn.text:
            result["connection_string"] = conn.text.strip()

        query = config_node.find(".//Query")
        if query is not None and query.text:
            result["query"] = query.text.strip()

        table = config_node.find(".//TableName")
        if table is not None and table.text:
            result["table_name"] = table.text.strip()

        # Passwords/encrypted fields (store indicator only, not the value)
        pwd = config_node.find(".//Password")
        if pwd is not None:
            result["has_password"] = True

        return result

    def extract_dboutput_config(self, config_node: ET.Element) -> dict:
        """Extract Database Output tool configuration."""
        result: dict = {
            "connection_string": "",
            "table_name": "",
            "output_type": "",
        }

        conn = config_node.find(".//ConnectionString")
        if conn is not None and conn.text:
            result["connection_string"] = conn.text.strip()

        table = config_node.find(".//TableName")
        if table is not None and table.text:
            result["table_name"] = table.text.strip()

        output_type = config_node.find(".//OutputType")
        if output_type is not None and output_type.text:
            result["output_type"] = output_type.text.strip()

        return result

    def extract_multirow_formula_config(self, config_node: ET.Element) -> dict:
        """Extract MultiRowFormula tool configuration."""
        result: dict = {
            "field": "",
            "expression": "",
            "type": "",
            "num_rows": 1,
            "direction": "Below",
        }

        field_elem = config_node.find(".//UpdateField")
        if field_elem is not None:
            result["field"] = field_elem.get("field", "")
            result["type"] = field_elem.get("type", "")

        expr = config_node.find(".//Expression")
        if expr is not None and expr.text:
            result["expression"] = expr.text.strip()

        nr = config_node.find(".//NumRows")
        if nr is not None and nr.text:
            try:
                result["num_rows"] = int(nr.text)
            except ValueError:
                pass

        return result

    def extract_record_id_config(self, config_node: ET.Element) -> dict:
        """Extract RecordID tool configuration."""
        result: dict = {
            "field_name": "RecordID",
            "starting_value": 1,
            "field_type": "Int32",
        }

        field = config_node.find(".//FieldName")
        if field is not None and field.text:
            result["field_name"] = field.text.strip()

        start = config_node.find(".//StartingValue")
        if start is not None and start.text:
            try:
                result["starting_value"] = int(start.text)
            except ValueError:
                pass

        ft = config_node.find(".//FieldType")
        if ft is not None and ft.text:
            result["field_type"] = ft.text.strip()

        return result

    def extract_transpose_config(self, config_node: ET.Element) -> dict:
        """Extract Transpose tool configuration."""
        key_fields = []
        data_fields = []

        for kf in config_node.findall(".//KeyFields/Field"):
            key_fields.append(kf.get("field", ""))

        for df in config_node.findall(".//DataFields/Field"):
            data_fields.append(df.get("field", ""))

        return {
            "key_fields": key_fields,
            "data_fields": data_fields,
        }

    def extract_append_fields_config(self, config_node: ET.Element) -> dict:
        """Extract AppendFields tool configuration."""
        return {
            "select_fields": self._extract_select_fields(config_node),
        }

    def extract_find_replace_config(self, config_node: ET.Element) -> dict:
        """Extract FindReplace tool configuration."""
        result: dict = {
            "find_field": "",
            "replace_field": "",
            "find_mode": "Normal",
        }

        ff = config_node.find(".//FindField")
        if ff is not None:
            result["find_field"] = ff.get("field", "")

        rf = config_node.find(".//ReplaceField")
        if rf is not None:
            result["replace_field"] = rf.get("field", "")

        return result

    def extract_regex_config(self, config_node: ET.Element) -> dict:
        """Extract Regex tool configuration."""
        result: dict = {
            "field": "",
            "expression": "",
            "output_method": "Replace",
        }

        field = config_node.find(".//Field")
        if field is not None:
            result["field"] = field.get("field", field.text or "")

        expr = config_node.find(".//Expression")
        if expr is not None and expr.text:
            result["expression"] = expr.text.strip()

        method = config_node.find(".//OutputMethod")
        if method is not None and method.text:
            result["output_method"] = method.text.strip()

        return result

    def _extract_select_fields(self, parent: ET.Element) -> list[dict]:
        """Extract field selection configurations."""
        fields = []

        for sf in parent.findall(".//SelectField"):
            field_name = sf.get("field", "")
            selected = sf.get("selected", "True").lower() in ("true", "1")
            rename = sf.get("rename", "")
            field_type = sf.get("type", "")
            size = sf.get("size", "")

            fields.append({
                "name": field_name,
                "selected": selected,
                "rename": rename if rename else None,
                "type": field_type if field_type else None,
                "size": size if size else None,
            })

        return fields
