"""Tests for the Phase 1 tool extractor."""

import os
import pytest

from alteryx_pyspark_converter.phase1_parser.parser.xml_extractor import XMLExtractor
from alteryx_pyspark_converter.phase1_parser.parser.tool_extractor import ToolExtractor

FIXTURES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "fixtures",
    "sample_workflows",
)
SIMPLE_WORKFLOW = os.path.join(FIXTURES_DIR, "simple_workflow.yxmd")


class TestToolExtractor:
    """Tests for ToolExtractor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.xml = XMLExtractor(SIMPLE_WORKFLOW)
        self.xml.parse()
        self.nodes = self.xml.get_nodes()
        self.extractor = ToolExtractor()

    def test_extract_tools(self):
        """Test basic tool extraction."""
        tools = self.extractor.extract(self.nodes)
        assert len(tools) > 0

        # Should not include containers
        tool_types = {t.tool_type for t in tools}
        assert "Container" not in tool_types

    def test_extract_tool_types(self):
        """Test that tool types are correctly identified."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        assert tool_map[1].tool_type == "TextInput"
        assert tool_map[2].tool_type == "Filter"
        assert tool_map[3].tool_type == "Formula"
        assert tool_map[4].tool_type == "Select"
        assert tool_map[5].tool_type == "Sort"

    def test_extract_annotations(self):
        """Test that annotations are extracted."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        assert tool_map[1].annotation == "Test Data"
        assert tool_map[2].annotation == "Filter Category A"

    def test_filter_config_extraction(self):
        """Test filter configuration extraction."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        filter_tool = tool_map[2]
        config = filter_tool.configuration
        assert config["expression"] == '[Category] = "A"'
        assert config["mode"] == "Custom"

    def test_formula_config_extraction(self):
        """Test formula configuration extraction."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        formula_tool = tool_map[3]
        config = formula_tool.configuration
        assert "formulas" in config
        assert len(config["formulas"]) == 2
        assert config["formulas"][0]["field"] == "DoubleValue"

    def test_select_config_extraction(self):
        """Test select configuration extraction."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        select_tool = tool_map[4]
        config = select_tool.configuration
        assert "fields" in config

        # Check that rename is captured
        name_field = None
        for f in config["fields"]:
            if f["name"] == "Name":
                name_field = f
                break
        assert name_field is not None
        assert name_field["rename"] == "FullName"

    def test_textinput_config_extraction(self):
        """Test TextInput data extraction."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        text_tool = tool_map[1]
        config = text_tool.configuration
        assert "fields" in config
        assert "data" in config
        assert len(config["fields"]) == 3
        assert config["row_count"] == 4

    def test_join_config_extraction(self):
        """Test join configuration extraction."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        join_tool = tool_map[7]
        config = join_tool.configuration
        assert not config["join_by_position"]
        assert "Name" in config["left_keys"]
        assert "person_name" in config["right_keys"]

    def test_sort_config_extraction(self):
        """Test sort configuration extraction."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        sort_tool = tool_map[5]
        config = sort_tool.configuration
        assert "fields" in config
        assert len(config["fields"]) == 1
        assert config["fields"][0]["field"] == "Value"
        assert config["fields"][0]["order"] == "Descending"

    def test_summarize_config_extraction(self):
        """Test summarize configuration extraction."""
        tools = self.extractor.extract(self.nodes)
        tool_map = {t.tool_id: t for t in tools}

        summ_tool = tool_map[8]
        config = summ_tool.configuration
        assert "fields" in config
        assert len(config["fields"]) == 3

        actions = [f["action"] for f in config["fields"]]
        assert "GroupBy" in actions
        assert "Sum" in actions
        assert "Count" in actions
