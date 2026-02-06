"""Tests for the Phase 1 workflow parser."""

import os
import json
import tempfile
import pytest

from alteryx_pyspark_converter.phase1_parser.parser.workflow_parser import WorkflowParser
from alteryx_pyspark_converter.phase1_parser.output.json_writer import JSONWriter
from alteryx_pyspark_converter.phase1_parser.models.workflow import Workflow

FIXTURES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "fixtures",
    "sample_workflows",
)
SIMPLE_WORKFLOW = os.path.join(FIXTURES_DIR, "simple_workflow.yxmd")


class TestWorkflowParser:
    """Tests for WorkflowParser."""

    def test_parse_simple_workflow(self):
        """Test parsing a simple workflow."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        assert workflow.name == "simple_workflow"
        assert len(workflow.containers) > 0
        assert len(workflow.tools) > 0
        assert len(workflow.connections) > 0

    def test_parse_extracts_metadata(self):
        """Test that metadata is extracted."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        assert workflow.metadata.alteryx_version == "2023.1.1.123"
        assert workflow.metadata.yxmd_version == "2023.1"

    def test_parse_extracts_containers(self):
        """Test container extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        assert len(workflow.containers) >= 1
        container = workflow.containers[0]
        assert container.name == "mod_test"
        assert not container.disabled
        assert len(container.child_tool_ids) > 0

    def test_parse_extracts_tools(self):
        """Test tool extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        tool_types = {t.tool_type for t in workflow.tools}
        assert "TextInput" in tool_types
        assert "Filter" in tool_types
        assert "Formula" in tool_types
        assert "Select" in tool_types
        assert "Sort" in tool_types
        assert "Join" in tool_types
        assert "Summarize" in tool_types

    def test_parse_extracts_connections(self):
        """Test connection extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        assert len(workflow.connections) == 8

        # Verify specific connection exists
        found_filter_to_formula = False
        for conn in workflow.connections:
            if (conn.origin_tool_id == 2
                    and conn.destination_tool_id == 3
                    and conn.origin_connection == "True"):
                found_filter_to_formula = True
                break

        assert found_filter_to_formula, "Filter True -> Formula connection not found"

    def test_parse_extracts_filter_config(self):
        """Test filter configuration extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        filter_tool = workflow.get_tool(2)
        assert filter_tool is not None
        assert filter_tool.tool_type == "Filter"
        assert "expression" in filter_tool.configuration
        assert '[Category] = "A"' in filter_tool.configuration["expression"]

    def test_parse_extracts_formula_config(self):
        """Test formula configuration extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        formula_tool = workflow.get_tool(3)
        assert formula_tool is not None
        assert formula_tool.tool_type == "Formula"
        assert "formulas" in formula_tool.configuration
        assert len(formula_tool.configuration["formulas"]) == 2

    def test_parse_extracts_textinput_data(self):
        """Test TextInput data extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        text_tool = workflow.get_tool(1)
        assert text_tool is not None
        assert text_tool.tool_type == "TextInput"
        config = text_tool.configuration
        assert "fields" in config
        assert "data" in config
        assert config["row_count"] == 4

    def test_parse_extracts_join_config(self):
        """Test join configuration extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        join_tool = workflow.get_tool(7)
        assert join_tool is not None
        assert join_tool.tool_type == "Join"
        config = join_tool.configuration
        assert config["left_keys"] == ["Name"]
        assert config["right_keys"] == ["person_name"]

    def test_parse_extracts_summarize_config(self):
        """Test summarize configuration extraction."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        summ_tool = workflow.get_tool(8)
        assert summ_tool is not None
        assert summ_tool.tool_type == "Summarize"
        config = summ_tool.configuration
        assert len(config["fields"]) == 3

    def test_container_tools(self):
        """Test getting tools within a container."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        container = workflow.containers[0]
        tools = workflow.get_container_tools(container.tool_id)
        assert len(tools) > 0

    def test_container_connections(self):
        """Test getting internal connections."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        container = workflow.containers[0]
        conns = workflow.get_container_connections(container.tool_id)
        assert len(conns) > 0

    def test_external_inputs(self):
        """Test identifying external inputs."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        container = workflow.containers[0]
        ext_inputs = workflow.get_container_external_inputs(container.tool_id)
        # The external DbInput (50) connects to Join (7) in the container
        assert len(ext_inputs) >= 1


class TestJSONWriter:
    """Tests for the JSON writer."""

    def test_write_and_load(self):
        """Test writing intermediate JSON and loading it back."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        with tempfile.TemporaryDirectory() as tmpdir:
            writer = JSONWriter()
            path = writer.write(workflow, tmpdir)

            assert os.path.exists(path)

            # Load it back
            loaded = JSONWriter.load(path)
            assert loaded.name == workflow.name
            assert len(loaded.containers) == len(workflow.containers)
            assert len(loaded.tools) == len(workflow.tools)
            assert len(loaded.connections) == len(workflow.connections)

    def test_write_string(self):
        """Test writing to string."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        writer = JSONWriter()
        json_str = writer.write_string(workflow)

        data = json.loads(json_str)
        assert "workflow" in data
        assert "containers" in data
        assert "tools" in data
        assert "connections" in data

    def test_roundtrip_fidelity(self):
        """Test that data survives a write-load roundtrip."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        writer = JSONWriter()
        json_str = writer.write_string(workflow)
        loaded = JSONWriter.load_string(json_str)

        # Check tools preserved
        for orig, loaded_t in zip(
            sorted(workflow.tools, key=lambda t: t.tool_id),
            sorted(loaded.tools, key=lambda t: t.tool_id),
        ):
            assert orig.tool_id == loaded_t.tool_id
            assert orig.tool_type == loaded_t.tool_type

    def test_container_summary(self):
        """Test container summary generation."""
        parser = WorkflowParser()
        workflow = parser.parse(SIMPLE_WORKFLOW)

        summaries = parser.get_container_summary(workflow)
        assert len(summaries) >= 1
        assert summaries[0]["name"] == "mod_test"
        assert summaries[0]["num_tools"] > 0
