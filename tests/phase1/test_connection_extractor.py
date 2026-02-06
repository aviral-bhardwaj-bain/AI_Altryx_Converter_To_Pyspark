"""Tests for the Phase 1 connection extractor."""

import os
import pytest

from alteryx_pyspark_converter.phase1_parser.parser.xml_extractor import XMLExtractor
from alteryx_pyspark_converter.phase1_parser.parser.connection_extractor import (
    ConnectionExtractor,
)

FIXTURES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "fixtures",
    "sample_workflows",
)
SIMPLE_WORKFLOW = os.path.join(FIXTURES_DIR, "simple_workflow.yxmd")


class TestConnectionExtractor:
    """Tests for ConnectionExtractor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.xml = XMLExtractor(SIMPLE_WORKFLOW)
        root = self.xml.parse()
        self.extractor = ConnectionExtractor()
        self.connections = self.extractor.extract(root)

    def test_extract_connections(self):
        """Test basic connection extraction."""
        assert len(self.connections) == 8

    def test_connection_properties(self):
        """Test that connection properties are correctly extracted."""
        # Find TextInput -> Filter connection
        conn = None
        for c in self.connections:
            if c.origin_tool_id == 1 and c.destination_tool_id == 2:
                conn = c
                break

        assert conn is not None
        assert conn.origin_connection == "Output"
        assert conn.destination_connection == "Input"

    def test_filter_outputs(self):
        """Test that Filter True output is captured."""
        conn = None
        for c in self.connections:
            if c.origin_tool_id == 2 and c.origin_connection == "True":
                conn = c
                break

        assert conn is not None
        assert conn.destination_tool_id == 3

    def test_join_connections(self):
        """Test join input connections."""
        left_conn = None
        right_conn = None
        for c in self.connections:
            if c.destination_tool_id == 7:
                if c.destination_connection == "Left":
                    left_conn = c
                elif c.destination_connection == "Right":
                    right_conn = c

        assert left_conn is not None
        assert right_conn is not None
        assert left_conn.origin_tool_id == 1  # TextInput
        assert right_conn.origin_tool_id == 50  # External DbInput

    def test_build_data_flow_graph(self):
        """Test building the data flow graph."""
        graph = self.extractor.build_data_flow_graph(self.connections)

        # TextInput (1) should have outputs
        assert 1 in graph
        assert len(graph[1]["outputs"]) >= 2  # To Filter and Join

        # Filter (2) should have inputs and outputs
        assert 2 in graph
        assert len(graph[2]["inputs"]) >= 1
        assert len(graph[2]["outputs"]) >= 1

    def test_find_source_tools(self):
        """Test finding source tools."""
        sources = self.extractor.find_source_tools(self.connections)
        assert 1 in sources  # TextInput
        assert 50 in sources  # External DbInput

    def test_find_sink_tools(self):
        """Test finding sink tools."""
        sinks = self.extractor.find_sink_tools(self.connections)
        assert 6 in sinks  # Browse

    def test_get_upstream_tools(self):
        """Test getting upstream tools."""
        upstream = self.extractor.get_upstream_tools(6, self.connections)
        assert 5 in upstream  # Sort
        assert 4 in upstream  # Select
        assert 3 in upstream  # Formula
        assert 2 in upstream  # Filter
        assert 1 in upstream  # TextInput

    def test_get_downstream_tools(self):
        """Test getting downstream tools."""
        downstream = self.extractor.get_downstream_tools(1, self.connections)
        assert 2 in downstream  # Filter
        assert 7 in downstream  # Join
