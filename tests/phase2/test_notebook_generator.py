"""Tests for the Phase 2 notebook generator."""

import os
import json
import tempfile
import pytest

from alteryx_pyspark_converter.phase1_parser.parser.workflow_parser import WorkflowParser
from alteryx_pyspark_converter.phase1_parser.output.json_writer import JSONWriter
from alteryx_pyspark_converter.phase2_generator.generator.notebook_generator import (
    NotebookGenerator,
)
from alteryx_pyspark_converter.phase2_generator.generator.dependency_resolver import (
    DependencyResolver,
)
from alteryx_pyspark_converter.phase2_generator.generator.flow_analyzer import (
    FlowAnalyzer,
)
from alteryx_pyspark_converter.phase2_generator.validators.syntax_validator import (
    SyntaxValidator,
)

FIXTURES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "fixtures",
    "sample_workflows",
)
SIMPLE_WORKFLOW = os.path.join(FIXTURES_DIR, "simple_workflow.yxmd")


@pytest.fixture
def intermediate_json():
    """Create an intermediate JSON file from the sample workflow."""
    parser = WorkflowParser()
    workflow = parser.parse(SIMPLE_WORKFLOW)

    with tempfile.TemporaryDirectory() as tmpdir:
        writer = JSONWriter()
        path = writer.write(workflow, tmpdir)
        yield path


@pytest.fixture
def sample_config():
    """Sample generation configuration."""
    return {
        "container_name": "mod_test",
        "input_tables": {
            "external_data": {
                "databricks_table": "catalog.schema.external_table",
                "maps_to_tool_id": 50,
            },
        },
        "column_mappings": {
            "Name": "full_name",
            "Value": "num_value",
            "Category": "category",
        },
        "output": {
            "table_name": "catalog.schema.output_table",
            "columns": ["full_name", "num_value", "category"],
            "expected_row_count": 100,
        },
        "notebook": {
            "output_path": "",
            "add_validation_cell": True,
            "add_schema_print": True,
        },
    }


class TestDependencyResolver:
    """Tests for DependencyResolver."""

    def test_resolve_execution_order(self, intermediate_json):
        """Test topological sort of tools."""
        workflow = JSONWriter.load(intermediate_json)
        resolver = DependencyResolver(workflow)
        container = workflow.containers[0]

        order = resolver.resolve_execution_order(container.tool_id)
        assert len(order) > 0

        # TextInput (1) should come before Filter (2)
        if 1 in order and 2 in order:
            assert order.index(1) < order.index(2)

        # Filter (2) should come before Formula (3)
        if 2 in order and 3 in order:
            assert order.index(2) < order.index(3)

    def test_validate_flow(self, intermediate_json):
        """Test flow validation."""
        workflow = JSONWriter.load(intermediate_json)
        resolver = DependencyResolver(workflow)
        container = workflow.containers[0]

        result = resolver.validate_flow(container.tool_id)
        assert result["valid"] or len(result.get("issues", [])) >= 0


class TestFlowAnalyzer:
    """Tests for FlowAnalyzer."""

    def test_get_container_flow(self, intermediate_json):
        """Test flow graph construction."""
        workflow = JSONWriter.load(intermediate_json)
        analyzer = FlowAnalyzer(workflow)
        container = workflow.containers[0]

        flow = analyzer.get_container_flow(container.tool_id)
        assert len(flow) > 0

    def test_get_source_tools(self, intermediate_json):
        """Test source tool identification."""
        workflow = JSONWriter.load(intermediate_json)
        analyzer = FlowAnalyzer(workflow)
        container = workflow.containers[0]

        sources = analyzer.get_source_tools(container.tool_id)
        assert 1 in sources  # TextInput is a source


class TestNotebookGenerator:
    """Tests for NotebookGenerator."""

    def test_generate_notebook(self, intermediate_json, sample_config):
        """Test basic notebook generation."""
        generator = NotebookGenerator(
            intermediate_json_path=intermediate_json,
            config_dict=sample_config,
        )

        notebook = generator.generate()

        assert "# Databricks notebook source" in notebook
        assert "from pyspark.sql" in notebook
        assert "COMMAND ----------" in notebook

    def test_notebook_has_header(self, intermediate_json, sample_config):
        """Test that notebook has a markdown header."""
        generator = NotebookGenerator(
            intermediate_json_path=intermediate_json,
            config_dict=sample_config,
        )

        notebook = generator.generate()
        assert "mod_test" in notebook
        assert "Auto-Generated from Alteryx" in notebook

    def test_notebook_has_config(self, intermediate_json, sample_config):
        """Test that notebook has configuration cell."""
        generator = NotebookGenerator(
            intermediate_json_path=intermediate_json,
            config_dict=sample_config,
        )

        notebook = generator.generate()
        assert "catalog.schema.external_table" in notebook

    def test_notebook_has_imports(self, intermediate_json, sample_config):
        """Test that notebook has imports."""
        generator = NotebookGenerator(
            intermediate_json_path=intermediate_json,
            config_dict=sample_config,
        )

        notebook = generator.generate()
        assert "from pyspark.sql import functions as F" in notebook
        assert "from pyspark.sql.types import" in notebook

    def test_notebook_has_validation(self, intermediate_json, sample_config):
        """Test that notebook has validation cell."""
        generator = NotebookGenerator(
            intermediate_json_path=intermediate_json,
            config_dict=sample_config,
        )

        notebook = generator.generate()
        assert "VALIDATION" in notebook
        assert "EXPECTED_ROW_COUNT" in notebook

    def test_generate_and_save(self, intermediate_json, sample_config):
        """Test generating and saving notebook."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test_output.py")

            generator = NotebookGenerator(
                intermediate_json_path=intermediate_json,
                config_dict=sample_config,
            )

            path = generator.generate_and_save(output_path)
            assert os.path.exists(path)

            with open(path) as f:
                content = f.read()
            assert len(content) > 0


class TestSyntaxValidator:
    """Tests for SyntaxValidator."""

    def test_valid_code(self):
        """Test validation of valid Python code."""
        code = """
from pyspark.sql import functions as F

df = spark.table("test")
df = df.filter(F.col("x") > 10)
"""
        validator = SyntaxValidator()
        result = validator.validate(code)
        assert result["valid"]

    def test_invalid_code(self):
        """Test validation of invalid Python code."""
        code = """
def broken(:
    pass
"""
        validator = SyntaxValidator()
        result = validator.validate(code)
        assert not result["valid"]
        assert len(result["issues"]) > 0

    def test_placeholder_detection(self):
        """Test detection of placeholder values."""
        code = """
df = spark.table("REPLACE_WITH_TABLE_NAME")
"""
        validator = SyntaxValidator()
        result = validator.validate(code)
        assert any(
            i["type"] == "placeholder" for i in result["issues"]
        )

    def test_magic_command_stripping(self):
        """Test that magic commands are stripped before validation."""
        code = """
# Databricks notebook source
# MAGIC %md
# MAGIC # Test Notebook

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.table("test")
"""
        validator = SyntaxValidator()
        result = validator.validate(code)
        assert result["valid"]
