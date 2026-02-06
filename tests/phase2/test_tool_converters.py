"""Tests for Phase 2 tool converters."""

import pytest

from alteryx_pyspark_converter.phase2_generator.config.column_mapping import ColumnMapper
from alteryx_pyspark_converter.phase2_generator.expression_converter.alteryx_to_pyspark import (
    AlteryxExpressionConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.filter_converter import (
    FilterConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.join_converter import (
    JoinConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.formula_converter import (
    FormulaConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.select_converter import (
    SelectConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.crosstab_converter import (
    CrossTabConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.summarize_converter import (
    SummarizeConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.union_converter import (
    UnionConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.sort_converter import (
    SortConverter,
)
from alteryx_pyspark_converter.phase2_generator.tool_converters.textinput_converter import (
    TextInputConverter,
)


@pytest.fixture
def mapper():
    return ColumnMapper({
        "Category": "category",
        "Value": "num_value",
        "Name": "full_name",
    })


@pytest.fixture
def expr_converter(mapper):
    return AlteryxExpressionConverter(mapper)


class TestFilterConverter:
    def test_generate_code(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 2,
            "tool_type": "Filter",
            "annotation": "Filter Category A",
            "configuration": {
                "expression": '[Category] = "A"',
                "mode": "Custom",
            },
        }
        converter = FilterConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_filter")

        assert "df_filter_true" in code
        assert "df_filter_false" in code
        assert ".filter(" in code
        assert "category" in code  # Mapped column name

    def test_empty_expression(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 2,
            "tool_type": "Filter",
            "annotation": "",
            "configuration": {"expression": "", "mode": "Custom"},
        }
        converter = FilterConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_filter")
        assert "WARNING" in code


class TestJoinConverter:
    def test_generate_same_keys(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 7,
            "tool_type": "Join",
            "annotation": "Test Join",
            "configuration": {
                "join_by_position": False,
                "left_keys": ["Name"],
                "right_keys": ["Name"],
                "left_select": [],
                "right_select": [],
                "join_select": [],
            },
        }
        converter = JoinConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code(
            "df_left", "df_join", right_df_name="df_right"
        )

        assert "df_join_join" in code
        assert "df_join_left" in code
        assert "df_join_right" in code
        assert "inner" in code
        assert "left_anti" in code

    def test_generate_different_keys(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 7,
            "tool_type": "Join",
            "annotation": "",
            "configuration": {
                "join_by_position": False,
                "left_keys": ["Name"],
                "right_keys": ["Category"],
                "left_select": [],
                "right_select": [],
                "join_select": [],
            },
        }
        converter = JoinConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code(
            "df_left", "df_join", right_df_name="df_right"
        )

        assert "==" in code  # Explicit join condition


class TestFormulaConverter:
    def test_generate_code(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 3,
            "tool_type": "Formula",
            "annotation": "Calc Fields",
            "configuration": {
                "formulas": [
                    {
                        "field": "DoubleValue",
                        "type": "Int32",
                        "expression": "[Value] * 2",
                    },
                ],
            },
        }
        converter = FormulaConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_formula")

        assert ".withColumn(" in code
        assert "DoubleValue" in code


class TestSelectConverter:
    def test_generate_with_rename(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 4,
            "tool_type": "Select",
            "annotation": "",
            "configuration": {
                "fields": [
                    {"name": "Name", "selected": True, "rename": "FullName"},
                    {"name": "Value", "selected": True, "rename": None},
                    {"name": "Category", "selected": False, "rename": None},
                ],
                "unknown_fields": "Select",
                "order_changed": False,
            },
        }
        converter = SelectConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_select")

        assert "withColumnRenamed" in code
        assert "drop" in code or "select" in code

    def test_generate_drop_only(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 4,
            "tool_type": "Select",
            "annotation": "",
            "configuration": {
                "fields": [
                    {"name": "Name", "selected": True, "rename": None},
                    {"name": "Category", "selected": False, "rename": None},
                ],
                "unknown_fields": "Select",
                "order_changed": False,
            },
        }
        converter = SelectConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_select")
        assert ".drop(" in code


class TestCrossTabConverter:
    def test_generate_code(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 10,
            "tool_type": "CrossTab",
            "annotation": "Pivot",
            "configuration": {
                "group_fields": ["Name"],
                "header_field": "Category",
                "data_field": "Value",
                "method": "Sum",
                "separator": "",
                "field_type": "",
            },
        }
        converter = CrossTabConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_pivot")

        assert ".groupBy(" in code
        assert ".pivot(" in code
        assert ".agg(" in code
        assert "F.sum" in code


class TestSummarizeConverter:
    def test_generate_code(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 8,
            "tool_type": "Summarize",
            "annotation": "Summarize",
            "configuration": {
                "fields": [
                    {"field": "Category", "action": "GroupBy", "output_name": "Category"},
                    {"field": "Value", "action": "Sum", "output_name": "TotalValue"},
                    {"field": "Value", "action": "Count", "output_name": "RecordCount"},
                ],
            },
        }
        converter = SummarizeConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_summary")

        assert ".groupBy(" in code
        assert ".agg(" in code
        assert "F.sum" in code
        assert "F.count" in code


class TestUnionConverter:
    def test_generate_code(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 9,
            "tool_type": "Union",
            "annotation": "Union All",
            "configuration": {"mode": "Auto"},
        }
        converter = UnionConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code(
            "df_input1",
            "df_union",
            additional_inputs=["df_input2", "df_input3"],
        )

        assert ".unionByName(" in code
        assert "allowMissingColumns=True" in code


class TestSortConverter:
    def test_generate_code(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 5,
            "tool_type": "Sort",
            "annotation": "Sort",
            "configuration": {
                "fields": [
                    {"field": "Value", "order": "Descending"},
                    {"field": "Name", "order": "Ascending"},
                ],
            },
        }
        converter = SortConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("df_input", "df_sorted")

        assert ".orderBy(" in code
        assert ".desc()" in code
        assert ".asc()" in code


class TestTextInputConverter:
    def test_generate_code(self, mapper, expr_converter):
        tool_config = {
            "tool_id": 1,
            "tool_type": "TextInput",
            "annotation": "Test Data",
            "configuration": {
                "fields": [
                    {"name": "Name", "type": "V_WString"},
                    {"name": "Value", "type": "Int32"},
                ],
                "data": [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 20},
                ],
                "row_count": 2,
            },
        }
        converter = TextInputConverter(tool_config, mapper, expr_converter)
        code = converter.generate_code("", "df_textinput")

        assert "spark.createDataFrame(" in code
        assert "StructType" in code
        assert '"Alice"' in code
        assert '"Bob"' in code
