"""Tests for the Phase 2 expression converter."""

import pytest

from alteryx_pyspark_converter.phase2_generator.config.column_mapping import ColumnMapper
from alteryx_pyspark_converter.phase2_generator.expression_converter.alteryx_to_pyspark import (
    AlteryxExpressionConverter,
)


class TestAlteryxExpressionConverter:
    """Tests for AlteryxExpressionConverter."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mapper = ColumnMapper({
            "nps_type": "typ_nps",
            "question_var": "id_survey_question",
            "answer": "txt_answer",
            "Name": "full_name",
            "Value": "num_value",
        })
        self.converter = AlteryxExpressionConverter(self.mapper)

    def test_simple_equality(self):
        """Test simple equality expression."""
        result = self.converter.convert('[nps_type] = "Provider NPS"')
        assert 'F.col("typ_nps")' in result
        assert '==' in result
        assert '"Provider NPS"' in result

    def test_column_reference_mapping(self):
        """Test that column references are mapped correctly."""
        result = self.converter.convert_column_reference("[nps_type]")
        assert result == 'F.col("typ_nps")'

    def test_unmapped_column_passthrough(self):
        """Test that unmapped columns pass through."""
        result = self.converter.convert_column_reference("[some_other_col]")
        assert result == 'F.col("some_other_col")'

    def test_inequality(self):
        """Test inequality operator conversion."""
        result = self.converter.convert('[Value] <> 0')
        assert "!=" in result

    def test_and_operator(self):
        """Test AND operator conversion."""
        result = self.converter.convert('[Value] > 10 AND [Value] < 50')
        assert "&" in result

    def test_or_operator(self):
        """Test OR operator conversion."""
        result = self.converter.convert('[Value] > 10 OR [Value] < 5')
        assert "|" in result

    def test_if_expression(self):
        """Test IF/ELSE/ENDIF conversion."""
        result = self.converter.convert(
            'IF [Value] > 10 THEN "High" ELSE "Low" ENDIF'
        )
        assert "F.when" in result
        assert ".otherwise" in result
        assert '"High"' in result or "High" in result
        assert '"Low"' in result or "Low" in result

    def test_if_elseif_expression(self):
        """Test IF/ELSEIF/ELSE/ENDIF conversion."""
        result = self.converter.convert(
            'IF [Value] > 20 THEN "High" '
            'ELSEIF [Value] > 10 THEN "Medium" '
            'ELSE "Low" ENDIF'
        )
        assert "F.when" in result
        assert ".when" in result  # ELSEIF becomes .when
        assert ".otherwise" in result

    def test_contains_function(self):
        """Test Contains() function conversion."""
        result = self.converter.convert('Contains([question_var], "Q9_")')
        assert "contains" in result
        assert "id_survey_question" in result

    def test_empty_expression(self):
        """Test empty expression handling."""
        result = self.converter.convert("")
        assert result == ""

    def test_null_expression(self):
        """Test Null() function conversion."""
        result = self.converter.convert("Null()")
        assert "F.lit(None)" in result

    def test_numeric_comparison(self):
        """Test numeric comparison."""
        result = self.converter.convert("[Value] > 100")
        assert 'F.col("num_value")' in result
        assert "> 100" in result

    def test_multiple_column_refs(self):
        """Test expression with multiple column references."""
        result = self.converter.convert("[Name] + [Value]")
        assert 'F.col("full_name")' in result
        assert 'F.col("num_value")' in result


class TestColumnMapper:
    """Tests for ColumnMapper."""

    def setup_method(self):
        self.mapper = ColumnMapper({
            "old_name": "new_name",
            "source_col": "target_col",
        })

    def test_to_databricks(self):
        """Test forward mapping."""
        assert self.mapper.to_databricks("old_name") == "new_name"
        assert self.mapper.to_databricks("unmapped") == "unmapped"

    def test_to_alteryx(self):
        """Test reverse mapping."""
        assert self.mapper.to_alteryx("new_name") == "old_name"
        assert self.mapper.to_alteryx("unmapped") == "unmapped"

    def test_convert_column_list(self):
        """Test list conversion."""
        result = self.mapper.convert_column_list(["old_name", "other", "source_col"])
        assert result == ["new_name", "other", "target_col"]

    def test_get_unmapped(self):
        """Test getting unmapped columns."""
        unmapped = self.mapper.get_unmapped_columns(
            ["old_name", "other", "source_col"]
        )
        assert unmapped == ["other"]

    def test_validate_against_schema(self):
        """Test schema validation."""
        result = self.mapper.validate_against_schema(
            columns=["old_name", "other"],
            available_columns=["new_name", "something_else"],
        )
        assert "old_name" in result["valid"]
        assert len(result["unmapped"]) == 1
