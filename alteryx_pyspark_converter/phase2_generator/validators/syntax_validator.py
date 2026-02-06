"""Validate PySpark syntax of generated code."""

import ast
import logging

logger = logging.getLogger(__name__)


class SyntaxValidator:
    """Validate that generated PySpark code is syntactically valid Python."""

    def validate(self, code: str) -> dict:
        """
        Validate Python syntax of generated code.

        Args:
            code: The generated Python code string.

        Returns:
            Validation result dictionary.
        """
        issues = []

        # Strip Databricks notebook magic commands for syntax checking
        clean_code = self._strip_magic_commands(code)

        # Check Python syntax
        try:
            ast.parse(clean_code)
        except SyntaxError as e:
            issues.append({
                "type": "syntax_error",
                "line": e.lineno,
                "offset": e.offset,
                "message": str(e.msg),
                "text": e.text,
            })

        # Check for common issues
        issues.extend(self._check_common_issues(code))

        return {
            "valid": len(issues) == 0,
            "issues": issues,
        }

    def _strip_magic_commands(self, code: str) -> str:
        """Remove Databricks magic commands that aren't valid Python."""
        lines = []
        for line in code.split("\n"):
            stripped = line.strip()
            if stripped.startswith("# MAGIC"):
                continue
            if stripped.startswith("# COMMAND ----------"):
                continue
            if stripped.startswith("# Databricks notebook source"):
                continue
            lines.append(line)
        return "\n".join(lines)

    def _check_common_issues(self, code: str) -> list[dict]:
        """Check for common issues in generated code."""
        issues = []

        # Check for placeholder values
        if "REPLACE_WITH_TABLE_NAME" in code:
            issues.append({
                "type": "placeholder",
                "message": "Contains placeholder table name that needs to be replaced",
            })

        if "df_unknown_" in code:
            issues.append({
                "type": "unresolved_input",
                "message": "Contains unresolved input DataFrame references",
            })

        # Check that imports are present
        if "from pyspark.sql" not in code and "import pyspark" not in code:
            issues.append({
                "type": "missing_import",
                "message": "PySpark imports may be missing",
            })

        return issues
