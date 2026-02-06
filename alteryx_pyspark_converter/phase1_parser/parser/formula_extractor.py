"""Extract and normalize Alteryx formula expressions."""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class FormulaExtractor:
    """
    Extract and normalize Alteryx formula expressions from tool configurations.

    This extractor handles the raw Alteryx expression syntax and produces
    clean, normalized expressions for the intermediate representation.
    """

    # Pattern to match column references: [ColumnName]
    COLUMN_REF_PATTERN = re.compile(r'\[([^\]]+)\]')

    # Pattern to match Alteryx function calls
    FUNCTION_PATTERN = re.compile(r'(\w+)\s*\(')

    # Known Alteryx functions for validation
    KNOWN_FUNCTIONS = {
        # String functions
        'Contains', 'StartsWith', 'EndsWith', 'Length', 'Lower', 'Upper',
        'Trim', 'TrimLeft', 'TrimRight', 'Substring', 'Left', 'Right',
        'FindString', 'ReplaceString', 'ReplaceChar', 'PadLeft', 'PadRight',
        'Reverse', 'CountWords', 'GetWord', 'TitleCase', 'Proper',
        'REGEX_Match', 'REGEX_Replace', 'REGEX_CountMatches',

        # Null handling
        'IsNull', 'IsEmpty', 'Null', 'IFNULL', 'IIF',

        # Type conversion
        'ToString', 'ToNumber', 'ToDate', 'BoolToInt', 'CharToInt',
        'IntToHex', 'HexToInt',

        # Date functions
        'DateTimeNow', 'DateTimeToday', 'DateTimeParse', 'DateTimeFormat',
        'DateTimeAdd', 'DateTimeDiff', 'DateTimeDay', 'DateTimeMonth',
        'DateTimeYear', 'DateTimeHour', 'DateTimeMinute', 'DateTimeSecond',
        'DateTimeTrim', 'DateTimeFirstOfMonth', 'DateTimeLastOfMonth',

        # Math functions
        'Abs', 'Ceil', 'Floor', 'Round', 'Mod', 'Pow', 'Sqrt',
        'Log', 'Log10', 'Exp', 'Sin', 'Cos', 'Tan', 'Asin', 'Acos', 'Atan',
        'RandInt', 'Rand', 'PI',

        # Aggregation (for Summarize)
        'Sum', 'Count', 'CountDistinct', 'Min', 'Max', 'Avg', 'First',
        'Last', 'Concat', 'Median', 'Mode', 'StdDev', 'Variance',

        # Conditional
        'IF', 'ELSEIF', 'ELSE', 'ENDIF', 'Switch', 'IIF',

        # Type checking
        'IsInteger', 'IsNumber', 'IsString',
    }

    def extract_column_references(self, expression: str) -> list[str]:
        """
        Extract all column references from an Alteryx expression.

        Args:
            expression: Alteryx formula expression.

        Returns:
            List of column names referenced (without brackets).
        """
        return self.COLUMN_REF_PATTERN.findall(expression)

    def extract_functions_used(self, expression: str) -> list[str]:
        """
        Extract all function names used in an expression.

        Args:
            expression: Alteryx formula expression.

        Returns:
            List of function names found.
        """
        matches = self.FUNCTION_PATTERN.findall(expression)
        # Filter out things that aren't actually functions (like IF/THEN/etc)
        keywords = {'IF', 'THEN', 'ELSEIF', 'ELSE', 'ENDIF', 'AND', 'OR', 'NOT', 'IN'}
        return [m for m in matches if m not in keywords or m in self.KNOWN_FUNCTIONS]

    def normalize_expression(self, expression: str) -> str:
        """
        Normalize an Alteryx expression for consistent processing.

        - Strips leading/trailing whitespace
        - Normalizes line breaks
        - Normalizes whitespace around operators

        Args:
            expression: Raw Alteryx expression.

        Returns:
            Normalized expression string.
        """
        if not expression:
            return ""

        # Normalize line breaks
        expr = expression.replace('\r\n', ' ').replace('\n', ' ')

        # Collapse multiple whitespace
        expr = re.sub(r'\s+', ' ', expr)

        # Strip
        expr = expr.strip()

        return expr

    def extract_if_branches(self, expression: str) -> list[dict]:
        """
        Extract IF/ELSEIF/ELSE branches from an Alteryx IF expression.

        Args:
            expression: Alteryx IF expression.

        Returns:
            List of dicts with 'condition' and 'value' keys.
        """
        branches = []
        expr = self.normalize_expression(expression)

        # Pattern for IF ... THEN ... ELSEIF ... THEN ... ELSE ... ENDIF
        # Use a simple state machine approach
        parts = re.split(
            r'\b(IF|THEN|ELSEIF|ELSE|ENDIF)\b',
            expr,
            flags=re.IGNORECASE,
        )

        current_condition = None
        state = "start"

        for part in parts:
            part = part.strip()
            upper = part.upper()

            if upper == "IF":
                state = "condition"
            elif upper == "ELSEIF":
                state = "condition"
            elif upper == "THEN":
                state = "value"
            elif upper == "ELSE":
                current_condition = None
                state = "else_value"
            elif upper == "ENDIF":
                break
            elif state == "condition" and part:
                current_condition = part
            elif state == "value" and part:
                branches.append({
                    "condition": current_condition,
                    "value": part,
                })
                current_condition = None
                state = "between"
            elif state == "else_value" and part:
                branches.append({
                    "condition": None,  # ELSE branch
                    "value": part,
                })

        return branches

    def extract_all_columns_from_tool(self, tool_config: dict) -> set[str]:
        """
        Extract all column names referenced by a tool's configuration.

        This looks at all expressions, field references, etc. in the
        tool configuration.

        Args:
            tool_config: Tool configuration dictionary.

        Returns:
            Set of column names.
        """
        columns: set[str] = set()

        # Extract from expressions in formulas
        formulas = tool_config.get("formulas", [])
        for formula in formulas:
            expr = formula.get("expression", "")
            columns.update(self.extract_column_references(expr))
            field = formula.get("field", "")
            if field:
                columns.add(field)

        # Extract from filter expression
        filter_expr = tool_config.get("expression", "")
        if filter_expr:
            columns.update(self.extract_column_references(filter_expr))

        # Extract from join keys
        for key_type in ("left_keys", "right_keys"):
            keys = tool_config.get(key_type, [])
            columns.update(keys)

        # Extract from group fields
        group_fields = tool_config.get("group_fields", [])
        columns.update(group_fields)

        # Extract from header/data fields
        for field_key in ("header_field", "data_field"):
            field = tool_config.get(field_key, "")
            if field:
                columns.add(field)

        # Extract from summarize fields
        sum_fields = tool_config.get("fields", [])
        for sf in sum_fields:
            if isinstance(sf, dict):
                field = sf.get("field", "")
                if field:
                    columns.add(field)
                name = sf.get("name", "")
                if name and name != "*Unknown":
                    columns.add(name)

        # Extract from sort fields
        sort_fields = tool_config.get("fields", [])
        for sf in sort_fields:
            if isinstance(sf, dict):
                field = sf.get("field", "")
                if field:
                    columns.add(field)

        # Extract from unique fields
        unique_fields = tool_config.get("unique_fields", [])
        columns.update(unique_fields)

        # Extract from select fields
        select_fields = tool_config.get("fields", [])
        for sf in select_fields:
            if isinstance(sf, dict):
                name = sf.get("name", "")
                if name and name != "*Unknown":
                    columns.add(name)
                rename = sf.get("rename")
                if rename:
                    columns.add(rename)

        return columns
