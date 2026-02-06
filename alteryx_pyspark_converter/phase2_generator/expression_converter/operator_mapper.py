"""Map Alteryx operators to PySpark equivalents."""

# Comparison operators
COMPARISON_OPERATORS: dict[str, str] = {
    "=": "==",
    "<>": "!=",
    "!=": "!=",
    ">": ">",
    "<": "<",
    ">=": ">=",
    "<=": "<=",
}

# Logical operators
LOGICAL_OPERATORS: dict[str, str] = {
    "AND": "&",
    "OR": "|",
    "NOT": "~",
    "and": "&",
    "or": "|",
    "not": "~",
}

# Arithmetic operators (same in both)
ARITHMETIC_OPERATORS: dict[str, str] = {
    "+": "+",
    "-": "-",
    "*": "*",
    "/": "/",
    "%": "%",
}

# String concatenation
STRING_OPERATORS: dict[str, str] = {
    "+": "concat",  # String concatenation in Alteryx uses +
}

# IN operator
SET_OPERATORS: dict[str, str] = {
    "IN": "isin",
    "NOT IN": "~isin",
}

# All operators combined
ALL_OPERATORS: dict[str, str] = {
    **COMPARISON_OPERATORS,
    **LOGICAL_OPERATORS,
    **ARITHMETIC_OPERATORS,
}
