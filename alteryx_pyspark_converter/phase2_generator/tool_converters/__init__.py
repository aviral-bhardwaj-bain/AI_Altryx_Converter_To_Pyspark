"""Per-tool PySpark converters."""

from .base_converter import BaseToolConverter
from .filter_converter import FilterConverter
from .join_converter import JoinConverter
from .formula_converter import FormulaConverter
from .select_converter import SelectConverter
from .crosstab_converter import CrossTabConverter
from .summarize_converter import SummarizeConverter
from .union_converter import UnionConverter
from .sort_converter import SortConverter
from .unique_converter import UniqueConverter
from .sample_converter import SampleConverter
from .textinput_converter import TextInputConverter
from .lockin_converter import (
    LockInInputConverter,
    LockInWriteConverter,
    LockInFilterConverter,
    LockInJoinConverter,
    LockInFormulaConverter,
    LockInSelectConverter,
    LockInCrossTabConverter,
    LockInSummarizeConverter,
    LockInUnionConverter,
    LockInSortConverter,
    LockInUniqueConverter,
    LockInSampleConverter,
)

# Map tool type strings to converter classes
CONVERTER_MAP: dict[str, type[BaseToolConverter]] = {
    "Filter": FilterConverter,
    "Join": JoinConverter,
    "Formula": FormulaConverter,
    "Select": SelectConverter,
    "CrossTab": CrossTabConverter,
    "Summarize": SummarizeConverter,
    "Union": UnionConverter,
    "Sort": SortConverter,
    "Unique": UniqueConverter,
    "Sample": SampleConverter,
    "TextInput": TextInputConverter,
    # LockIn tools
    "LockInFilter": LockInFilterConverter,
    "LockInJoin": LockInJoinConverter,
    "LockInFormula": LockInFormulaConverter,
    "LockInSelect": LockInSelectConverter,
    "LockInCrossTab": LockInCrossTabConverter,
    "LockInSummarize": LockInSummarizeConverter,
    "LockInUnion": LockInUnionConverter,
    "LockInSort": LockInSortConverter,
    "LockInUnique": LockInUniqueConverter,
    "LockInSample": LockInSampleConverter,
    "LockInInput": LockInInputConverter,
    "LockInWrite": LockInWriteConverter,
}

__all__ = [
    "BaseToolConverter",
    "CONVERTER_MAP",
    "FilterConverter",
    "JoinConverter",
    "FormulaConverter",
    "SelectConverter",
    "CrossTabConverter",
    "SummarizeConverter",
    "UnionConverter",
    "SortConverter",
    "UniqueConverter",
    "SampleConverter",
    "TextInputConverter",
    "LockInInputConverter",
    "LockInWriteConverter",
]
