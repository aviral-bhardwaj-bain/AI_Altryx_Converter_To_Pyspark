"""Enumerations for Alteryx tool types, connection types, and data types."""

from enum import Enum


class ToolType(str, Enum):
    """Supported Alteryx tool types."""
    FILTER = "Filter"
    JOIN = "Join"
    FORMULA = "Formula"
    SELECT = "Select"
    CROSSTAB = "CrossTab"
    SUMMARIZE = "Summarize"
    UNION = "Union"
    SORT = "Sort"
    UNIQUE = "Unique"
    SAMPLE = "Sample"
    TEXT_INPUT = "TextInput"
    DB_INPUT = "DbInput"
    DB_OUTPUT = "DbOutput"
    BROWSE = "Browse"
    CONTAINER = "Container"
    COMMENT = "Comment"
    EXPLORER_BOX = "ExplorerBox"
    MACRO_INPUT = "MacroInput"
    MACRO_OUTPUT = "MacroOutput"

    # LockIn (In-Database) tools
    LOCKIN_FILTER = "LockInFilter"
    LOCKIN_SELECT = "LockInSelect"
    LOCKIN_FORMULA = "LockInFormula"
    LOCKIN_INPUT = "LockInInput"
    LOCKIN_JOIN = "LockInJoin"
    LOCKIN_SUMMARIZE = "LockInSummarize"
    LOCKIN_UNION = "LockInUnion"
    LOCKIN_CROSSTAB = "LockInCrossTab"
    LOCKIN_SAMPLE = "LockInSample"
    LOCKIN_SORT = "LockInSort"
    LOCKIN_UNIQUE = "LockInUnique"
    LOCKIN_WRITE = "LockInWrite"

    # Additional tools
    MULTI_ROW_FORMULA = "MultiRowFormula"
    MULTI_FIELD_FORMULA = "MultiFieldFormula"
    GENERATE_ROWS = "GenerateRows"
    RECORD_ID = "RecordID"
    RUNNING_TOTAL = "RunningTotal"
    TRANSPOSE = "Transpose"
    APPEND_FIELDS = "AppendFields"
    FIND_REPLACE = "FindReplace"
    REGEX = "Regex"
    DATE_TIME_PARSE = "DateTimeParse"
    AUTO_FIELD = "AutoField"
    IMPUTATION = "Imputation"
    TILE = "Tile"
    WEIGHTED_AVERAGE = "WeightedAverage"

    UNKNOWN = "Unknown"


class ConnectionType(str, Enum):
    """Types of connections between tools."""
    OUTPUT = "Output"
    TRUE = "True"
    FALSE = "False"
    JOIN = "Join"
    LEFT = "Left"
    RIGHT = "Right"
    INPUT = "Input"
    UNKNOWN = "Unknown"


class FilterMode(str, Enum):
    """Filter tool modes."""
    SIMPLE = "Simple"
    CUSTOM = "Custom"


class JoinType(str, Enum):
    """Join output types."""
    INNER = "Join"
    LEFT_UNMATCHED = "Left"
    RIGHT_UNMATCHED = "Right"


class SummarizeAction(str, Enum):
    """Summarize tool aggregation actions."""
    GROUP_BY = "GroupBy"
    SUM = "Sum"
    COUNT = "Count"
    COUNT_DISTINCT = "CountDistinct"
    COUNT_NON_NULL = "CountNonNull"
    MIN = "Min"
    MAX = "Max"
    AVG = "Avg"
    FIRST = "First"
    LAST = "Last"
    CONCAT = "Concat"
    CONCATENATE = "Concatenate"
    MEDIAN = "Median"
    MODE = "Mode"
    STANDARD_DEVIATION = "StdDev"
    VARIANCE = "Variance"
    PERCENT_OF_TOTAL = "PercentOfTotal"


class CrossTabMethod(str, Enum):
    """CrossTab aggregation methods."""
    CONCATENATE = "Concatenate"
    SUM = "Sum"
    COUNT = "Count"
    COUNT_DISTINCT = "CountDistinct"
    FIRST = "First"
    LAST = "Last"
    MIN = "Min"
    MAX = "Max"
    AVG = "Avg"


class AlteryxDataType(str, Enum):
    """Alteryx field data types."""
    BOOL = "Bool"
    BYTE = "Byte"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    FIXED_DECIMAL = "FixedDecimal"
    FLOAT = "Float"
    DOUBLE = "Double"
    STRING = "String"
    WSTRING = "WString"
    V_STRING = "V_String"
    V_WSTRING = "V_WString"
    DATE = "Date"
    TIME = "Time"
    DATETIME = "DateTime"
    BLOB = "Blob"
    SPATIAL_OBJ = "SpatialObj"
    UNKNOWN = "Unknown"


# Plugin name to ToolType mapping
PLUGIN_TYPE_MAP: dict[str, ToolType] = {
    "AlteryxBasePluginsGui.Filter.Filter": ToolType.FILTER,
    "AlteryxBasePluginsGui.Join.Join": ToolType.JOIN,
    "AlteryxBasePluginsGui.Formula.Formula": ToolType.FORMULA,
    "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect": ToolType.SELECT,
    "AlteryxBasePluginsGui.CrossTab.CrossTab": ToolType.CROSSTAB,
    "AlteryxBasePluginsGui.Summarize.Summarize": ToolType.SUMMARIZE,
    "AlteryxBasePluginsGui.Union.Union": ToolType.UNION,
    "AlteryxBasePluginsGui.Sort.Sort": ToolType.SORT,
    "AlteryxBasePluginsGui.Unique.Unique": ToolType.UNIQUE,
    "AlteryxBasePluginsGui.Sample.Sample": ToolType.SAMPLE,
    "AlteryxBasePluginsGui.TextInput.TextInput": ToolType.TEXT_INPUT,
    "AlteryxBasePluginsGui.DbFileInput.DbFileInput": ToolType.DB_INPUT,
    "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput": ToolType.DB_OUTPUT,
    "AlteryxBasePluginsGui.Browse.Browse": ToolType.BROWSE,
    "AlteryxBasePluginsGui.Comment.Comment": ToolType.COMMENT,
    "AlteryxBasePluginsGui.ExplorerBox.ExplorerBox": ToolType.EXPLORER_BOX,
    "AlteryxBasePluginsGui.MacroInput.MacroInput": ToolType.MACRO_INPUT,
    "AlteryxBasePluginsGui.MacroOutput.MacroOutput": ToolType.MACRO_OUTPUT,
    "AlteryxBasePluginsGui.MultiRowFormula.MultiRowFormula": ToolType.MULTI_ROW_FORMULA,
    "AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula": ToolType.MULTI_FIELD_FORMULA,
    "AlteryxBasePluginsGui.GenerateRows.GenerateRows": ToolType.GENERATE_ROWS,
    "AlteryxBasePluginsGui.RecordID.RecordID": ToolType.RECORD_ID,
    "AlteryxBasePluginsGui.RunningTotal.RunningTotal": ToolType.RUNNING_TOTAL,
    "AlteryxBasePluginsGui.Transpose.Transpose": ToolType.TRANSPOSE,
    "AlteryxBasePluginsGui.AppendFields.AppendFields": ToolType.APPEND_FIELDS,
    "AlteryxBasePluginsGui.FindReplace.FindReplace": ToolType.FIND_REPLACE,
    "AlteryxBasePluginsGui.RegEx.RegEx": ToolType.REGEX,
    "AlteryxBasePluginsGui.DateTimeParse.DateTimeParse": ToolType.DATE_TIME_PARSE,
    "AlteryxBasePluginsGui.AutoField.AutoField": ToolType.AUTO_FIELD,
    "AlteryxBasePluginsGui.Imputation.Imputation": ToolType.IMPUTATION,
    "AlteryxBasePluginsGui.Tile.Tile": ToolType.TILE,
    "AlteryxBasePluginsGui.WeightedAverage.WeightedAverage": ToolType.WEIGHTED_AVERAGE,
    "AlteryxGuiToolkit.ToolContainer.ToolContainer": ToolType.CONTAINER,
    # LockIn (In-Database) tools
    "LockInGui.LockInFilter.LockInFilter": ToolType.LOCKIN_FILTER,
    "LockInGui.LockInSelect.LockInSelect": ToolType.LOCKIN_SELECT,
    "LockInGui.LockInFormula.LockInFormula": ToolType.LOCKIN_FORMULA,
    "LockInGui.LockInInput.LockInInput": ToolType.LOCKIN_INPUT,
    "LockInGui.LockInJoin.LockInJoin": ToolType.LOCKIN_JOIN,
    "LockInGui.LockInSummarize.LockInSummarize": ToolType.LOCKIN_SUMMARIZE,
    "LockInGui.LockInUnion.LockInUnion": ToolType.LOCKIN_UNION,
    "LockInGui.LockInCrossTab.LockInCrossTab": ToolType.LOCKIN_CROSSTAB,
    "LockInGui.LockInSample.LockInSample": ToolType.LOCKIN_SAMPLE,
    "LockInGui.LockInSort.LockInSort": ToolType.LOCKIN_SORT,
    "LockInGui.LockInUnique.LockInUnique": ToolType.LOCKIN_UNIQUE,
    "LockInGui.LockInWrite.LockInWrite": ToolType.LOCKIN_WRITE,
}
