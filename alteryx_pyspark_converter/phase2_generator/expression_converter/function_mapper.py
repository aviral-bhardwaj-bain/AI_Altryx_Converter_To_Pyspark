"""Map Alteryx functions to PySpark equivalents."""


# Mapping: Alteryx function name -> (PySpark function, template)
# Template placeholders: {0}, {1}, etc. for arguments
FUNCTION_MAP: dict[str, tuple[str, str]] = {
    # =========================================================================
    # String functions
    # =========================================================================
    "Contains": ("contains", 'F.col("{0}").contains({1})'),
    "StartsWith": ("startswith", 'F.col("{0}").startswith({1})'),
    "EndsWith": ("endswith", 'F.col("{0}").endswith({1})'),
    "Length": ("length", 'F.length(F.col("{0}"))'),
    "Lower": ("lower", 'F.lower(F.col("{0}"))'),
    "Upper": ("upper", 'F.upper(F.col("{0}"))'),
    "Trim": ("trim", 'F.trim(F.col("{0}"))'),
    "TrimLeft": ("ltrim", 'F.ltrim(F.col("{0}"))'),
    "TrimRight": ("rtrim", 'F.rtrim(F.col("{0}"))'),
    "Substring": ("substring", 'F.substring(F.col("{0}"), {1}, {2})'),
    "Left": ("substring", 'F.substring(F.col("{0}"), 1, {1})'),
    "Right": ("substring", 'F.substring(F.col("{0}"), -({1}), {1})'),
    "FindString": ("instr", 'F.instr(F.col("{0}"), {1}) - 1'),
    "ReplaceString": ("regexp_replace", 'F.regexp_replace(F.col("{0}"), {1}, {2})'),
    "ReplaceChar": ("translate", 'F.translate(F.col("{0}"), {1}, {2})'),
    "PadLeft": ("lpad", 'F.lpad(F.col("{0}"), {1}, {2})'),
    "PadRight": ("rpad", 'F.rpad(F.col("{0}"), {1}, {2})'),
    "Reverse": ("reverse", 'F.reverse(F.col("{0}"))'),
    "TitleCase": ("initcap", 'F.initcap(F.col("{0}"))'),
    "Proper": ("initcap", 'F.initcap(F.col("{0}"))'),
    "CountWords": ("size_split", 'F.size(F.split(F.col("{0}"), "\\\\s+"))'),
    "GetWord": ("split_getitem", 'F.split(F.col("{0}"), "\\\\s+").getItem({1})'),
    "REGEX_Match": ("rlike", 'F.col("{0}").rlike({1})'),
    "REGEX_Replace": ("regexp_replace", 'F.regexp_replace(F.col("{0}"), {1}, {2})'),

    # =========================================================================
    # Null handling
    # =========================================================================
    "IsNull": ("isNull", 'F.col("{0}").isNull()'),
    "IsEmpty": (
        "isNull_or_empty",
        '(F.col("{0}").isNull() | (F.trim(F.col("{0}")) == ""))',
    ),
    "Null": ("lit_null", "F.lit(None)"),
    "IFNULL": ("coalesce", 'F.coalesce(F.col("{0}"), {1})'),
    "IIF": ("when_iif", 'F.when({0}, {1}).otherwise({2})'),

    # =========================================================================
    # Type conversion
    # =========================================================================
    "ToString": ("cast_string", 'F.col("{0}").cast("string")'),
    "ToNumber": ("cast_double", 'F.col("{0}").cast("double")'),
    "ToDate": ("to_date", 'F.to_date(F.col("{0}"), {1})'),
    "BoolToInt": ("cast_int", 'F.col("{0}").cast("integer")'),
    "CharToInt": ("ascii", 'F.ascii(F.col("{0}"))'),

    # =========================================================================
    # Date functions
    # =========================================================================
    "DateTimeNow": ("current_timestamp", "F.current_timestamp()"),
    "DateTimeToday": ("current_date", "F.current_date()"),
    "DateTimeParse": ("to_timestamp", 'F.to_timestamp(F.col("{0}"), {1})'),
    "DateTimeFormat": ("date_format", 'F.date_format(F.col("{0}"), {1})'),
    "DateTimeAdd": ("date_add", 'F.expr("dateadd({2}, {1}, {0})")'),
    "DateTimeDiff": ("datediff", 'F.datediff(F.col("{0}"), F.col("{1}"))'),
    "DateTimeDay": ("dayofmonth", 'F.dayofmonth(F.col("{0}"))'),
    "DateTimeMonth": ("month", 'F.month(F.col("{0}"))'),
    "DateTimeYear": ("year", 'F.year(F.col("{0}"))'),
    "DateTimeHour": ("hour", 'F.hour(F.col("{0}"))'),
    "DateTimeMinute": ("minute", 'F.minute(F.col("{0}"))'),
    "DateTimeSecond": ("second", 'F.second(F.col("{0}"))'),
    "DateTimeTrim": ("date_trunc", 'F.date_trunc({1}, F.col("{0}"))'),
    "DateTimeFirstOfMonth": (
        "trunc",
        'F.trunc(F.col("{0}"), "month")',
    ),
    "DateTimeLastOfMonth": ("last_day", 'F.last_day(F.col("{0}"))'),

    # =========================================================================
    # Math functions
    # =========================================================================
    "Abs": ("abs", 'F.abs(F.col("{0}"))'),
    "Ceil": ("ceil", 'F.ceil(F.col("{0}"))'),
    "Floor": ("floor", 'F.floor(F.col("{0}"))'),
    "Round": ("round", 'F.round(F.col("{0}"), {1})'),
    "Mod": ("mod", '(F.col("{0}") % {1})'),
    "Pow": ("pow", 'F.pow(F.col("{0}"), {1})'),
    "Sqrt": ("sqrt", 'F.sqrt(F.col("{0}"))'),
    "Log": ("log", 'F.log(F.col("{0}"))'),
    "Log10": ("log10", 'F.log10(F.col("{0}"))'),
    "Exp": ("exp", 'F.exp(F.col("{0}"))'),
    "PI": ("pi", "F.lit(3.141592653589793)"),
    "RandInt": ("rand_int", 'F.floor(F.rand() * ({1} - {0}) + {0})'),
    "Rand": ("rand", "F.rand()"),

    # =========================================================================
    # Aggregation functions (used in Summarize tool context)
    # =========================================================================
    "Sum": ("sum", 'F.sum(F.col("{0}"))'),
    "Count": ("count", 'F.count(F.col("{0}"))'),
    "CountDistinct": ("countDistinct", 'F.countDistinct(F.col("{0}"))'),
    "Min": ("min", 'F.min(F.col("{0}"))'),
    "Max": ("max", 'F.max(F.col("{0}"))'),
    "Avg": ("avg", 'F.avg(F.col("{0}"))'),
    "First": ("first", 'F.first(F.col("{0}"))'),
    "Last": ("last", 'F.last(F.col("{0}"))'),
    "Concat": ("concat_ws", 'F.concat_ws({0}, F.col("{1}"))'),
    "Median": ("median", 'F.percentile_approx(F.col("{0}"), 0.5)'),
}

# Alteryx date format -> PySpark/Java date format
DATE_FORMAT_MAP: dict[str, str] = {
    "%Y": "yyyy",
    "%m": "MM",
    "%d": "dd",
    "%H": "HH",
    "%M": "mm",
    "%S": "ss",
    "%y": "yy",
    "%b": "MMM",
    "%B": "MMMM",
    "%p": "a",
    "%I": "hh",
    "%j": "DDD",
    "%A": "EEEE",
    "%a": "EEE",
}


def convert_date_format(alteryx_format: str) -> str:
    """Convert an Alteryx date format string to a Java/PySpark format string."""
    result = alteryx_format
    for alt_fmt, spark_fmt in DATE_FORMAT_MAP.items():
        result = result.replace(alt_fmt, spark_fmt)
    return result
