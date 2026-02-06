"""
Context Builder
================
Transforms parsed Alteryx workflow data into a structured prompt context
that Claude AI can use to generate accurate PySpark code.
"""

import xml.etree.ElementTree as ET
from typing import Optional
from .models import Workflow, Container, Tool, Connection


def build_container_prompt(
    container: Container,
    context: dict,
    workflow: Workflow,
    source_tables_config: Optional[dict] = None,
) -> str:
    """
    Build a detailed prompt describing a container's full logic for Claude AI.

    Returns a structured text description including:
    - Container name and purpose
    - All tools with their configurations
    - Data flow (connections) in execution order
    - External inputs and outputs
    - Inline data (TextInput tools)
    - Sub-container details
    """
    parts = []

    # ── Header ───────────────────────────────────────────────────────
    parts.append(f"# Container: {container.name}")
    parts.append(f"# Container ToolID: {container.tool_id}")
    parts.append("")

    # ── Sub-containers ───────────────────────────────────────────────
    sub_containers = context.get("sub_containers", [])
    if sub_containers:
        parts.append("## Sub-Containers (nested groups within this container):")
        for sc in sub_containers:
            parts.append(f"  - '{sc.name}' (ToolID={sc.tool_id})")
        parts.append("")

    # ── External inputs ──────────────────────────────────────────────
    external_inputs = context.get("external_inputs", [])
    source_tools = context.get("source_tools", {})
    if external_inputs:
        parts.append("## External Inputs (data flowing INTO this container):")
        for conn in external_inputs:
            src_info = source_tools.get(conn.origin_tool_id, {})
            src_desc = (
                f"Tool {conn.origin_tool_id} "
                f"({src_info.get('type', '?')}, "
                f"annotation='{src_info.get('annotation', '')}', "
                f"from container='{src_info.get('container', 'Root')}')"
            )
            parts.append(
                f"  - {src_desc} "
                f"--[{conn.origin_connection}]--> "
                f"Tool {conn.dest_tool_id} [{conn.dest_connection}]"
            )
        parts.append("")

    # ── External outputs ─────────────────────────────────────────────
    external_outputs = context.get("external_outputs", [])
    if external_outputs:
        parts.append("## External Outputs (data flowing OUT of this container):")
        for conn in external_outputs:
            dest_tool = workflow.get_tool(conn.dest_tool_id)
            dest_desc = ""
            if dest_tool:
                dest_container = workflow._find_tool_container_name(dest_tool.tool_id)
                dest_desc = (
                    f"Tool {conn.dest_tool_id} "
                    f"({dest_tool.tool_type}, "
                    f"'{dest_tool.annotation}', "
                    f"container='{dest_container}')"
                )
            else:
                dest_desc = f"Tool {conn.dest_tool_id}"
            parts.append(
                f"  - Tool {conn.origin_tool_id} "
                f"--[{conn.origin_connection}]--> {dest_desc}"
            )
        parts.append("")

    # ── Internal connections (data flow) ─────────────────────────────
    internal_connections = context.get("internal_connections", [])
    if internal_connections:
        parts.append("## Internal Data Flow (connections between tools):")
        for conn in internal_connections:
            parts.append(
                f"  Tool {conn.origin_tool_id} "
                f"--[{conn.origin_connection}]--> "
                f"Tool {conn.dest_tool_id} [{conn.dest_connection}]"
            )
        parts.append("")

    # ── Tools detail ─────────────────────────────────────────────────
    tools = context.get("tools", [])
    # Sort by position (left to right, top to bottom) for logical ordering
    tools_sorted = sorted(tools, key=lambda t: (t.position.get("x", 0), t.position.get("y", 0)))

    parts.append("## Tools (detailed configuration):")
    parts.append("")

    for tool in tools_sorted:
        parts.append(f"### Tool ID={tool.tool_id} | Type={tool.tool_type} | Plugin={tool.plugin}")
        if tool.annotation:
            parts.append(f"    Annotation: {tool.annotation}")
        if tool.container_id and tool.container_id != container.tool_id:
            # This tool is in a sub-container
            sub = workflow.get_container(tool.container_id)
            if sub:
                parts.append(f"    Inside sub-container: '{sub.name}'")

        # Configuration XML
        if tool.configuration_xml:
            parts.append(f"    Configuration XML:")
            # Indent the XML for readability
            for line in tool.configuration_xml.split("\n"):
                parts.append(f"      {line}")

        parts.append("")

    # ── Text Input inline data ───────────────────────────────────────
    text_input_data = context.get("text_input_data", {})
    if text_input_data:
        parts.append("## Inline Data (from TextInput tools):")
        for tid, rows in text_input_data.items():
            parts.append(f"  TextInput Tool {tid}:")
            if rows:
                # Show headers
                headers = list(rows[0].keys())
                parts.append(f"    Columns: {headers}")
                parts.append(f"    Rows ({len(rows)} total):")
                for row in rows:
                    parts.append(f"      {row}")
            parts.append("")

    # ── Source tables config ─────────────────────────────────────────
    if source_tables_config:
        parts.append("## Source Table Mapping (Alteryx → Databricks):")
        for key, value in source_tables_config.items():
            parts.append(f"  {key} → {value}")
        parts.append("")

    return "\n".join(parts)


def build_system_prompt() -> str:
    """Build the system prompt for Claude AI code generation."""
    return """You are an expert data engineer converting Alteryx workflows to production-ready PySpark code for Databricks.

## Your Task
Given a detailed description of an Alteryx container (a group of connected tools), generate a complete, correct PySpark script that replicates the exact same data transformation logic.

## Critical Rules
1. **Accuracy over everything**: Every filter, join, formula, select, and connection must be faithfully translated. Do NOT skip any tool or simplify logic.
2. **Follow the data flow**: Use the internal connections to determine execution order. Track which tool's output feeds into which tool's input, including multi-output tools (Filter True/False, Join Join/Left/Right).
3. **Handle all Alteryx tool types correctly**:
   - **Filter**: Creates two outputs (True and False). Use `.filter(condition)` and `.filter(~condition)`.
   - **Select**: Column selection, renaming, and reordering. Deselected columns must be dropped.
   - **Formula**: `.withColumn()` with proper expression conversion.
   - **Join**: `.join()` with proper join keys. Creates three outputs: Join (matched), Left (unmatched left), Right (unmatched right). Drop duplicate columns per the SelectConfiguration.
   - **Union**: `.unionByName(allowMissingColumns=True)`.
   - **Summarize**: `.groupBy().agg()` with proper aggregation functions.
   - **CrossTab**: `.groupBy().pivot().agg()`.
   - **TextInput**: `spark.createDataFrame()` with the inline data.
   - **LockInFilter/LockInStreamOut**: These are In-Database filter/stream tools; treat as filter + passthrough.
   - **Browse**: `display()` or comment out.
   - **Sort**: `.orderBy()`.
   - **Unique**: `.dropDuplicates()`.
4. **Alteryx expression conversion**:
   - `[ColumnName]` → `F.col("ColumnName")`
   - `IF...THEN...ELSEIF...ELSE...ENDIF` → `F.when(...).when(...).otherwise(...)`
   - `Contains([field], "text")` → `F.col("field").contains("text")`
   - `!Contains(...)` → `~F.col("field").contains("text")`
   - `IFNULL([x], default)` → `F.coalesce(F.col("x"), F.lit(default))`
   - `Null()` → `F.lit(None)`
   - `ToString([x])` → `F.col("x").cast("string")`
   - String concatenation with `+` → `F.concat()`
   - `Upper/Lower/Trim/Left/Right` → `F.upper/F.lower/F.trim/F.substring`
5. **External inputs**: These are DataFrames coming from upstream containers or source tables. Define them as `spark.table("your_catalog.your_schema.table_name")` with clear TODO comments showing what they represent.
6. **Variable naming**: Use descriptive DataFrame names based on what the tool does, not just `df_123`. E.g., `nps_filtered`, `provider_joined`, etc.
7. **Comments**: Add clear section headers and comments explaining each step. Reference the original Tool IDs for traceability.

## Output Format
Generate a single, complete Python file with:
1. Module docstring explaining the container's purpose
2. Imports
3. Source table definitions (with placeholder catalog/schema)
4. Step-by-step transformations following the data flow
5. Final output write/temp view creation

## Important
- Use `from pyspark.sql import functions as F` consistently
- Use `from pyspark.sql.types import *` for schema definitions
- NEVER use pandas - always PySpark DataFrames
- Handle null values properly with `F.coalesce()` or `F.when().isNull()`
- For CrossTab/Pivot, specify pivot values explicitly when the data shows them
- For Join SelectConfiguration: when a Right_ field has `selected="False"`, that column should be dropped after the join
- When a field is renamed (e.g., `rename="new_name"`), apply `.withColumnRenamed()`
"""
