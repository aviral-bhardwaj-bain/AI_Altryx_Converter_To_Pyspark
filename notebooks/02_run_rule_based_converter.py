# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx to PySpark - Rule-Based Converter (Phase 1 + Phase 2)
# MAGIC
# MAGIC Converts Alteryx `.yxmd` workflows into Databricks notebooks using a **deterministic, rule-based** approach.
# MAGIC No AI API calls required — uses tool-specific converters and expression mapping.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. **Phase 1 (Parser):** Parses the `.yxmd` XML into an intermediate JSON representation
# MAGIC 2. **Phase 2 (Generator):** Converts the JSON + user config into a PySpark Databricks notebook
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Run `00_setup` notebook first to install dependencies
# MAGIC 2. Upload the full project repo to your Databricks Workspace
# MAGIC 3. Upload your `.yxmd` file to a Databricks Volume or DBFS
# MAGIC 4. Create a config YAML (see `examples/sample_config.yaml`)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

dbutils.widgets.text(
    "workflow_path",
    "",
    "1. Workflow File Path (.yxmd)"
)

dbutils.widgets.text(
    "config_path",
    "",
    "2. Config YAML Path"
)

dbutils.widgets.text(
    "output_dir",
    "/Workspace/Users/shared/alteryx_converter/output",
    "3. Output Directory"
)

dbutils.widgets.text(
    "project_root",
    "/Workspace/Users/shared/alteryx_converter/AI_Altryx_Converter_To_Pyspark",
    "4. Project Root Path (where repo is uploaded)"
)

dbutils.widgets.dropdown(
    "run_mode",
    "full_convert",
    ["full_convert", "parse_only", "generate_only", "list_containers", "show_columns"],
    "5. Run Mode"
)

dbutils.widgets.text(
    "container_name",
    "",
    "6. Container Name (for show_columns mode)"
)

dbutils.widgets.dropdown(
    "skip_validation",
    "false",
    ["false", "true"],
    "7. Skip Syntax Validation"
)

# COMMAND ----------

WORKFLOW_PATH = dbutils.widgets.get("workflow_path")
CONFIG_PATH = dbutils.widgets.get("config_path")
OUTPUT_DIR = dbutils.widgets.get("output_dir")
PROJECT_ROOT = dbutils.widgets.get("project_root")
RUN_MODE = dbutils.widgets.get("run_mode")
CONTAINER_NAME = dbutils.widgets.get("container_name").strip() or None
SKIP_VALIDATION = dbutils.widgets.get("skip_validation") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Set Up Python Path

# COMMAND ----------

import sys
import os

# Add the project root to Python path so imports work
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print(f"Project root: {PROJECT_ROOT}")
print(f"Python path configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate Configuration

# COMMAND ----------

errors = []

if RUN_MODE in ("full_convert", "parse_only") and not WORKFLOW_PATH:
    errors.append("Workflow file path is required for this run mode.")

if RUN_MODE in ("full_convert", "generate_only") and not CONFIG_PATH:
    errors.append("Config YAML path is required for this run mode.")

if errors:
    for err in errors:
        print(f"CONFIG ERROR: {err}")
    raise ValueError("Configuration validation failed.")

print("Configuration validated.")
print(f"  Workflow:   {WORKFLOW_PATH}")
print(f"  Config:     {CONFIG_PATH}")
print(f"  Output:     {OUTPUT_DIR}")
print(f"  Mode:       {RUN_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Import Converter Modules

# COMMAND ----------

try:
    from alteryx_pyspark_converter.phase1_parser.parser.workflow_parser import WorkflowParser
    from alteryx_pyspark_converter.phase1_parser.output.json_writer import JSONWriter
    from alteryx_pyspark_converter.phase2_generator.generator.notebook_generator import NotebookGenerator
    from alteryx_pyspark_converter.phase2_generator.validators.syntax_validator import SyntaxValidator
    print("All converter modules imported successfully.")
except ImportError as e:
    print(f"ERROR: Could not import converter modules: {e}")
    print(f"Make sure the project is uploaded to: {PROJECT_ROOT}")
    print("The directory should contain the 'alteryx_pyspark_converter' package.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Phase 1 - Parse Workflow
# MAGIC Parse the `.yxmd` workflow XML into an intermediate JSON representation.

# COMMAND ----------

import time
import json

intermediate_json_path = None

if RUN_MODE in ("full_convert", "parse_only", "list_containers", "show_columns"):
    # Handle DBFS paths
    workflow_path = WORKFLOW_PATH
    if workflow_path.startswith("dbfs:"):
        local_path = "/tmp/workflow_to_parse.yxmd"
        dbutils.fs.cp(workflow_path, f"file:{local_path}")
        workflow_path = local_path

    print(f"\nPhase 1: Parsing workflow...")
    print(f"  File: {workflow_path}")

    t0 = time.time()
    parser = WorkflowParser()
    workflow = parser.parse(workflow_path)
    parse_time = time.time() - t0

    print(f"  Parsed in {parse_time:.1f}s")
    print(f"  Workflow name: {workflow.name}")
    print(f"  Containers: {len(workflow.containers)}")
    print(f"  Tools: {len(workflow.tools)}")
    print(f"  Connections: {len(workflow.connections)}")

    # Save intermediate JSON
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    writer = JSONWriter()
    intermediate_json_path = writer.write(workflow, OUTPUT_DIR)
    print(f"  Intermediate JSON: {intermediate_json_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### List Containers

# COMMAND ----------

if RUN_MODE in ("full_convert", "parse_only", "list_containers", "show_columns"):
    summaries = parser.get_container_summary(workflow)

    print("\nContainers found in workflow:")
    print("=" * 80)
    for s in summaries:
        status = " [DISABLED]" if s["disabled"] else ""
        parent = f" (parent: {s['parent_container']})" if s["parent_container"] else ""
        print(f"\n  [{s['tool_id']}] {s['name']}{status}{parent}")
        print(f"    Tools: {s['num_tools']}, Sub-containers: {s['num_sub_containers']}")
        print(f"    Internal connections: {s['num_internal_connections']}")
        print(f"    External inputs: {s['num_external_inputs']}, outputs: {s['num_external_outputs']}")
        if s["tool_types"]:
            types_str = ", ".join(f"{t}: {c}" for t, c in sorted(s["tool_types"].items()))
            print(f"    Tool types: {types_str}")
    print("=" * 80)

    if RUN_MODE == "list_containers":
        dbutils.notebook.exit("LISTED_CONTAINERS")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Columns (Optional)

# COMMAND ----------

if RUN_MODE == "show_columns" and CONTAINER_NAME:
    from alteryx_pyspark_converter.phase1_parser.parser.formula_extractor import FormulaExtractor

    container = workflow.get_container_by_name(CONTAINER_NAME)
    if not container:
        try:
            container = workflow.get_container(int(CONTAINER_NAME))
        except (ValueError, TypeError):
            pass

    if not container:
        print(f"Container '{CONTAINER_NAME}' not found.")
        dbutils.notebook.exit("CONTAINER_NOT_FOUND")
    else:
        formula_extractor = FormulaExtractor()
        tools = workflow.get_container_tools(container.tool_id)
        for sub_id in container.child_container_ids:
            sub_tools = workflow.get_container_tools(sub_id)
            tools.extend(sub_tools)

        all_columns = set()
        for tool in tools:
            cols = formula_extractor.extract_all_columns_from_tool(tool.configuration)
            all_columns.update(cols)

        print(f"\nColumns in container: {container.name}")
        print("=" * 60)
        print(f"  Total unique columns: {len(all_columns)}")
        for col in sorted(all_columns):
            print(f"    - {col}")

        dbutils.notebook.exit(f"COLUMNS_LISTED: {len(all_columns)} columns found")

# COMMAND ----------

if RUN_MODE == "parse_only":
    print("\nParse-only mode complete.")
    dbutils.notebook.exit(f"PARSE_COMPLETE: {intermediate_json_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Phase 2 - Generate PySpark Notebook
# MAGIC Convert the intermediate JSON + config into a Databricks notebook.

# COMMAND ----------

if RUN_MODE in ("full_convert", "generate_only"):
    # For generate_only mode, use a pre-existing intermediate JSON
    if RUN_MODE == "generate_only":
        # Look for intermediate.json in output dir
        intermediate_json_path = os.path.join(OUTPUT_DIR, "intermediate.json")
        if not os.path.exists(intermediate_json_path):
            print(f"ERROR: Intermediate JSON not found at: {intermediate_json_path}")
            print("Run in 'full_convert' or 'parse_only' mode first.")
            dbutils.notebook.exit("INTERMEDIATE_JSON_NOT_FOUND")

    # Handle DBFS config path
    config_path = CONFIG_PATH
    if config_path.startswith("dbfs:"):
        local_config = "/tmp/converter_config.yaml"
        dbutils.fs.cp(config_path, f"file:{local_config}")
        config_path = local_config

    print(f"\nPhase 2: Generating PySpark notebook...")
    print(f"  Intermediate JSON: {intermediate_json_path}")
    print(f"  Config: {config_path}")

    t0 = time.time()
    generator = NotebookGenerator(
        intermediate_json_path=intermediate_json_path,
        config_path=config_path,
    )

    # Determine output path
    output_path = os.path.join(OUTPUT_DIR, f"{generator.config.container_name}.py")
    path = generator.generate_and_save(output_path)
    gen_time = time.time() - t0

    print(f"  Generated in {gen_time:.1f}s")
    print(f"  Output notebook: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate Generated Code (Optional)

# COMMAND ----------

if RUN_MODE in ("full_convert", "generate_only") and not SKIP_VALIDATION:
    print("\nValidating generated code...")

    with open(path, "r", encoding="utf-8") as f:
        code = f.read()

    validator = SyntaxValidator()
    result = validator.validate(code)

    if result["valid"]:
        print("  Syntax validation: PASSED")
    else:
        print("  Syntax validation: ISSUES FOUND")
        for issue in result.get("issues", []):
            print(f"    - {issue.get('type', 'unknown')}: {issue.get('message', '')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Summary

# COMMAND ----------

if RUN_MODE in ("full_convert", "generate_only"):
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"  Intermediate JSON: {intermediate_json_path}")
    print(f"  Generated notebook: {path}")

    # Show output file size
    if os.path.exists(path):
        size = os.path.getsize(path)
        print(f"  File size: {size:,} bytes")

    print()
    print("Next steps:")
    print("  1. Open the generated notebook in Databricks")
    print("  2. Review the generated PySpark code")
    print("  3. Update table names and column mappings as needed")
    print("  4. Run the notebook to execute the transformation")
    print("=" * 60)

    dbutils.notebook.exit(f"COMPLETE: Generated {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generated Files

# COMMAND ----------

print(f"\nFiles in output directory: {OUTPUT_DIR}")
print("-" * 60)
try:
    for f in sorted(os.listdir(OUTPUT_DIR)):
        filepath = os.path.join(OUTPUT_DIR, f)
        size = os.path.getsize(filepath)
        print(f"  {f:<40s} ({size:,} bytes)")
except Exception as e:
    print(f"  Could not list files: {e}")
