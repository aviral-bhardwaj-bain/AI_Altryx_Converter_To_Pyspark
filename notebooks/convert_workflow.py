# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx to PySpark AI Converter
# MAGIC ### Main Orchestrator Notebook
# MAGIC
# MAGIC Converts Alteryx `.yxmd` workflows into production-ready PySpark code using Claude AI.
# MAGIC Generates one output file per container (module).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Configure the widgets at the top of this notebook:
# MAGIC    - **Workflow File Path**: Path to your `.yxmd` file (uploaded to a Databricks Volume or DBFS)
# MAGIC    - **Output Directory**: Where generated PySpark files will be written
# MAGIC    - **Container Name**: Leave blank to convert all, or specify one container
# MAGIC    - **Run Mode**: `convert` (full run), `list_containers` (inspect), or `dry_run` (parse only)
# MAGIC    - **Claude Model**: Select the model to use
# MAGIC    - **Secret Scope/Key**: Where your Anthropic API key is stored
# MAGIC 2. Click **Run All** or run cells individually
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Notebook dependency chain:**
# MAGIC ```
# MAGIC convert_workflow (this notebook)
# MAGIC   |-- %run ./config          --> widgets, secrets, validation
# MAGIC   |-- %run ./ai_generator    --> Claude API integration
# MAGIC   |     |-- %run ./context_builder  --> prompt building
# MAGIC   |           |-- %run ./models     --> data classes
# MAGIC   |-- %run ./parser          --> XML parsing
# MAGIC   |     |-- %run ./models     --> (already loaded)
# MAGIC   |-- %run ./utils           --> display helpers
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Configuration & Dependencies

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./ai_generator

# COMMAND ----------

# MAGIC %run ./parser

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

import time
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Validate Configuration

# COMMAND ----------

validate_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse the Alteryx Workflow

# COMMAND ----------

print_banner()

# -- Validate that the workflow file exists --
workflow_path = WORKFLOW_PATH

# Support both DBFS and local/Volumes paths
if workflow_path.startswith("dbfs:"):
    # Copy from DBFS to local temp for XML parsing
    local_path = "/tmp/workflow_to_parse.yxmd"
    dbutils.fs.cp(workflow_path, f"file:{local_path}")
    workflow_path = local_path
elif workflow_path.startswith("/Volumes/"):
    # Volumes paths are accessible directly
    pass
elif workflow_path.startswith("/dbfs/"):
    # /dbfs/ mount path, accessible directly
    pass

print(f"Parsing workflow: {workflow_path}")

# COMMAND ----------

t0 = time.time()
parser_obj = AlteryxWorkflowParser(workflow_path)
workflow = parser_obj.parse()
parse_time = time.time() - t0

print(f"  Parsed in {parse_time:.1f}s")
print(f"  Containers: {len(workflow.containers)}")
print(f"  Total tools: {len(workflow.all_tools)}")
print(f"  Total connections: {len(workflow.connections)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: List Containers (All Modes)
# MAGIC Shows all containers found in the workflow for reference.

# COMMAND ----------

print("\nContainers found in workflow:")
print("-" * 60)
for i, container in enumerate(workflow.containers, 1):
    tool_count = len(container.child_tools)
    disabled_tag = " [DISABLED]" if container.disabled else ""
    print(f"  {i:2d}. {container.name:<40s} ({tool_count} tools){disabled_tag}")
print("-" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Execute Based on Run Mode

# COMMAND ----------

if RUN_MODE == "list_containers":
    print("\nRun mode: list_containers -- listing complete. No further action.")
    dbutils.notebook.exit("LISTED_CONTAINERS")

# COMMAND ----------

# -- Filter to specific container if requested --
containers_to_convert = workflow.containers

if CONTAINER_NAME:
    containers_to_convert = [
        c for c in workflow.containers
        if c.name.lower() == CONTAINER_NAME.lower()
    ]
    if not containers_to_convert:
        print(f"ERROR: Container '{CONTAINER_NAME}' not found.")
        print("Available containers:")
        for c in workflow.containers:
            print(f"  - {c.name}")
        dbutils.notebook.exit("CONTAINER_NOT_FOUND")
    print(f"\nFiltered to container: '{CONTAINER_NAME}'")
else:
    print(f"\nConverting all {len(containers_to_convert)} containers")

# COMMAND ----------

# -- Dry-run mode: show context extraction without calling Claude --
if RUN_MODE == "dry_run":
    print("\nDry-run mode -- extracted context for each container:\n")
    for container in containers_to_convert:
        context = workflow.get_container_context(container.tool_id)
        print(f"-- {container.name} --")
        print(f"   Tools: {len(container.child_tools)}")
        print(f"   External inputs: {len(context['external_inputs'])}")
        print(f"   External outputs: {len(context['external_outputs'])}")
        print(f"   Sub-containers: {len(context['sub_containers'])}")
        print()
    dbutils.notebook.exit("DRY_RUN_COMPLETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Generate PySpark Code via Claude AI
# MAGIC This step calls the Claude API for each container, generating production-ready PySpark code.

# COMMAND ----------

# -- Set up output directory --
output_dir = OUTPUT_DIR

# Create output directory (works for Workspace paths)
try:
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")
except Exception:
    # For DBFS paths, use dbutils
    try:
        dbutils.fs.mkdirs(output_dir)
        print(f"Output directory (DBFS): {output_dir}")
    except Exception as e:
        print(f"WARNING: Could not create output directory: {e}")

# COMMAND ----------

# -- Load source tables config --
source_tables = load_source_tables_config()

# COMMAND ----------

# -- Retrieve API key and initialize the generator --
api_key = get_api_key()

generator = ClaudeCodeGenerator(
    api_key=api_key,
    model=MODEL,
    max_retries=MAX_RETRIES,
    source_tables_config=source_tables,
)

print(f"Claude AI generator initialized (model={MODEL}, retries={MAX_RETRIES})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Code for Each Container

# COMMAND ----------

results = []

for i, container in enumerate(containers_to_convert, 1):
    print(f"\n{'='*60}")
    print(f"[{i}/{len(containers_to_convert)}] Generating: {container.name}")
    print(f"{'='*60}")

    context = workflow.get_container_context(container.tool_id)
    t0 = time.time()

    try:
        code = generator.generate_container_code(
            container=container,
            context=context,
            workflow=workflow,
        )
        gen_time = time.time() - t0

        # Build safe filename
        safe_name = container.name.lower().replace(" ", "_").replace("-", "_")
        safe_name = "".join(c for c in safe_name if c.isalnum() or c == "_")
        output_file = os.path.join(output_dir, f"{safe_name}.py")

        # Write output file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(code)

        results.append({
            "container": container.name,
            "file": output_file,
            "status": "Success",
            "time": f"{gen_time:.1f}s",
            "tools": len(container.child_tools),
        })
        print(f"   Written to: {output_file} ({gen_time:.1f}s)")

    except Exception as e:
        gen_time = time.time() - t0
        results.append({
            "container": container.name,
            "file": "-",
            "status": f"Failed: {str(e)[:80]}",
            "time": f"{gen_time:.1f}s",
            "tools": len(container.child_tools),
        })
        print(f"   Failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Conversion Summary

# COMMAND ----------

print_summary(results)

# COMMAND ----------

# -- Rich HTML summary for Databricks UI --
try:
    display_summary_table(results)
except Exception:
    # Fallback if displayHTML is not available
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: View Generated Files
# MAGIC List the generated output files.

# COMMAND ----------

print(f"\nGenerated files in: {output_dir}")
print("-" * 60)
try:
    for f in sorted(os.listdir(output_dir)):
        if f.endswith(".py"):
            filepath = os.path.join(output_dir, f)
            size = os.path.getsize(filepath)
            print(f"  {f:<40s} ({size:,} bytes)")
except Exception as e:
    print(f"  Could not list files: {e}")

# COMMAND ----------

# -- Return summary as notebook exit value --
success_count = sum(1 for r in results if "Success" in r["status"])
total_count = len(results)
dbutils.notebook.exit(f"COMPLETE: {success_count}/{total_count} containers converted successfully")
