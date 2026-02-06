# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration
# MAGIC Shared configuration for the Alteryx to PySpark AI Converter.
# MAGIC
# MAGIC This notebook sets up Databricks widgets for user-configurable parameters
# MAGIC and retrieves secrets securely via `dbutils.secrets`.
# MAGIC
# MAGIC **Usage:** This notebook is imported via `%run ./config`
# MAGIC
# MAGIC ## Setup Instructions
# MAGIC Before running, ensure you have:
# MAGIC 1. Created a Databricks secret scope (e.g., `alteryx-converter`)
# MAGIC 2. Stored your Anthropic API key: `databricks secrets put-secret alteryx-converter anthropic-api-key`
# MAGIC 3. Uploaded your `.yxmd` workflow file to a Databricks Volume or DBFS path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widget Parameters
# MAGIC These widgets appear at the top of the notebook for interactive configuration.

# COMMAND ----------

# -- Databricks Widgets (replace argparse CLI arguments) --

dbutils.widgets.text(
    "workflow_path",
    "",
    "Workflow File Path (Volume/DBFS)"
)

dbutils.widgets.text(
    "output_dir",
    "/Workspace/Users/shared/alteryx_converter/output",
    "Output Directory"
)

dbutils.widgets.text(
    "container_name",
    "",
    "Container Name (blank = all)"
)

dbutils.widgets.dropdown(
    "model",
    "claude-sonnet-4-20250514",
    ["claude-sonnet-4-20250514", "claude-opus-4-20250514", "claude-haiku-4-5-20251001"],
    "Claude Model"
)

dbutils.widgets.dropdown(
    "max_retries",
    "2",
    ["0", "1", "2", "3", "4", "5"],
    "Max Retries"
)

dbutils.widgets.text(
    "secret_scope",
    "alteryx-converter",
    "Secret Scope Name"
)

dbutils.widgets.text(
    "secret_key",
    "anthropic-api-key",
    "Secret Key Name"
)

dbutils.widgets.dropdown(
    "run_mode",
    "convert",
    ["convert", "list_containers", "dry_run"],
    "Run Mode"
)

dbutils.widgets.text(
    "source_tables_json",
    "",
    "Source Tables JSON Path (optional)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Widget Values

# COMMAND ----------

# -- Read widget values into variables --

WORKFLOW_PATH = dbutils.widgets.get("workflow_path")
OUTPUT_DIR = dbutils.widgets.get("output_dir")
CONTAINER_NAME = dbutils.widgets.get("container_name").strip() or None
MODEL = dbutils.widgets.get("model")
MAX_RETRIES = int(dbutils.widgets.get("max_retries"))
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
SECRET_KEY = dbutils.widgets.get("secret_key")
RUN_MODE = dbutils.widgets.get("run_mode")
SOURCE_TABLES_JSON = dbutils.widgets.get("source_tables_json").strip() or None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve API Key from Databricks Secrets

# COMMAND ----------

# -- Retrieve API key securely from Databricks Secrets --

def get_api_key():
    """Retrieve the Anthropic API key from Databricks secrets."""
    try:
        api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
        if not api_key:
            raise ValueError("API key is empty")
        print(f"API key retrieved from scope='{SECRET_SCOPE}', key='{SECRET_KEY}'")
        return api_key
    except Exception as e:
        print(f"ERROR: Could not retrieve API key from Databricks Secrets.")
        print(f"  Scope: {SECRET_SCOPE}")
        print(f"  Key:   {SECRET_KEY}")
        print(f"  Error: {e}")
        print()
        print("To set up secrets, run these CLI commands:")
        print(f"  databricks secrets create-scope {SECRET_SCOPE}")
        print(f"  databricks secrets put-secret {SECRET_SCOPE} {SECRET_KEY}")
        raise

# COMMAND ----------

# -- Load source tables config if provided --

def load_source_tables_config():
    """Load the source tables JSON mapping file if provided."""
    if not SOURCE_TABLES_JSON:
        return None
    try:
        import json
        # Read from DBFS or Volumes path
        content = dbutils.fs.head(SOURCE_TABLES_JSON, 65536)
        config = json.loads(content)
        print(f"Loaded source tables config from: {SOURCE_TABLES_JSON}")
        print(f"  Mappings: {len(config)} entries")
        return config
    except Exception as e:
        print(f"WARNING: Could not load source tables config from '{SOURCE_TABLES_JSON}': {e}")
        return None

# COMMAND ----------

# -- Validate configuration --

def validate_config():
    """Validate that required configuration is present."""
    errors = []

    if not WORKFLOW_PATH:
        errors.append("Workflow file path is required. Set the 'workflow_path' widget.")

    if RUN_MODE == "convert":
        try:
            get_api_key()
        except Exception:
            errors.append("API key is required for conversion mode. Configure Databricks Secrets.")

    if errors:
        for err in errors:
            print(f"CONFIG ERROR: {err}")
        raise ValueError("Configuration validation failed. See errors above.")

    print("Configuration validated successfully.")
    print(f"  Workflow:  {WORKFLOW_PATH}")
    print(f"  Output:    {OUTPUT_DIR}")
    print(f"  Mode:      {RUN_MODE}")
    print(f"  Model:     {MODEL}")
    print(f"  Container: {CONTAINER_NAME or '(all)'}")
