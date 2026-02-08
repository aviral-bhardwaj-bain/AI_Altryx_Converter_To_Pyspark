# Databricks notebook source
# MAGIC %md
# MAGIC # Setup & Installation
# MAGIC Installs required dependencies for the Alteryx to PySpark converter.
# MAGIC
# MAGIC **Run this notebook once** before using any other converter notebooks.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Python Dependencies

# COMMAND ----------

# MAGIC %pip install anthropic>=0.39.0 pyyaml>=6.0 jinja2>=3.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Restart Python Interpreter
# MAGIC After pip install, the Python interpreter must be restarted for packages to be available.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Installation

# COMMAND ----------

import importlib

packages = {
    "anthropic": "anthropic",
    "yaml": "pyyaml",
    "jinja2": "jinja2",
}

print("Package Verification")
print("=" * 50)
all_ok = True
for import_name, pkg_name in packages.items():
    try:
        mod = importlib.import_module(import_name)
        version = getattr(mod, "__version__", "installed")
        print(f"  {pkg_name:<20s} {version}")
    except ImportError:
        print(f"  {pkg_name:<20s} MISSING")
        all_ok = False

# Built-in modules (always available)
builtins = ["xml.etree.ElementTree", "json", "re", "logging", "dataclasses"]
for mod_name in builtins:
    try:
        importlib.import_module(mod_name)
        print(f"  {mod_name:<20s} built-in")
    except ImportError:
        print(f"  {mod_name:<20s} MISSING")
        all_ok = False

print("=" * 50)
if all_ok:
    print("All dependencies installed successfully.")
else:
    print("WARNING: Some dependencies are missing. Re-run Step 1.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Set Up Databricks Secrets (One-Time)
# MAGIC
# MAGIC Store your Anthropic API key securely using Databricks Secrets.
# MAGIC Run these commands from your **local terminal** or **Databricks CLI**:
# MAGIC
# MAGIC ```bash
# MAGIC # Install Databricks CLI if not already installed
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # Configure CLI (use your workspace URL and token)
# MAGIC databricks configure --token
# MAGIC
# MAGIC # Create a secret scope
# MAGIC databricks secrets create-scope alteryx-converter
# MAGIC
# MAGIC # Store the API key
# MAGIC databricks secrets put-secret alteryx-converter anthropic-api-key
# MAGIC ```
# MAGIC
# MAGIC **Alternative (Databricks UI):**
# MAGIC 1. Go to your Databricks workspace URL + `#secrets/createScope`
# MAGIC 2. Create scope: `alteryx-converter`
# MAGIC 3. Use the CLI to add the secret key

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Secret Access (Optional)
# MAGIC Uncomment and run the cell below to verify your secret is accessible.

# COMMAND ----------

# Uncomment to test secret access:
# try:
#     api_key = dbutils.secrets.get(scope="alteryx-converter", key="anthropic-api-key")
#     print(f"API key retrieved successfully (length: {len(api_key)} chars)")
# except Exception as e:
#     print(f"ERROR: Could not retrieve API key: {e}")
#     print("Please set up secrets as described in Step 4.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC You can now use the converter notebooks:
# MAGIC - **`01_run_ai_converter`** - AI-powered conversion (recommended)
# MAGIC - **`02_run_rule_based_converter`** - Rule-based Phase 1 + Phase 2 conversion
# MAGIC - **`convert_workflow`** - Direct AI converter (legacy)
