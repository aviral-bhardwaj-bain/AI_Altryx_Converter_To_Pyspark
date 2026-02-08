# Alteryx to PySpark Converter for Databricks

Converts Alteryx `.yxmd` workflows into production-ready **PySpark code** for **Databricks**. Supports two conversion modes:

| Mode | Approach | Best For |
|------|----------|----------|
| **AI-Powered** | Sends workflow context to Claude AI | Complex workflows, idiomatic code |
| **Rule-Based** | Deterministic Phase 1 (parse) + Phase 2 (generate) | Predictable output, no API needed |

---

## Running in Databricks (Recommended)

### Prerequisites

- Databricks workspace with a running cluster (DBR 13.0+)
- Python 3.10+ on the cluster
- Anthropic API key (for AI-powered mode only)
- An Alteryx `.yxmd` workflow file

### Step 1: Upload Project to Databricks Workspace

Upload the entire project to your Databricks workspace using one of these methods:

**Option A: Git integration (recommended)**
```
Workspace > Repos > Add Repo > paste the repository URL
```

**Option B: Manual upload**
1. Go to **Workspace > Users > your_username**
2. Create a folder: `alteryx_converter`
3. Import the project files:
   - Import the `notebooks/` directory contents directly into this folder
   - Import the full project repo for rule-based mode

Your workspace structure should look like:
```
/Workspace/Users/<you>/alteryx_converter/
  |- 00_setup.py              # Dependency installer
  |- 01_run_ai_converter.py   # AI-powered converter
  |- 02_run_rule_based_converter.py  # Rule-based converter
  |- config.py                # Shared config (for AI mode)
  |- models.py                # Data models
  |- parser.py                # XML parser
  |- context_builder.py       # Prompt builder
  |- ai_generator.py          # Claude API client
  |- utils.py                 # Display helpers
  |- convert_workflow.py      # Legacy AI converter
```

For the rule-based converter, also upload the full project (including `alteryx_pyspark_converter/`):
```
/Workspace/Users/<you>/alteryx_converter/AI_Altryx_Converter_To_Pyspark/
  |- alteryx_pyspark_converter/    # Rule-based converter package
  |- src/                          # AI converter source
  |- examples/                     # Config examples
  |- ...
```

### Step 2: Upload Your Workflow File

Upload your `.yxmd` file to a **Databricks Volume** or **DBFS**:

**Using Volumes (recommended):**
```
/Volumes/<catalog>/<schema>/<volume>/my_workflow.yxmd
```

**Using DBFS:**
```
dbfs:/FileStore/workflows/my_workflow.yxmd
```

**Using the Databricks UI:**
1. Go to **Catalog > your_catalog > your_schema > Volumes**
2. Click **Upload** and select your `.yxmd` file

### Step 3: Install Dependencies

Open and run the `00_setup` notebook:

1. Navigate to `/Workspace/Users/<you>/alteryx_converter/00_setup`
2. Click **Run All**
3. This installs: `anthropic`, `pyyaml`, `jinja2`
4. Restarts the Python interpreter automatically

> You only need to run this once per cluster restart.

### Step 4: Set Up Secrets (AI Mode Only)

Store your Anthropic API key securely:

```bash
# From your local terminal (one-time setup)
pip install databricks-cli
databricks configure --token

# Create secret scope and store API key
databricks secrets create-scope alteryx-converter
databricks secrets put-secret alteryx-converter anthropic-api-key
# Paste your API key when prompted
```

**Alternative via Databricks UI:**
1. Navigate to `https://<workspace-url>/#secrets/createScope`
2. Scope name: `alteryx-converter`
3. Use CLI to add the key: `databricks secrets put-secret alteryx-converter anthropic-api-key`

### Step 5: Run the Converter

#### Option A: AI-Powered Converter (`01_run_ai_converter`)

1. Open `/Workspace/Users/<you>/alteryx_converter/01_run_ai_converter`
2. Configure the widgets at the top:

| Widget | Example Value | Description |
|--------|---------------|-------------|
| **Workflow File Path** | `/Volumes/my_catalog/my_schema/vol/workflow.yxmd` | Path to your `.yxmd` file |
| **Output Directory** | `/Workspace/Users/<you>/output` | Where generated `.py` files are saved |
| **Container Name** | *(blank for all)* | Convert a specific container, or leave blank for all |
| **Run Mode** | `convert` | `convert`, `list_containers`, or `dry_run` |
| **Claude Model** | `claude-sonnet-4-20250514` | AI model to use |
| **Max Retries** | `2` | Retries on API failure |
| **Secret Scope** | `alteryx-converter` | Databricks secret scope name |
| **Secret Key** | `anthropic-api-key` | Secret key name |
| **Source Tables JSON** | *(optional)* | Path to table mapping JSON |

3. Click **Run All**
4. Generated PySpark files appear in the output directory

#### Option B: Rule-Based Converter (`02_run_rule_based_converter`)

1. Create a config YAML file (see [Config File Format](#config-file-format) below)
2. Upload the config file to a Volume or DBFS
3. Open `/Workspace/Users/<you>/alteryx_converter/02_run_rule_based_converter`
4. Configure the widgets:

| Widget | Example Value | Description |
|--------|---------------|-------------|
| **Workflow File Path** | `/Volumes/my_catalog/my_schema/vol/workflow.yxmd` | Path to your `.yxmd` file |
| **Config YAML Path** | `/Volumes/my_catalog/my_schema/vol/config.yaml` | Path to config YAML |
| **Output Directory** | `/Workspace/Users/<you>/output` | Where generated files are saved |
| **Project Root Path** | `/Workspace/Users/<you>/alteryx_converter/AI_Altryx_Converter_To_Pyspark` | Where the full project repo is uploaded |
| **Run Mode** | `full_convert` | `full_convert`, `parse_only`, `generate_only`, `list_containers`, `show_columns` |
| **Container Name** | *(for show_columns mode)* | Container to inspect |
| **Skip Validation** | `false` | Skip syntax checking |

5. Click **Run All**

### Step 6: Use the Generated Code

The converter outputs `.py` files in Databricks notebook format. To use them:

1. Navigate to the output directory in Workspace
2. Open the generated notebook (e.g., `mod_provider.py`)
3. Review the PySpark code
4. Update `spark.table()` references with your actual Databricks table paths
5. Run the notebook

---

## Config File Format

For the rule-based converter, create a YAML configuration file:

```yaml
# Container to convert (name or tool ID)
container_name: "mod_provider"

# Input tables and their schemas
input_tables:
  fact_nps:
    databricks_table: "gold_catalog.insurance.st_fact_nps"
    maps_to_tool_id: 3111

  fact_response:
    databricks_table: "gold_catalog.insurance.st_fact_response"
    maps_to_tool_id: 2367
    # Optional: Pre-filter to apply when loading
    pre_filter: >
      (typ_question = 'multiple')
      AND (num_answer != '0')

# Column mappings: Alteryx column name -> Databricks column name
column_mappings:
  nps_type: typ_nps
  verbatim: txt_verbatim
  nps_score: num_nps_score
  question_var: id_survey_question

# Output configuration
output:
  table_name: "gold_catalog.insurance.mod_provider"
  columns:
    - provider_name
    - gid_respondent
    - gid_provider

# Notebook settings
notebook:
  add_validation_cell: true
  add_schema_print: true
```

See `examples/sample_config.yaml` for a complete template.

## Source Table Mapping (AI Mode)

For the AI-powered converter, create a JSON file mapping Alteryx inputs to Databricks tables:

```json
{
    "nps_fact": "gold_catalog.insurance.nps_fact",
    "dim_provider": "gold_catalog.insurance.dim_provider",
    "dim_time": "gold_catalog.insurance.dim_time"
}
```

Upload to a Volume and reference it in the **Source Tables JSON Path** widget.

---

## Running from the Command Line (Alternative)

If you prefer running locally instead of in Databricks:

### AI-Powered Mode

```bash
# Install dependencies
pip install anthropic

# Set API key
export ANTHROPIC_API_KEY=sk-ant-api03-...

# Convert all containers
python convert.py my_workflow.yxmd -o ./output

# Convert a specific container
python convert.py my_workflow.yxmd -c mod_provider -o ./output

# List containers first
python convert.py my_workflow.yxmd --list-containers

# Dry run (parse only, no AI calls)
python convert.py my_workflow.yxmd --dry-run
```

### Rule-Based Mode

```bash
# Install the package
pip install -e alteryx_pyspark_converter/

# Phase 1: Parse workflow to JSON
alteryx-parse parse -i workflow.yxmd -o ./intermediate/

# Phase 2: Generate notebook from JSON + config
alteryx-generate generate -i ./intermediate/intermediate.json -c config.yaml -o ./output/notebook.py
```

### CLI Options (AI Mode)

```
usage: convert.py [-h] [--output-dir DIR] [--container NAME]
                  [--list-containers] [--model MODEL] [--dry-run]
                  [--max-retries N] [--source-tables-config FILE]
                  workflow

positional arguments:
  workflow              Path to the .yxmd Alteryx workflow file

options:
  --output-dir, -o      Output directory (default: ./output)
  --container, -c       Convert only this container
  --list-containers, -l List all containers and exit
  --model               Claude model (default: claude-sonnet-4-20250514)
  --dry-run             Parse only, no AI calls
  --max-retries         Retries on failure (default: 2)
  --source-tables-config  JSON mapping Alteryx inputs to Databricks tables
```

---

## Calling Notebooks Programmatically

You can run the converter from another Databricks notebook using `dbutils.notebook.run()`:

### AI-Powered Converter

```python
result = dbutils.notebook.run(
    "/Workspace/Users/<you>/alteryx_converter/01_run_ai_converter",
    timeout_seconds=3600,
    arguments={
        "workflow_path": "/Volumes/catalog/schema/vol/workflow.yxmd",
        "output_dir": "/Workspace/Users/<you>/output",
        "run_mode": "convert",
        "model": "claude-sonnet-4-20250514",
        "container_name": "",          # blank = all containers
        "secret_scope": "alteryx-converter",
        "secret_key": "anthropic-api-key",
    }
)
print(result)  # "COMPLETE: 3/3 containers converted successfully"
```

### Rule-Based Converter

```python
result = dbutils.notebook.run(
    "/Workspace/Users/<you>/alteryx_converter/02_run_rule_based_converter",
    timeout_seconds=1800,
    arguments={
        "workflow_path": "/Volumes/catalog/schema/vol/workflow.yxmd",
        "config_path": "/Volumes/catalog/schema/vol/config.yaml",
        "output_dir": "/Workspace/Users/<you>/output",
        "project_root": "/Workspace/Users/<you>/alteryx_converter/AI_Altryx_Converter_To_Pyspark",
        "run_mode": "full_convert",
    }
)
print(result)  # "COMPLETE: Generated /Workspace/Users/.../mod_provider.py"
```

### Inline Usage (AI Mode)

```python
import sys
sys.path.append("/Workspace/Users/<you>/alteryx_converter/AI_Altryx_Converter_To_Pyspark")

from src.parser import AlteryxWorkflowParser
from src.ai_generator import ClaudeCodeGenerator

# Parse
parser = AlteryxWorkflowParser("/Volumes/catalog/schema/vol/workflow.yxmd")
workflow = parser.parse()

# List containers
for c in workflow.containers:
    print(f"{c.name}: {len(c.child_tools)} tools")

# Generate code for a specific container
generator = ClaudeCodeGenerator(
    api_key=dbutils.secrets.get("alteryx-converter", "anthropic-api-key"),
    model="claude-sonnet-4-20250514",
)

container = workflow.containers[0]
context = workflow.get_container_context(container.tool_id)
code = generator.generate_container_code(
    container=container,
    context=context,
    workflow=workflow,
)
print(code)
```

---

## Architecture

```
                          Alteryx .yxmd Workflow
                                  |
                 +----------------+----------------+
                 |                                  |
         AI-Powered Mode                   Rule-Based Mode
         (Claude API)                      (Deterministic)
                 |                                  |
    +------------+------------+        +------------+------------+
    |                         |        |                         |
    v                         v        v                         v
 XML Parser              Context    Phase 1                  Phase 2
 (parser.py)             Builder    Parser                   Generator
    |                    (builds    (XML -> JSON)            (JSON -> PySpark)
    |                    prompts)       |                         |
    v                        |         v                         v
 Workflow Model              |    Intermediate              Tool Converters
 (models.py)                 |    JSON                      Expression Converter
    |                        v         |                    Notebook Assembler
    |                    Claude AI     |                         |
    |                        |         v                         v
    v                        v    intermediate.json          Generated
 Containers             Generated                           Databricks
 Tools                  PySpark                             Notebook
 Connections            Code                                (.py)
```

### Notebook Structure (Databricks)

```
notebooks/
  |- 00_setup.py                    # Install dependencies (run first)
  |- 01_run_ai_converter.py         # AI-powered converter (main)
  |- 02_run_rule_based_converter.py # Rule-based Phase 1+2 converter
  |- config.py                      # Widget configuration & secrets
  |- models.py                      # Data model classes
  |- parser.py                      # Alteryx XML parser
  |- context_builder.py             # Prompt construction
  |- ai_generator.py                # Claude API integration
  |- utils.py                       # Display helpers
  |- convert_workflow.py            # Legacy orchestrator
```

### Full Project Structure

```
AI_Altryx_Converter_To_Pyspark/
  |- convert.py                        # CLI entry point (AI mode)
  |- requirements.txt                  # Python dependencies
  |- README.md                         # This file
  |
  |- src/                              # AI-Powered Converter
  |   |- parser.py                     # Alteryx XML parser
  |   |- models.py                     # Data models
  |   |- context_builder.py            # Builds prompts for Claude
  |   |- ai_generator.py               # Claude API integration
  |   |- utils.py                      # CLI utilities
  |
  |- alteryx_pyspark_converter/        # Rule-Based Converter
  |   |- phase1_parser/                # Parse .yxmd -> intermediate JSON
  |   |   |- parser/                   # XML extraction modules
  |   |   |- models/                   # Data model classes
  |   |   |- output/                   # JSON writer
  |   |   |- cli.py                    # Phase 1 CLI
  |   |
  |   |- phase2_generator/            # Generate PySpark from JSON
  |   |   |- generator/               # Code generation engine
  |   |   |- tool_converters/         # Per-tool converters (15+ types)
  |   |   |- expression_converter/    # Alteryx formula -> PySpark
  |   |   |- validators/              # Code validation
  |   |   |- config/                  # Configuration handling
  |   |   |- cli.py                   # Phase 2 CLI
  |   |
  |   |- common/                      # Shared utilities
  |
  |- notebooks/                        # Databricks Notebooks
  |- examples/                         # Config templates
  |- tests/                            # Test suite
```

## Supported Alteryx Tools

The converter handles these Alteryx tool types:

| Alteryx Tool | PySpark Equivalent | Notes |
|-------------|-------------------|-------|
| Filter | `.filter()` | True/False output tracking |
| Formula | `.withColumn()` | Full expression conversion |
| Join | `.join()` | Join/Left/Right output tracking |
| Select | `.select()` / `.drop()` / `.withColumnRenamed()` | Column selection, renaming, reordering |
| Union | `.unionByName(allowMissingColumns=True)` | Multiple inputs |
| Summarize | `.groupBy().agg()` | All aggregation functions |
| CrossTab | `.groupBy().pivot().agg()` | Pivot tables |
| TextInput | `spark.createDataFrame()` | Inline data |
| Sort | `.orderBy()` | Multi-column sorting |
| Unique | `.dropDuplicates()` | Unique/Dupes outputs |
| Sample | `.sample()` / `.limit()` | Various sampling methods |
| RegEx | `F.regexp_extract()` / `F.regexp_replace()` | Pattern matching |
| TextToColumns | `F.split()` | Column splitting |
| Transpose | Stack/unpivot | Row/column transposition |
| RecordID | `F.monotonically_increasing_id()` | Row numbering |
| MultiRowFormula | `Window` functions | Cross-row calculations |
| LockIn* (In-DB) | Standard equivalents | 12 In-Database tool variants |

## Model Selection (AI Mode)

| Model | Best For | Cost |
|-------|----------|------|
| `claude-sonnet-4-20250514` | Best balance of quality/speed/cost (recommended) | $$ |
| `claude-opus-4-20250514` | Most complex workflows with intricate logic | $$$$ |
| `claude-haiku-4-5-20251001` | Simple containers, budget-conscious | $ |

## Troubleshooting

### Common Issues

**"ModuleNotFoundError: No module named 'alteryx_pyspark_converter'"**
- Make sure the `project_root` widget points to the directory containing the `alteryx_pyspark_converter` folder
- Verify the project was fully uploaded to Workspace

**"API key not found in secrets"**
- Run: `databricks secrets list-secrets --scope alteryx-converter`
- Verify the scope and key names match your widget configuration
- Re-run `00_setup` for instructions

**"Workflow file not found"**
- Use absolute paths: `/Volumes/catalog/schema/volume/file.yxmd`
- For DBFS: use `dbfs:/path/to/file.yxmd` prefix
- Verify the file exists: `dbutils.fs.ls("/Volumes/catalog/schema/volume/")`

**"Generated code has syntax errors"**
- Try a more capable model (`claude-opus-4-20250514`)
- Increase `max_retries` to 3-5
- Check if the container has unsupported tool types

**Cluster times out during conversion**
- Large workflows with many containers may take time
- Convert one container at a time using the `container_name` widget
- Increase cluster auto-termination timeout

### Getting Help

- Check `examples/` directory for sample configs
- Use `list_containers` mode to inspect your workflow before converting
- Use `dry_run` mode to verify parsing without API calls
- Use `show_columns` mode (rule-based) to inspect column usage

## Requirements

- Python 3.10+
- `anthropic>=0.39.0` (optional for AI mode, falls back to urllib)
- `pyyaml>=6.0`
- `jinja2>=3.0`
- Databricks Runtime 13.0+ (for notebook execution)

## License

MIT
