# Alteryx to PySpark AI Converter (Claude-Powered)

A next-generation Alteryx workflow converter that uses **Claude AI** to generate production-ready PySpark code — one file per container/module. Unlike rule-based converters that do mechanical 1:1 tool translation, this tool sends the **full context** of each container (tools, connections, expressions, inline data, data flow) to Claude, which generates idiomatic, well-commented PySpark code that a human would actually write.

## Why AI-Powered?

| Aspect | Rule-Based Converter | AI-Powered Converter |
|--------|---------------------|---------------------|
| Expression handling | Regex-based, brittle | Understands semantic intent |
| Code quality | Mechanical, verbose | Idiomatic, clean PySpark |
| Complex logic | Breaks on nested IF/ELSE, CrossTab | Handles any complexity |
| Business context | None — just tool → code mapping | Understands what the workflow does |
| Output | One giant file | One file per container/module |
| Join handling | Often misses multi-output routing | Tracks J/L/R outputs correctly |

## Quick Start

### 1. Install

```bash
pip install anthropic   # optional, falls back to urllib
```

### 2. Set your API key

```bash
export ANTHROPIC_API_KEY=sk-ant-api03-...
```

### 3. Run

```bash
# Convert all containers
python convert.py my_workflow.yxmd -o ./output

# Convert a specific container
python convert.py my_workflow.yxmd -c mod_provider -o ./output

# List containers first
python convert.py my_workflow.yxmd --list-containers

# Dry run (parse only, no AI calls)
python convert.py my_workflow.yxmd --dry-run
```

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────┐
│  .yxmd XML File │────▶│  XML Parser  │────▶│Context Builder │────▶│ Claude AI│
│                 │     │              │     │               │     │          │
│ - Nodes         │     │ - Containers │     │ - Tools+Config│     │ Generates│
│ - Connections   │     │ - Tools      │     │ - Data Flow   │     │ PySpark  │
│ - Configuration │     │ - Connections│     │ - Inline Data │     │ Code     │
└─────────────────┘     └──────────────┘     │ - External I/O│     └────┬─────┘
                                             └───────────────┘          │
                                                                        ▼
                                                              ┌──────────────────┐
                                                              │ Output Files     │
                                                              │ mod_provider.py  │
                                                              │ mod_product.py   │
                                                              │ mod_episode.py   │
                                                              └──────────────────┘
```

### How It Works

1. **Parse**: The XML parser extracts every container, tool, connection, TextInput data, and configuration from the `.yxmd` file
2. **Context Build**: For each container, the context builder assembles:
   - All tools inside with full XML configuration
   - Internal connections (data flow between tools)
   - External inputs (which upstream tools/containers feed data in)
   - External outputs (where data flows to next)
   - Sub-containers (nested groups)
   - Inline data from TextInput tools
3. **AI Generate**: This rich context is sent to Claude with a detailed system prompt that teaches it Alteryx → PySpark translation patterns
4. **Validate**: Generated code is syntax-checked and validated
5. **Output**: One `.py` file per container, ready for Databricks

## Project Structure

```
alteryx_ai_converter/
├── convert.py              # CLI entry point
├── requirements.txt
├── README.md
├── src/
│   ├── __init__.py
│   ├── parser.py           # Alteryx XML parser
│   ├── models.py           # Data models (Workflow, Container, Tool, Connection)
│   ├── context_builder.py  # Builds rich prompts from parsed data
│   ├── ai_generator.py     # Claude API integration
│   └── utils.py            # CLI utilities
├── examples/
│   └── source_tables.json  # Example table mapping config
└── output/                 # Generated PySpark files go here
```

## CLI Options

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

## Source Table Mapping

Create a JSON file to map Alteryx input sources to your Databricks tables:

```json
{
    "nps_fact": "gold_catalog.insurance.nps_fact",
    "dim_provider": "gold_catalog.insurance.dim_provider",
    "dim_time": "gold_catalog.insurance.dim_time"
}
```

Pass it with `--source-tables-config`:

```bash
python convert.py workflow.yxmd --source-tables-config source_tables.json
```

## Use in Databricks Notebook

```python
# Upload the converter to your workspace, then:
import sys
sys.path.append("/Workspace/Users/you/alteryx_ai_converter")

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
    api_key=dbutils.secrets.get("my-scope", "anthropic-key"),
    model="claude-sonnet-4-20250514",
)

context = workflow.get_container_context(container.tool_id)
code = generator.generate_container_code(
    container=container,
    context=context,
    workflow=workflow,
)
print(code)
```

## Supported Alteryx Tools

The AI converter handles **all** Alteryx tools by sending their full XML configuration to Claude. Key tools with special handling:

- **Filter** → `.filter()` with True/False output tracking
- **Formula** → `.withColumn()` with full expression conversion
- **Join** → `.join()` with Join/Left/Right output tracking
- **Select** → Column selection, renaming, reordering
- **Union** → `.unionByName()`
- **Summarize** → `.groupBy().agg()`
- **CrossTab** → `.groupBy().pivot().agg()`
- **TextInput** → `spark.createDataFrame()` with inline data
- **LockIn* (In-DB tools)** → Treated as their standard equivalents
- **Sort, Unique, Sample, RegEx, etc.** → All supported

## Requirements

- Python 3.8+
- `anthropic` SDK (optional, urllib fallback available)
- Anthropic API key with Claude access

## Model Selection

| Model | Best For | Cost |
|-------|----------|------|
| `claude-sonnet-4-20250514` | Best balance of quality/speed/cost (recommended) | $$ |
| `claude-opus-4-20250514` | Most complex workflows with intricate logic | $$$$ |
| `claude-haiku-4-5-20251001` | Simple containers, budget-conscious | $ |

## Databricks Notebooks

The `notebooks/` directory contains the full converter adapted as **Databricks notebooks** with proper inter-notebook connections via `%run`.

### Notebook Structure

```
notebooks/
├── convert_workflow.py     # Main orchestrator (run this one)
├── config.py               # Widgets, secrets, validation
├── ai_generator.py         # Claude API integration
├── context_builder.py      # Prompt construction
├── parser.py               # Alteryx XML parser
├── models.py               # Data model classes
└── utils.py                # Display helpers
```

### Notebook Dependency Chain

```
convert_workflow (entry point)
  |-- %run ./config          --> widgets, secrets, validation
  |-- %run ./ai_generator    --> Claude API integration
  |     |-- %run ./context_builder  --> prompt building
  |           |-- %run ./models     --> data classes
  |-- %run ./parser          --> XML parsing
  |     |-- %run ./models     --> (already loaded)
  |-- %run ./utils           --> display helpers
```

### Key Differences from CLI Version

| Aspect | CLI (`convert.py`) | Databricks (`notebooks/`) |
|--------|-------------------|--------------------------|
| Parameters | `argparse` CLI args | `dbutils.widgets` (interactive UI) |
| API Key | `ANTHROPIC_API_KEY` env var | `dbutils.secrets.get()` |
| File paths | Local filesystem | Volumes, DBFS, or Workspace paths |
| Module imports | `from src.parser import ...` | `%run ./parser` |
| Output display | `print()` to terminal | `print()` + `displayHTML()` tables |
| Exit handling | `sys.exit()` | `dbutils.notebook.exit()` |

### Databricks Setup

1. **Upload notebooks** to your Databricks workspace:
   ```
   /Workspace/Users/<you>/alteryx_converter/
   ```

2. **Store your API key** in Databricks Secrets:
   ```bash
   databricks secrets create-scope alteryx-converter
   databricks secrets put-secret alteryx-converter anthropic-api-key
   ```

3. **Upload your `.yxmd` file** to a Databricks Volume:
   ```
   /Volumes/<catalog>/<schema>/<volume>/workflow.yxmd
   ```

4. **Open `convert_workflow`** notebook, configure the widgets, and click **Run All**.

### Widget Parameters

| Widget | Default | Description |
|--------|---------|-------------|
| `workflow_path` | *(required)* | Path to `.yxmd` file (Volume or DBFS) |
| `output_dir` | `/Workspace/Users/shared/alteryx_converter/output` | Where to write generated files |
| `container_name` | *(blank = all)* | Convert a specific container only |
| `model` | `claude-sonnet-4-20250514` | Claude model to use |
| `max_retries` | `2` | API call retries per container |
| `secret_scope` | `alteryx-converter` | Databricks secret scope |
| `secret_key` | `anthropic-api-key` | Secret key for the API key |
| `run_mode` | `convert` | `convert`, `list_containers`, or `dry_run` |
| `source_tables_json` | *(blank)* | Optional JSON mapping file path |

### Running from Another Notebook

You can also call the converter programmatically from another notebook:

```python
result = dbutils.notebook.run(
    "/Workspace/Users/<you>/alteryx_converter/convert_workflow",
    timeout_seconds=3600,
    arguments={
        "workflow_path": "/Volumes/catalog/schema/vol/workflow.yxmd",
        "output_dir": "/Workspace/Users/<you>/output",
        "run_mode": "convert",
        "model": "claude-sonnet-4-20250514",
        "container_name": "Provider Module",
    }
)
print(result)  # "COMPLETE: 1/1 containers converted successfully"
```

## License

MIT
