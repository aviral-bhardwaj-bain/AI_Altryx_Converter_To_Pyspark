"""CLI for Phase 2: Databricks Notebook Generator."""

import argparse
import json
import sys
import logging

from .generator.notebook_generator import NotebookGenerator
from .validators.syntax_validator import SyntaxValidator
from .validators.mapping_validator import MappingValidator
from .config.column_mapping import ColumnMapper
from ..phase1_parser.output.json_writer import JSONWriter
from ..phase1_parser.parser.formula_extractor import FormulaExtractor


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_generate(args: argparse.Namespace) -> None:
    """Generate a Databricks notebook."""
    generator = NotebookGenerator(
        intermediate_json_path=args.intermediate,
        config_path=args.config,
    )

    output_path = args.output or generator.config.output.notebook_path
    if not output_path:
        output_path = f"./output/{generator.config.container_name}.py"

    path = generator.generate_and_save(output_path)

    # Validate generated code
    if not args.skip_validation:
        from ..common.file_utils import read_text
        code = read_text(path)
        validator = SyntaxValidator()
        result = validator.validate(code)

        if not result["valid"]:
            print(f"\nWARNING: Generated code has issues:")
            for issue in result["issues"]:
                print(f"  - {issue['type']}: {issue['message']}")
        else:
            print("Syntax validation: PASSED")

    print(f"\nGenerated notebook: {path}")


def cmd_validate_mappings(args: argparse.Namespace) -> None:
    """Validate column mappings against the workflow."""
    import yaml

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    workflow = JSONWriter.load(args.intermediate)
    column_mapper = ColumnMapper(config.get("column_mappings", {}))
    validator = MappingValidator(workflow, column_mapper)

    container_name = config.get("container_name", "")
    container = workflow.get_container_by_name(container_name)
    if not container:
        # Try by ID
        try:
            container = workflow.get_container(int(container_name))
        except (ValueError, TypeError):
            pass

    if not container:
        print(f"Container '{container_name}' not found")
        sys.exit(1)

    result = validator.validate_container_mappings(container.tool_id)

    print(f"\nColumn Mapping Validation: {container.name}")
    print("=" * 60)
    print(f"  Total columns: {result['total_columns']}")
    print(f"  Coverage: {result['mapping_coverage']}")

    if result["mapped"]:
        print(f"\n  Mapped ({len(result['mapped'])}):")
        for m in result["mapped"]:
            print(f"    {m}")

    if result["unmapped"]:
        print(f"\n  Unmapped ({len(result['unmapped'])}) - will use original names:")
        for u in result["unmapped"]:
            print(f"    {u}")

    if result["unused_mappings"]:
        print(f"\n  Unused mappings ({len(result['unused_mappings'])}):")
        for u in result["unused_mappings"]:
            print(f"    {u}")


def cmd_show_columns(args: argparse.Namespace) -> None:
    """Show columns used by a container."""
    workflow = JSONWriter.load(args.intermediate)
    formula_extractor = FormulaExtractor()

    container = workflow.get_container_by_name(args.container)
    if not container:
        try:
            container = workflow.get_container(int(args.container))
        except (ValueError, TypeError):
            pass

    if not container:
        print(f"Container '{args.container}' not found")
        sys.exit(1)

    all_columns: set[str] = set()
    tools = workflow.get_container_tools(container.tool_id)

    # Also check sub-containers
    for sub_id in container.child_container_ids:
        sub_tools = workflow.get_container_tools(sub_id)
        tools.extend(sub_tools)

    columns_by_tool: dict[str, set[str]] = {}

    for tool in tools:
        cols = formula_extractor.extract_all_columns_from_tool(tool.configuration)
        all_columns.update(cols)
        tool_desc = f"{tool.tool_type} ({tool.tool_id})"
        if tool.annotation:
            tool_desc += f": {tool.annotation}"
        if cols:
            columns_by_tool[tool_desc] = cols

    print(f"\nColumns in container: {container.name}")
    print("=" * 60)
    print(f"\n  All unique columns ({len(all_columns)}):")
    for col in sorted(all_columns):
        print(f"    - {col}")

    if args.by_tool:
        print(f"\n  Columns by tool:")
        for tool_desc, cols in sorted(columns_by_tool.items()):
            print(f"\n    {tool_desc}:")
            for col in sorted(cols):
                print(f"      - {col}")


def cmd_suggest_mappings(args: argparse.Namespace) -> None:
    """Suggest column mappings by fuzzy matching."""
    workflow = JSONWriter.load(args.intermediate)
    column_mapper = ColumnMapper({})
    validator = MappingValidator(workflow, column_mapper)

    container = workflow.get_container_by_name(args.container)
    if not container:
        try:
            container = workflow.get_container(int(args.container))
        except (ValueError, TypeError):
            pass

    if not container:
        print(f"Container '{args.container}' not found")
        sys.exit(1)

    # Get available columns (from config or table)
    available_columns = []
    if args.columns_file:
        with open(args.columns_file) as f:
            data = json.load(f)
            if isinstance(data, list):
                available_columns = data
            elif isinstance(data, dict):
                for cols in data.values():
                    if isinstance(cols, list):
                        available_columns.extend(cols)

    if not available_columns:
        print("No available columns provided. Use --columns-file.")
        sys.exit(1)

    suggestions = validator.suggest_mappings(
        container.tool_id, available_columns
    )

    print(f"\nSuggested Column Mappings: {container.name}")
    print("=" * 60)

    # Output as YAML-friendly format
    print("\ncolumn_mappings:")
    for s in suggestions:
        if s["suggested"]:
            confidence = s["confidence"]
            print(f"  {s['alteryx_col']}: {s['suggested']}  # {confidence}")
        else:
            print(f"  # {s['alteryx_col']}: ???  # no match found")


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="phase2_generator",
        description="Databricks Notebook Generator - Phase 2",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # generate command
    gen_cmd = subparsers.add_parser(
        "generate",
        help="Generate a Databricks notebook",
    )
    gen_cmd.add_argument(
        "--intermediate", "-i",
        required=True,
        help="Path to intermediate JSON from Phase 1",
    )
    gen_cmd.add_argument(
        "--config", "-c",
        required=True,
        help="Path to generation config YAML",
    )
    gen_cmd.add_argument(
        "--output", "-o",
        help="Output path for the notebook",
    )
    gen_cmd.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip syntax validation of generated code",
    )

    # validate-mappings command
    val_cmd = subparsers.add_parser(
        "validate-mappings",
        help="Validate column mappings",
    )
    val_cmd.add_argument(
        "--intermediate", "-i",
        required=True,
        help="Path to intermediate JSON",
    )
    val_cmd.add_argument(
        "--config", "-c",
        required=True,
        help="Path to generation config YAML",
    )

    # show-columns command
    cols_cmd = subparsers.add_parser(
        "show-columns",
        help="Show columns used by a container",
    )
    cols_cmd.add_argument(
        "--intermediate", "-i",
        required=True,
        help="Path to intermediate JSON",
    )
    cols_cmd.add_argument(
        "--container", "-c",
        required=True,
        help="Container name or tool ID",
    )
    cols_cmd.add_argument(
        "--by-tool",
        action="store_true",
        help="Group columns by tool",
    )

    # suggest-mappings command
    sug_cmd = subparsers.add_parser(
        "suggest-mappings",
        help="Suggest column mappings",
    )
    sug_cmd.add_argument(
        "--intermediate", "-i",
        required=True,
        help="Path to intermediate JSON",
    )
    sug_cmd.add_argument(
        "--container", "-c",
        required=True,
        help="Container name or tool ID",
    )
    sug_cmd.add_argument(
        "--columns-file",
        help="JSON file with available column names",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    commands = {
        "generate": cmd_generate,
        "validate-mappings": cmd_validate_mappings,
        "show-columns": cmd_show_columns,
        "suggest-mappings": cmd_suggest_mappings,
    }

    cmd_func = commands.get(args.command)
    if cmd_func:
        cmd_func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
