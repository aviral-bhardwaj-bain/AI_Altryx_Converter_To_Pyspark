"""CLI for Phase 1: Alteryx workflow parser."""

import argparse
import json
import sys
import logging

from .parser.workflow_parser import WorkflowParser
from .output.json_writer import JSONWriter


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_parse(args: argparse.Namespace) -> None:
    """Parse a workflow and save intermediate JSON."""
    parser = WorkflowParser()
    workflow = parser.parse(args.input)

    writer = JSONWriter()
    output_file = writer.write(workflow, args.output)

    print(f"Parsed workflow: {workflow.name}")
    print(f"  Containers: {len(workflow.containers)}")
    print(f"  Tools: {len(workflow.tools)}")
    print(f"  Connections: {len(workflow.connections)}")
    print(f"  Output: {output_file}")


def cmd_list_containers(args: argparse.Namespace) -> None:
    """List all containers in a parsed workflow."""
    workflow = JSONWriter.load(args.input)
    parser = WorkflowParser()
    summaries = parser.get_container_summary(workflow)

    if not summaries:
        print("No containers found in workflow.")
        return

    print(f"\nContainers in: {workflow.name}")
    print("=" * 80)

    for s in summaries:
        status = " [DISABLED]" if s["disabled"] else ""
        parent = f" (parent: {s['parent_container']})" if s["parent_container"] else ""
        print(f"\n  [{s['tool_id']}] {s['name']}{status}{parent}")
        print(f"    Tools: {s['num_tools']}, Sub-containers: {s['num_sub_containers']}")
        print(f"    Internal connections: {s['num_internal_connections']}")
        print(f"    External inputs: {s['num_external_inputs']}, outputs: {s['num_external_outputs']}")
        if s["tool_types"]:
            types_str = ", ".join(
                f"{t}: {c}" for t, c in sorted(s["tool_types"].items())
            )
            print(f"    Tool types: {types_str}")


def cmd_show_container(args: argparse.Namespace) -> None:
    """Show details of a specific container."""
    workflow = JSONWriter.load(args.input)

    # Find container by name or ID
    container = None
    try:
        container_id = int(args.container)
        container = workflow.get_container(container_id)
    except ValueError:
        container = workflow.get_container_by_name(args.container)

    if not container:
        print(f"Container not found: {args.container}")
        sys.exit(1)

    print(f"\nContainer: {container.name} (ID: {container.tool_id})")
    print("=" * 80)
    print(f"  Disabled: {container.disabled}")
    print(f"  Folded: {container.folded}")
    print(f"  Parent: {container.parent_container_id}")
    print(f"  Position: {container.position}")

    # List child tools
    tools = workflow.get_container_tools(container.tool_id)
    print(f"\n  Tools ({len(tools)}):")
    for tool in tools:
        ann = f" - {tool.annotation}" if tool.annotation else ""
        print(f"    [{tool.tool_id}] {tool.tool_type}{ann}")

    # Sub-containers
    if container.child_container_ids:
        print(f"\n  Sub-containers ({len(container.child_container_ids)}):")
        for sub_id in container.child_container_ids:
            sub = workflow.get_container(sub_id)
            if sub:
                print(f"    [{sub.tool_id}] {sub.name}")

    # Internal connections
    internal_conns = workflow.get_container_connections(container.tool_id)
    print(f"\n  Internal connections ({len(internal_conns)}):")
    for conn in internal_conns:
        print(
            f"    {conn.origin_tool_id}:{conn.origin_connection} "
            f"-> {conn.destination_tool_id}:{conn.destination_connection}"
        )

    # External inputs
    ext_inputs = workflow.get_container_external_inputs(container.tool_id)
    print(f"\n  External inputs ({len(ext_inputs)}):")
    for conn in ext_inputs:
        src = workflow.get_tool(conn.origin_tool_id)
        src_desc = f"{src.tool_type}" if src else "Unknown"
        if src and src.annotation:
            src_desc += f" ({src.annotation})"
        print(
            f"    From {conn.origin_tool_id} ({src_desc}):{conn.origin_connection} "
            f"-> {conn.destination_tool_id}:{conn.destination_connection}"
        )


def cmd_show_dependencies(args: argparse.Namespace) -> None:
    """Show dependencies of a specific container."""
    workflow = JSONWriter.load(args.input)
    parser = WorkflowParser()

    # Find container by name or ID
    container_id = None
    try:
        container_id = int(args.container)
    except ValueError:
        container = workflow.get_container_by_name(args.container)
        if container:
            container_id = container.tool_id

    if container_id is None:
        print(f"Container not found: {args.container}")
        sys.exit(1)

    deps = parser.get_container_dependencies(workflow, container_id)

    if "error" in deps:
        print(deps["error"])
        sys.exit(1)

    print(f"\nDependencies for: {deps['container_name']} (ID: {deps['container_id']})")
    print("=" * 80)

    print(f"\n  External data inputs ({len(deps['dependencies'])}):")
    for dep in deps["dependencies"]:
        print(
            f"    Tool {dep['source_tool_id']} ({dep['source_type']}: "
            f"{dep['source_annotation']}) "
            f"via {dep['source_connection']} "
            f"-> Tool {dep['destination_tool_id']} ({dep['destination_type']})"
        )

    if deps["columns_referenced"]:
        print(f"\n  Columns referenced ({len(deps['columns_referenced'])}):")
        for col in deps["columns_referenced"]:
            print(f"    - {col}")

    # Output as JSON if requested
    if hasattr(args, 'json') and args.json:
        print(f"\n  JSON:")
        print(json.dumps(deps, indent=2))


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="phase1_parser",
        description="Alteryx Workflow Parser - Phase 1",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # parse command
    parse_cmd = subparsers.add_parser(
        "parse",
        help="Parse an Alteryx .yxmd workflow",
    )
    parse_cmd.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the .yxmd file",
    )
    parse_cmd.add_argument(
        "--output", "-o",
        required=True,
        help="Output directory for intermediate JSON",
    )

    # list-containers command
    list_cmd = subparsers.add_parser(
        "list-containers",
        help="List all containers in a parsed workflow",
    )
    list_cmd.add_argument(
        "--input", "-i",
        required=True,
        help="Path to intermediate JSON",
    )

    # show-container command
    show_cmd = subparsers.add_parser(
        "show-container",
        help="Show details of a specific container",
    )
    show_cmd.add_argument(
        "--input", "-i",
        required=True,
        help="Path to intermediate JSON",
    )
    show_cmd.add_argument(
        "--container", "-c",
        required=True,
        help="Container name or tool ID",
    )

    # show-dependencies command
    deps_cmd = subparsers.add_parser(
        "show-dependencies",
        help="Show dependencies of a container",
    )
    deps_cmd.add_argument(
        "--input", "-i",
        required=True,
        help="Path to intermediate JSON",
    )
    deps_cmd.add_argument(
        "--container", "-c",
        required=True,
        help="Container name or tool ID",
    )
    deps_cmd.add_argument(
        "--json",
        action="store_true",
        help="Output dependencies as JSON",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    commands = {
        "parse": cmd_parse,
        "list-containers": cmd_list_containers,
        "show-container": cmd_show_container,
        "show-dependencies": cmd_show_dependencies,
    }

    cmd_func = commands.get(args.command)
    if cmd_func:
        cmd_func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
