#!/usr/bin/env python3
"""
Alteryx to PySpark AI Converter - Main Entry Point
===================================================
Uses Claude AI to convert Alteryx .yxmd workflows into production-ready
PySpark code, generating a single unified Databricks notebook for the
entire workflow.

Processes ALL tools in the workflow — both those inside ToolContainer
elements and those at the root level — as one combined pipeline.

Usage:
    python convert.py <workflow.yxmd> [--output-dir ./output]

Environment:
    ANTHROPIC_API_KEY  - Required. Your Claude API key.
    CLAUDE_MODEL       - Optional. Default: claude-sonnet-4-20250514
"""

import argparse
import os
import sys
import time
from pathlib import Path

from src.parser import AlteryxWorkflowParser
from src.ai_generator import ClaudeCodeGenerator
from src.utils import print_banner, print_summary


def main():
    parser = argparse.ArgumentParser(
        description="Convert Alteryx .yxmd workflows to PySpark using Claude AI"
    )
    parser.add_argument(
        "workflow",
        help="Path to the .yxmd Alteryx workflow file",
    )
    parser.add_argument(
        "--output-dir", "-o",
        default="./output",
        help="Directory to write generated PySpark files (default: ./output)",
    )
    parser.add_argument(
        "--list-containers", "-l",
        action="store_true",
        help="List all containers and root-level tools in the workflow and exit.",
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
        help="Claude model to use (default: claude-sonnet-4-20250514)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and show workflow structure but don't call Claude AI.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Max retries on AI generation failure (default: 2)",
    )
    parser.add_argument(
        "--source-tables-config",
        default=None,
        help="Optional JSON file mapping Alteryx input tool IDs/names to "
             "Databricks catalog.schema.table paths.",
    )
    args = parser.parse_args()

    print_banner()

    # ── Validate inputs ──────────────────────────────────────────────
    workflow_path = Path(args.workflow)
    if not workflow_path.exists():
        print(f"  Workflow file not found: {workflow_path}")
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key and not args.dry_run and not args.list_containers:
        print("  ANTHROPIC_API_KEY environment variable is required.")
        print("   Set it with: export ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    # ── Parse the workflow ───────────────────────────────────────────
    print(f"Parsing workflow: {workflow_path.name}")
    t0 = time.time()
    parser_obj = AlteryxWorkflowParser(str(workflow_path))
    workflow = parser_obj.parse()
    parse_time = time.time() - t0

    root_tools = workflow.get_root_tools()
    container_tool_count = sum(len(c.child_tools) for c in workflow.containers)

    print(f"   Parsed in {parse_time:.1f}s")
    print(f"   Containers: {len(workflow.containers)}")
    print(f"   Tools in containers: {container_tool_count}")
    print(f"   Root-level tools: {len(root_tools)}")
    print(f"   Total tools: {len(workflow.all_tools)}")
    print(f"   Total connections: {len(workflow.connections)}")

    # ── List containers mode ─────────────────────────────────────────
    if args.list_containers:
        print("\nWorkflow structure:")
        print("-" * 60)
        idx = 1
        for container in workflow.containers:
            tool_count = len(container.child_tools)
            disabled_tag = " [DISABLED]" if container.disabled else ""
            print(f"   {idx:2d}. Container: {container.name:<35s} ({tool_count} tools){disabled_tag}")
            idx += 1
        if root_tools:
            print(f"   {idx:2d}. Root-level tools                        ({len(root_tools)} tools)")
        print("-" * 60)
        print(f"   Total: {len(workflow.all_tools)} tools, {len(workflow.connections)} connections")
        print(f"\n   All tools will be converted as a single unified notebook.")
        sys.exit(0)

    # ── Dry-run mode ─────────────────────────────────────────────────
    unified_ctx = workflow.get_unified_context()

    if args.dry_run:
        print("\nDry-run mode -- unified workflow context:\n")
        print(f"  Total tools: {len(unified_ctx['tools'])}")
        print(f"  Total connections: {len(unified_ctx['internal_connections'])}")
        print(f"  Containers: {len(unified_ctx['sub_containers'])}")
        for sc in unified_ctx["sub_containers"]:
            print(f"    - {sc.name} ({len(sc.child_tools)} tools)")
        print(f"  Text inputs: {len(unified_ctx['text_input_data'])}")
        print()
        print("  All tools will be converted as a single unified notebook.")
        sys.exit(0)

    # ── Generate PySpark code via Claude AI ──────────────────────────
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load source tables config if provided
    source_tables = None
    if args.source_tables_config:
        import json
        with open(args.source_tables_config) as f:
            source_tables = json.load(f)

    generator = ClaudeCodeGenerator(
        api_key=api_key,
        model=args.model,
        max_retries=args.max_retries,
        source_tables_config=source_tables,
    )

    container = unified_ctx["container"]

    # Use workflow filename as output name
    workflow_name = workflow_path.stem
    safe_name = workflow_name.lower().replace(" ", "_").replace("-", "_")
    safe_name = "".join(c for c in safe_name if c.isalnum() or c == "_")

    print(f"\n{'='*60}")
    print(f"Generating unified notebook: {safe_name}.py")
    print(f"  Tools: {len(unified_ctx['tools'])}")
    print(f"  Connections: {len(unified_ctx['internal_connections'])}")
    print(f"{'='*60}")

    t0 = time.time()
    results = []

    try:
        code = generator.generate_container_code(
            container=container,
            context=unified_ctx,
            workflow=workflow,
        )
        gen_time = time.time() - t0

        output_file = output_dir / f"{safe_name}.py"
        output_file.write_text(code, encoding="utf-8")

        results.append({
            "container": f"{workflow_name} (unified)",
            "file": str(output_file),
            "status": "Success",
            "time": f"{gen_time:.1f}s",
            "tools": len(unified_ctx["tools"]),
        })
        print(f"   Written to: {output_file} ({gen_time:.1f}s)")

    except Exception as e:
        gen_time = time.time() - t0
        results.append({
            "container": f"{workflow_name} (unified)",
            "file": "-",
            "status": f"Failed: {str(e)[:80]}",
            "time": f"{gen_time:.1f}s",
            "tools": len(unified_ctx["tools"]),
        })
        print(f"   Failed: {e}")

    # ── Summary ──────────────────────────────────────────────────────
    print_summary(results)


if __name__ == "__main__":
    main()
