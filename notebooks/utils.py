# Databricks notebook source
# MAGIC %md
# MAGIC # Utility Functions
# MAGIC Utility functions for the converter, adapted for Databricks notebook output.
# MAGIC
# MAGIC **Usage:** This notebook is imported via `%run ./utils`

# COMMAND ----------

def print_banner():
    """Print the application banner."""
    print()
    print("=" * 60)
    print("   Alteryx -> PySpark AI Converter (Powered by Claude)")
    print("   Running on Databricks")
    print("=" * 60)
    print()

# COMMAND ----------

def print_summary(results: list):
    """Print a summary table of conversion results."""
    print("\n" + "=" * 70)
    print("CONVERSION SUMMARY")
    print("=" * 70)
    print(f"{'Container':<35} {'Status':<15} {'Time':<8} {'Tools':<6}")
    print("-" * 70)
    for r in results:
        name = r["container"][:34]
        print(f"{name:<35} {r['status']:<15} {r['time']:<8} {r['tools']:<6}")
    print("-" * 70)
    success = sum(1 for r in results if "Success" in r["status"])
    failed = len(results) - success
    print(f"Total: {len(results)} containers | {success} success | {failed} failed")
    if all("Success" in r["status"] for r in results):
        print("\nAll containers converted successfully!")
    print()

# COMMAND ----------

def display_summary_table(results: list):
    """Display conversion results as a Databricks-friendly HTML table."""
    html = """
    <style>
        .summary-table { border-collapse: collapse; width: 100%; font-family: monospace; }
        .summary-table th { background-color: #1B3A5C; color: white; padding: 8px 12px; text-align: left; }
        .summary-table td { padding: 6px 12px; border-bottom: 1px solid #ddd; }
        .summary-table tr:hover { background-color: #f5f5f5; }
        .status-success { color: #2e7d32; font-weight: bold; }
        .status-failed { color: #c62828; font-weight: bold; }
    </style>
    <table class="summary-table">
        <tr>
            <th>Container</th>
            <th>Status</th>
            <th>Time</th>
            <th>Tools</th>
            <th>Output File</th>
        </tr>
    """
    for r in results:
        status_class = "status-success" if "Success" in r["status"] else "status-failed"
        html += f"""
        <tr>
            <td>{r['container']}</td>
            <td class="{status_class}">{r['status']}</td>
            <td>{r['time']}</td>
            <td>{r['tools']}</td>
            <td>{r.get('file', '-')}</td>
        </tr>
        """
    html += "</table>"

    success = sum(1 for r in results if "Success" in r["status"])
    failed = len(results) - success
    html += f"<p><strong>Total:</strong> {len(results)} containers | {success} success | {failed} failed</p>"

    displayHTML(html)
