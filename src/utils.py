"""
Utility functions for the converter CLI.
"""


def print_banner():
    """Print the application banner."""
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Alteryx → PySpark AI Converter (Powered by Claude)   ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()


def print_summary(results: list):
    """Print a summary table of conversion results."""
    print("\n" + "=" * 70)
    print("📊 CONVERSION SUMMARY")
    print("=" * 70)
    print(f"{'Container':<35} {'Status':<15} {'Time':<8} {'Tools':<6}")
    print("-" * 70)
    for r in results:
        name = r["container"][:34]
        print(f"{name:<35} {r['status']:<15} {r['time']:<8} {r['tools']:<6}")
    print("-" * 70)
    success = sum(1 for r in results if "✅" in r["status"])
    failed = len(results) - success
    print(f"Total: {len(results)} containers | ✅ {success} success | ❌ {failed} failed")
    if all("✅" in r["status"] for r in results):
        print("\n🎉 All containers converted successfully!")
    print()
