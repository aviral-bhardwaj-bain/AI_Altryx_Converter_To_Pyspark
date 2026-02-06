"""
Alteryx to PySpark/Databricks Converter Framework.

Two-phase generic converter:
- Phase 1: Parse any Alteryx .yxmd workflow into intermediate JSON
- Phase 2: Generate Databricks notebooks from intermediate JSON + user config
"""

__version__ = "1.0.0"
