"""Delta Lake + Unity Catalog medallion lakehouse with reconciliation controls.

The Databricks-native counterpart to `lakehouse-recon-platform` (Trino/Iceberg).
Same reconciliation philosophy, same proven engine — the warehouse adapter is the
only thing that changed, by design (see `spark_control_totals.py`).
"""

__version__ = "0.1.0"
