"""Runtime configuration — Unity Catalog's three-level namespace throughout.

Unity Catalog addresses every table as ``catalog.schema.table``. Keeping that
addressing in one place means a workspace move (dev catalog -> prod catalog)
is a config change, not a code change.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DBX_LAKEHOUSE_", env_file=".env", extra="ignore")

    # --- Unity Catalog ---
    catalog: str = "lakehouse_recon"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    # --- Storage (Unity Catalog manages this when running on a real workspace;
    # locally this is a plain filesystem path so tests need no workspace at all) ---
    warehouse_path: str = "./warehouse"

    # --- Auto Loader (Databricks-only; see ingestion.py) ---
    autoloader_source_path: str = "/Volumes/lakehouse_recon/bronze/landing"
    autoloader_checkpoint_path: str = "/Volumes/lakehouse_recon/bronze/_checkpoints/transactions"

    # --- Reconciliation tolerances — zero by default; amounts are integer minor units ---
    amount_tolerance_minor: int = Field(default=0, ge=0)
    relative_tolerance: float = Field(default=0.0, ge=0.0, le=0.01)

    def table(self, schema: str, name: str) -> str:
        """Unity Catalog three-level name: catalog.schema.table."""
        return f"{self.catalog}.{schema}.{name}"

    @property
    def bronze_transactions(self) -> str:
        return self.table(self.bronze_schema, "transactions_raw")

    @property
    def silver_transactions(self) -> str:
        return self.table(self.silver_schema, "silver_transactions")

    @property
    def gold_fact(self) -> str:
        return self.table(self.gold_schema, "fct_transaction")

    @property
    def gold_dim_account(self) -> str:
        return self.table(self.gold_schema, "dim_account")


def load_settings(**overrides: object) -> Settings:
    return Settings(**overrides)  # type: ignore[arg-type]
