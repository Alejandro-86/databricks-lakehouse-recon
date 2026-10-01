"""CLI: bootstrap, generate+ingest, transform, reconcile, maintain, serve, demo."""

from __future__ import annotations

import sys

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from databricks_lakehouse_recon.config import load_settings
from databricks_lakehouse_recon.ddl import bootstrap
from databricks_lakehouse_recon.generator import TransactionGenerator
from databricks_lakehouse_recon.ingestion import ingest_batch
from databricks_lakehouse_recon.maintenance import DeltaMaintenance
from databricks_lakehouse_recon.pipeline import run_reconciliation
from databricks_lakehouse_recon.reconciliation import ReconciliationReport
from databricks_lakehouse_recon.spark_session import get_local_spark, replace_table
from databricks_lakehouse_recon.transforms import build_gold, build_silver

app = typer.Typer(add_completion=False, help="Delta + Unity Catalog medallion with reconciliation controls")
console = Console()


def _render(report: ReconciliationReport) -> None:
    table = Table(title="Reconciliation")
    for col in ("check", "partitions", "breaks", "result"):
        table.add_column(col)
    for r in report.results:
        table.add_row(r.check, str(r.partitions_compared), str(len(r.breaks)),
                      "[green]pass[/green]" if r.passed else "[red]FAIL[/red]")
    console.print(table)
    for b in report.breaks:
        colour = "red" if b.severity.value == "error" else "yellow"
        console.print(f"[{colour}]{b.describe()}[/{colour}]")


@app.command()
def demo(count: int = 2000, seed: int = 42, inject_break: bool = False) -> None:
    """End-to-end: bootstrap, generate, ingest, transform, reconcile — all local."""
    settings = load_settings()
    spark = get_local_spark(warehouse_path=settings.warehouse_path)
    bootstrap(spark, settings)

    generator = TransactionGenerator(seed=seed)
    events = generator.generate(count)
    written = ingest_batch(spark, settings, events)
    console.print(f"[green]landed[/green] {written} row(s) in bronze ({generator.manifest.unique_events} unique)")

    silver, rejected = build_silver(spark, settings)
    replace_table(spark, silver, settings.silver_transactions)
    replace_table(spark, rejected, settings.silver_transactions + "_rejected")

    gold = build_gold(spark, settings, inject_break=inject_break)
    replace_table(spark, gold["fct_transaction"], settings.gold_fact)
    replace_table(spark, gold["dim_account"], settings.gold_dim_account)
    console.print("[green]built[/green] silver + gold")

    report = run_reconciliation(spark, settings)
    _render(report)
    spark.stop()
    if not report.passed:
        sys.exit(1)


@app.command()
def maintain(
    table: str = typer.Option(None),  # noqa: B008 — Typer's own documented default pattern
    zorder: list[str] = typer.Option(None),  # noqa: B008
) -> None:
    settings = load_settings()
    spark = get_local_spark(warehouse_path=settings.warehouse_path)
    target = table or settings.bronze_transactions
    ops = DeltaMaintenance(spark)
    ops.optimize(target, zorder or None)
    console.print(f"[green]optimized[/green] {target}" + (f" ZORDER BY {zorder}" if zorder else ""))
    spark.stop()


@app.command()
def history(table: str = typer.Option(None), limit: int = 10) -> None:
    settings = load_settings()
    spark = get_local_spark(warehouse_path=settings.warehouse_path)
    target = table or settings.bronze_transactions
    ops = DeltaMaintenance(spark)
    rows = ops.history(target, limit=limit).select("version", "timestamp", "operation").collect()
    grid = Table(title=f"history: {target}")
    for c in ("version", "timestamp", "operation"):
        grid.add_column(c)
    for row in rows:
        grid.add_row(str(row["version"]), str(row["timestamp"]), str(row["operation"]))
    console.print(grid)
    spark.stop()


@app.command()
def serve(port: int = 8000) -> None:
    """Serve gold's dim_account over a thin FastAPI read layer (Topic 3 architecture)."""
    from databricks_lakehouse_recon.serving import build_app

    settings = load_settings()
    spark = get_local_spark(warehouse_path=settings.warehouse_path)
    rows = [row.asDict() for row in spark.read.table(settings.gold_dim_account).collect()]
    spark.stop()
    uvicorn.run(build_app(rows), host="0.0.0.0", port=port)


if __name__ == "__main__":  # pragma: no cover
    app()
