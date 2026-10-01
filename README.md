# databricks-lakehouse-recon

A Delta Lake + Unity Catalog medallion lakehouse with the same philosophy as its
sibling project, [`lakehouse-recon-platform`](https://github.com/Alejandro-86/lakehouse-recon-platform)
(Trino + Iceberg): **reconciliation is a control that fails the run**, not a
dashboard nobody opens. This repo exists to prove that philosophy — and the
code backing it — genuinely ports to the Databricks-native stack, not just the
open-source lakehouse stack.

```
                 SOURCE (seeded generator + manifest)
                           │  ingest_batch() — local/tests
                           │  autoload_stream() — Auto Loader, Databricks-only
                           ▼
 ┌──────────────────────────────────────────────────────┐
 │ BRONZE  lakehouse_recon.bronze.transactions_raw       │  append-only Delta
 │ at-least-once duplicates retained on purpose          │  partitioned, event_date
 └──────┬─────────────────────────────────────────────────┘
        │  PySpark: deduplicate_bronze() (window fn), validity filter
        ▼
 ┌──────────────────────────────────────────────────────┐
 │ SILVER  silver_transactions                           │  one row per event_id
 │         silver_transactions_rejected                  │  quarantine, with reasons
 └──────┬─────────────────────────────────────────────────┘
        │  PySpark: star-schema build
        ▼
 ┌──────────────────────────────────────────────────────┐
 │ GOLD    fct_transaction                               │  additive minor-unit measure
 │         dim_account                                   │  conformed dimension
 └──────────────────────────────────────────────────────┘
        │
        ▼  thin FastAPI serving layer (serving.py) — Topic 3 architecture

 ╔══════════════════════════════════════════════════════════════╗
 ║ RECONCILIATION — pure engine, imports nothing from Spark      ║
 ║   bronze → silver   row count · amount sum  (dedup-aware)     ║
 ║   silver → gold     row count · amount sum                    ║
 ║   within silver     deduplication effectiveness                ║
 ║   within gold       referential integrity, fact → dimension    ║
 ╚══════════════════════════════════════════════════════════════╝
            breaks → non-zero exit code, same as the Trino version
```

## What's genuinely different from the Trino/Iceberg version — found by running it

Porting this wasn't a search-and-replace. Three real differences surfaced by
actually building and running the Delta version, documented where they live in
code rather than glossed over:

1. **Delta has no partition-transform equivalent to Iceberg's hidden
   partitioning.** `PARTITIONED BY (DATE(occurred_at))` is valid Iceberg DDL;
   on Delta it fails with `DELTA_OPERATION_NOT_ALLOWED`. Databricks' own idiom
   is a *generated column*; this repo uses the simpler, equally valid
   alternative (a plain column populated at write time) because generated
   columns need protocol support not reliably available outside a real Unity
   Catalog workspace. See `ddl.py`.
2. **`.mode("overwrite").saveAsTable()` isn't reliable** on this combination of
   Spark 3.5 + OSS `delta-spark` outside a real workspace — it goes through a
   `SupportsTruncate` code path that fails with `does not support truncate in
   batch mode`. `replace_table()` in `spark_session.py` does an explicit
   drop-then-create instead.
3. **Local Spark's session catalog is purely in-memory** — a table created in
   one process is invisible to the next, even against the same warehouse
   directory on disk, unless Hive support is enabled for a persistent (Derby)
   local metastore. Real Unity Catalog doesn't have this problem; a laptop
   running `demo` then `maintain` as two separate commands does.

## Delta vs. Iceberg maintenance operations — same concepts, different syntax

| Operation | Iceberg (`lakehouse-recon-platform`) | Delta (this repo) |
|---|---|---|
| Compaction | `ALTER TABLE ... EXECUTE optimize(...)` | `OPTIMIZE table [ZORDER BY (cols)]` |
| Orphan/old file cleanup | `remove_orphan_files` | `VACUUM table [RETAIN n HOURS]` |
| Time travel | `FOR VERSION/TIMESTAMP AS OF` | `VERSION AS OF` / `TIMESTAMP AS OF` (no `FOR`) |
| Rollback | `CALL system.rollback_to_snapshot(...)` | `RESTORE TABLE table TO VERSION AS OF n` |
| Audit trail | `"table$snapshots"` metadata table | `DESCRIBE HISTORY table` |
| Schema evolution | `ADD COLUMN` | `ADD COLUMN` (+ `mergeSchema` write option) |

All verified against a real local Delta table, not just written from memory —
see `test_transforms_ddl_maintenance.py`.

## Run it

```bash
pip install -e ".[dev]"

export DBX_LAKEHOUSE_CATALOG=spark_catalog   # local only — see "Unity Catalog locally" below

dbx-lakehouse demo --count 3000              # bootstrap, generate, ingest, transform, reconcile
dbx-lakehouse maintain --zorder account_id   # OPTIMIZE ... ZORDER BY, as a separate process
dbx-lakehouse history                        # DESCRIBE HISTORY — real Delta transaction log
dbx-lakehouse serve                          # FastAPI serving layer over gold
```

### Watch the controls catch something

```bash
dbx-lakehouse demo --count 3000 --inject-break
```

Drops FX conversions from the fact table — the same "someone added a WHERE
clause for a good local reason" incident shape as the Trino version's demo —
and the run exits 1 with the exact per-partition deltas.

### Unity Catalog locally

Production/a real workspace: set `DBX_LAKEHOUSE_CATALOG` to an actual Unity
Catalog catalog name — three-level `catalog.schema.table` addressing throughout
the codebase is unchanged. Locally there's no Unity Catalog metastore, so this
points at Spark's own default catalog (`spark_catalog`) instead; see
`docs/databricks_free_edition_setup.md` for pointing this same code at a real
workspace.

## Testing

```bash
pytest tests/ -v    # 34 tests, local Delta + Spark (Java 11, PySpark 3.5, delta-spark 3.3)
ruff check src/ tests/
mypy src/ --ignore-missing-imports
```

The reconciliation engine (`reconciliation.py`) is pure — no Spark import —
reused close to verbatim from `lakehouse-recon-platform`. The only new code
when porting warehouses was the adapter (`spark_control_totals.py`) and the
Delta-specific maintenance operations (`maintenance.py`): the separation of
concerns claim is demonstrated by what *didn't* need to change, not just
asserted.

## Honest scope

- **Auto Loader (`ingestion.py::autoload_stream`) is written for correctness
  but not exercised in CI** — `cloudFiles` is a Databricks-only source and
  needs a real cluster. `ingest_batch()` is the laptop-testable path used by
  the test suite and the CLI demo.
- **The serving layer reads gold into memory once at startup** — the honest
  size for a demo. At real scale this would be a sync job into Postgres/Redis,
  the same shape as `growth-data-platform`'s reverse-ETL pattern, not an
  in-process dict.
- **Single-node, synthetic data, local metastore.** No cluster sizing, no
  concurrency tuning, no cost story at scale, and the generated-column /
  overwrite-mode workarounds above are specifically local-environment
  workarounds — say so if asked, don't imply they're needed on a real
  workspace without having verified that.

## Layout

```
src/databricks_lakehouse_recon/
  reconciliation.py        # pure check engine — unchanged from lakehouse-recon-platform
  spark_control_totals.py  # Spark/Delta adapter — the only thing that changed porting it
  spark_session.py         # local Spark+Delta+Hive factory; replace_table() workaround
  pipeline.py               # which layers get compared, with which checks
  transforms.py             # bronze -> silver -> gold, plain PySpark
  ingestion.py               # ingest_batch() (local) + autoload_stream() (Databricks-only)
  maintenance.py             # OPTIMIZE/ZORDER, VACUUM, RESTORE, DESCRIBE HISTORY
  ddl.py  config.py  events.py  generator.py  serving.py  cli.py
docs/databricks_free_edition_setup.md   # pointing this code at a real workspace
```
