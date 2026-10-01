# Running this on a real Databricks workspace (Free Edition)

Everything in this repo runs locally with no workspace at all (`pytest`, the
CLI demo). This doc is for the next step: pointing the *same code* at a real
Databricks workspace to verify what can't be verified locally — genuine Unity
Catalog governance, Auto Loader, MLflow, and whether the local-only workarounds
documented in the README (`replace_table()`, the plain-column-instead-of-
generated-column choice) are actually necessary there or just a local quirk.

## 1. Get a workspace

Sign up at [Databricks Free Edition](https://www.databricks.com/). No card is
needed for the free tier; it includes Unity Catalog (catalogs/schemas/tables/
volumes), serverless compute for notebooks, the SQL editor, and dashboards.

## 2. Create a catalog and a personal access token

In the workspace UI: **Catalog → Create Catalog**, name it `lakehouse_recon`
(matches `Settings.catalog`'s default — no code change needed). Then
**Settings → Developer → Access tokens → Generate new token**.

## 3. Point the code at it

Two ways to run the actual package against the workspace rather than locally:

**Option A — a notebook.** Fastest path. Upload/paste the contents of
`src/databricks_lakehouse_recon/` as notebook cells (or `%pip install` the repo
from a Git folder). `spark` is already in scope — skip `spark_session.py`
entirely and call `ddl.bootstrap(spark, settings)`, `transforms.build_silver(...)`,
etc. directly. This is the fastest way to confirm genuine Unity Catalog
behaviour this weekend.

**Option B — Databricks Connect**, for running the CLI from a laptop against
the remote cluster:

```bash
pip install databricks-connect
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<the token from step 2>
export DBX_LAKEHOUSE_CATALOG=lakehouse_recon
```

`get_local_spark()` would need swapping for `DatabricksSession.builder.getOrCreate()`
in this mode — not currently wired up, since the point this weekend is reading
and understanding the Spark/Delta code, not productionising a connection layer.

## 4. Things specifically worth checking for real, that can't be checked locally

- **Does `.mode("overwrite").saveAsTable()` actually fail here too**, or is
  that a quirk of OSS `delta-spark` outside Unity Catalog? If it works fine,
  `replace_table()`'s workaround is locally-necessary only — know that before
  claiming it's a Databricks-wide issue in an interview.
- **Generated columns** (`event_date DATE GENERATED ALWAYS AS (CAST(occurred_at AS DATE))`)
  — does `ddl.py`'s commented-out approach work here? If so, that's the more
  idiomatic version to describe, with this repo's plain-column version framed
  as the local fallback.
- **Unity Catalog's automatic lineage** — Catalog Explorer → a table → the
  Lineage tab. Compare this mentally against hand-rolled lineage tracking
  (e.g. GSK's BigQuery Data Catalog policy tags) — a real, specific comparison
  is worth more in an interview than reciting that Unity Catalog "has lineage."
- **One MLflow run**, logged from a notebook (`mlflow.log_metric(...)`) —
  enough to go from zero experience to "I've used it once, briefly," which is
  an honest, truthful upgrade from "never touched it."
- **Auto Loader for real** — land a JSON file in a Unity Catalog volume, point
  `ingestion.autoload_stream` at it, watch it pick the file up incrementally.
  This is the one piece of this repo that is *written* but not yet *proven*.

## 5. What to actually produce from this weekend

Not a second polished repo — three to five **specific, true observations**
about where the real workspace matched or diverged from the local
approximation. That's the material worth having for Monday, not a bigger
codebase.
