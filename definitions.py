"""Dagster definitions for the FSC modern data stack (medallion on DuckLake).

Manual-trigger assets, no schedules yet. Lineage:

  bronze (dlt)                     silver (dbt)          gold (dbt)
  ------------------               -------------         --------------------
  salesforce_bronze   ─┐
  dataverse_bronze    ─┼─────────▶ silver ────────────▶ gold
  salesforce_id_sync  ─┘ (hard-delete "id 拉齐", manual, feeds silver anti-join)
  dataverse_id_sync   ─┘

  - bronze: dlt loads all columns into DuckLake `bronze` (incremental merge).
  - *_id_sync: manual full live-id pull so silver can drop hard-deleted rows.
  - silver: regenerate models from silver_config, `dbt run` the silver layer
    (specified columns, dedupe, soft/hard-delete removal), publish parquet to blob.
  - gold: `dbt run` the gold snapshot models (ports of daily.py/monthly_test.py),
    stamped with a snapshot_date, publish parquet to blob.

Blob publishing targets the `duck` container (see publish.py).
"""

import os
import subprocess
from datetime import date

from dagster import AssetExecutionContext, Definitions, asset

import dataverse_bronze as dv
import gen_silver
import publish
import sf_bronze as sf

# Project root = this file's directory (forward-slash, portable Linux/Windows).
BASE = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
DBT_DIR = f"{BASE}/dbt"


def _run(context, cmd, cwd=None):
    context.log.info("$ " + " ".join(cmd))
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if proc.stdout:
        context.log.info(proc.stdout)
    if proc.stderr:
        context.log.info(proc.stderr)
    if proc.returncode != 0:
        raise Exception(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.stdout


def _dbt(context, *args):
    return _run(context, ["dbt", *args, "--profiles-dir", DBT_DIR], cwd=DBT_DIR)


# ---------------------------------------------------------------- bronze (dlt)
@asset(group_name="bronze")
def salesforce_bronze(context: AssetExecutionContext):
    """Incrementally load Salesforce entities into DuckLake bronze via dlt."""
    info = sf.run_bronze()
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def dataverse_bronze(context: AssetExecutionContext):
    """Incrementally load Dataverse entities into DuckLake bronze via dlt."""
    info = dv.run_bronze()
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def salesforce_id_sync(context: AssetExecutionContext):
    """Manual: refresh Salesforce live-id sets so silver can drop hard-deleted rows.

    Soft-deletes (IsDeleted) are already caught by the incremental query_all; this
    reconciles *hard* deletes (physically removed rows). Run on demand, then
    rebuild silver. Not part of a normal incremental.
    """
    info = sf.run_id_reconcile()
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def dataverse_id_sync(context: AssetExecutionContext):
    """Manual: refresh Dataverse live-id sets so silver can drop hard-deleted rows.

    Dataverse has no soft-delete marker reachable by an incremental pull, so this
    is the only way to reconcile deletes. Run on demand, not every increment.
    """
    info = dv.run_id_reconcile()
    context.log.info(str(info))
    return str(info)


# ---------------------------------------------------------------- silver (dbt)
@asset(group_name="silver", deps=[salesforce_bronze, dataverse_bronze])
def silver(context: AssetExecutionContext):
    """Build the silver layer: regenerate models from config, dbt run, publish.

    Regenerating from silver_config keeps models in sync with the ENTITIES /
    column lists, then dbt materializes the specified-column, deduped,
    delete-reconciled silver tables, and the result is published to blob as parquet.
    """
    gen_silver.main()
    _dbt(context, "seed")
    _dbt(context, "run", "--select", "silver")
    published = publish.publish_silver()
    context.log.info(f"published silver: {published}")
    return published


# ------------------------------------------------------------------ gold (dbt)
@asset(group_name="gold", deps=[silver])
def gold(context: AssetExecutionContext):
    """Build the gold layer: snapshot stats stamped with today's date, then publish.

    Each run appends a dated snapshot (incremental append; re-running the same day
    is idempotent). Publishes the gold tables to blob as parquet.
    """
    snapshot_date = date.today().isoformat()
    _dbt(context, "run", "--select", "gold", "--vars", f"snapshot_date: {snapshot_date}")
    published = publish.publish_gold()
    context.log.info(f"published gold ({snapshot_date}): {published}")
    return published


defs = Definitions(
    assets=[
        salesforce_bronze,
        dataverse_bronze,
        salesforce_id_sync,
        dataverse_id_sync,
        silver,
        gold,
    ]
)
