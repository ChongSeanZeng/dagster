"""Dagster definitions for the FSC modern data stack (medallion on DuckLake).

Manual-trigger assets, no schedules yet. Everything is driven from the Dagster UI
(Materialize buttons + per-run Config in the Launchpad) — no command line needed.
Lineage:

  bronze (dlt)                     silver (dbt)          gold (dbt)
  ------------------               -------------         --------------------
  salesforce_bronze   ─┐
  dataverse_bronze    ─┼─────────▶ silver ────────────▶ gold
  salesforce_id_sync  ─┘ (hard-delete "id 拉齐", manual, feeds silver anti-join)
  dataverse_id_sync   ─┘

Per-asset Config (editable in the Launchpad when you Materialize):
  - bronze assets: `entities` (subset to load; empty = all), `seed_since_days`
    (bound the first-run backfill for a quick test; empty = full history).
  - id-sync assets: `entities` (subset; empty = all).
  - silver: `select` (dbt selector, default "silver"; e.g. "silver.recordtype" to
    test one model), `publish` (upload parquet to blob; turn off for local tests).
  - gold: `select` (default "gold"), `snapshot_date` (empty = today), `publish`.

Blob publishing targets the `duck` container (see publish.py).
"""

import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from dagster import AssetExecutionContext, Config, Definitions, asset, in_process_executor

# Ensure the sibling modules (sf_bronze, dataverse_bronze, gen_silver, publish)
# are importable no matter how Dagster loads this file or what the cwd is — the
# project dir isn't automatically on sys.path when loaded via workspace.yaml.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import dataverse_bronze as dv  # noqa: E402
import gen_silver  # noqa: E402
import publish  # noqa: E402
import sf_bronze as sf  # noqa: E402

# Project root = this file's directory (forward-slash, portable Linux/Windows).
BASE = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
DBT_DIR = f"{BASE}/dbt"


# ----------------------------------------------------------------- run configs
class BronzeConfig(Config):
    # Subset of entities to load (Salesforce API names / Dataverse entity sets).
    # Empty = all configured entities. e.g. ["RecordType"] for a quick test.
    entities: list[str] = []
    # Bound the FIRST-run backfill to the last N days (handy for testing large
    # tables). Empty/None = full history. Ignored once dlt has a stored cursor.
    seed_since_days: Optional[int] = None


class IdSyncConfig(Config):
    # Subset of entities to reconcile live-ids for. Empty = all.
    entities: list[str] = []


class SilverConfig(Config):
    # dbt selector. Default builds the whole silver layer; use e.g.
    # "silver.recordtype" to build a single model for testing.
    select: str = "silver"
    # Upload the built tables to blob as parquet. Turn OFF for pure local tests.
    publish: bool = True


class GoldConfig(Config):
    select: str = "gold"
    # Snapshot date stamped on the gold rows. Empty = today.
    snapshot_date: str = ""
    publish: bool = True


# ------------------------------------------------------------------- dbt helper
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
def salesforce_bronze(context: AssetExecutionContext, config: BronzeConfig):
    """Incrementally load Salesforce entities into DuckLake bronze via dlt."""
    seed = None
    if config.seed_since_days:
        seed = datetime.now(timezone.utc) - timedelta(days=config.seed_since_days)
    info = sf.run_bronze(entities=config.entities or None, seed_since=seed)
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def dataverse_bronze(context: AssetExecutionContext, config: BronzeConfig):
    """Incrementally load Dataverse entities into DuckLake bronze via dlt."""
    seed = None
    if config.seed_since_days:
        seed = (datetime.now(timezone.utc) - timedelta(days=config.seed_since_days)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    info = dv.run_bronze(entities=config.entities or None, seed_since=seed)
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def salesforce_id_sync(context: AssetExecutionContext, config: IdSyncConfig):
    """Manual: refresh Salesforce live-id sets so silver can drop hard-deleted rows.

    Soft-deletes (IsDeleted) are already caught by the incremental query_all; this
    reconciles *hard* deletes (physically removed rows). Run on demand, then
    rebuild silver.
    """
    info = sf.run_id_reconcile(entities=config.entities or None)
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def dataverse_id_sync(context: AssetExecutionContext, config: IdSyncConfig):
    """Manual: refresh Dataverse live-id sets so silver can drop hard-deleted rows.

    Dataverse has no soft-delete marker reachable by an incremental pull, so this
    is the only way to reconcile deletes. Run on demand, not every increment.
    """
    info = dv.run_id_reconcile(entities=config.entities or None)
    context.log.info(str(info))
    return str(info)


# ---------------------------------------------------------------- silver (dbt)
@asset(group_name="silver", deps=[salesforce_bronze, dataverse_bronze])
def silver(context: AssetExecutionContext, config: SilverConfig):
    """Build the silver layer: regenerate models from config, dbt run, publish.

    `config.select` narrows the dbt build (default whole silver layer);
    `config.publish` controls whether the result is uploaded to blob.
    """
    gen_silver.main()
    _dbt(context, "seed")
    _dbt(context, "run", "--select", config.select)
    if config.publish:
        published = publish.publish_silver()
        context.log.info(f"published silver: {published}")
        return published
    context.log.info("publish disabled (config.publish = false)")
    return "built (not published)"


# ------------------------------------------------------------------ gold (dbt)
@asset(group_name="gold", deps=[silver])
def gold(context: AssetExecutionContext, config: GoldConfig):
    """Build the gold layer: snapshot stats stamped with a date, then publish.

    Each run appends a dated snapshot (incremental append; re-running the same day
    is idempotent). `config.snapshot_date` empty = today; `config.publish` controls upload.
    """
    snapshot_date = config.snapshot_date or date.today().isoformat()
    _dbt(context, "run", "--select", config.select, "--vars", f"snapshot_date: {snapshot_date}")
    if config.publish:
        published = publish.publish_gold()
        context.log.info(f"published gold ({snapshot_date}): {published}")
        return published
    context.log.info(f"gold built for {snapshot_date}; publish disabled")
    return f"built {snapshot_date} (not published)"


# DuckLake's catalog (SQLite) is single-writer: two assets writing the lake at the
# same time raise "Failed to commit DuckLake transaction". in_process_executor runs
# a run's assets serially (one process), so materializing several at once — or
# "Materialize all" — never writes the lake concurrently. See README for how to also
# cap CONCURRENT RUNS (separate launches) via dagster.yaml.
defs = Definitions(
    assets=[
        salesforce_bronze,
        dataverse_bronze,
        salesforce_id_sync,
        dataverse_id_sync,
        silver,
        gold,
    ],
    executor=in_process_executor,
)
