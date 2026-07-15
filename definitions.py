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

from dagster import AssetExecutionContext, Config, Definitions, asset, multiprocess_executor

# Ensure the sibling modules (sf_bronze, dataverse_bronze, gen_silver, publish)
# are importable no matter how Dagster loads this file or what the cwd is — the
# project dir isn't automatically on sys.path when loaded via workspace.yaml.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import catalog  # noqa: E402
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
    # Drop the selected entities' bronze table + dlt incremental cursor and reload
    # from scratch. Use to fix a cursor left at the wrong watermark by an earlier
    # bounded/test run. Combine with `entities` to refresh just one; leave
    # `seed_since_days` empty for full history.
    full_refresh: bool = False


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
    # Inject the DuckLake catalog connection (with password) via env so it stays
    # out of dbt/profiles.yml and out of the logged command line.
    env = {**os.environ, **catalog.dbt_env()}
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env)
    if proc.stdout:
        context.log.info(proc.stdout)
    if proc.stderr:
        context.log.info(proc.stderr)
    if proc.returncode != 0:
        raise Exception(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.stdout


# Resolve the dbt console script next to the running interpreter (…/.venv/bin/dbt).
# Dagster may be launched with a PATH that doesn't include the venv bin, so a bare
# "dbt" raises FileNotFoundError; an absolute path is PATH-independent. Fall back to
# "dbt" on PATH if for some reason it isn't a sibling of sys.executable.
_DBT_BIN = os.path.join(os.path.dirname(sys.executable), "dbt")
if not os.path.exists(_DBT_BIN):
    _DBT_BIN = "dbt"


def _dbt(context, *args):
    return _run(context, [_DBT_BIN, *args, "--profiles-dir", DBT_DIR], cwd=DBT_DIR)


# ---------------------------------------------------------------- bronze (dlt)
@asset(group_name="bronze")
def salesforce_bronze(context: AssetExecutionContext, config: BronzeConfig):
    """Incrementally load Salesforce entities into DuckLake bronze (direct, no dlt)."""
    seed = None
    if config.seed_since_days:
        seed = datetime.now(timezone.utc) - timedelta(days=config.seed_since_days)
    info = sf.run_bronze(
        entities=config.entities or None,
        seed_since=seed,
        full_refresh=config.full_refresh,
        log=context.log.info,
    )
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
    info = dv.run_bronze(
        entities=config.entities or None,
        seed_since=seed,
        full_refresh=config.full_refresh,
        log=context.log.info,
    )
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def salesforce_id_sync(context: AssetExecutionContext, config: IdSyncConfig):
    """Manual: refresh Salesforce live-id sets so silver can drop hard-deleted rows.

    Soft-deletes (IsDeleted) are already caught by the incremental query_all; this
    reconciles *hard* deletes (physically removed rows). Run on demand, then
    rebuild silver.
    """
    info = sf.run_id_reconcile(entities=config.entities or None, log=context.log.info)
    context.log.info(str(info))
    return str(info)


@asset(group_name="bronze")
def dataverse_id_sync(context: AssetExecutionContext, config: IdSyncConfig):
    """Manual: refresh Dataverse live-id sets so silver can drop hard-deleted rows.

    Dataverse has no soft-delete marker reachable by an incremental pull, so this
    is the only way to reconcile deletes. Run on demand, not every increment.
    """
    info = dv.run_id_reconcile(entities=config.entities or None, log=context.log.info)
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


# Concurrency: the DuckLake catalog now lives in Postgres, which supports
# concurrent writers (different tables are independent; same-table concurrent
# commits use optimistic concurrency and retry). So within a run, assets can run
# in parallel — e.g. salesforce_bronze and dataverse_bronze write different tables
# at once. max_concurrent caps the worker processes per run; cross-run concurrency
# is capped separately by QueuedRunCoordinator in home/dagster.yaml.
defs = Definitions(
    assets=[
        salesforce_bronze,
        dataverse_bronze,
        salesforce_id_sync,
        dataverse_id_sync,
        silver,
        gold,
    ],
    executor=multiprocess_executor.configured({"max_concurrent": 4}),
)
