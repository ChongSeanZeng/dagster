"""Shared DuckLake catalog connection (Postgres backend).

The catalog METADATA lives in a local Postgres database (role `fsc` / db
`fsc_lake`, provisioned per README); the Parquet DATA files live under ./lake_pg.
Credentials come from local.setting.json (gitignored) so no secret lands in code
or in dbt/profiles.yml.

Three clients build their connection from here:
  * sf_bronze.py     -> duckdb ATTACH  (duckdb_attach_target(), libpq keyword form)
  * dataverse_bronze -> dlt catalog    (dlt_catalog(), SQLAlchemy URL form)
  * definitions._dbt -> DUCKLAKE_CATALOG env var injected into dbt (dbt_env())
    which dbt/profiles.yml reads via {{ env_var('DUCKLAKE_CATALOG') }}.

Why Postgres: the old SQLite catalog was single-writer, so two pipelines writing
the lake at once raised "Failed to commit DuckLake transaction". Postgres allows
concurrent writers (different tables are independent; same-table concurrent
commits use optimistic concurrency and may retry), which is what lets Dagster run
pipelines concurrently.
"""

import ast
import os

BASE = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
DUCKLAKE_NAME = "fsc_lake"
# Parquet data dir for the Postgres-backed catalog. (Named ./lake_pg to keep it
# distinct from the now-removed legacy ./lake used by the old SQLite catalog.)
STORAGE = f"file:///{BASE.lstrip('/')}/lake_pg"


def _pg():
    cfg = ast.literal_eval(open(f"{BASE}/local.setting.json").read())
    return cfg["catalog_pg"]


def duckdb_attach_target():
    """`ducklake:...` target for a duckdb ATTACH (libpq keyword form)."""
    p = _pg()
    return (
        f"ducklake:postgres:dbname={p['dbname']} host={p['host']} "
        f"port={p['port']} user={p['user']} password={p['password']}"
    )


def dlt_catalog():
    """Catalog connection string for dlt DuckLakeCredentials (SQLAlchemy URL form)."""
    p = _pg()
    return f"postgresql://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['dbname']}"


def dbt_env():
    """Env vars to inject into dbt so profiles.yml can attach the same catalog
    without hardcoding the password: DUCKLAKE_CATALOG = the `postgres:...` inner
    (dbt uses `ducklake:{{ env_var('DUCKLAKE_CATALOG') }}`)."""
    return {"DUCKLAKE_CATALOG": duckdb_attach_target().split("ducklake:", 1)[1]}
