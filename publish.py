"""Publish silver / gold tables from DuckLake to Azure Blob as parquet.

Other applications consume the lakehouse via flat parquet files, not the
DuckLake catalog. This module reads each table out of the DuckLake `lake`
catalog (the shared SQLite catalog written by the dlt bronze + dbt silver/gold
layers), writes it to a local parquet under ./publish, and uploads it to the
`duck` container of the `synapsedeltalake2022` storage account (credentials from
local.setting.json, the same connection string sf.py uses).

Blob layout (within the `duck` container):
    duck/silver/<table>.parquet
    duck/gold/<table>.parquet

Every silver / gold table present in the catalog is published — add a model and
it is picked up automatically (ephemeral gold intermediates never materialize,
so they are naturally excluded).
"""

import os

import duckdb
from azure.storage.blob import BlobServiceClient

BASE = "/home/ubuntu/dagster"
CATALOG_SQLITE = f"{BASE}/lake_catalog.duckdb"
PUBLISH_DIR = f"{BASE}/publish"
CONTAINER = "duck"


def _connect():
    """DuckDB connection with the DuckLake catalog attached as `lake`."""
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite; INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:sqlite:{CATALOG_SQLITE}' AS lake")
    return con


def _container_client():
    with open(f"{BASE}/local.setting.json") as f:
        login = eval(f.read())
    svc = BlobServiceClient.from_connection_string(login["connection_string"])
    client = svc.get_container_client(CONTAINER)
    try:
        client.create_container()  # no-op if it already exists
    except Exception:
        pass
    return client


def _tables(con, schema):
    rows = con.execute(
        "select table_name from information_schema.tables "
        "where table_catalog = 'lake' and table_schema = ? order by table_name",
        [schema],
    ).fetchall()
    return [r[0] for r in rows]


def _publish_schema(schema):
    """Export every table in `lake.<schema>` to parquet and upload to duck/<schema>/."""
    os.makedirs(PUBLISH_DIR, exist_ok=True)
    con = _connect()
    container = _container_client()
    published = []
    try:
        for t in _tables(con, schema):
            local = f"{PUBLISH_DIR}/{t}.parquet"
            con.execute(
                f'COPY (SELECT * FROM lake."{schema}"."{t}") '
                f"TO '{local}' (FORMAT PARQUET)"
            )
            with open(local, "rb") as fh:
                container.upload_blob(name=f"{schema}/{t}.parquet", data=fh, overwrite=True)
            published.append(t)
            print(f"published {schema}.{t} -> {CONTAINER}/{schema}/{t}.parquet", flush=True)
    finally:
        con.close()
    return published


def publish_silver():
    return _publish_schema("silver")


def publish_gold():
    return _publish_schema("gold")


if __name__ == "__main__":
    print("silver:", publish_silver())
    print("gold:", publish_gold())
