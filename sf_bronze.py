"""Bronze ingestion: Salesforce -> DuckLake via dlt.

Give an entity's Salesforce API name; we `describe()` it, pull EVERY queryable
column via Bulk 1.0 `queryAll` (so IsDeleted rows are captured for the silver-layer
soft-delete), and MERGE into the DuckLake `bronze` dataset keyed on Id.

Incremental cursor is SystemModstamp (always present once we select all columns).
A small LAG re-pulls the boundary each run; merge on Id makes that idempotent —
this replaces the hand-rolled watermark.csv / enforce_schema logic in sf.py.

Metadata (the DuckLake catalog) lives in a local DuckDB file; the actual data
files are Parquet under ./lake. Nothing here touches Salesforce for deletes on a
normal run — hard-delete reconciliation ("id 拉齐") is a separate manual step.
"""

import csv
import io
import os
from datetime import datetime, timedelta, timezone

# Preserve original Salesforce column names (Id, SystemModstamp, Genus__c) instead
# of dlt's default snake_case — keeps the "don't specify columns" promise end-to-end
# and lets published parquet match what existing consumers already expect.
os.environ.setdefault("SCHEMA__NAMING", "direct")

import dlt
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
from simple_salesforce import Salesforce

BASE = "/home/ubuntu/dagster"
# The DuckLake catalog metadata lives in a SQLite database (the on-disk
# lake_catalog.duckdb is a SQLite file). dlt (this bronze writer) and dbt
# (the silver/gold reader) MUST point at the SAME catalog with the SAME
# backend, otherwise silver cannot see bronze. dbt attaches
# `ducklake:sqlite:/home/ubuntu/dagster/lake_catalog.duckdb`; the equivalent
# SQLAlchemy-style connection string for dlt's DuckLakeCredentials is below.
# NOTE: verify this scheme against the installed dlt version on the VM before
# the first run — the goal is one shared SQLite catalog, not the backend name.
CATALOG = f"sqlite:///{BASE}/lake_catalog.duckdb"
STORAGE = f"file://{BASE}/lake"
DUCKLAKE_NAME = "fsc_lake"

# To add a new Salesforce entity: append its API name here. That's it — all
# columns are pulled automatically via describe(), and the cursor defaults to
# SystemModstamp (present on every standard/custom object). The bronze table is
# named entity.lower(); silver column projection lives in silver_config.py.
ENTITIES = [
    "Product_Species__c",
    "Product_Classification__c",
    "Account",
    "Certificate_c__c",
    "Certificate_Status__c",
    "Certificate_Attachment__c",
    "Contact",
    "Non_Certificate_Holder__c",
    "Country_Data__c",
    "RecordType",
    "Species__c",
    "Evaluation__c",
    "Project_Certificate__c",
    "Sales_History__c",
    "Transaction_Verification_Findings__c",
]

# Only override for objects that have NO SystemModstamp (History / Attachment use
# CreatedDate — append-only, so an updated-timestamp cursor doesn't apply).
CURSOR_OVERRIDES = {
    # "Certificate_c__History": "CreatedDate",
    # "Attachment": "CreatedDate",
}


def _cursor_for(entity):
    return CURSOR_OVERRIDES.get(entity, "SystemModstamp")

# Bulk 1.0 cannot query compound (address/location) or base64 fields; drop them.
EXCLUDED_FIELD_TYPES = {"address", "location", "base64"}
LAG_SECONDS = 120  # safety margin, mirrors sf.py's -2min; merge makes the re-pull free
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _login():
    with open(f"{BASE}/local.setting.json") as f:
        return eval(f.read())


def get_sf():
    login = _login()
    return Salesforce(
        username="gdreports@fsc.org.integration",
        password=login["password"],
        security_token=login["security_token"],
    )


def _parse_dt(v):
    """Salesforce datetime -> tz-aware UTC datetime.

    Bulk 1.0 JSON returns datetimes as epoch-millisecond integers; the REST/CSV
    paths return ISO strings. Handle both.
    """
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
    s = str(v).strip().replace("Z", "+0000")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def queryable_fields(sf, entity):
    fields = getattr(sf, entity).describe()["fields"]
    return [f["name"] for f in fields if f["type"] not in EXCLUDED_FIELD_TYPES]


def _make_resource(sf, entity, cursor_field, initial_value=EPOCH):
    @dlt.resource(name=entity.lower(), primary_key="Id", write_disposition="merge")
    def _res(cursor=dlt.sources.incremental(cursor_field, initial_value=initial_value)):
        cols = queryable_fields(sf, entity)
        last = cursor.last_value or EPOCH
        since = (last - timedelta(seconds=LAG_SECONDS)).astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        soql = (
            f"SELECT {', '.join(cols)} FROM {entity} "
            f"WHERE {cursor_field} > {since} ORDER BY {cursor_field} ASC"
        )
        # First load pulls live rows only (plain `query`) — deleted records are
        # irrelevant with no prior state and just bloat the backfill. Incremental
        # runs use `query_all` (queryAll) so they also catch rows that flipped to
        # IsDeleted since the last watermark, letting silver soft-delete them.
        is_first = cursor.last_value is None or cursor.last_value == initial_value
        method = "query" if is_first else "query_all"
        print(f"[{entity}] {method} since {since} ({len(cols)} cols)", flush=True)
        # lazy_operation=True streams result batches instead of materializing the
        # ENTIRE result set as one Python list — essential for multi-million-row
        # tables (Product_Species__c is ~7.4M rows). Memory stays bounded per batch.
        n = 0
        for batch in getattr(getattr(sf.bulk, entity), method)(soql, lazy_operation=True):
            for rec in batch:
                rec.pop("attributes", None)
                # normalize the cursor field so dlt's incremental compare is unambiguous
                rec[cursor_field] = _parse_dt(rec.get(cursor_field))
                yield rec
            n += len(batch)
            print(f"[{entity}] streamed {n} rows", flush=True)

    return _res


def _make_ids_resource(sf, entity):
    """id-only full pull for hard-delete reconciliation ("id 拉齐"), mirroring
    dataverse_bronze._make_ids_resource.

    Salesforce soft-deletes (IsDeleted=True) are already caught by the
    incremental `query_all` and dropped in silver. A *hard* delete physically
    removes the row, so it simply stops appearing — no incremental pull can see
    it. This resource pulls the full set of live Ids (cheapest possible request)
    via Bulk 2.0 (plain query = live rows only) into <entity>__live_ids, and the
    silver layer anti-joins it to drop rows the source no longer has. Run on
    demand (manual), not every increment.
    """

    @dlt.resource(name=f"{entity.lower()}__live_ids", write_disposition="replace")
    def _res():
        print(f"[{entity}] id 拉齐 (full live ids via Bulk 2.0)", flush=True)
        n = 0
        for page in getattr(sf.bulk2, entity).query(f"SELECT Id FROM {entity}"):
            batch = [{"Id": row["Id"]} for row in csv.DictReader(io.StringIO(page))]
            n += len(batch)
            print(f"[{entity}] streamed {n} live ids", flush=True)
            yield batch

    return _res


@dlt.source(name="salesforce")
def salesforce_source(sf=None, initial_value=EPOCH):
    sf = sf or get_sf()
    for entity in ENTITIES:
        yield _make_resource(sf, entity, _cursor_for(entity), initial_value)


@dlt.source(name="salesforce_ids")
def salesforce_ids_source(sf=None):
    sf = sf or get_sf()
    for entity in ENTITIES:
        yield _make_ids_resource(sf, entity)


def _pipeline():
    creds = DuckLakeCredentials(
        ducklake_name=DUCKLAKE_NAME, catalog=CATALOG, storage=STORAGE
    )
    return dlt.pipeline(
        pipeline_name="sf_bronze",
        destination=ducklake(credentials=creds),
        dataset_name="bronze",
    )


def run_bronze(entities=None, seed_since=None):
    """Run bronze ingestion.

    seed_since: only used on a resource's FIRST run (before dlt has stored a
    last_value). Pass e.g. datetime.now(tz)-timedelta(days=1) to seed a bounded
    backfill for testing; leave None to backfill from EPOCH (full history).
    After the first run dlt tracks the cursor itself and this is ignored.
    """
    src = salesforce_source(initial_value=seed_since or EPOCH)
    if entities:
        src = src.with_resources(*[e.lower() for e in entities])
    return _pipeline().run(src, loader_file_format="parquet")


def run_id_reconcile(entities=None):
    """Manual: refresh the live-id sets used by silver to drop hard-deleted rows.

    Run after a normal bronze load so the live-id snapshot is fresh; silver then
    anti-joins <entity>__live_ids to remove ids the source no longer has.
    """
    src = salesforce_ids_source()
    if entities:
        src = src.with_resources(*[f"{e.lower()}__live_ids" for e in entities])
    return _pipeline().run(src, loader_file_format="parquet")


if __name__ == "__main__":
    print(run_bronze())
