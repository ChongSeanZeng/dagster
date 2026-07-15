"""Bronze ingestion: Salesforce -> DuckLake (direct, no dlt).

Give an entity's Salesforce API name; we `describe()` it, pull EVERY queryable
column and MERGE it into the DuckLake `bronze` dataset keyed on Id.

Two extraction paths:
  * FIRST load (backfill): Bulk API 2.0 `query` — far faster on large tables.
    Live rows only (deletes are irrelevant with no prior state).
  * INCREMENTAL load: Bulk API 1.0 `queryAll` — the only path that returns rows
    that flipped to IsDeleted since the watermark, so silver can soft-delete them.
Both order by the cursor ASC and stream page-by-page; every business column is
normalized to text and the cursor to a UTC datetime, so the two paths produce an
IDENTICAL destination schema (silver/gold cast types as needed).

Why no dlt (Salesforce only): dlt's extract->normalize->load round-trip and its
pinned DuckLake extension added latency and version-coupling with the shared
catalog. Here we write straight to DuckLake with duckdb — build an Arrow batch
per page and upsert it (delete-by-Id + insert). Because pages arrive ordered by
the cursor and are committed as they load, a partial/interrupted run is a safe
prefix: the next run resumes from max(cursor) already stored. That watermark IS
the incremental state — no watermark.csv, no dlt cursor. Dataverse still uses dlt
(see dataverse_bronze.py); both write the same DuckLake `bronze` dataset.

The DuckLake catalog metadata is a local Postgres database (see catalog.py); data
files are Parquet under ./lake_pg. Hard-delete reconciliation ("id 拉齐") is a
separate manual step (run_id_reconcile) that snapshots the live Ids into
<table>__live_ids.
"""

import csv
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# Preserve original Salesforce column names (Id, SystemModstamp, Genus__c) instead
# of any snake_casing — keeps the "don't specify columns" promise end-to-end and
# lets published parquet match what existing consumers already expect.
os.environ.setdefault("SCHEMA__NAMING", "direct")

import duckdb
import pyarrow as pa
from simple_salesforce import Salesforce

import catalog

# Project root = this file's directory, forward-slash (portable Linux/Windows).
BASE = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
# One shared DuckLake catalog — metadata now in local Postgres (db `fsc_lake`),
# data files as Parquet under ./lake_pg. Connection built in catalog.py from
# local.setting.json; MUST match dataverse_bronze.py and dbt/profiles.yml.
# Requires duckdb >= 1.5 (ducklake 1.0-format catalog).
DUCKLAKE_NAME = catalog.DUCKLAKE_NAME
BRONZE_SCHEMA = "bronze"

# To add a new Salesforce entity: append its API name here. All columns are
# pulled automatically via describe(); the cursor defaults to SystemModstamp. The
# bronze table is named entity.lower(); silver projection lives in silver_config.py.
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


# Optional per-entity business filter (SOQL WHERE fragment), ported from the
# original sf.py / weekly.py QUERIES so bronze pulls the same business subset as
# the legacy full-load (not the entire raw table). Combined with the incremental
# cursor filter. Entities not listed here pull all rows. This is why e.g.
# Product_Species__c loads a subset instead of ~7.4M raw rows.
ENTITY_FILTERS = {
    "Product_Species__c": "Product_Class__c != null",
    "Account": (
        "Hide_Site__c = false AND Type NOT IN ("
        "'03-CORP grants and MEMB', '04-DEV AG', '05-FND - private', "
        "'05-FND - public', '06-FUNDING CONSORTIUM', '07-GOV - tech corp', "
        "'08-NGO', 'Corporation')"
    ),
    "Certificate_c__c": "CB__c <> 'ASI' AND Cert_Status__c <> 'Will not be certified'",
    "Certificate_Attachment__c": (
        "Active__c = true AND Document_Type__c IN ("
        "'(Extended) Company Risk Assessment (available on Website)', "
        "'Ecosystem Services Certification Document (available on Website)', "
        "'Forest Management Standard (available on website)', "
        "'List of participating sites/group members/FMUs (available on website)', "
        "'Product Registration Form (available on website)', "
        "'Public Summary Report (available on website)', "
        "'Supplementary File (available on website)')"
    ),
    "Evaluation__c": "Display_publicly__c = true AND Schedule_Evaluation__c = true",
}


def _print_flush(msg):
    """Default progress sink: print that flushes immediately (visible in piped/
    file output). Dagster overrides this with context.log.info."""
    print(msg, flush=True)


# Bulk 1.0 cannot query compound (address/location) or base64 fields; drop them.
EXCLUDED_FIELD_TYPES = {"address", "location", "base64"}
LAG_SECONDS = 120  # safety margin, mirrors sf.py's -2min; merge makes the re-pull free
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# --------------------------------------------------------------- Salesforce auth
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
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
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


def _norm(rec, cursor_field):
    """Normalize a raw Salesforce record to a stable destination schema:
    the cursor field -> UTC datetime, every other column -> text (or None).

    Bulk 2.0 (CSV) yields all-string values with "" for null; Bulk 1.0 (JSON)
    yields typed values. Forcing business columns to text makes both paths produce
    the SAME schema; silver/gold cast as needed.
    """
    rec.pop("attributes", None)
    out = {}
    for k, v in rec.items():
        if k == cursor_field:
            continue
        out[k] = None if v is None or v == "" else str(v)
    out[cursor_field] = _parse_dt(rec.get(cursor_field))
    return out


# ------------------------------------------------------------------ DuckLake I/O
def _connect():
    """DuckDB connection with the shared (Postgres-backed) DuckLake catalog
    attached + bronze schema ensured."""
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH '{catalog.duckdb_attach_target()}' AS {DUCKLAKE_NAME}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DUCKLAKE_NAME}.{BRONZE_SCHEMA}")
    return con


def _fqtn(table):
    return f'{DUCKLAKE_NAME}.{BRONZE_SCHEMA}."{table}"'


def _table_exists(con, table):
    return (
        con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema=? AND table_name=?",
            [DUCKLAKE_NAME, BRONZE_SCHEMA, table],
        ).fetchone()[0]
        > 0
    )


def _watermark(con, table, cursor_field):
    """Max cursor already stored = the incremental watermark (EPOCH if new/empty)."""
    if not _table_exists(con, table):
        return EPOCH
    v = con.execute(f'SELECT max("{cursor_field}") FROM {_fqtn(table)}').fetchone()[0]
    if v is None:
        return EPOCH
    return v if v.tzinfo else v.replace(tzinfo=timezone.utc)


def _to_arrow(rows, cols, cursor_field):
    """Columnar batch with an explicit, stable schema: cursor -> UTC timestamp,
    every other column -> text. Explicit types keep the schema identical across
    pages and across the Bulk 2.0 / Bulk 1.0 paths (no type inference drift)."""
    fields, arrays = [], []
    for c in cols:
        if c == cursor_field:
            typ = pa.timestamp("us", tz="UTC")
        else:
            typ = pa.string()
        fields.append(pa.field(c, typ))
        arrays.append(pa.array([r.get(c) for r in rows], type=typ))
    return pa.table(arrays, schema=pa.schema(fields))


def _upsert(con, table, batch, primary_key):
    """Idempotent merge of an Arrow batch: create the table on first sight,
    otherwise delete rows whose pk is in the batch and re-insert (by column name)."""
    con.register("_incoming", batch)
    try:
        if not _table_exists(con, table):
            con.execute(f"CREATE TABLE {_fqtn(table)} AS SELECT * FROM _incoming")
        else:
            con.execute(
                f'DELETE FROM {_fqtn(table)} '
                f'WHERE "{primary_key}" IN (SELECT "{primary_key}" FROM _incoming)'
            )
            con.execute(f"INSERT INTO {_fqtn(table)} BY NAME SELECT * FROM _incoming")
    finally:
        con.unregister("_incoming")


# ----------------------------------------------------------------- extraction
def _extract(sf, entity, cols, cursor_field, since, first):
    """Yield (rows, running_total) per page.

    first=True  -> Bulk 2.0 query (fast backfill, live rows only). NO ORDER BY:
        an ORDER BY on Bulk 2.0 makes Salesforce sort the WHOLE table server-side
        before returning the first row — minutes of latency on big tables. The
        caller runs the backfill in one transaction, so we don't need an ordered
        prefix for crash-safety (an interrupted backfill just commits nothing).
    first=False -> Bulk 1.0 queryAll (includes IsDeleted rows for soft-delete),
        ORDER BY cursor ASC: the incremental result set is small (rows since the
        watermark), so the sort is cheap and per-page commits form a safe prefix.
    """
    select = f"SELECT {', '.join(cols)} FROM {entity}"
    where = f"{cursor_field} > {since}"
    biz = ENTITY_FILTERS.get(entity)
    if biz:  # AND the business subset filter (ported from sf.py) with the cursor
        where = f"({biz}) AND {where}"
    n = 0
    if first:
        soql = f"{select} WHERE {where}"
        for chunk in getattr(sf.bulk2, entity).query(soql):
            rows = [_norm(r, cursor_field) for r in csv.DictReader(io.StringIO(chunk))]
            n += len(rows)
            yield rows, n
    else:
        soql = f"{select} WHERE {where} ORDER BY {cursor_field} ASC"
        for batch in getattr(sf.bulk, entity).query_all(soql, lazy_operation=True):
            rows = [_norm(rec, cursor_field) for rec in batch]
            n += len(rows)
            yield rows, n


# ------------------------------------------------------------------- public API
def _load_entity(sf, entity, seed_dt, full_refresh, log):
    """Load one Salesforce entity into bronze on its OWN duckdb connection
    (connections aren't thread-safe to share). Backfill runs in a single
    transaction so an interrupted run commits nothing; incremental commits
    per page (ordered, so a partial run is a safe prefix). Returns (table, rows)."""
    table = entity.lower()
    cursor_field = _cursor_for(entity)
    con = _connect()
    try:
        cols = queryable_fields(sf, entity)
        if full_refresh and _table_exists(con, table):
            con.execute(f"DROP TABLE {_fqtn(table)}")
            log(f"[{entity}] full_refresh: dropped existing bronze.{table}")

        wm = _watermark(con, table, cursor_field)
        first = wm == EPOCH
        if first and seed_dt:
            wm = seed_dt.astimezone(timezone.utc)
        since = (wm - timedelta(seconds=LAG_SECONDS)).astimezone(
            timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        mode = "BULK2 backfill" if first else "BULK1 incremental"
        log(f"[{entity}] {mode} since {since} ({len(cols)} cols) -> bronze.{table}")

        total = 0
        if first:
            con.execute("BEGIN")  # atomic backfill: interrupted -> nothing committed
        try:
            for rows, n in _extract(sf, entity, cols, cursor_field, since, first):
                if rows:
                    _upsert(con, table, _to_arrow(rows, cols, cursor_field), "Id")
                total = n
                log(f"[{entity}] streamed {total} rows")
            if first:
                con.execute("COMMIT")
        except Exception:
            if first:
                con.execute("ROLLBACK")
            raise
        log(f"[{entity}] done -> {total} rows into bronze.{table}")
        return table, total
    finally:
        con.close()


def run_bronze(entities=None, seed_since=None, full_refresh=False, log=_print_flush,
               max_workers=4):
    """Load Salesforce entities into DuckLake bronze (direct, no dlt), entities in
    PARALLEL (each on its own connection / Salesforce bulk job).

    entities: subset of API names to load (None = all configured).
    seed_since: on a resource's FIRST load only, bound the backfill (datetime or
        ISO string); ignored once the bronze table has rows. None = from EPOCH.
    full_refresh: drop the entity's bronze table first and reload from scratch —
        use to fix a watermark left wrong by an earlier bounded/test run.
    log: callable for progress lines (default print; Dagster passes context.log.info
        so the current entity/table shows up in the run log).
    max_workers: how many entities to load concurrently. The Postgres catalog
        allows concurrent writers (each entity is a different table), and bulk
        extraction is I/O-bound, so this cuts a full load to ~the slowest entity.
    """
    sf = get_sf()
    todo = entities or ENTITIES
    seed_dt = _parse_dt(seed_since) if isinstance(seed_since, str) else seed_since
    results = {}
    errors = {}
    workers = max(1, min(max_workers, len(todo)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_load_entity, sf, e, seed_dt, full_refresh, log): e for e in todo}
        for fut in as_completed(futs):
            entity = futs[fut]
            try:
                table, total = fut.result()
                results[table] = total
            except Exception as e:  # keep loading the rest; report at the end
                errors[entity] = repr(e)
                log(f"[{entity}] FAILED: {e!r}")
    if errors:
        raise RuntimeError(f"salesforce bronze: {len(errors)} entity/entities failed: {errors}")
    return results


def run_id_reconcile(entities=None, log=_print_flush):
    """Manual: refresh Salesforce live-id snapshots so silver can drop hard-deletes.

    Soft-deletes (IsDeleted) are already caught by the incremental query_all; this
    reconciles *hard* deletes (physically removed rows). Pulls the full set of live
    Ids (cheapest request) via Bulk 2.0 into <table>__live_ids (replace semantics);
    silver anti-joins it. Run on demand, then rebuild silver.
    """
    sf = get_sf()
    todo = entities or ENTITIES
    con = _connect()
    results = {}
    try:
        for entity in todo:
            table = f"{entity.lower()}__live_ids"
            log(f"[{entity}] id 拉齐 (full live ids via Bulk 2.0) -> bronze.{table}")
            n = 0
            created = False
            for page in getattr(sf.bulk2, entity).query(f"SELECT Id FROM {entity}"):
                ids = [row["Id"] for row in csv.DictReader(io.StringIO(page))]
                batch = pa.table(
                    [pa.array(ids, pa.string())],
                    schema=pa.schema([pa.field("Id", pa.string())]),
                )
                con.register("_incoming", batch)
                try:
                    if not created:
                        # replace semantics: drop the old snapshot, recreate
                        if _table_exists(con, table):
                            con.execute(f"DROP TABLE {_fqtn(table)}")
                        con.execute(f"CREATE TABLE {_fqtn(table)} AS SELECT * FROM _incoming")
                        created = True
                    else:
                        con.execute(f"INSERT INTO {_fqtn(table)} BY NAME SELECT * FROM _incoming")
                finally:
                    con.unregister("_incoming")
                n += len(ids)
                log(f"[{entity}] streamed {n} live ids")
            if not created:
                # source returned zero rows: leave an empty snapshot so the
                # silver anti-join drops everything (source truly has no rows)
                if _table_exists(con, table):
                    con.execute(f"DROP TABLE {_fqtn(table)}")
                con.execute(f'CREATE TABLE {_fqtn(table)} ("Id" VARCHAR)')
            results[table] = n
    finally:
        con.close()
    return results


if __name__ == "__main__":
    print(run_bronze())
