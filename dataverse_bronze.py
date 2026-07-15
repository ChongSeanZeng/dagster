"""Bronze ingestion: Dataverse (Dynamics 365) -> DuckLake via dlt.

Same medallion lake as Salesforce (one DuckLake catalog, shared silver/gold),
just a different bronze extractor: Dataverse is pulled over OData with a
`modifiedon gt <last>` incremental filter and paged via @odata.nextLink.

Key difference vs Salesforce: Dataverse has NO soft-delete marker reachable by an
incremental pull — `modifiedon gt` only returns rows that still exist, so a hard
delete simply stops appearing. Deletes are therefore handled ONLY by the separate,
manually-triggered "id 拉齐": pull the full set of live primary keys (id-only,
cheapest possible request) into <entity>__live_ids, and let the silver layer
anti-join it to drop rows the source no longer has.

To add a Dataverse entity: append `"entity_set_plural": "primarykey_field"` to
ENTITIES. All columns are pulled (no $select on the data resource); annotation /
FormattedValue handling is deferred to silver, so bronze stays raw.
"""

import os
import time
from datetime import datetime

# Preserve original Dataverse column names (accountid, modifiedon, _owningbusinessunit_value)
# instead of dlt's snake_case, matching the Salesforce bronze convention.
os.environ.setdefault("SCHEMA__NAMING", "direct")

import adal
import dlt
import requests
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

import catalog

# Project root = this file's own directory, forward-slash (portable Linux/Windows).
# See sf_bronze.py for the rationale.
BASE = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
# Shared DuckLake catalog — metadata in local Postgres, data as Parquet under
# ./lake_pg (connection built in catalog.py). Must match sf_bronze.py and
# dbt/profiles.yml.
CATALOG = catalog.dlt_catalog()
STORAGE = catalog.STORAGE
DUCKLAKE_NAME = catalog.DUCKLAKE_NAME

# To add a Dataverse entity: "<entity set (plural)>": "<primary key field>".
# account -> set "accounts", pk "accountid".
ENTITIES = {
    "accounts": "accountid",
    # NB: the entity's logical name is already "fsc_aaf_calculations" (trailing s),
    # so Dataverse pluralises the *set* to "...calculationses" (adds -es). The URL
    # segment must be the set name, not the logical name.
    "fsc_aaf_calculationses": "fsc_aaf_calculationsid"
}

CURSOR_FIELD = "modifiedon"
INITIAL_VALUE = "1970-01-01T00:00:00Z"
PAGE_SIZE = 5000

# Azure AD app registration for the FSC Dataverse. The client id / secret are NOT
# stored in the repo — they live in local.setting.json (gitignored) under a
# "dataverse" block. The tenant authority + resource URL are non-secret defaults
# but can be overridden in the same block. See README.
_CREDS = None


def _creds():
    """Lazily load & cache the Dataverse credentials from local.setting.json."""
    global _CREDS
    if _CREDS is None:
        defaults = {
            "authority_url": "https://login.microsoftonline.com/124e69db-ef65-496b-96a9-5d56bec1d291",
            "resource": "https://fsc.crm4.dynamics.com",
            "client_id": None,
            "client_secret": None,
        }
        with open(f"{BASE}/local.setting.json") as f:
            cfg = eval(f.read())
        dv = cfg.get("dataverse", {})
        defaults.update({k: v for k, v in dv.items() if v is not None})
        if not defaults["client_id"] or not defaults["client_secret"]:
            raise RuntimeError(
                "Dataverse client_id/client_secret missing — add a 'dataverse' "
                "block to local.setting.json (see README)."
            )
        _CREDS = defaults
    return _CREDS


def get_access_token():
    creds = _creds()
    ctx = adal.AuthenticationContext(creds["authority_url"])
    token = ctx.acquire_token_with_client_credentials(
        creds["resource"], creds["client_id"], creds["client_secret"]
    )
    return token["accessToken"]


def _headers(annotations=True):
    prefer = f"odata.maxpagesize={PAGE_SIZE}"
    if annotations:
        prefer = 'odata.include-annotations="*", ' + prefer
    return {
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Prefer": prefer,
    }


# Progress sink. Defaults to a flushing print (so lines show immediately even when
# stdout is a pipe/file); run_bronze / run_id_reconcile set this to the caller's
# logger (Dagster passes context.log.info) so the current entity/table shows up in
# the run log. Kept as a module global rather than a resource arg so a
# non-serializable callable never flows through dlt's @source/@resource signatures.
def _print_flush(msg):
    print(msg, flush=True)


_LOG = _print_flush


def _log(msg):
    _LOG(msg)


def _paged_get(url, headers):
    """Yield pages (lists of records) following @odata.nextLink, with simple retry."""
    session = requests.Session()
    page = 0
    while url:
        try:
            resp = session.get(url, headers=headers, timeout=120)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.exceptions.RequestException(f"transient {resp.status_code}")
            if resp.status_code != 200:
                _log(f"  non-retriable {resp.status_code}: {resp.text[:200]}")
                break
            data = resp.json()
            items = data.get("value", [])
            page += 1
            _log(f"  page {page}: {len(items)} rows")
            yield items
            url = data.get("@odata.nextLink")
        except requests.exceptions.RequestException as e:
            _log(f"  transient error: {e}; retrying in 10s")
            time.sleep(10)
            session = requests.Session()
            continue


def _make_resource(entity_set, id_field, initial_value=INITIAL_VALUE):
    @dlt.resource(name=entity_set, primary_key=id_field, write_disposition="merge")
    def _res(cursor=dlt.sources.incremental(CURSOR_FIELD, initial_value=initial_value)):
        token = get_access_token()
        headers = _headers(annotations=True)
        headers["Authorization"] = "Bearer " + token
        last = cursor.last_value
        if isinstance(last, datetime):
            last = last.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (
            f"{_creds()['resource']}/api/data/v9.2/{entity_set}"
            f"?$filter={CURSOR_FIELD} gt {last}&$orderby={CURSOR_FIELD} asc"
        )
        _log(f"[{entity_set}] incremental since {last} -> bronze.{entity_set}")
        for items in _paged_get(url, headers):
            yield items

    return _res


def _make_ids_resource(entity_set, id_field):
    """id-only full pull for hard-delete reconciliation (manual step)."""

    @dlt.resource(name=f"{entity_set}__live_ids", write_disposition="replace")
    def _res():
        token = get_access_token()
        headers = _headers(annotations=False)
        headers["Authorization"] = "Bearer " + token
        url = f"{_creds()['resource']}/api/data/v9.2/{entity_set}?$select={id_field}"
        _log(f"[{entity_set}] id 拉齐 (full ids) -> bronze.{entity_set}__live_ids")
        for items in _paged_get(url, headers):
            yield [{id_field: r[id_field]} for r in items]

    return _res


@dlt.source(name="dataverse")
def dataverse_source(initial_value=INITIAL_VALUE):
    for entity_set, id_field in ENTITIES.items():
        yield _make_resource(entity_set, id_field, initial_value)


@dlt.source(name="dataverse_ids")
def dataverse_ids_source():
    for entity_set, id_field in ENTITIES.items():
        yield _make_ids_resource(entity_set, id_field)


def _pipeline():
    creds = DuckLakeCredentials(
        ducklake_name=DUCKLAKE_NAME, catalog=CATALOG, storage=STORAGE
    )
    return dlt.pipeline(
        pipeline_name="dataverse_bronze",
        destination=ducklake(credentials=creds),
        dataset_name="bronze",
    )


def run_bronze(entities=None, seed_since=None, full_refresh=False, log=print):
    """entities: optional list of entity sets (e.g. ['accounts']) to limit the run;
    None = all configured. seed_since: only used on a resource's first run to bound
    the backfill (e.g. a recent date for testing); ignored afterwards.
    full_refresh: drop the selected resources' table + dlt incremental cursor and
    reload from scratch (INITIAL_VALUE, i.e. full history unless seed_since is set).
    Use this to fix a cursor left at the wrong watermark by an earlier bounded run.
    log: progress sink (Dagster passes context.log.info) so the current entity
    shows up in the run log."""
    global _LOG
    _LOG = log
    src = dataverse_source(initial_value=seed_since or INITIAL_VALUE)
    if entities:
        src = src.with_resources(*entities)
    # "drop_resources" wipes only the resources selected for this run (so a full
    # refresh of one entity leaves the others' cursors intact); None = normal
    # incremental. See sf_bronze.run_bronze for the mirror implementation.
    refresh = "drop_resources" if full_refresh else None
    return _pipeline().run(src, loader_file_format="parquet", refresh=refresh)


def run_id_reconcile(entities=None, log=print):
    """Manual: refresh the live-id sets used by silver to drop hard-deleted rows.
    entities: optional list of entity sets to limit the run; None = all.
    log: progress sink (Dagster passes context.log.info)."""
    global _LOG
    _LOG = log
    src = dataverse_ids_source()
    if entities:
        src = src.with_resources(*[f"{e}__live_ids" for e in entities])
    return _pipeline().run(src, loader_file_format="parquet")


if __name__ == "__main__":
    print(run_bronze())
