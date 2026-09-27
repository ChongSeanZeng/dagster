# FSC Modern Data Stack (Dagster + dlt + dbt + DuckLake)

A medallion-layered data platform (Bronze / Silver / Gold) built on the **DuckLake** lakehouse:

- **Orchestration:** [Dagster](https://dagster.io/) — all assets are currently **triggered manually**; no scheduled runs are configured.
- **Extraction (Bronze):** [dlt](https://dlthub.com/) — incrementally loads **all columns** from **Salesforce** / **Dataverse** into DuckLake `bronze`, using primary-key-based `merge`.
- **Transformation (Silver / Gold):** [dbt](https://www.getdbt.com/) — Silver keeps only **specified columns**, deduplicates, and removes deleted records; Gold periodically **snapshots** common metrics.
- **Publishing:** Silver / Gold are published as **Parquet** files to the Azure Blob `duck` container for direct consumption by other applications.

> ✅ **All paths are relative:** Each Python module treats its own directory as the project root (`os.path.dirname(os.path.abspath(__file__))`), and dbt uses paths relative to the `dbt/` directory. The same code therefore runs on both a Linux VM and local Windows without any path changes. Production is deployed on a Linux VM; local development/testing can be done on Windows (see [Local testing (Windows)](#local-testing-windows)).

---

## Contents

- [Architecture and data flow](#architecture-and-data-flow)
- [Directory structure](#directory-structure)
- [Deploying to a VM](#deploying-to-a-vm)
- [Local testing (Windows)](#local-testing-windows)
- [Configuration](#configuration)
- [How to use](#how-to-use)
  - [Method 1: Dagster UI (recommended)](#method-1-dagster-ui-recommended)
  - [Method 2: Run directly from the command line](#method-2-run-directly-from-the-command-line)
- [Hard-delete ID reconciliation](#hard-delete-id-reconciliation)
- [Querying DuckLake externally](#querying-ducklake-externally)
- [Adding entities / columns](#adding-entities--columns)
- [Notes and troubleshooting](#notes-and-troubleshooting)

---

## Architecture and data flow

```text
     Salesforce ─┐                          ┌── silver (dbt, selected columns/deduplication/deletion filtering)
                 ├─ dlt ─▶ DuckLake bronze ─┤
      Dataverse ─┘         (all columns,     └── gold (dbt, metric snapshots)
                            merge)
                                │                        │
                    *_id_sync (fetch all live IDs,       │
                     used to anti-join hard-deleted      │
                     records from silver)                │
                                                         ▼
                                       publish.py ─▶ Azure Blob container `duck`
                                         duck/silver/*.parquet
                                         duck/gold/*.parquet
```

| Layer | Tool | Location | Description |
|---|---|---|---|
| Bronze | dlt | DuckLake schema `bronze` | Extracts all columns and merges by `Id`. **The initial full load uses Bulk 2.0 (fast)**; **incremental loads use Bulk 1.0 `queryAll`** to capture soft deletes (`IsDeleted`). Both paths use a consistent schema: non-cursor columns are stored as text and cursor columns as datetime. |
| Silver | dbt | DuckLake schema `silver` | Keeps only business columns specified in `silver_config.py`; deduplicates by cursor and keeps the latest record; filters soft deletes; anti-joins against `__live_ids` to remove hard-deleted records; removes dlt system columns. |
| Gold | dbt | DuckLake schema `gold` | Aggregates common metrics from Silver (ported from `daily.py` / `monthly_test.py`). Each run **appends** a snapshot with a `snapshot_date`. |
| Publishing | duckdb + azure | Blob container `duck` | Exports each Silver / Gold table as Parquet and uploads it. |

**Important:** DuckLake's metadata catalog is an **SQLite** file (`lake_catalog.duckdb`, despite the misleading filename); the actual data is stored as Parquet under `lake/`. dlt and dbt **must point to the same SQLite catalog**, otherwise Silver cannot read Bronze.

---

## Directory structure

```text
dagster/
├── definitions.py              # Dagster asset definitions (bronze / id_sync / silver / gold)
├── workspace.yaml              # Dagster loading entry point (points to definitions.py)
├── sf_bronze.py                 # Salesforce → bronze (dlt) + hard-delete ID reconciliation
├── dataverse_bronze.py          # Dataverse → bronze (dlt) + hard-delete ID reconciliation
├── silver_config.py             # List of selected Silver columns for each entity
├── gen_silver.py                # Generates Silver dbt models + _sources.yml from silver_config
├── publish.py                   # Exports Silver/Gold tables as Parquet and uploads to Blob `duck`
├── daily.py / monthly_test.py    # Legacy reference scripts (Gold has been rewritten in dbt; for comparison only)
├── sf.py                         # Legacy full-load script (reference source for the Silver column lists)
├── local.setting.json            # Credentials (not committed; see .gitignore)
├── lake_catalog.duckdb           # DuckLake catalog (SQLite; not committed)
├── lake/                         # DuckLake data files (Parquet; not committed)
├── publish/                      # Local Parquet staging area before publishing (not committed)
├── home/                         # DAGSTER_HOME (run history/logs; not committed)
├── requirements.txt
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml              # DuckLake connection (ducklake:sqlite:...)
    ├── seeds/regions.csv         # ISO3 → region mapping (used by the members model)
    ├── macros/
    │   ├── silver_dedupe.sql     # Shared Silver transformations (deduplication/deletion filtering/column selection)
    │   ├── gold_helpers.sql      # Shared macros for snapshot_date, account types, etc.
    │   └── generate_schema_name.sql
    └── models/
        ├── silver/*.sql          # Generated by gen_silver.py
        └── gold/*.sql            # Snapshot metric models
```

---

## Deploying to a VM

Run the following commands on the **Linux VM**. The assumed working directory is `/home/ubuntu/dagster`.

### 1. Get the code

```bash
cd /home/ubuntu
git clone <your-repo-url> dagster
cd dagster
```

> The project root can be located anywhere. `BASE` is inferred automatically from each module's `__file__`, and dbt uses relative paths, so no path constants need to be changed.

### 2. Create a virtual environment and install dependencies

> ⚠️ Use **Python 3.11 / 3.12** (`dbt-core` does not support 3.13 / 3.14).

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python --version                 # Should show 3.12.x
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Prepare credentials

Create `local.setting.json` (**do not commit it**; it is already excluded in `.gitignore`):

```json
{
  "password": "<Salesforce password>",
  "security_token": "<Salesforce security token>",
  "key1": "<fscdatastore account key, used by legacy scripts>",
  "connection_string": "DefaultEndpointsProtocol=https;AccountName=synapsedeltalake2022;AccountKey=<key>;EndpointSuffix=core.windows.net;",
  "dataverse": {
    "client_id": "<Azure AD application client ID>",
    "client_secret": "<Azure AD application client secret>"
  }
}
```

- `connection_string` is used to publish Parquet files to the `duck` container in the `synapsedeltalake2022` storage account.
- `dataverse.client_id` / `dataverse.client_secret` are credentials for the Dataverse Azure AD application and **must be stored here** (this file is excluded by `.gitignore`). Do not put them in `dataverse_bronze.py`—GitHub secret scanning may block commits containing plaintext secrets. The tenant `authority_url` and `resource` have non-secret defaults; override them in this `dataverse` block if needed.

> This file is parsed with `eval` as a **Python dict literal**, not strict JSON (the `connection_string` uses single quotes in the actual file). Follow the existing format exactly.

### 4. Set the Dagster home directory

```bash
export DAGSTER_HOME=$(pwd)/home        # Points to the project's home/ directory
```

It is recommended to add this line to `~/.bashrc` or the environment variables of the systemd service. `DAGSTER_HOME` must be an **absolute path** (required by Dagster); `$(pwd)/home` derives it dynamically from the project root, so the path does not need to be hard-coded. If it is not set, Dagster uses a temporary directory (run history will not persist; acceptable for local testing).

### 5. Verify catalog connectivity first (important)

Run a small entity first to confirm that dlt and dbt use the same SQLite catalog:

```bash
python -c "import sf_bronze; print(sf_bronze.run_bronze(entities=['RecordType']))"
cd dbt && dbt run --select silver.recordtype --profiles-dir . && cd ..
```

If `lake.silver.recordtype` contains data and only has the `Id, Name` columns, the connection is working.

> If the dlt catalog connection string raises an error, check the `CATALOG` scheme in `sf_bronze.py` / `dataverse_bronze.py` against the installed dlt version on the VM. It must point to the same **SQLite** file, `lake_catalog.duckdb`, as `dbt/profiles.yml`.

### 6. Start Dagster

```bash
dagster dev -h 0.0.0.0 -p 3000
```

Open `http://<vm-ip>:3000` in a browser. Since runs are currently manual and no schedules are configured, `dagster dev` is sufficient. For long-running background operation, use systemd or `nohup`; if schedules are added later, also run `dagster-daemon run`.

---

## Local testing (Windows)

Because paths are relative/dynamic, Windows can run an **independent local lakehouse** (the catalog, `lake/`, and `publish/` are created inside the project directory) for development and debugging. The steps are below.

All paths are relative/dynamic, so Windows can run a separate local lakehouse for testing. **After the initial environment setup, everything can be done by clicking buttons in the Dagster UI—no command line is needed.**

> ⚠️ **Python version:** use **3.11 / 3.12**. `dbt-core` does not support 3.13 / 3.14. On 3.14, `import dbt` fails because of `mashumaro` (for example, `UnserializableField: Field "schema" ...`). dlt/Bronze can run on 3.14, but dbt cannot.

**① Set up the environment and start the UI (command line, once)**

```powershell
# From the project root in PowerShell, using Python 3.12
py -3.12 -m venv .venv          # Or: python3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version                # Should show 3.12.x
pip install -r requirements.txt

# Prepare local.setting.json (same as on the VM; see "Configuration")
# DAGSTER_HOME must be an absolute path; derive it from the current directory
$env:DAGSTER_HOME = "$PWD\home"

# Start the UI from the project root; open http://localhost:3000 in a browser
dagster dev
```

**② After that, use buttons in the UI** (on the Assets page, click **Materialize** in the top-right of each asset; assets with ▸ can be configured in the Launchpad):

1. **Bronze:** Select `salesforce_bronze` → **Materialize**. To test a single entity, enter this in the Launchpad:

   ```yaml
   ops:
     salesforce_bronze:
       config:
         entities: ["RecordType"]   # Empty = all 15 entities
         # seed_since_days: 7      # Optional: backfill only the last N days; useful for testing large tables
   ```

2. **Silver:** Select `silver` → **Materialize**. To test one model without uploading to Blob:

   ```yaml
   ops:
     silver:
       config:
         select: "silver.recordtype"  # Empty/default = the entire Silver layer
         publish: false               # Disable uploads for local testing; default is true
   ```

3. **Gold:** Select `gold` → **Materialize**. Its config can set `snapshot_date`, `publish`, etc. (see [How to use](#how-to-use)).

Key points:

- **Bronze must run before dbt (Silver/Gold).** When creating a DuckLake catalog, `DATA_PATH` is required (provided by dlt's `STORAGE`). dbt only attaches to an existing catalog and does not supply `DATA_PATH`, so Bronze must create the catalog first. In the UI, Silver depends on Bronze; materialize Bronze first, then Silver.
- **The catalog and `lake/` must remain on the same machine.** The absolute data-file paths are recorded in catalog metadata. A `lake_catalog.duckdb` created on the VM will not work on Windows because it contains Linux paths. For local testing, run Bronze again to create a local catalog.
- End-to-end testing has been verified on **Windows with Python 3.12**: `salesforce_bronze` (config `entities:["RecordType"]`) → `silver` (config `select:"silver.recordtype"`) both returned `RUN_SUCCESS`. The Silver output had only `Id,Name`, 46 rows, and no dlt columns.
- An internet connection is needed the first time DuckDB runs `INSTALL ducklake/sqlite/azure` to download extensions. Bronze extraction still requires Salesforce/Dataverse credentials. Pure dbt/publish/query logic can be tested offline against a local lakehouse.

---

## Configuration

### `dbt/profiles.yml`

```yaml
fsc_lake:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dbt.duckdb                     # Relative to dbt/; used only as a DuckDB session anchor
      threads: 1                            # SQLite catalog allows a single writer; keep this serial
      extensions: [sqlite, ducklake]
      attach:
        - path: "ducklake:sqlite:../lake_catalog.duckdb"   # Relative path to the catalog in the project root
          alias: lake                       # Models access lake.silver.* / lake.gold.*
```

> Paths are **relative to the `dbt/` directory**, so dbt must be run from `dbt/` (`silver` / `gold` Dagster assets already use `cwd=dbt/`; when running manually, `cd dbt` first). This works on Linux and Windows without path changes.

### Entity lists

- **Salesforce:** edit `ENTITIES` in `sf_bronze.py` (approximately 15 API names).
- **Dataverse:** edit `ENTITIES` in `dataverse_bronze.py` (`entity_set: primary_key`).

### Silver column lists

Edit `SILVER_COLUMNS` in `silver_config.py` (based on `QUERIES` in the legacy `sf.py`, with additional fields required by Gold). After changes, regenerate the models (see below).

---

## How to use

### Method 1: Dagster UI (recommended; button-driven workflow)

On the Dagster UI **Assets** page, manually click **Materialize**. Each asset's **Config** can be changed in the Launchpad.

| Asset | Group | Purpose | Config options in Launchpad |
|---|---|---|---|
| `salesforce_bronze` | bronze | Incrementally load Salesforce → Bronze | `entities` (subset; empty = all), `seed_since_days` (number of days to backfill on initial load) |
| `dataverse_bronze` | bronze | Incrementally load Dataverse → Bronze | Same as above |
| `salesforce_id_sync` | bronze | **Manual:** sync live Salesforce IDs (hard-delete reconciliation) | `entities` (subset; empty = all) |
| `dataverse_id_sync` | bronze | **Manual:** sync live Dataverse IDs (hard-delete reconciliation) | `entities` |
| `silver` | silver | Regenerate models → `dbt seed` → `dbt run` for Silver → publish Parquet | `select` (default `silver`; can specify `silver.<model>`), `publish` (default `true`) |
| `gold` | gold | `dbt run` for Gold (with `snapshot_date`) → publish Parquet | `select` (default `gold`), `snapshot_date` (empty = today), `publish` |

Example config (paste YAML into the Launchpad):

```yaml
ops:
  salesforce_bronze:
    config:
      entities: ["Account", "Certificate_c__c"]   # Load only these two; empty = all
      seed_since_days: 30                          # On initial load, backfill only the last 30 days
```

Typical workflow: run `salesforce_bronze` / `dataverse_bronze` first, then `silver`, and finally `gold`. Trigger hard-delete reconciliation separately when needed (see the next section). For local testing, set `publish: false` in Silver's config to disable uploads.

### Method 2: Run directly from the command line (optional; scripts / VM automation)

For routine operations, use the UI above. To run via scripts, activate the virtual environment and set `DAGSTER_HOME` from the project root first:

```bash
# Bronze — incremental load (no argument = all entities; can also specify one entity)
python -c "import sf_bronze; print(sf_bronze.run_bronze())"
python -c "import sf_bronze; print(sf_bronze.run_bronze(entities=['Account']))"
python -c "import dataverse_bronze as d; print(d.run_bronze())"

# Hard-delete ID reconciliation (manual, as needed)
python -c "import sf_bronze; print(sf_bronze.run_id_reconcile())"
python -c "import dataverse_bronze as d; print(d.run_id_reconcile())"

# Silver — generate models and build
python gen_silver.py
cd dbt
dbt seed --profiles-dir .
dbt run --select silver --profiles-dir .
cd ..

# Gold — specify the snapshot date
cd dbt
dbt run --select gold --vars 'snapshot_date: 2026-07-14' --profiles-dir .
cd ..

# Publish Parquet files to Blob container duck/
python -c "import publish; print(publish.publish_silver()); print(publish.publish_gold())"
```

---

## Hard-delete ID reconciliation

- **Soft deletes** (`IsDeleted=true` in Salesforce): captured by Bronze through `queryAll` and automatically filtered out in Silver; no extra action is needed.
- **Hard deletes** (records physically deleted): incremental extraction cannot retrieve them. Run ID reconciliation to fetch **all currently live IDs** from the source into `bronze.<entity>__live_ids`; Silver uses an anti-join (`Id in (select Id from ..._live_ids)`) to remove deleted records.

Recommended sequence:

```bash
# 1) Run the normal incremental load
python -c "import sf_bronze; sf_bronze.run_bronze()"
# 2) Reconcile currently live IDs (snapshot)
python -c "import sf_bronze; sf_bronze.run_id_reconcile()"
# 3) Rebuild Silver; hard-deleted records will be removed
cd dbt && dbt run --select silver --profiles-dir . && cd ..
```

> Note: `__live_ids` is refreshed only when reconciliation is run manually. If records are **added at the source** between reconciliation and rebuilding Silver, those new records may temporarily be filtered out until the next reconciliation. Therefore, run reconciliation and the Silver rebuild consecutively. Before the first reconciliation, Silver automatically skips the anti-join, so it will not error.

---

## Querying DuckLake externally

There are two ways to consume the data. **Method A is recommended** because external applications do not need access to the VM's catalog or files.

### Method A: Read published Parquet files from Blob (recommended)

After publishing, the `duck` container contains:

```text
duck/silver/<table_name>.parquet      # e.g. account, certificate_c, country_data ...
duck/gold/<table_name>.parquet        # e.g. members, coc_per_country, baseline_certification ...
```

Read the files directly with DuckDB (on any machine with DuckDB installed):

```sql
INSTALL azure; LOAD azure;

-- Create a secret using the same connection string as local.setting.json
CREATE SECRET az (
    TYPE azure,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=synapsedeltalake2022;AccountKey=<key>;EndpointSuffix=core.windows.net;'
);

-- Query a table
SELECT * FROM read_parquet('az://duck/gold/members.parquet') LIMIT 100;

-- View historical snapshots for a metric
SELECT snapshot_date, country, certificates
FROM read_parquet('az://duck/gold/coc_per_country.parquet')
ORDER BY snapshot_date DESC, certificates DESC;
```

Python (polars / pandas) can also read the files directly:

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")
con.execute("CREATE SECRET az (TYPE azure, CONNECTION_STRING '<connection_string>')")
df = con.execute("SELECT * FROM read_parquet('az://duck/silver/account.parquet')").pl()
```

> You can also download the Parquet files from Blob and read them locally with any tool. These Parquet files are self-contained and do not depend on the catalog.

### Method B: Connect directly to the DuckLake catalog (requires access to the VM's catalog and `lake/` directory)

This is suitable on the VM itself, or when both `lake_catalog.duckdb` and the `lake/` directory have been copied. DuckLake stores data-file paths in catalog metadata (`file://.../lake`), so **the catalog and `lake/` must both be accessible and their paths must match**.

```sql
INSTALL sqlite;   LOAD sqlite;
INSTALL ducklake; LOAD ducklake;

ATTACH 'ducklake:sqlite:/home/ubuntu/dagster/lake_catalog.duckdb' AS lake;

-- List all tables
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_catalog = 'lake'
ORDER BY 1, 2;

-- Query Silver / Gold
SELECT * FROM lake.silver.account LIMIT 100;
SELECT * FROM lake.gold.members WHERE snapshot_date = (SELECT max(snapshot_date) FROM lake.gold.members);
```

Command line:

```bash
duckdb -c "INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite; ATTACH 'ducklake:sqlite:/home/ubuntu/dagster/lake_catalog.duckdb' AS lake; SELECT * FROM lake.gold.fm_per_country LIMIT 20;"
```

---

## Adding entities / columns

**Add a Salesforce entity:**

1. Add its API name to `ENTITIES` in `sf_bronze.py` (all columns are extracted automatically).
2. Add the entity and its column list to `SILVER_COLUMNS` in `silver_config.py` (the Bronze table name is the lowercase API name).
3. Run `python gen_silver.py` to regenerate the Silver models and `_sources.yml`.
4. Run `salesforce_bronze` → `silver` (the Dagster `silver` asset already includes step 3).

**Add a column to an existing entity:** Add the column name in `silver_config.py` → run `python gen_silver.py` → rebuild Silver. (Bronze already extracts all columns, so extraction code does not need to change.)

**Customize a Silver model:** Add the model name to the `SKIP` set in `gen_silver.py`, then write that model's `.sql` file manually. The generator will no longer overwrite it.

---

## Notes and troubleshooting

- **Portable paths:** Each module infers the project root from `__file__`, and dbt uses relative paths, so Linux VM and local Windows need no path changes. Note that the catalog and `lake/` are tied to the same machine because metadata contains absolute paths. Do not move `lake_catalog.duckdb` to another machine by itself; rerun Bronze on the new machine.
- **The catalog is SQLite:** `lake_catalog.duckdb` is actually SQLite. dlt (`CATALOG` in `sf_bronze.py` / `dataverse_bronze.py`) and dbt (`attach` in `profiles.yml`) must point to the same SQLite catalog; otherwise Silver cannot read Bronze.
- **Column-name case:** Silver projects exact column names using quoted identifiers. If the capitalization in Bronze differs from `silver_config.py` (for example, `policy_omit_trademark_symbols__c`), the Silver build will fail with a “column does not exist” error. Troubleshooting: run `DESCRIBE lake.bronze.<table_name>;` to check the actual column name and update the config accordingly.
- **Single-writer catalog / concurrency:** The catalog is SQLite and supports a single writer. If two assets write to the lakehouse at the same time, you may see `Failed to commit DuckLake transaction`. Therefore, do not increase dbt `threads: 1`; Dagster uses `in_process_executor` (assets run serially within one run, so **Materialize all** is safe). To serialize **across separate runs** (for example, when clicking Materialize multiple times), add this to `DAGSTER_HOME/dagster.yaml`:

  ```yaml
  run_coordinator:
    module: dagster.core.run_coordinator
    class: QueuedRunCoordinator
    config:
      max_concurrent_runs: 1
  ```

- **Gold snapshot idempotency:** Running Gold more than once for the same `snapshot_date` does not append duplicates (the model includes deduplication protection). A different date appends a new snapshot.
- **Gold numeric fields:** Salesforce numeric/date values may be strings in Bronze. Gold uses `try_cast` for safe conversion; values that cannot be parsed become `NULL` and are excluded by filters.
- **Legacy scripts:** `daily.py` / `monthly_test.py` are retained only as references for the Gold logic. The new stack no longer depends on them. `monthly_test.py` had a pre-existing syntax issue on lines 169–171, which was avoided in the dbt port.
