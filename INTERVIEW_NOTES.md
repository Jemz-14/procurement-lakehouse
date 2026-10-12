# Interview Notes

Plain spoken English I can say out loud. One section per phase:
what we built, the decision, why, and the alternative with its trade-off.

## Name changes (interviewers may use either name)

| Current name | Old name | Notes |
|---|---|---|
| Lakeflow Spark Declarative Pipelines (SDP) | Delta Live Tables (DLT), then Lakeflow Declarative Pipelines | Python import is now `from pyspark import pipelines as dp` (old: `import dlt`) |
| AUTO CDC: `dp.create_auto_cdc_flow()` / `CREATE FLOW ... AUTO CDC INTO` | `dlt.apply_changes()` / `APPLY CHANGES INTO` | same arguments, just renamed |
| Lakeflow Jobs | Databricks Workflows (and before that, just "Jobs") | |
| Git folders | Databricks Repos | |
| Lakeflow Connect | (new) managed connectors for SaaS and databases | |

## How my Azure experience maps to Databricks

| What I did on Azure | Databricks equivalent |
|---|---|
| ADLS containers for raw JSON | Unity Catalog volume (`/Volumes/catalog/schema/volume/...`) |
| ADF watermark incremental load | Auto Loader with a checkpoint (tracks files, not a max timestamp) |
| ADF pipeline and triggers | Lakeflow Jobs |
| Synapse serverless SQL over files | `read_files()` or a SQL warehouse querying Delta tables |
| Synapse dedicated SQL pool | SQL warehouse (serverless) over Delta tables |
| Synapse database.schema.table | Unity Catalog catalog.schema.table |
| Quarantine table for bad rows | `ops.quarantine`, plus expectations in declarative pipelines |
| SCD Type 1 with MERGE | SCD Type 1 and Type 2 with MERGE, or AUTO CDC |

---

## Phase 0: Setup

### What we built

A GitHub repo synced into Databricks through a Git folder, and a rerunnable SQL setup notebook (`setup/00_setup.sql`) that creates a `procurement` catalog with five schemas, `landing`, `bronze`, `silver`, `gold` and `ops`, plus two volumes: `landing.raw` for incoming files and `ops.checkpoints` for streaming state. All of it runs on Databricks Free Edition with serverless compute.

### Decisions, why, and the alternative

**1. A dedicated `procurement` catalog instead of the default `workspace` catalog.**
- Why: a catalog is the top-level unit of isolation and permissions in Unity Catalog. Keeping the project in its own catalog means I can grant or revoke access to the whole domain in one place. It is also the pattern you scale to in production, with one catalog per environment, like `procurement_dev` and `procurement_prod`.
- Alternative: put everything in the default catalog. Simpler, but the project gets mixed with anything else in the workspace, and you lose the clean environment boundary.

**2. One schema per medallion layer, rather than one schema with table prefixes like `bronze_suppliers`.**
- Why: in Unity Catalog, permissions are inherited down from catalog to schema to table. With a schema per layer, I can give analysts `SELECT` on `gold` only, and they never see raw or quarantined data. It also makes the layer obvious from the full name, `procurement.silver.suppliers`.
- Alternative: prefixes in one schema. Fewer objects, but access control has to be done table by table, which is error prone.

**3. Raw files land in a Unity Catalog volume, not straight into a table.**
- Why: a volume is a governed folder for non-tabular files. It is the Databricks version of my ADLS raw container, but with Unity Catalog permissions and lineage on it. Keeping the original files means I can always replay or reprocess from source.
- Alternative: in production the files would usually sit in cloud storage owned by the source team, exposed through an external location or an external volume. Free Edition does not allow custom storage locations, so I used a managed volume. The ingestion code is the same either way, only the path changes.

**4. Managed tables and managed volumes, not external.**
- Why: with managed tables, Unity Catalog owns the storage and the lifecycle. Databricks can then run features like predictive optimization automatically, and Databricks now recommends managed tables by default. The trade-off is that `DROP TABLE` deletes the data too (it can be recovered with `UNDROP` for a few days).
- Alternative: external tables point at a path I control, and dropping the table only removes the metadata. That is useful when other tools must read the same files directly, or data must outlive the table definition. Free Edition does not support custom storage, so managed was the only choice here, but it is also the one I would pick by default.

**5. A separate `ops` schema for quarantine, run logs and checkpoints.**
- Why: operational data is not business data. Analysts get no grants on `ops`, and the engineering team has one place to look when something fails. Checkpoints go in a volume because on serverless compute, Unity Catalog volumes are the governed place to write files.
- Alternative: keep quarantine next to the silver tables. That works, but it mixes rejected rows with trusted data and makes permissions messier.

**6. The setup script is idempotent (`IF NOT EXISTS` everywhere).**
- Why: I can rerun it on a fresh workspace or after a partial failure without errors or side effects. Same principle as the pipelines later.
- Alternative: one-off manual clicks in Catalog Explorer. Quick, but not repeatable and not in version control.

### Concepts I should be able to explain

- **Workspace:** the environment where notebooks, jobs, pipelines and dashboards live. One Unity Catalog metastore can serve many workspaces.
- **Compute types:**
  - *All-purpose (interactive) compute:* a cluster you start and use from notebooks; you pay while it is running, even if idle. For development.
  - *Job compute:* a cluster created for one job run and terminated after. Cheaper per unit and the right choice for scheduled production pipelines.
  - *SQL warehouse:* compute for SQL only, used by the SQL editor, dashboards and BI tools like Power BI. Closest to a Synapse SQL pool.
  - *Serverless:* Databricks manages the machines; starts in seconds, no cluster config. Available for notebooks, jobs, pipelines and SQL warehouses. Free Edition is serverless only.
- **Three-level namespace:** `catalog.schema.table`, for example `procurement.gold.mart_spend_monthly`. Volumes, views, functions and models live at the same level as tables.
- **Managed vs external:** who owns the storage and what `DROP` does (see decision 4).
- **Volumes:** governed file storage, path `/Volumes/<catalog>/<schema>/<volume>/...`.

### Quiz answers to say out loud

**Which compute for a nightly pipeline and for a Power BI dashboard?**
The nightly pipeline runs as a Lakeflow Job on job compute, or serverless job compute. The cluster starts for the run and shuts down when it finishes, so I only pay for the run itself. Power BI connects to a SQL warehouse, which is built for many concurrent SQL queries and auto-stops when nobody is querying. I would not use all-purpose compute for either, because it is meant for interactive development, costs more per unit, and keeps billing while it is idle.

**Explain `procurement.gold.mart_spend_monthly`.**
`procurement` is the catalog, `gold` is the schema, and `mart_spend_monthly` is the table. Above the catalog sits the Unity Catalog metastore, which is the top-level container for one region and can be attached to many workspaces. Volumes live at the same level as tables, so the raw files are in `procurement.landing.raw`, with the path `/Volumes/procurement/landing/raw/`. Permissions are inherited downwards, so a grant on the `gold` schema covers every table in it.

**What does `DROP TABLE` do to a managed table and to an external table?**
For a managed table, Unity Catalog owns the storage, so dropping it removes the definition and the data files. In Unity Catalog I can still `UNDROP` it for a few days. For an external table, dropping it only removes the definition from the catalog. The files stay in ADLS or S3, and I can recreate the table on the same path. I default to managed, because Databricks can then handle optimization and clean-up for me. I use external only when other tools need to read the files directly, or when the data has to outlive the table.

**Why land raw files in a volume first?**
First, I keep an untouched copy of exactly what the source sent. If my parsing logic has a bug, or the schema changes, I can fix the code and reprocess from the original files instead of asking the source to resend. Second, it decouples the source from my pipeline: the procurement system just drops files and needs no access to my tables. Third, Auto Loader can then pick up new files incrementally. In production, a P2P platform like Coupa or SAP Ariba would export files to cloud storage owned by the data team. That storage is registered in Unity Catalog as an external location or external volume, or the data comes in through Lakeflow Connect if there is a connector.

**Why does each layer have its own schema, and why a separate `ops` schema?**
Mainly access control and lifecycle. Permissions in Unity Catalog are inherited, so I can give analysts `SELECT` on `gold` and nothing else, and they never see raw or rejected data. Each layer also has a different owner, different quality guarantees and potentially a different retention policy. `ops` keeps operational data, like quarantined rows, run logs and checkpoints, away from business data, and only engineers get access to it.

### Honesty check

I built this on **Databricks Free Edition**. I should say that plainly. It means serverless only, no custom storage locations, no account console, so some production features (external locations, real groups from SCIM, job clusters with custom sizes) I can explain but did not configure myself.

---

## Phase 1: Data generator

### What we built

A Python generator (`generator/procurement_generator.py`, run from the `run_generator` notebook) that simulates a procure-to-pay system dropping one extract per batch into the volume: suppliers as CSV, purchase orders as JSON Lines with nested lines, goods receipts as CSV, invoices as JSON Lines. Each batch covers 30 days. It injects realistic problems at a configurable error rate: hard defects to quarantine, soft formatting issues to standardise, duplicates, updated records, late-arriving receipts, maverick invoices, price variances, and a new `contract_id` column from batch 5.

### Decisions, why, and the alternative

**1. The generator simulates a real lifecycle instead of producing random unrelated rows.**
- Why: each PO gets a plan when it is created: receipt dates per line, short deliveries, an invoice date, or a cancellation. Each batch then emits only the events dated in its window. So receipts and invoices always point at real POs, POs move through APPROVED, PARTIALLY_RECEIVED, RECEIVED and CLOSED, and the gold marts (OTIF, three-way match, payment compliance) have meaningful data to measure.
- Alternative: random rows per entity. Much simpler, but joins would mostly fail and the KPIs would be noise.

**2. State is saved between batches, and batches run in order.**
- Why: a real source system remembers what it sent before. Batch 3 needs to know which POs are still open and which receipts are late. The state is a JSON file per batch under `_generator_state/`, and the underscore means Spark and Auto Loader ignore that folder.
- Alternative: regenerate all previous batches from the seed every time. No state file, but if I change `rows_per_entity` for a later batch, the history would silently change.

**3. Seeded randomness with a dedicated `random.Random` instance per batch.**
- Why: the same seed and parameters always produce byte-identical files. That makes the pipeline tests repeatable: if I rerun batch 3, I get exactly the same input, so any difference in output is my pipeline, not the data. I tested this by regenerating a batch and comparing file hashes.
- Alternative: the global `random` module with no seed. Every run is different and bugs are hard to reproduce.

**4. Hard defects versus soft defects.**
- Why: they need different handling. A negative quantity or an unknown supplier ID is wrong, so silver quarantines the row with a reason. `' active '` versus `ACTIVE` is the right value in the wrong format, so silver standardises it. Rejecting those would throw away good data. This is the difference between validation and standardisation.
- Alternative: treat every problem as an error. Simpler rules, but you lose valid rows and the quarantine fills with noise.

**5. JSON Lines for nested entities, CSV for flat ones.**
- Why: POs have a header with an array of lines, which CSV cannot hold without flattening. JSON Lines puts one object per line, so a file can be split and read in parallel, and it is Auto Loader's default JSON mode. CSV is typical for flat master data extracts like suppliers.
- Alternative: one big JSON array per file. That needs `multiLine` mode and cannot be split across tasks.

**6. Initial supplier versions are dated before the first batch, and new suppliers the day before their window.**
- Why: in phase 4 the PO fact joins to the supplier version valid on the order date. If a supplier's first version started after its first PO, that join would find nothing.

### Python concepts I should be able to explain

- `random.Random(seed)` creates an independent generator; using it everywhere instead of the global `random` keeps runs reproducible.
- `copy.deepcopy` versus `dict(...)`: a shallow copy shares the nested `lines` list, so corrupting a copy would also corrupt the saved state.
- `csv.DictWriter` with `newline=""`, and `json.dumps` per row for JSON Lines.
- `collections.Counter` for counting defects.
- `pathlib.Path` for file paths, which works the same on Windows locally and on `/Volumes/...` in Databricks.
