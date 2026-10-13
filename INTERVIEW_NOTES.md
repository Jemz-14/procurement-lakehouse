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

### Quiz answers to say out loud

**Why seed the generator, and what goes wrong without a seed?**
The seed makes the input reproducible: the same seed and parameters always produce byte-identical files, which I checked with a file hash. That matters for testing, because when I rerun a batch and the output changes, I know the change came from my pipeline code, not from the data. It also means a bug can be reproduced, and a colleague can regenerate exactly the same dataset. Without a seed, Python seeds itself from the system clock or the OS, so every run produces different data and a failure might never happen twice. I use a dedicated `random.Random` instance seeded per batch, rather than the global `random` module, so nothing else in the process can disturb the sequence.

**Hard defects versus soft defects.**
A hard defect means the value is wrong or missing and I cannot safely fix it: a negative quantity, a null supplier ID, an unknown supplier, a date like 30 February. Silver sends those rows to `ops.quarantine` with a reject reason. A soft defect means the value is right but formatted inconsistently, like `' active '` instead of `ACTIVE`, or `cc-200` instead of `CC-200`. Silver standardises those with something like `upper(trim(status))` and keeps the row. Where the line sits between the two is a business rule, so it should be written down and agreed. For example, `'1,234.50'` as text could be repaired, but I chose to quarantine it, because guessing at number formats is how wrong amounts reach finance.

**Why JSON Lines rather than one JSON array or CSV?**
CSV cannot hold the nested PO lines without flattening them into repeated header rows, or splitting them into a separate file. Between the JSON options, JSON Lines puts one complete record per line. Spark can split a large file across many tasks and parse each line on its own, and one corrupt line only affects that record. A single JSON array is one document: Spark needs `multiLine` mode, cannot split the file, so one task reads it all, and one missing bracket breaks the whole file. JSON Lines is also the default for Spark and Auto Loader.

**Why was `amount` a string, and how do I avoid losing `'1,234.50'`?**
`read_files` infers types by sampling the data. Most amounts were JSON numbers, but a few were JSON strings, so the only type that fits every value is STRING. Because everything fits a string, nothing was rescued. `_rescued_data` holds values that do not match the schema: wrong type, extra columns, or columns that differ only by case. It stores them as JSON together with the source file path. To make sure nothing is lost, I fix the type with a schema hint, `amount DOUBLE`. Then good values are typed correctly, and bad values go to `_rescued_data` instead of silently becoming null. Silver then quarantines those rows.

**Why not `WHERE po_id IS NULL` for the maverick share?**
`WHERE` filters rows before the aggregation runs, so the total would only include the maverick invoices and I would have nothing to divide by. Instead I used conditional aggregation: `count_if(po_id IS NULL)` and `SUM(CASE WHEN po_id IS NULL THEN amount ELSE 0 END)`, next to `SUM(amount)` over all rows, all in one pass. I also cleaned the amount once in a CTE rather than repeating the cast.

**`dict(po)` versus `copy.deepcopy(po)`.**
`dict(po)` is a shallow copy: a new outer dictionary, but the `lines` value is still the same list with the same line dictionaries inside. Changing `copy["status"]` is safe, but changing `copy["lines"][0]["quantity"]` also changes the original. `copy.deepcopy` copies everything recursively. In my generator, the state keeps the clean POs, and `inject_problems` corrupts the rows that get written. If those were shallow copies, a negative quantity would leak back into the state and corrupt later batches. So I deep copy before injecting defects. For suppliers I use `dict(supplier)`, which is fine because a supplier record is flat.

---

## Phase 2: Bronze with Auto Loader

### What we built

One append-only Delta table per entity in `procurement.bronze`, loaded by Auto Loader from the landing volume with `trigger(availableNow=True)`. Each entity has its own checkpoint in `/Volumes/procurement/ops/checkpoints/bronze/<entity>`, which also stores the inferred schema. Every row gets `_ingested_at`, `_source_file` (from the hidden `_metadata` column) and `_batch` (parsed from the `batch=<n>` folder name), plus Auto Loader's `_rescued_data`. All source columns stay as strings.

What I proved: after loading batches 1 to 3, I generated batch 4 and reran. Only the batch 4 files were processed, `DESCRIBE HISTORY` showed one new commit with exactly the batch 4 row count, and `VERSION AS OF` the previous version still returned the old count. A rerun with no new files loaded nothing. Earlier runs that crashed after the write had committed did not duplicate anything either, because the checkpoint had already recorded those files.

### Decisions, why, and the alternative

**1. Auto Loader rather than a plain batch read or a watermark.**
- Why: Auto Loader tracks which files it has processed in the checkpoint, so each file is loaded exactly once, and I do not maintain any watermark table myself. My ADF pipelines selected rows by value, `updated_at` greater than the last watermark. Tracking files instead of values means a late-arriving file is still picked up even if its dates are old, like my late goods receipts.
- Alternative: `COPY INTO`, a SQL command that is also idempotent and skips files it has loaded before. It is fine for simple, smaller loads, but Auto Loader scales better to many files and handles schema inference and evolution. A plain `spark.read` of the folder would reload everything every time.

**2. `trigger(availableNow=True)` instead of a continuously running stream.**
- Why: the source drops extracts on a schedule, not every second, so a cluster running 24/7 would mostly sit idle and cost money. `availableNow` processes everything new and then stops, so I get batch-job cost with streaming's exactly-once file tracking. It is also what serverless supports: continuous and `processingTime` triggers are not allowed there.
- Alternative: a continuous stream with a `processingTime` trigger on classic compute, for near real-time data. You pay for the cluster all the time.

**3. Bronze keeps every column as a string.**
- Why: bronze's job is to store exactly what was delivered and never reject or lose anything. If bronze typed `quantity` as an integer, a bad value would become null plus a JSON blob in `_rescued_data`, and bad-data handling would be split across two layers. With strings, every value lands as sent, and silver does all the typing in one place with `try_cast`, quarantining failures with a clear reason. This is Auto Loader's default for CSV and JSON.
- Alternative: typed bronze with `cloudFiles.inferColumnTypes` and schema hints. You catch type problems earlier and store less, but the type rules live in two places.

**4. One checkpoint per entity, in a governed volume in `ops`, and reset together with the table.**
- Why: each stream needs its own checkpoint. Keeping them in a Unity Catalog volume means they are governed and visible, and engineers can inspect them with `cloud_files_state()`. The table and the checkpoint are a pair: delete only the checkpoint and every file is reloaded, creating duplicates; drop only the table and the checkpoint still thinks every file is done, so the table stays empty.

**5. Metadata columns, with `_batch` parsed from the file path.**
- Why: `_source_file` tells me exactly which file a bad row came from, `_ingested_at` tells me when it arrived, and `_batch` lets me count and reconcile per delivery. Silver will also use them to pick the latest copy when duplicates arrive. I parse `_batch` explicitly from the path with a regex rather than rely on Auto Loader's inferred partition column, then drop that inferred `batch` column so there is only one.

**6. Schema evolution: `mergeSchema` on the write, default `addNewColumns` mode on the read.**
- Why: when batch 5 adds `contract_id`, Auto Loader notices the new column, updates the saved schema, and stops the stream once with an `UnknownFieldException`. The next run picks up the new schema, and `mergeSchema` lets the Delta table gain the column. In production a job retry handles that restart automatically.
- Alternative: `rescue` mode never fails and puts new columns into `_rescued_data`. More stable, but a new column can go unnoticed, which is worse for a data contract.

**7. Counting loaded rows from the table, not from stream progress.**
- Why: on serverless, the streaming progress metric `numInputRows` came back empty, so my counter printed 0 even when rows had loaded. I stopped trusting it and count the table before and after the load instead, which is valid because bronze is append-only. For tables updated by MERGE, I would read the operation metrics in `DESCRIBE HISTORY` instead.

### Delta concepts I should be able to explain

- A Delta table is Parquet data files plus a `_delta_log` folder of numbered JSON commit files. The log is the source of truth for which files make up each version.
- ACID: a write either commits fully or not at all, readers only ever see committed versions, and concurrent writers are checked against each other (optimistic concurrency), so one fails instead of corrupting the table.
- `DESCRIBE HISTORY` lists every version with the operation and metrics like `numOutputRows`.
- Time travel: `VERSION AS OF n` or `TIMESTAMP AS OF '...'` reads an old version. It only works while the old data files still exist, so `VACUUM` (default retention 7 days) limits how far back you can go.
- `DESCRIBE DETAIL` shows the storage location, number of files and size.

### Quiz answers to say out loud

**How does Auto Loader know what is new, compared with an ADF watermark?**
A watermark stores one value, such as the highest `updated_at` loaded so far, and the next run asks the source for anything greater than that value. Auto Loader tracks files instead of values. Its checkpoint keeps a record of every file it has already processed, and each run loads only files that are not in that record. The difference shows with late data. If a file arrives today containing goods receipts dated last week, those dates are older than the watermark, so a watermark load skips them. Auto Loader still loads the file, because it is a file it has not seen before.

**What is in the checkpoint, what happens if one side is deleted, and why did the crashes not cause duplicates?**
The checkpoint stores the list of files already processed, the progress of each micro-batch, and, because I pointed `schemaLocation` at it, the inferred schema and its history. If the checkpoint is deleted but the table is kept, the data in the table is safe, but the stream forgets what it has read, so the next run reloads every file and creates duplicates. If the table is dropped but the checkpoint is kept, the stream still thinks every file is done, so the recreated table stays empty apart from new files. The two must always be reset together. My crashes did not create duplicates because the crash happened after the write had finished. Each micro-batch is committed to the Delta table and then recorded in the checkpoint, and the Delta sink also remembers which micro-batches it has committed. So even if a run fails between those two steps, the restart skips the batch that was already written. That combination gives exactly-once loading.

**What happens when batch 5 adds `contract_id`?**
With the default mode, `addNewColumns`, Auto Loader detects the new column, adds it to the schema stored in the `schemaLocation`, and then deliberately stops the stream with an `UnknownFieldException`. That first run fails. On the next run, the stream starts with the updated schema and loads the data, and `mergeSchema` on the write allows the Delta table to add the new column. So `schemaLocation` is where Auto Loader keeps and evolves the source schema, and `mergeSchema` is what allows the target table to change. In production, a job retry handles the restart automatically. In `rescue` mode, the schema never changes and the stream never fails, and the `contract_id` values are stored in `_rescued_data` instead of a new column. That is more stable, but a new column can go unnoticed.

**Why `availableNow`, and why strings in bronze?**
The source delivers files in batches, not continuously, so a stream running all the time would mostly be paying for idle compute. `availableNow` processes everything new, then stops, so I only pay while there is work, and I still get exactly-once file tracking. It is also the trigger serverless compute supports. Bronze keeps every column as a string because its job is to store exactly what was delivered, without rejecting or changing anything. If bronze applied types, bad values would be turned into nulls at the first step. Keeping strings means all typing and validation happen in one place, in silver, where failed conversions are sent to quarantine with a clear reason.

**What makes a Delta write ACID, how does time travel work, and how did you prove batch 4 loaded once?**
A Delta table is a set of Parquet files plus a transaction log. A write first adds new data files, then commits by creating the next numbered entry in `_delta_log`. Until that commit exists, readers do not see the new files, so a write is either fully visible or not visible at all. Readers always read one complete version, and if two writers try to commit at the same time, Delta's optimistic concurrency control lets one succeed and makes the other retry or fail, so the table is never corrupted. Time travel reads an older version with `VERSION AS OF` or `TIMESTAMP AS OF`. It is limited by retention: `VACUUM` deletes old data files after seven days by default, and the log itself is kept for 30 days by default. To prove batch 4 loaded exactly once, I checked four things: rows per `_batch` showed batch 4 with exactly the row count the generator wrote and earlier batches unchanged; `DESCRIBE HISTORY` showed one new commit whose `numOutputRows` matched; `VERSION AS OF` the previous version returned the old total; and a further run with no new files added no rows.
