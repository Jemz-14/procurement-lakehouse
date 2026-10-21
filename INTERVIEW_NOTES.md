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

---

## Phase 3: Silver

### What we built

Silver tables for purchase orders, PO lines, goods receipts and invoices, plus `dim_supplier` as a hand-written SCD Type 2 dimension, and `ops.quarantine` for rejected rows. Each pipeline streams new rows from bronze with `readStream.table`, and in `foreachBatch` it types and standardises the data, validates it, sends failures to quarantine, deduplicates with `ROW_NUMBER` and `QUALIFY`, and MERGEs on the business key. Shared helpers live in one `silver_helpers` notebook that the pipeline notebooks load with `%run`. The tables are created up front with explicit DDL, comments and CHECK constraints.

What I proved: deleting the silver checkpoints and reprocessing everything changed no row counts, and `DESCRIBE HISTORY` showed the replayed MERGEs inserting and updating 0 rows. I also hit a real crash halfway through a micro-batch: the header MERGE had committed and the lines step failed. When I fixed the code and reran, the batch was replayed and the header MERGE changed nothing the second time. For `dim_supplier`, there is exactly one current row per supplier (48 suppliers, 48 current rows, 66 versions after batch 4). In batch 5 the source added `contract_id`, and the pipeline picked it up (see decision 9).

### Decisions, why, and the alternative

**1. `foreachBatch` with MERGE on business keys, so every write is idempotent.**
- Why: `foreachBatch` is at-least-once. After a failure, the same micro-batch can run again. A plain append would then duplicate rows, but a MERGE on the business key gives the same result however many times it runs.
- Alternative: append with Delta's idempotent write options, `txnAppId` and `txnVersion` set to the batch ID. That works for appends but not for upserts, and it breaks if the checkpoint is reset while the app ID stays the same. In phase 5, AUTO CDC does this declaratively.

**2. Validate first, then deduplicate, and guard the MERGE with `updated_at`.**
- Why: if a batch contains a good older version and a bad newer version of the same PO, silver keeps the latest valid one instead of losing the PO. The MERGE only updates when the incoming `updated_at` is newer than the target's, so a late-arriving older copy can never overwrite newer data.
- Alternative: deduplicate first. Simpler, but a corrupt latest version would push a good row into quarantine.

**3. Quarantine with every failed rule, the raw record, and a hash ID.**
- Why: `reject_reason` lists every rule a row failed, so whoever fixes the source sees all problems at once. `raw_record` keeps the original bronze row, so nothing is lost. The `quarantine_id` is a hash of the entity, the raw record and the source file, and quarantine is loaded with an insert-only MERGE on it, so replays do not duplicate rejects.
- Alternative: drop bad rows silently, which hides problems, or fail the whole pipeline on one bad row, which stops good data for everyone.

**4. Hard defects are rejected; soft defects are standardised.**
- Why: `upper(trim(status))` turns `' active '` into `ACTIVE`, which is the right value in the wrong format. A negative quantity or a 30 February date is wrong, and guessing a fix would put bad numbers in front of finance.

**5. Silver only checks rules within one row. Cross-entity checks come later.**
- Why: data arrives out of order. A receipt can legitimately arrive before its PO, so rejecting "receipt for an unknown PO" in silver would throw away valid data. Unknown supplier IDs and unknown PO lines are kept, flagged by data quality checks, and mapped to an "unknown" dimension member in gold.

**6. A PO is rejected as a whole if any of its lines is bad.**
- Why: header and lines stay consistent. The next valid version of the PO replaces it.

**7. Nested JSON is parsed in silver with an explicit schema.**
- Why: bronze keeps the PO lines as the original JSON text, because it stores everything as delivered. Silver parses them with `from_json` against an explicit schema, so the structure is a contract rather than a guess. Unparseable lines become null and are quarantined by the "no lines" rule.

**8. Explicit table DDL with types, comments and CHECK constraints; money as `DECIMAL(18,2)`.**
- Why: the table definition is the data contract for consumers. CHECK constraints such as `quantity > 0` are a safety net: if validation ever misses something, the write fails rather than storing bad data. Delta only allows CHECK constraints through `ALTER TABLE ... ADD CONSTRAINT`, not inline in `CREATE TABLE`. Decimals are exact; doubles cannot represent amounts like 0.10 exactly.

**9. New source columns flow through automatically.**
- Why: the cleaning step passes through any column it does not recognise, and `MERGE WITH SCHEMA EVOLUTION` adds it to the silver table. Old rows get null, so nothing downstream breaks. That is a backward-compatible schema change. Auto Loader stops once when it sees the new column (`addNewColumns` mode) and works on restart; the silver stream reading bronze can also stop once when the bronze schema changes. A job retry handles both.
- Alternative: contract-first, where unknown columns are ignored until an engineer adds them deliberately. Safer for certified datasets, but slower to pick up changes. A sensible middle ground is to auto-evolve silver but add new columns to gold deliberately.

**10. SCD Type 2 written by hand with a single MERGE.**
- Why: closing the old version and inserting the new one must happen atomically. The staged source contains each change twice: a close row with `merge_key = supplier_id`, which matches the current row and closes it, and an insert row with `merge_key = NULL`, which can never match and is therefore inserted. A batch can hold several versions of one supplier, so I close the current row at the earliest new version and insert every new version, with `valid_to` taken from `LEAD(updated_at)`. Only versions newer than the current row are processed, so a replay changes nothing. `supplier_sk` is an identity column, because `supplier_id` is no longer unique once history is kept.
- Limitations I would mention: a genuinely new version that arrives out of order, older than the current one, is ignored and would need a backfill. A re-sent version with no attribute changes but a newer timestamp would create a new version; in production I would compare a hash of the tracked attributes.
- Alternative: AUTO CDC in a declarative pipeline (phase 5), which does all of this from a few lines of configuration.

**11. Receipts and invoices are insert-only, and the earliest copy wins.**
- Why: they never change once sent in this source, and keeping the first copy means `_batch` records when each one first arrived, which matters for late-arrival analysis.

### Problems I hit and fixed (good stories to tell)

- **Inline CHECK constraints are not supported in `CREATE TABLE`**: Delta only accepts primary and foreign keys inline, so I moved them to `ALTER TABLE ... ADD CONSTRAINT`.
- **Nested JSON landed in bronze as a string**, because Auto Loader infers strings by default, including for nested structures. My `exists(lines, ...)` rule failed with a type error, which led to parsing with `from_json` in silver.
- **`x AS INT` renames a column, it does not cast it.** You need `try_cast(x AS INT) AS x`.
- **A silent SCD2 bug, caught in review:** `ON ... AND t.is_current IS NULL` never matches, so old rows would never close and suppliers would get two current rows, with no error. The check "current rows equals distinct suppliers" exists to catch exactly that.
- **Widget defaults only apply when the widget is first created.** Changing the default in code did not change the batch number. Because the generator is deterministic and Auto Loader ignores files it has already processed, regenerating batch 4 by mistake changed nothing.
- **On serverless, the streaming progress metric `numInputRows` was empty**, so I count rows from the table and from `DESCRIBE HISTORY` instead.

### Quiz answers to say out loud

**Take one purchase order from bronze to silver.**
"Silver reads only the new rows from bronze, using a streaming read on the bronze table, and processes each micro-batch in `foreachBatch`. First it cleans the record: it trims the IDs, upper-cases and trims codes like status and cost centre, converts dates and timestamps with `try_cast` so bad values become null instead of failing, and parses the nested lines from JSON text with an explicit schema. Then it checks the validation rules, and every rule the row fails is listed in `reject_reason`. Failed rows go to `ops.quarantine` with the reason and the original raw record. Valid rows are deduplicated to one per `po_id`, keeping the newest `updated_at`, and merged into `silver.purchase_orders`. The MERGE only updates when the incoming version is newer, so an older copy arriving late cannot overwrite it. Finally the lines are exploded to one row per line and merged into `silver.po_lines` the same way. A negative quantity on any line fails the line quantity rule, so the whole PO version is quarantined, which keeps the header and lines consistent. A status of `' approved '` is not an error: it is the right value in the wrong format, so it is standardised to `APPROVED` and the row is kept."

**What does at-least-once mean, and how did you make silver safe to rerun?**
"At-least-once means that after a failure, Spark can run the same micro-batch again, because the checkpoint only records a batch as finished once the whole `foreachBatch` function succeeds. So every write inside it must give the same result when repeated. I made all of them MERGEs on the business key: POs and lines only update when the source is newer, receipts and invoices are insert-only, and quarantine is an insert-only MERGE on a hash of the record. This was tested for real: a run failed halfway through a batch, after the header MERGE had committed but before the lines step. When I fixed the code and reran, Spark replayed that batch, and `DESCRIBE HISTORY` showed the header MERGE inserting and updating 0 rows. I then deleted the checkpoints on purpose to force a full reprocess, and the row counts did not change."

**Walk me through your SCD Type 2 MERGE.**
"Changing a supplier needs two actions at once: close the current version and insert the new one, and they must be atomic. A MERGE can only take one action per matched row, so I build a staged source that contains each change twice. The close row has `merge_key` set to the supplier ID, so it matches the supplier's current row, and the MERGE sets that row's `valid_to` and makes it not current. The insert row has `merge_key` set to NULL. NULL never equals anything, so it can never match, and it falls through to `WHEN NOT MATCHED`, where it is inserted as a new version. I also guard the insert with `merge_key IS NULL`, so a brand-new supplier's close row, which matches nothing, is not inserted a second time. If one batch has three new versions of the same supplier, I first keep only versions newer than the current row, then use `LEAD` to give each version a `valid_to` equal to the next version's start. The current row is closed at the earliest new version, all three are inserted, and only the last one is current. To detect a broken implementation, I check that the number of current rows equals the number of distinct suppliers, that each version's `valid_to` equals the next version's `valid_from`, and that rerunning the same data changes nothing."

**What happened when batch 5 added `contract_id`, and would you let gold evolve the same way?**
"In bronze, Auto Loader noticed the new column, added it to the schema stored in the schema location, and deliberately stopped the stream. The rerun loaded the file with the new schema, and `mergeSchema` added the column to the bronze table. In silver, the cleaning step passes through any column it does not recognise, and `MERGE WITH SCHEMA EVOLUTION` with `UPDATE SET *` and `INSERT *` added `contract_id` to the silver table. Older POs simply have null, so nothing that used the table before breaks: that is a backward-compatible change. I would not let gold evolve automatically. Gold tables are certified datasets that dashboards and KPIs depend on, so a new column there should be added deliberately, documented, and communicated through change control. My approach would be to evolve silver automatically and promote new columns into gold on purpose."

**Why `ROW_NUMBER` and not `RANK`, why the extra `ORDER BY` columns, and how would you write it without `QUALIFY`?**
"`ROW_NUMBER` gives every row in a partition a unique number, so filtering on 1 always keeps exactly one row per key. `RANK` and `DENSE_RANK` give tied rows the same number, so two copies with the same `updated_at` would both be ranked 1 and both kept, which defeats deduplication. The extra columns, `_ingested_at` and `_source_file`, are tie-breakers. If two copies have the same `updated_at` and nothing else decides between them, Spark can return either one, and the result might change between runs. The tie-breakers make the choice deterministic, so the same input always gives the same output. Without `QUALIFY`, you compute the row number in a subquery and filter in the outer query, because `WHERE` is evaluated before window functions and cannot reference them directly: `SELECT * EXCEPT (rn) FROM (SELECT *, ROW_NUMBER() OVER (...) AS rn FROM po_valid) WHERE rn = 1`. `QUALIFY` is simply a filter that runs after the window functions, like `HAVING` runs after aggregation."

---

## Phase 4: Gold

### What we built

A star schema in `procurement.gold`, rebuilt from silver on every run with `CREATE OR REPLACE TABLE ... AS SELECT`:
- dimensions: `dim_date` (generated calendar), `dim_category`, and `dim_supplier` as a view over the silver SCD Type 2 table plus an unknown member with key `-1`
- facts: `fact_po_line` (one row per PO line), `fact_receipt` (one row per receipt, with an `is_orphan` data quality flag) and `fact_invoice` (one row per invoice, with `is_maverick`)
- marts: `mart_spend_monthly` (month, supplier, category, with a running total and share of month), `mart_supplier_performance` (on-time, in-full and OTIF percentages per supplier and month) and `mart_invoice_match` (three-way match per PO with a match status)

Every gold table has a comment and a stated grain.

### Decisions, why, and the alternative

**1. State the grain of every table before building it.**
- Why: the grain decides the `GROUP BY`, tells users what they can safely add up, and gives a simple check: a fact with one row per PO line must have exactly as many rows as silver has PO lines. More rows means a join duplicated something.

**2. Facts store the surrogate key of the supplier version valid at the time of the event.**
- Why: the fact joins to the dimension row whose window contains the event date: `valid_from` inclusive, `valid_to` exclusive, and an open end for the current version. So a PO from January keeps the payment terms that applied in January, even after the supplier moves to new terms. Start inclusive and end exclusive means a date on a boundary belongs to exactly one version.
- Alternative: join on the business key to the current row only (SCD Type 1 behaviour). Simpler, but history is rewritten: last year's report would show this year's terms.

**3. An unknown member instead of dropping rows.**
- Why: facts whose supplier cannot be found, such as a supplier still in quarantine or an unknown ID like `SUP-9999`, get `supplier_sk = -1` through a left join and `coalesce`. With an inner join that spend would silently disappear from every report. With an unknown member it stays visible and can be chased up.

**4. Aggregate each side to the target grain before joining (avoid fan-out).**
- Why: a PO with three lines and two invoices joined directly gives six rows, and every amount is counted several times. In the three-way match I aggregate ordered, received and invoiced values to one row per PO first, then join them one to one. In OTIF I collapse receipts to one row per PO line before joining to the lines. And I always join on the most specific shared key: receipts join to lines on `po_line_id`, not `po_id`.

**5. Flags in the SELECT, filters in the WHERE.**
- Why: to calculate an on-time percentage you need the late lines as well as the on-time ones, so each line gets a true or false flag and the rate is `avg(cast(flag as int))`. `WHERE` is only for rows that must never count, such as cancelled POs in spend.

**6. Full rebuild with `CREATE OR REPLACE TABLE`, not incremental.**
- Why: the data is small, a full rebuild is always correct, and the replace is atomic, so readers see either the old or the new table, never half of one. It keeps the table's history for time travel.
- Alternative: materialized views, which Databricks refreshes, incrementally where it can; or incremental MERGE driven by Change Data Feed (phase 8). Both scale better, at the cost of more moving parts.

**7. `gold.dim_supplier` is a view over silver.**
- Why: silver owns the SCD Type 2 logic. Gold only adds the unknown member, so there is one copy of the logic and nothing to keep in sync.

**8. The three-way match tolerance is 1 percent.**
- Why: real matching always allows a small tolerance for rounding and minor price differences. In a real team that number is a business rule agreed with procurement, not an engineering choice. The `CASE` checks the missing cases first (nothing received, nothing invoiced), because `CASE` returns the first branch that is true.

### SQL points to be able to explain

- Every dimension joins to the fact, never to another dimension: that is what makes it a star.
- `SUM(x) OVER (PARTITION BY a ORDER BY month)` is a running total; without `ORDER BY` it is the partition total, which gives shares.
- Averaging percentages across suppliers is wrong; weight them by the number of lines.
- After a `LEFT JOIN`, a NULL key on the right-hand side means "no match", which is how "not received" and "not invoiced" are detected.

### Quiz answers to say out loud

**What is the grain, and how did you check it?**
"`fact_po_line` has one row per purchase order line, and `mart_spend_monthly` has one row per month, supplier and category. I checked the fact by comparing its row count with `silver.po_lines`: they must be equal. More rows would mean a join duplicated lines, for example the supplier join matching two versions because their validity windows overlap. Fewer rows would mean an inner join dropped lines."

**How do a January PO and a March PO find the right supplier version?**
"The supplier has two versions: 30-day terms valid until 14 February, and 60-day terms from 14 February with no end date. The fact joins on the supplier ID and on the order date falling inside the version's window: on or after `valid_from`, and before `valid_to` or with no `valid_to` at all. The January PO only fits the first window, so it gets the 30-day version. The March PO only fits the second, open window, so it gets 60 days. The start is inclusive and the end exclusive because one version's end is exactly the next version's start, so a PO dated 14 February belongs to exactly one version, never to both and never to neither."

**Why a left join and an unknown member instead of an inner join?**
"Some facts have no matching supplier version: an unknown ID like `SUP-9999`, a supplier whose only record is still in quarantine, or a date before the supplier's first version. An inner join would drop those rows, so total spend in every report would be understated and nobody would know. With a left join and `coalesce(supplier_sk, -1)`, that spend appears under 'Unknown supplier', the totals stay complete, and the data quality problem is visible so someone can fix it."

**Why aggregate to `po_id` before joining in the three-way match?**
"Because the three sides have different grains. A PO with three lines and two invoices, joined directly, produces six rows. Summing the ordered amount over those rows counts it twice, and the invoiced amount three times, so a correctly invoiced PO would look badly over-invoiced. Aggregating ordered, received and invoiced to one row per PO first makes every join one to one, so each amount is counted once."

**How do you calculate on-time percentage, and why not average the supplier percentages?**
"Each received line gets a boolean flag, `last_received_date <= expected_delivery_date`, in the `SELECT`. The rate is `avg(cast(flag as int)) * 100`, because true becomes 1 and false 0, so the average of the ones and zeros is the share that were on time. It cannot be a `WHERE`, because filtering out the late lines would leave only on-time lines and every supplier would score 100 percent. Across all suppliers I would not average the per-supplier percentages, because a supplier with 2 lines would count as much as one with 200. The correct overall rate is total on-time lines divided by total lines, or the supplier percentages weighted by their line counts."
