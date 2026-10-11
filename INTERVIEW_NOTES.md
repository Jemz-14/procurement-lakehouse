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

### Honesty check

I built this on **Databricks Free Edition**. I should say that plainly. It means serverless only, no custom storage locations, no account console, so some production features (external locations, real groups from SCIM, job clusters with custom sizes) I can explain but did not configure myself.
