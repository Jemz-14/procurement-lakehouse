# Procurement Lakehouse on Databricks: Project Brief

## Status (update at the end of every phase)

- Current phase: **1 (Generator)**, in progress. Phase 0 done (catalog `procurement` created).
- Catalog in use: `procurement` (CREATE CATALOG works on Free Edition)
- Last session: Mon 12 Oct 2026 (one day behind schedule)

## Context (read this first)

I'm James, a data engineer. I have one year of experience at a 3PL fulfilment company, where I built Azure pipelines: ADF watermark incremental loads from the ShipStation API, ADLS raw JSON, Synapse serverless and dedicated SQL, MERGE upserts, a quarantine table for bad rows, a star schema with SCD Type 1, and Power BI.

On **Fri 23 Oct 2026** I have a technical interview for a Data Engineer role in the **Procurement Data & Insights** team at Betfair Romania (part of Flutter). The interviewers are the Procurement Data & Insights Lead and a Senior Data Engineer. The interview is mostly about **Databricks and medallion architecture**, plus Python and SQL questions.

**I have never used Databricks.** This project is how I learn it. The goal is understanding, not just a finished repo. In the interview I need to explain every design decision out loud and say truthfully, "I built this on Databricks."

### What the role cares about (from the job description)

Flutter Procurement governs over $6bn of third-party spend. The role builds and runs procurement pipelines, governance and analytics on Databricks: ingestion, transformation and orchestration; data contracts and schema changes with backward compatibility; monitoring, alerting, runbooks, MTTR; data quality framework and SLAs; RBAC, privacy, retention; catalog, lineage and glossary; dimensional models for suppliers, spend, contracts, POs, invoices and savings; supplier normalisation (master data); certified datasets and KPI definitions for BI; performance and cost tuning.

## How to work with me (follow these rules)

1. **Teach before you build.** At the start of each phase, explain the key concepts in 5 to 10 lines. Where it helps, compare them to the Azure tools I know (ADF, ADLS, Synapse).
2. **I write the core logic.** For the important pieces, give me the goal, the relevant API names and a skeleton with TODOs, then let me write the code. Review it afterwards and point out mistakes. The important pieces are: the Auto Loader read, deduplication, MERGE, SCD Type 2, the gold aggregations and the expectations. You can write boilerplate, config, setup and the data generator yourself.
3. **One phase at a time.** Don't start the next phase until I say so.
4. **Quiz me at the end of every phase.** Ask 5 interview-style questions about what we built. Wait for my answers, then correct or improve them.
5. **Keep `INTERVIEW_NOTES.md` up to date.** For each phase, record: what we built, the design decision, why we made it, the alternative and its trade-off. Write them in plain spoken English I can say in an interview.
6. **Use current product names and check the Databricks docs** when you're unsure about an API, since several things were renamed recently. Always give me the old name too, because interviewers may use either. (See the name table in `INTERVIEW_NOTES.md`.)
7. **Keep it simple.** Don't over-engineer. If something doesn't work on Free Edition, tell me and use the closest alternative.
8. **Keep it honest.** If I'm overstating what I built or understand, tell me.
9. **No em dashes** in any docs you write.

## Environment

- **Databricks Free Edition:** serverless compute only, small quotas (fair usage policy; exceeding it stops compute for the day), Unity Catalog enabled, outbound internet limited to trusted domains, one 2X-Small SQL warehouse, one active pipeline per pipeline type, max 5 concurrent job tasks, no account console, no SSO/SCIM, no custom storage locations. Check what Free Edition allows before relying on a feature.
- **GitHub repo:** I edit code locally with Claude and sync it to Databricks through a **Git folder**.
- **Languages:** PySpark and Spark SQL for the pipelines, plain Python for the generator.

## Data source

**Synthetic procurement data** from our own Python generator. It runs as a Databricks notebook and writes files into a Unity Catalog **volume**, to simulate a procurement system (for example Coupa or SAP Ariba) dropping daily extracts.

### Entities

| Entity | Format | Key columns |
|---|---|---|
| suppliers | CSV | supplier_id, name, country, category, payment_terms_days, status, updated_at |
| purchase_orders | JSON with nested lines | po_id, supplier_id, buyer, cost_centre, order_date, status, updated_at, lines[po_line_id, item, category, quantity, unit_price, expected_delivery_date] |
| goods_receipts | CSV | receipt_id, po_id, po_line_id, received_qty, received_date |
| invoices | JSON | invoice_id, supplier_id, po_id (nullable), invoice_date, amount, due_date, paid_date |

All amounts are in EUR, no currency conversion.

### Deliberate problems (configurable error rate)

- duplicate records across batches, and records updated later with a newer `updated_at`
- suppliers changing terms or status over time (to drive SCD Type 2)
- nulls in required fields, bad dates, negative quantities, invalid status values
- invoices with no PO (maverick spend)
- invoice amounts that don't match PO price x received quantity
- late-arriving goods receipts
- in **batch 5**, a new column added to purchase_orders (to test schema evolution)

### Generator

Parameters: `batch_number`, `start_date`, `rows_per_entity`, `error_rate`, `seed`.
Output: `/Volumes/<catalog>/landing/raw/<entity>/batch=<n>/`

## Target architecture

| Schema | Contents |
|---|---|
| `landing` | volume `raw` for incoming files |
| `bronze` | raw data as delivered, append only, with `_ingested_at`, `_source_file`, `_batch` |
| `silver` | typed, cleaned, deduplicated, standardised; MERGE upserts on business keys; `dim_supplier` as SCD Type 2 |
| `gold` | star schema and procurement marts |
| `ops` | quarantine table (bad rows with a reason), run logs, volume `checkpoints` |

### Gold layer

- **Facts:** `fact_po_line` (one row per PO line), `fact_receipt`, `fact_invoice`
- **Dimensions:** `dim_supplier` (SCD Type 2), `dim_category`, `dim_date`
- **Marts:** `mart_spend_monthly`, `mart_supplier_performance` (on-time %, in-full %, OTIF), `mart_invoice_match` (three-way match), `mart_maverick_spend`, `mart_payment_compliance`

## Phases

- **0 Setup:** workspace, compute types, UC namespace, managed vs external, volumes. Build repo, Git folder, catalog/schemas/volume via SQL notebook.
- **1 Generator:** clean functions, seeding, JSON/CSV, nested structures. Produce batches 1 to 3. Exercise: query files with `read_files`.
- **2 Bronze with Auto Loader:** `cloudFiles`, checkpoints, `schemaLocation`, inference and hints, rescued data, `availableNow`, file tracking vs ADF watermarks, Delta log, ACID, history, time travel. Exercise: batch 4 processes only new files; time travel query.
- **3 Silver:** incremental reads, `foreachBatch` + MERGE, idempotency, `ROW_NUMBER`/`QUALIFY`, exploding PO lines, quarantine, schema evolution, hand-written SCD Type 2. Exercise: rerun a batch with no duplicates; batch 5 new column.
- **4 Gold and SQL:** grain, identity surrogate keys, point-in-time SCD2 joins, MVs vs tables, SQL warehouses, AI/BI dashboards. Exercise: change supplier terms, old POs keep old version.
- **5 Lakeflow Declarative Pipelines (formerly DLT):** streaming tables vs MVs, expectations, AUTO CDC (formerly `apply_changes`), event log. Exercise: hand-written vs declarative comparison in notes.
- **6 Orchestration:** Lakeflow Jobs (formerly Workflows), tasks, params, retries, repair runs, job vs all-purpose compute cost, Asset Bundles if supported. Exercise: fail a task, repair run.
- **7 Governance:** GRANTs, row filters, column masks, comments, tags, lineage, system tables. Exercise: lineage from gold mart to bronze files.
- **8 Performance and cost:** small files, OPTIMIZE, liquid clustering vs partitioning vs Z-ORDER, VACUUM and time travel, predictive optimization, query profile, EXPLAIN, broadcast joins, AQE, Change Data Feed. Exercise: larger batch, before/after.
- **9 Wrap-up:** README with Mermaid diagram and "What I'd add in production", final notes pass, 3-minute walkthrough script, mock interview.

## Schedule (interview Fri 23 Oct)

| Day | Work |
|---|---|
| Sun 11 | Phases 0 and 1 |
| Mon 12 | Phase 2 |
| Tue 13 | Phase 3 (first half) |
| Wed 14, Thu 15 | Travelling, no project work (revise `INTERVIEW_NOTES.md` offline) |
| Fri 16 | Finish phase 3, then phase 4 |
| Sat 17 | Phase 5 |
| Sun 18 | Phases 6 and 7 |
| Mon 19 | Another test in the morning, then phase 8 |
| Tue 20 | Phase 9 |
| Wed 21 | Mock interview and fixing weak spots |
| Thu 22 | Light review only |

If we fall behind, cut in this order: phase 8 extras, then phase 7 row filters, then the dashboard. Never cut phases 2 to 5.

## Definition of done

- The job runs end to end from a fresh batch, and rerunning it creates no duplicates.
- I can explain each layer, every MERGE, the SCD Type 2 logic, Auto Loader, expectations, and liquid clustering without notes.
- `INTERVIEW_NOTES.md` and the 3-minute walkthrough are finished.
