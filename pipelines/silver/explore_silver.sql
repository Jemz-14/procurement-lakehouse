-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver checks (phase 3 exercise)
-- MAGIC
-- MAGIC 1. Run `silver_transactions`, then TODO 1 to 6.
-- MAGIC 2. Idempotency: run the "forget progress" cell in `silver_transactions`, run the notebook
-- MAGIC    again, then rerun TODO 1 and TODO 2. Nothing may change. Then TODO 7.

-- COMMAND ----------

USE CATALOG procurement;

-- COMMAND ----------

-- TODO 1: prove there are no duplicate keys.
-- For silver.purchase_orders: GROUP BY po_id HAVING count(*) > 1 must return no rows.
-- Repeat for po_lines (po_line_id), goods_receipts (receipt_id), invoices (invoice_id).

-- COMMAND ----------

-- TODO 2: bronze vs silver.
-- count(*) and count(DISTINCT po_id) in bronze.purchase_orders, next to count(*) in silver.purchase_orders.
-- Why are the three numbers different?

-- COMMAND ----------

-- TODO 3: quarantine summary: rows per entity and reject_reason, largest first.

-- COMMAND ----------

-- TODO 4: pick one po_id that has more than one status in bronze.
-- Show its bronze rows ordered by updated_at, then its silver row. Did silver keep the latest?

-- COMMAND ----------

-- TODO 5: did standardisation work? SELECT DISTINCT status and DISTINCT cost_centre from silver.purchase_orders.

-- COMMAND ----------

-- TODO 6: DESCRIBE HISTORY silver.purchase_orders.
-- In operationMetrics, find numTargetRowsInserted and numTargetRowsUpdated for the MERGE.

-- COMMAND ----------

-- TODO 7 (after the idempotency rerun): DESCRIBE HISTORY again.
-- What did the latest MERGE insert and update? Why?
