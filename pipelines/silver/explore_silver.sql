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

SELECT invoice_id
FROM silver.invoices
GROUP BY invoice_id HAVING COUNT(*) > 1


-- COMMAND ----------

-- TODO 2: bronze vs silver.
-- count(*) and count(DISTINCT po_id) in bronze.purchase_orders, next to count(*) in silver.purchase_orders.
-- Why are the three numbers different?
WITH bronze_metrics AS (
    SELECT 
        COUNT(*) AS bronze_total_records,
        COUNT(DISTINCT po_id) AS bronze_distinct_po_ids
    FROM bronze.purchase_orders
),
silver_metrics AS (
    SELECT 
        COUNT(*) AS silver_total_records
    FROM silver.purchase_orders
)
SELECT *
FROM bronze_metrics
CROSS JOIN silver_metrics;

-- COMMAND ----------

-- TODO 3: quarantine summary: rows per entity and reject_reason, largest first.
SELECT entity,reject_reason, COUNT(*) AS rejected_rows
FROM ops.quarantine
GROUP BY entity, reject_reason
ORDER BY rejected_rows DESC;

-- COMMAND ----------

-- TODO 4: pick one po_id that has more than one status in bronze.
-- Show its bronze rows ordered by updated_at, then its silver row. Did silver keep the latest?
SELECT 
    'bronze' AS layer,
    po_id,
    status,
    updated_at,
    _ingested_at
FROM bronze.purchase_orders
WHERE po_id = 'PO-000342'

UNION ALL

SELECT 
    'silver' AS layer,
    po_id,
    status,
    updated_at,
    _ingested_at
FROM silver.purchase_orders
WHERE po_id = 'PO-000342'

ORDER BY
layer ASC,
updated_at ASC

-- COMMAND ----------

-- TODO 5: did standardisation work? SELECT DISTINCT status and DISTINCT cost_centre from silver.purchase_orders.


-- COMMAND ----------

-- TODO 6: DESCRIBE HISTORY silver.purchase_orders.
-- In operationMetrics, find numTargetRowsInserted and numTargetRowsUpdated for the MERGE.
DESCRIBE HISTORY procurement.silver.purchase_orders

-- COMMAND ----------

-- TODO 7 (after the idempotency rerun): DESCRIBE HISTORY again.
-- What did the latest MERGE insert and update? Why?

-- COMMAND ----------

SELECT _batch, COUNT(*) as POS, COUNT(contract_id) AS with_contract
FROM procurement.silver.purchase_orders
GROUP BY _batch
ORDER BY _batch;