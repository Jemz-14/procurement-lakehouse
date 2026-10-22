-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data quality gate
-- MAGIC
-- MAGIC Last task of the job. Each check uses `assert_true`, which raises an error when its condition is
-- MAGIC false, so one failed check fails the task and the run goes red. These are the checks done by
-- MAGIC hand in the explore notebooks, automated.
-- MAGIC
-- MAGIC | Check | Protects against |
-- MAGIC |---|---|
-- MAGIC | no duplicate business keys in silver | a broken MERGE or dedup |
-- MAGIC | one current row per supplier | a broken SCD Type 2 close |
-- MAGIC | fact_po_line has the grain of silver.po_lines | fan-out in the point-in-time join |
-- MAGIC | reject rate under the threshold | a bad delivery from the source system |
-- MAGIC | silver is as fresh as bronze | a silver stream that silently stopped |

-- COMMAND ----------

USE CATALOG procurement;

-- COMMAND ----------

-- 1. No duplicate business keys in silver.
SELECT assert_true(count(*) = 0, 'Duplicate po_id in silver.purchase_orders')
FROM (SELECT po_id FROM silver.purchase_orders GROUP BY po_id HAVING count(*) > 1);

SELECT assert_true(count(*) = 0, 'Duplicate po_line_id in silver.po_lines')
FROM (SELECT po_line_id FROM silver.po_lines GROUP BY po_line_id HAVING count(*) > 1);

SELECT assert_true(count(*) = 0, 'Duplicate receipt_id in silver.goods_receipts')
FROM (SELECT receipt_id FROM silver.goods_receipts GROUP BY receipt_id HAVING count(*) > 1);

SELECT assert_true(count(*) = 0, 'Duplicate invoice_id in silver.invoices')
FROM (SELECT invoice_id FROM silver.invoices GROUP BY invoice_id HAVING count(*) > 1);

-- COMMAND ----------

-- 2. SCD Type 2: every supplier has exactly one current version.
SELECT assert_true(count(*) = 0, 'A supplier in silver.dim_supplier does not have exactly one current row')
FROM (SELECT supplier_id FROM silver.dim_supplier GROUP BY supplier_id HAVING count_if(is_current) <> 1);

-- COMMAND ----------

-- 3. Grain: one fact row per PO line, no more and no fewer.
SELECT assert_true(
  (SELECT count(*) FROM gold.fact_po_line) = (SELECT count(*) FROM silver.po_lines),
  'gold.fact_po_line row count differs from silver.po_lines (fan-out or dropped rows)'
);

-- COMMAND ----------

-- 4. Reject rate: share of all delivered rows that ended up in quarantine.
-- THRESHOLD: 10 percent. The generator's error rate is 5 percent, so a normal run is well below it.
SELECT assert_true(
  rejected * 100.0 / delivered <= 10,
  concat('Reject rate ', round(rejected * 100.0 / delivered, 2), '% is above the 10% threshold')
)
FROM (
  SELECT
    (SELECT count(*) FROM ops.quarantine) AS rejected,
    (SELECT count(*) FROM bronze.suppliers) + (SELECT count(*) FROM bronze.purchase_orders)
      + (SELECT count(*) FROM bronze.goods_receipts) + (SELECT count(*) FROM bronze.invoices) AS delivered
);

-- COMMAND ----------

-- 5. Freshness: the latest batch in bronze has reached silver.
SELECT assert_true(
  (SELECT max(_batch) FROM silver.purchase_orders) = (SELECT max(_batch) FROM bronze.purchase_orders),
  'silver.purchase_orders is behind bronze: the latest batch has not been processed'
);

-- COMMAND ----------

-- Informational, does not fail the run: cross-entity issues kept in silver by design.
SELECT
  (SELECT count_if(is_orphan) FROM gold.fact_receipt)                  AS orphan_receipts,
  (SELECT count(*) FROM gold.fact_po_line WHERE supplier_sk = -1)      AS po_lines_unknown_supplier,
  (SELECT count(*) FROM gold.mart_invoice_match WHERE match_status IN ('OVER_INVOICED', 'UNDER_INVOICED'))
                                                                       AS invoice_match_exceptions;