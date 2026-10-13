-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Explore the raw files (phase 1 exercise)
-- MAGIC
-- MAGIC Query the files in the volume directly, before any table exists.
-- MAGIC Docs: `LIST`, and the `read_files` table-valued function.

-- COMMAND ----------

-- TODO 1: list the batch folders for purchase_orders (hint: LIST '<volume path>')
LIST '/Volumes/procurement/landing/raw'

-- COMMAND ----------

-- TODO 2: read all supplier CSV files with read_files.
-- Hints: format => 'csv', header => true. What type did payment_terms_days get?
-- Is there a column you did not put in the file? Where did it come from?
SELECT *
FROM read_files(
    '/Volumes/procurement/landing/raw/suppliers/',
    format => 'csv',
    header => true
)

-- COMMAND ----------

-- TODO 3: read purchase_orders (format => 'json'). What type is `lines`? (array)
SELECT *
FROM read_files(
    '/Volumes/procurement/landing/raw/purchase_orders/',
    format => 'json'
);

-- COMMAND ----------

-- TODO 4: one row per PO line. Hint: explode(lines), then select line.po_line_id, line.quantity, ...
SELECT line.po_line_id, line.quantity
FROM (
        SELECT explode(lines) AS line
        FROM read_files(
            '/Volumes/procurement/landing/raw/purchase_orders/',
            format => 'json')
);

-- COMMAND ----------

-- TODO 5: find duplicate receipt_ids across all goods_receipts batches (GROUP BY ... HAVING)
SELECT receipt_id
FROM read_files(
    '/Volumes/procurement/landing/raw/goods_receipts/',
    format => 'csv',
    header => true
)
GROUP BY receipt_id
HAVING COUNT(receipt_id) > 1;

-- COMMAND ----------

-- TODO 6: count invoices with no PO, and their share of total amount.
-- Watch out: some amounts are text like '1,234.50'. What does read_files do with them?
WITH invoices AS (
  SELECT po_id, CAST(REPLACE(amount, ',', '') AS DOUBLE) AS amount_eur
  FROM read_files('/Volumes/procurement/landing/raw/invoices/', format => 'json')
)

SELECT
    count_if(po_id IS NULL) AS maverick_count,
    COUNT(*) AS invoice_count,
    SUM(CASE WHEN po_id IS NULL THEN amount_eur ELSE 0 END) AS maverick_amount,
    SUM(amount_eur) AS total_amount,
    ROUND(maverick_amount / total_amount*100,2)
FROM invoices;

-- COMMAND ----------

-- TODO 7: add the source file path to any query above with _metadata.file_path
SELECT 
    invoice_id, 
    po_id, 
    _metadata.file_path
FROM read_files(
    '/Volumes/procurement/landing/raw/invoices/',
    format => 'json'
);