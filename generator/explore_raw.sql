-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Explore the raw files (phase 1 exercise)
-- MAGIC
-- MAGIC Query the files in the volume directly, before any table exists.
-- MAGIC Docs: `LIST`, and the `read_files` table-valued function.

-- COMMAND ----------

-- TODO 1: list the batch folders for purchase_orders (hint: LIST '<volume path>')

-- COMMAND ----------

-- TODO 2: read all supplier CSV files with read_files.
-- Hints: format => 'csv', header => true. What type did payment_terms_days get?
-- Is there a column you did not put in the file? Where did it come from?

-- COMMAND ----------

-- TODO 3: read purchase_orders (format => 'json'). What type is `lines`?

-- COMMAND ----------

-- TODO 4: one row per PO line. Hint: explode(lines), then select line.po_line_id, line.quantity, ...

-- COMMAND ----------

-- TODO 5: find duplicate receipt_ids across all goods_receipts batches (GROUP BY ... HAVING)

-- COMMAND ----------

-- TODO 6: count invoices with no PO, and their share of total amount.
-- Watch out: some amounts are text like '1,234.50'. What does read_files do with them?

-- COMMAND ----------

-- TODO 7: add the source file path to any query above with _metadata.file_path
