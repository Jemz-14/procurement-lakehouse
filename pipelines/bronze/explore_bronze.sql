-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze checks (phase 2 exercise)
-- MAGIC
-- MAGIC 1. Run `ingest_bronze` (batches 1 to 3 are already in the volume). Run TODO 1 to 3.
-- MAGIC 2. Generate batch 4 with `generator/run_generator` (batch_number = 4).
-- MAGIC 3. Run `ingest_bronze` again, then rerun TODO 1 to 3 and prove only batch 4 was loaded.
-- MAGIC 4. Run `ingest_bronze` a third time with no new files. How many rows load?
-- MAGIC 5. TODO 4 and 5: time travel.

-- COMMAND ----------

-- TODO 1: rows per _batch in bronze.purchase_orders (GROUP BY _batch ORDER BY _batch)
SELECT _batch, count(*) FROM procurement.bronze.purchase_orders GROUP BY _batch ORDER BY _batch;

-- COMMAND ----------

-- TODO 2: which files has Auto Loader processed for purchase_orders?
-- Hint: SELECT * FROM cloud_files_state('<checkpoint path for purchase_orders>')
SELECT *
FROM cloud_files_state('/Volumes/procurement/ops/checkpoints/bronze/purchase_orders/')

-- COMMAND ----------

-- TODO 3: DESCRIBE HISTORY on bronze.purchase_orders.
-- Look at the version, operation and operationMetrics (numOutputRows) columns.
DESCRIBE HISTORY procurement.bronze.purchase_orders

-- COMMAND ----------

-- TODO 4: row count of bronze.purchase_orders as it was BEFORE batch 4 (VERSION AS OF <n>)
SELECT COUNT(*)
FROM procurement.bronze.purchase_orders
VERSION AS OF 1

-- COMMAND ----------

-- TODO 5: the same with TIMESTAMP AS OF, using a timestamp from DESCRIBE HISTORY
SELECT COUNT(*)
FROM procurement.bronze.purchase_orders
TIMESTAMP AS OF '2026-10-12T13:20:00.000+00:00'

-- COMMAND ----------

-- TODO 6: DESCRIBE DETAIL bronze.purchase_orders. Where is the table stored, and how many files does it have?
DESCRIBE DETAIL procurement.bronze.purchase_orders