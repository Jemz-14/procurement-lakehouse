-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Declarative pipeline checks (phase 5 exercise)
-- MAGIC Run after the pipeline has completed an update.

-- COMMAND ----------

-- Run once, before creating the pipeline.
CREATE SCHEMA IF NOT EXISTS procurement.ldp COMMENT 'Lakeflow declarative pipeline version of bronze and silver';

-- COMMAND ----------

USE CATALOG procurement;

-- COMMAND ----------

-- 1. SCD Type 2 from AUTO CDC vs the hand-written one: versions, current rows, suppliers.
-- AUTO CDC marks the current version with __END_AT IS NULL.
SELECT 'auto_cdc' AS version, count(*) AS versions, count_if(__END_AT IS NULL) AS current_rows,
       count(DISTINCT supplier_id) AS suppliers
FROM ldp.silver_dim_supplier
UNION ALL
SELECT 'hand_written', count(*), count_if(is_current), count(DISTINCT supplier_id)
FROM silver.dim_supplier;

-- COMMAND ----------

-- 2. One supplier's history in both tables, side by side.
SELECT supplier_id, payment_terms_days, status, __START_AT, __END_AT
FROM ldp.silver_dim_supplier
WHERE supplier_id = (SELECT supplier_id FROM silver.dim_supplier GROUP BY 1 HAVING count(*) > 2 LIMIT 1)
ORDER BY __START_AT;

-- COMMAND ----------

-- 3. Invoices: AUTO CDC vs hand-written. Do the counts match? If not, why?
SELECT (SELECT count(*) FROM ldp.silver_invoices) AS auto_cdc_invoices,
       (SELECT count(*) FROM silver.invoices)     AS hand_written_invoices;

-- COMMAND ----------

-- 4. The materialized view.
SELECT * FROM ldp.mart_maverick_monthly ORDER BY year_month;

-- COMMAND ----------

-- 5. Data quality metrics from the event log: rows passing and failing each expectation.
SELECT
  timestamp,
  details:flow_progress:data_quality:expectations AS expectations
FROM event_log(TABLE(procurement.ldp.invoices_clean))
WHERE event_type = 'flow_progress'
  AND details:flow_progress:data_quality:expectations IS NOT NULL
ORDER BY timestamp DESC;
