-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold checks (phase 4 exercise)
-- MAGIC Run after `gold_build`. Try running these in the SQL editor on the SQL warehouse too.

-- COMMAND ----------

USE CATALOG procurement;

-- COMMAND ----------

-- TODO 1: grain check. count(*) of gold.fact_po_line next to count(*) of silver.po_lines. They must be equal.
SELECT 
    'gold' AS layer,
    COUNT(po_line_id) AS `po line id count`
FROM gold.fact_po_line

UNION ALL

SELECT 
    'silver' AS layer,
    COUNT(po_line_id) AS `po line id count`
FROM silver.po_lines;

-- COMMAND ----------

-- TODO 2: how much spend sits on the unknown supplier (supplier_sk = -1), and which supplier_ids cause it?
-- Hint: join fact_po_line back to silver.purchase_orders to see the original supplier_id.
select count(supplier_id)
from gold.mart_spend_monthly
JOIN silver.purchase_orders

-- COMMAND ----------

select *
from silver.dim_supplier

-- COMMAND ----------

-- TODO 3 (the phase 4 exercise): prove the point-in-time join.
-- Pick a supplier whose payment_terms_days changed (more than one version in silver.dim_supplier with
-- different terms). Show its PO lines: order_date, supplier_sk, and payment_terms_days from
-- gold.dim_supplier joined on supplier_sk, ordered by order_date.
-- POs before the change date must show the old terms, POs after it the new terms.

-- the supplier whose payment terms changed, with the most PO lines
WITH changed AS (
  SELECT supplier_id
  FROM procurement.silver.dim_supplier
  GROUP BY supplier_id
  HAVING count(DISTINCT payment_terms_days) > 1
)
SELECT s.supplier_id, f.order_date, f.supplier_sk, s.payment_terms_days, s.valid_from, s.valid_to
FROM procurement.gold.fact_po_line AS f
JOIN procurement.gold.dim_supplier AS s ON f.supplier_sk = s.supplier_sk
WHERE s.supplier_id = (SELECT supplier_id FROM changed LIMIT 1)
ORDER BY f.order_date;


-- COMMAND ----------

-- TODO 4: the 10 largest three-way match exceptions by absolute variance.

-- COMMAND ----------

-- TODO 5: suppliers ranked by OTIF % over all months (only suppliers with at least 20 received lines).
-- Use RANK() and explain why RANK is the right choice here when it was the wrong choice for dedup.