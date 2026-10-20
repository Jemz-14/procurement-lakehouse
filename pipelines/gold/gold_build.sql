-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: star schema and procurement marts
-- MAGIC
-- MAGIC Full rebuild from silver on every run with `CREATE OR REPLACE TABLE ... AS SELECT`.
-- MAGIC The data is small, a rebuild is always correct, the replace is atomic, and time travel keeps old versions.
-- MAGIC
-- MAGIC | Table | Type | Grain (one row per...) |
-- MAGIC |---|---|---|
-- MAGIC | `dim_date` | dimension | calendar day |
-- MAGIC | `dim_category` | dimension | spend category |
-- MAGIC | `dim_supplier` | dimension (view) | supplier version (SCD Type 2), plus an unknown member `-1` |
-- MAGIC | `fact_po_line` | fact | purchase order line |
-- MAGIC | `fact_receipt` | fact | goods receipt |
-- MAGIC | `fact_invoice` | fact | supplier invoice |
-- MAGIC | `mart_spend_monthly` | mart | month, supplier, category |
-- MAGIC | `mart_supplier_performance` | mart | supplier, month of expected delivery |
-- MAGIC | `mart_invoice_match` | mart | purchase order with an invoice or a receipt |

-- COMMAND ----------

USE CATALOG procurement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_date
COMMENT 'Calendar, one row per day, 2025 to 2027'
AS
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d                                       AS date,
  year(d)                                 AS year,
  quarter(d)                              AS quarter,
  month(d)                                AS month,
  date_format(d, 'MMMM')                  AS month_name,
  date_format(d, 'yyyy-MM')               AS year_month,
  weekofyear(d)                           AS week_of_year,
  dayofweek(d)                            AS day_of_week,   -- 1 = Sunday
  dayofweek(d) IN (1, 7)                  AS is_weekend
FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d);

-- COMMAND ----------

-- Keys are numbered on each rebuild. That is fine because the facts are rebuilt in the same run.
CREATE OR REPLACE TABLE gold.dim_category
COMMENT 'Spend categories'
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY category) AS category_key,
  category                              AS category_name
FROM (
  SELECT category FROM silver.po_lines     WHERE category IS NOT NULL
  UNION
  SELECT category FROM silver.dim_supplier WHERE category IS NOT NULL
);

-- COMMAND ----------

-- Silver owns the SCD Type 2 logic; gold exposes it and adds the unknown member.
CREATE OR REPLACE VIEW gold.dim_supplier
COMMENT 'Supplier dimension, SCD Type 2, one row per supplier version. supplier_sk = -1 is the unknown supplier.'
AS
SELECT supplier_sk, supplier_id, name AS supplier_name, country, category,
       payment_terms_days, status, valid_from, valid_to, is_current
FROM silver.dim_supplier
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown supplier', NULL, NULL, NULL, NULL, TIMESTAMP'1900-01-01', NULL, true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Facts

-- COMMAND ----------

-- TODO F1 [core]: gold.fact_po_line, one row per PO line.
--
-- CREATE OR REPLACE TABLE gold.fact_po_line COMMENT '...' AS
-- FROM silver.po_lines AS l
--   JOIN silver.purchase_orders AS h            on po_id (every line has a valid header)
--   LEFT JOIN silver.dim_supplier AS d          POINT-IN-TIME:
--        same supplier_id
--        AND CAST(h.order_date AS TIMESTAMP) >= d.valid_from
--        AND (d.valid_to IS NULL OR CAST(h.order_date AS TIMESTAMP) < d.valid_to)
--   LEFT JOIN gold.dim_category AS c            on category name
--
-- Columns:
--   l.po_line_id, l.po_id,
--   coalesce(d.supplier_sk, -1) AS supplier_sk      (the unknown member when no version matches)
--   c.category_key,
--   order_date_key and expected_delivery_date_key   (date_format(..., 'yyyyMMdd') cast to INT, as in dim_date)
--   h.order_date, l.expected_delivery_date,
--   h.buyer, h.cost_centre, h.status AS po_status, h.contract_id,
--   l.quantity, l.unit_price, l.quantity * l.unit_price AS line_amount
--
-- Check after: SELECT count(*) must equal the row count of silver.po_lines. More rows means the
-- supplier join matched two versions (overlapping windows); fewer means an inner join dropped rows.



-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fact_receipt
COMMENT 'One row per goods receipt'
AS
SELECT
  r.receipt_id,
  r.po_id,
  r.po_line_id,
  CAST(date_format(r.received_date, 'yyyyMMdd') AS INT) AS received_date_key,
  r.received_date,
  r.received_qty,
  l.po_line_id IS NULL AS is_orphan   -- data quality flag: no matching PO line in silver
FROM silver.goods_receipts AS r
LEFT JOIN silver.po_lines AS l ON l.po_line_id = r.po_line_id;

-- COMMAND ----------

-- TODO F2 [core]: gold.fact_invoice, one row per invoice. Same point-in-time pattern as F1,
-- but on invoice_date. Columns:
--   invoice_id, po_id, coalesce(supplier_sk, -1) AS supplier_sk, i.supplier_id,
--   invoice_date_key, invoice_date, due_date, paid_date, amount,
--   po_id IS NULL AS is_maverick



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Marts
-- MAGIC Window functions are the SQL practice here. Watch the grain at every join.

-- COMMAND ----------

-- TODO M1 [core]: gold.mart_spend_monthly, one row per (year_month, supplier, category).
--   From fact_po_line, excluding po_status = 'CANCELLED'.
--   Join gold.dim_supplier on supplier_sk (supplier_id, supplier_name),
--        gold.dim_category on category_key (category_name),
--        gold.dim_date on order_date_key (year_month).
--   Measures: spend = SUM(line_amount), po_count = COUNT(DISTINCT po_id), line_count = COUNT(*).
--   Window functions on top of the aggregate (use a CTE for the aggregate first):
--     cumulative_spend   running total per supplier and category, ordered by year_month
--     share_of_month_pct spend / SUM(spend) OVER (PARTITION BY year_month) * 100



-- COMMAND ----------

-- TODO M2 [core]: gold.mart_supplier_performance, one row per (supplier, year_month of expected delivery).
--   Step 1 (CTE): one row per PO line that has been received:
--     received_qty = SUM(r.received_qty), last_received_date = MAX(r.received_date)
--     from fact_receipt, GROUP BY po_line_id. Aggregate BEFORE joining, so a line with two
--     receipts is not counted twice.
--   Step 2: join to fact_po_line and flag each line:
--     is_on_time = last_received_date <= expected_delivery_date
--     is_in_full = received_qty >= quantity
--     is_otif    = both
--   Step 3: group by supplier and month (dim_supplier, dim_date on expected_delivery_date_key):
--     lines_received, on_time_pct, in_full_pct, otif_pct  (hint: avg(CAST(flag AS INT)) * 100)



-- COMMAND ----------

-- TODO M3 [core]: gold.mart_invoice_match, three-way match, one row per po_id.
--   Compare what was ordered, received and invoiced, each aggregated to po_id FIRST (fan-out!):
--     ordered  = SUM(line_amount) per po_id                                from fact_po_line
--     received = SUM(received_qty * unit_price) per po_id                  receipts joined to lines
--     invoiced = SUM(amount) per po_id WHERE po_id IS NOT NULL             from fact_invoice
--   Then FULL OUTER JOIN or LEFT JOIN the three on po_id and set match_status:
--     'NOT_RECEIVED'    invoiced but nothing received
--     'NOT_INVOICED'    received but no invoice yet
--     'MATCHED'         abs(invoiced - received) <= 1% of received
--     'OVER_INVOICED'   invoiced > received by more than 1%
--     'UNDER_INVOICED'  invoiced < received by more than 1%
--   Include variance = invoiced - received, and is_exception = match_status NOT IN ('MATCHED', 'NOT_INVOICED').



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optional, if time allows
-- MAGIC - `mart_maverick_spend`: per month, invoice spend with no PO and its share of all invoice spend.
-- MAGIC - `mart_payment_compliance`: per supplier, share of paid invoices with `paid_date > due_date`.
