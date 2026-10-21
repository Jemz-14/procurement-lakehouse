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

CREATE OR REPLACE TABLE gold.fact_po_line
COMMENT 'One row per purchase order line, joined to the supplier version valid on the order date'
AS
SELECT
  l.po_line_id,
  l.po_id,
  coalesce(d.supplier_sk, -1)                                     AS supplier_sk,
  c.category_key,
  CAST(date_format(h.order_date, 'yyyyMMdd') AS INT)              AS order_date_key,
  CAST(date_format(l.expected_delivery_date, 'yyyyMMdd') AS INT)  AS expected_delivery_date_key,
  h.order_date,
  l.expected_delivery_date,
  h.buyer,
  h.cost_centre,
  h.status                                                        AS po_status,
  h.contract_id,
  l.quantity,
  l.unit_price,
  l.quantity * l.unit_price                                       AS line_amount
FROM silver.po_lines AS l
JOIN silver.purchase_orders AS h
  ON l.po_id = h.po_id
LEFT JOIN silver.dim_supplier AS d
  ON h.supplier_id = d.supplier_id
 AND CAST(h.order_date AS TIMESTAMP) >= d.valid_from
 AND (d.valid_to IS NULL OR CAST(h.order_date AS TIMESTAMP) < d.valid_to)
LEFT JOIN gold.dim_category AS c
  ON c.category_name = l.category;


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

CREATE OR REPLACE TABLE gold.fact_invoice
COMMENT 'One row per invoice'
AS
SELECT
invoice_id,
po_id,
coalesce(supplier_sk,-1) AS supplier_sk,
i.supplier_id,
CAST(date_format(invoice_date,'yyyyMMdd') AS INT) AS invoice_date_key,
invoice_date,
due_date,
paid_date,
amount

FROM silver.invoices i
LEFT JOIN silver.dim_supplier d 
ON i.supplier_id = d.supplier_id
AND CAST(i.invoice_date AS TIMESTAMP) >= d.valid_from
AND(d.valid_to IS NULL OR CAST(i.invoice_date AS TIMESTAMP) < d.valid_to)




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
CREATE OR REPLACE TABLE gold.mart_spend_monthly
COMMENT 'Spend by month, supplier and category (cancelled POs excluded)'
AS

WITH monthly AS (
    SELECT
        dt.year_month,
        s.supplier_id,
        s.supplier_name,
        c.category_name,
        SUM(f.line_amount) AS spend,
        COUNT(DISTINCT f.po_id) AS po_count,
        COUNT(*) AS line_count
    FROM gold.fact_po_line AS f
    JOIN gold.dim_supplier AS s
    ON f.supplier_sk = s.supplier_sk
    JOIN gold.dim_category AS c
    ON f.category_key = c.category_key
    JOIN gold.dim_date AS dt
    ON f.order_date_key = dt.date_key
    WHERE f.po_status <> 'CANCELLED'
    GROUP BY dt.year_month, s.supplier_id, s.supplier_name, c.category_name


)

SELECT *,
SUM(spend) OVER (PARTITION BY supplier_id,category_name ORDER BY year_month) AS cumulative_spend,
ROUND(spend/SUM(spend) OVER (PARTITION BY year_month)* 100,2) AS    share_of_month_pct
FROM monthly;





-- COMMAND ----------

select year_month,
ROUND(SUM(share_of_month_pct))
FROM gold.mart_spend_monthly
GROUP BY year_month
ORDER BY year_month;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.mart_supplier_performance
COMMENT 'Supplier delivery performance by month of expected delivery: on time, in full, OTIF'
AS
WITH received AS (
    SELECT
        r.po_line_id,
        SUM(r.received_qty)  AS received_qty,
        MAX(r.received_date) AS last_received_date
    FROM gold.fact_receipt AS r
    GROUP BY r.po_line_id
),
flagged AS (
    SELECT
        f.supplier_sk,
        f.expected_delivery_date_key,
        rc.last_received_date <= f.expected_delivery_date AS is_on_time,
        rc.received_qty >= f.quantity                     AS is_in_full,
        rc.last_received_date <= f.expected_delivery_date
          AND rc.received_qty >= f.quantity               AS is_otif
    FROM gold.fact_po_line AS f
    JOIN received AS rc ON rc.po_line_id = f.po_line_id
)
SELECT
    s.supplier_id,
    s.supplier_name,
    dt.year_month,
    COUNT(*)                                        AS lines_received,
    ROUND(AVG(CAST(fl.is_on_time AS INT)) * 100, 1) AS on_time_pct,
    ROUND(AVG(CAST(fl.is_in_full AS INT)) * 100, 1) AS in_full_pct,
    ROUND(AVG(CAST(fl.is_otif    AS INT)) * 100, 1) AS otif_pct
FROM flagged AS fl
JOIN gold.dim_supplier AS s  ON fl.supplier_sk = s.supplier_sk
JOIN gold.dim_date     AS dt ON fl.expected_delivery_date_key = dt.date_key
GROUP BY s.supplier_id, s.supplier_name, dt.year_month;

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


CREATE OR REPLACE TABLE gold.mart_invoice_match
AS
WITH ordered AS (
    SELECT
        po_id,
        SUM(line_amount) AS ordered_amount
    FROM gold.fact_po_line
    GROUP BY po_id
),
received AS (
    SELECT
        r.po_id,
        SUM(r.received_qty * f.unit_price) AS received_amount
    FROM gold.fact_receipt AS r
    JOIN gold.fact_po_line AS f
    ON r.po_line_id = f.po_line_id
    GROUP BY r.po_id
),

invoiced AS (
    SELECT
        po_id,
        SUM(amount) AS invoiced_amount
    FROM gold.fact_invoice
    WHERE po_id IS NOT NULL
    GROUP BY po_id
)
SELECT
    o.po_id,
    o.ordered_amount,
    coalesce(r.received_amount, 0)                                  AS received_amount,
    coalesce(i.invoiced_amount, 0)                                  AS invoiced_amount,
    coalesce(i.invoiced_amount, 0) - coalesce(r.received_amount, 0) AS variance,
    CASE
        WHEN r.po_id IS NULL THEN 'NOT_RECEIVED'
        WHEN i.po_id IS NULL THEN 'NOT_INVOICED'
        WHEN abs(i.invoiced_amount - r.received_amount) <= 0.01 * r.received_amount THEN 'MATCHED'
        WHEN i.invoiced_amount > r.received_amount THEN 'OVER_INVOICED'
        ELSE 'UNDER_INVOICED'
    END                                                             AS match_status
FROM ordered AS o
LEFT JOIN received AS r ON r.po_id = o.po_id
LEFT JOIN invoiced AS i ON i.po_id = o.po_id
WHERE r.po_id IS NOT NULL OR i.po_id IS NOT NULL;

-- COMMAND ----------

SELECT * FROM gold.mart_invoice_match 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optional, if time allows
-- MAGIC - `mart_maverick_spend`: per month, invoice spend with no PO and its share of all invoice spend.
-- MAGIC - `mart_payment_compliance`: per supplier, share of paid invoices with `paid_date > due_date`.