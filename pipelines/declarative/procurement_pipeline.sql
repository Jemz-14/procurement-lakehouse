-- Lakeflow Spark Declarative Pipeline (formerly Delta Live Tables): suppliers and invoices, bronze to silver.
--
-- Pipeline settings: default catalog `procurement`, default schema `ldp`, serverless.
-- Unqualified names below resolve to procurement.ldp, so these tables never touch the hand-written ones.
--
-- Flow:
--   read_files (Auto Loader) -> bronze_* streaming tables
--   -> *_clean streaming tables with expectations
--   -> AUTO CDC -> silver_dim_supplier (SCD Type 2) and silver_invoices (SCD Type 1)
--   -> mart_maverick_monthly materialized view
--
-- No checkpoints, foreachBatch, MERGE or temp views: the pipeline manages all of that.


-- ---------------------------------------------------------------------------
-- Bronze: raw files as delivered (strings, as in the hand-written version)
-- ---------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_suppliers
COMMENT 'Raw supplier extracts, as delivered'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/procurement/landing/raw/suppliers/',
  format => 'csv',
  header => true,
  inferColumnTypes => false
);

CREATE OR REFRESH STREAMING TABLE bronze_invoices
COMMENT 'Raw invoice extracts, as delivered'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/procurement/landing/raw/invoices/',
  format => 'json',
  inferColumnTypes => false
);


-- ---------------------------------------------------------------------------
-- Suppliers: clean + expectations, then AUTO CDC as SCD Type 2
-- ---------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE suppliers_clean (
  -- TODO E1 [core]: expectations. The same five rules as supplier_rules() in silver_dim_supplier,
  -- each as:  CONSTRAINT <name> EXPECT (<condition that must be TRUE>) ON VIOLATION DROP ROW
  -- separated by commas. Careful: EXPECT states what a GOOD row looks like (the opposite of a reject rule).
  --   supplier_id is not null; name is not null; status is one of ACTIVE, INACTIVE, BLOCKED;
  --   payment_terms_days is not null and >= 0; updated_at is not null.
  CONSTRAINT has_supplier_id EXPECT (supplier_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IS NOT NULL AND status is IN ('ACTIVE', 'INACTIVE', 'BLOCKED')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payment_terms EXPECT (payment_terms_days IS NOT NULL OR payments_term days >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT has_updated_at EXPECT (updated_at IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Typed and standardised suppliers; rows failing an expectation are dropped and counted'
AS SELECT
  trim(supplier_id)                    AS supplier_id,
  trim(name)                           AS name,
  upper(trim(country))                 AS country,
  trim(category)                       AS category,
  try_cast(payment_terms_days AS INT)  AS payment_terms_days,
  upper(trim(status))                  AS status,
  try_cast(updated_at AS TIMESTAMP)    AS updated_at,
  _source_file,
  CAST(batch AS INT)                   AS _batch,
  _ingested_at
FROM STREAM(bronze_suppliers);


CREATE OR REFRESH STREAMING TABLE silver_dim_supplier
COMMENT 'Supplier dimension, SCD Type 2, maintained by AUTO CDC (__START_AT / __END_AT are the validity window)';

-- TODO C1 [core]: the AUTO CDC flow into silver_dim_supplier.
--   CREATE FLOW dim_supplier_cdc AS AUTO CDC INTO silver_dim_supplier
--   FROM STREAM(<the clean suppliers table>)
--   KEYS (<business key>)
--   SEQUENCE BY <the column that orders versions>
--   STORED AS SCD TYPE 2
--   TRACK HISTORY ON * EXCEPT (<the three technical columns>);
--   Why TRACK HISTORY: a re-sent copy differs only in _source_file / _ingested_at / _batch, and those
--   changes must not create a new version. This is the "attribute hash" gap in the hand-written version.

CREATE FLOW dim_supplier_cdc AS AUTO CDC INTO silver_dim_supplier
FROM STREAM(suppliers_clean)
  KEYS (supplier_id)
  SEQUENCE BY updated_at
  STORED AS SCD TYPE 2
  TRACK HISTORY ON * EXCEPT (_source_file, _batch, _ingested_at);



-- ---------------------------------------------------------------------------
-- Invoices: clean + expectations (all three actions), then AUTO CDC as SCD Type 1
-- ---------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE invoices_clean (
  -- TODO E2 [core]: expectations, showing all three actions:
  --   FAIL UPDATE: invoice_id is not null (a broken contract: stop rather than load nonsense)
  --   DROP ROW:    supplier_id is not null; invoice_date is not null; amount is not null;
  --                amount >= 0; due_date is not null; due_date >= invoice_date
  --   warn (no ON VIOLATION clause): po_id is not null
  --                (maverick invoices are valid data, we only want them counted)
  CONSTRAINT has_invoice_id     EXPECT (invoice_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_supplier_id    EXPECT (supplier_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_invoice_date EXPECT (invoice_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount       EXPECT (amount IS NOT NULL AND amount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_due_date     EXPECT (due_date IS NOT NULL AND due_date >= invoice_date) ON VIOLATION DROP ROW,
  CONSTRAINT has_po             EXPECT (po_id IS NOT NULL)



)
COMMENT 'Typed invoices; bad rows dropped and counted, maverick invoices kept and counted'
AS SELECT
  trim(invoice_id)                      AS invoice_id,
  trim(supplier_id)                     AS supplier_id,
  trim(po_id)                           AS po_id,
  try_cast(invoice_date AS DATE)        AS invoice_date,
  try_cast(amount AS DECIMAL(18,2))     AS amount,
  try_cast(due_date AS DATE)            AS due_date,
  try_cast(paid_date AS DATE)           AS paid_date,
  _source_file,
  CAST(batch AS INT)                    AS _batch,
  _ingested_at
FROM STREAM(bronze_invoices);


CREATE OR REFRESH STREAMING TABLE silver_invoices
COMMENT 'One row per invoice, deduplicated by AUTO CDC (SCD Type 1)';

-- TODO C2 [core]: the AUTO CDC flow into silver_invoices, SCD Type 1.
--   Same shape as C1, but: KEYS (invoice_id), SEQUENCE BY _ingested_at, STORED AS SCD TYPE 1,
--   and no TRACK HISTORY (type 1 keeps no history). This one statement replaces TODO 7 and 8
--   of silver_transactions (dedup + MERGE).
CREATE FLOW invoices_cdc AS AUTO CDC INTO silver_invoices
FROM STREAM(invoices_clean)
KEYS (invoice_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;


-- ---------------------------------------------------------------------------
-- Gold-style materialized view: maverick spend per month
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW mart_maverick_monthly
COMMENT 'Invoice spend per month, and the share with no PO (maverick spend)'
AS SELECT
  date_format(invoice_date, 'yyyy-MM')                          AS year_month,
  count(*)                                                      AS invoices,
  count_if(po_id IS NULL)                                       AS maverick_invoices,
  sum(amount)                                                   AS invoice_spend,
  sum(CASE WHEN po_id IS NULL THEN amount ELSE 0 END)           AS maverick_spend,
  round(sum(CASE WHEN po_id IS NULL THEN amount ELSE 0 END) / sum(amount) * 100, 2) AS maverick_share_pct
FROM silver_invoices
GROUP BY date_format(invoice_date, 'yyyy-MM');
