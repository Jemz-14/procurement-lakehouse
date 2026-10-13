-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver and quarantine tables
-- MAGIC
-- MAGIC Created up front so the schema is an explicit contract: types, keys, comments and
-- MAGIC CHECK constraints. MERGE needs the target to exist. Safe to rerun.
-- MAGIC
-- MAGIC `dim_supplier` (SCD Type 2) is added later in phase 3.

-- COMMAND ----------

USE CATALOG procurement;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.purchase_orders (
  po_id         STRING    NOT NULL COMMENT 'Business key',
  supplier_id   STRING,
  buyer         STRING,
  cost_centre   STRING    COMMENT 'Standardised to upper case, e.g. CC-200',
  order_date    DATE,
  status        STRING    COMMENT 'APPROVED, PARTIALLY_RECEIVED, RECEIVED, CLOSED or CANCELLED',
  updated_at    TIMESTAMP COMMENT 'Source change time; the newest valid version wins',
  _source_file  STRING,
  _batch        INT,
  _ingested_at  TIMESTAMP
)
COMMENT 'One row per purchase order: the latest valid version';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.po_lines (
  po_line_id              STRING        NOT NULL COMMENT 'Business key, <po_id>-<n>',
  po_id                   STRING        NOT NULL,
  item                    STRING,
  category                STRING,
  quantity                INT,
  unit_price              DECIMAL(18,2) COMMENT 'EUR',
  expected_delivery_date  DATE,
  po_updated_at           TIMESTAMP     COMMENT 'updated_at of the PO version these lines came from',
  _source_file            STRING,
  _batch                  INT,
  _ingested_at            TIMESTAMP,
  CONSTRAINT positive_quantity CHECK (quantity > 0),
  CONSTRAINT non_negative_price CHECK (unit_price >= 0)
)
COMMENT 'One row per purchase order line, from the latest valid PO version';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.goods_receipts (
  receipt_id     STRING NOT NULL COMMENT 'Business key',
  po_id          STRING NOT NULL,
  po_line_id     STRING NOT NULL,
  received_qty   INT,
  received_date  DATE,
  _source_file   STRING,
  _batch         INT,
  _ingested_at   TIMESTAMP,
  CONSTRAINT positive_received_qty CHECK (received_qty > 0)
)
COMMENT 'One row per goods receipt';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.invoices (
  invoice_id    STRING        NOT NULL COMMENT 'Business key',
  supplier_id   STRING        NOT NULL,
  po_id         STRING        COMMENT 'NULL means a non-PO (maverick) invoice',
  invoice_date  DATE,
  amount        DECIMAL(18,2) COMMENT 'EUR',
  due_date      DATE,
  paid_date     DATE          COMMENT 'NULL means unpaid',
  _source_file  STRING,
  _batch        INT,
  _ingested_at  TIMESTAMP,
  CONSTRAINT non_negative_amount CHECK (amount >= 0)
)
COMMENT 'One row per supplier invoice';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ops.quarantine (
  quarantine_id   STRING NOT NULL COMMENT 'sha2 of entity, raw record and source file; makes inserts idempotent',
  entity          STRING NOT NULL,
  business_key    STRING,
  reject_reason   STRING NOT NULL COMMENT 'Every rule the row failed, separated by "; "',
  raw_record      STRING COMMENT 'The original bronze row as JSON',
  _source_file    STRING,
  _batch          INT,
  quarantined_at  TIMESTAMP
)
COMMENT 'Rows rejected by silver validation, with the reason';

-- COMMAND ----------

SHOW TABLES IN silver;
