-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 Setup: catalog, schemas and volumes
-- MAGIC
-- MAGIC Creates the Unity Catalog objects for the procurement lakehouse.
-- MAGIC Every statement uses `IF NOT EXISTS`, so the notebook is safe to rerun.
-- MAGIC
-- MAGIC | Object | Purpose |
-- MAGIC |---|---|
-- MAGIC | `procurement` | catalog for the whole project |
-- MAGIC | `landing.raw` | volume where the generator drops source files |
-- MAGIC | `bronze` | raw data as delivered, append only |
-- MAGIC | `silver` | cleaned, typed, deduplicated |
-- MAGIC | `gold` | star schema and marts |
-- MAGIC | `ops` | quarantine, run logs, and the `checkpoints` volume for Auto Loader |
-- MAGIC
-- MAGIC **Fallback:** if `CREATE CATALOG` fails on Free Edition, skip the next cell and change
-- MAGIC `USE CATALOG procurement` to `USE CATALOG workspace` (the default catalog).

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS procurement
COMMENT 'Procurement lakehouse: synthetic P2P data (suppliers, POs, receipts, invoices)';

-- COMMAND ----------

USE CATALOG procurement;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Incoming source files, simulating daily extracts from a procurement system';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Raw data as delivered, append only, plus ingestion metadata';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Typed, cleaned, deduplicated and standardised data; dim_supplier is SCD Type 2';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Star schema and procurement marts for BI';
CREATE SCHEMA IF NOT EXISTS ops     COMMENT 'Operational data: quarantine, run logs, streaming checkpoints';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS landing.raw     COMMENT 'Raw files: /Volumes/<catalog>/landing/raw/<entity>/batch=<n>/';
CREATE VOLUME IF NOT EXISTS ops.checkpoints COMMENT 'Auto Loader checkpoints and schema locations';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

SHOW VOLUMES IN landing;

-- COMMAND ----------

SHOW VOLUMES IN ops;

-- COMMAND ----------

-- Where does Unity Catalog store the managed data for this catalog?
DESCRIBE CATALOG EXTENDED procurement;