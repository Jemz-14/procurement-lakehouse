# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver: dim_supplier as SCD Type 2 (hand-written MERGE)
# MAGIC
# MAGIC SCD Type 1 overwrites a changed supplier, so history is lost. SCD Type 2 keeps every
# MAGIC version as its own row with a validity window, so a PO from March can still be joined
# MAGIC to the payment terms that applied in March.
# MAGIC
# MAGIC | supplier_sk | supplier_id | payment_terms_days | valid_from | valid_to | is_current |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 7 | SUP-0009 | 30 | 2025-06-02 | 2026-02-14 | false |
# MAGIC | 52 | SUP-0009 | 60 | 2026-02-14 | NULL | true |
# MAGIC
# MAGIC When a new version arrives, two things must happen in ONE atomic MERGE:
# MAGIC 1. **close** the current row: set `valid_to` and `is_current = false`
# MAGIC 2. **insert** the new version as the current row
# MAGIC
# MAGIC A single MERGE can only do one action per matched target row, and the new version must be
# MAGIC inserted, not matched. The standard trick is a staged source with each change in it twice:
# MAGIC - a **close row** with `merge_key = supplier_id`: it matches the current row and closes it
# MAGIC - an **insert row** with `merge_key = NULL`: NULL never equals anything, so it never matches
# MAGIC   and falls through to `WHEN NOT MATCHED`, where it is inserted
# MAGIC
# MAGIC One batch can contain several new versions of the same supplier (for example the first run
# MAGIC sees batches 1 to 4 at once). So we close the current row at the **earliest** new version and
# MAGIC insert **every** new version, each with `valid_to` = the next version's `updated_at`.
# MAGIC
# MAGIC Idempotency: only versions newer than the current row are processed. A replayed batch finds
# MAGIC nothing newer, so the MERGE changes nothing.
# MAGIC
# MAGIC **Before running:** TODO B0 (the table) and TODO B1 (shared helpers) below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO B0 [boilerplate]: create the table
# MAGIC Add a cell to `00_create_silver_tables` (before the constraints cell) creating
# MAGIC `silver.dim_supplier` with these columns, and run it:
# MAGIC - `supplier_sk BIGINT GENERATED ALWAYS AS IDENTITY` (surrogate key: Delta assigns it, you never insert it)
# MAGIC - `supplier_id` STRING NOT NULL, `name`, `country`, `category` STRING, `payment_terms_days` INT, `status` STRING
# MAGIC - `valid_from` TIMESTAMP NOT NULL, `valid_to` TIMESTAMP (NULL = still valid), `is_current` BOOLEAN NOT NULL
# MAGIC - `_source_file` STRING, `_batch` INT, `_ingested_at` TIMESTAMP
# MAGIC - a table COMMENT, and a COMMENT on `valid_to`
# MAGIC
# MAGIC ### TODO B1 [boilerplate]: share the helpers instead of copying them
# MAGIC 1. In Databricks, create a notebook `pipelines/silver/silver_helpers`.
# MAGIC 2. Move the widgets cell and the "Shared helpers" code cell from `silver_transactions` into it.
# MAGIC 3. In `silver_transactions`, replace those two cells with one cell containing `%run ./silver_helpers`.
# MAGIC 4. Run `silver_transactions` once with no new data to check it still works (0 changes).
# MAGIC
# MAGIC `%run` executes the other notebook in this notebook's session, so `catalog`, `checkpoint_root`,
# MAGIC `split_valid`, `run_incremental` and the rest exist here afterwards.

# COMMAND ----------

# MAGIC %run ./silver_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean and validate

# COMMAND ----------

SUPPLIER_STATUSES = ["ACTIVE", "INACTIVE", "BLOCKED"]


def clean_suppliers(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.to_json(F.struct(*bronze.columns)).alias("_raw"),
        F.trim("supplier_id").alias("supplier_id"),
        F.trim("name").alias("name"),
        standardise("country").alias("country"),
        F.trim("category").alias("category"),
        try_cast("payment_terms_days", "INT").alias("payment_terms_days"),
        standardise("status").alias("status"),
        try_cast("updated_at", "TIMESTAMP").alias("updated_at"),
        *TECHNICAL_COLUMNS,
    )


def supplier_rules() -> list:

       return [
        ("missing supplier_id", F.col("supplier_id").isNull()),
        ("missing name", F.col("name").isNull()),
        ("invalid status", F.col("status").isNull() | ~F.col("status").isin(SUPPLIER_STATUSES)),
        ("invalid payment_terms_days",
         F.col("payment_terms_days").isNull() | (F.col("payment_terms_days") < 0)),
        ("invalid updated_at", F.col("updated_at").isNull()),
    ]
        

    
    # TODO B3 [boilerplate]: same pattern as receipt_rules. Reject when:
    #   supplier_id is null; name is null; status is null or not in SUPPLIER_STATUSES;
    #   payment_terms_days is null or negative; updated_at is null.
    


# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 MERGE

# COMMAND ----------


def process_suppliers(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    session = batch_df.sparkSession
    split_valid(clean_suppliers(batch_df), supplier_rules(), "suppliers", "supplier_id", "suppliers_valid")

    # TODO C1 [core]: one row per (supplier_id, updated_at).
    #   Exact copies of the same version (cross-batch duplicates) collapse to one.
    #   QUALIFY ROW_NUMBER() ... = 1, keeping the earliest ingested copy.
    session.sql("""
        -- TODO C1
        SELECT *
        FROM suppliers_valid
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY supplier_id, updated_at
            ORDER BY _ingested_at, _source_file
        ) = 1
        
    """).createOrReplaceTempView("supplier_versions")

    # TODO C2 [core]: keep only versions NEWER than the current dim row, with their validity window.
    #   FROM supplier_versions AS v
    #   LEFT JOIN {catalog}.silver.dim_supplier AS d ON d.supplier_id = v.supplier_id AND d.is_current
    #   WHERE d.supplier_id IS NULL            (a brand new supplier)
    #      OR v.updated_at > d.valid_from      (newer than what we have)
    #   Select v.* plus:
    #     valid_from = v.updated_at
    #     valid_to   = LEAD(v.updated_at) OVER (PARTITION BY v.supplier_id ORDER BY v.updated_at)
    #     is_current = (that LEAD is NULL)
    #   Hint: compute valid_to in a CTE first, then is_current = valid_to IS NULL in the outer SELECT.
    #   Drop updated_at from the output (valid_from replaces it).
    session.sql(f"""
        -- TODO C2
   WITH newer AS (                       
    SELECT v.*
    FROM supplier_versions AS v
    LEFT JOIN {catalog}.silver.dim_supplier AS d
        ON d.supplier_id = v.supplier_id
       AND d.is_current
    WHERE d.supplier_id IS NULL
       OR v.updated_at > d.valid_from
),
windowed AS (                          
    SELECT *,
           updated_at AS valid_from,
           LEAD(updated_at) OVER (PARTITION BY supplier_id ORDER BY updated_at) AS valid_to
    FROM newer
)
SELECT * EXCEPT (updated_at),         
       valid_to IS NULL AS is_current
FROM windowed

    """).createOrReplaceTempView("new_versions")

    # TODO C3 [core]: the SCD Type 2 MERGE.
    #   Source (staged), as a CTE or subquery:
    #     close rows:  the EARLIEST new version per supplier, with supplier_id AS merge_key
    #                  (QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY valid_from) = 1)
    #     UNION ALL
    #     insert rows: every new version, with NULL AS merge_key
    #   MERGE INTO {catalog}.silver.dim_supplier AS t
    #   USING (...) AS s
    #   ON t.supplier_id = s.merge_key AND t.is_current
    #   WHEN MATCHED THEN UPDATE SET valid_to = s.valid_from, is_current = false
    #   WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT (<every column except supplier_sk>)
    #                                                VALUES (<the same columns from s>)
    #   Why the AND s.merge_key IS NULL: a brand new supplier's close row matches nothing,
    #   and without this guard it would be inserted twice.
    #   No INSERT * here: the source has merge_key, and supplier_sk is generated by Delta.
    session.sql(f"""
        -- TODO C3
    MERGE INTO {catalog}.silver.dim_supplier AS t
    USING (
    (SELECT supplier_id AS merge_key, *
     FROM new_versions
     QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY valid_from) = 1)
    UNION ALL
    SELECT NULL AS merge_key, *
    FROM new_versions
    ) AS s
    ON t.supplier_id = s.merge_key AND t.is_current
    WHEN MATCHED THEN
        UPDATE SET valid_to = s.valid_from, is_current = false
    WHEN NOT MATCHED AND s.merge_key IS NULL THEN
        INSERT (supplier_id, name, country, category, payment_terms_days, status,
                valid_from, valid_to, is_current, _source_file, _batch, _ingested_at)
        VALUES (s.supplier_id, s.name, s.country, s.category, s.payment_terms_days, s.status,
                s.valid_from, s.valid_to, s.is_current, s._source_file, s._batch, s._ingested_at)
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

run_incremental(f"{catalog}.bronze.suppliers", "dim_supplier", process_suppliers)

display(spark.sql(f"""
    SELECT count(*) AS versions,
           count_if(is_current) AS current_rows,
           count(DISTINCT supplier_id) AS suppliers
    FROM {catalog}.silver.dim_supplier
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM procurement.silver.dim_supplier
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks (run these after the first load)
# MAGIC - `current_rows` must equal `suppliers`: exactly one current row per supplier.
# MAGIC - No overlapping windows: for each supplier, each version's `valid_to` equals the next version's `valid_from`.
# MAGIC - Idempotency: delete the checkpoint (cell below), rerun, and the three numbers above must not change.

# COMMAND ----------

#dbutils.fs.rm(f"{checkpoint_root}/dim_supplier", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full reset (development only)

# COMMAND ----------

# spark.sql(f"TRUNCATE TABLE {catalog}.silver.dim_supplier")
# spark.sql(f"DELETE FROM {catalog}.ops.quarantine WHERE entity = 'suppliers'")
# dbutils.fs.rm(f"{checkpoint_root}/dim_supplier", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from procurement.silver.dim_supplier