# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver: purchase orders, PO lines, goods receipts, invoices
# MAGIC
# MAGIC For each entity: stream new rows from bronze, then in `foreachBatch`:
# MAGIC 1. type and standardise (`try_cast`, `upper(trim(...))`)
# MAGIC 2. validate: every failed rule is listed in `reject_reason`
# MAGIC 3. rejected rows go to `ops.quarantine`
# MAGIC 4. valid rows are deduplicated to one row per business key
# MAGIC 5. MERGE into the silver table
# MAGIC
# MAGIC `foreachBatch` is at-least-once: after a failure a micro-batch can be replayed. Every
# MAGIC write below is therefore a MERGE on a key, so replaying a batch changes nothing.
# MAGIC
# MAGIC Run `00_create_silver_tables` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared helpers
# MAGIC Everything a `foreachBatch` function uses is defined in this notebook, because the
# MAGIC function is serialised and run by Spark. Inside it, use `batch_df.sparkSession`, not `spark`.

# COMMAND ----------

# MAGIC %run ./silver_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purchase orders and PO lines

# COMMAND ----------

PO_FIELDS = ["po_id", "supplier_id", "buyer", "cost_centre", "order_date", "status", "updated_at", "lines"]
PO_STATUSES = ["APPROVED", "PARTIALLY_RECEIVED", "RECEIVED", "CLOSED", "CANCELLED"]
LINES_SCHEMA = (
    "array<struct<po_line_id: string, item: string, category: string, "
    "quantity: string, unit_price: string, expected_delivery_date: string>>"
)


def clean_purchase_orders(bronze: DataFrame) -> DataFrame:
    # Columns the source adds later (contract_id from batch 5) pass through unchanged,
    # so MERGE WITH SCHEMA EVOLUTION can add them to silver. Old batches simply lack them.
    new_columns = [c for c in bronze.columns if c not in PO_FIELDS + TECHNICAL_COLUMNS + ["_rescued_data"]]
    return bronze.select(
        F.to_json(F.struct(*bronze.columns)).alias("_raw"),
        F.trim("po_id").alias("po_id"),
        F.trim("supplier_id").alias("supplier_id"),
        F.trim("buyer").alias("buyer"),
        standardise("cost_centre").alias("cost_centre"),
        try_cast("order_date", "DATE").alias("order_date"),
        standardise("status").alias("status"),
        try_cast("updated_at", "TIMESTAMP").alias("updated_at"),
        F.from_json("lines", LINES_SCHEMA).alias("lines"),
        *new_columns,
        *TECHNICAL_COLUMNS,
    )


def po_rules() -> list:
    return [
        ("missing po_id", F.col("po_id").isNull()),
        ("missing supplier_id", F.col("supplier_id").isNull()),
        ("invalid order_date", F.col("order_date").isNull()),
        ("invalid updated_at", F.col("updated_at").isNull()),
        ("invalid status", F.col("status").isNull() | ~F.col("status").isin(PO_STATUSES)),
        ("no lines", F.expr("lines IS NULL OR size(lines) = 0")),
        ("invalid line quantity", F.expr("exists(lines, l -> coalesce(try_cast(l.quantity AS INT), 0) <= 0)")),
        ("invalid line unit_price", F.expr(
            "exists(lines, l -> coalesce(try_cast(l.unit_price AS DECIMAL(18,2)), -1) < 0)")),
    ]


def process_purchase_orders(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    session = batch_df.sparkSession
    split_valid(clean_purchase_orders(batch_df), po_rules(), "purchase_orders", "po_id", "po_valid")

    # TODO 1: deduplicate po_valid to ONE row per po_id: the latest version.
    #   ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) and QUALIFY.
    #   Order by updated_at, newest first. Break ties with _ingested_at, then _source_file,
    #   so the result is the same every time.
    #   Do not output helper columns such as a row number: MERGE WITH SCHEMA EVOLUTION
    #   would add them to the silver table.
    session.sql("""
        -- TODO 1
        SELECT *
        FROM po_valid
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY po_id
            ORDER BY updated_at DESC, _ingested_at DESC, _source_file
        ) = 1
    """).createOrReplaceTempView("po_latest")

    # TODO 2: MERGE the PO headers into silver.purchase_orders.
    #   - MERGE WITH SCHEMA EVOLUTION INTO {catalog}.silver.purchase_orders AS t
    #   - source: po_latest without the lines column (hint: SELECT * EXCEPT (lines))
    #   - match on the business key
    #   - update only when the source version is NEWER than the target (protects against late, older copies)
    #   - insert when the PO is new
    #   - UPDATE SET * and INSERT * so a new column like contract_id flows through
    session.sql(f"""
        -- TODO 2
        MERGE WITH SCHEMA EVOLUTION INTO {catalog}.silver.purchase_orders AS t
        USING(
            SELECT * EXCEPT (lines)
            FROM po_latest
        ) AS s
        ON t.po_id = s.po_id
        WHEN MATCHED AND s.updated_at > t.updated_at
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
 """)

    # TODO 3: one row per PO line from po_latest.
    #   explode(lines) AS line, then select:
    #   line.po_line_id, po_id, line.item, line.category,
    #   line.quantity as INT, line.unit_price as DECIMAL(18,2), line.expected_delivery_date as DATE
    #   (try_cast is fine; the rules already rejected bad values),
    #   updated_at AS po_updated_at, plus _source_file, _batch, _ingested_at.
    session.sql("""
        -- TODO 3
        WITH ExplodePO AS(
            SELECT *,
            explode(lines) AS line
            FROM po_latest
        )
        SELECT
            line.po_line_id,
            po_id,
            line.item,
            line.category,
            try_cast(line.quantity AS INT) AS quantity,
            try_cast(line.unit_price AS DECIMAL(18,2)) AS unit_price,
            try_cast(line.expected_delivery_date AS DATE) AS expected_delivery_date,
            updated_at AS po_updated_at,
            _source_file,
            _batch,
            _ingested_at
        FROM ExplodePO
        
    """).createOrReplaceTempView("po_lines_latest")

    # TODO 4: MERGE po_lines_latest into silver.po_lines.
    #   Same pattern as TODO 2: key po_line_id, update only when po_updated_at is newer.
    session.sql(f"""
        -- TODO 4
        MERGE WITH SCHEMA EVOLUTION INTO {catalog}.silver.po_lines AS t
        USING (
            SELECT *
            FROM po_lines_latest
        ) AS s
        ON t.po_line_id = s.po_line_id
        WHEN MATCHED AND s.po_updated_at > t.po_updated_at
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Goods receipts

# COMMAND ----------


def clean_goods_receipts(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.to_json(F.struct(*bronze.columns)).alias("_raw"),
        F.trim("receipt_id").alias("receipt_id"),
        F.trim("po_id").alias("po_id"),
        F.trim("po_line_id").alias("po_line_id"),
        try_cast("received_qty", "INT").alias("received_qty"),
        try_cast("received_date", "DATE").alias("received_date"),
        *TECHNICAL_COLUMNS,
    )


def receipt_rules() -> list:
    return [
        ("missing receipt_id", F.col("receipt_id").isNull()),
        ("missing po_id", F.col("po_id").isNull()),
        ("missing po_line_id", F.col("po_line_id").isNull()),
        ("invalid received_qty", F.col("received_qty").isNull() | (F.col("received_qty") <= 0)),
        ("invalid received_date", F.col("received_date").isNull()),
    ]


def process_goods_receipts(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    session = batch_df.sparkSession
    split_valid(clean_goods_receipts(batch_df), receipt_rules(), "goods_receipts", "receipt_id", "receipts_valid")

    # TODO 5: deduplicate receipts_valid to one row per receipt_id.
    #   Receipts have no updated_at; duplicates are exact copies. Keep the earliest ingested copy
    #   (so _batch shows when the receipt first arrived), ties broken by _source_file.
    session.sql("""
        -- TODO 5
        SELECT *
        FROM receipts_valid
        QUALIFY ROW_NUMBER() OVER (PARTITION BY receipt_id 
        ORDER BY _ingested_at, _source_file) = 1
    """).createOrReplaceTempView("receipts_latest")

    # TODO 6: insert-only MERGE into silver.goods_receipts.
    #   Receipts never change once sent, so there is no WHEN MATCHED clause.
    session.sql(f"""
        -- TODO 6
        MERGE WITH SCHEMA EVOLUTION INTO {catalog}.silver.goods_receipts AS t
        USING (
            SELECT *
            FROM receipts_latest
        ) AS s
        ON t.receipt_id = s.receipt_id
        WHEN NOT MATCHED
            THEN INSERT *
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Invoices

# COMMAND ----------


def clean_invoices(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.to_json(F.struct(*bronze.columns)).alias("_raw"),
        F.trim("invoice_id").alias("invoice_id"),
        F.trim("supplier_id").alias("supplier_id"),
        F.trim("po_id").alias("po_id"),
        try_cast("invoice_date", "DATE").alias("invoice_date"),
        try_cast("amount", "DECIMAL(18,2)").alias("amount"),
        try_cast("due_date", "DATE").alias("due_date"),
        try_cast("paid_date", "DATE").alias("paid_date"),
        *TECHNICAL_COLUMNS,
    )


def invoice_rules() -> list:
    return [
        ("missing invoice_id", F.col("invoice_id").isNull()),
        ("missing supplier_id", F.col("supplier_id").isNull()),
        ("invalid invoice_date", F.col("invoice_date").isNull()),
        ("invalid amount", F.col("amount").isNull()),  # includes text like '1,234.50'
        ("negative amount", F.col("amount") < 0),
        ("invalid due_date", F.col("due_date").isNull()),
        ("due_date before invoice_date", F.col("due_date") < F.col("invoice_date")),
    ]


def process_invoices(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    session = batch_df.sparkSession
    split_valid(clean_invoices(batch_df), invoice_rules(), "invoices", "invoice_id", "invoices_valid")

    # TODO 7: same as TODO 5, for invoice_id.
    session.sql("""
        -- TODO 7
        SELECT *
        FROM invoices_valid
        QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id
        ORDER BY _ingested_at, _source_file) = 1
    """).createOrReplaceTempView("invoices_latest")

    # TODO 8: same as TODO 6, into silver.invoices.
    session.sql(f"""
        -- TODO 8
        MERGE WITH SCHEMA EVOLUTION INTO {catalog}.silver.invoices AS t
        USING (
            SELECT *
            FROM invoices_latest
        ) AS s
        ON t.invoice_id = s.invoice_id
        WHEN NOT MATCHED
            THEN INSERT *
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

run_incremental(f"{catalog}.bronze.purchase_orders", "purchase_orders", process_purchase_orders)
run_incremental(f"{catalog}.bronze.goods_receipts", "goods_receipts", process_goods_receipts)
run_incremental(f"{catalog}.bronze.invoices", "invoices", process_invoices)

display(spark.sql(f"""
    SELECT 'purchase_orders' AS table_name, count(*) AS row_count FROM {catalog}.silver.purchase_orders
    UNION ALL SELECT 'po_lines',       count(*) FROM {catalog}.silver.po_lines
    UNION ALL SELECT 'goods_receipts', count(*) FROM {catalog}.silver.goods_receipts
    UNION ALL SELECT 'invoices',       count(*) FROM {catalog}.silver.invoices
    UNION ALL SELECT 'quarantine',     count(*) FROM {catalog}.ops.quarantine
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idempotency test: forget progress, keep the tables
# MAGIC Deletes only the silver checkpoints, so the next run reprocesses every bronze row.
# MAGIC Because every write is a MERGE on a key, the row counts must not change.
# MAGIC (Compare with bronze, where the same reset would duplicate every row.)

# COMMAND ----------

# for name in ["purchase_orders", "goods_receipts", "invoices"]:
#   dbutils.fs.rm(f"{checkpoint_root}/{name}", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full reset (development only): empty the tables and the checkpoints

# COMMAND ----------

# for table in ["purchase_orders", "po_lines", "goods_receipts", "invoices"]:
#     spark.sql(f"TRUNCATE TABLE {catalog}.silver.{table}")
# spark.sql(f"DELETE FROM {catalog}.ops.quarantine "
#           "WHERE entity IN ('purchase_orders', 'goods_receipts', 'invoices')")
# for name in ["purchase_orders", "goods_receipts", "invoices"]:
#     dbutils.fs.rm(f"{checkpoint_root}/{name}", True)