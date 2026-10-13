# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze: ingest raw files with Auto Loader
# MAGIC
# MAGIC One append-only Delta table per entity in `<catalog>.bronze`, loaded incrementally
# MAGIC from `/Volumes/<catalog>/landing/raw/<entity>/`.
# MAGIC
# MAGIC - Columns stay as strings, exactly as delivered (Auto Loader's default for CSV and JSON).
# MAGIC   Typing and validation happen in silver.
# MAGIC - Metadata columns: `_ingested_at`, `_source_file`, `_batch`, plus Auto Loader's `_rescued_data`.
# MAGIC - The checkpoint for each entity lives in `/Volumes/<catalog>/ops/checkpoints/bronze/<entity>`
# MAGIC   and records which files have already been processed. The inferred schema is stored there too.
# MAGIC - `availableNow` processes every new file, then stops. Rerunning with no new files loads 0 rows.

# COMMAND ----------

dbutils.widgets.text("catalog", "procurement")
catalog = dbutils.widgets.get("catalog")

landing_root = f"/Volumes/{catalog}/landing/raw"
checkpoint_root = f"/Volumes/{catalog}/ops/checkpoints/bronze"

# entity -> (file format, extra reader options)
ENTITIES = {
    "suppliers": ("csv", {"header": "true"}),
    "purchase_orders": ("json", {}),
    "goods_receipts": ("csv", {"header": "true"}),
    "invoices": ("json", {}),
}

# COMMAND ----------

from pyspark.sql import functions as F


def row_count(table: str) -> int:
    return spark.table(table).count() if spark.catalog.tableExists(table) else 0

def ingest_entity(entity: str, file_format: str, reader_options: dict) -> int:
    
    """Load new files for one entity into its bronze table. Returns the number of rows loaded."""
    source_path = f"{landing_root}/{entity}/"
    checkpoint_path = f"{checkpoint_root}/{entity}"
    target_table = f"{catalog}.bronze.{entity}"
    rows_before = row_count(target_table)

    # TODO 1: streaming read with Auto Loader.
    #   spark.readStream.format("cloudFiles")
    #   .option("cloudFiles.format", ...)
    #   .option("cloudFiles.schemaLocation", ...)   use the checkpoint path
    #   .options(**reader_options)                  header=true for CSV
    #   .load(...)
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation",checkpoint_path)
        .options(**reader_options)
        .load(source_path)
    )

    # TODO 2: add the metadata columns.
    #   _ingested_at  F.current_timestamp()
    #   _source_file  the file path from the hidden _metadata column
    #   _batch        the <n> from "batch=<n>" in the file path (F.regexp_extract), cast to int
    #   Then drop the "batch" column Auto Loader infers from the folder names, so there is only one.
    bronze = (
        raw
        .withColumn("_ingested_at",F.current_timestamp())
        .withColumn("_source_file",F.col("_metadata.file_path"))
        .withColumn("_batch", F.regexp_extract(F.col("_source_file"), r"batch=(\d+)", 1).cast("int"))        
        .drop("batch")
    )




    # TODO 3: write the stream to the bronze table.
    #   bronze.writeStream
    #   .option("checkpointLocation", ...)
    #   .option("mergeSchema", "true")     lets the table gain new columns (batch 5)
    #   .trigger(availableNow=True)
    #   .toTable(...)
    query = (
        bronze.writeStream
        .option("checkpointLocation",checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()
    return row_count(target_table) - rows_before

# COMMAND ----------

for entity, (file_format, reader_options) in ENTITIES.items():
    rows = ingest_entity(entity, file_format, reader_options)
    print(f"{entity:<16} {rows:>6} new rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset bronze (development only)
# MAGIC The table and its checkpoint must be reset together:
# MAGIC - drop only the checkpoint, and every file is reprocessed, duplicating rows in the table;
# MAGIC - drop only the table, and the checkpoint still thinks every file is done, so the table stays empty.

# COMMAND ----------

# for entity in ENTITIES:
#     spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.{entity}")
#     dbutils.fs.rm(f"{checkpoint_root}/{entity}", True)