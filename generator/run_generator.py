# Databricks notebook source
# MAGIC %md
# MAGIC # Run the procurement data generator
# MAGIC
# MAGIC Writes one batch of extracts to `/Volumes/<catalog>/landing/raw/<entity>/batch=<n>/`.
# MAGIC
# MAGIC - Batches must be generated in order (1, 2, 3, ...). Each batch continues from the
# MAGIC   state the previous one saved in `/Volumes/<catalog>/landing/raw/_generator_state/`.
# MAGIC - Rerunning a batch with the same parameters rewrites identical files.
# MAGIC - The widgets become job parameters in phase 6.

# COMMAND ----------

dbutils.widgets.text("catalog", "procurement")
dbutils.widgets.text("batch_number", "1")
dbutils.widgets.text("start_date", "2026-01-01")
dbutils.widgets.text("rows_per_entity", "200")
dbutils.widgets.text("error_rate", "0.05")
dbutils.widgets.text("seed", "42")

# COMMAND ----------

import importlib
import json


import procurement_generator

# Pick up code changes after a Git pull without restarting Python.
importlib.reload(procurement_generator)

catalog = dbutils.widgets.get("catalog")
params = {
    "start_date": dbutils.widgets.get("start_date"),
    "rows_per_entity": int(dbutils.widgets.get("rows_per_entity")),
    "error_rate": float(dbutils.widgets.get("error_rate")),
    "seed": int(dbutils.widgets.get("seed")),
    "base_path": f"/Volumes/{catalog}/landing/raw",
}

# COMMAND ----------

summary = procurement_generator.generate_batch(int(dbutils.widgets.get("batch_number")), **params)
print(json.dumps(summary, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### First-time only: generate batches 1 to 3 in one go
# MAGIC Uncomment and run this instead of the cell above.

# COMMAND ----------

# for batch_number in [1, 2, 3]:
#     summary = procurement_generator.generate_batch(batch_number, **params)
#     print(batch_number, {e: summary[e]["rows_written"] for e in procurement_generator.ENTITIES})
