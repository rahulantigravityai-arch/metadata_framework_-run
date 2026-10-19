# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 – Auto Loader: files -> Bronze (one table)
# MAGIC The logic is the function `load_files_to_bronze` in `00_pipeline_lib`. Read it there, run it here.
# MAGIC Steps inside it: read config row -> log STARTED -> Auto Loader (checkpoint remembers files) -> append to Bronze -> log SUCCESS/FAILED.

# COMMAND ----------

# MAGIC %run ./00_pipeline_lib

# COMMAND ----------

import uuid

dbutils.widgets.text("table_id", "1")
dbutils.widgets.text("run_id", "")
table_id = int(dbutils.widgets.get("table_id"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

cfg = get_config(table_id)
print(f"run_id={run_id}\n{cfg.source_location}  ->  {cfg.bronze_table}")
load_files_to_bronze(cfg, run_id)

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows_in_bronze FROM {cfg.bronze_table}"))
display(spark.sql("SELECT * FROM mfw.control.ctl_run_log ORDER BY start_time DESC LIMIT 10"))
