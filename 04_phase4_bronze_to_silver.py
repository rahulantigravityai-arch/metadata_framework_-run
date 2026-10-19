# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 – Bronze -> Silver (one table)
# MAGIC The logic is the function `bronze_to_silver` in `00_pipeline_lib`.
# MAGIC Steps inside it: keep the latest row per primary key -> trim text, drop null keys -> MERGE into Silver -> log.

# COMMAND ----------

# MAGIC %run ./00_pipeline_lib

# COMMAND ----------

import uuid

dbutils.widgets.text("table_id", "1")
dbutils.widgets.text("run_id", "")
table_id = int(dbutils.widgets.get("table_id"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

cfg = get_config(table_id)
print(f"run_id={run_id}\n{cfg.bronze_table} -> {cfg.silver_table}   key={cfg.primary_key}")
bronze_to_silver(cfg, run_id)

# COMMAND ----------

# Checks
pks = [c.strip() for c in cfg.primary_key.split(",")]
b = spark.table(cfg.bronze_table).count()
s = spark.table(cfg.silver_table).count()
print(f"Bronze rows: {b}   Silver rows: {s}")

dupes = spark.sql(f"SELECT {', '.join(pks)}, count(*) AS n FROM {cfg.silver_table} GROUP BY {', '.join(pks)} HAVING count(*) > 1")
print("duplicate keys in Silver (should be 0):", dupes.count())

display(spark.sql("SELECT * FROM mfw.control.ctl_run_log WHERE layer = 'silver' ORDER BY start_time DESC LIMIT 10"))
