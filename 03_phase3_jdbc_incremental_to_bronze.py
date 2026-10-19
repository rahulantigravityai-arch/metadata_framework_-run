# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 – JDBC incremental: database -> Bronze (one table)
# MAGIC The logic is the function `load_jdbc_to_bronze` in `00_pipeline_lib`.
# MAGIC Steps inside it: read config + watermark -> ask Postgres only for rows newer than the watermark -> append to Bronze
# MAGIC -> move the watermark (only after a successful write) -> log.

# COMMAND ----------

# MAGIC %run ./00_pipeline_lib

# COMMAND ----------

import uuid

dbutils.widgets.text("table_id", "2")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pg_host", "aws-0-<region>.pooler.supabase.com")
dbutils.widgets.text("pg_port", "5432")
dbutils.widgets.text("pg_db", "postgres")
dbutils.widgets.text("pg_user", "postgres.<project-ref>")
dbutils.widgets.text("pg_password", "")        # learning fallback; a secret (scope 'mfw', key 'pg_password') wins if it exists

table_id = int(dbutils.widgets.get("table_id"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

jdbc_url, jdbc_props = make_jdbc_conn(dbutils.widgets.get("pg_host"), dbutils.widgets.get("pg_port"),
                                      dbutils.widgets.get("pg_db"), dbutils.widgets.get("pg_user"),
                                      dbutils.widgets.get("pg_password"))

cfg = get_config(table_id)
print(f"run_id={run_id}\n{cfg.source_location} -> {cfg.bronze_table}")
load_jdbc_to_bronze(cfg, run_id, jdbc_url, jdbc_props)

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows_in_bronze FROM {cfg.bronze_table}"))
display(spark.table("mfw.control.ctl_watermark").orderBy("table_id"))
display(spark.sql("SELECT * FROM mfw.control.ctl_run_log ORDER BY start_time DESC LIMIT 10"))
