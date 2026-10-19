# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Phase 5 – The driver
# MAGIC Reads the config, and for every active table runs: Bronze step (chosen by source_type) -> Silver step.
# MAGIC Everything runs in THIS session by calling functions from `00_pipeline_lib` (no child notebooks, so it works on Free Edition).
# MAGIC One failing table never stops the others. Everything shares ONE run_id, so the log tells the whole story.

# COMMAND ----------

# MAGIC %run ./00_pipeline_lib

# COMMAND ----------

import uuid

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("only_table_id", "")        # blank = every active table; a number = just that table
dbutils.widgets.text("pg_host", "aws-0-ap-northeast-1.pooler.supabase.com")
dbutils.widgets.text("pg_port", "5432")
dbutils.widgets.text("pg_db", "postgres")
dbutils.widgets.text("pg_user", "postgres.swsbemrillygimvngitp")
dbutils.widgets.text("pg_password", "")

run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
only = dbutils.widgets.get("only_table_id").strip()
print("run_id =", run_id)

jdbc_url, jdbc_props = make_jdbc_conn(dbutils.widgets.get("pg_host"), dbutils.widgets.get("pg_port"),
                                      dbutils.widgets.get("pg_db"), dbutils.widgets.get("pg_user"),
                                      dbutils.widgets.get("pg_password"))

# COMMAND ----------

# 1) Which tables should run?  (the config decides, not this code)
cfgs = spark.table("mfw.control.ctl_source_config").filter("is_active = true")
if only:
    cfgs = cfgs.filter(f"table_id = {int(only)}")
cfgs = cfgs.orderBy("table_id").collect()
print("tables to run:", [c.table_name for c in cfgs])

# 2) Which function handles which source type?  (this small map is the only "routing" in the framework)
BRONZE_STEP = {
    "autoloader": lambda cfg: load_files_to_bronze(cfg, run_id),
    "jdbc":       lambda cfg: load_jdbc_to_bronze(cfg, run_id, jdbc_url, jdbc_props),
}

# COMMAND ----------

# 3) Loop. Each step logs its own STARTED/SUCCESS/FAILED to ctl_run_log; the driver just keeps going.
results = []
for c in cfgs:
    if c.source_type not in BRONZE_STEP:
        results.append((c.table_id, c.table_name, "SKIPPED", f"unknown source_type {c.source_type}"))
        continue
    try:
        print(f"--- {c.table_name}: bronze")
        BRONZE_STEP[c.source_type](c)
        print(f"--- {c.table_name}: silver")
        bronze_to_silver(c, run_id)
        results.append((c.table_id, c.table_name, "OK", ""))
    except Exception as e:
        # bronze failed -> silver is skipped for this table, and we move on to the next table
        results.append((c.table_id, c.table_name, "FAILED", str(e)[:300]))

# COMMAND ----------

# 3b) Gold: build it only if EVERY table above succeeded (never publish numbers made from partial data)
if only:
    results.append((100, "daily_sales_by_customer", "SKIPPED", "only_table_id was set"))
elif any(r[2] != "OK" for r in results):
    results.append((100, "daily_sales_by_customer", "SKIPPED", "an earlier table failed"))
else:
    try:
        build_gold_daily_sales(run_id)
        results.append((100, "daily_sales_by_customer", "OK", ""))
    except Exception as e:
        results.append((100, "daily_sales_by_customer", "FAILED", str(e)[:300]))

# COMMAND ----------

# 4) Report
display(spark.createDataFrame(results, "table_id INT, table_name STRING, outcome STRING, detail STRING"))
display(spark.sql("""SELECT table_id, layer, status, rows_processed, start_time, end_time, error_message
                     FROM mfw.control.ctl_run_log WHERE run_id = :r ORDER BY table_id, layer""", args={"r": run_id}))

failed = [r for r in results if r[2] != "OK"]
if failed:
    # Make the whole run RED so a scheduler / alert notices, but only AFTER every table had its turn
    raise Exception(f"{len(failed)} table(s) did not complete: {[(r[1], r[2]) for r in failed]}")