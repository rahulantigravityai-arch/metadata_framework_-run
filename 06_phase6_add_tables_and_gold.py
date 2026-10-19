# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Phase 6 – Add tables with config rows only, then look at Gold
# MAGIC Before running this notebook:
# MAGIC 1. Supabase: run `00_sample_data/postgres/04_add_suppliers.sql`
# MAGIC 2. Databricks volume: create folder `/Volumes/mfw/control/landing/returns/` and upload `returns_day1.csv` ONLY
# MAGIC
# MAGIC Then run the cells below, then run the DRIVER (05). We do not touch any pipeline code.

# COMMAND ----------

# Part A + B: two new tables = two config rows + one watermark row. (Safe to re-run.)
spark.sql("DELETE FROM mfw.control.ctl_source_config WHERE table_id IN (4, 5)")
spark.sql("DELETE FROM mfw.control.ctl_watermark     WHERE table_id IN (4, 5)")

spark.sql("""
INSERT INTO mfw.control.ctl_source_config VALUES
 (4, 'suppliers', 'jdbc',       'public.suppliers', NULL,
     'mfw.bronze.suppliers', 'mfw.silver.suppliers', 'supplier_id', 'updated_at', 'merge', true, 'Phase 6: added by config only'),
 (5, 'returns',   'autoloader', '/Volumes/mfw/control/landing/returns/', 'csv',
     'mfw.bronze.returns',   'mfw.silver.returns',   'return_id',   NULL,         'merge', true, 'Phase 6: added by config only')
""")

# only database tables need a watermark row
spark.sql("INSERT INTO mfw.control.ctl_watermark VALUES (4, '1900-01-01 00:00:00', current_timestamp())")

display(spark.table("mfw.control.ctl_source_config").orderBy("table_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now run notebook 05 (the driver)
# MAGIC Expect: orders, customers, products, suppliers, returns all OK, then the Gold step OK.
# MAGIC Come back here for the cells below.

# COMMAND ----------

# Part C: the Gold table the driver just built
display(spark.sql("""
    SELECT order_date, customer_name, city, orders, units, revenue, refunds, net_revenue
    FROM mfw.gold.daily_sales_by_customer
    ORDER BY order_date, customer_name
"""))

# COMMAND ----------

# Part D: follow ONE order from the file all the way to Gold
oid = 1005

print("BRONZE: every version that ever arrived")
display(spark.sql(f"SELECT order_id, status, _source_file, _ingested_at FROM mfw.bronze.orders WHERE order_id = {oid} ORDER BY _ingested_at"))

print("SILVER: one current row")
display(spark.sql(f"SELECT order_id, customer_id, quantity, unit_price, status FROM mfw.silver.orders WHERE order_id = {oid}"))

print("GOLD: it is part of this customer's day")
display(spark.sql(f"""
    SELECT g.* FROM mfw.gold.daily_sales_by_customer g
    JOIN mfw.silver.orders o ON o.customer_id = g.customer_id AND CAST(o.order_ts AS DATE) = g.order_date
    WHERE o.order_id = {oid}"""))

print("RUN LOG: the last runs, newest first")
display(spark.sql("""SELECT run_id, table_id, layer, status, rows_processed, start_time FROM mfw.control.ctl_run_log
                     ORDER BY start_time DESC LIMIT 20"""))