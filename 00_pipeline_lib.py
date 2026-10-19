# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline library
# MAGIC All the real logic lives here as plain functions. Two things use them:
# MAGIC * notebooks 02, 03, 04 (run ONE step for ONE table, handy for learning)
# MAGIC * notebook 05, the driver (runs every step for every table, in one session)
# MAGIC
# MAGIC Load it with `%run ./00_pipeline_lib` (it must be alone in its cell).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

LANDING_BASE = "/Volumes/mfw/control/landing"      # also holds Auto Loader checkpoints and schemas

# COMMAND ----------

# ---------- small helpers ----------

def get_config(table_id):
    """The one config row for this table, from ctl_source_config."""
    rows = spark.table("mfw.control.ctl_source_config").filter(f"table_id = {int(table_id)}").collect()
    assert len(rows) == 1, f"expected exactly 1 config row for table_id={table_id}, got {len(rows)}"
    assert rows[0].is_active, f"table {rows[0].table_name} is switched off in config"
    return rows[0]


def make_logger(run_id, table_id, layer):
    """Returns (log_start, log_end) that write to ctl_run_log for this run / table / layer."""
    def log_start():
        spark.sql("""INSERT INTO mfw.control.ctl_run_log
                     VALUES (:run_id, :table_id, :layer, 'STARTED', NULL, current_timestamp(), NULL, NULL)""",
                  args={"run_id": run_id, "table_id": table_id, "layer": layer})

    def log_end(status, rows=None, error=None):
        spark.sql("""UPDATE mfw.control.ctl_run_log
                     SET status = :status, rows_processed = :rows, end_time = current_timestamp(), error_message = :error
                     WHERE run_id = :run_id AND table_id = :table_id AND layer = :layer""",
                  args={"status": status, "rows": rows, "error": error,
                        "run_id": run_id, "table_id": table_id, "layer": layer})
    return log_start, log_end


def make_jdbc_conn(host, port, db, user, password_widget_value):
    """Builds the JDBC url + properties. Password: a secret (scope 'mfw', key 'pg_password') if it exists, else what was typed."""
    try:
        password = dbutils.secrets.get("mfw", "pg_password")
    except Exception:
        password = password_widget_value
    url = f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require"
    return url, {"user": user, "password": password, "driver": "org.postgresql.Driver"}

# COMMAND ----------

# ---------- STEP A: files -> Bronze (Auto Loader) ----------

def load_files_to_bronze(cfg, run_id):
    assert cfg.source_type == "autoloader", f"expected autoloader, got {cfg.source_type}"
    log_start, log_end = make_logger(run_id, cfg.table_id, "bronze")

    checkpoint_path = f"{LANDING_BASE}/_checkpoints/{cfg.table_name}"   # remembers which files were already read
    schema_path = f"{LANDING_BASE}/_schemas/{cfg.table_name}"           # remembers the inferred schema

    log_start()
    try:
        count_before = spark.table(cfg.bronze_table).count() if spark.catalog.tableExists(cfg.bronze_table) else 0

        df = (spark.readStream
                .format("cloudFiles")                                   # <- Auto Loader
                .option("cloudFiles.format", cfg.source_format)
                .option("cloudFiles.schemaLocation", schema_path)
                .option("cloudFiles.inferColumnTypes", "true")
                .option("header", "true")
                .load(cfg.source_location)
                .withColumn("_source_file", F.col("_metadata.file_path"))
                .withColumn("_ingested_at", F.current_timestamp()))

        query = (df.writeStream
                   .option("checkpointLocation", checkpoint_path)
                   .trigger(availableNow=True)                          # process what's new, then stop
                   .toTable(cfg.bronze_table))
        query.awaitTermination()

        rows = spark.table(cfg.bronze_table).count() - count_before     # Bronze is append-only
        log_end("SUCCESS", rows=rows)
        print(f"[{cfg.table_name}] bronze SUCCESS: {rows} new rows")
        return rows
    except Exception as e:
        log_end("FAILED", error=str(e)[:2000])
        raise

# COMMAND ----------

# ---------- STEP B: database -> Bronze (JDBC incremental with a watermark) ----------

def load_jdbc_to_bronze(cfg, run_id, jdbc_url, jdbc_props):
    assert cfg.source_type == "jdbc", f"expected jdbc, got {cfg.source_type}"
    assert cfg.watermark_column, "jdbc tables need a watermark_column in config"
    log_start, log_end = make_logger(run_id, cfg.table_id, "bronze")

    wm_rows = spark.table("mfw.control.ctl_watermark").filter(f"table_id = {cfg.table_id}").collect()
    assert len(wm_rows) == 1, f"no watermark row for table_id={cfg.table_id} (add one with '1900-01-01 00:00:00')"
    last_wm = wm_rows[0].last_value

    log_start()
    try:
        # The subquery runs INSIDE Postgres, so only new/changed rows travel over the network.
        query = f"""(SELECT * FROM {cfg.source_location}
                     WHERE {cfg.watermark_column} > '{last_wm}'::timestamp) AS src"""

        df = (spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props)
                   .withColumn("_run_id", F.lit(run_id))
                   .withColumn("_ingested_at", F.current_timestamp()))
        df.write.mode("append").saveAsTable(cfg.bronze_table)

        # Measure from the rows we just wrote (tagged with this run_id), not from a second read of Postgres.
        stats = (spark.table(cfg.bronze_table).filter(F.col("_run_id") == run_id)
                      .agg(F.count("*").alias("n"),
                           F.max(cfg.watermark_column).cast("string").alias("max_wm"))
                      .collect()[0])
        rows, new_wm = stats["n"], stats["max_wm"]

        # Move the watermark ONLY after a successful write, and only if we got rows.
        if rows > 0:
            spark.sql("""UPDATE mfw.control.ctl_watermark SET last_value = :v, updated_at = current_timestamp()
                         WHERE table_id = :t""", args={"v": new_wm, "t": cfg.table_id})

        log_end("SUCCESS", rows=rows)
        print(f"[{cfg.table_name}] bronze SUCCESS: {rows} new rows; watermark {last_wm} -> {new_wm if rows > 0 else last_wm}")
        return rows
    except Exception as e:
        log_end("FAILED", error=str(e)[:2000])
        raise

# COMMAND ----------

# ---------- STEP C: Bronze -> Silver (latest row per key, then MERGE) ----------

def bronze_to_silver(cfg, run_id):
    assert cfg.load_type == "merge", f"only 'merge' is implemented, got {cfg.load_type}"
    log_start, log_end = make_logger(run_id, cfg.table_id, "silver")
    pks = [c.strip() for c in cfg.primary_key.split(",")]       # supports composite keys: "a, b"

    log_start()
    try:
        bronze = spark.table(cfg.bronze_table)

        # 1) DEDUPE: latest version of each key (watermark column first, then newest ingest)
        order_cols = []
        if cfg.watermark_column:
            order_cols.append(F.col(cfg.watermark_column).desc_nulls_last())
        for c in ["_ingested_at", "_source_file"]:
            if c in bronze.columns:
                order_cols.append(F.col(c).desc())
        w = Window.partitionBy(*pks).orderBy(*order_cols)
        latest = bronze.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

        # 2) MINIMAL CLEANING: same rules for every table
        for f in latest.schema.fields:
            if f.dataType.simpleString() == "string" and not f.name.startswith("_"):
                latest = latest.withColumn(f.name, F.trim(F.col(f.name)))
        latest = latest.filter(" AND ".join(f"`{c}` IS NOT NULL" for c in pks))
        latest = latest.drop("_run_id").withColumn("_silver_updated_at", F.current_timestamp())

        # 3) WRITE: first run creates the table, later runs MERGE
        if not spark.catalog.tableExists(cfg.silver_table):
            latest.write.saveAsTable(cfg.silver_table)
            rows = spark.table(cfg.silver_table).count()
            print(f"[{cfg.table_name}] silver created with {rows} rows")
        else:
            target = DeltaTable.forName(spark, cfg.silver_table)
            cond = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pks)
            (target.alias("t")
                   .merge(latest.alias("s"), cond)
                   .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")   # only if Bronze has a NEWER version
                   .whenNotMatchedInsertAll()
                   .execute())
            m = target.history(1).select("operationMetrics").collect()[0][0]
            inserted, updated = int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))
            rows = inserted + updated
            print(f"[{cfg.table_name}] silver merge: {inserted} inserted, {updated} updated")

        log_end("SUCCESS", rows=rows)
        return rows
    except Exception as e:
        log_end("FAILED", error=str(e)[:2000])
        raise

# COMMAND ----------

# ---------- STEP D: Silver -> Gold (a business-ready aggregate) ----------

GOLD_TABLE_ID = 100   # Gold tables are not in ctl_source_config, so this one gets a fixed id in the run log

def build_gold_daily_sales(run_id):
    """Sales per customer per day, net of refunds. Fully rebuilt from Silver every run (cheap and always correct)."""
    log_start, log_end = make_logger(run_id, GOLD_TABLE_ID, "gold")
    log_start()
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS mfw.gold")
        spark.sql("""
            CREATE OR REPLACE TABLE mfw.gold.daily_sales_by_customer AS
            WITH refunds AS (
                SELECT order_id, SUM(refund_amount) AS refund_amount
                FROM mfw.silver.returns
                GROUP BY order_id
            )
            SELECT CAST(o.order_ts AS DATE)                                   AS order_date,
                   o.customer_id,
                   CONCAT(c.first_name, ' ', c.last_name)                     AS customer_name,
                   c.city,
                   COUNT(*)                                                   AS orders,
                   SUM(o.quantity)                                            AS units,
                   ROUND(SUM(o.quantity * o.unit_price), 2)                   AS revenue,
                   ROUND(SUM(COALESCE(r.refund_amount, 0)), 2)                AS refunds,
                   ROUND(SUM(o.quantity * o.unit_price) - SUM(COALESCE(r.refund_amount, 0)), 2) AS net_revenue
            FROM mfw.silver.orders o
            LEFT JOIN mfw.silver.customers c ON o.customer_id = c.customer_id
            LEFT JOIN refunds r              ON o.order_id    = r.order_id
            GROUP BY ALL
        """)
        rows = spark.table("mfw.gold.daily_sales_by_customer").count()
        log_end("SUCCESS", rows=rows)
        print(f"[gold] daily_sales_by_customer rebuilt: {rows} rows")
        return rows
    except Exception as e:
        log_end("FAILED", error=str(e)[:2000])
        raise
