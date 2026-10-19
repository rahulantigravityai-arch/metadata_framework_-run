# Phase 0 – Setup and connectivity checks

Goal: by the end you have (1) a catalog with empty layers, (2) sample files in a landing place,
(3) a Postgres with 2 tables, and (4) proof that Databricks can reach both.
Nothing "metadata-driven" yet – we are just laying the ground.

## Vocabulary (read once)
| Word | Meaning here |
|---|---|
| Catalog / schema | Folders for tables: `mfw.bronze.orders` = catalog `mfw`, schema `bronze`, table `orders` |
| Volume | A folder for *files* inside Unity Catalog |
| Landing zone | Where raw files arrive (your S3 folder, or a Volume as fallback) |
| Bronze | Raw copy of the source + a few audit columns. No cleaning. |
| Silver | Cleaned, de-duplicated, latest version of each row |
| Control tables | Our metadata: they describe *what* to load. (Phase 1) |

## Step 1 – Put the order files in S3
1. In your bucket create the folder: `s3://<your-bucket>/landing/orders/`
2. Upload ONLY `orders_day1.csv` for now (from `00_sample_data/orders/`).
   Keep day2 and day3 on your PC – we upload them later to watch Auto Loader pick up new files.

## Step 2 – Create the Postgres source
1. Create a free Postgres (Neon or Supabase). Copy host, port, database, user, password.
2. In DBeaver connect to it and run, in order:
   - `00_sample_data/postgres/01_create_tables.sql`
   - `00_sample_data/postgres/02_seed_initial_data.sql`
3. Check: `SELECT count(*) FROM customers;` -> 10, `products` -> 5.
   Do NOT run `03_incremental_changes.sql` yet.

## Step 3 – Create catalog and schemas in Databricks
Open a notebook (SQL or Python) and run:

```sql
CREATE CATALOG IF NOT EXISTS mfw;
CREATE SCHEMA  IF NOT EXISTS mfw.control;   -- our 3 metadata tables
CREATE SCHEMA  IF NOT EXISTS mfw.bronze;
CREATE SCHEMA  IF NOT EXISTS mfw.silver;
CREATE VOLUME  IF NOT EXISTS mfw.control.landing;   -- fallback landing zone + checkpoints
SHOW SCHEMAS IN mfw;
```
Expected: control, bronze, silver (plus default / information_schema).
If CREATE CATALOG is refused in Free Edition, use the catalog Databricks gives you and tell me its name.

## Step 4 – Test: can Databricks read the S3 files?
```python
path = "s3://<your-bucket>/landing/orders/"
display(dbutils.fs.ls(path))
spark.read.option("header", True).csv(path).show(5)
```
- Works -> great, S3 is your landing zone.
- Fails with a permissions / credentials error -> **Fallback (equally valid for learning):**
  upload the CSVs through the UI into the volume at
  `/Volumes/mfw/control/landing/orders/` (Catalog -> mfw -> control -> landing -> Upload).
  In this framework the path is just a value in a config row, so switching to S3 later is a
  one-row update. That is the whole point of the design.

## Step 5 – Test: can Databricks reach Postgres?
Store the password in a secret instead of the notebook if you can; for learning, a widget is OK.
```python
jdbc_url = "jdbc:postgresql://<host>:5432/<database>?sslmode=require"
props = {"user": "<user>", "password": "<password>", "driver": "org.postgresql.Driver"}
spark.read.jdbc(jdbc_url, "customers", properties=props).show(3)
```
- Works -> JDBC path is ready.
- Fails with a timeout / unknown host -> Free Edition restricts outbound internet to approved
  destinations, so the database is blocked. Tell me the exact error. Options then:
  a) export `customers` / `products` from DBeaver to CSV and treat them as a second file source
     while I show you the JDBC code to read later in a full workspace, or
  b) try a different hosted database.
- Fails with driver not found -> tell me; we add the driver differently.

## Check yourself before Phase 1
- [ ] Catalog + 3 schemas + volume exist
- [ ] You can read orders_day1.csv from Databricks (S3 or volume)
- [ ] You can read `customers` from Postgres (or we agreed on the fallback)

Reply with what worked / the exact errors, and we start Phase 1: the three control tables.
