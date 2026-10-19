-- Run in DBeaver against your cloud Postgres (Neon / Supabase / etc.)
-- Creates two source tables. Both have updated_at: that is our WATERMARK column.

DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;

CREATE TABLE customers (
    customer_id  INT PRIMARY KEY,
    first_name   VARCHAR(50),
    last_name    VARCHAR(50),
    email        VARCHAR(100),
    city         VARCHAR(50),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id    INT PRIMARY KEY,
    product_name  VARCHAR(100),
    category      VARCHAR(50),
    unit_price    NUMERIC(10,2),
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);
