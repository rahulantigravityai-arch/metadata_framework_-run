-- Phase 6, Part A: a brand-new source table. Run in Supabase / DBeaver.
-- Nothing in Databricks code will change to load this - only config rows.

DROP TABLE IF EXISTS suppliers;

CREATE TABLE suppliers (
    supplier_id    INT PRIMARY KEY,
    supplier_name  VARCHAR(100),
    country        VARCHAR(50),
    rating         NUMERIC(3,1),
    updated_at     TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO suppliers (supplier_id, supplier_name, country, rating, updated_at) VALUES
(1, 'Northwind Traders',  'USA',     4.5, '2026-10-02 09:00:00'),
(2, 'Globex Components',  'Germany', 4.1, '2026-10-02 09:00:00'),
(3, 'Initech Supplies',   'India',   3.8, '2026-10-02 09:00:00'),
(4, 'Umbrella Wholesale', 'UK',      4.7, '2026-10-02 09:00:00'),
(5, 'Soylent Logistics',  'Canada',  3.9, '2026-10-02 09:00:00');

-- Optional later, to watch an incremental load pick up a change:
-- UPDATE suppliers SET rating = 4.9, updated_at = now() WHERE supplier_id = 3;
