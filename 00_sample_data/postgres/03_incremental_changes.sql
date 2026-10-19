-- DO NOT run this yet. We run it later (Phase 3/4) AFTER the first load,
-- to prove that the pipeline only picks up what changed.
-- Note: every change bumps updated_at, otherwise the watermark can't see it.

-- 1) an UPDATE to an existing row
UPDATE customers SET city = 'Mumbai', updated_at = now() WHERE customer_id = 1;

-- 2) a brand new row
INSERT INTO customers (customer_id, first_name, last_name, email, city, updated_at)
VALUES (11, 'Pooja', 'Desai', 'pooja.desai@example.com', 'Surat', now());

-- 3) a price change on a product
UPDATE products SET unit_price = 1150.00, updated_at = now() WHERE product_id = 1;
