-- Initial load: the first time the pipeline runs it will pick all of these up.

INSERT INTO customers (customer_id, first_name, last_name, email, city, updated_at) VALUES
(1,  'Asha',   'Patil',   'asha.patil@example.com',   'Pune',      '2026-10-01 09:00:00'),
(2,  'Rohan',  'Shah',    'rohan.shah@example.com',   'Mumbai',    '2026-10-01 09:00:00'),
(3,  'Meera',  'Iyer',    'meera.iyer@example.com',   'Chennai',   '2026-10-01 09:00:00'),
(4,  'Vikram', 'Singh',   'vikram.singh@example.com', 'Delhi',     '2026-10-01 09:00:00'),
(5,  'Neha',   'Kulkarni','neha.k@example.com',       'Pune',      '2026-10-01 09:00:00'),
(6,  'Arjun',  'Rao',     'arjun.rao@example.com',    'Bengaluru', '2026-10-01 09:00:00'),
(7,  'Sana',   'Khan',    'sana.khan@example.com',    'Hyderabad', '2026-10-01 09:00:00'),
(8,  'Karan',  'Mehta',   'karan.mehta@example.com',  'Ahmedabad', '2026-10-01 09:00:00'),
(9,  'Divya',  'Nair',    'divya.nair@example.com',   'Kochi',     '2026-10-01 09:00:00'),
(10, 'Imran',  'Sheikh',  'imran.sheikh@example.com', 'Nagpur',    '2026-10-01 09:00:00');

INSERT INTO products (product_id, product_name, category, unit_price, updated_at) VALUES
(1, 'Laptop Pro 14',      'Electronics', 1200.00, '2026-10-01 09:00:00'),
(2, 'Wireless Mouse',     'Accessories',   25.50, '2026-10-01 09:00:00'),
(3, 'Mechanical Keyboard','Accessories',   80.00, '2026-10-01 09:00:00'),
(4, 'USB-C Cable',        'Accessories',   15.75, '2026-10-01 09:00:00'),
(5, 'Monitor 27 inch',    'Electronics',  300.00, '2026-10-01 09:00:00');
