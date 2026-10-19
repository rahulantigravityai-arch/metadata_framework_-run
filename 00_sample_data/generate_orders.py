"""Generates 3 small order CSV files to upload to your S3 landing folder.

Run:  python generate_orders.py
Output: ./orders/orders_day1.csv, orders_day2.csv, orders_day3.csv

Teaching points built into the data:
  - day2 contains order 1005 AGAIN with a new status  -> an UPDATE of an existing key (Silver MERGE handles it)
  - day2 also contains an exact duplicate row          -> Silver dedupe handles it
  - customer_id / product_id match the Postgres tables  -> you can join later in Gold
"""
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)
out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "orders")
os.makedirs(out_dir, exist_ok=True)

header = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts", "status"]
prices = {1: 1200.00, 2: 25.50, 3: 80.00, 4: 15.75, 5: 300.00}


def make_orders(start_id, n, day):
    rows = []
    for i in range(n):
        pid = random.randint(1, 5)
        ts = datetime(2026, 10, day, random.randint(8, 20), random.randint(0, 59), random.randint(0, 59))
        rows.append([start_id + i, random.randint(1, 10), pid, random.randint(1, 5), prices[pid],
                     ts.strftime("%Y-%m-%d %H:%M:%S"), "PLACED"])
    return rows


def write(name, rows):
    with open(os.path.join(out_dir, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {name}: {len(rows)} rows")


day1 = make_orders(1001, 15, 1)
write("orders_day1.csv", day1)

day2 = make_orders(1016, 15, 2)
updated = list(day1[4])          # order 1005 comes back with a new status
updated[6] = "SHIPPED"
day2.append(updated)
day2.append(list(day2[0]))       # exact duplicate row
write("orders_day2.csv", day2)

day3 = make_orders(1031, 10, 3)
write("orders_day3.csv", day3)
