"""Generates 2 small 'returns' CSV files for Phase 6 (a second FILE source).

Run (after generate_orders.py):  python generate_returns.py
Output: ./returns/returns_day1.csv, returns_day2.csv

Teaching point: returns_day2 repeats return_id 9001 with a CORRECTED refund -> Silver keeps the latest version.
"""
import csv
import os
import random

random.seed(7)
here = os.path.dirname(os.path.abspath(__file__))
out_dir = os.path.join(here, "returns")
os.makedirs(out_dir, exist_ok=True)


def read_orders(name):
    with open(os.path.join(here, "orders", name), newline="") as f:
        return list(csv.DictReader(f))


header = ["return_id", "order_id", "reason", "refund_amount", "return_ts"]
reasons = ["Damaged item", "Wrong item", "Changed mind", "Late delivery"]


def make_return(rid, order, day, hour, full=True):
    amount = float(order["quantity"]) * float(order["unit_price"])
    if not full:
        amount = round(amount / 2, 2)
    return [rid, order["order_id"], random.choice(reasons), round(amount, 2), f"2026-10-{day:02d} {hour:02d}:15:00"]


day1_orders = random.sample(read_orders("orders_day1.csv"), 4)
day1 = [make_return(9001 + i, o, 2, 10 + i) for i, o in enumerate(day1_orders)]

day2_orders = random.sample(read_orders("orders_day2.csv")[:15], 2)
day2 = [make_return(9005 + i, o, 3, 11 + i, full=False) for i, o in enumerate(day2_orders)]
corrected = list(day1[0])            # same return_id 9001, refund corrected to half
corrected[3] = round(float(corrected[3]) / 2, 2)
corrected[2] = "Partial refund agreed"
day2.append(corrected)

for name, rows in [("returns_day1.csv", day1), ("returns_day2.csv", day2)]:
    with open(os.path.join(out_dir, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {name}: {len(rows)} rows")
