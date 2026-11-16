"""Create a large SQLite database for trying the streaming engine and job files.

    python tools/make_big_table.py big.db --rows 1000000          # customers + orders
    python tools/make_big_table.py big.db --add 5000               # new orders (for incremental runs)
    python tools/make_big_table.py big.db --update 2000            # change existing orders (for upsert)

The data is synthetic but shaped like a real shop: money as DECIMAL, time-zone
timestamps stored as text, a 0/1 flag declared BOOLEAN, empty notes, e-mails.
"""
from __future__ import annotations

import argparse
import random
import sqlite3
import time
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
STATUSES = ["placed", "paid", "shipped", "delivered", "returned"]
CITIES = ["Jodhpur", "Jaipur", "Delhi", "Mumbai", "Pune", "Bengaluru", "Chennai", "Kolkata"]
FIRST = ["Aarav", "Diya", "Kabir", "Ananya", "Vihaan", "Isha", "Rohan", "Meera", "Arjun", "Sara"]
LAST = ["Sharma", "Gupta", "Iyer", "Khan", "Patel", "Reddy", "Singh", "Das", "Mehta", "Joshi"]
START = datetime(2023, 1, 1, tzinfo=IST)


def create(db: sqlite3.Connection, rows: int, customers: int, seed: int) -> None:
    rng = random.Random(seed)
    db.executescript("""
        DROP TABLE IF EXISTS orders; DROP TABLE IF EXISTS customers;
        CREATE TABLE customers (id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, email TEXT NOT NULL,
                                city TEXT, created_at TIMESTAMP NOT NULL);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers(id),
                             amount DECIMAL(12,2) NOT NULL, placed_at TEXT NOT NULL, status TEXT NOT NULL,
                             is_gift BOOLEAN NOT NULL, note TEXT, updated_at TIMESTAMP NOT NULL);
    """)
    db.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", (
        (i, f"{rng.choice(FIRST)} {rng.choice(LAST)}", f"user{i}@example.com", rng.choice(CITIES),
         (START + timedelta(days=rng.randrange(700))).strftime("%Y-%m-%d %H:%M:%S")) for i in range(1, customers + 1)))
    insert_orders(db, 1, rows, customers, rng)
    db.commit()


def insert_orders(db, first_id: int, n: int, customers: int, rng, base: datetime | None = None) -> None:
    base = base or START
    chunk = []
    for i in range(first_id, first_id + n):
        placed = base + timedelta(minutes=rng.randrange(0, 60 * 24 * 690))
        chunk.append((i, rng.randrange(1, customers + 1), f"{rng.randrange(100, 2_000_000) / 100:.2f}",
                      placed.isoformat(), rng.choice(STATUSES), rng.random() < 0.1,
                      None if rng.random() < 0.7 else f"note {i}",
                      (placed + timedelta(hours=rng.randrange(1, 48))).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")))
        if len(chunk) == 50_000:
            db.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", chunk)
            chunk = []
    if chunk:
        db.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", chunk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db")
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--customers", type=int, default=50_000)
    ap.add_argument("--add", type=int, default=0, help="append this many new orders, updated now")
    ap.add_argument("--update", type=int, default=0, help="change this many existing orders, updated now")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    t0 = time.time()
    db = sqlite3.connect(a.db)
    rng = random.Random(a.seed + int(time.time()))
    if a.add or a.update:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if a.add:
            nxt = db.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM orders").fetchone()[0]
            ncust = db.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
            insert_orders(db, nxt, a.add, ncust, rng)
            db.execute("UPDATE orders SET updated_at = ? WHERE id >= ?", (now, nxt))
        if a.update:
            ids = [r[0] for r in db.execute("SELECT id FROM orders ORDER BY RANDOM() LIMIT ?", (a.update,))]
            db.executemany("UPDATE orders SET status = 'returned', amount = amount + 1, updated_at = ? WHERE id = ?",
                           [(now, i) for i in ids])
        db.commit()
        print(f"added {a.add} and changed {a.update} orders in {time.time() - t0:.1f}s")
    else:
        create(db, a.rows, a.customers, a.seed)
        print(f"created {a.db}: {a.customers:,} customers, {a.rows:,} orders in {time.time() - t0:.1f}s")
    db.close()


if __name__ == "__main__":
    main()
