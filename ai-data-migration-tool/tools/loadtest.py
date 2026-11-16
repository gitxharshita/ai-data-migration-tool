"""Simulate many users on the dashboard and report response times.

Start the dashboard first, for example:
    python -m amt ui --public --no-browser --port 8765
Then:
    python tools/loadtest.py --users 100 --ramp 60
    python tools/loadtest.py --users 100 --ramp 0        # worst case: everyone at the same second

Each simulated user behaves like a real visitor: opens the page, connects to a
random demo database, generates a mapping, runs a migration, downloads the
result and tries the fault-injection view, pausing briefly between steps.
"""
from __future__ import annotations

import argparse
import http.cookiejar
import json
import random
import statistics
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict

DEMOS = {
    "demo:ecommerce": [("orders", "placed_at"), ("payments", "paid_at"), ("carts", "updated_at"), ("products", "")],
    "demo:healthcare": [("claims", "submitted_at"), ("lab_results", "collected_at"), ("encounters", "")],
    "demo:logistics": [("shipments", "shipped_at"), ("tracking_events", "event_at"), ("vehicles", "")],
    "demo:support": [("tickets", "opened_at"), ("messages", "sent_at"), ("accounts", "")],
}
FAULTS = ["truncated_decimal", "duplicated_row", "timezone_shift", "off_by_one_partition"]


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.times = defaultdict(list)
        self.errors = defaultdict(int)
        self.users_done = 0

    def add(self, key, seconds):
        with self.lock:
            self.times[key].append(seconds)

    def error(self, key):
        with self.lock:
            self.errors[key] += 1


class User:
    def __init__(self, base, stats, think):
        self.base, self.stats, self.think = base, stats, think
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, path, body=None, key=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, headers={"Content-Type": "application/json"})
        t0 = time.perf_counter()
        try:
            with self.opener.open(req, timeout=300) as r:
                raw = r.read()
                if key:
                    self.stats.add(key, time.perf_counter() - t0)
                return json.loads(raw) if "json" in r.headers.get("Content-Type", "") else raw
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            self.stats.error(key or path)
            raise RuntimeError(f"{path}: {e}") from e

    def job(self, job_type, body):
        t0 = time.perf_counter()
        r = self.call("/api/jobs", {"type": job_type, **body}, key="submit job")
        while True:
            st = self.call(f"/api/jobs/{r['job_id']}", key="poll status")
            if st["status"] == "done":
                self.stats.add(f"job: {job_type}", time.perf_counter() - t0)
                return r["job_id"], st["result"]
            if st["status"] == "error":
                self.stats.error(f"job: {job_type}")
                raise RuntimeError(st["error"])
            time.sleep(1.0 if st["status"] == "queued" else 0.4)

    def pause(self):
        time.sleep(random.uniform(0.5, 1.5) * self.think)

    def visit(self):
        rnd = random.Random()
        self.call("/", key="page load")
        self.call("/api/info", key="info")
        source = rnd.choice(list(DEMOS))
        self.call("/api/connect", {"source": source}, key="connect")
        table, part = rnd.choice(DEMOS[source])
        body = {"source": source, "table": table, "strategy": rnd.choice(["h1", "h2"]), "partition_by": part}
        self.pause()
        self.job("plan", body)
        self.pause()
        jid, result = self.job("migrate", body)
        if result["summary"]["status"] != "PASS" and body["strategy"] == "h2":
            self.stats.error("unexpected FAIL")
        self.call(f"/api/jobs/{jid}/download", key="download zip")
        self.pause()
        self.job("break", {**body, "injection": rnd.choice(FAULTS)})


def pct(values, q):
    v = sorted(values)
    return v[min(len(v) - 1, int(q * len(v)))]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://127.0.0.1:8765")
    ap.add_argument("--users", type=int, default=100)
    ap.add_argument("--ramp", type=float, default=60, help="seconds over which users arrive (0 = all at once)")
    ap.add_argument("--think", type=float, default=2.0, help="average pause between a user's steps, seconds")
    args = ap.parse_args()

    stats = Stats()
    failures = []

    def run_user(i):
        try:
            User(args.url, stats, args.think).visit()
            with stats.lock:
                stats.users_done += 1
        except Exception as e:
            failures.append(str(e))

    print(f"Simulating {args.users} users arriving over {args.ramp:.0f}s against {args.url} ...")
    t0 = time.perf_counter()
    threads = []
    for i in range(args.users):
        th = threading.Thread(target=run_user, args=(i,), daemon=True)
        th.start()
        threads.append(th)
        if args.ramp:
            time.sleep(args.ramp / args.users)
    for th in threads:
        th.join()
    wall = time.perf_counter() - t0

    print(f"\nFinished in {wall:.1f}s: {stats.users_done}/{args.users} users completed every step.")
    print(f"{'step':<18}{'count':>7}{'median':>10}{'p95':>10}{'max':>10}   (seconds)")
    for key in ["page load", "info", "connect", "submit job", "poll status", "download zip",
                "job: plan", "job: migrate", "job: break"]:
        v = stats.times.get(key)
        if v:
            print(f"{key:<18}{len(v):>7}{statistics.median(v):>10.2f}{pct(v, .95):>10.2f}{max(v):>10.2f}")
    jobs = sum(len(stats.times.get(k, [])) for k in ("job: plan", "job: migrate", "job: break"))
    print(f"\nThroughput: {jobs / wall * 60:.0f} jobs per minute")
    print(f"Errors: {dict(stats.errors) or 'none'}")
    for f in failures[:5]:
        print("  failure:", f)
    try:
        health = json.loads(urllib.request.urlopen(args.url + "/api/health", timeout=10).read())
        print(f"Server: {health}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
