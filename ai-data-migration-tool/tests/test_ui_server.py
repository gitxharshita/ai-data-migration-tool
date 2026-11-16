"""End-to-end test of the multi-user dashboard API (real server, real worker processes)."""
import http.cookiejar
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["AMT_UI_WORKDIR"] = tempfile.mkdtemp(prefix="amt_ui_test_")

from amt.ui import server  # noqa: E402


class Client:
    """One browser: its own cookie jar, so its own session."""

    def __init__(self, base):
        self.base = base
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, headers={"Content-Type": "application/json"})
        try:
            with self.opener.open(req, timeout=60) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if r.headers.get("Content-Type") == "application/json" else raw)
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def run(self, job_type, body, timeout=120):
        code, r = self.call("/api/jobs", {"type": job_type, **body})
        assert code == 202, (code, r)
        deadline = time.time() + timeout
        while time.time() < deadline:
            code, st = self.call(f"/api/jobs/{r['job_id']}")
            if st["status"] in ("done", "error"):
                return r["job_id"], st
            time.sleep(0.2)
        raise TimeoutError(job_type)


def start(public):
    httpd = server.make_server("127.0.0.1", 0, public=public, workers=2)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


class DashboardServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd, cls.base = start(public=True)

    @classmethod
    def tearDownClass(cls):
        cls.httpd.app.jobs.shutdown()
        cls.httpd.shutdown()

    def test_full_flow_and_download(self):
        c = Client(self.base)
        code, info = c.call("/api/info")
        self.assertEqual(code, 200)
        self.assertTrue(info["public"])
        self.assertEqual([d["id"] for d in info["destinations"]], ["local"])
        code, conn = c.call("/api/connect", {"source": "demo:ecommerce"})
        self.assertEqual(len(conn["tables"]), 6)
        body = {"source": "demo:ecommerce", "table": "orders", "strategy": "h2", "partition_by": "placed_at"}
        _, st = c.run("plan", body)
        self.assertEqual(st["status"], "done", st)
        self.assertEqual(st["result"]["dry_run_problems"], [])
        jid, st = c.run("migrate", body)
        self.assertEqual(st["result"]["summary"]["status"], "PASS", st)
        code, data = c.call(f"/api/jobs/{jid}/download")
        self.assertEqual(code, 200)
        names = zipfile.ZipFile(io.BytesIO(data)).namelist()
        self.assertIn("reconciliation_report.md", names)
        self.assertTrue(any(n.endswith(".parquet") for n in names))
        _, st = c.run("break", {**body, "injection": "timezone_shift"})
        caught = [rs["summary"]["status"] for rs in st["result"]["rule_sets"]]
        self.assertEqual(caught, ["PASS", "FAIL"])   # generic misses it, schema-aware catches it

    def test_sessions_are_isolated(self):
        a, b = Client(self.base), Client(self.base)
        jid, _ = a.run("plan", {"source": "demo:healthcare", "table": "claims", "strategy": "h1"})
        code, _ = b.call(f"/api/jobs/{jid}")
        self.assertEqual(code, 404)        # another user cannot see or download it
        code, _ = b.call(f"/api/jobs/{jid}/download")
        self.assertEqual(code, 404)

    def test_public_mode_blocks_files_and_cloud(self):
        c = Client(self.base)
        code, _ = c.call("/api/connect", {"source": "C:/Windows/win.ini"})
        self.assertEqual(code, 403)
        code, _ = c.call("/api/jobs", {"type": "migrate", "source": "sqlite:///x.db", "table": "t"})
        self.assertEqual(code, 403)
        code, _ = c.call("/api/jobs", {"type": "migrate", "source": "demo:ecommerce", "table": "orders",
                                       "destination": "aws", "target_uri": "s3://bucket/x"})
        self.assertEqual(code, 403)
        code, _ = c.call("/api/test_destination", {"destination": "aws"})
        self.assertEqual(code, 403)

    def test_bad_input_is_a_clean_error(self):
        c = Client(self.base)
        code, r = c.call("/api/jobs", {"type": "nonsense", "source": "demo:ecommerce", "table": "orders"})
        self.assertEqual(code, 400)
        self.assertIn("unknown job type", r["error"])
        _, st = c.run("plan", {"source": "demo:ecommerce", "table": "no_such_table"})
        self.assertEqual(st["status"], "error")
        self.assertIn("not found", st["error"])


if __name__ == "__main__":
    unittest.main()
