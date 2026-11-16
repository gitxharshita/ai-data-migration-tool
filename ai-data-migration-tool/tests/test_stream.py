"""Streaming engine (#1), job files (#2) and lakehouse formats (#3)."""
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from amt.agent import AgentManager  # noqa: E402
from amt.connectors.sources import SQLiteSource  # noqa: E402
from amt.connectors.targets import LocalTarget  # noqa: E402
from amt.mappers import SampleAwareMapper  # noqa: E402
from amt.pipeline import JobError, load_job, run_job  # noqa: E402
from amt.retriever import PrecedentStore  # noqa: E402
from amt.stream import DeltaSink, FileSink, IcebergSink, run_stream  # noqa: E402
from eval import corpus  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import make_big_table  # noqa: E402


def plan_for(src, name):
    ts = next(t for s, t in corpus.all_tables() if t.name == name)
    spec = corpus.target_spec(ts, "")
    plan = AgentManager(SampleAwareMapper(PrecedentStore()), verify=True).plan(src, name, spec, [])
    return plan, spec


class StreamEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work = Path(tempfile.mkdtemp())
        corpus.build_schema_db("ecommerce", cls.work / "e.db", 7)
        cls.src = SQLiteSource(str(cls.work / "e.db"))

    def test_all_formats_pass_with_database_crosscheck(self):
        plan, spec = plan_for(self.src, "orders")
        sinks = [FileSink(LocalTarget(str(self.work / "lake")), "orders", spec, "r1"),
                 DeltaSink(str(self.work / "delta" / "orders"), spec, "r1"),
                 IcebergSink({"type": "sql", "uri": f"sqlite:///{(self.work / 'cat.db').as_posix()}",
                              "warehouse": (self.work / "wh").as_posix()}, "lake.orders", spec, "r1")]
        for sink in sinks:
            rep = run_stream(self.src, plan.table, plan.mapping, spec, sink, plan.rules, run_id="r1")
            self.assertEqual(rep.summary["status"], "PASS", type(sink).__name__)
            self.assertEqual(rep.rows_written, 300)
            self.assertTrue(rep.crosscheck and all(r.passed for r in rep.crosscheck))
            self.assertEqual(len(rep.partitions), 12)

    def test_one_cent_change_is_pinned_to_its_partition_and_repaired(self):
        plan, spec = plan_for(self.src, "orders")
        target = LocalTarget(str(self.work / "lake2"))
        run_stream(self.src, plan.table, plan.mapping, spec, FileSink(target, "orders", spec, "a"), plan.rules)
        key = next(k for k in target.list("orders") if "placed_at=2024-03" in k)
        t = pq.ParquetFile(self.work / "lake2" / key).read()
        rows = t.to_pylist()
        rows[0]["total_amount"] += Decimal("0.01")
        pq.write_table(pa.Table.from_pylist(rows, schema=t.schema), self.work / "lake2" / key)
        rep = run_stream(self.src, plan.table, plan.mapping, spec, FileSink(target, "orders", spec, "b"),
                         plan.rules, verify_only=True)
        self.assertEqual(rep.summary["status"], "FAIL")
        self.assertEqual(rep.failed_partitions, ["placed_at=2024-03"])
        fix = run_stream(self.src, plan.table, plan.mapping, spec, FileSink(target, "orders", spec, "c"),
                         plan.rules, mode="partitions", partitions={"placed_at=2024-03"})
        self.assertEqual(fix.summary["status"], "PASS")
        self.assertLess(fix.rows_written, 300)                 # only that month was rewritten
        again = run_stream(self.src, plan.table, plan.mapping, spec, FileSink(target, "orders", spec, "d"),
                           plan.rules, verify_only=True)
        self.assertEqual(again.summary["status"], "PASS")


class JobTest(unittest.TestCase):
    def setUp(self):
        self.work = Path(tempfile.mkdtemp())
        self.db = self.work / "shop.db"
        con = sqlite3.connect(self.db)
        make_big_table.create(con, 3000, 300, 7)
        con.close()

    def tearDown(self):
        shutil.rmtree(self.work, ignore_errors=True)

    def job(self, fmt="parquet", mode="append", extra=""):
        w = self.work.as_posix()
        target = "" if fmt == "iceberg" else f"target: {w}/out_{fmt}\n"
        cat = (f"catalog: {{type: sql, uri: 'sqlite:///{w}/cat.db', warehouse: '{w}/wh', namespace: shop}}\n"
               if fmt == "iceberg" else "")
        p = self.work / f"job_{fmt}.yaml"
        p.write_text(f"source: sqlite:///{self.db.as_posix()}\n{target}format: {fmt}\n{cat}{extra}tables:\n"
                     f"  - name: customers\n    mask: [email]\n"
                     f"  - name: orders\n    partition_by: updated_at\n"
                     f"    incremental: {{column: updated_at, mode: {mode}}}\n", encoding="utf-8")
        return load_job(str(p))

    def run_quiet(self, job, **kw):
        return run_job(job, log=lambda m: None, **kw)

    def test_full_then_incremental_then_nothing_new(self):
        job = self.job()
        code, out, rep = self.run_quiet(job)
        self.assertEqual(code, 0)
        self.assertEqual([o.table for o in out], ["customers", "orders"])     # parents first
        self.assertEqual({o.status for o in out}, {"PASS"})
        con = sqlite3.connect(self.db)
        make_big_table.insert_orders(con, 3001, 40, 300, __import__("random").Random(1))
        con.execute("UPDATE orders SET updated_at = '2099-01-01 00:00:00' WHERE id > 3000")
        con.commit()
        con.close()
        code, out, _ = self.run_quiet(job)
        orders = next(o for o in out if o.table == "orders")
        self.assertEqual((code, orders.status, orders.mode, orders.rows_written), (0, "PASS", "append", 40))
        code, out, _ = self.run_quiet(job)
        self.assertEqual(next(o for o in out if o.table == "orders").status, "SKIPPED")
        self.assertTrue((Path(job["report_dir"]) / "last_run.json").exists())
        state = json.loads(Path(job["state_file"]).read_text())
        self.assertEqual(state["orders"]["watermark"]["v"], "2099-01-01 00:00:00")

    def test_masked_column_in_output(self):
        job = self.job()
        self.run_quiet(job)
        emails = [r["email"] for f in (self.work / "out_parquet" / "customers").rglob("*.parquet")
                  for r in pq.ParquetFile(f).read().to_pylist()]
        self.assertTrue(emails and all("***@" in e for e in emails))

    def test_delta_upsert_replaces_changed_rows(self):
        job = self.job("delta", "upsert")
        self.assertEqual(self.run_quiet(job)[0], 0)
        con = sqlite3.connect(self.db)
        con.execute("UPDATE orders SET status = 'returned', updated_at = '2099-01-01 00:00:00' WHERE id <= 25")
        con.commit()
        con.close()
        code, out, _ = self.run_quiet(job)
        self.assertEqual((code, next(o for o in out if o.table == "orders").mode), (0, "upsert"))
        from deltalake import DeltaTable
        t = DeltaTable(str(self.work / "out_delta" / "orders")).to_pyarrow_table(columns=["id", "status"])
        self.assertEqual(t.num_rows, 3000)                                        # no duplicates
        self.assertEqual(sum(1 for s in t.column("status").to_pylist() if s == "returned") >= 25, True)
        self.assertEqual(self.run_quiet(job, verify=True)[0], 0)

    def test_iceberg_job(self):
        code, out, _ = self.run_quiet(self.job("iceberg"))
        self.assertEqual(code, 0, [o.message for o in out])

    def test_exit_code_1_on_damage_and_rerun_failed_repairs(self):
        job = self.job()
        self.run_quiet(job)
        f = next((self.work / "out_parquet" / "orders").rglob("*.parquet"))
        t = pq.ParquetFile(f).read()
        pq.write_table(t.slice(1), f)                                            # one row lost
        code, out, _ = self.run_quiet(job, verify=True, only=["orders"])
        self.assertEqual(code, 1)
        self.assertEqual(len(out[0].failed_partitions), 1)
        code, out, _ = self.run_quiet(job, rerun_failed=True, only=["orders"])
        self.assertEqual((code, out[0].mode), (0, "partitions"))
        self.assertEqual(self.run_quiet(job, verify=True, only=["orders"])[0], 0)

    def test_bad_job_files_explain_themselves(self):
        p = self.work / "bad.yaml"
        for text, words in [("source: x\ntables: [a]\nformat: excel\n", "format must be"),
                            ("source: x\ntables: [a]\ntarget: y\nspeed: fast\n", "Unknown setting"),
                            ("source: x\ntarget: y\ntables:\n  - name: a\n    incremental: {mode: append}\n", "needs a column"),
                            ("source: x\ntarget: y\ntables:\n  - name: a\n    incremental: {column: u, mode: upsert}\n",
                             "needs format: delta"),
                            ("source: x\nformat: iceberg\ntables: [a]\n", "needs a catalog")]:
            p.write_text(text, encoding="utf-8")
            with self.assertRaises(JobError) as cm:
                load_job(str(p))
            self.assertIn(words, str(cm.exception))

    def test_large_table_sampling_uses_offsets(self):
        import amt.connectors.sources as s
        old, s.LARGE_TABLE = s.LARGE_TABLE, 100
        try:
            t = SQLiteSource(str(self.db)).describe("orders", 20)
        finally:
            s.LARGE_TABLE = old
        self.assertEqual(len(t.sample), 20)
        ids = [r["id"] for r in t.sample]
        self.assertEqual(ids, sorted(ids))
        self.assertGreater(ids[-1], 2500)                                        # spread over the whole table


if __name__ == "__main__":
    unittest.main()
