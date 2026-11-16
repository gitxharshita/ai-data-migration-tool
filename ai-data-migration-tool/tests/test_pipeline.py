"""Run with:  python -m unittest discover -s tests -v"""
import json
import random
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.agent import AgentManager, execution_verification, reconcile_written
from amt.connectors import LocalTarget, SQLiteSource
from amt.executor import ConversionError, convert_value, read_output
from amt.llm import extract_json
from amt.mappers import DeclaredTypeMapper, SampleAwareMapper, parse_response
from amt.models import ColumnMapping, MigrationPlan, TableMapping
from amt.reconcile import schema_aware_rules
from amt.retriever import PrecedentStore
from amt.typesys import canon, lossy_reason, profile_values
from eval import corpus
from eval.inject import inject


class TypeSystemTests(unittest.TestCase):
    def test_profile_detects_hidden_types(self):
        self.assertEqual(profile_values([0, 1, 1, 0])["shape"], "binary_int")
        self.assertEqual(profile_values(['{"a": 1}', '{"b": {"c": 2}}'])["shape"], "json")
        self.assertEqual(profile_values(["2024-01-01T10:00:00+05:30"])["shape"], "timestamp_tz")
        self.assertEqual(profile_values([Decimal("1.25"), Decimal("3.5")])["max_scale"], 2)

    def test_lossiness(self):
        self.assertIsNotNone(lossy_reason("decimal(18,2)", "float64"))
        self.assertIsNotNone(lossy_reason("timestamp_tz", "timestamp"))
        self.assertIsNone(lossy_reason("decimal(18,2)", "decimal(20,2)"))

    def test_canon_compares_instants(self):
        self.assertEqual(canon("2024-01-01T10:00:00+05:30", "timestamp_tz"),
                         canon(datetime(2024, 1, 1, 4, 30, tzinfo=timezone.utc), "timestamp_tz"))

    def test_convert_value(self):
        cm = ColumnMapping("x", "x", "decimal(18,2)")
        self.assertEqual(convert_value("12.345", cm), Decimal("12.34"))  # banker's rounding, visible loss
        with self.assertRaises(ConversionError):
            convert_value("abc", ColumnMapping("x", "x", "int64"))
        self.assertEqual(convert_value(None, ColumnMapping("x", "x", "string", null_handling="empty_string")), "")


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        db = Path(cls.tmp.name) / "shop.db"
        corpus.build_schema_db("ecommerce", db)
        cls.src = SQLiteSource(str(db))
        cls.target = LocalTarget(str(Path(cls.tmp.name) / "lake"))

    @classmethod
    def tearDownClass(cls):
        cls.src.close()
        cls.tmp.cleanup()

    def spec(self, name):
        ts = next(t for t in corpus.SCHEMAS["ecommerce"] if t.name == name)
        return ts, corpus.target_spec(ts, "")

    def test_sample_aware_mapper_matches_gold_on_carts(self):
        ts, spec = self.spec("carts")
        table = self.src.describe("carts")
        mapping, _ = SampleAwareMapper().map_table(table, spec)
        gold = corpus.gold_mapping(ts)
        for g in gold.columns:
            self.assertEqual(mapping.for_source(g.source).target_type, g.target_type, g.source)

    def test_declared_mapper_misses_hidden_types_and_verification_flags_it(self):
        ts, spec = self.spec("carts")
        table = self.src.describe("carts")
        mapping, _ = DeclaredTypeMapper().map_table(table, spec)
        self.assertEqual(mapping.for_source("cart_total").target_type, "float64")
        errs = execution_verification(table, mapping, spec)
        self.assertTrue(any("cart_total" in e for e in errs))

    def test_end_to_end_migration_passes_reconciliation(self):
        for name in ("orders", "carts", "order_items"):
            ts, spec = self.spec(name)
            rep = AgentManager(SampleAwareMapper(), verify=True).run(self.src, name, spec, self.target)
            self.assertIsNone(rep.error, rep.log)
            self.assertEqual(rep.summary["status"], "PASS", [r.detail for r in rep.results if not r.passed])

    def test_injected_discrepancies_are_detected(self):
        ts, spec = self.spec("orders")
        table = self.src.describe("orders")
        gold = corpus.gold_mapping(ts)
        rep = AgentManager(SampleAwareMapper(), verify=True).run(self.src, "orders", spec, self.target, "o")
        files = read_output(self.target, "o", "parquet")
        rules = schema_aware_rules(table, gold, spec)
        from amt.reconcile import Reconciler
        src_rows = list(self.src.iter_rows_schema(table))
        for kind in ("truncated_decimal", "duplicated_row", "timezone_shift", "off_by_one_partition"):
            edited = inject(kind, files, gold, True, random.Random(1))
            res = Reconciler(table, gold, spec, src_rows, edited).run(rules)
            self.assertTrue(any(not r.passed for r in res), kind)

    def test_llm_response_parsing(self):
        text = 'Sure:\n{"columns":[{"source":"id","target":"id","target_type":"int64"}],' \
               '"rules":[{"kind":"row_count"},{"kind":"bogus"}]} trailing'
        table = self.src.describe("customers")
        mapping, rules = parse_response(table, extract_json(text))
        self.assertEqual(mapping.columns[0].target_type, "int64")
        self.assertEqual([r.kind for r in rules], ["row_count"])

    def test_retriever_prefers_similar_columns(self):
        store = PrecedentStore()
        store.add({"schema": "a", "table": "t", "column": "is_paid", "declared_type": "SMALLINT",
                   "profile": {"shape": "binary_int"}, "mapping": {"target_type": "bool", "transform": "int_to_bool", "null_handling": "preserve"}})
        store.add({"schema": "a", "table": "t", "column": "invoice_note", "declared_type": "TEXT",
                   "profile": {"shape": "text"}, "mapping": {"target_type": "string", "transform": "none", "null_handling": "preserve"}})
        table = self.src.describe("payments")
        hits = store.search(table.column("is_refunded"), [r["is_refunded"] for r in table.sample], k=1)
        self.assertEqual(hits[0][1]["column"], "is_paid")


if __name__ == "__main__":
    unittest.main()
