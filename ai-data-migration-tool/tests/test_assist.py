"""AI assistant features (amt/assist.py) with a stand-in AI: no model needed."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.assist import (apply_overrides, find_sensitive, merge_sensitive, migration_facts,  # noqa: E402
                        review_mapping, summarise_migration)
from amt.models import Column, ColumnMapping, Table, TableMapping  # noqa: E402


class StubAI:
    model = "stub"

    def __init__(self, reply):
        self.reply = reply

    def complete(self, *a, **k):
        return json.dumps(self.reply)


def table():
    rows = [{"id": i, "email": f"user{i}@example.com", "notes": f"call me on 98765432{i:02d}",
             "is_vip": i % 2, "amount": "12.50"} for i in range(8)]
    return Table("customers", [Column("id", "INTEGER", False, True), Column("email", "TEXT"),
                               Column("notes", "TEXT"), Column("is_vip", "INTEGER"), Column("amount", "TEXT")],
                 sample=rows, row_count=300)


def mapping():
    return TableMapping("customers", [ColumnMapping("id", "id", "int64"), ColumnMapping("email", "email", "string"),
                                      ColumnMapping("notes", "notes", "string"),
                                      ColumnMapping("is_vip", "is_vip", "int64"),
                                      ColumnMapping("amount", "amount", "decimal(10,2)")])


class PrivacyTest(unittest.TestCase):
    def test_rules_find_names_and_values(self):
        found = {s["column"]: s["kind"] for s in find_sensitive(table())}
        self.assertEqual(found.get("email"), "email address")      # by column name
        self.assertEqual(found.get("notes"), "phone number")       # phone numbers hidden in free text
        self.assertNotIn("id", found)
        self.assertNotIn("amount", found)

    def test_ai_adds_only_new_columns(self):
        merged = merge_sensitive([{"column": "email", "kind": "email address", "reason": "", "found_by": "rules"}],
                                 [{"column": "email", "kind": "x", "reason": "", "found_by": "AI"},
                                  {"column": "notes", "kind": "phone", "reason": "", "found_by": "AI"}])
        self.assertEqual([m["column"] for m in merged], ["email", "notes"])


class ReviewTest(unittest.TestCase):
    def test_only_valid_new_suggestions_survive(self):
        ai = StubAI({"flags": [
            {"column": "is_vip", "suggested_type": "bool", "suggested_transform": "int_to_bool",
             "suggested_null_handling": "preserve", "reason": "Only 0 and 1: a yes/no flag."},
            {"column": "does_not_exist", "suggested_type": "bool", "reason": "invented column"},
            {"column": "amount", "suggested_type": "money", "reason": "not a real type"},
            {"column": "id", "suggested_type": "int64", "suggested_transform": "none",
             "suggested_null_handling": "preserve", "reason": "same as now"}],
            "sensitive": [{"column": "notes", "kind": "phone number", "reason": "free text with phones"},
                          {"column": "ghost", "kind": "email", "reason": "no such column"}]})
        r = review_mapping(ai, table(), mapping())
        self.assertEqual([f["column"] for f in r["flags"]], ["is_vip"])
        self.assertEqual(r["flags"][0]["suggested"]["target_type"], "bool")
        self.assertEqual([s["column"] for s in r["sensitive"]], ["notes"])

    def test_accepting_a_suggestion_changes_the_mapping(self):
        m = mapping()
        apply_overrides(m, {"is_vip": {"target_type": "bool", "transform": "int_to_bool", "null_handling": "preserve"}})
        self.assertEqual((m.for_source("is_vip").target_type, m.for_source("is_vip").transform), ("bool", "int_to_bool"))
        with self.assertRaises(ValueError):
            apply_overrides(m, {"amount": {"target_type": "money"}})


class MaskingTest(unittest.TestCase):
    def test_mask_values(self):
        from amt.typesys import mask_value
        self.assertEqual(mask_value("ravi.k@example.com"), "r***@example.com")
        self.assertEqual(mask_value("+91 98765 43210"), "********3210")
        self.assertEqual(mask_value("1990-05-12"), "1990-**-**")
        self.assertEqual(mask_value("Ravi Kumar"), "R*** K***")
        self.assertEqual(mask_value("please call me on 9876543210 or mail ravi@x.com"),
                         "please call me on ******3210 or mail r***@x.com")
        self.assertIsNone(mask_value(None))
        self.assertEqual(mask_value("a@b.co"), mask_value("a@b.co"))     # deterministic

    def test_id_and_partition_columns_cannot_be_masked(self):
        from amt.assist import apply_masks
        with self.assertRaises(ValueError):
            apply_masks(mapping(), table(), ["id"], None)
        m = mapping()
        self.assertEqual(apply_masks(m, table(), ["email", "notes"], None), ["email", "notes"])
        self.assertEqual((m.for_source("email").target_type, m.for_source("email").transform), ("string", "mask"))

    def test_masked_migration_passes_all_checks_and_hides_data(self):
        import tempfile
        from amt.agent import AgentManager, execution_verification, reconcile_written
        from amt.assist import apply_masks
        from amt.connectors.sources import SQLiteSource
        from amt.connectors.targets import LocalTarget
        from amt.executor import execute, read_output
        from amt.mappers import SampleAwareMapper
        from amt.retriever import PrecedentStore
        from eval import corpus
        work = Path(tempfile.mkdtemp())
        corpus.build_schema_db("ecommerce", work / "e.db", 7)
        src = SQLiteSource(str(work / "e.db"))
        ts = next(t for s, t in corpus.all_tables() if t.name == "customers")
        spec = corpus.target_spec(ts, str(work / "lake"))
        agent = AgentManager(SampleAwareMapper(PrecedentStore()), verify=True)
        plan = agent.plan(src, "customers", spec, [])
        apply_masks(plan.mapping, plan.table, ["email", "full_name"], spec.partition_by)
        self.assertEqual(execution_verification(plan.table, plan.mapping, spec), [])
        plan.rules = agent.build_rules(plan.table, plan.mapping, spec, [])
        target = LocalTarget(str(work / "lake"))
        execute(src, plan.table, plan.mapping, spec, target, "customers")
        results, summary = reconcile_written(src, plan, target, "customers")
        self.assertEqual(summary["status"], "PASS", [r.detail for r in results if not r.passed])
        rows = [r for _, rs in read_output(target, "customers", spec.fmt, plan.mapping) for r in rs]
        self.assertTrue(all("***@" in r["email"] for r in rows if r["email"]))
        self.assertTrue(all("***" in r["full_name"] for r in rows if r["full_name"]))


class SummaryTest(unittest.TestCase):
    def facts(self):
        res = [{"rule": {"kind": "row_count", "column": None}, "passed": True}]
        return migration_facts("orders", 300, 300, 12, "a local folder", {"rules": 12, "failed": 0, "status": "PASS"},
                               res, ["placed_at"], ["email"])

    def test_numbers_checked_against_facts(self):
        ok = summarise_migration(StubAI({"summary": "All 300 orders were copied into 12 files and all 12 checks passed."}),
                                 self.facts())
        self.assertEqual(ok["by"], "AI")

    def test_invented_number_falls_back_to_template(self):
        bad = summarise_migration(StubAI({"summary": "All 310 orders were copied and 13 checks passed."}), self.facts())
        self.assertEqual(bad["by"], "template")
        self.assertIn("300 rows", bad["text"])
        self.assertIn("310", bad["note"])


if __name__ == "__main__":
    unittest.main()
