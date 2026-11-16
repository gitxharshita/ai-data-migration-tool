"""Validation and Reconciliation Engine.

Rules may be proposed by an LLM, but every value is recomputed here
deterministically from the source and the written files. An LLM-claimed
expected value is only compared against the recomputed one (to measure
Reconciliation Math Delusion); it is never trusted.
"""
from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from decimal import Decimal

import pyarrow as pa

from .executor import partition_label
from .models import ReconRule, RuleResult, Table, TableMapping, TargetSpec
from .typesys import arrow_type, canon, mask_value, parse_decimal_type


# --------------------------------------------------------------------------
# Rule generation
# --------------------------------------------------------------------------

def generic_rules(table: Table, spec: TargetSpec) -> list[ReconRule]:
    """What conventional ETL tools check: counts and schema only."""
    return [ReconRule("row_count", "row_count", rationale="total row count must match"),
            ReconRule("schema_conformity", "schema_conformity", rationale="written schema must match mapping")]


def deterministic_rules(table: Table, spec: TargetSpec) -> list[ReconRule]:
    rules = generic_rules(table, spec)
    if table.primary_key:
        rules.append(ReconRule("pk_unique", "pk_unique", table.primary_key,
                               rationale="primary key must stay unique (no duplicated rows)"))
    if spec.partition_by:
        rules.append(ReconRule("partition_row_count", "partition_row_count", spec.partition_by,
                               rationale="every partition must hold exactly the rows that belong to it"))
    return rules


def typed_rules(table: Table, mapping: TableMapping) -> list[ReconRule]:
    """Schema-specific checks derived from the mapping's target types."""
    rules = []
    for cm in mapping.columns:
        if not cm.source:
            continue
        col = table.column(cm.source)
        t, c = cm.target_type, cm.source
        if parse_decimal_type(t):
            rules += [ReconRule(f"sum_exact:{c}", "sum_exact", c, rationale="decimal total must match to the last digit"),
                      ReconRule(f"checksum:{c}", "column_checksum", c, rationale="every decimal value preserved exactly")]
        elif t == "timestamp_tz" or t == "timestamp":
            rules.append(ReconRule(f"timestamp_exact:{c}", "timestamp_exact", c,
                                   rationale="every instant preserved in UTC"))
        elif t == "json_string" or cm.transform == "json_flatten":
            rules.append(ReconRule(f"json_roundtrip:{c}", "json_roundtrip", c,
                                   rationale="JSON documents must round-trip unchanged"))
        elif t == "bool":
            rules.append(ReconRule(f"true_ratio:{c}", "true_ratio", c, rationale="true/false ratio preserved"))
        elif t in ("int64", "float64", "date"):
            rules.append(ReconRule(f"min_max:{c}", "min_max", c, rationale="value range preserved"))
        elif t == "string":
            rules.append(ReconRule(f"distinct:{c}", "distinct_count", c, rationale="set of distinct values preserved"))
        if col is not None and col.nullable:
            rules.append(ReconRule(f"null_count:{c}", "null_count", c,
                                   rationale="NULLs must stay NULL (no empty-string/zero substitution)"))
    for r in rules:
        r.origin = "generated"
    return rules


def schema_aware_rules(table: Table, mapping: TableMapping, spec: TargetSpec) -> list[ReconRule]:
    return deterministic_rules(table, spec) + typed_rules(table, mapping)


# --------------------------------------------------------------------------
# Rule execution
# --------------------------------------------------------------------------

def _digest(values) -> str:
    h = hashlib.sha256()
    for v in sorted("\x00NULL" if v is None else repr(v) for v in values):
        h.update(v.encode())
        h.update(b"\n")
    return h.hexdigest()[:16]


class Reconciler:
    def __init__(self, table: Table, mapping: TableMapping, spec: TargetSpec,
                 source_rows: list[dict], files: list[tuple[str, list[dict]]],
                 written_schema: pa.Schema | None = None):
        self.table, self.mapping, self.spec = table, mapping, spec
        self.src = source_rows
        self.files = files
        self.tgt = [r for _, rows in files for r in rows]
        self.written_schema = written_schema

    # helpers ---------------------------------------------------------------
    def _cm(self, col):
        return self.mapping.for_source(col)

    def _src_vals(self, col):
        cm = self._cm(col)
        t = cm.target_type if cm else "string"
        if cm and cm.transform == "mask":       # compare masked source with masked output
            return [canon(mask_value(r.get(col)), t) for r in self.src]
        return [canon(r.get(col), t) for r in self.src]

    def _tgt_vals(self, col):
        cm = self._cm(col)
        if cm is None:
            return None
        if cm.transform == "json_flatten":
            return None
        if self.tgt and cm.target not in self.tgt[0]:
            return None
        return [canon(r.get(cm.target), cm.target_type) for r in self.tgt]

    # rule kinds ------------------------------------------------------------
    def _row_count(self, r):
        return len(self.src), len(self.tgt)

    def _pk_unique(self, r):
        vals = self._tgt_vals(r.column) or []
        return len(set(self._src_vals(r.column))), len(set(vals)) if len(set(vals)) == len(vals) else f"{len(vals) - len(set(vals))} duplicate keys"

    def _partition_row_count(self, r):
        exp = Counter(partition_label(row, self.spec) for row in self.src)
        got = Counter()
        for key, rows in self.files:
            label = next((p for p in key.split("/") if p.startswith(self.spec.partition_by + "=")), None)
            got[label] += len(rows)
        return dict(sorted(exp.items())), dict(sorted(got.items()))

    def _schema_conformity(self, r):
        exp = {}
        for cm in self.mapping.columns:
            if cm.transform != "json_flatten":
                exp[cm.target] = str(arrow_type(cm.target_type))
        if self.written_schema is not None:
            got = {f.name: str(f.type) for f in self.written_schema}
        else:
            got = {k: exp.get(k, "?") for k in (self.tgt[0] if self.tgt else exp)}
        return exp, {k: got.get(k, "<missing>") for k in exp} | {k: v for k, v in got.items() if k not in exp}

    def _null_count(self, r):
        t = self._tgt_vals(r.column)
        return sum(v is None for v in self._src_vals(r.column)), None if t is None else sum(v is None for v in t)

    def _column_checksum(self, r):
        t = self._tgt_vals(r.column)
        return _digest(self._src_vals(r.column)), None if t is None else _digest(t)

    def _sum_exact(self, r):
        def tot(vals):
            return None if vals is None else str(sum((Decimal(v) for v in vals if v is not None), Decimal(0)).normalize())
        return tot(self._src_vals(r.column)), tot(self._tgt_vals(r.column))

    def _min_max(self, r):
        def mm(vals):
            if vals is None:
                return None
            v = [x for x in vals if x is not None]
            if not v:
                return (None, None)
            key = (lambda x: Decimal(x)) if all(_is_num(x) for x in v) else str
            return (min(v, key=key), max(v, key=key))
        return mm(self._src_vals(r.column)), mm(self._tgt_vals(r.column))

    def _distinct_count(self, r):
        t = self._tgt_vals(r.column)
        return len(set(self._src_vals(r.column))), None if t is None else len(set(t))

    def _true_ratio(self, r):
        def ratio(vals):
            if vals is None:
                return None
            v = [x for x in vals if x is not None]
            return round(sum(1 for x in v if x is True) / len(v), 6) if v else 0.0
        return ratio(self._src_vals(r.column)), ratio(self._tgt_vals(r.column))

    def _timestamp_exact(self, r):
        t = self._tgt_vals(r.column)
        return _digest(self._src_vals(r.column)), None if t is None else _digest(t)

    def _json_roundtrip(self, r):
        return self._column_checksum(r)

    def run_rule(self, r: ReconRule) -> RuleResult:
        fn = getattr(self, "_" + r.kind, None)
        if fn is None:
            return RuleResult(r, False, detail=f"unknown rule kind {r.kind}")
        if r.column and r.kind not in ("partition_row_count",) and self.table.column(r.column) is None:
            return RuleResult(r, False, detail=f"rule references unknown column {r.column}")
        try:
            s, t = fn(r)
        except Exception as e:  # a broken rule is a failed rule, never a silent pass
            return RuleResult(r, False, detail=f"rule error: {e}")
        if t is None:
            return RuleResult(r, False, s, t, "target column missing or restructured")
        if r.tolerance and isinstance(s, (int, float)) and isinstance(t, (int, float)):
            passed = abs(s - t) <= r.tolerance * max(abs(s), 1)
        else:
            passed = s == t
        detail = "" if passed else _explain(r, s, t)
        if r.claimed_source_value is not None and str(r.claimed_source_value) != str(s):
            detail = (detail + "; " if detail else "") + \
                f"LLM-claimed source value {r.claimed_source_value!r} != recomputed {s!r}"
        return RuleResult(r, passed, s, t, detail)

    def run(self, rules: list[ReconRule]) -> list[RuleResult]:
        return [self.run_rule(r) for r in rules]


def _is_num(x) -> bool:
    try:
        Decimal(x)
        return True
    except Exception:
        return False


def _explain(r: ReconRule, s, t) -> str:
    if r.kind == "partition_row_count":
        diff = {k: (s.get(k, 0), t.get(k, 0)) for k in set(s) | set(t) if s.get(k, 0) != t.get(k, 0)}
        return f"partition counts differ (expected, actual): {diff}"
    if r.kind == "schema_conformity":
        diff = {k: (s.get(k), t.get(k)) for k in set(s) | set(t) if s.get(k) != t.get(k)}
        return f"schema differs (expected, actual): {diff}"
    return f"source={s!r} target={t!r}"


def summarise(results: list[RuleResult]) -> dict:
    failed = [r for r in results if not r.passed]
    by_col = defaultdict(list)
    for r in failed:
        by_col[r.rule.column or "<table>"].append(r.rule.kind)
    return {"status": "PASS" if not failed else "FAIL", "rules": len(results),
            "failed": len(failed), "failed_by_column": dict(by_col)}
