"""Evaluation metrics (Section III-F): MVR, Mapping Accuracy, Reconciliation
Fidelity, hallucination rate by category, and the Composite Migration Score."""
from __future__ import annotations

from amt.models import ReconRule, Table, TableMapping
from amt.typesys import declared_decimal_precision, lossy_reason, parse_decimal_type

VALUE_CHANGING = {"json_flatten", "trim", "lower"}

HALLUCINATIONS = [
    ("mapping_contradiction", "Mapping Contradiction", "schema"),
    ("type_overanalysis", "Type Overanalysis", "schema"),
    ("null_convention", "Null-Convention Misrepresentation", "schema"),
    ("redundant_transformation", "Redundant Transformation", "logic"),
    ("trivial_rule", "Trivial-Rule Abuse", "logic"),
    ("math_delusion", "Reconciliation Math Delusion", "logic"),
]


def decimal_faithful(pred_t: str, gold_t: str, declared: str | None = None) -> bool:
    """A decimal target is faithful if it keeps at least the gold scale and at
    least as many integer digits as the source column can hold (e.g. SQL Server
    MONEY -> decimal(19,4) is faithful for a gold decimal(18,2))."""
    pd, gd = parse_decimal_type(pred_t), parse_decimal_type(gold_t)
    if not (pd and gd):
        return False
    need_int = gd[0] - gd[1]
    src = declared_decimal_precision(declared) if declared else None
    if src:
        need_int = min(need_int, src[0] - src[1])
    return pd[1] >= gd[1] and pd[0] - pd[1] >= need_int


def column_correct(pred, gold, declared: str | None = None) -> bool:
    type_ok = pred.target_type == gold.target_type or decimal_faithful(pred.target_type, gold.target_type, declared)
    return (pred.target.lower() == gold.target.lower()
            and type_ok
            and pred.null_handling == gold.null_handling
            and (pred.transform not in VALUE_CHANGING or pred.transform == gold.transform))


def mapping_counts(pred: TableMapping, gold: TableMapping, table: Table | None = None) -> tuple[int, int, int]:
    """(true positives, predicted columns, gold columns)."""
    tp = 0
    for g in gold.columns:
        p = pred.for_source(g.source)
        col = table.column(g.source) if table is not None else None
        if p is not None and column_correct(p, g, col.declared_type if col else None):
            tp += 1
    return tp, len(pred.columns), len(gold.columns)


def prf(tp, n_pred, n_gold):
    p = tp / n_pred if n_pred else 0.0
    r = tp / n_gold if n_gold else 0.0
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def _lossy(gold_t: str, pred_t: str, declared: str | None = None) -> bool:
    gd, pd = parse_decimal_type(gold_t), parse_decimal_type(pred_t)
    if gd and pd:
        return not decimal_faithful(pred_t, gold_t, declared)
    return gold_t != pred_t and lossy_reason(gold_t, pred_t) is not None


def hallucinations(table: Table, pred: TableMapping, gold: TableMapping, rules: list[ReconRule],
                   rule_results=None) -> dict[str, bool]:
    gold_src = {g.source: g for g in gold.columns}
    h = {k: False for k, _, _ in HALLUCINATIONS}
    for p in pred.columns:
        g = gold_src.get(p.source)
        if g is None:
            h["type_overanalysis"] = True
            continue
        col = table.column(p.source)
        if _lossy(g.target_type, p.target_type, col.declared_type if col else None):
            h["mapping_contradiction"] = True
        if p.null_handling != g.null_handling:
            h["null_convention"] = True
        if p.transform in VALUE_CHANGING and p.transform != g.transform:
            h["redundant_transformation"] = True
    for r in rules:
        col = table.column(r.column) if r.column else None
        if r.tolerance >= 0.5 or (r.kind == "row_count" and r.tolerance > 0):
            h["trivial_rule"] = True
        if r.kind == "null_count" and col is not None and not col.nullable:
            h["trivial_rule"] = True
    for res in rule_results or []:
        if "LLM-claimed source value" in res.detail:
            h["math_delusion"] = True
    return h


def cms(mvr, ma_f1, rf_f1, mean_halluc):
    return 0.3 * mvr + 0.3 * ma_f1 + 0.3 * rf_f1 + 0.1 * (1 - mean_halluc)
