"""Reconciliation summary report (Markdown for people, JSON for machines)."""
from __future__ import annotations

import json
from dataclasses import asdict

from .agent import MigrationReport


def _short(v, n=60):
    s = json.dumps(v, default=str) if not isinstance(v, str) else v
    return s if len(s) <= n else s[: n - 3] + "..."


def to_markdown(reports: list[MigrationReport]) -> str:
    out = ["# Migration reconciliation report", ""]
    out.append("| Table | Strategy | Rows written | Rules | Failed | Status |")
    out.append("|---|---|---:|---:|---:|---|")
    for r in reports:
        status = "ERROR" if r.error else r.summary.get("status", "?")
        out.append(f"| {r.table} | {r.strategy} | {r.execution.rows_written if r.execution else '-'} | "
                   f"{r.summary.get('rules', '-')} | {r.summary.get('failed', '-')} | **{status}** |")
    for r in reports:
        out += ["", f"## {r.table}", ""]
        out += [f"- {line}" for line in r.log]
        if r.plan:
            out += ["", "### Column mapping", "", "| Source | Target | Type | Transform | NULLs | Why |", "|---|---|---|---|---|---|"]
            for cm in r.plan.mapping.columns:
                out.append(f"| {cm.source} | {cm.target} | {cm.target_type} | {cm.transform} | {cm.null_handling} | {_short(cm.rationale, 70)} |")
        if r.results:
            out += ["", "### Reconciliation rules", "", "| Rule | Origin | Result | Source | Target | Detail |", "|---|---|---|---|---|---|"]
            for res in r.results:
                out.append(f"| {res.rule.rule_id} | {res.rule.origin} | {'pass' if res.passed else '**FAIL**'} | "
                           f"{_short(res.source_value, 30)} | {_short(res.target_value, 30)} | {_short(res.detail, 90)} |")
    return "\n".join(out) + "\n"


def to_json(reports: list[MigrationReport]) -> str:
    def enc(r: MigrationReport):
        return {
            "table": r.table, "strategy": r.strategy, "error": r.error, "seconds": round(r.seconds, 3),
            "summary": r.summary, "log": r.log,
            "mapping": r.plan.mapping.to_dict() if r.plan else None,
            "execution": asdict(r.execution) if r.execution else None,
            "results": [{"rule": asdict(x.rule), "passed": x.passed, "source": x.source_value,
                         "target": x.target_value, "detail": x.detail} for x in r.results],
        }
    return json.dumps([enc(r) for r in reports], indent=2, default=str)
