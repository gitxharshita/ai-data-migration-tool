"""Agent Manager: coordinates the pipeline through three verification stages.

1. request verification        - is the migration specification resolvable?
2. execution verification      - dry-run the mapping on sample rows, one
                                 bounded round of self-repair on failure
3. implementation verification - full reconciliation of the written files
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

import pyarrow as pa

from .connectors import SourceConnector, TargetConnector
from .executor import ExecutionResult, convert_rows, execute, output_schema, read_output
from .mappers import Mapper
from .models import MigrationPlan, ReconRule, RuleResult, Table, TableMapping, TargetSpec
from .reconcile import Reconciler, deterministic_rules, generic_rules, schema_aware_rules, summarise, typed_rules
from .typesys import (declared_family, is_valid_target_type, lossy_reason, parse_decimal_type,
                      profile_values)


class VerificationError(RuntimeError):
    pass


@dataclass
class MigrationReport:
    table: str
    strategy: str
    plan: MigrationPlan | None = None
    execution: ExecutionResult | None = None
    results: list[RuleResult] = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    log: list[str] = field(default_factory=list)
    error: str | None = None
    seconds: float = 0.0


def request_verification(source: SourceConnector, table_name: str, spec: TargetSpec) -> list[str]:
    errs = []
    if table_name not in source.list_tables():
        errs.append(f"source table {table_name!r} not found")
    if spec.fmt not in ("parquet", "csv"):
        errs.append(f"unsupported target format {spec.fmt!r}")
    if spec.partition_grain not in ("month", "day", "value"):
        errs.append(f"unsupported partition grain {spec.partition_grain!r}")
    return errs


def semantic_type(col, prof) -> str | None:
    """Best evidence-based guess of a column's true semantic type (for dry-run checks)."""
    shape = prof["shape"]
    if shape == "timestamp_tz":
        return "timestamp_tz"
    if shape == "json":
        return "json_string"
    if declared_family(col.declared_type) == "decimal" and shape == "decimal":
        return f"decimal(38,{prof['max_scale']})"
    return None


def execution_verification(table: Table, mapping: TableMapping, spec: TargetSpec) -> list[str]:
    errs = []
    sources = [cm.source for cm in mapping.columns]
    for c in table.columns:
        if sources.count(c.name) == 0:
            errs.append(f"source column {c.name!r} is not mapped")
        elif sources.count(c.name) > 1:
            errs.append(f"source column {c.name!r} is mapped more than once")
    targets = [cm.target for cm in mapping.columns]
    if len(set(targets)) != len(targets):
        errs.append("target field names are not unique")
    for cm in mapping.columns:
        if cm.source is None or table.column(cm.source) is None:
            errs.append(f"target field {cm.target!r} has no source column")
            continue
        if not is_valid_target_type(cm.target_type):
            errs.append(f"{cm.source}: unknown target type {cm.target_type!r}")
            continue
        col = table.column(cm.source)
        prof = profile_values([r.get(cm.source) for r in table.sample])
        sem = semantic_type(col, prof)
        if cm.transform == "mask":              # deliberate, user-approved privacy masking
            if cm.target_type != "string":
                errs.append(f"{cm.source}: a masked column must be stored as text")
            if prof["nulls"] and cm.null_handling != "preserve":
                errs.append(f"{cm.source}: sample contains NULLs but null_handling={cm.null_handling} rewrites them")
            continue
        if sem and cm.transform != "json_flatten":
            dec_s, dec_t = parse_decimal_type(sem), parse_decimal_type(cm.target_type)
            if dec_s and dec_t:
                if dec_t[1] < dec_s[1]:
                    errs.append(f"{cm.source}: sample values use {dec_s[1]} decimal places but target scale is {dec_t[1]}")
            elif dec_s and cm.target_type in ("float64", "string"):
                errs.append(f"{cm.source}: {lossy_reason('decimal(18,2)', cm.target_type)}")
            elif not dec_s and (why := lossy_reason(sem, cm.target_type)):
                errs.append(f"{cm.source}: samples look like {sem} but target is {cm.target_type} ({why})")
        if cm.transform == "json_flatten" and prof.get("nested"):
            errs.append(f"{cm.source}: json_flatten would drop nested objects present in the samples")
        if prof["nulls"] and cm.null_handling != "preserve":
            errs.append(f"{cm.source}: sample contains NULLs but null_handling={cm.null_handling} rewrites them")
    if spec.partition_by and table.column(spec.partition_by) is None:
        errs.append(f"partition column {spec.partition_by!r} does not exist")
    rows, conv_errs, _ = convert_rows(table.sample, mapping)
    errs += [f"dry-run conversion error: {e}" for e in conv_errs[:10]]
    if not conv_errs:
        try:
            pa.Table.from_pylist(rows, schema=output_schema(mapping, rows))
        except (pa.ArrowException, ValueError, TypeError) as e:
            errs.append(f"dry-run write error: {e}")
    return errs


class AgentManager:
    def __init__(self, mapper: Mapper, verify: bool = True, repair_rounds: int = 1,
                 rule_mode: str = "auto", sample_size: int = 20):
        self.mapper = mapper
        self.verify = verify
        self.repair_rounds = repair_rounds
        self.rule_mode = rule_mode    # auto | generic | deterministic | schema_aware
        self.sample_size = sample_size

    def build_rules(self, table, mapping, spec, proposed: list[ReconRule]) -> list[ReconRule]:
        mode = self.rule_mode
        if mode == "generic":
            return generic_rules(table, spec)
        if mode == "deterministic":
            return deterministic_rules(table, spec)
        if mode == "schema_aware" or not self.mapper.proposes_rules:
            return schema_aware_rules(table, mapping, spec)
        base = deterministic_rules(table, spec)
        have = {r.kind for r in base}
        return base + [r for r in proposed if r.kind not in have or r.column]

    def plan(self, source: SourceConnector, table_name: str, spec: TargetSpec, log: list[str]) -> MigrationPlan:
        errs = request_verification(source, table_name, spec)
        if errs:
            raise VerificationError("request verification failed: " + "; ".join(errs))
        log.append("request verification passed")
        table = source.describe(table_name, self.sample_size)
        mapping, proposed = self.mapper.map_table(table, spec)
        log.append(f"{self.mapper.name} proposed {len(mapping.columns)} column mappings, {len(proposed)} rules")
        if self.verify:
            for rnd in range(self.repair_rounds + 1):
                errs = execution_verification(table, mapping, spec)
                if not errs:
                    log.append("execution verification passed" + (f" after {rnd} repair round(s)" if rnd else ""))
                    break
                log.append(f"execution verification found {len(errs)} problem(s): " + " | ".join(errs[:5]))
                if rnd < self.repair_rounds:
                    mapping, proposed = self.mapper.repair(table, spec, mapping, proposed, errs)
                    log.append("self-repair round completed")
        return MigrationPlan(table, spec, mapping, self.build_rules(table, mapping, spec, proposed))

    def run(self, source: SourceConnector, table_name: str, spec: TargetSpec,
            target: TargetConnector, prefix: str | None = None) -> MigrationReport:
        t0 = time.perf_counter()
        rep = MigrationReport(table_name, self.mapper.name)
        try:
            rep.plan = self.plan(source, table_name, spec, rep.log)
            prefix = prefix or table_name
            rep.execution = execute(source, rep.plan.table, rep.plan.mapping, spec, target, prefix)
            rep.log.append(f"wrote {rep.execution.rows_written} rows to {len(rep.execution.files)} file(s)")
            rep.results, rep.summary = reconcile_written(source, rep.plan, target, prefix)
            rep.log.append(f"implementation verification: {rep.summary['status']} "
                           f"({rep.summary['failed']}/{rep.summary['rules']} rules failed)")
        except Exception as e:
            rep.error = f"{type(e).__name__}: {e}"
            rep.log.append("FAILED: " + rep.error)
        rep.seconds = time.perf_counter() - t0
        return rep


def reconcile_written(source: SourceConnector, plan: MigrationPlan, target: TargetConnector,
                      prefix: str, rules: list[ReconRule] | None = None):
    files = read_output(target, prefix, plan.target.fmt, plan.mapping)
    schema = None
    keys = [k for k, _ in files]
    if keys and plan.target.fmt == "parquet":
        import io
        import pyarrow.parquet as pq
        schema = pq.read_schema(io.BytesIO(target.read_bytes(keys[0])))
    src_rows = list(source.iter_rows_schema(plan.table))
    rec = Reconciler(plan.table, plan.mapping, plan.target, src_rows, files, schema)
    results = rec.run(rules if rules is not None else plan.rules)
    return results, summarise(results)
