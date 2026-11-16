"""Schema Mapper strategies.

LLM strategies compared in the paper (Section III-D):
  S1  ZeroShotMapper   - column names + declared types only
  S2  FineTunedMapper  - same prompt as S1, served by a LoRA-fine-tuned model
  S3  RAGMapper        - schema + sample rows block, separate precedent block;
                         paired with execution-verification self-repair

Non-LLM reference baselines (run fully offline, no API key needed):
  H1  DeclaredTypeMapper  - fixed rules on declared SQL types
  H2  SampleAwareMapper   - H1 refined with sample-value profiles and a
                            vote over retrieved precedent mappings
"""
from __future__ import annotations

import json
from collections import Counter

from .llm import LLMClient, LLMError, extract_json
from .models import ColumnMapping, ReconRule, Table, TableMapping, TargetSpec, RULE_KINDS
from .retriever import PrecedentStore
from .typesys import declared_decimal_precision, declared_family, describe_shape, profile_values


class Mapper:
    name = "base"
    uses_llm = False
    proposes_rules = False

    def map_table(self, table: Table, spec: TargetSpec) -> tuple[TableMapping, list[ReconRule]]:
        raise NotImplementedError

    def repair(self, table: Table, spec: TargetSpec, mapping: TableMapping,
               rules: list[ReconRule], errors: list[str]) -> tuple[TableMapping, list[ReconRule]]:
        return mapping, rules


# --------------------------------------------------------------------------
# Rule-based baselines
# --------------------------------------------------------------------------

def declared_mapping(col) -> ColumnMapping:
    fam = declared_family(col.declared_type)
    t, tr = {
        "int": ("int64", "none"), "float": ("float64", "none"), "bool": ("bool", "none"),
        "timestamp_tz": ("timestamp_tz", "tz_to_utc"), "timestamp": ("timestamp", "none"),
        "date": ("date", "parse_date"), "json": ("json_string", "json_canonical"),
        "enum": ("string", "none"), "string": ("string", "none"),
    }.get(fam, ("string", "none"))
    if fam == "decimal":
        p = declared_decimal_precision(col.declared_type)
        t, tr = (f"decimal({p[0]},{p[1]})", "none") if p else ("float64", "none")
    return ColumnMapping(col.name, col.name, t, tr, "preserve", f"declared type {col.declared_type}")


class DeclaredTypeMapper(Mapper):
    name = "H1-declared"

    def map_table(self, table, spec):
        return TableMapping(table.name, [declared_mapping(c) for c in table.columns]), []


BOOL_PREFIXES = ("is", "has", "can", "was", "should", "allow", "enable", "enabled", "flag", "opt")


class SampleAwareMapper(Mapper):
    name = "H2-sample+precedent"

    def __init__(self, precedents: PrecedentStore | None = None, k: int = 8, exclude_schema: str | None = None):
        self.precedents = precedents
        self.k = k
        self.exclude_schema = exclude_schema

    def _vote(self, col, values):
        if not self.precedents or not self.precedents.entries:
            return None
        hits = self.precedents.search(col, values, self.k, self.exclude_schema)
        votes = Counter((e["mapping"]["target_type"], e["mapping"]["transform"]) for s, e in hits if s > 0.2)
        return votes.most_common(1)[0][0] if votes else None

    def map_column(self, table, col) -> ColumnMapping:
        values = [r.get(col.name) for r in table.sample]
        prof = profile_values(values)
        cm = declared_mapping(col)
        fam, shape = declared_family(col.declared_type), prof["shape"]
        why = f"declared {col.declared_type}; samples: {describe_shape(prof)}"
        if fam == "int" and shape == "binary_int":
            first = col.name.lower().split("_")[0]
            vote = self._vote(col, values)
            if first in BOOL_PREFIXES or (vote and vote[0] == "bool"):
                cm.target_type, cm.transform = "bool", "int_to_bool"
        elif fam == "decimal" and declared_decimal_precision(col.declared_type) is None and shape == "decimal":
            cm.target_type = f"decimal(18,{max(2, prof['max_scale'])})"
        elif fam == "string" and shape == "json":
            cm.target_type, cm.transform = "json_string", "json_canonical"
        elif fam in ("string", "timestamp") and shape == "timestamp_tz":
            cm.target_type, cm.transform = "timestamp_tz", "parse_timestamp_tz" if fam == "string" else "tz_to_utc"
        elif fam == "string" and shape == "date":
            cm.target_type, cm.transform = "date", "parse_date"
        cm.rationale = why
        return cm

    def map_table(self, table, spec):
        return TableMapping(table.name, [self.map_column(table, c) for c in table.columns]), []

    def repair(self, table, spec, mapping, rules, errors):
        return self.map_table(table, spec)


# --------------------------------------------------------------------------
# LLM strategies
# --------------------------------------------------------------------------

SYSTEM = """You are the Schema Mapper of a data-migration tool that copies relational tables into \
cloud object storage as Parquet or CSV. For one source table you produce (1) a column mapping and \
(2) reconciliation rules that will later be executed against the source and the written files.

Allowed target_type values: int64, float64, decimal(p,s), string, bool, date, timestamp (naive), \
timestamp_tz (UTC-normalised), json_string.
Allowed transform values: none, tz_to_utc, parse_timestamp_tz, int_to_bool, json_canonical, \
parse_date, json_flatten, trim, lower.
Allowed null_handling values: preserve, empty_string, zero, drop_row.
Allowed rule kinds: row_count, pk_unique, partition_row_count, schema_conformity, null_count, \
column_checksum, sum_exact, min_max, distinct_count, true_ratio, timestamp_exact, json_roundtrip.

The migration must be lossless: every value, type and NULL in the source must be faithfully \
represented. Map each source column exactly once; keep the source column name as the target name.

Reply with ONE JSON object and nothing else:
{"columns": [{"source": str, "target": str, "target_type": str, "transform": str, \
"null_handling": str, "rationale": str}],
 "rules": [{"kind": str, "column": str|null, "tolerance": number, "rationale": str}]}"""


def schema_block(table: Table, with_samples: bool) -> str:
    lines = [f"Table: {table.name}" + (f" ({table.row_count} rows)" if table.row_count is not None else "")]
    for c in table.columns:
        flags = [x for x, on in (("PRIMARY KEY", c.primary_key), ("NOT NULL", not c.nullable and not c.primary_key)) if on]
        line = f"- {c.name}: {c.declared_type}" + (f" [{', '.join(flags)}]" if flags else "")
        if with_samples:
            vals = [r.get(c.name) for r in table.sample]
            prof = profile_values(vals)
            shown = [str(v) for v in vals if v is not None][:5]
            line += f"\n    sample profile: {describe_shape(prof)}; nulls in sample: {prof['nulls']}/{prof['n']}"
            line += f"\n    sample values: {json.dumps(shown, default=str)[:400]}"
        lines.append(line)
    for fk in table.foreign_keys:
        lines.append(f"- foreign key {fk.column} -> {fk.ref_table}.{fk.ref_column}")
    return "\n".join(lines)


def target_block(spec: TargetSpec) -> str:
    part = f"partitioned by {spec.partition_by} ({spec.partition_grain})" if spec.partition_by else "not partitioned"
    return f"Target: {spec.fmt.upper()} files, {part}."


def parse_response(table: Table, data: dict) -> tuple[TableMapping, list[ReconRule]]:
    cols = []
    for c in data.get("columns", []):
        cols.append(ColumnMapping(
            source=c.get("source"), target=str(c.get("target") or c.get("source") or "unnamed"),
            target_type=str(c.get("target_type", "string")).strip().lower().replace(" ", ""),
            transform=str(c.get("transform") or "none"), null_handling=str(c.get("null_handling") or "preserve"),
            rationale=str(c.get("rationale", ""))[:300]))
    rules = []
    for i, r in enumerate(data.get("rules", [])):
        kind = str(r.get("kind", ""))
        if kind not in RULE_KINDS:
            continue
        rules.append(ReconRule(f"llm{i}:{kind}:{r.get('column') or ''}", kind, r.get("column"),
                               float(r.get("tolerance") or 0.0), "llm", str(r.get("rationale", ""))[:300],
                               r.get("expected_source_value")))
    return TableMapping(table.name, cols), rules


class LLMMapper(Mapper):
    uses_llm = True
    proposes_rules = True
    with_samples = False

    def __init__(self, client: LLMClient):
        self.client = client

    def build_prompt(self, table: Table, spec: TargetSpec) -> str:
        return f"{target_block(spec)}\n\n{schema_block(table, self.with_samples)}"

    def _ask(self, table, prompt):
        try:
            return parse_response(table, extract_json(self.client.complete(SYSTEM, prompt)))
        except (LLMError, ValueError) as e:
            raise LLMError(f"{self.name}: unusable response for {table.name}: {e}") from e

    def map_table(self, table, spec):
        return self._ask(table, self.build_prompt(table, spec))

    def repair(self, table, spec, mapping, rules, errors):
        prompt = (self.build_prompt(table, spec) +
                  "\n\nYour previous answer was:\n" +
                  json.dumps({"columns": [vars(c) for c in mapping.columns],
                              "rules": [{"kind": r.kind, "column": r.column, "tolerance": r.tolerance} for r in rules]}) +
                  "\n\nA dry run of that mapping on the sample rows found these problems:\n- " +
                  "\n- ".join(errors[:15]) + "\n\nReturn a corrected JSON object in the same format.")
        return self._ask(table, prompt)


class ZeroShotMapper(LLMMapper):
    name = "S1-zero-shot"


class FineTunedMapper(LLMMapper):
    """Same prompt as S1; `client` must point at the fine-tuned model."""
    name = "S2-fine-tuned"


class RAGMapper(LLMMapper):
    name = "S3-rag"
    with_samples = True

    def __init__(self, client: LLMClient, precedents: PrecedentStore, k: int = 8, exclude_schema: str | None = None):
        super().__init__(client)
        self.precedents, self.k, self.exclude_schema = precedents, k, exclude_schema

    def precedent_block(self, table: Table) -> str:
        seen, lines = set(), []
        for c in table.columns:
            hits = self.precedents.search(c, [r.get(c.name) for r in table.sample], self.k, self.exclude_schema)
            for score, e in hits[:3]:
                key = (e["table"], e["column"])
                if score <= 0.15 or key in seen:
                    continue
                seen.add(key)
                m = e["mapping"]
                lines.append(f"- {e['table']}.{e['column']} ({e['declared_type']}; {describe_shape(e.get('profile', {'shape': 'empty'}))})"
                             f" -> {m['target_type']}, transform={m['transform']}, null_handling={m['null_handling']}"
                             f"; rules: {', '.join(e.get('rules', [])) or 'n/a'}")
        return "\n".join(lines[: self.k * 3]) or "(no similar precedents)"

    def build_prompt(self, table, spec):
        # Two separately labelled grounding blocks (task-alignment intuition).
        return (f"{target_block(spec)}\n\n"
                f"<schema_and_samples>\n{schema_block(table, True)}\n</schema_and_samples>\n\n"
                f"<precedent_mappings>\nMappings from previously completed migrations of structurally "
                f"similar columns (names may differ):\n{self.precedent_block(table)}\n</precedent_mappings>\n\n"
                "Use the sample values, not only the declared types, to decide each target type.")
