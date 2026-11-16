"""Streaming engine for large tables, incremental loads and lakehouse formats.

The original executor and Reconciler (executor.py, reconcile.py) hold a whole
table in memory, which is fine for the evaluation corpus but not for real
tables. This module keeps memory flat at any table size:

* one pass over the source, in chunks: every row is converted and written AND
  folded into the source side of every check in the same pass;
* the output is then read back file by file (or batch by batch for Delta /
  Iceberg) into the target side of the checks;
* checks are computed from mergeable summaries (counts, exact decimal sums,
  min/max, 64-bit value hashes, order-independent multiset fingerprints), so no
  check ever needs all values at once;
* every partition gets its own fingerprint, so a failure is reported as
  "March 2024 differs", and only that partition needs to be written again;
* the database itself independently recomputes counts, NULL counts and sums
  with SQL ("database cross-check"), compared against the written output.

Output formats: Parquet / CSV files on any target store, Delta Lake tables
(deltalake) and Apache Iceberg tables (pyiceberg, any pyiceberg catalog).
"""
from __future__ import annotations

import hashlib
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from .executor import ConversionError, convert_value, decode, encode, partition_label
from .models import ReconRule, RuleResult, Table, TableMapping, TargetSpec
from .reconcile import summarise
from .typesys import arrow_type, canon, declared_family, mask_value, parse_decimal_type

RUN_COL = "_amt_run_id"            # lineage column in Delta / Iceberg tables: which run wrote the row
NULL_PART = "__null__"
MASK64 = (1 << 64) - 1


def new_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:6]


def _h64(s: str) -> int:
    return int.from_bytes(hashlib.blake2b(s.encode("utf-8", "surrogatepass"), digest_size=8).digest(), "big")


def part_col(spec: TargetSpec) -> str | None:
    """Column that carries the partition value in Delta / Iceberg tables."""
    if not spec.partition_by:
        return None
    return f"{spec.partition_by}_{spec.partition_grain}" if spec.partition_grain in ("month", "day") \
        else f"{spec.partition_by}_part"


def label_value(label: str | None) -> str | None:
    return None if label is None else label.split("=", 1)[1]


from .executor import arrow_rows  # noqa: E402  (shared with the original engine)


# ==========================================================================
# Summaries: one per side (source / target), folded row by row
# ==========================================================================

class _MinMax:
    __slots__ = ("nmin", "nmax", "smin", "smax", "numeric", "seen")

    def __init__(self):
        self.nmin = self.nmax = self.smin = self.smax = None
        self.numeric, self.seen = True, False

    def add(self, v):
        if v is None:
            return
        self.seen = True
        s = str(v)
        self.smin = s if self.smin is None or s < self.smin else self.smin
        self.smax = s if self.smax is None or s > self.smax else self.smax
        if self.numeric:
            try:
                d = Decimal(s)
            except InvalidOperation:
                self.numeric = False
                return
            if self.nmin is None or d < self.nmin[0]:
                self.nmin = (d, s)
            if self.nmax is None or d > self.nmax[0]:
                self.nmax = (d, s)

    def value(self):
        if not self.seen:
            return (None, None)
        return (self.nmin[1], self.nmax[1]) if self.numeric else (self.smin, self.smax)


class Summary:
    """Everything the checks need from one side, without keeping the values."""

    def __init__(self, rules: list[ReconRule], mapping: TableMapping, cols: list[str]):
        self.cols = cols                                   # mapped source columns, in mapping order
        self.types = {cm.source: cm.target_type for cm in mapping.columns if cm.source in cols}
        need = defaultdict(set)
        for r in rules:
            if r.column:
                need[r.column].add(r.kind)
        self.need = need
        self.rows = 0
        self.nulls = Counter()
        self.sums: dict[str, Decimal] = defaultdict(Decimal)
        self.bad_sum: set[str] = set()
        self.minmax = defaultdict(_MinMax)
        self.distinct: dict[str, set] = defaultdict(set)
        self.trues, self.nonnull = Counter(), Counter()
        self.fp: dict[str, int] = defaultdict(int)       # order-independent multiset fingerprint
        self.keys: dict[str, set] = defaultdict(set)
        self.dups = Counter()
        self.parts = Counter()
        self.part_fp: dict[str | None, int] = defaultdict(int)

    def add(self, canon_row: dict, label: str | None) -> None:
        """canon_row: column -> canonical value (see typesys.canon)."""
        self.rows += 1
        self.parts[label] += 1
        row_hashes = []
        for c in self.cols:
            v = canon_row.get(c)
            h = _h64("\x00NULL" if v is None else repr(v))
            row_hashes.append(h)
            kinds = self.need.get(c)
            if not kinds:
                continue
            if v is None:
                self.nulls[c] += 1
            if "sum_exact" in kinds and v is not None:
                try:
                    self.sums[c] += Decimal(str(v))
                except InvalidOperation:
                    self.bad_sum.add(c)
            if "min_max" in kinds:
                self.minmax[c].add(v)
            if "distinct_count" in kinds:
                self.distinct[c].add(h)
            if "true_ratio" in kinds and v is not None:
                self.nonnull[c] += 1
                self.trues[c] += v is True
            if kinds & {"column_checksum", "timestamp_exact", "json_roundtrip"}:
                self.fp[c] = (self.fp[c] + h) & MASK64
            if "pk_unique" in kinds:
                if h in self.keys[c]:
                    self.dups[c] += 1
                else:
                    self.keys[c].add(h)
        self.part_fp[label] = (self.part_fp[label] + (hash(tuple(row_hashes)) & MASK64)) & MASK64


def _source_canon(row: dict, mapping: TableMapping, cols: list[str]) -> dict:
    out = {}
    for cm in mapping.columns:
        if cm.source not in cols:
            continue
        v = row.get(cm.source)
        out[cm.source] = canon(mask_value(v) if cm.transform == "mask" else v, cm.target_type)
    return out


def _target_canon(row: dict, mapping: TableMapping, cols: list[str]) -> dict:
    return {cm.source: canon(row.get(cm.target), cm.target_type) for cm in mapping.columns if cm.source in cols}


def _rule_values(r: ReconRule, s: Summary, missing: set[str]):
    c = r.column
    if c and c in missing:
        return None
    k = r.kind
    if k == "row_count":
        return s.rows
    if k == "null_count":
        return s.nulls[c]
    if k == "sum_exact":
        return "<unparseable values>" if c in s.bad_sum else _dec_text(s.sums[c])
    if k == "min_max":
        return s.minmax[c].value()
    if k == "distinct_count":
        return len(s.distinct[c])
    if k == "true_ratio":
        return round(s.trues[c] / s.nonnull[c], 6) if s.nonnull[c] else 0.0
    if k in ("column_checksum", "timestamp_exact", "json_roundtrip"):
        return f"{s.fp[c]:016x}"
    if k == "pk_unique":
        return len(s.keys[c]) if not s.dups[c] else f"{s.dups[c]} duplicate keys"
    if k == "partition_row_count":
        return {lb or "(all)": n for lb, n in sorted(s.parts.items(), key=lambda kv: kv[0] or "")}
    raise KeyError(k)


def _dec_text(d: Decimal) -> str:
    """Exact decimal as plain text: 45150, 1234.5 (never 4.515E+4)."""
    return format(d.normalize(), "f") if d else "0"


def _explain(r: ReconRule, sv, tv) -> str:
    if r.kind == "partition_row_count":
        diff = {k: (sv.get(k, 0), tv.get(k, 0)) for k in set(sv) | set(tv) if sv.get(k, 0) != tv.get(k, 0)}
        return f"partition counts differ (expected, actual): {diff}"
    if r.kind in ("column_checksum", "timestamp_exact", "json_roundtrip"):
        return "some values in the output differ from the source"
    return f"source={sv!r} target={tv!r}"


def _norm_type(t: str) -> str:
    t = t.replace("large_string", "string").replace("string_view", "string")
    if t.startswith("timestamp["):
        unit_tz = t[len("timestamp["):-1].split(",")
        tz = unit_tz[1].strip() if len(unit_tz) > 1 else ""
        tz = "UTC" if tz.replace("tz=", "") in ("UTC", "+00:00", "Etc/UTC", "utc") else tz.replace("tz=", "")
        return f"timestamp[us{', tz=' + tz if tz else ''}]"
    return t


def compare(rules: list[ReconRule], src: Summary, tgt: Summary, mapping: TableMapping,
            written_schema: pa.Schema | None, missing: set[str], extra: set[str] = frozenset()) -> list[RuleResult]:
    """extra: columns the tool itself adds to lakehouse tables (partition value, run id)."""
    out = []
    for r in rules:
        if r.kind == "schema_conformity":
            exp = {cm.target: _norm_type(str(arrow_type(cm.target_type))) for cm in mapping.columns
                   if cm.transform != "json_flatten"}
            got = {} if written_schema is None else {
                f.name: _norm_type(str(f.type)) for f in written_schema if f.name not in extra}
            got = {k: got.get(k, "<missing>") for k in exp} | {k: v for k, v in got.items() if k not in exp}
            ok = exp == got
            diff = {k: (exp.get(k), got.get(k)) for k in set(exp) | set(got) if exp.get(k) != got.get(k)}
            out.append(RuleResult(r, ok, exp, got, "" if ok else f"schema differs (expected, actual): {diff}"))
            continue
        try:
            sv, tv = _rule_values(r, src, set()), _rule_values(r, tgt, missing)
        except KeyError:
            out.append(RuleResult(r, False, detail=f"unknown rule kind {r.kind}"))
            continue
        if tv is None:
            out.append(RuleResult(r, False, sv, None, "target column missing or restructured"))
            continue
        ok = sv == tv
        out.append(RuleResult(r, ok, sv, tv, "" if ok else _explain(r, sv, tv)))
    return out


# ==========================================================================
# Database cross-check: the source database recomputes aggregates with SQL
# ==========================================================================

def crosscheck_plan(source, table: Table, mapping: TableMapping) -> list[tuple[ReconRule, str]]:
    """(rule, SQL expression) pairs that are safe to compute in the database.

    Only where SQL gives the exact value the tool would compute: COUNT(*) always,
    NULL counts for columns whose NULLs are preserved, and SUM/MIN/MAX for integer
    columns copied as integers (and for DECIMAL columns on engines with exact
    decimal arithmetic, i.e. not SQLite, which stores decimals as floats)."""
    from .connectors.sources import SQLiteSource
    q = source.quote
    plan = [(ReconRule("db:row_count", "row_count", origin="database",
                       rationale="the database counts the rows itself"), "COUNT(*)")]
    sqlite = isinstance(source, SQLiteSource)
    for cm in mapping.columns:
        col = table.column(cm.source) if cm.source else None
        if col is None or cm.transform not in ("none", "mask"):
            continue
        if col.nullable and cm.null_handling == "preserve":
            plan.append((ReconRule(f"db:null_count:{col.name}", "null_count", col.name, origin="database",
                                   rationale="the database counts the NULLs itself"),
                         f"COUNT(*) - COUNT({q(col.name)})"))
        if cm.transform != "none":
            continue
        fam = declared_family(col.declared_type)
        exact_dec = parse_decimal_type(cm.target_type) and fam == "decimal" and not sqlite
        if (fam == "int" and cm.target_type == "int64") or exact_dec:
            arg = q(col.name) if sqlite or exact_dec else f"CAST({q(col.name)} AS DECIMAL(38,0))"
            plan.append((ReconRule(f"db:sum:{col.name}", "sum_exact", col.name, origin="database",
                                   rationale="the database adds up the column itself"), f"SUM({arg})"))
        if fam == "int" and cm.target_type == "int64":
            plan.append((ReconRule(f"db:min_max:{col.name}", "min_max", col.name, origin="database",
                                   rationale="the database finds the smallest and largest value itself"),
                         f"MIN({q(col.name)})|MAX({q(col.name)})"))
    return plan


def database_crosscheck(source, table: Table, mapping: TableMapping, tgt: Summary, missing: set[str],
                        window: tuple | None = None, plan: list | None = None) -> list[RuleResult]:
    plan = plan if plan is not None else crosscheck_plan(source, table, mapping)
    exprs = {}
    for i, (_, e) in enumerate(plan):
        for j, part in enumerate(e.split("|")):
            exprs[f"a{i}_{j}"] = part
    col, after, upto = window or (None, None, None)
    try:
        got = source.aggregate(table.name, exprs, col, after, upto)
    except Exception as e:  # the cross-check is extra evidence; report, never crash the run
        return [RuleResult(ReconRule("db:error", "row_count", origin="database"), False,
                           detail=f"the database cross-check could not run: {e}")]
    lower = {str(k).lower(): v for k, v in got.items()}
    out = []
    for i, (rule, e) in enumerate(plan):
        vals = [lower.get(f"a{i}_{j}") for j in range(len(e.split("|")))]
        if rule.kind == "min_max":
            sv = tuple(None if v is None else str(Decimal(str(v)).normalize()) for v in vals)
        elif rule.kind == "sum_exact":
            sv = "0" if vals[0] is None else _dec_text(Decimal(str(vals[0])))
        else:
            sv = int(vals[0] or 0)
        tv = _rule_values(rule, tgt, missing) if not (rule.column and rule.column in missing) else None
        ok = tv is not None and sv == tv
        out.append(RuleResult(rule, ok, sv, tv, "" if ok else f"database says {sv!r}, output has {tv!r}"))
    return out


# ==========================================================================
# Sinks: where the converted rows go
# ==========================================================================

def output_schema(mapping: TableMapping, spec: TargetSpec, lakehouse: bool) -> pa.Schema:
    fields = []
    for cm in mapping.columns:
        if cm.transform == "json_flatten":
            raise ConversionError("json_flatten cannot be used with the streaming engine; keep JSON as json_string")
        fields.append(pa.field(cm.target, arrow_type(cm.target_type)))
    if lakehouse:
        if part_col(spec):
            fields.append(pa.field(part_col(spec), pa.string()))
        fields.append(pa.field(RUN_COL, pa.string()))
    return pa.schema(fields)


@dataclass
class WriteResult:
    files: dict[str, int] = field(default_factory=dict)
    location: str = ""


class FileSink:
    """Parquet / CSV files under <prefix>/<partition>/part-<run>-NNNNN.<fmt> on any target store.
    Buffers are bounded: a file is written when a partition reaches rows_per_file, or the
    largest buffer is flushed when all buffers together exceed `budget` rows."""

    lakehouse = False

    def __init__(self, target, prefix: str, spec: TargetSpec, run_id: str, budget: int = 200_000):
        self.target, self.prefix, self.spec, self.run_id, self.budget = target, prefix, spec, run_id, budget

    def prepare(self, mode: str, partitions: set | None) -> None:
        if mode == "overwrite":
            self.target.delete_prefix(self.prefix)
        elif mode == "partitions":
            for lb in partitions or ():
                self.target.delete_prefix(f"{self.prefix}/{lb}/")

    def write(self, batches: Iterator[list[tuple[str | None, dict]]], schema: pa.Schema) -> WriteResult:
        res = WriteResult(location=self.prefix)
        bufs: dict[str | None, list] = defaultdict(list)
        seq = Counter()
        total = 0

        def flush(label):
            nonlocal total
            rows = bufs.pop(label, [])
            if not rows:
                return
            key = "/".join(p for p in (self.prefix, label, f"part-{self.run_id}-{seq[label]:05d}.{self.spec.fmt}") if p)
            seq[label] += 1
            self.target.write_bytes(key, encode(pa.Table.from_pylist(rows, schema=schema), self.spec.fmt))
            res.files[key] = len(rows)
            total -= len(rows)

        for batch in batches:
            for label, row in batch:
                bufs[label].append(row)
                total += 1
                if len(bufs[label]) >= self.spec.rows_per_file:
                    flush(label)
            while total > self.budget:
                flush(max(bufs, key=lambda k: len(bufs[k])))
        for label in list(bufs):
            flush(label)
        return res

    def read_back(self, scope: dict) -> Iterator[tuple[str | None, list[dict]]]:
        """(partition label, rows) for every file in scope (this run / these partitions / all)."""
        for key in self.target.list(self.prefix):
            if not key.endswith("." + self.spec.fmt):
                continue
            if scope.get("run_id") and f"part-{scope['run_id']}-" not in key:
                continue
            label = next((p for p in key.split("/") if self.spec.partition_by and
                          p.startswith(self.spec.partition_by + "=")), None)
            if scope.get("partitions") is not None and label not in scope["partitions"]:
                continue
            yield label, arrow_rows(decode(self.target.read_bytes(key), self.spec.fmt, scope.get("csv_schema")))

    def schema(self, scope: dict) -> pa.Schema | None:
        for key in self.target.list(self.prefix):
            if key.endswith(".parquet") and (not scope.get("run_id") or f"part-{scope['run_id']}-" in key):
                import io
                return pq.read_schema(io.BytesIO(self.target.read_bytes(key)))
            if key.endswith(".csv"):
                return scope.get("expected_schema")
        return None


class DeltaSink:
    """A Delta Lake table (deltalake / delta-rs). Local paths, s3://, abfss:// and gs:// URIs;
    cloud credentials via `storage_options` (or the usual environment variables)."""

    lakehouse = True

    def __init__(self, uri: str, spec: TargetSpec, run_id: str, storage_options: dict | None = None):
        self.uri, self.spec, self.run_id = uri, spec, run_id
        self.opts = storage_options or None
        self.mode, self.partitions = "overwrite", None
        self.keys: list[str] = []          # primary key for upsert

    def prepare(self, mode: str, partitions: set | None) -> None:
        self.mode, self.partitions = mode, partitions

    def _upsert(self, batches, schema: pa.Schema) -> WriteResult:
        """MERGE each chunk on the primary key: changed rows replace their old version."""
        from deltalake import DeltaTable, write_deltalake
        res = WriteResult(location=self.uri)
        counts = Counter()
        pc = part_col(self.spec)
        pred = " AND ".join(f"t.{k} = s.{k}" for k in self.keys)
        for batch in batches:
            rows = []
            for label, row in batch:
                if pc:
                    row[pc] = label_value(label)
                row[RUN_COL] = self.run_id
                rows.append(row)
                counts[label] += 1
            if not rows:
                continue
            data = pa.Table.from_pylist(rows, schema=schema)
            if not DeltaTable.is_deltatable(self.uri, storage_options=self.opts):
                write_deltalake(self.uri, data, mode="error", partition_by=[pc] if pc else None,
                                storage_options=self.opts)
                continue
            (DeltaTable(self.uri, storage_options=self.opts)
             .merge(source=data, predicate=pred, source_alias="s", target_alias="t")
             .when_matched_update_all().when_not_matched_insert_all().execute())
        res.files = {f"{self.uri} [{lb or 'all'}]": n for lb, n in counts.items()}
        return res

    def write(self, batches, schema: pa.Schema) -> WriteResult:
        from deltalake import write_deltalake
        if self.mode == "upsert":
            return self._upsert(batches, schema)
        res = WriteResult(location=self.uri)
        counts = Counter()
        pc = part_col(self.spec)

        def gen():
            for batch in batches:
                rows = []
                for label, row in batch:
                    if pc:
                        row[pc] = label_value(label)
                    row[RUN_COL] = self.run_id
                    rows.append(row)
                    counts[label] += 1
                if rows:
                    yield pa.RecordBatch.from_pylist(rows, schema=schema)

        reader = pa.RecordBatchReader.from_batches(schema, gen())
        kw = {"partition_by": [pc] if pc else None, "storage_options": self.opts, "schema_mode": "overwrite"
              if self.mode == "overwrite" else None}
        if self.mode == "partitions":
            vals = ", ".join("'" + str(label_value(lb)).replace("'", "''") + "'" for lb in sorted(self.partitions))
            kw.update(mode="overwrite", predicate=f"{pc} IN ({vals})", schema_mode=None)
        else:
            kw["mode"] = "overwrite" if self.mode == "overwrite" else "append"
        write_deltalake(self.uri, reader, **{k: v for k, v in kw.items() if v is not None})
        res.files = {f"{self.uri} [{lb or 'all'}]": n for lb, n in counts.items()}
        return res

    def _dataset(self):
        from deltalake import DeltaTable
        return DeltaTable(self.uri, storage_options=self.opts).to_pyarrow_dataset()

    def read_back(self, scope: dict):
        import pyarrow.compute as pcmp
        pc = part_col(self.spec)
        want_parts = [label_value(lb) for lb in scope["partitions"]] if scope.get("partitions") is not None and pc else None

        def as_str(col):
            # files written by a Delta MERGE hold text as string_view, which Arrow's
            # compare kernels (and dataset filters) do not support yet: convert first
            return col.cast(pa.string()) if col.type != pa.string() else col

        for b in self._dataset().to_batches(batch_size=50_000):
            if scope.get("run_id"):
                b = b.filter(pcmp.equal(as_str(b.column(RUN_COL)), scope["run_id"]))
            if want_parts is not None:
                b = b.filter(pcmp.is_in(as_str(b.column(pc)), value_set=pa.array(want_parts)))
            if not b.num_rows:
                continue
            rows = arrow_rows(b)
            if not pc:
                yield None, rows
                continue
            by = defaultdict(list)
            for r in rows:
                v = r.get(pc)
                by[None if v is None else f"{self.spec.partition_by}={v}"].append(r)
            yield from by.items()

    def schema(self, scope: dict):
        return self._dataset().schema


class IcebergSink:
    """An Apache Iceberg table in any pyiceberg catalog: a local SQL catalog (SQLite file),
    or AWS Glue / REST / Hive with the catalog properties pyiceberg documents."""

    lakehouse = True

    def __init__(self, catalog_props: dict, identifier: str, spec: TargetSpec, run_id: str):
        self.props, self.identifier, self.spec, self.run_id = dict(catalog_props), identifier, spec, run_id
        self.mode, self.partitions = "overwrite", None
        self._table = None

    def catalog(self):
        from pyiceberg.catalog import load_catalog
        import re
        props = dict(self.props)
        name = props.pop("name", "amt")
        wh = str(props.get("warehouse") or "")
        if re.match(r"^file:///[A-Za-z]:/", wh):       # pyiceberg mangles file:///C:/... on Windows
            props["warehouse"] = wh[len("file:///"):]
        return load_catalog(name, **props)

    def prepare(self, mode: str, partitions: set | None) -> None:
        self.mode, self.partitions = mode, partitions

    def _ensure(self, schema: pa.Schema):
        from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
        cat = self.catalog()
        ns = self.identifier.rsplit(".", 1)[0] if "." in self.identifier else "default"
        try:
            cat.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass
        ident = self.identifier if "." in self.identifier else f"{ns}.{self.identifier}"
        try:
            tbl = cat.load_table(ident)
            if self.mode == "overwrite":
                cat.drop_table(ident)
                raise NoSuchTableError(ident)
        except NoSuchTableError:
            tbl = cat.create_table(ident, schema=schema)
            pc = part_col(self.spec)
            if pc:
                from pyiceberg.transforms import IdentityTransform
                with tbl.update_spec() as us:
                    us.add_field(pc, IdentityTransform(), pc)
        self._table = tbl
        return tbl

    def write(self, batches, schema: pa.Schema) -> WriteResult:
        from pyiceberg.expressions import In
        tbl = self._ensure(schema)
        pc = part_col(self.spec)
        counts = Counter()
        res = WriteResult(location=self.identifier)
        with tbl.transaction() as tx:
            if self.mode == "partitions":
                tx.delete(In(pc, [label_value(lb) for lb in self.partitions]))
            for batch in batches:
                rows = []
                for label, row in batch:
                    if pc:
                        row[pc] = label_value(label)
                    row[RUN_COL] = self.run_id
                    rows.append(row)
                    counts[label] += 1
                if rows:
                    tx.append(pa.Table.from_pylist(rows, schema=schema))
        res.files = {f"{self.identifier} [{lb or 'all'}]": n for lb, n in counts.items()}
        return res

    def read_back(self, scope: dict):
        from pyiceberg.expressions import AlwaysTrue, And, EqualTo, In
        tbl = self.catalog().load_table(self._ident())
        pc = part_col(self.spec)
        flt = AlwaysTrue()
        if scope.get("run_id"):
            flt = EqualTo(RUN_COL, scope["run_id"])
        if scope.get("partitions") is not None and pc:
            flt = And(flt, In(pc, [label_value(lb) for lb in scope["partitions"]]))
        for b in tbl.scan(row_filter=flt).to_arrow_batch_reader():
            rows = arrow_rows(b)
            if not pc:
                yield None, rows
                continue
            by = defaultdict(list)
            for r in rows:
                v = r.get(pc)
                by[None if v is None else f"{self.spec.partition_by}={v}"].append(r)
            yield from by.items()

    def _ident(self):
        return self.identifier if "." in self.identifier else f"default.{self.identifier}"

    def schema(self, scope: dict):
        return self.catalog().load_table(self._ident()).schema().as_arrow()


# ==========================================================================
# The run: one pass to write + summarise the source, one pass to read back
# ==========================================================================

@dataclass
class StreamReport:
    table: str
    run_id: str
    mode: str
    rows_read: int = 0
    rows_written: int = 0
    rows_dropped: int = 0
    files: dict[str, int] = field(default_factory=dict)
    location: str = ""
    results: list[RuleResult] = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    partitions: list[dict] = field(default_factory=list)    # per-partition verdicts
    crosscheck: list[RuleResult] = field(default_factory=list)
    window: dict | None = None
    seconds: float = 0.0
    peak_buffer_rows: int = 0

    @property
    def failed_partitions(self) -> list[str]:
        return [p["partition"] for p in self.partitions if not p["match"]]


def run_stream(source, table: Table, mapping: TableMapping, spec: TargetSpec, sink, rules: list[ReconRule],
               *, mode: str = "overwrite", window: tuple | None = None, partitions: set | None = None,
               chunk_rows: int = 50_000, run_id: str | None = None, crosscheck: bool = True,
               progress=None, verify_only: bool = False) -> StreamReport:
    """Migrate and verify one table without holding it in memory.

    mode: "overwrite" (full load), "append" (incremental: only rows in `window`
    = (column, after, upto)), "upsert" (incremental for Delta: changed rows replace
    their old version, matched on the primary key), or "partitions" (rewrite only
    `partitions`, e.g. the ones that failed last time). verify_only=True re-checks output that is already
    there (written earlier, or by another tool) without writing anything."""
    t0 = time.perf_counter()
    run_id = run_id or new_run_id()
    rep = StreamReport(table.name, run_id, mode)
    if window:
        rep.window = {"column": window[0], "after": window[1], "upto": window[2]}
    cols = [cm.source for cm in mapping.columns if cm.source and cm.transform != "json_flatten"]
    schema = output_schema(mapping, spec, sink.lakehouse)
    cross = crosscheck_plan(source, table, mapping) if crosscheck and mode != "partitions" else []
    need = rules + [r for r, _ in cross]          # the output must also summarise what the database checks
    src_sum = Summary(need, mapping, cols)
    if not verify_only:
        sink.prepare(mode, partitions)

    def batches() -> Iterator[list]:
        batch = []
        col, after, upto = window or (None, None, None)
        for r in source.stream_rows(table, col, after, upto):
            label = partition_label(r, spec)
            if partitions is not None and label not in partitions:
                continue
            rep.rows_read += 1
            out, drop = {}, False
            for cm in mapping.columns:
                v = r.get(cm.source) if cm.source else None
                if v is None and cm.null_handling == "drop_row":
                    drop = True
                    break
                out[cm.target] = convert_value(v, cm)
            if drop:
                rep.rows_dropped += 1
                continue
            src_sum.add(_source_canon(r, mapping, cols), label)
            batch.append((label, out))
            if len(batch) >= chunk_rows:
                if progress:
                    progress(f"copied {rep.rows_read:,} rows")
                yield batch
                batch = []
        if batch:
            yield batch

    if verify_only:
        for _ in batches():           # source side only
            pass
    else:
        wr = sink.write(batches(), schema)
        rep.files, rep.location = wr.files, wr.location
        rep.rows_written = sum(wr.files.values())

    # ---- read back what this run is responsible for, and compare ----
    if progress:
        progress("reading the output back and checking it")
    scope = {"run_id": run_id if mode in ("append", "upsert") and not verify_only else None,
             "partitions": partitions if mode == "partitions" else None,
             "csv_schema": {cm.target: arrow_type(cm.target_type) for cm in mapping.columns},
             "expected_schema": pa.schema([f for f in schema if f.name != RUN_COL])}
    tgt_sum = Summary(need, mapping, cols)
    missing: set[str] = set()
    for label, rows in sink.read_back(scope):
        if rows and not missing:
            missing = {cm.source for cm in mapping.columns if cm.source in cols and cm.target not in rows[0]}
        for row in rows:
            tgt_sum.add(_target_canon(row, mapping, cols), label)
    if verify_only:
        rep.rows_written = tgt_sum.rows
    written_schema = sink.schema(scope)
    extra = {RUN_COL, part_col(spec)} if sink.lakehouse else set()
    rep.results = compare(rules, src_sum, tgt_sum, mapping, written_schema, missing, extra)

    # ---- per-partition verdicts ----
    for lb in sorted(set(src_sum.parts) | set(tgt_sum.parts), key=lambda x: x or ""):
        sr, tr = src_sum.parts.get(lb, 0), tgt_sum.parts.get(lb, 0)
        ok = sr == tr and src_sum.part_fp.get(lb, 0) == tgt_sum.part_fp.get(lb, 0)
        rep.partitions.append({"partition": lb or "(all)", "source_rows": sr, "target_rows": tr, "match": ok})

    # ---- the database recomputes counts and sums itself ----
    if cross:
        rep.crosscheck = database_crosscheck(source, table, mapping, tgt_sum, missing, window, cross)
    rep.summary = summarise(rep.results + rep.crosscheck)
    rep.summary["failed_partitions"] = rep.failed_partitions
    rep.seconds = round(time.perf_counter() - t0, 2)
    return rep


def results_json(results: Iterable[RuleResult]) -> list[dict]:
    from dataclasses import asdict
    return [{"rule": asdict(r.rule), "passed": r.passed, "source": r.source_value, "target": r.target_value,
             "detail": r.detail} for r in results]
