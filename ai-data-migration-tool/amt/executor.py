"""Migration Executor: applies a mapping mu to source rows and writes
partitioned Parquet/CSV files to the target store."""
from __future__ import annotations

import io
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Iterable

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .connectors import SourceConnector, TargetConnector
from .models import ColumnMapping, TableMapping, TargetSpec
from .typesys import arrow_type, mask_value, parse_decimal_type, to_bool, to_datetime, to_decimal


class ConversionError(ValueError):
    pass


@dataclass
class ExecutionResult:
    table: str
    rows_read: int = 0
    rows_written: int = 0
    rows_dropped: int = 0
    files: dict[str, int] = field(default_factory=dict)   # key -> row count


def _flatten(v) -> dict:
    doc = json.loads(v) if isinstance(v, str) else (v or {})
    if not isinstance(doc, dict):
        return {}
    # Top-level scalars only: nested structure is dropped, which is exactly the
    # silent loss the reconciliation engine has to catch.
    return {k: (None if isinstance(x, (dict, list)) else str(x)) for k, x in doc.items()}


def convert_value(v, cm: ColumnMapping):
    if v is None:
        nh = cm.null_handling
        if nh == "preserve" or nh == "drop_row":
            return None
        if nh == "empty_string":
            return ""
        if nh == "zero":
            return 0
        raise ConversionError(f"unknown null handling {nh}")
    t, tr = cm.target_type, cm.transform
    try:
        if tr == "trim":
            v = str(v).strip()
        elif tr == "lower":
            v = str(v).lower()
        elif tr == "int_to_bool":
            v = to_bool(v)
        elif tr == "mask":
            v = mask_value(v)
        elif tr == "json_canonical":
            doc = json.loads(v) if isinstance(v, str) else v
            v = json.dumps(doc, sort_keys=True, separators=(",", ":"))
        dec = parse_decimal_type(t)
        if dec:
            d = to_decimal(v)
            q = d.quantize(Decimal(1).scaleb(-dec[1]), rounding=ROUND_HALF_EVEN)
            if len(q.as_tuple().digits) > dec[0]:
                raise ConversionError(f"{v} overflows decimal({dec[0]},{dec[1]})")
            return q
        if t == "int64":
            if isinstance(v, bool):
                return int(v)
            d = to_decimal(v)
            if d != d.to_integral_value():
                raise ConversionError(f"{v} is not an integer")
            return int(d)
        if t == "float64":
            return float(v)
        if t == "bool":
            return to_bool(v)
        if t == "date":
            return v if isinstance(v, date) and not isinstance(v, datetime) else to_datetime(v).date()
        if t == "timestamp":
            d = to_datetime(v)
            return d.replace(tzinfo=None)          # keeps wall-clock time, drops the offset
        if t == "timestamp_tz":
            d = to_datetime(v)
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            return d.astimezone(timezone.utc)
        if t in ("string", "json_string"):
            if isinstance(v, (datetime, date)):
                return v.isoformat()
            if isinstance(v, (dict, list)):
                return json.dumps(v, sort_keys=True, separators=(",", ":"))
            return str(v)
    except ConversionError:
        raise
    except (ValueError, TypeError, ArithmeticError) as e:
        raise ConversionError(f"{cm.source}: cannot convert {v!r} to {t} ({e})") from e
    raise ConversionError(f"unknown target type {t}")


def convert_rows(rows: Iterable[dict], mapping: TableMapping) -> tuple[list[dict], list[str], int]:
    """Convert source rows; returns (rows, errors, dropped). Errors are capped."""
    out, errors, dropped = [], [], 0
    for r in rows:
        o, drop = {}, False
        for cm in mapping.columns:
            v = r.get(cm.source) if cm.source else None
            if v is None and cm.null_handling == "drop_row":
                drop = True
                break
            if cm.source and cm.source not in r:
                if len(errors) < 20:
                    errors.append(f"source column {cm.source!r} does not exist")
                continue
            try:
                if cm.transform == "json_flatten":
                    for k, x in _flatten(v).items():
                        o[f"{cm.target}__{k}"] = x
                else:
                    o[cm.target] = convert_value(v, cm)
            except ConversionError as e:
                if len(errors) < 20:
                    errors.append(str(e))
                o[cm.target] = None
        if drop:
            dropped += 1
        else:
            out.append(o)
    return out, errors, dropped


def output_schema(mapping: TableMapping, rows: list[dict]) -> pa.Schema:
    fields = []
    for cm in mapping.columns:
        if cm.transform == "json_flatten":
            keys = sorted({k for r in rows for k in r if k.startswith(cm.target + "__")})
            fields += [pa.field(k, pa.string()) for k in keys]
        else:
            fields.append(pa.field(cm.target, arrow_type(cm.target_type)))
    return pa.schema(fields)


def partition_label(row: dict, spec: TargetSpec) -> str | None:
    if not spec.partition_by:
        return None
    v = row.get(spec.partition_by)
    if v is None:
        return f"{spec.partition_by}=__null__"
    if spec.partition_grain in ("month", "day"):
        d = to_datetime(v)
        if d.tzinfo is not None:
            d = d.astimezone(timezone.utc)
        val = d.strftime("%Y-%m" if spec.partition_grain == "month" else "%Y-%m-%d")
    else:
        val = str(v)
    return f"{spec.partition_by}={val}"


def encode(table: pa.Table, fmt: str) -> bytes:
    buf = io.BytesIO()
    if fmt == "parquet":
        pq.write_table(table, buf)
    elif fmt == "csv":
        pacsv.write_csv(table, buf)
    else:
        raise ValueError(f"unsupported format {fmt}")
    return buf.getvalue()


def decode(data: bytes, fmt: str, schema: pa.Schema | None = None) -> pa.Table:
    if fmt == "parquet":
        return pq.read_table(io.BytesIO(data))
    opts = pacsv.ConvertOptions(column_types=schema) if schema is not None else None
    return pacsv.read_csv(io.BytesIO(data), convert_options=opts)


def write_rows(target: TargetConnector, prefix: str, groups: dict[str | None, list[dict]],
               mapping: TableMapping, spec: TargetSpec) -> dict[str, int]:
    files = {}
    for label, grp in sorted(groups.items(), key=lambda kv: kv[0] or ""):
        schema = output_schema(mapping, grp)
        chunks = [grp] if label else [grp[i:i + spec.rows_per_file] for i in range(0, len(grp), spec.rows_per_file)] or [[]]
        for i, chunk in enumerate(chunks):
            key = "/".join(p for p in (prefix, label, f"part-{i:05d}.{spec.fmt}") if p)
            target.write_bytes(key, encode(pa.Table.from_pylist(chunk, schema=schema), spec.fmt))
            files[key] = len(chunk)
    return files


def execute(source: SourceConnector, table, mapping: TableMapping, spec: TargetSpec,
            target: TargetConnector, prefix: str | None = None) -> ExecutionResult:
    """Full extraction -> conversion -> partitioned write. Raises on conversion errors."""
    prefix = prefix if prefix is not None else table.name
    target.delete_prefix(prefix)
    res = ExecutionResult(table.name)
    groups: dict[str | None, list[dict]] = defaultdict(list)
    for r in source.iter_rows_schema(table):
        res.rows_read += 1
        out, errs, dropped = convert_rows([r], mapping)
        if errs:
            raise ConversionError(errs[0])
        res.rows_dropped += dropped
        if out:
            groups[partition_label(r, spec)].append(out[0])
    res.files = write_rows(target, prefix, groups, mapping, spec)
    res.rows_written = sum(res.files.values())
    return res


def arrow_rows(data) -> list[dict]:
    """Rows of an Arrow table / batch as dicts, fast.

    Time-zone timestamps are first turned into plain UTC timestamps: converting zoned
    values to Python one by one makes Arrow look up a time-zone module for every single
    value (very slow on Windows), and reconciliation reads naive values as UTC anyway."""
    cols, changed = [], False
    for col in data.columns:
        t = col.type
        if pa.types.is_timestamp(t) and t.tz is not None:
            col = col.cast(pa.timestamp(t.unit))
            changed = True
        cols.append(col)
    if changed:
        names = data.schema.names
        data = pa.table(cols, names=names) if isinstance(data, pa.Table) else pa.RecordBatch.from_arrays(cols, names=names)
    return data.to_pylist()


def read_output(target: TargetConnector, prefix: str, fmt: str,
                mapping: TableMapping | None = None) -> list[tuple[str, list[dict]]]:
    """Read every output file back as (key, rows)."""
    out = []
    for key in target.list(prefix):
        if not key.endswith("." + fmt):
            continue
        schema = None
        if fmt == "csv" and mapping is not None:
            schema = {cm.target: arrow_type(cm.target_type) for cm in mapping.columns
                      if cm.transform != "json_flatten"}
        out.append((key, arrow_rows(decode(target.read_bytes(key), fmt, schema))))
    return out
