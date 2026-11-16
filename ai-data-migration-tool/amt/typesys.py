"""Relational -> file type system: parsing, value-shape profiling, lossiness
checks and the canonical value form used by reconciliation."""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import lru_cache

import pyarrow as pa

_DEC_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")
_DECL_DEC_RE = re.compile(r"^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", re.I)

# --- privacy masking (transform "mask") -------------------------------------
_EMAIL_RE = re.compile(r"([\w.+-])[\w.+-]*@([\w-]+\.[\w.]+)")
_LONGNUM_RE = re.compile(r"(?:\+?\d[\d\s-]{6,}\d)")      # phone, Aadhaar, card and account numbers
_PAN_RE = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
_DATE_RE = re.compile(r"^(\d{4})-\d{2}-\d{2}")


def _mask_digits(s: str) -> str:
    """Keep only the last 4 digits: '+91 98765 43210' -> '******3210'."""
    d = [c for c in s if c.isdigit()]
    return "*" * max(len(d) - 4, 2) + "".join(d[-4:])


def mask_value(v) -> str | None:
    """Deterministic, irreversible masking of one personal value (NULL stays NULL).

    e-mail  ravi.k@example.com -> r***@example.com
    numbers +91 98765 43210    -> ******3210   (phone, ID, card, account numbers)
    dates   1990-05-12         -> 1990-**-**   (date of birth: keep the year only)
    short   Ravi Kumar         -> R*** K***    (names, cities, short codes)
    text    'call 9876543210 or ravi@x.com' -> 'call ******3210 or r***@x.com'
    """
    if v is None:
        return None
    s = v.isoformat() if isinstance(v, (date, datetime)) else str(v)
    if not s.strip():
        return s
    if m := _DATE_RE.match(s):
        return f"{m.group(1)}-**-**"
    words = s.split()
    if len(words) > 3:                       # free text: hide only the personal bits inside it
        s = _EMAIL_RE.sub(lambda m: f"{m.group(1)}***@{m.group(2)}", s)
        s = _PAN_RE.sub("*****####*", s)
        return _LONGNUM_RE.sub(lambda m: _mask_digits(m.group()), s)
    if "@" in s:
        local, _, domain = s.partition("@")
        return f"{local[:1]}***@{domain}"
    if sum(c.isdigit() for c in s) >= 4 and sum(c.isdigit() for c in s) >= 0.6 * len(s.replace(" ", "").replace("-", "")):
        return _mask_digits(s)
    return " ".join(w[:1] + "***" for w in words)


@lru_cache(maxsize=1024)      # called for every value: cache the parse of each type string
def parse_decimal_type(t: str) -> tuple[int, int] | None:
    m = _DEC_RE.match(t.strip().lower())
    return (int(m.group(1)), int(m.group(2))) if m else None


def is_valid_target_type(t: str) -> bool:
    from .models import TARGET_TYPES
    return t in TARGET_TYPES or parse_decimal_type(t) is not None


@lru_cache(maxsize=4096)
def declared_family(declared: str) -> str:
    """Coarse family of a declared SQL type (no samples consulted)."""
    d = declared.strip().upper()
    if "WITH TIME ZONE" in d or d in ("TIMESTAMPTZ", "DATETIMEOFFSET"):
        return "timestamp_tz"
    if d.startswith(("TIMESTAMP", "DATETIME")):
        return "timestamp"
    if d == "DATE":
        return "date"
    if d.startswith(("DECIMAL", "NUMERIC", "MONEY")):
        return "decimal"
    if d.startswith(("BOOL", "BIT")):
        return "bool"
    if d.startswith(("JSON",)):
        return "json"
    if d.startswith("ENUM") or d.endswith("_ENUM"):
        return "enum"
    if any(k in d for k in ("INT", "SERIAL")):
        return "int"
    if any(k in d for k in ("REAL", "FLOAT", "DOUBLE")):
        return "float"
    return "string"


def declared_decimal_precision(declared: str) -> tuple[int, int] | None:
    d = declared.strip().upper()
    if d == "MONEY":            # SQL Server fixed-point currency types
        return 19, 4
    if d == "SMALLMONEY":
        return 10, 4
    m = _DECL_DEC_RE.match(declared.strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2) or 0)


# --------------------------------------------------------------------------
# Value-shape profiling of sample rows (used by sample-aware grounding)
# --------------------------------------------------------------------------

def _parse_ts(v):
    if isinstance(v, datetime):
        return v
    if not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _as_json(v):
    if isinstance(v, (dict, list)):
        return v
    if not isinstance(v, str) or not v.strip().startswith(("{", "[")):
        return None
    try:
        return json.loads(v)
    except ValueError:
        return None


def _decimal_scale(v) -> int | None:
    if isinstance(v, bool):
        return None
    try:
        d = Decimal(repr(v)) if isinstance(v, float) else Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None
    exp = d.as_tuple().exponent
    return max(0, -exp) if isinstance(exp, int) else None


def profile_values(values: list) -> dict:
    """Summarise non-null sample values into shape features."""
    vals = [v for v in values if v is not None]
    prof: dict = {"n": len(values), "nulls": len(values) - len(vals), "shape": "empty"}
    if not vals:
        return prof
    if all(isinstance(v, bool) for v in vals):
        prof["shape"] = "bool"
    elif all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
        prof["shape"] = "binary_int" if set(vals) <= {0, 1} and len(set(vals)) == 2 else "int"
        prof["min"], prof["max"] = min(vals), max(vals)
    elif all(isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) for v in vals):
        scales = [_decimal_scale(v) or 0 for v in vals]
        prof["shape"] = "decimal"
        prof["max_scale"] = max(scales)
        prof["max_int_digits"] = max(len(str(abs(int(Decimal(str(v)))))) for v in vals)
    elif all(_as_json(v) is not None for v in vals):
        prof["shape"] = "json"
        prof["nested"] = any(isinstance(x, (dict, list))
                             for v in vals for x in (_as_json(v).values() if isinstance(_as_json(v), dict) else _as_json(v)))
    elif all((t := _parse_ts(v)) is not None for v in vals):
        aware = [(_parse_ts(v).tzinfo is not None) for v in vals]
        if all(aware):
            prof["shape"] = "timestamp_tz"
        elif isinstance(vals[0], str) and all(len(v.strip()) == 10 for v in vals):
            prof["shape"] = "date"
        else:
            prof["shape"] = "timestamp"
    else:
        strs = [str(v) for v in vals]
        distinct = set(strs)
        prof["shape"] = "categorical" if len(distinct) <= max(3, len(strs) // 4) else "text"
        prof["distinct"] = sorted(distinct)[:8]
        prof["max_len"] = max(len(s) for s in strs)
    return prof


def describe_shape(prof: dict) -> str:
    s = prof["shape"]
    if s == "decimal":
        return f"decimal values, max scale {prof['max_scale']}, max integer digits {prof['max_int_digits']}"
    if s == "categorical":
        return "categorical strings " + "/".join(prof["distinct"])
    if s == "json":
        return "JSON documents" + (" with nested objects" if prof.get("nested") else "")
    if s == "binary_int":
        return "integers taking only values 0 and 1"
    return s.replace("_", " ")


# --------------------------------------------------------------------------
# Lossiness: is mapping a value with semantic type `src` to `dst` faithful?
# --------------------------------------------------------------------------

def lossy_reason(src: str, dst: str) -> str | None:
    """Return why converting semantic type src -> target type dst loses information."""
    if src == dst:
        return None
    sd, dd = parse_decimal_type(src), parse_decimal_type(dst)
    if sd and dd:
        if dd[1] < sd[1]:
            return f"scale reduced from {sd[1]} to {dd[1]}"
        if dd[0] - dd[1] < sd[0] - sd[1]:
            return "integer digits reduced"
        return None
    if sd and dst == "float64":
        return "fixed-precision decimal stored as binary float"
    if sd and dst == "string":
        return "decimal stored as text loses numeric type"
    if src == "timestamp_tz" and dst in ("timestamp", "string", "date"):
        return "time-zone information dropped"
    if src == "timestamp" and dst == "date":
        return "time of day dropped"
    if src == "bool" and dst in ("string", "int64"):
        return "boolean coerced to non-boolean type"
    if src == "json_string" and dst != "json_string":
        return "nested JSON not preserved"
    if src == "int64" and dst in ("float64",):
        return "large integers lose precision as float"
    if src in ("int64", "float64", "date", "timestamp") and dst == "string":
        return "typed value stored as untyped text"
    if src == "float64" and dst == "int64":
        return "fractional part truncated"
    if src == "string" and dst != "string":
        return "text coerced to a narrower type"
    return None


# --------------------------------------------------------------------------
# Arrow types and value conversion
# --------------------------------------------------------------------------

def arrow_type(t: str) -> pa.DataType:
    dec = parse_decimal_type(t)
    if dec:
        return pa.decimal128(*dec)
    return {
        "int64": pa.int64(), "float64": pa.float64(), "string": pa.string(),
        "bool": pa.bool_(), "date": pa.date32(), "timestamp": pa.timestamp("us"),
        "timestamp_tz": pa.timestamp("us", tz="UTC"), "json_string": pa.string(),
    }[t]


def to_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        if v in (0, 1):
            return bool(v)
        raise ValueError(f"not a boolean: {v!r}")
    s = str(v).strip().lower()
    if s in ("t", "true", "1", "y", "yes"):
        return True
    if s in ("f", "false", "0", "n", "no"):
        return False
    raise ValueError(f"not a boolean: {v!r}")


def to_datetime(v) -> datetime:
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    d = _parse_ts(v)
    if d is None:
        raise ValueError(f"not a timestamp: {v!r}")
    return d


def to_decimal(v) -> Decimal:
    if isinstance(v, bool):
        raise ValueError("bool is not a decimal")
    return Decimal(repr(v)) if isinstance(v, float) else Decimal(str(v))


# --------------------------------------------------------------------------
# Canonical comparison form, shared by source and target sides of a rule
# --------------------------------------------------------------------------

def canon(v, target_type: str):
    """Map a value to a comparable canonical form under target_type semantics.

    Timestamps are compared as UTC instants (naive values are read as UTC),
    numbers as normalised decimals, JSON as key-sorted documents.
    """
    if v is None:
        return None
    try:
        if target_type in ("timestamp", "timestamp_tz"):
            d = to_datetime(v)
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            return d.astimezone(timezone.utc).isoformat()
        if parse_decimal_type(target_type) or target_type in ("float64", "int64"):
            return str(to_decimal(v).normalize())
        if target_type == "bool":
            return to_bool(v)
        if target_type == "date":
            return (v if isinstance(v, date) else to_datetime(v)).isoformat()[:10]
        if target_type == "json_string":
            j = _as_json(v)
            return json.dumps(j, sort_keys=True, separators=(",", ":")) if j is not None else str(v)
    except (ValueError, InvalidOperation, TypeError):
        return f"<unparseable:{v!r}>"
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)
