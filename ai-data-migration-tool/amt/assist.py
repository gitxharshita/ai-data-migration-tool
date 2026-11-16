"""AI assistant features for the dashboard, layered on top of the deterministic pipeline.

The AI never decides on its own and never does the arithmetic:

* review_mapping   - reviews the rule-based mapping and flags only the columns it
                     disagrees with; the user accepts or keeps each suggestion, and an
                     accepted suggestion is re-checked by the same dry run as any mapping.
* find_sensitive   - a rule-based privacy scan (names and value patterns: e-mail, phone,
                     PAN, Aadhaar, card numbers...) that the AI review extends with
                     columns whose meaning only shows in the values (e.g. phone numbers
                     inside a free-text notes column).
* summarise_migration - a plain-English paragraph for a manager, written from the
                     reconciliation facts; every number in it is checked against those
                     facts and the text is replaced by a template if one does not match.
"""
from __future__ import annotations

import json
import re

from .llm import LLMClient, extract_json
from .models import NULL_HANDLING, TRANSFORMS, Table, TableMapping
from .typesys import is_valid_target_type

# --------------------------------------------------------------------------
# Rule-based privacy scan (instant, no AI)
# --------------------------------------------------------------------------

_NAME_HINTS = [
    (r"e[-_]?mail", "email address"),
    (r"phone|mobile|cell|contact_no|whatsapp", "phone number"),
    (r"\bpan\b|pan_?(no|number|card)", "PAN number"),
    (r"aadha?a?r", "Aadhaar number"),
    (r"passport", "passport number"),
    (r"ssn|social_security", "social security number"),
    (r"card_?(no|num|number)|credit_?card|cc_?num", "card number"),
    (r"iban|account_?(no|num|number)|acct_?no|ifsc", "bank account details"),
    (r"(^|_)(dob|birth_?date|date_?of_?birth)", "date of birth"),
    (r"(first|last|full|middle|given|family|sur)_?name|^name$|customer_name|patient_name|employee_name",
     "person's name"),
    (r"address|street|postcode|zip_?code|pincode", "postal address"),
    (r"(^|_)ip(_?addr(ess)?)?$", "IP address"),
    (r"salary|wage|income|net_pay|gross_pay", "salary / pay"),
    (r"diagnos|icd_?code|medical|prescription|drug", "health information"),
    (r"gender|religion|caste|ethnic", "sensitive personal attribute"),
]
_VALUE_PATTERNS = [
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"), "email address"),
    (re.compile(r"(?<!\d)(?:\+91[\s-]?)?[6-9]\d{9}(?!\d)"), "phone number"),
    (re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b"), "PAN number"),
    (re.compile(r"(?<!\d)\d{4}\s\d{4}\s\d{4}(?!\d)"), "Aadhaar number"),
    (re.compile(r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)"), "card number"),
]


def _luhn(digits: str) -> bool:
    d = [int(c) for c in digits if c.isdigit()]
    total = sum(d[-1::-2]) + sum(sum(divmod(2 * x, 10)) for x in d[-2::-2])
    return len(d) >= 13 and total % 10 == 0


def find_sensitive(table: Table) -> list[dict]:
    """Columns that look like personal data, from their names and sample values."""
    out = []
    for col in table.columns:
        name = col.name.lower()
        kind = next((k for pat, k in _NAME_HINTS if re.search(pat, name)), None)
        reason = f"the column name '{col.name}' suggests it" if kind else ""
        if kind is None:
            values = [str(r.get(col.name)) for r in table.sample if r.get(col.name) is not None]
            for rx, k in _VALUE_PATTERNS:
                hits = [v for v in values if rx.search(v) and (k != "card number" or _luhn(rx.search(v).group()))]
                if hits and len(hits) >= max(1, len(values) // 4):
                    kind, reason = k, f"{len(hits)} of {len(values)} sample values look like one"
                    break
        if kind:
            out.append({"column": col.name, "kind": kind, "reason": reason, "found_by": "rules"})
    return out


# --------------------------------------------------------------------------
# AI review: mapping second opinion + privacy (one call, to keep laptops fast)
# --------------------------------------------------------------------------

REVIEW_SYSTEM = """You review a column mapping made by a rule-based data-migration tool that copies a \
table into Parquet files. The rules are usually right. In "flags" list ONLY columns whose chosen type, \
transform or NULL handling you would CHANGE because it loses or misrepresents data (e.g. a 0/1 integer \
that is a yes/no flag, dates kept as text, money as a float). Never list a column you would leave as it is.

In "sensitive" list columns holding personal data worth masking before a cloud upload (names, e-mails, \
phones, ID numbers, addresses, health data, salaries), including personal data inside free text. \
Not plain IDs, codes, amounts, statuses or timestamps.

Types: int64, float64, decimal(p,s), string, bool, date, timestamp, timestamp_tz, json_string.
Transforms: none, tz_to_utc, parse_timestamp_tz, int_to_bool, json_canonical, parse_date, trim, lower.
Null handling: preserve, empty_string, zero, drop_row.

Reply with ONE short JSON object (reasons under 15 words; empty lists if nothing):
{"flags": [{"column": str, "type": str, "transform": str, "nulls": str, "reason": str}],
 "sensitive": [{"column": str, "kind": str}]}"""


def review_prompt(table: Table, mapping: TableMapping) -> str:
    lines = [f"Table: {table.name} ({table.row_count} rows)", "Columns (name | declared SQL type | "
             "sample values | chosen target type | transform | null handling):"]
    for col in table.columns:
        cm = mapping.for_source(col.name)
        vals = [r.get(col.name) for r in table.sample][:6]
        shown = ", ".join("NULL" if v is None else repr(v)[:40] for v in vals)
        chosen = f"{cm.target_type} | {cm.transform} | {cm.null_handling}" if cm else "(not mapped)"
        lines.append(f"- {col.name} | {col.declared_type}{' | PK' if col.primary_key else ''} | {shown} | {chosen}")
    return "\n".join(lines)


def review_mapping(client: LLMClient, table: Table, mapping: TableMapping) -> dict:
    """Ask the AI for a second opinion. Returns validated flags and sensitive columns."""
    raw = extract_json(client.complete(REVIEW_SYSTEM, review_prompt(table, mapping), max_tokens=500))
    names = {c.name for c in table.columns}
    flags = []
    for f in raw.get("flags") or []:
        if not isinstance(f, dict):
            continue
        col = str(f.get("column") or "")
        cm = mapping.for_source(col)
        if col not in names or cm is None:
            continue                        # AI named a column that does not exist
        t = str(f.get("type") or f.get("suggested_type") or cm.target_type).strip().lower().replace(" ", "")
        tr = str(f.get("transform") or f.get("suggested_transform") or cm.transform).strip()
        nh = str(f.get("nulls") or f.get("suggested_null_handling") or cm.null_handling).strip()
        if not is_valid_target_type(t) or tr not in TRANSFORMS or nh not in NULL_HANDLING \
                or tr in ("json_flatten", "mask"):
            continue                        # not something the executor can do (masking is the user's call)
        if (t, tr, nh) == (cm.target_type, cm.transform, cm.null_handling):
            continue                        # "suggestion" identical to the current mapping
        flags.append({"column": col, "current": {"target_type": cm.target_type, "transform": cm.transform,
                                                 "null_handling": cm.null_handling},
                      "suggested": {"target_type": t, "transform": tr, "null_handling": nh},
                      "reason": str(f.get("reason") or "")[:300]})
    sensitive = []
    for s in raw.get("sensitive") or []:
        if isinstance(s, dict) and str(s.get("column")) in names:
            sensitive.append({"column": str(s["column"]), "kind": str(s.get("kind") or "personal data")[:60],
                              "reason": str(s.get("reason") or "the AI recognised it from the values")[:300],
                              "found_by": "AI"})
    return {"flags": flags, "sensitive": sensitive}


def with_previews(table: Table, found: list[dict]) -> list[dict]:
    """Add a before/after example of masking to each finding, from the first sample value."""
    from .typesys import mask_value
    out = []
    for s in found:
        v = next((r.get(s["column"]) for r in table.sample if r.get(s["column"]) not in (None, "")), None)
        out.append({**s, "example": None if v is None else {"before": str(v)[:60], "after": str(mask_value(v))[:60]}})
    return out


def mask_blockers(table: Table, partition_by: str | None) -> dict:
    """Columns that cannot be masked, and why (masking them would break the migration)."""
    out = {}
    for c in table.columns:
        if c.primary_key:
            out[c.name] = "it is the table's ID column"
        elif partition_by and c.name == partition_by:
            out[c.name] = "it is used to split the files by month"
    return out


def apply_masks(mapping: TableMapping, table: Table, columns: list, partition_by: str | None) -> list[str]:
    """Mask the chosen columns: stored as text with personal parts replaced (typesys.mask_value)."""
    blocked = mask_blockers(table, partition_by)
    done = []
    for col in columns or []:
        cm = mapping.for_source(str(col))
        if cm is None:
            continue
        if col in blocked:
            raise ValueError(f"'{col}' cannot be masked because {blocked[col]}")
        cm.target_type, cm.transform, cm.null_handling = "string", "mask", "preserve"
        cm.rationale = "masked for privacy at the user's request"
        done.append(str(col))
    return done


def merge_sensitive(rule_based: list[dict], ai: list[dict]) -> list[dict]:
    """Rule findings first; the AI adds columns the rules did not catch."""
    seen = {s["column"] for s in rule_based}
    return rule_based + [s for s in ai if s["column"] not in seen]


def apply_overrides(mapping: TableMapping, overrides: dict) -> list[str]:
    """Apply accepted suggestions {column: {target_type, transform, null_handling}}; returns what changed."""
    changed = []
    for col, o in (overrides or {}).items():
        cm = mapping.for_source(col)
        if cm is None or not isinstance(o, dict):
            continue
        t, tr, nh = o.get("target_type", cm.target_type), o.get("transform", cm.transform), \
            o.get("null_handling", cm.null_handling)
        if not is_valid_target_type(t) or tr not in TRANSFORMS or nh not in NULL_HANDLING:
            raise ValueError(f"invalid change for column {col}")
        cm.target_type, cm.transform, cm.null_handling = t, tr, nh
        cm.rationale = "changed by the user after the AI review"
        changed.append(f"{col}: {t}, {tr}, NULL {nh}")
    return changed


# --------------------------------------------------------------------------
# Plain-English summary, with every number checked against the facts
# --------------------------------------------------------------------------

SUMMARY_SYSTEM = """You write a short summary of a completed database-to-cloud migration for a \
non-technical manager. Use ONLY the facts given; do not invent numbers, causes or recommendations \
that the facts do not support. Write every number as digits, exactly as in the facts. Three to five plain sentences, no bullet points, no jargon \
(say "checks", not "reconciliation rules"). If any check failed, say clearly that the copy should \
not be trusted until it is fixed, and name the affected columns. Mention masked columns, and end with \
a gentle reminder if "personal_data_left_unmasked_reminder" lists any columns. Skip empty facts.
Reply with ONE JSON object: {"summary": str}"""


def migration_facts(table: str, rows_read: int, rows_written: int, files: int, destination: str,
                    summary: dict, results: list[dict], adjusted_columns: list[str],
                    sensitive: list[str], masked: list[str] | None = None) -> dict:
    failed = [r for r in results if not r.get("passed")]
    masked = masked or []
    sensitive = [s for s in sensitive if s not in masked]
    return {
        "table": table, "rows_read": rows_read, "rows_written": rows_written, "files_written": files,
        "destination": destination, "checks_run": summary.get("rules"), "checks_failed": summary.get("failed"),
        "status": summary.get("status"),
        "failed_checks": [f"{r['rule']['kind']} on {r['rule'].get('column') or 'whole table'}" for r in failed][:10],
        "columns_with_misleading_declared_types_corrected": adjusted_columns[:10],
        "columns_masked_for_privacy": masked[:10],
        "personal_data_left_unmasked_reminder": sensitive[:10],
    }


def _numbers(text: str) -> set[str]:
    return {n.replace(",", "") for n in re.findall(r"\d[\d,]*(?:\.\d+)?", text)}


def template_summary(f: dict) -> str:
    ok = f["status"] == "PASS"
    s = (f"The table '{f['table']}' was copied to {f['destination']}: {f['rows_read']} rows were read and "
         f"{f['rows_written']} written into {f['files_written']} file(s). ")
    s += (f"All {f['checks_run']} checks passed, so the copy matches the source exactly. " if ok else
          f"{f['checks_failed']} of {f['checks_run']} checks failed, so the copy should not be trusted until "
          f"this is fixed ({'; '.join(f['failed_checks'])}). ")
    if f["columns_with_misleading_declared_types_corrected"]:
        s += (f"Some columns had misleading declared types and were stored correctly: "
              f"{', '.join(f['columns_with_misleading_declared_types_corrected'])}. ")
    if f["columns_masked_for_privacy"]:
        s += f"Personal data was masked in: {', '.join(f['columns_masked_for_privacy'])}. "
    if f["personal_data_left_unmasked_reminder"]:
        s += f"Reminder: these columns may hold personal data and were not masked: {', '.join(f['personal_data_left_unmasked_reminder'])}."
    return s.strip()


def summarise_migration(client: LLMClient, facts: dict) -> dict:
    """AI-written summary; falls back to the template if it states a number not in the facts."""
    text = str(extract_json(client.complete(SUMMARY_SYSTEM, json.dumps(facts, indent=1), max_tokens=600))
               .get("summary") or "").strip()
    allowed = _numbers(json.dumps(facts))
    bad = sorted(_numbers(text) - allowed)
    if not text or bad:
        return {"text": template_summary(facts), "by": "template",
                "note": ("The AI summary mentioned numbers that are not in the report "
                         f"({', '.join(bad)}), so a standard summary is shown instead." if bad else
                         "The AI returned no summary, so a standard summary is shown instead.")}
    return {"text": text, "by": "AI", "note": "Every number in this summary was checked against the report."}
