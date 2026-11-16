"""Synthetic migration corpus (Section III-E): six relational schemas,
36 tables, four difficulty tiers (9 Easy / 11 Medium / 10 Hard / 6 Extra Hard).

Each column is declared by a *kind* that fixes its declared SQL type, its
value generator and its gold mapping. "Deceptive" kinds (intbool, textts,
textjson, numeric_money) have a declared type that does not reveal the
correct target type; only the sample values do.
"""
from __future__ import annotations

import json
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from amt.models import ColumnMapping, TableMapping, TargetSpec

ROWS = 300
EPOCH = datetime(2024, 1, 1)
OFFSETS = [timedelta(0), timedelta(hours=5, minutes=30), timedelta(hours=-4), timedelta(hours=1)]
WORDS = "alpha beta gamma delta omega nova terra lumen vega orion atlas zephyr kilo metro".split()
FIRST = "Aarav Diya Kabir Meera Rohan Ananya Ishaan Sara Vikram Nisha Arjun Priya".split()
LAST = "Sharma Gupta Iyer Khan Mehta Reddy Das Singh Nair Joshi Kapoor Bose".split()


def _ts(rng, aware=True):
    d = EPOCH + timedelta(days=rng.randint(0, 364), seconds=rng.randint(0, 86399))
    if not aware:
        return d.strftime("%Y-%m-%d %H:%M:%S")
    return d.replace(tzinfo=timezone(rng.choice(OFFSETS))).isoformat()


def _money(rng, hi=50000):
    return str(Decimal(rng.randint(100, hi * 100)) / 100)


def _json(rng, i):
    return json.dumps({"source": rng.choice(["web", "app", "api"]), "tags": rng.sample(WORDS, 2),
                       "meta": {"v": rng.randint(1, 5), "region": rng.choice(["north", "south", "west"])},
                       "score": rng.randint(0, 100)})


def _null(rng, v, p=0.15):
    return None if rng.random() < p else v


# kind -> (declared type, gold target_type, gold transform, NOT NULL?, edge case?, generator)
KINDS = {
    "pk":       ("INTEGER", "int64", "none", True, False, lambda r, i: i + 1),
    "int":      ("INTEGER", "int64", "none", True, False, lambda r, i: r.randint(0, 5000)),
    "qty":      ("SMALLINT", "int64", "none", True, False, lambda r, i: r.randint(2, 40)),
    "float":    ("DOUBLE PRECISION", "float64", "none", True, False, lambda r, i: round(r.uniform(0, 12), 3)),
    "str":      ("VARCHAR(80)", "string", "none", True, False, lambda r, i: f"{r.choice(WORDS)}-{r.randint(100, 999)}"),
    "name":     ("VARCHAR(120)", "string", "none", True, False, lambda r, i: f"{r.choice(FIRST)} {r.choice(LAST)}"),
    "email":    ("VARCHAR(255)", "string", "none", True, False, lambda r, i: f"user{i}@example.org"),
    "text":     ("TEXT", "string", "none", True, False, lambda r, i: " ".join(r.choices(WORDS, k=8))),
    "nstr":     ("VARCHAR(200)", "string", "none", False, False, lambda r, i: _null(r, " ".join(r.choices(WORDS, k=4)), 0.25)),
    "date":     ("DATE", "date", "parse_date", True, False, lambda r, i: (EPOCH + timedelta(days=r.randint(0, 900))).date().isoformat()),
    "ts":       ("TIMESTAMP", "timestamp", "none", True, False, lambda r, i: _ts(r, aware=False)),
    "bool":     ("BOOLEAN", "bool", "none", True, False, lambda r, i: r.randint(0, 1)),
    # type-system edge cases
    "tstz":     ("TIMESTAMP WITH TIME ZONE", "timestamp_tz", "tz_to_utc", True, True, lambda r, i: _ts(r)),
    "ntstz":    ("TIMESTAMP WITH TIME ZONE", "timestamp_tz", "tz_to_utc", False, True, lambda r, i: _null(r, _ts(r), 0.3)),
    "money":    ("DECIMAL(18,2)", "decimal(18,2)", "none", True, True, lambda r, i: _money(r)),
    "nmoney":   ("DECIMAL(18,2)", "decimal(18,2)", "none", False, True, lambda r, i: _null(r, _money(r, 5000))),
    "rate":     ("DECIMAL(10,4)", "decimal(10,4)", "none", True, True, lambda r, i: str(Decimal(r.randint(1, 999999)) / 10000)),
    "nbool":    ("BOOLEAN", "bool", "none", False, True, lambda r, i: _null(r, r.randint(0, 1))),
    "enum":     ("ENUM", "string", "none", True, True, None),
    "json":     ("JSONB", "json_string", "json_canonical", True, True, lambda r, i: _json(r, i)),
    # deceptive edge cases: declared type hides the true semantic type
    "numeric_money": ("NUMERIC", "decimal(18,2)", "none", True, True, lambda r, i: _money(r)),
    "intbool":  ("SMALLINT", "bool", "int_to_bool", True, True, lambda r, i: r.randint(0, 1)),
    # trap: a genuine small count that happens to take only 0/1 values -> stays int64
    "binint":   ("SMALLINT", "int64", "none", True, True, lambda r, i: int(r.random() < 0.3)),
    "textts":   ("VARCHAR(40)", "timestamp_tz", "parse_timestamp_tz", True, True, lambda r, i: _ts(r)),
    "textjson": ("TEXT", "json_string", "json_canonical", True, True, lambda r, i: _json(r, i)),
}


@dataclass
class TableSpec:
    name: str
    tier: str
    columns: list[tuple]        # (name, kind[, enum values | fk table])
    partition_by: str | None = None


def T(name, tier, cols, part=None):
    return TableSpec(name, tier, [tuple(c.split(":", 2)) for c in cols], part)


SCHEMAS: dict[str, list[TableSpec]] = {
    "ecommerce": [
        T("customers", "Easy", ["id:pk", "full_name:name", "email:email", "city:str", "signup_date:date"]),
        T("products", "Medium", ["id:pk", "sku:str", "title:text", "unit_price:money", "stock:qty"]),
        T("orders", "Hard", ["id:pk", "customer_id:fk:customers", "placed_at:tstz",
                             "status:enum:pending|shipped|delivered|cancelled", "total_amount:money", "gift_wrap:nbool"], "placed_at"),
        T("order_items", "Easy", ["id:pk", "order_id:fk:orders", "product_id:fk:products", "quantity:qty", "line_no:int"]),
        T("payments", "Hard", ["id:pk", "order_id:fk:orders", "paid_at:tstz", "amount:money",
                               "method:enum:card|upi|netbanking|cod", "is_refunded:intbool"], "paid_at"),
        T("carts", "Extra Hard", ["id:pk", "customer_id:fk:customers", "updated_at:tstz", "items:json",
                                  "cart_total:numeric_money", "is_abandoned:intbool"], "updated_at"),
    ],
    "healthcare": [
        T("patients", "Easy", ["id:pk", "mrn:str", "full_name:name", "birth_date:date", "blood_group:str"]),
        T("providers", "Easy", ["id:pk", "full_name:name", "specialty:str", "license_no:str"]),
        T("encounters", "Medium", ["id:pk", "patient_id:fk:patients", "provider_id:fk:providers",
                                   "encounter_ts:tstz", "notes:nstr"]),
        T("claims", "Hard", ["id:pk", "encounter_id:fk:encounters", "submitted_at:tstz",
                             "status:enum:submitted|approved|denied|pending", "billed_amount:money", "is_denied:nbool"], "submitted_at"),
        T("claim_lines", "Medium", ["id:pk", "claim_id:fk:claims", "cpt_code:str", "units:qty", "line_amount:money"]),
        T("prescriptions", "Hard", ["id:pk", "encounter_id:fk:encounters", "drug:str", "dosage_mg:rate",
                                    "prescribed_at:textts", "controlled:intbool"]),
        T("lab_results", "Extra Hard", ["id:pk", "encounter_id:fk:encounters", "collected_at:tstz", "panel:json",
                                        "result_value:rate", "abnormal:nbool"], "collected_at"),
    ],
    "hr_payroll": [
        T("departments", "Easy", ["id:pk", "dept_name:str", "cost_center:str"]),
        T("employees", "Medium", ["id:pk", "dept_id:fk:departments", "full_name:name", "email:email",
                                  "hired_on:date", "salary:money"]),
        T("payroll_runs", "Hard", ["id:pk", "run_at:tstz", "period:str", "gross_total:money", "is_final:intbool"], "run_at"),
        T("payslips", "Hard", ["id:pk", "run_id:fk:payroll_runs", "employee_id:fk:employees", "gross:numeric_money",
                               "tax:money", "net:money", "bonus_eligible:nbool", "arrears_months:binint"]),
        T("leave_requests", "Medium", ["id:pk", "employee_id:fk:employees", "start_date:date", "end_date:date",
                                       "leave_type:enum:casual|sick|earned"]),
        T("timesheets", "Easy", ["id:pk", "employee_id:fk:employees", "work_date:date", "hours:float", "project_code:str"]),
    ],
    "logistics": [
        T("warehouses", "Easy", ["id:pk", "code:str", "city:str", "capacity:int"]),
        T("shipments", "Hard", ["id:pk", "warehouse_id:fk:warehouses", "shipped_at:tstz", "delivered_at:ntstz",
                                "weight_kg:rate", "status:enum:created|in_transit|delivered|returned"], "shipped_at"),
        T("tracking_events", "Extra Hard", ["id:pk", "shipment_id:fk:shipments", "event_at:textts", "payload:textjson",
                                            "exception_raised:intbool"], "event_at"),
        T("vehicles", "Medium", ["id:pk", "reg_no:str", "model:str", "fuel_cost_per_km:rate"]),
        T("drivers", "Medium", ["id:pk", "full_name:name", "phone:str", "is_active:intbool"]),
        T("routes", "Extra Hard", ["id:pk", "origin:str", "destination:str", "waypoints:json",
                                   "distance_km:numeric_money", "last_optimized_at:tstz"]),
    ],
    "subscription": [
        T("plans", "Medium", ["id:pk", "plan_name:str", "monthly_price:money", "seats:int"]),
        T("subscribers", "Easy", ["id:pk", "email:email", "country:str", "created_on:date"]),
        T("subscriptions", "Hard", ["id:pk", "subscriber_id:fk:subscribers", "plan_id:fk:plans", "started_at:tstz",
                                    "auto_renew:nbool", "status:enum:trial|active|paused|cancelled"], "started_at"),
        T("invoices", "Hard", ["id:pk", "subscription_id:fk:subscriptions", "issued_at:tstz", "amount_due:numeric_money",
                               "tax_rate:rate", "is_paid:intbool"], "issued_at"),
        T("usage_events", "Extra Hard", ["id:pk", "subscription_id:fk:subscriptions", "occurred_at:tstz",
                                         "attributes:json", "units_consumed:rate", "billable:nbool"], "occurred_at"),
    ],
    "support": [
        T("agents", "Easy", ["id:pk", "full_name:name", "team:str", "email:email"]),
        T("accounts", "Medium", ["id:pk", "account_name:name", "email:email", "tier:enum:free|pro|enterprise"]),
        T("tickets", "Extra Hard", ["id:pk", "account_id:fk:accounts", "opened_at:tstz", "closed_at:ntstz",
                                    "priority:enum:low|medium|high|urgent", "metadata:json", "is_escalated:intbool", "reopen_count:binint"], "opened_at"),
        T("messages", "Hard", ["id:pk", "ticket_id:fk:tickets", "sent_at:tstz", "sender:enum:agent|customer|bot",
                               "body:text"], "sent_at"),
        T("sla_policies", "Medium", ["id:pk", "policy_name:str", "response_minutes:int", "penalty:money"]),
        T("csat_surveys", "Medium", ["id:pk", "ticket_id:fk:tickets", "score:int", "comment:nstr", "anonymous:intbool"]),
    ],
}


def column_kind(col: tuple) -> str:
    return "int" if col[1] == "fk" else col[1]


def declared(col: tuple) -> str:
    return KINDS[column_kind(col)][0]


def gold_mapping(ts: TableSpec) -> TableMapping:
    cols = []
    for c in ts.columns:
        _, gt, gtr, *_ = KINDS[column_kind(c)]
        cols.append(ColumnMapping(c[0], c[0], gt, gtr, "preserve", "gold"))
    return TableMapping(ts.name, cols)


def edge_cases(ts: TableSpec) -> list[str]:
    return [c[0] for c in ts.columns if KINDS[column_kind(c)][4]]


def target_spec(ts: TableSpec, uri: str, fmt: str = "parquet") -> TargetSpec:
    return TargetSpec(uri, fmt, ts.partition_by, "month", rows_per_file=100)


def build_schema_db(schema: str, path: Path, seed: int = 7) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    for ts in SCHEMAS[schema]:
        defs, fks = [], []
        for c in ts.columns:
            kind = column_kind(c)
            decl, _, _, notnull, _, _ = KINDS[kind]
            d = f'"{c[0]}" {decl}' + (" PRIMARY KEY" if kind == "pk" else " NOT NULL" if notnull else "")
            defs.append(d)
            if c[1] == "fk":
                fks.append(f'FOREIGN KEY ("{c[0]}") REFERENCES "{c[2]}"("id")')
        conn.execute(f'CREATE TABLE "{ts.name}" ({", ".join(defs + fks)})')
        rng = random.Random(f"{seed}:{schema}:{ts.name}")
        rows = []
        for i in range(ROWS):
            row = []
            for c in ts.columns:
                kind = column_kind(c)
                if c[1] == "fk":
                    row.append(rng.randint(1, ROWS))
                elif kind == "enum":
                    row.append(rng.choice(c[2].split("|")))
                else:
                    row.append(KINDS[kind][5](rng, i))
            rows.append(row)
        conn.executemany(f'INSERT INTO "{ts.name}" VALUES ({",".join("?" * len(ts.columns))})', rows)
    conn.commit()
    conn.close()


def all_tables():
    for schema, tables in SCHEMAS.items():
        for ts in tables:
            yield schema, ts
