"""Core data structures shared by every stage of the pipeline.

Notation follows Section III-A of the paper: a source schema S = <T, C, K, D>
(tables, typed columns, keys, sample rows), a target specification tau, a
column mapping mu, and a set of reconciliation rules rho.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class Column:
    name: str
    declared_type: str
    nullable: bool = True
    primary_key: bool = False


@dataclass
class ForeignKey:
    column: str
    ref_table: str
    ref_column: str


@dataclass
class Table:
    name: str
    columns: list[Column]
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    sample: list[dict] = field(default_factory=list)
    row_count: int | None = None

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)

    @property
    def primary_key(self) -> str | None:
        return next((c.name for c in self.columns if c.primary_key), None)


@dataclass
class TargetSpec:
    """Where and how a table is written (tau)."""
    uri: str                        # file:///dir, s3://bucket/prefix, gs://bucket/prefix
    fmt: str = "parquet"            # parquet | csv
    partition_by: str | None = None # source column used for partitioning
    partition_grain: str = "month"  # month | day | value
    rows_per_file: int = 200        # used when partition_by is None


# Canonical target types understood by the executor.
TARGET_TYPES = (
    "int64", "float64", "string", "bool", "date", "timestamp", "timestamp_tz",
    "json_string",
)  # plus decimal(p,s)

TRANSFORMS = (
    "none", "tz_to_utc", "parse_timestamp_tz", "int_to_bool", "json_canonical",
    "parse_date", "json_flatten", "trim", "lower",
    "mask",            # privacy: replace personal data with a masked form (see typesys.mask_value)
)

NULL_HANDLING = ("preserve", "empty_string", "zero", "drop_row")


@dataclass
class ColumnMapping:
    source: str | None              # None => derived field with no source column
    target: str
    target_type: str
    transform: str = "none"
    null_handling: str = "preserve"
    rationale: str = ""


@dataclass
class TableMapping:
    table: str
    columns: list[ColumnMapping]

    def for_source(self, name: str) -> ColumnMapping | None:
        return next((m for m in self.columns if m.source == name), None)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "TableMapping":
        return TableMapping(d["table"], [ColumnMapping(**c) for c in d["columns"]])


RULE_KINDS = (
    "row_count", "pk_unique", "partition_row_count", "schema_conformity",
    "null_count", "column_checksum", "sum_exact", "min_max", "distinct_count",
    "true_ratio", "timestamp_exact", "json_roundtrip",
)


@dataclass
class ReconRule:
    rule_id: str
    kind: str
    column: str | None = None       # source column name
    tolerance: float = 0.0
    origin: str = "deterministic"   # deterministic | generated | llm
    rationale: str = ""
    claimed_source_value: Any = None  # an LLM-asserted expected value, re-checked deterministically


@dataclass
class RuleResult:
    rule: ReconRule
    passed: bool
    source_value: Any = None
    target_value: Any = None
    detail: str = ""


@dataclass
class MigrationPlan:
    table: Table
    target: TargetSpec
    mapping: TableMapping
    rules: list[ReconRule]
    notes: list[str] = field(default_factory=list)
