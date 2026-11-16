"""Job files: migrate many tables from one YAML (or JSON) file, ready for schedulers.

    python -m amt init-job --source sqlite:///shop.db --target C:/lake > shop.yaml
    python -m amt run shop.yaml                  # full load first time, incremental after
    python -m amt run shop.yaml --rerun-failed   # rewrite only partitions that failed last time
    python -m amt run shop.yaml --full           # ignore the saved watermarks
    python -m amt verify shop.yaml               # re-check what is already there, write nothing

Exit codes (for Airflow, Azure Data Factory, cron, CI):
    0  every table passed (or had no new rows)
    1  at least one check failed: the data must not be trusted
    2  the job could not run (bad job file, connection or permission problem)
"""
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from .models import TargetSpec

FORMATS = ("parquet", "csv", "delta", "iceberg")
TABLE_KEYS = {"name", "partition_by", "partition_grain", "incremental", "mask", "overrides", "path", "strategy"}
JOB_KEYS = {"name", "source", "target", "format", "strategy", "rows_per_file", "chunk_rows", "catalog",
            "storage_options", "state_file", "report_dir", "tables", "crosscheck", "precedents"}


class JobError(Exception):
    """The job cannot run (exit code 2). The message is meant for a person."""


# --------------------------------------------------------------------------
# Loading and checking the job file
# --------------------------------------------------------------------------

def _expand_env(v):
    """${VAR} in any string value is replaced by the environment variable (keeps secrets out of the file)."""
    if isinstance(v, str):
        import re
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), v)
    if isinstance(v, dict):
        return {k: _expand_env(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_expand_env(x) for x in v]
    return v


def load_job(path: str) -> dict:
    p = Path(path)
    if not p.is_file():
        raise JobError(f"Job file not found: {path}")
    text = p.read_text(encoding="utf-8")
    try:
        if p.suffix.lower() == ".json":
            job = json.loads(text)
        else:
            import yaml
            job = yaml.safe_load(text)
    except Exception as e:
        raise JobError(f"Could not read {path}: {e}") from e
    if not isinstance(job, dict):
        raise JobError(f"{path} must contain a mapping of settings (source, target, tables, ...).")
    job = _expand_env(job)
    unknown = set(job) - JOB_KEYS
    if unknown:
        raise JobError(f"Unknown setting(s) in {path}: {', '.join(sorted(unknown))}. "
                       f"Allowed: {', '.join(sorted(JOB_KEYS))}.")
    for key in ("source", "tables"):
        if not job.get(key):
            raise JobError(f"The job file needs '{key}'.")
    fmt = job.setdefault("format", "parquet")
    if fmt not in FORMATS:
        raise JobError(f"format must be one of {', '.join(FORMATS)} (got {fmt!r}).")
    if fmt == "iceberg":
        if not isinstance(job.get("catalog"), dict) or not job["catalog"].get("type"):
            raise JobError("format: iceberg needs a catalog, e.g. catalog: {type: sql, uri: sqlite:///catalog.db, "
                           "warehouse: C:/lake/warehouse}")
    elif not job.get("target"):
        raise JobError(f"format: {fmt} needs a 'target' (a folder, s3://, abfss:// or gs:// location).")
    name = job.setdefault("name", p.stem)
    job.setdefault("strategy", "h2")
    job.setdefault("rows_per_file", 100_000)
    job.setdefault("chunk_rows", 50_000)
    job.setdefault("crosscheck", True)
    job.setdefault("state_file", str(p.parent / ".amt_state" / f"{name}.json"))
    job.setdefault("report_dir", str(p.parent / "reports" / name))
    tables = job["tables"]
    if tables == "*" or tables == ["*"]:
        job["tables"] = "*"
    elif isinstance(tables, list):
        norm = []
        for t in tables:
            t = {"name": t} if isinstance(t, str) else t
            if not isinstance(t, dict) or not t.get("name"):
                raise JobError(f"Each entry under 'tables' needs a name (got {t!r}).")
            bad = set(t) - TABLE_KEYS
            if bad:
                raise JobError(f"Unknown setting(s) for table {t['name']}: {', '.join(sorted(bad))}. "
                               f"Allowed: {', '.join(sorted(TABLE_KEYS))}.")
            inc = t.get("incremental")
            if inc is not None:
                if not isinstance(inc, dict) or not inc.get("column"):
                    raise JobError(f"incremental for {t['name']} needs a column, e.g. incremental: {{column: updated_at}}")
                inc.setdefault("mode", "append")
                if inc["mode"] not in ("append", "upsert"):
                    raise JobError(f"incremental mode for {t['name']} must be append or upsert.")
                if inc["mode"] == "upsert" and fmt != "delta":
                    raise JobError(f"incremental mode upsert ({t['name']}) needs format: delta; "
                                   "plain files can only be appended to.")
            norm.append(t)
        job["tables"] = norm
    else:
        raise JobError("'tables' must be a list of table names / settings, or \"*\" for all tables.")
    return job


# --------------------------------------------------------------------------
# Watermark state (what an incremental run has already copied)
# --------------------------------------------------------------------------

def _enc(v):
    if isinstance(v, datetime):
        return {"t": "datetime", "v": v.isoformat()}
    if isinstance(v, date):
        return {"t": "date", "v": v.isoformat()}
    if isinstance(v, Decimal):
        return {"t": "decimal", "v": str(v)}
    return {"t": type(v).__name__, "v": v}


def _dec(d):
    if d is None:
        return None
    t, v = d["t"], d["v"]
    return {"datetime": datetime.fromisoformat, "date": date.fromisoformat, "decimal": Decimal}.get(t, lambda x: x)(v)


def load_state(path: str) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_state(path: str, state: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, p)


# --------------------------------------------------------------------------
# Running
# --------------------------------------------------------------------------

@dataclass
class TableOutcome:
    table: str
    status: str                     # PASS | FAIL | SKIPPED | ERROR
    mode: str = ""
    rows_read: int = 0
    rows_written: int = 0
    seconds: float = 0.0
    location: str = ""
    message: str = ""
    failed_checks: list = field(default_factory=list)
    failed_partitions: list = field(default_factory=list)
    partitions: int = 0
    crosscheck: str = ""
    window: dict | None = None
    register_sql: str = ""
    results: list = field(default_factory=list)


def order_by_dependencies(source, names: list[str]) -> list[str]:
    """Parents before children (a table that references another comes after it)."""
    deps = {}
    for n in names:
        try:
            deps[n] = {fk.ref_table for fk in source._foreign_keys(n)} & set(names) - {n}
        except Exception:
            deps[n] = set()
    done, out = set(), []
    while len(out) < len(names):
        ready = sorted(n for n in names if n not in done and deps[n] <= done)
        if not ready:                               # a cycle: keep the remaining order as listed
            ready = [n for n in names if n not in done]
        for n in ready:
            done.add(n)
            out.append(n)
    return out


def _agent(strategy: str, job: dict):
    from .agent import AgentManager
    from .mappers import DeclaredTypeMapper, RAGMapper, SampleAwareMapper
    from .retriever import PrecedentStore
    store = PrecedentStore.load(job["precedents"]) if job.get("precedents") else PrecedentStore()
    if strategy == "h2":
        return AgentManager(SampleAwareMapper(store), verify=True)
    if strategy == "h1":
        return AgentManager(DeclaredTypeMapper(), verify=False, rule_mode="schema_aware")
    if strategy == "s3":
        from .llm import make_client
        return AgentManager(RAGMapper(make_client(), store), verify=True, rule_mode="schema_aware")
    raise JobError(f"strategy must be h2 (default), h1 or s3 (got {strategy!r}).")


def _sink(job: dict, tconf: dict, table: str, spec: TargetSpec, run_id: str, target):
    from .stream import DeltaSink, FileSink, IcebergSink
    fmt = job["format"]
    path = tconf.get("path") or table
    if fmt in ("parquet", "csv"):
        return FileSink(target, path, spec, run_id)
    if fmt == "delta":
        base = str(job["target"]).rstrip("/\\")
        return DeltaSink(f"{base}/{path}", spec, run_id, job.get("storage_options"))
    cat = dict(job["catalog"])
    ns = cat.pop("namespace", "default")
    return IcebergSink(cat, f"{ns}.{path}", spec, run_id)


def _register_sql(job: dict, tconf: dict, table: str, sink) -> str:
    """How to query the result from Spark / Databricks / Athena (Delta), or where Iceberg registered it."""
    if job["format"] == "delta":
        loc = sink.uri.replace("\\", "/")
        return (f"-- Spark / Databricks (Unity Catalog or Hive metastore):\n"
                f"CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{loc}';\n"
                f"-- DuckDB: SELECT * FROM delta_scan('{loc}');")
    if job["format"] == "iceberg":
        return f"-- registered in the {job['catalog'].get('type')} catalog as {sink.identifier}"
    return ""


def _greater(a, b) -> bool:
    try:
        return a > b
    except TypeError:
        return str(a) > str(b)


def run_job(job: dict, *, full: bool = False, rerun_failed: bool = False, only: list[str] | None = None,
            verify: bool = False, dry_run: bool = False, log=print) -> tuple[int, list[TableOutcome], dict]:
    """Run a loaded job. Returns (exit code, per-table outcomes, report)."""
    from .agent import execution_verification
    from .assist import apply_masks, apply_overrides
    from .connectors import open_source, open_target
    from .stream import new_run_id, run_stream

    t_job = time.perf_counter()
    try:
        source = open_source(job["source"] if "://" in job["source"] else f"sqlite:///{job['source']}")
    except Exception as e:
        raise JobError(f"Could not connect to the source database: {e}") from e
    target = None
    if job["format"] in ("parquet", "csv"):
        try:
            target = open_target(job["target"])
        except Exception as e:
            raise JobError(f"Could not open the target {job['target']}: {e}") from e
    available = source.list_tables()
    confs = {t: {"name": t} for t in available} if job["tables"] == "*" else {t["name"]: t for t in job["tables"]}
    missing = [t for t in confs if t not in available]
    if missing:
        raise JobError(f"Table(s) not found in the source: {', '.join(missing)}. "
                       f"Available: {', '.join(available) or '(none)'}.")
    names = [n for n in confs if not only or n in only]
    names = order_by_dependencies(source, names)
    state = load_state(job["state_file"])
    run_id = new_run_id()
    outcomes: list[TableOutcome] = []
    log(f"job {job['name']}: {len(names)} table(s) -> {job['format']}  (run {run_id})")

    for name in names:
        tconf = confs[name]
        t0 = time.perf_counter()
        st = state.setdefault(name, {})
        try:
            spec = TargetSpec(job.get("target") or "", "parquet" if job["format"] != "csv" else "csv",
                              tconf.get("partition_by"), tconf.get("partition_grain", "month"),
                              int(job["rows_per_file"]))
            agent = _agent(tconf.get("strategy", job["strategy"]), job)
            plog: list[str] = []
            plan = agent.plan(source, name, spec, plog)
            if tconf.get("overrides"):
                apply_overrides(plan.mapping, tconf["overrides"])
            if tconf.get("mask"):
                apply_masks(plan.mapping, plan.table, tconf["mask"], spec.partition_by)
            if tconf.get("overrides") or tconf.get("mask"):
                problems = execution_verification(plan.table, plan.mapping, spec)
                if problems:
                    raise JobError("the dry run failed after your overrides / masks: " + "; ".join(problems[:3]))
                plan.rules = agent.build_rules(plan.table, plan.mapping, spec, [])
            sink = _sink(job, tconf, name, spec, run_id, target)
            inc = tconf.get("incremental")
            mode, window, parts = "overwrite", None, None
            if verify:
                # the whole table, as of the last verified load (newer source rows are not copied yet)
                if inc and st.get("watermark"):
                    window = (inc["column"], None, _dec(st["watermark"]))
            elif rerun_failed:
                if not st.get("failed_partitions"):
                    outcomes.append(TableOutcome(name, "SKIPPED", "rerun-failed",
                                                 message="nothing failed last time"))
                    log(f"  {name:<24} SKIPPED  nothing failed last time")
                    continue
                mode, parts = "partitions", set(st["failed_partitions"])
                if inc and st.get("watermark"):
                    # rewrite the partition as of the last verified run, so the next
                    # incremental run does not copy the newer rows twice
                    window = (inc["column"], None, _dec(st["watermark"]))
            elif inc:
                col = inc["column"]
                upto = source.max_value(name, col)
                after = None if full else _dec(st.get("watermark"))
                if upto is None or (after is not None and not _greater(upto, after)):
                    outcomes.append(TableOutcome(name, "SKIPPED", "incremental", message="no new rows since the last run"))
                    log(f"  {name:<24} SKIPPED  no new rows since the last run")
                    continue
                window = (col, after, upto)
                if after is not None:
                    mode = "upsert" if inc["mode"] == "upsert" else "append"
                if mode == "upsert":
                    sink.keys = [plan.table.primary_key] if plan.table.primary_key else []
                    if not sink.keys:
                        raise JobError(f"upsert needs a primary key, and {name} has none.")
            if dry_run:
                outcomes.append(TableOutcome(name, "SKIPPED", mode, message="dry run: planned only, nothing written",
                                             window=None if not window else {"column": window[0]}))
                log(f"  {name:<24} PLANNED  {mode}; {len(plan.mapping.columns)} columns, {len(plan.rules)} checks")
                continue

            def progress(msg, _n=name):
                log(f"  {_n:<24} ...      {msg}")

            rep = run_stream(source, plan.table, plan.mapping, spec, sink, plan.rules, mode=mode,
                             window=window, partitions=parts, chunk_rows=int(job["chunk_rows"]), run_id=run_id,
                             crosscheck=bool(job["crosscheck"]), progress=progress, verify_only=verify)
            failed = [f"{r.rule.rule_id}: {r.detail}" for r in rep.results + rep.crosscheck if not r.passed]
            out = TableOutcome(name, rep.summary["status"], mode if not verify else "verify", rep.rows_read,
                               rep.rows_written, rep.seconds, rep.location, failed_checks=failed,
                               failed_partitions=rep.failed_partitions, partitions=len(rep.partitions),
                               window=rep.window, register_sql=_register_sql(job, tconf, name, sink))
            ok_db = sum(r.passed for r in rep.crosscheck)
            out.crosscheck = f"{ok_db}/{len(rep.crosscheck)} confirmed by the database" if rep.crosscheck else ""
            from .stream import results_json
            out.results = results_json(rep.results + rep.crosscheck)
            outcomes.append(out)
            # failed partitions are remembered after a run *or* a verify, so --rerun-failed can repair them
            if mode == "partitions" or verify:
                st["failed_partitions"] = rep.failed_partitions
            else:
                st["failed_partitions"] = rep.failed_partitions if out.status == "FAIL" else []
            if not verify:
                st["last_run"] = run_id
                st["last_status"] = out.status
                if out.status == "PASS" and window:
                    st["watermark"] = _enc(window[2])          # advance only after a verified run
            save_state(job["state_file"], state)
            extra = f"; failed partitions: {', '.join(rep.failed_partitions[:5])}" if rep.failed_partitions else ""
            log(f"  {name:<24} {out.status:<8} {out.rows_written:>10,} rows  {len(rep.results)} checks"
                f"{', ' + out.crosscheck if out.crosscheck else ''}  {out.seconds}s{extra}")
        except JobError as e:
            outcomes.append(TableOutcome(name, "ERROR", message=str(e), seconds=round(time.perf_counter() - t0, 2)))
            log(f"  {name:<24} ERROR    {e}")
        except Exception as e:  # report and continue with the other tables
            outcomes.append(TableOutcome(name, "ERROR", message=f"{type(e).__name__}: {e}",
                                         seconds=round(time.perf_counter() - t0, 2)))
            log(f"  {name:<24} ERROR    {type(e).__name__}: {e}")

    statuses = {o.status for o in outcomes}
    code = 2 if "ERROR" in statuses else 1 if "FAIL" in statuses else 0
    report = {"job": job["name"], "run_id": run_id, "format": job["format"], "exit_code": code,
              "status": {0: "PASS", 1: "FAIL", 2: "ERROR"}[code],
              "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "seconds": round(time.perf_counter() - t_job, 2),
              "tables": [asdict(o) for o in outcomes]}
    write_report(job, report)
    log(f"job {job['name']}: {report['status']} (exit code {code}); report in {job['report_dir']}")
    return code, outcomes, report


def write_report(job: dict, report: dict) -> None:
    d = Path(job["report_dir"])
    d.mkdir(parents=True, exist_ok=True)
    (d / "last_run.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (d / f"run_{report['run_id']}.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    L = [f"# Job {report['job']}: {report['status']}", "",
         f"Run {report['run_id']}, format {report['format']}, {report['seconds']} s, exit code {report['exit_code']}", "",
         "| Table | Status | Mode | Rows written | Partitions | Database cross-check | Seconds |",
         "|---|---|---|---|---|---|---|"]
    for t in report["tables"]:
        L.append(f"| {t['table']} | {t['status']} | {t['mode']} | {t['rows_written']:,} | {t['partitions']} | "
                 f"{t['crosscheck'] or '-'} | {t['seconds']} |")
    for t in report["tables"]:
        if t["status"] in ("FAIL", "ERROR") or t["message"]:
            L += ["", f"## {t['table']}: {t['status']}"]
            if t["message"]:
                L.append(t["message"])
            L += [f"- {c}" for c in t["failed_checks"][:20]]
            if t["failed_partitions"]:
                L.append(f"- failed partitions (rerun with --rerun-failed): {', '.join(t['failed_partitions'])}")
    regs = [t for t in report["tables"] if t["register_sql"]]
    if regs:
        L += ["", "## Querying the tables", ""]
        for t in regs:
            L += [f"**{t['table']}**", "", "```sql", t["register_sql"], "```", ""]
    (d / "last_run.md").write_text("\n".join(L) + "\n", encoding="utf-8")


def init_job(source_uri: str, target: str, fmt: str = "parquet") -> str:
    """A starter job file for every table in the source, with sensible guesses."""
    from .connectors import open_source
    from .typesys import declared_family
    src = open_source(source_uri if "://" in source_uri else f"sqlite:///{source_uri}")
    lines = [f"# Job file generated by: python -m amt init-job", f"name: {Path(str(target)).name or 'migration'}",
             f"source: {source_uri}", f"target: {target}", f"format: {fmt}            # parquet | csv | delta | iceberg",
             "strategy: h2                # h2 = rules (default), s3 = AI mapping", "rows_per_file: 100000",
             "chunk_rows: 50000", "# catalog:                  # needed for format: iceberg",
             "#   type: sql", "#   uri: sqlite:///catalog.db", "#   warehouse: C:/lake/warehouse",
             "#   namespace: default", "tables:"]
    for name in src.list_tables():
        cols = src._columns(name)
        ts = [c.name for c in cols if declared_family(c.declared_type) in ("timestamp", "timestamp_tz", "date")]
        upd = next((c for c in ts if any(k in c.lower() for k in ("updated", "modified", "changed"))), None)
        lines.append(f"  - name: {name}")
        if ts:
            lines.append(f"    partition_by: {ts[0]}         # monthly folders")
        if upd:
            lines.append(f"    incremental: {{column: {upd}, mode: append}}")
        sens = [c.name for c in cols if any(k in c.name.lower() for k in ("email", "phone", "mobile", "aadhaar", "pan"))]
        if sens:
            lines.append(f"    # mask: [{', '.join(sens)}]   # uncomment to mask personal data")
    return "\n".join(lines) + "\n"
