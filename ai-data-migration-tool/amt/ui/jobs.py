"""Background job engine for the multi-user dashboard.

Heavy work (planning, migrating, reconciling) runs in worker *processes*, so
many users are served in parallel on all CPU cores, and the web server stays
responsive. Each job gets its own folder, so concurrent users never touch each
other's output. A job reports its progress by writing `progress.json` in its
folder, which the web server reads when the page polls.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import random
import shutil
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

JOB_TYPES = ("plan", "migrate", "break", "review", "summary")
AI_STRATEGIES = ("s1", "s3")
AI_JOBS = ("review", "summary")      # AI assistant features (amt/assist.py)

INJECTION_TEXT = {
    "truncated_decimal": "One money value loses its last digit (e.g. 1234.56 becomes 1234.50).",
    "dropped_null_row": "A row that contains an empty (NULL) value silently disappears.",
    "duplicated_row": "One row is written twice.",
    "timezone_shift": "Every timestamp in one file is shifted by 5 h 30 min (wrong time zone).",
    "off_by_one_partition": "A row lands in the wrong monthly folder (or one row is lost at a file boundary).",
    "flattened_json": "A nested JSON column is flattened into separate columns, losing nested data.",
}


class JobRejected(Exception):
    """Raised when a job cannot be accepted (limits, bad input). `status` is the HTTP code."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def json_default(o):
    if isinstance(o, Decimal):
        return str(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, bytes):
        return o.decode("utf-8", "replace")
    return str(o)


# ==========================================================================
# Worker side (runs inside a worker process; one job at a time per process)
# ==========================================================================

_W: dict = {}


def init_worker(work_dir: str) -> None:
    _W["work"] = Path(work_dir)
    _W["sources"] = {}


def _precedents():
    from amt.retriever import PrecedentStore
    if "prec" not in _W:
        _W["prec"] = PrecedentStore.load(str(_W["work"] / "precedents.jsonl"))
    return _W["prec"]


def _source(source: str):
    """One cached connection per source per worker process (never shared between threads)."""
    from amt.connectors import SQLiteSource, open_source
    cache = _W["sources"]
    if source not in cache:
        if source.startswith("demo:"):
            cache[source] = SQLiteSource(str(_W["work"] / "db" / f"{source[5:]}.db"))
        else:
            cache[source] = open_source(source if "://" in source else f"sqlite:///{source}")
    return cache[source]


def _agent(strategy: str, source: str):
    from amt.agent import AgentManager
    from amt.mappers import DeclaredTypeMapper, RAGMapper, SampleAwareMapper, ZeroShotMapper
    exclude = source[5:] if source.startswith("demo:") else None
    if strategy == "h1":
        return AgentManager(DeclaredTypeMapper(), verify=False)
    if strategy == "h2":
        return AgentManager(SampleAwareMapper(_precedents(), exclude_schema=exclude), verify=True)
    from amt.llm import make_client
    client = make_client(cache_dir=str(_W["work"] / "llm_cache"))
    if strategy == "s1":
        return AgentManager(ZeroShotMapper(client), verify=False)
    if strategy == "s3":
        # Hybrid: the AI maps, ordinary code chooses the checks (best in the evaluation).
        return AgentManager(RAGMapper(client, _precedents(), exclude_schema=exclude), verify=True,
                            rule_mode="schema_aware")
    raise ValueError(f"unknown strategy {strategy}")


def _progress(job_dir: Path, stage: str, detail: str = "") -> None:
    """Best-effort progress note for the page. Never fails the job: on Windows the
    replace is refused while the server is reading the file, so retry briefly and
    otherwise skip this update (the next one, or the result, will follow)."""
    tmp = job_dir / f"progress.{os.getpid()}.tmp"
    try:
        tmp.write_text(json.dumps({"stage": stage, "detail": detail, "t": time.time()}), encoding="utf-8")
        for _ in range(20):
            try:
                os.replace(tmp, job_dir / "progress.json")
                return
            except PermissionError:
                time.sleep(0.01)
        tmp.unlink(missing_ok=True)
    except OSError:
        pass


def _spec(body: dict, lake: str):
    from amt.models import TargetSpec
    fmt = body.get("fmt", "parquet")
    if fmt not in ("parquet", "csv"):
        raise ValueError("format must be parquet or csv")
    return TargetSpec(lake, fmt, body.get("partition_by") or None, "month",
                      rows_per_file=int(body.get("rows_per_file") or 100))


def plan_dict(plan, log: list[str]) -> dict:
    from amt.agent import execution_verification
    from amt.typesys import describe_shape, profile_values
    table = plan.table
    cols = []
    for cm in plan.mapping.columns:
        col = table.column(cm.source) if cm.source else None
        values = [r.get(cm.source) for r in table.sample] if col else []
        prof = profile_values(values) if col else {"shape": "empty"}
        cols.append({**asdict(cm), "declared_type": col.declared_type if col else None,
                     "nullable": col.nullable if col else None, "primary_key": col.primary_key if col else False,
                     "shape": describe_shape(prof) if col else "",
                     "samples": [v for v in values if v is not None][:3]})
    problems = execution_verification(table, plan.mapping, plan.target)
    return {"table": table.name, "row_count": table.row_count, "columns": cols,
            "rules": [asdict(r) for r in plan.rules], "log": log, "dry_run_problems": problems}


def _results(results) -> list[dict]:
    return [{"rule": asdict(r.rule), "passed": r.passed, "source": r.source_value,
             "target": r.target_value, "detail": r.detail} for r in results]


def _open_target(body: dict, job_dir: Path, public: bool):
    from amt.connectors import LocalTarget, open_target
    uri = (body.get("target_uri") or "").strip()
    if public or body.get("destination", "local") == "local" or not uri:
        lake = job_dir / "lake"
        return LocalTarget(str(lake)), str(lake)
    return open_target(uri), uri.split("?")[0]


_FAMILY = {"int64": "int", "float64": "float", "string": "string", "bool": "bool", "date": "date",
           "timestamp": "timestamp", "timestamp_tz": "timestamp_tz", "json_string": "json"}


def corrected_columns(plan) -> list[str]:
    """Columns whose declared SQL type was misleading and were stored as something else."""
    from amt.typesys import declared_family, parse_decimal_type
    out = []
    for cm in plan.mapping.columns:
        col = plan.table.column(cm.source) if cm.source else None
        if col is None:
            continue
        fam = "decimal" if parse_decimal_type(cm.target_type) else _FAMILY.get(cm.target_type, cm.target_type)
        decl = declared_family(col.declared_type)
        if decl != fam and (decl, fam) != ("enum", "string"):
            out.append(cm.source)
    return out


def _plan_with_overrides(body, src, spec, log):
    """Plan with the chosen strategy, then apply the suggestions the user accepted
    from the AI review, and re-run the dry run on the changed mapping."""
    from amt.agent import VerificationError, execution_verification
    from amt.assist import apply_masks, apply_overrides
    agent = _agent(body.get("strategy", "h2"), body["source"])
    plan = agent.plan(src, body["table"], spec, log)
    overrides, masks = body.get("overrides") or {}, body.get("masks") or []
    if overrides or masks:
        if overrides:
            changed = apply_overrides(plan.mapping, overrides)
            log.append("applied the AI review suggestions you accepted: " + "; ".join(changed))
        if masks:
            try:
                done = apply_masks(plan.mapping, plan.table, masks, spec.partition_by)
            except ValueError as e:
                raise VerificationError(str(e)) from e
            log.append("masked for privacy: " + ", ".join(done))
        problems = execution_verification(plan.table, plan.mapping, spec)
        if problems:
            raise VerificationError("a change you chose failed the dry run, so nothing was migrated: "
                                    + "; ".join(problems[:3]) + ". Undo that change and try again.")
        log.append("dry run passed with your changes")
        plan.rules = agent.build_rules(plan.table, plan.mapping, spec, [])   # checks follow the new types
    return agent, plan


def _job_plan(body, job_dir: Path, public: bool) -> dict:
    from amt.assist import find_sensitive, mask_blockers, with_previews
    _progress(job_dir, "plan", "Reading the table and planning the mapping")
    src = _source(body["source"])
    log: list[str] = []
    spec = _spec(body, "")
    plan = _agent(body.get("strategy", "h2"), body["source"]).plan(src, body["table"], spec, log)
    _progress(job_dir, "verify", "Dry run on sample rows")
    return {**plan_dict(plan, log), "sensitive": with_previews(plan.table, find_sensitive(plan.table)),
            "mask_blocked": mask_blockers(plan.table, spec.partition_by)}


def _job_review(body, job_dir: Path, public: bool) -> dict:
    """AI second opinion on the Standard mapping, plus the AI part of the privacy scan."""
    from amt.assist import find_sensitive, merge_sensitive, review_mapping, with_previews
    from amt.llm import make_client
    _progress(job_dir, "plan", "Preparing the mapping for review")
    src = _source(body["source"])
    plan = _agent("h2", body["source"]).plan(src, body["table"], _spec(body, ""), [])
    _progress(job_dir, "review", "The AI is reviewing every column")
    client = make_client(cache_dir=str(_W["work"] / "llm_cache"))
    r = review_mapping(client, plan.table, plan.mapping)
    return {"flags": r["flags"],
            "sensitive": with_previews(plan.table, merge_sensitive(find_sensitive(plan.table), r["sensitive"])),
            "model": getattr(client, "model", "")}


def _job_summary(body, job_dir: Path, public: bool) -> dict:
    """Plain-English summary of a finished migration, saved into that migration's download."""
    from amt.assist import summarise_migration
    from amt.llm import make_client
    mig_dir = Path(body["_migration_dir"])
    facts = json.loads((mig_dir / "facts.json").read_text(encoding="utf-8"))
    _progress(job_dir, "review", "The AI is writing the summary")
    out = summarise_migration(make_client(cache_dir=str(_W["work"] / "llm_cache")), facts)
    try:
        (mig_dir / "summary.md").write_text(f"# Migration summary\n\n{out['text']}\n\n_{out['note']}_\n", encoding="utf-8")
    except OSError:
        pass
    return out


def _job_migrate(body, job_dir: Path, public: bool) -> dict:
    from amt.agent import MigrationReport, reconcile_written
    from amt.assist import find_sensitive, migration_facts
    from amt.executor import execute, read_output
    from amt.report import to_markdown
    t0 = time.perf_counter()
    src = _source(body["source"])
    target, where = _open_target(body, job_dir, public)
    spec = _spec(body, where)
    prefix = body["table"]
    log: list[str] = []
    _progress(job_dir, "plan", "Planning the mapping and running the dry run")
    agent, plan = _plan_with_overrides(body, src, spec, log)
    _progress(job_dir, "migrate", f"Copying {plan.table.row_count} rows")
    ex = execute(src, plan.table, plan.mapping, spec, target, prefix)
    log.append(f"wrote {ex.rows_written} rows to {len(ex.files)} file(s)")
    _progress(job_dir, "verify", "Reading the files back and checking them")
    results, summary = reconcile_written(src, plan, target, prefix)
    log.append(f"implementation verification: {summary['status']} ({summary['failed']}/{summary['rules']} rules failed)")
    rep = MigrationReport(body["table"], agent.mapper.name, plan, ex, results, summary, log,
                          seconds=time.perf_counter() - t0)
    (job_dir / "reconciliation_report.md").write_text(to_markdown([rep]), encoding="utf-8")
    files = read_output(target, prefix, spec.fmt, plan.mapping)
    res = _results(results)
    facts = migration_facts(body["table"], ex.rows_read, ex.rows_written, len(ex.files),
                            "a private download" if public else ("a local folder" if where == str(job_dir / "lake")
                                                                 else where.split("://")[0].upper() + " storage"),
                            summary, res, corrected_columns(plan), [s["column"] for s in find_sensitive(plan.table)],
                            [cm.source for cm in plan.mapping.columns if cm.transform == "mask"])
    (job_dir / "facts.json").write_text(json.dumps(facts, default=json_default), encoding="utf-8")
    return {"error": None, "log": log, "seconds": round(rep.seconds, 2), "summary": summary,
            "plan": plan_dict(plan, list(log)), "rows_read": ex.rows_read, "rows_written": ex.rows_written,
            "files": [{"key": k[len(prefix) + 1:], "rows": n} for k, n in ex.files.items()],
            "preview": files[0][1][:5] if files else [],
            "output_dir": "your private download" if public else f"{where.rstrip('/')}/{prefix}",
            "downloadable": not body.get("target_uri") or public,
            "results": res}


def _job_break(body, job_dir: Path, public: bool) -> dict:
    from amt.connectors import LocalTarget
    from amt.executor import execute, read_output
    from amt.reconcile import Reconciler, generic_rules, schema_aware_rules, summarise
    from eval.inject import inject
    kind = body["injection"]
    if kind not in INJECTION_TEXT:
        raise ValueError(f"unknown fault {kind}")
    src = _source(body["source"])
    target = LocalTarget(str(job_dir / "lake"))
    spec = _spec(body, str(job_dir / "lake"))
    log: list[str] = []
    _progress(job_dir, "plan", "Planning a correct migration")
    plan = _agent(body.get("strategy", "h2"), body["source"]).plan(src, body["table"], spec, log)
    _progress(job_dir, "migrate", "Migrating correctly")
    execute(src, plan.table, plan.mapping, spec, target, "t")
    files = read_output(target, "t", spec.fmt, plan.mapping)
    _progress(job_dir, "inject", "Damaging the output on purpose")
    edited = inject(kind, files, plan.mapping, bool(spec.partition_by), random.Random(body.get("seed", 1)))
    if edited is None:
        return {"applicable": False,
                "message": f"This table has nothing that '{kind.replace('_', ' ')}' can damage "
                           "(for example no money, time-zone or JSON column, or no NULL values). "
                           "Pick another table or fault."}
    _progress(job_dir, "verify", "Running both sets of checks")
    src_rows = list(src.iter_rows_schema(plan.table))
    out = {"applicable": True, "injection": kind, "description": INJECTION_TEXT[kind], "rule_sets": []}
    for name, rules in (("Generic checks (what typical ETL tools do)", generic_rules(plan.table, spec)),
                        ("Schema-aware checks (this tool)", schema_aware_rules(plan.table, plan.mapping, spec))):
        res = Reconciler(plan.table, plan.mapping, spec, src_rows, edited).run(rules)
        out["rule_sets"].append({"name": name, "summary": summarise(res), "results": _results(res)})
    return out


def run_job(job_type: str, body: dict, job_dir: str, public: bool) -> str:
    """Entry point executed in a worker process. Returns the result as JSON text."""
    d = Path(job_dir)
    fn = {"plan": _job_plan, "migrate": _job_migrate, "break": _job_break,
          "review": _job_review, "summary": _job_summary}[job_type]
    result = fn(body, d, public)
    return json.dumps(result, default=json_default)


# ==========================================================================
# Server side
# ==========================================================================

class JobManager:
    """Queues jobs onto worker processes and tracks their state per session."""

    def __init__(self, work: Path, workers: int, ai_workers: int = 1, max_active: int = 400,
                 per_session: int = 3, ttl: float = 1800, public: bool = False):
        self.work = work
        self.public = public
        self.max_active = max_active
        self.per_session = per_session
        self.ttl = ttl
        self.workers = workers
        ctx = multiprocessing.get_context("spawn")
        args = dict(mp_context=ctx, initializer=init_worker, initargs=(str(work),))
        self.pool = ProcessPoolExecutor(max_workers=workers, **args)
        # AI jobs can take minutes on a laptop; keep them off the main pool.
        self.ai_pool = ProcessPoolExecutor(max_workers=ai_workers, **args)
        self.jobs: dict[str, dict] = {}
        self.lock = threading.Lock()
        self.completed = 0
        self.durations: list[float] = []
        (work / "jobs").mkdir(parents=True, exist_ok=True)
        self._stop = threading.Event()
        threading.Thread(target=self._janitor, daemon=True).start()

    # -- submission ----------------------------------------------------------
    def submit(self, session: str, job_type: str, body: dict) -> str:
        if job_type not in JOB_TYPES:
            raise JobRejected(f"unknown job type {job_type!r}")
        if job_type in AI_JOBS and self.public:
            raise JobRejected("The AI assistant is not available on this shared demo.", 403)
        if job_type == "summary":
            # The summary reads the facts of one of *this user's* finished migrations.
            body = {"_migration_dir": str(self.job_dir(session, str(body.get("job_id") or ""))),
                    "source": "-", "table": "-"}
        for key in ("source", "table"):
            if not body.get(key):
                raise JobRejected(f"missing {key!r}")
        if self.public:
            if not str(body["source"]).startswith("demo:"):
                raise JobRejected("This shared demo only allows the built-in demo databases.", 403)
            if body.get("target_uri") or body.get("destination", "local") != "local":
                raise JobRejected("This shared demo only writes to private local output.", 403)
        if not isinstance(body.get("overrides") or {}, dict):
            raise JobRejected("overrides must be an object")
        if not isinstance(body.get("masks") or [], list):
            raise JobRejected("masks must be a list of column names")
        ai = body.get("strategy") in AI_STRATEGIES or job_type in AI_JOBS
        with self.lock:
            active = [j for j in self.jobs.values() if j["status"] in ("queued", "running")]
            if len(active) >= self.max_active:
                raise JobRejected("The server is very busy right now. Please try again in a minute.", 503)
            mine = sum(1 for j in active if j["session"] == session)
            if mine >= self.per_session:
                raise JobRejected("You already have jobs running. Please wait for them to finish.", 429)
            jid = uuid.uuid4().hex[:16]
            job_dir = self.work / "jobs" / jid
            job_dir.mkdir(parents=True)
            job = {"id": jid, "session": session, "type": job_type, "status": "queued",
                   "submitted": time.time(), "started": None, "finished": None,
                   "result": None, "error": None, "dir": job_dir, "ai": ai}
            self.jobs[jid] = job
            pool = self.ai_pool if ai else self.pool
            fut: Future = pool.submit(run_job, job_type, body, str(job_dir), self.public)
        fut.add_done_callback(lambda f, j=job: self._done(j, f))
        return jid

    def _done(self, job: dict, fut: Future) -> None:
        with self.lock:
            job["finished"] = time.time()
            exc = fut.exception()
            if exc is None:
                job["status"], job["result"] = "done", fut.result()
            else:
                job["status"], job["error"] = "error", friendly_error(exc)
            self.completed += 1
            self.durations.append(job["finished"] - job["submitted"])
            self.durations = self.durations[-500:]

    # -- status --------------------------------------------------------------
    def _progress(self, job: dict) -> dict | None:
        try:
            return json.loads((job["dir"] / "progress.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    def get(self, session: str, jid: str) -> dict:
        with self.lock:
            job = self.jobs.get(jid)
            if job is None or job["session"] != session:
                raise JobRejected("This job does not exist or has expired.", 404)
            prog = self._progress(job) if job["status"] in ("queued", "running") else None
            if job["status"] == "queued" and prog is not None:
                job["status"], job["started"] = "running", prog.get("t", time.time())
            out = {"id": jid, "type": job["type"], "status": job["status"],
                   "elapsed": round(time.time() - job["submitted"], 1)}
            if job["status"] == "queued":
                ahead = sum(1 for j in self.jobs.values()
                            if j["status"] == "queued" and j["ai"] == job["ai"] and j["submitted"] < job["submitted"])
                capacity = 1 if job["ai"] else self.workers
                out["position"] = max(0, ahead - capacity + 1) + 1
            elif job["status"] == "running":
                out["stage"], out["detail"] = (prog or {}).get("stage"), (prog or {}).get("detail")
            elif job["status"] == "done":
                out["result"] = json.loads(job["result"])
            else:
                out["error"] = job["error"]
            return out

    def job_dir(self, session: str, jid: str) -> Path:
        with self.lock:
            job = self.jobs.get(jid)
            if job is None or job["session"] != session or job["status"] != "done":
                raise JobRejected("This job does not exist, is not finished, or has expired.", 404)
            return job["dir"]

    def stats(self) -> dict:
        with self.lock:
            c = Counter(j["status"] for j in self.jobs.values())
            d = sorted(self.durations)
        pct = (lambda q: round(d[min(len(d) - 1, int(q * len(d)))], 2) if d else None)
        return {"workers": self.workers, "queued": c["queued"], "running": c["running"],
                "completed": self.completed, "job_seconds_p50": pct(0.5), "job_seconds_p95": pct(0.95)}

    # -- housekeeping ----------------------------------------------------------
    def _janitor(self) -> None:
        while not self._stop.wait(30):
            cutoff = time.time() - self.ttl
            with self.lock:
                old = [jid for jid, j in self.jobs.items() if j["finished"] and j["finished"] < cutoff]
                dirs = [self.jobs.pop(jid)["dir"] for jid in old]
            for d in dirs:
                shutil.rmtree(d, ignore_errors=True)

    def shutdown(self) -> None:
        self._stop.set()
        self.pool.shutdown(wait=False, cancel_futures=True)
        self.ai_pool.shutdown(wait=False, cancel_futures=True)


def friendly_error(exc: BaseException) -> str:
    """Turn an internal exception into a message a non-specialist can act on."""
    name = type(exc).__name__
    text = str(exc)
    if name == "LLMError":
        return f"The AI model could not answer: {text}"
    if name in ("ConversionError",):
        return f"A value could not be converted with this mapping: {text}"
    if name == "VerificationError":
        return f"The request could not be checked: {text}"
    if name == "BrokenProcessPool":
        return "A worker stopped unexpectedly. Please try again."
    return f"Something went wrong ({name}): {text}"
