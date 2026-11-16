"""Command-line interface.

  python -m amt ui        # web dashboard
  python -m amt demo
  python -m amt describe --source sqlite:///shop.db
  python -m amt plan     --source sqlite:///shop.db --table orders --strategy h2
  python -m amt migrate  --source sqlite:///shop.db --table orders --target file:///tmp/lake \
                         --format parquet --partition-by placed_at --strategy s3

  For data engineers (large tables, many tables, schedulers, Delta Lake / Iceberg):
  python -m amt init-job --source sqlite:///shop.db --target C:/lake > shop.yaml
  python -m amt run shop.yaml [--full | --rerun-failed | --dry-run]    # exit 0 pass, 1 fail, 2 error
  python -m amt verify shop.yaml
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .agent import AgentManager
from .connectors import open_source, open_target
from .mappers import DeclaredTypeMapper, FineTunedMapper, RAGMapper, SampleAwareMapper, ZeroShotMapper
from .models import TargetSpec
from .report import to_json, to_markdown
from .retriever import PrecedentStore


def build_agent(args) -> AgentManager:
    store = PrecedentStore.load(args.precedents) if args.precedents else PrecedentStore()
    s = args.strategy
    if s == "h1":
        return AgentManager(DeclaredTypeMapper(), verify=False, rule_mode="schema_aware")
    if s == "h2":
        return AgentManager(SampleAwareMapper(store), verify=True)
    from .llm import OpenAICompatClient, make_client
    if s == "s1":
        return AgentManager(ZeroShotMapper(make_client(args.provider, args.model)), verify=False)
    if s == "s2":
        return AgentManager(FineTunedMapper(OpenAICompatClient(args.ft_model, args.ft_base_url)), verify=False)
    if s == "s3":
        # Hybrid: the AI maps, ordinary code chooses the checks (best in the evaluation).
        return AgentManager(RAGMapper(make_client(args.provider, args.model), store), verify=True,
                            rule_mode="schema_aware")
    raise SystemExit(f"unknown strategy {s}")


def spec_from(args) -> TargetSpec:
    return TargetSpec(args.target or "", args.format, args.partition_by, args.partition_grain, args.rows_per_file)


def cmd_describe(args):
    src = open_source(args.source)
    for name in src.list_tables():
        t = src.describe(name, args.sample_size)
        print(f"{name} ({t.row_count} rows)")
        for c in t.columns:
            print(f"  {c.name:<24} {c.declared_type:<28} {'PK' if c.primary_key else ''}{'' if c.nullable else ' NOT NULL'}")


def cmd_plan(args):
    src = open_source(args.source)
    log: list[str] = []
    plan = build_agent(args).plan(src, args.table, spec_from(args), log)
    print("\n".join(log), file=sys.stderr)
    print(json.dumps({"mapping": plan.mapping.to_dict(), "rules": [asdict(r) for r in plan.rules]}, indent=2, default=str))


def cmd_migrate(args):
    src, tgt = open_source(args.source), open_target(args.target)
    agent = build_agent(args)
    tables = args.table.split(",") if args.table else src.list_tables()
    reports = []
    for t in tables:
        rep = agent.run(src, t, spec_from(args), tgt)
        print(f"{t}: {'ERROR ' + rep.error if rep.error else rep.summary['status']}", file=sys.stderr)
        reports.append(rep)
    out = Path(args.report_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "reconciliation_report.md").write_text(to_markdown(reports), encoding="utf-8")
    (out / "reconciliation_report.json").write_text(to_json(reports), encoding="utf-8")
    print(f"report written to {out / 'reconciliation_report.md'}", file=sys.stderr)
    return 0 if all(r.error is None and r.summary.get("status") == "PASS" for r in reports) else 1


def cmd_demo(args):
    """Build the e-commerce demo schema and migrate it with the offline H2 strategy."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from eval import corpus
    work = Path(args.workdir)
    db = work / "ecommerce.db"
    corpus.build_schema_db("ecommerce", db)
    src, tgt = open_source(f"sqlite:///{db.as_posix()}"), open_target(str(work / "lake"))
    agent = AgentManager(SampleAwareMapper(PrecedentStore()), verify=True)
    reports = []
    for ts in corpus.SCHEMAS["ecommerce"]:
        rep = agent.run(src, ts.name, corpus.target_spec(ts, str(work / "lake")), tgt)
        print(f"{ts.name:<12} {'ERROR ' + rep.error if rep.error else rep.summary['status']:<6} "
              f"rows={rep.execution.rows_written if rep.execution else 0}")
        reports.append(rep)
    (work / "reconciliation_report.md").write_text(to_markdown(reports), encoding="utf-8")
    print(f"\nParquet files: {work / 'lake'}\nReport: {work / 'reconciliation_report.md'}")


def cmd_run(args, verify=False):
    """Run (or re-check) a job file. Exit code 0 = passed, 1 = a check failed, 2 = could not run."""
    from .pipeline import JobError, load_job, run_job
    try:
        job = load_job(args.job)
        code, _, _ = run_job(job, full=getattr(args, "full", False), rerun_failed=getattr(args, "rerun_failed", False),
                             only=args.tables.split(",") if args.tables else None, verify=verify,
                             dry_run=getattr(args, "dry_run", False), log=lambda m: print(m, flush=True))
        return code
    except JobError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


def cmd_init_job(args):
    from .pipeline import init_job
    try:
        print(init_job(args.source, args.target, args.format), end="")
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


def cmd_ui(args):
    from .ui.server import serve
    serve(args.port, open_browser=not args.no_browser, host=args.host, public=args.public,
          workers=args.workers)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="amt", description="AI Data Migration Tool")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, needs_table=True):
        p.add_argument("--source", required=True, help="sqlite:///x.db | postgresql://... | mysql://...")
        p.add_argument("--table", required=needs_table, help="table name (comma-separated for migrate)")
        p.add_argument("--target", default=None, help="file:///dir | s3://bucket/prefix | gs://bucket/prefix")
        p.add_argument("--format", default="parquet", choices=["parquet", "csv"])
        p.add_argument("--partition-by", default=None)
        p.add_argument("--partition-grain", default="month", choices=["month", "day", "value"])
        p.add_argument("--rows-per-file", type=int, default=100000)
        p.add_argument("--strategy", default="h2", choices=["h1", "h2", "s1", "s2", "s3"])
        p.add_argument("--precedents", default=None, help="JSONL precedent store for h2/s3")
        p.add_argument("--provider", default=None, choices=["auto", "anthropic", "ollama"],
                       help="LLM for s1/s3: anthropic (paid) or ollama (free, local)")
        p.add_argument("--model", default=None)
        p.add_argument("--ft-model", default=None)
        p.add_argument("--ft-base-url", default=None)

    p = sub.add_parser("describe")
    p.add_argument("--source", required=True)
    p.add_argument("--sample-size", type=int, default=20)
    p.set_defaults(fn=cmd_describe)
    p = sub.add_parser("plan")
    common(p)
    p.set_defaults(fn=cmd_plan)
    p = sub.add_parser("migrate")
    common(p, needs_table=False)
    p.add_argument("--report-dir", default="reports")
    p.set_defaults(fn=cmd_migrate)
    p = sub.add_parser("demo")
    p.add_argument("--workdir", default="demo_out")
    p.set_defaults(fn=cmd_demo)
    p = sub.add_parser("run", help="run a job file (many tables, incremental, Delta/Iceberg); exit 0/1/2")
    p.add_argument("job", help="job file (.yaml or .json)")
    p.add_argument("--full", action="store_true", help="ignore saved watermarks and copy everything again")
    p.add_argument("--rerun-failed", action="store_true", help="rewrite only the partitions that failed last time")
    p.add_argument("--tables", default=None, help="only these tables (comma-separated)")
    p.add_argument("--dry-run", action="store_true", help="plan every table, write nothing")
    p.set_defaults(fn=cmd_run)
    p = sub.add_parser("verify", help="re-check the output of a job file without writing anything")
    p.add_argument("job")
    p.add_argument("--tables", default=None)
    p.set_defaults(fn=lambda a: cmd_run(a, verify=True))
    p = sub.add_parser("init-job", help="print a starter job file for every table in a database")
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--format", default="parquet", choices=["parquet", "csv", "delta", "iceberg"])
    p.set_defaults(fn=cmd_init_job)
    p = sub.add_parser("ui", help="open the web dashboard")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--host", default="127.0.0.1",
                   help="127.0.0.1 = only this computer; 0.0.0.0 = reachable from other computers")
    p.add_argument("--public", action="store_true",
                   help="shared-demo mode for many users: demo databases and private local output only")
    p.add_argument("--workers", type=int, default=None, help="parallel worker processes (default: CPU cores - 1)")
    p.add_argument("--no-browser", action="store_true")
    p.set_defaults(fn=cmd_ui)
    args = ap.parse_args(argv)
    if args.cmd == "migrate" and not args.target:
        ap.error("--target is required for migrate")
    return args.fn(args) or 0


if __name__ == "__main__":
    sys.exit(main())
