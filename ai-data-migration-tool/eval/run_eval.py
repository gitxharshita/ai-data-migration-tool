"""End-to-end evaluation harness.

    python -m eval.run_eval                         # offline baselines H1, H2 + rule-set study
    python -m eval.run_eval --strategies s1,s3      # LLM strategies (needs ANTHROPIC_API_KEY)
    python -m eval.run_eval --strategies s2 --ft-model my-lora --ft-base-url http://localhost:8000/v1

Precedent mappings are used leave-one-schema-out: a table never retrieves
precedents from its own schema.
"""
from __future__ import annotations

import argparse
import io
import json
import random
import shutil
import statistics
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.agent import AgentManager, reconcile_written  # noqa: E402
from amt.connectors import (LocalTarget, PostgresSource, SourceConnector, SQLiteSource,  # noqa: E402
                            open_source, open_target)
from amt.executor import encode, execute, read_output  # noqa: E402
from amt.llm import OpenAICompatClient, make_client  # noqa: E402
from amt.mappers import (DeclaredTypeMapper, FineTunedMapper, RAGMapper,  # noqa: E402
                         SampleAwareMapper, ZeroShotMapper)
from amt.models import MigrationPlan  # noqa: E402
from amt.reconcile import deterministic_rules, generic_rules, schema_aware_rules  # noqa: E402
from amt.retriever import PrecedentStore  # noqa: E402
from amt.typesys import profile_values  # noqa: E402
from eval import corpus  # noqa: E402
from eval.inject import INJECTIONS, inject  # noqa: E402
from eval.metrics import HALLUCINATIONS, cms, hallucinations, mapping_counts, prf  # noqa: E402

TIERS = ["Easy", "Medium", "Hard", "Extra Hard"]


class Harness:
    def __init__(self, work: Path, seed: int = 7, pg_dsn: str | None = None, s3_uri: str | None = None,
                 mssql_uri: str | None = None):
        self.work = work
        self.seed = seed
        # Scratch files go outside OneDrive/Dropbox folders, whose sync client
        # locks freshly written files and makes deleting them fail on Windows.
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True, exist_ok=True)
        if s3_uri:
            # a fresh prefix per run, so concurrent or earlier runs never collide
            self.target = open_target(f"{s3_uri.rstrip('/')}/run-{time.strftime('%Y%m%d-%H%M%S')}")
        else:
            self.target = LocalTarget(str(work / "store"))
        self.sources: dict[str, SourceConnector] = {}
        if mssql_uri:
            from eval.setup_backends import load_sqlserver
            load_sqlserver(mssql_uri, seed)
            sep = '&' if '?' in mssql_uri else '?'
            for schema in corpus.SCHEMAS:
                self.sources[schema] = open_source(f"{mssql_uri}{sep}schema={schema}")
        elif pg_dsn:
            from eval.setup_backends import load_postgres
            load_postgres(pg_dsn, seed)                 # identical rows to the SQLite corpus
            for schema in corpus.SCHEMAS:
                self.sources[schema] = PostgresSource(pg_dsn, schema)
        else:
            for schema in corpus.SCHEMAS:
                db = work / "db" / f"{schema}.db"
                corpus.build_schema_db(schema, db, seed)
                self.sources[schema] = SQLiteSource(str(db))
        self.backend = (f"source: {'SQL Server' if mssql_uri else 'PostgreSQL' if pg_dsn else 'SQLite'}; "
                        f"target: {'S3-compatible object store (' + s3_uri + ')' if s3_uri else 'local filesystem'}")
        self.precedents = self._build_precedents()
        self.runs = self._prepare_injection_runs()

    def _build_precedents(self) -> PrecedentStore:
        store = PrecedentStore()
        for schema, ts in corpus.all_tables():
            table = self.sources[schema].describe(ts.name)
            gold = corpus.gold_mapping(ts)
            spec = corpus.target_spec(ts, "")
            rules = schema_aware_rules(table, gold, spec)
            for cm in gold.columns:
                col = table.column(cm.source)
                store.add({"schema": schema, "table": ts.name, "column": cm.source,
                           "declared_type": col.declared_type,
                           "profile": profile_values([r.get(cm.source) for r in table.sample]),
                           "mapping": {"target_type": cm.target_type, "transform": cm.transform,
                                       "null_handling": cm.null_handling},
                           "rules": sorted({r.kind for r in rules if r.column == cm.source})})
        return store

    def _prepare_injection_runs(self):
        """Execute every table with its gold mapping once, then write one clean
        copy and one copy per applicable injected discrepancy."""
        runs = {}
        for schema, ts in corpus.all_tables():
            src = self.sources[schema]
            table = src.describe(ts.name)
            gold = corpus.gold_mapping(ts)
            spec = corpus.target_spec(ts, "")
            base = f"gold/{schema}/{ts.name}"
            execute(src, table, gold, spec, self.target, base)
            files = read_output(self.target, base, "parquet")
            schema_pa = pq.read_schema(io.BytesIO(self.target.read_bytes(files[0][0])))
            rng = random.Random(f"{self.seed}:{schema}:{ts.name}:inject")
            entries = [("clean", base)]
            for kind in INJECTIONS:
                edited = inject(kind, files, gold, bool(ts.partition_by), rng)
                if edited is None:
                    continue
                prefix = f"inject/{schema}/{ts.name}/{kind}"
                for key, rows in edited:
                    sch = None if kind == "flattened_json" else schema_pa
                    new_key = prefix + key[len(base):]
                    self.target.write_bytes(new_key, encode(pa.Table.from_pylist(rows, schema=sch), "parquet"))
                entries.append((kind, prefix))
            runs[(schema, ts.name)] = (table, gold, spec, entries)
        return runs

    def reconcile_runs(self, schema, ts, rules):
        table, gold, spec, entries = self.runs[(schema, ts.name)]
        plan = MigrationPlan(table, spec, gold, rules)
        out = []
        for kind, prefix in entries:
            results, summary = reconcile_written(self.sources[schema], plan, self.target, prefix, rules)
            out.append((kind, summary["status"] == "FAIL", summary))
        return out


# --------------------------------------------------------------------------

def make_agent(name: str, schema: str, h: Harness, args) -> AgentManager:
    ex = schema
    if name == "h1":
        return AgentManager(DeclaredTypeMapper(), verify=False)
    if name == "h1+verify":
        return AgentManager(DeclaredTypeMapper(), verify=True)
    if name == "h2-noverify":
        return AgentManager(SampleAwareMapper(h.precedents, exclude_schema=ex), verify=False)
    if name == "h2":
        return AgentManager(SampleAwareMapper(h.precedents, exclude_schema=ex), verify=True)
    if name in ("s1", "s1+verify"):
        return AgentManager(ZeroShotMapper(args.client), verify=name.endswith("verify"))
    if name == "s2":
        return AgentManager(FineTunedMapper(args.ft_client), verify=False)
    if name in ("s3", "s3-noverify"):
        return AgentManager(RAGMapper(args.client, h.precedents, exclude_schema=ex), verify=name == "s3")
    # Hybrid ("best of both"): the AI builds the mapping, ordinary code chooses the
    # checks from that mapping; the AI's own proposed checks are ignored.
    if name == "s1+code":
        return AgentManager(ZeroShotMapper(args.client), verify=True, rule_mode="schema_aware")
    if name == "s3+code":
        return AgentManager(RAGMapper(args.client, h.precedents, exclude_schema=ex), verify=True,
                            rule_mode="schema_aware")
    raise ValueError(name)


LABELS = {
    "h1": "H1 Declared-type rules (no LLM)",
    "h1+verify": "H1 + execution verification",
    "h2-noverify": "H2 Sample+precedent rules, no verification",
    "h2": "H2 Sample+precedent rules + verification (no LLM)",
    "s1": "S1 Zero-shot prompting",
    "s1+verify": "S1 + execution verification",
    "s2": "S2 Fine-tuned",
    "s3-noverify": "S3 RAG, no verification",
    "s3": "S3 Schema+sample-aware RAG + verification",
    "s1+code": "Hybrid: S1 mapping + code-generated checks",
    "s3+code": "Hybrid: S3 mapping + code-generated checks",
}


def evaluate_strategy(name: str, h: Harness, args) -> dict:
    per_table = []
    client_stats0 = _client_stats(args)
    for schema, ts in corpus.all_tables():
        if args.tables and ts.name not in args.tables:
            continue
        agent = make_agent(name, schema, h, args)
        rep = agent.run(h.sources[schema], ts.name, corpus.target_spec(ts, ""), h.target,
                        prefix=f"runs/{name}/{schema}/{ts.name}")
        table, gold, spec, _ = h.runs[(schema, ts.name)]
        if rep.plan:
            tp, npred, ngold = mapping_counts(rep.plan.mapping, gold, table)
            rules = rep.plan.rules
            halluc = hallucinations(table, rep.plan.mapping, gold, rules, rep.results)
        else:
            tp, npred, ngold = 0, 0, len(gold.columns)
            rules = deterministic_rules(table, spec)
            halluc = {k: False for k, _, _ in HALLUCINATIONS}
        recon = h.reconcile_runs(schema, ts, rules)
        per_table.append({
            "schema": schema, "table": ts.name, "tier": ts.tier, "valid": rep.error is None,
            "error": rep.error, "tp": tp, "n_pred": npred, "n_gold": ngold, "halluc": halluc,
            "recon": [(k, d) for k, d, _ in recon], "seconds": rep.seconds, "log": rep.log,
            "mapping": rep.plan.mapping.to_dict() if rep.plan else None,
        })
        print(f"  [{name}] {schema}.{ts.name:<16} valid={rep.error is None!s:<5} "
              f"MA={tp}/{ngold}  {rep.seconds:5.2f}s", flush=True)
    return aggregate(name, per_table, _client_delta(args, client_stats0))


def _client_stats(args):
    return {k: (c.calls, c.input_tokens, c.output_tokens) for k, c in
            (("client", getattr(args, "client", None)), ("ft", getattr(args, "ft_client", None))) if c}


def _client_delta(args, before):
    after = _client_stats(args)
    return {k: [a - b for a, b in zip(after[k], before.get(k, (0, 0, 0)))] for k in after}


def aggregate(name: str, rows: list[dict], llm_usage: dict) -> dict:
    n = len(rows)
    mvr = sum(r["valid"] for r in rows) / n
    p, rc, f1 = prf(sum(r["tp"] for r in rows), sum(r["n_pred"] for r in rows), sum(r["n_gold"] for r in rows))
    by_tier = {}
    for t in TIERS:
        rs = [r for r in rows if r["tier"] == t]
        if rs:
            by_tier[t] = prf(sum(r["tp"] for r in rs), sum(r["n_pred"] for r in rs), sum(r["n_gold"] for r in rs))[2]
    halluc = {k: sum(r["halluc"][k] for r in rows) / n for k, _, _ in HALLUCINATIONS}
    measured = [k for k, _, _ in HALLUCINATIONS if k != "math_delusion"]
    mean_h = statistics.mean(halluc[k] for k in measured)
    det = defaultdict(lambda: [0, 0])
    tp_runs = fp_runs = inj_runs = 0
    clean_runs = 0
    for r in rows:
        for kind, detected in r["recon"]:
            if kind == "clean":
                clean_runs += 1
                fp_runs += detected
            else:
                inj_runs += 1
                tp_runs += detected
                det[kind][0] += detected
                det[kind][1] += 1
    rf_r = tp_runs / inj_runs if inj_runs else 0.0
    rf_p = tp_runs / (tp_runs + fp_runs) if tp_runs + fp_runs else 0.0
    rf_f1 = 2 * rf_p * rf_r / (rf_p + rf_r) if rf_p + rf_r else 0.0
    return {
        "strategy": name, "label": LABELS.get(name, name), "tables": n, "MVR": mvr,
        "MA_P": p, "MA_R": rc, "MA_F1": f1, "MA_by_tier": by_tier,
        "RF_P": rf_p, "RF_R": rf_r, "RF_F1": rf_f1, "false_positive_runs": fp_runs, "clean_runs": clean_runs,
        "recall_by_injection": {k: v[0] / v[1] for k, v in det.items()},
        "injection_runs": {k: v[1] for k, v in det.items()},
        "hallucination": halluc, "mean_hallucination": mean_h,
        "CMS": cms(mvr, f1, rf_f1, mean_h),
        "avg_seconds": statistics.mean(r["seconds"] for r in rows), "llm_usage": llm_usage,
        "per_table": rows,
    }


def ruleset_study(h: Harness) -> dict:
    """Reconciliation fidelity of rule sets alone, on gold migrations."""
    out = {}
    for label, fn in (("Generic (row count + schema)", lambda t, g, s: generic_rules(t, s)),
                      ("Deterministic (+ PK, partition counts)", lambda t, g, s: deterministic_rules(t, s)),
                      ("Schema-aware (typed rules from mapping)", lambda t, g, s: schema_aware_rules(t, g, s))):
        det, fp, clean, explained = defaultdict(lambda: [0, 0]), 0, 0, 0
        for schema, ts in corpus.all_tables():
            table, gold, spec, _ = h.runs[(schema, ts.name)]
            for kind, detected, summary in h.reconcile_runs(schema, ts, fn(table, gold, spec)):
                if kind == "clean":
                    clean += 1
                    fp += detected
                else:
                    det[kind][0] += detected
                    det[kind][1] += 1
                    explained += detected and any(c != "<table>" for c in summary["failed_by_column"])
        tot = sum(v[1] for v in det.values())
        hit = sum(v[0] for v in det.values())
        out[label] = {"recall_by_injection": {k: v[0] / v[1] for k, v in det.items()},
                      "runs": {k: v[1] for k, v in det.items()},
                      "recall": hit / tot, "false_positive_runs": fp, "clean_runs": clean,
                      "column_localised": explained / hit if hit else 0.0}
    return out


# --------------------------------------------------------------------------

def to_markdown(res: dict) -> str:
    L = ["# Evaluation results", "", f"Corpus: {res['corpus']}", "", f"Backend: {res.get('backend', 'n/a')}", ""]
    S = res["strategies"]
    if S:
        L += ["## Overall", "", "| Strategy | MVR | MA P | MA R | MA F1 | RF P | RF R | RF F1 | Mean halluc. | CMS | s/table |",
              "|---|---|---|---|---|---|---|---|---|---|---|"]
        for s in S:
            L.append(f"| {s['label']} | {s['MVR']:.2f} | {s['MA_P']:.2f} | {s['MA_R']:.2f} | {s['MA_F1']:.2f} | "
                     f"{s['RF_P']:.2f} | {s['RF_R']:.2f} | {s['RF_F1']:.2f} | {s['mean_hallucination']:.2f} | "
                     f"{s['CMS']:.2f} | {s['avg_seconds']:.2f} |")
        L += ["", "## Mapping accuracy (F1) by tier", "", "| Strategy | " + " | ".join(TIERS) + " |", "|---" * (len(TIERS) + 1) + "|"]
        for s in S:
            L.append(f"| {s['label']} | " + " | ".join(f"{s['MA_by_tier'].get(t, float('nan')):.2f}" for t in TIERS) + " |")
        L += ["", "## Hallucination rate by category (fraction of tables)", "",
              "| Category | " + " | ".join(s["strategy"] for s in S) + " |", "|---" * (len(S) + 1) + "|"]
        for k, lab, grp in HALLUCINATIONS:
            L.append(f"| {lab} ({grp}) | " + " | ".join(f"{s['hallucination'][k]:.2f}" for s in S) + " |")
        L += ["", "## Reconciliation recall by injected discrepancy", "",
              "| Discrepancy | runs | " + " | ".join(s["strategy"] for s in S) + " |", "|---" * (len(S) + 2) + "|"]
        for k in INJECTIONS:
            runs = S[0]["injection_runs"].get(k, 0)
            L.append(f"| {k} | {runs} | " + " | ".join(f"{s['recall_by_injection'].get(k, 0):.2f}" for s in S) + " |")
        L.append("| false-positive clean runs | " + str(S[0]["clean_runs"]) + " | " +
                 " | ".join(str(s["false_positive_runs"]) for s in S) + " |")
    if res.get("rulesets"):
        R = res["rulesets"]
        L += ["", "## Rule-set study (gold mappings, injected discrepancies)", "",
              "| Discrepancy | " + " | ".join(R) + " |", "|---" * (len(R) + 1) + "|"]
        for k in INJECTIONS:
            L.append(f"| {k} | " + " | ".join(f"{R[r]['recall_by_injection'].get(k, 0):.2f}" for r in R) + " |")
        L.append("| overall recall | " + " | ".join(f"{R[r]['recall']:.2f}" for r in R) + " |")
        L.append("| false positives (clean runs) | " + " | ".join(f"{R[r]['false_positive_runs']}/{R[r]['clean_runs']}" for r in R) + " |")
        L.append("| detections localised to a column | " + " | ".join(f"{R[r]['column_localised']:.2f}" for r in R) + " |")
    return "\n".join(L) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--strategies", default="h1,h2-noverify,h2")
    ap.add_argument("--out", default="results")
    ap.add_argument("--provider", default=None, choices=["auto", "anthropic", "ollama"],
                    help="LLM for S1/S3: anthropic (paid API) or ollama (free, local); auto = anthropic if ANTHROPIC_API_KEY is set, else ollama")
    ap.add_argument("--model", default=None,
                    help="model name (default claude-opus-5-5 for anthropic, qwen2.5:7b for ollama)")
    ap.add_argument("--ft-model", default=None, help="fine-tuned model name for S2")
    ap.add_argument("--ft-base-url", default=None, help="OpenAI-compatible endpoint serving the S2 model")
    ap.add_argument("--tables", default=None, help="comma-separated subset of table names (quick runs)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--workdir", default=None,
                    help="scratch folder for intermediate files (default: a fresh folder in the system temp dir)")
    ap.add_argument("--no-rulesets", action="store_true")
    ap.add_argument("--pg-dsn", default=None,
                    help="run against PostgreSQL, e.g. postgresql://postgres:pw@localhost:5432/amt_corpus (corpus is reloaded)")
    ap.add_argument("--mssql", default=None,
                    help="run against SQL Server, e.g. mssql://localhost/amt_corpus?instance=SQLEXPRESS (corpus is reloaded)")
    ap.add_argument("--s3", default=None, help="write output to an S3/MinIO location, e.g. s3://amt-lake/eval")
    args = ap.parse_args(argv)
    args.tables = set(args.tables.split(",")) if args.tables else None
    names = [s.strip() for s in args.strategies.split(",") if s.strip()]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if any(n.startswith(("s1", "s3")) for n in names):
        args.client = make_client(args.provider, args.model, cache_dir=str(out / "llm_cache"))
        print(f"LLM: {type(args.client).__name__} {args.client.model}", flush=True)
    if "s2" in names:
        if not args.ft_model:
            ap.error("--ft-model is required for s2")
        args.ft_client = OpenAICompatClient(args.ft_model, args.ft_base_url, cache_dir=str(out / "llm_cache"))

    t0 = time.time()
    print("building corpus, precedents and injected runs ...", flush=True)
    work = Path(args.workdir) if args.workdir else Path(tempfile.mkdtemp(prefix="amt_eval_"))
    h = Harness(work, args.seed, args.pg_dsn, args.s3, args.mssql)
    print(h.backend, flush=True)
    tiers = defaultdict(int)
    for _, ts in corpus.all_tables():
        tiers[ts.tier] += 1
    n_runs = sum(len(v[3]) for v in h.runs.values())
    res = {"corpus": f"{len(corpus.SCHEMAS)} schemas, {sum(tiers.values())} tables "
                     f"({', '.join(f'{t}: {tiers[t]}' for t in TIERS)}), {corpus.ROWS} rows/table, "
                     f"{n_runs} reconciliation runs per strategy (clean + injected)",
           "backend": h.backend, "strategies": [], "rulesets": None,
           "llm": f"{type(args.client).__name__} {args.client.model}" if getattr(args, "client", None) else None}
    for n in names:
        print(f"strategy {n}", flush=True)
        res["strategies"].append(evaluate_strategy(n, h, args))
    if not args.no_rulesets:
        res["rulesets"] = ruleset_study(h)
    res["seconds"] = time.time() - t0
    (out / "results.json").write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    (out / "results.md").write_text(to_markdown(res), encoding="utf-8")
    print(to_markdown(res))
    for s in h.sources.values():
        s.close()
    if args.s3:
        h.target.delete_prefix("")
    if not args.workdir:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
