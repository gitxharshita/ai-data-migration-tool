"""Export (schema prompt, gold mapping + rules) pairs as chat-format JSONL for
LoRA fine-tuning of the S2 model. Hold out the evaluation schemas you will
test on with --holdout so S2 never sees them during training.

    python -m eval.make_finetune_data --out finetune.jsonl --holdout support,logistics
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.connectors import SQLiteSource  # noqa: E402
from amt.mappers import SYSTEM, schema_block, target_block  # noqa: E402
from amt.reconcile import typed_rules  # noqa: E402
from eval import corpus  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="finetune.jsonl")
    ap.add_argument("--holdout", default="", help="comma-separated schemas to exclude")
    args = ap.parse_args(argv)
    hold = set(filter(None, args.holdout.split(",")))
    n = 0
    with tempfile.TemporaryDirectory() as tmp, open(args.out, "w", encoding="utf-8") as f:
        for schema in corpus.SCHEMAS:
            if schema in hold:
                continue
            db = Path(tmp) / f"{schema}.db"
            corpus.build_schema_db(schema, db)
            src = SQLiteSource(str(db))
            for ts in corpus.SCHEMAS[schema]:
                table = src.describe(ts.name)
                gold = corpus.gold_mapping(ts)
                spec = corpus.target_spec(ts, "")
                answer = {"columns": [{k: v for k, v in vars(c).items() if k != "rationale"} for c in gold.columns],
                          "rules": [{"kind": r.kind, "column": r.column, "tolerance": 0}
                                    for r in typed_rules(table, gold)]}
                f.write(json.dumps({"messages": [
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": f"{target_block(spec)}\n\n{schema_block(table, False)}"},
                    {"role": "assistant", "content": json.dumps(answer)}]}) + "\n")
                n += 1
            src.close()
    print(f"wrote {n} examples to {args.out}")


if __name__ == "__main__":
    main()
