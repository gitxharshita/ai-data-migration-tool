"""Deliberate data-fidelity discrepancies injected into written output
(Section III-E). Each injector edits the files of a correct (gold)
migration and returns the edited files, or None if it does not apply."""
from __future__ import annotations

import copy
import json
import random
from datetime import timedelta
from decimal import Decimal

from amt.models import TableMapping
from amt.typesys import parse_decimal_type

INJECTIONS = ["truncated_decimal", "dropped_null_row", "duplicated_row", "timezone_shift",
              "off_by_one_partition", "flattened_json"]


def _cols(mapping: TableMapping, pred):
    return [cm.target for cm in mapping.columns if pred(cm)]


def inject(kind: str, files: list[tuple[str, list[dict]]], mapping: TableMapping, partitioned: bool,
           rng: random.Random) -> list[tuple[str, list[dict]]] | None:
    files = copy.deepcopy(files)
    rows = [(fi, ri) for fi, (_, rs) in enumerate(files) for ri in range(len(rs))]
    if not rows:
        return None

    if kind == "truncated_decimal":
        cols = _cols(mapping, lambda cm: parse_decimal_type(cm.target_type))
        cands = [(fi, ri, c) for c in cols for fi, ri in rows
                 if files[fi][1][ri][c] is not None and files[fi][1][ri][c] != files[fi][1][ri][c].quantize(Decimal("0.1"), rounding="ROUND_DOWN")]
        if not cands:
            return None
        fi, ri, c = rng.choice(cands)
        v = files[fi][1][ri][c]
        files[fi][1][ri][c] = v.quantize(Decimal("0.1"), rounding="ROUND_DOWN").quantize(v)
        return files

    if kind == "dropped_null_row":
        cands = [(fi, ri) for fi, ri in rows if any(v is None for v in files[fi][1][ri].values())]
        if not cands:
            return None
        fi, ri = rng.choice(cands)
        del files[fi][1][ri]
        return files

    if kind == "duplicated_row":
        fi, ri = rng.choice(rows)
        files[fi][1].append(dict(files[fi][1][ri]))
        return files

    if kind == "timezone_shift":
        cols = _cols(mapping, lambda cm: cm.target_type == "timestamp_tz")
        if not cols:
            return None
        c = rng.choice(cols)
        fi = rng.randrange(len(files))  # one file written with the wrong session time zone
        for r in files[fi][1]:
            if r[c] is not None:
                r[c] = r[c] + timedelta(hours=5, minutes=30)
        return files

    if kind == "off_by_one_partition":
        if partitioned and len(files) >= 2:
            # boundary row lands in the neighbouring partition: totals unchanged
            fi = rng.randrange(len(files) - 1)
            if not files[fi][1]:
                return None
            files[fi + 1][1].append(files[fi][1].pop())
            return files
        fi = rng.randrange(len(files))   # bad chunk boundary drops the last row
        if not files[fi][1]:
            return None
        files[fi][1].pop()
        return files

    if kind == "flattened_json":
        cols = _cols(mapping, lambda cm: cm.target_type == "json_string")
        if not cols:
            return None
        c = rng.choice(cols)
        for _, rs in files:
            for r in rs:
                doc = json.loads(r.pop(c)) if r.get(c) else {}
                for k, x in doc.items():
                    r[f"{c}__{k}"] = None if isinstance(x, (dict, list)) else str(x)
        return files

    raise ValueError(kind)
