"""Precedent-mapping retriever (Section III-C).

Indexes column-level mappings from previously completed migrations and
returns the most structurally similar ones for a new column. Uses a small
TF-IDF model over name tokens, declared-type family and value shape, so it
needs no embedding service; swap in dense embeddings via `encode` if desired.
"""
from __future__ import annotations

import json
import math
import re
from collections import Counter
from pathlib import Path

from .models import Column
from .typesys import declared_family, profile_values


def column_features(name: str, declared: str, prof: dict) -> list[str]:
    toks = [t for t in re.split(r"[_\W]+", name.lower()) if t]
    feats = [f"w:{t}" for t in toks] + [f"w2:{a}_{b}" for a, b in zip(toks, toks[1:])]
    if toks:
        feats += [f"prefix:{toks[0]}", f"suffix:{toks[-1]}"]
    feats.append(f"decl:{declared_family(declared)}")
    shape = prof.get("shape", "empty")
    if shape == "decimal":
        shape += f"_s{prof.get('max_scale', 0)}"
    feats += [f"shape:{shape}"] * 2
    if prof.get("nulls"):
        feats.append("has_nulls")
    return feats


class PrecedentStore:
    def __init__(self, entries: list[dict] | None = None):
        self.entries: list[dict] = []
        self.df: Counter = Counter()
        for e in entries or []:
            self.add(e)

    def add(self, entry: dict) -> None:
        """entry: {schema, table, column, declared_type, profile, mapping{target_type, transform, null_handling}}"""
        entry = dict(entry)
        entry["features"] = column_features(entry["column"], entry["declared_type"], entry.get("profile", {}))
        self.entries.append(entry)
        self.df.update(set(entry["features"]))

    def _vec(self, feats: list[str]) -> dict[str, float]:
        n = max(1, len(self.entries))
        tf = Counter(feats)
        v = {f: c * (math.log((1 + n) / (1 + self.df.get(f, 0))) + 1) for f, c in tf.items()}
        norm = math.sqrt(sum(x * x for x in v.values())) or 1.0
        return {f: x / norm for f, x in v.items()}

    def search(self, col: Column, sample_values: list, k: int = 8, exclude_schema: str | None = None) -> list[tuple[float, dict]]:
        q = self._vec(column_features(col.name, col.declared_type, profile_values(sample_values)))
        scored = []
        for e in self.entries:
            if exclude_schema and e.get("schema") == exclude_schema:
                continue
            ev = self._vec(e["features"])
            s = sum(q[f] * ev.get(f, 0.0) for f in q)
            scored.append((s, e))
        scored.sort(key=lambda x: -x[0])
        return scored[:k]

    def save(self, path: str) -> None:
        Path(path).write_text("\n".join(json.dumps({k: v for k, v in e.items() if k != "features"}, default=str)
                                        for e in self.entries), encoding="utf-8")

    @staticmethod
    def load(path: str) -> "PrecedentStore":
        p = Path(path)
        if not p.exists():
            return PrecedentStore()
        return PrecedentStore([json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()])
