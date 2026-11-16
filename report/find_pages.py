"""Finds the printed page number of every heading and caption in the rendered report PDF.

    python find_pages.py report.pdf pages.json build_report.py
"""
import importlib.util
import json
import re
import sys

import fitz  # PyMuPDF

pdf, out, build = sys.argv[1], sys.argv[2], sys.argv[3]
norm = lambda s: re.sub(r"\s+", " ", s).strip()

# read the section list from build_report.py without running its document code
src = open(build, encoding="utf-8").read()
B = []
for m in re.finditer(r'^(ch|sec)\("([\d.]+)", "([^"]+)"\)', src, re.M):
    B.append((m.group(1), m.group(2), m.group(3)))
figs = re.findall(r'\("fig", "([\d.]+)"', src)
tabs = re.findall(r'\("table", "([\d.]+)"', src)

doc = fitz.open(pdf)
pages = []
for pg in doc:
    words = pg.get_text("words")
    label = ""
    if words:
        bottom = max(words, key=lambda w: w[3])
        if bottom[3] > pg.rect.height * 0.85 and re.fullmatch(r"\d+|[ivxlc]+", bottom[4]):
            label = bottom[4]
    lines = [norm(l) for l in pg.get_text("text").splitlines() if norm(l)]
    pages.append((label, lines))

list_end = max(i for i, (_, ls) in enumerate(pages) if "List of Tables" in ls)
contents = min(i for i, (_, ls) in enumerate(pages) if "Contents" in ls)


def find(pred, start, stop=None):
    for i in range(start, stop if stop is not None else len(pages)):
        if any(pred(l) for l in pages[i][1]):
            return pages[i][0]
    return "?"


res = {}
for key in ("Acknowledgements", "Abstract"):
    res[key] = find(lambda l, k=key: l == k, 1, contents)
for kind, num, title in B:
    text = f"{num}. {title.upper()}" if kind == "ch" else f"{num} {title}"
    res[f"{kind}:{num}"] = find(lambda l, t=norm(text): l == t, list_end + 1)
for key in ("Publications", "Appendix", "References"):
    res[key] = find(lambda l, k=key: l == k, list_end + 1)
for n in figs:
    res[f"fig:{n}"] = find(lambda l, n=n: l.startswith(f"Figure {n}:"), list_end + 1)
for n in tabs:
    res[f"tab:{n}"] = find(lambda l, n=n: l.startswith(f"Table {n}:"), list_end + 1)

missing = [k for k, v in res.items() if v in ("?", "")]
json.dump(res, open(out, "w", encoding="utf-8"), indent=1)
labels = [p[0] for p in pages]
print(f"{len(doc)} pages; labels: {' '.join(l or '-' for l in labels)}")
print("not found:", missing or "none")
