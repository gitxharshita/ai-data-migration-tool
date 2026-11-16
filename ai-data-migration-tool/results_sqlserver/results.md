# Evaluation results

Corpus: 6 schemas, 36 tables (Easy: 9, Medium: 11, Hard: 10, Extra Hard: 6), 300 rows/table, 158 reconciliation runs per strategy (clean + injected)

Backend: source: SQL Server; target: local filesystem

## Overall

| Strategy | MVR | MA P | MA R | MA F1 | RF P | RF R | RF F1 | Mean halluc. | CMS | s/table |
|---|---|---|---|---|---|---|---|---|---|---|
| H1 Declared-type rules (no LLM) | 1.00 | 0.91 | 0.91 | 0.91 | 1.00 | 0.98 | 0.99 | 0.07 | 0.96 | 0.36 |
| H2 Sample+precedent rules, no verification | 1.00 | 0.99 | 0.99 | 0.99 | 1.00 | 1.00 | 1.00 | 0.00 | 1.00 | 0.31 |
| H2 Sample+precedent rules + verification (no LLM) | 1.00 | 0.99 | 0.99 | 0.99 | 1.00 | 1.00 | 1.00 | 0.00 | 1.00 | 0.30 |

## Mapping accuracy (F1) by tier

| Strategy | Easy | Medium | Hard | Extra Hard |
|---|---|---|---|---|
| H1 Declared-type rules (no LLM) | 1.00 | 0.96 | 0.92 | 0.73 |
| H2 Sample+precedent rules, no verification | 1.00 | 1.00 | 0.98 | 0.97 |
| H2 Sample+precedent rules + verification (no LLM) | 1.00 | 1.00 | 0.98 | 0.97 |

## Hallucination rate by category (fraction of tables)

| Category | h1 | h2-noverify | h2 |
|---|---|---|---|
| Mapping Contradiction (schema) | 0.33 | 0.00 | 0.00 |
| Type Overanalysis (schema) | 0.00 | 0.00 | 0.00 |
| Null-Convention Misrepresentation (schema) | 0.00 | 0.00 | 0.00 |
| Redundant Transformation (logic) | 0.00 | 0.00 | 0.00 |
| Trivial-Rule Abuse (logic) | 0.00 | 0.00 | 0.00 |
| Reconciliation Math Delusion (logic) | 0.00 | 0.00 | 0.00 |

## Reconciliation recall by injected discrepancy

| Discrepancy | runs | h1 | h2-noverify | h2 |
|---|---|---|---|---|
| truncated_decimal | 18 | 1.00 | 1.00 | 1.00 |
| dropped_null_row | 10 | 1.00 | 1.00 | 1.00 |
| duplicated_row | 36 | 1.00 | 1.00 | 1.00 |
| timezone_shift | 16 | 0.88 | 1.00 | 1.00 |
| off_by_one_partition | 36 | 1.00 | 1.00 | 1.00 |
| flattened_json | 6 | 1.00 | 1.00 | 1.00 |
| false-positive clean runs | 36 | 0 | 0 | 0 |

## Rule-set study (gold mappings, injected discrepancies)

| Discrepancy | Generic (row count + schema) | Deterministic (+ PK, partition counts) | Schema-aware (typed rules from mapping) |
|---|---|---|---|
| truncated_decimal | 0.00 | 0.00 | 1.00 |
| dropped_null_row | 1.00 | 1.00 | 1.00 |
| duplicated_row | 1.00 | 1.00 | 1.00 |
| timezone_shift | 0.00 | 0.00 | 1.00 |
| off_by_one_partition | 0.64 | 1.00 | 1.00 |
| flattened_json | 1.00 | 1.00 | 1.00 |
| overall recall | 0.61 | 0.72 | 1.00 |
| false positives (clean runs) | 0/36 | 0/36 | 0/36 |
| detections localised to a column | 0.00 | 0.93 | 1.00 |
