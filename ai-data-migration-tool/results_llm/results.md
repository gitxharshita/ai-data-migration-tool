# Evaluation results

Corpus: 6 schemas, 36 tables (Easy: 9, Medium: 11, Hard: 10, Extra Hard: 6), 300 rows/table, 158 reconciliation runs per strategy (clean + injected)

Backend: source: SQLite; target: local filesystem

## Overall

| Strategy | MVR | MA P | MA R | MA F1 | RF P | RF R | RF F1 | Mean halluc. | CMS | s/table |
|---|---|---|---|---|---|---|---|---|---|---|
| H1 Declared-type rules (no LLM) | 1.00 | 0.91 | 0.91 | 0.91 | 1.00 | 0.96 | 0.98 | 0.06 | 0.96 | 0.26 |
| H2 Sample+precedent rules + verification (no LLM) | 1.00 | 0.99 | 0.99 | 0.99 | 1.00 | 1.00 | 1.00 | 0.00 | 1.00 | 0.28 |
| S1 Zero-shot prompting | 0.83 | 0.67 | 0.67 | 0.67 | 1.00 | 0.72 | 0.84 | 0.31 | 0.77 | 136.91 |
| S1 + execution verification | 0.86 | 0.68 | 0.68 | 0.68 | 1.00 | 0.72 | 0.84 | 0.31 | 0.78 | 136.91 |
| Hybrid: S1 mapping + code-generated checks | 0.86 | 0.68 | 0.68 | 0.68 | 1.00 | 1.00 | 1.00 | 0.24 | 0.84 | 136.91 |
| S3 Schema+sample-aware RAG + verification | 0.97 | 0.99 | 0.99 | 0.99 | 0.89 | 0.86 | 0.88 | 0.01 | 0.95 | 228.08 |
| S3 RAG, no verification | 0.97 | 0.99 | 0.99 | 0.99 | 0.89 | 0.86 | 0.88 | 0.01 | 0.95 | 228.08 |
| Hybrid: S3 mapping + code-generated checks | 0.97 | 0.99 | 0.99 | 0.99 | 1.00 | 1.00 | 1.00 | 0.01 | 0.99 | 228.08 |

## Mapping accuracy (F1) by tier

| Strategy | Easy | Medium | Hard | Extra Hard |
|---|---|---|---|---|
| H1 Declared-type rules (no LLM) | 1.00 | 0.96 | 0.88 | 0.81 |
| H2 Sample+precedent rules + verification (no LLM) | 1.00 | 1.00 | 0.98 | 0.97 |
| S1 Zero-shot prompting | 0.69 | 0.69 | 0.67 | 0.62 |
| S1 + execution verification | 0.69 | 0.69 | 0.67 | 0.68 |
| Hybrid: S1 mapping + code-generated checks | 0.69 | 0.69 | 0.67 | 0.68 |
| S3 Schema+sample-aware RAG + verification | 1.00 | 1.00 | 0.98 | 1.00 |
| S3 RAG, no verification | 1.00 | 1.00 | 0.98 | 1.00 |
| Hybrid: S3 mapping + code-generated checks | 1.00 | 1.00 | 0.98 | 1.00 |

## Hallucination rate by category (fraction of tables)

| Category | h1 | h2 | s1 | s1+verify | s1+code | s3 | s3-noverify | s3+code |
|---|---|---|---|---|---|---|---|---|
| Mapping Contradiction (schema) | 0.31 | 0.00 | 0.11 | 0.11 | 0.11 | 0.00 | 0.00 | 0.00 |
| Type Overanalysis (schema) | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Null-Convention Misrepresentation (schema) | 0.00 | 0.00 | 1.00 | 1.00 | 1.00 | 0.03 | 0.03 | 0.03 |
| Redundant Transformation (logic) | 0.00 | 0.00 | 0.08 | 0.08 | 0.08 | 0.00 | 0.00 | 0.00 |
| Trivial-Rule Abuse (logic) | 0.00 | 0.00 | 0.36 | 0.36 | 0.00 | 0.03 | 0.03 | 0.00 |
| Reconciliation Math Delusion (logic) | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

## Reconciliation recall by injected discrepancy

| Discrepancy | runs | h1 | h2 | s1 | s1+verify | s1+code | s3 | s3-noverify | s3+code |
|---|---|---|---|---|---|---|---|---|---|
| truncated_decimal | 18 | 0.83 | 1.00 | 0.00 | 0.00 | 1.00 | 0.67 | 0.67 | 1.00 |
| dropped_null_row | 10 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| duplicated_row | 36 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| timezone_shift | 16 | 0.88 | 1.00 | 0.00 | 0.00 | 1.00 | 0.31 | 0.31 | 1.00 |
| off_by_one_partition | 36 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| flattened_json | 6 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| false-positive clean runs | 36 | 0 | 0 | 0 | 0 | 0 | 13 | 13 | 0 |

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


AI times per table are from the first (uncached) overnight run; the hybrid rows reuse the same AI answers.
