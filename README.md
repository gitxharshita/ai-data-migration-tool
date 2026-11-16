# AI-Assisted Schema Mapping and Reconciliation for Relational-to-Cloud-Storage Data Migration

M.Tech. Data Engineering project, Indian Institute of Technology Jodhpur
(School of Artificial Intelligence and Data Science). Supervisor: Dr. Aman Pathak.

An AI Data Migration Tool that copies relational tables into cloud storage and proves,
check by check, that nothing was lost or changed. It generates the schema mapping and the
reconciliation checks that verify it as one pipeline, recomputes every check from the
source and the written files, and catches silent errors (rounded decimals, time-zone
shifts, lost NULLs, flattened JSON) that row-count checks miss.

## Contents

| Folder | What is in it |
|---|---|
| [`ai-data-migration-tool/`](ai-data-migration-tool/) | The tool: connectors, mappers, reconciliation engine, streaming engine, web dashboard, evaluation harness, results and 50 automated tests. Start with its [README](ai-data-migration-tool/README.md). |
| [`report/`](report/) | The M.Tech project report (Word and PDF) and the scripts that build it from the IIT Jodhpur format. |

## Quick start

```bash
cd ai-data-migration-tool
pip install -r requirements.txt
python -m amt ui                       # web dashboard at http://localhost:8765
python -m eval.run_eval                # reproduce the evaluation
python -m unittest discover -s tests   # run the tests
```

For data engineers (large tables, job files, Delta Lake and Iceberg), see
[`DATA_ENGINEERS.md`](ai-data-migration-tool/DATA_ENGINEERS.md).

## Key results

* On a 36-table fault-injection corpus, row-count checks detected 61% of injected faults and none
  of the rounded decimals or time-zone shifts; the schema-aware checks detected all 122, with no
  false alarms, on SQLite, SQL Server and MinIO.
* Grounding the mapper in sample values raised mapping F1 from 0.91 to 0.99.
* A local LLM (Qwen2.5 7B) reached 0.99 with retrieval; letting it map but letting code choose the
  checks gave 100% detection with no false alarms.
* The streaming engine migrated and verified 1,000,000 rows in 177 s with 471 MB of memory.
