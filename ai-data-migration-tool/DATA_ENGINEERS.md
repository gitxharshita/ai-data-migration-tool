# Using the tool in data pipelines

For large tables, many tables, schedulers and lakehouse formats. The dashboard is
for exploring one table; everything here runs from the command line.

## Quick start

```powershell
python -m amt init-job --source sqlite:///shop.db --target C:/lake > shop.yaml   # a starter job file
python -m amt run shop.yaml            # first run: full load; later runs: only new rows
python -m amt verify shop.yaml         # re-check the output, write nothing
python -m amt run shop.yaml --rerun-failed   # rewrite only the partitions that failed
```

`init-job` looks at every table and guesses a month partition column, an
incremental column (`updated_at`, `modified...`) and columns to mask (`email`, `phone`...).
Edit the file, then run it.

## What happens in a run

1. **Plan** each table: column types from declared types, sample values and past
   migrations (or the AI with `strategy: s3`), then a dry run on sample rows.
2. **Copy in one streaming pass.** Rows are read in chunks (`chunk_rows`, default 50,000),
   converted and written. In the same pass, the source side of every check is computed.
   Memory stays flat whatever the table size.
3. **Read the output back** file by file (or batch by batch for Delta and Iceberg), and
   compute the output side of every check.
4. **Compare**: row counts, exact decimal totals, NULL counts, time-zone-safe timestamps,
   min/max, distinct values, true/false ratios, duplicate keys, per-month row counts and
   a fingerprint of every value.
5. **Per-partition verdicts**: every month gets its own fingerprint, so a failure reads
   "updated_at=2023-05 differs", and `--rerun-failed` rewrites only that month.
6. **Database cross-check**: the source database recomputes `COUNT(*)`, NULL counts and
   integer / decimal `SUM`, `MIN`, `MAX` itself with SQL, compared with the output. This is
   independent evidence that the extraction itself lost nothing. (SQLite decimals are
   floats, so decimal sums are cross-checked only on PostgreSQL, MySQL and SQL Server.)
7. **Report** in `report_dir`: `last_run.json` (for machines), `last_run.md` (for people),
   plus one JSON file per run.

All checks use mergeable summaries (counts, exact sums, 64-bit value hashes,
order-independent fingerprints), so no check ever holds a whole column in memory, except
distinct counts and duplicate-key checks, which keep one 8-byte hash per distinct value.

## Exit codes

| Code | Meaning | What a scheduler should do |
|---|---|---|
| 0 | every table passed, or had no new rows | continue |
| 1 | a check failed: the data must not be trusted | stop downstream jobs, alert |
| 2 | the job could not run (bad job file, connection, permission) | alert |

## Incremental loads

```yaml
  - name: orders
    incremental: {column: updated_at, mode: append}
```

* The first run copies everything up to the current `MAX(updated_at)` and saves it as the
  **watermark** in `state_file`.
* Each later run copies only rows with `updated_at` > watermark, checks exactly those rows
  (the files of this run, or the rows tagged with this run in Delta / Iceberg), and moves
  the watermark **only if every check passed**. A failed run is retried in full next time.
* `mode: upsert` (Delta only): changed rows *replace* their old version, matched on the
  primary key (Delta MERGE). With `append`, a changed row is added again.
* `--full` ignores the watermark and reloads the table.

## Output formats

| `format` | Written as | Notes |
|---|---|---|
| `parquet`, `csv` | `<target>/<table>/<partition>/part-<run>-NNNNN.parquet` | any target: folder, S3, MinIO, ADLS, GCS |
| `delta` | a Delta Lake table at `<target>/<table>` | needs `pip install deltalake`; cloud credentials in `storage_options` |
| `iceberg` | an Apache Iceberg table `<namespace>.<table>` | needs `pip install "pyiceberg[sql-sqlite,pyarrow]"` and a `catalog` |

Delta and Iceberg tables get two extra columns: `<column>_month` (the partition value)
and `_amt_run_id` (which run wrote the row, for lineage and incremental checks).

**Catalogs.** Iceberg tables are registered in the catalog you configure: a local SQL
catalog (a SQLite file, tested), or AWS Glue, a REST catalog (e.g. Polaris, Unity Catalog's
Iceberg REST endpoint) or Hive with the properties pyiceberg documents (not tested
here). For Delta, the report contains the SQL to register the table in Databricks /
Spark (`CREATE TABLE ... USING DELTA LOCATION ...`) and to query it from DuckDB.

## Job file reference

| Setting | Default | Meaning |
|---|---|---|
| `name` | file name | used for the report and state folders |
| `source` | required | database URI (same formats as the dashboard) |
| `target` | required except Iceberg | folder or cloud location |
| `format` | `parquet` | `parquet`, `csv`, `delta`, `iceberg` |
| `strategy` | `h2` | `h2` rules, `h1` declared types only, `s3` AI mapping (Ollama / Anthropic) |
| `rows_per_file` | 100000 | rows per output file (Parquet / CSV) |
| `chunk_rows` | 50000 | rows processed at a time |
| `crosscheck` | `true` | database cross-check on or off |
| `catalog` | none | Iceberg catalog properties, plus `namespace` |
| `storage_options` | none | Delta cloud credentials |
| `state_file` | `.amt_state/<name>.json` | watermarks and failed partitions |
| `report_dir` | `reports/<name>` | reports |
| `tables` | required | list of tables, or `"*"` for all |

Per table: `name`, `partition_by`, `partition_grain` (`month`, `day`, `value`),
`incremental` (`column`, `mode`), `mask` (columns), `overrides`
(`{column: {target_type, transform, null_handling}}`), `path` (output folder / table
name), `strategy`.

`${VARIABLE}` anywhere in the file is replaced by that environment variable, so passwords
and keys never have to be written in the job file.

Tables are processed parents first (foreign keys), so referenced tables land before the
tables that point to them.

## Scheduling examples

**cron (Linux)**

```cron
0 2 * * *  cd /opt/amt && python -m amt run jobs/shop.yaml >> logs/shop.log 2>&1
```

**Windows Task Scheduler**: action `python`, arguments `-m amt run C:\jobs\shop.yaml`,
start in the tool folder.

**Airflow**

```python
from airflow.operators.bash import BashOperator
migrate = BashOperator(task_id="migrate_shop", bash_command="cd /opt/amt && python -m amt run jobs/shop.yaml")
# exit code 1 or 2 fails the task, so downstream tasks do not run on untrusted data
```

**Azure Data Factory / Synapse**: a *Custom* activity (Azure Batch) or an Azure
Function running the same command; fail the pipeline on a non-zero exit code.

**CI (GitHub Actions)**: `run: python -m amt verify jobs/shop.yaml`, which fails the
build if the lake no longer matches the source.

## Measured performance

Measured with `tools/bench_scale.ps1` on a 4-core laptop (Intel i5-10310U, 16 GB), SQLite
source, Parquet output split by month, every check plus the database cross-check:

| Run | Time | Peak memory | Result |
|---|---|---|---|
| in-memory engine (dashboard), 200,000 rows | 20 s | 424 MB | PASS |
| streaming engine, 200,000 rows | 26 s | 287 MB | PASS |
| streaming engine, 1,000,000 rows | 177 s | 471 MB | PASS (database confirmed 6/6) |

Both engines run at a similar speed; the difference is memory. The in-memory engine holds
the whole table (twice), so its memory grows with every row and very large tables do not
fit; the streaming engine holds at most `chunk_rows` rows plus small summaries, and adds
per-partition verdicts, the database cross-check and incremental loads. Its memory grows
slowly with size only because duplicate-key and distinct-value checks keep one hash per
distinct value. (Before a fix in both engines, reading time-zone timestamps back took the
in-memory engine 220 s for the same 200,000 rows.)

## Limits

* Throughput is roughly 5,000 rows per second on a 4-core laptop. The work is in
  Python (exact type conversion and canonical values), which is reliable but not the fastest
  possible. Several tables can run in parallel as separate jobs.
* `json_flatten` mappings cannot be streamed; keep JSON columns as `json_string`.
* Tested here: SQLite and SQL Server sources; local folders, MinIO, Delta and Iceberg
  (local SQL catalog). PostgreSQL / MySQL server-side cursors, cloud Delta storage and the
  Glue / REST / Hive catalogs are implemented but not tested in this environment.
