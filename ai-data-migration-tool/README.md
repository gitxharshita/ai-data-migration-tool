# AI Data Migration Tool

LLM-assisted schema mapping and reconciliation for relational → cloud-storage
(Parquet/CSV) migration. M.Tech project, Harshita Gupta, IIT Jodhpur.

## Architecture (maps to Section III of the paper)

| Paper component | Code |
|---|---|
| Connector Layer (PostgreSQL, SQL Server, MySQL, SQLite → S3/MinIO, Azure Data Lake, GCS, local) | `amt/connectors/` |
| Schema Mapper: S1 zero-shot, S2 fine-tuned, S3 RAG | `amt/mappers.py`, `amt/llm.py` |
| Precedent-mapping retriever (top-k = 8) | `amt/retriever.py` |
| Migration Executor (type coercion, partitioned writes) | `amt/executor.py` |
| Validation & Reconciliation Engine | `amt/reconcile.py` |
| Agent Manager (request / execution / implementation verification, one self-repair round) | `amt/agent.py` |
| Reconciliation summary report (Markdown + JSON) | `amt/report.py` |
| 36-table corpus, 6 schemas, tiers 9/11/10/6 | `eval/corpus.py` |
| Six injected discrepancy types | `eval/inject.py` |
| MVR, MA, RF, hallucination taxonomy, CMS | `eval/metrics.py` |
| Evaluation harness | `eval/run_eval.py` |

Every reconciliation value is recomputed deterministically from the source and
the written files. The LLM chooses which rules to run but never supplies the
numbers. This is the hardening against Reconciliation Math Delusion described
in Section IV-H.

## Setup

```bash
pip install -r requirements.txt
```

Only `pyarrow` is required. `psycopg2-binary` (PostgreSQL), `pyodbc` (SQL Server),
`pymysql` (MySQL), `boto3` (S3/MinIO) and `google-cloud-storage` are needed only for
the matching connectors. To run on real PostgreSQL, SQL Server and MinIO, see
[SETUP_POSTGRES_MINIO.md](SETUP_POSTGRES_MINIO.md).

## Web dashboard

```bash
python -m amt ui        # opens http://localhost:8765 in your browser; Ctrl+C to stop
```

Four tabs: **Migrate a table** (connect -> plan mapping with dry run -> migrate ->
reconciliation report), **Try to break it** (inject a known fault and compare
generic checks with schema-aware checks), **Experiment results** (charts from
`results/results.json`) and **How it works** (plain-language explanation).
AI strategies appear once `ANTHROPIC_API_KEY` is set before starting it.
Code: `amt/ui/server.py` (standard-library HTTP + JSON API) and `amt/ui/index.html`.

## Destinations

| Destination | Target address | Python package | Credentials |
|---|---|---|---|
| Local folder | `C:\lake` or `file:///C:/lake` | none | none |
| Amazon S3 | `s3://bucket/path` | `boto3` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` (or `aws configure`) |
| MinIO | `s3://bucket/path?endpoint=http://localhost:9000` | `boto3` | MinIO user/password in the two AWS key variables |
| Azure Data Lake Gen2 / Blob | `abfss://container@account.dfs.core.windows.net/path` | `azure-storage-blob` (+ `azure-identity` for `az login`) | `AZURE_STORAGE_CONNECTION_STRING`, or `AZURE_STORAGE_KEY`, or `az login` |
| Google Cloud Storage | `gs://bucket/path` | `google-cloud-storage` | `GOOGLE_APPLICATION_CREDENTIALS` or `gcloud auth application-default login` |

In the dashboard, step 3 lets you pick any of these, shows whether each is ready on
this machine, and has a **Test connection** button. From the command line, pass the
address as `--target`, for example
`python -m amt migrate --source ... --table orders --target abfss://lake@myacct.dfs.core.windows.net/raw`.
Set credentials as environment variables *before* starting the dashboard or command.

## Multi-user / shared demo

```bash
python -m amt ui --host 0.0.0.0 --public --no-browser   # many users, demo data only
python tools/loadtest.py --users 100 --ramp 60          # simulate 100 users
```

Work runs as background jobs on worker processes, every visitor has a private
session, and results can be downloaded as a .zip. Measured on this laptop: 100
users with 0 errors. See [DEPLOY.md](DEPLOY.md) for numbers, Docker and hosting.

## Usage

```bash
python -m amt demo                                   # builds the e-commerce schema, migrates it, writes a report
python -m amt describe --source sqlite:///shop.db
python -m amt plan     --source sqlite:///shop.db --table orders --strategy h2
python -m amt migrate  --source postgresql://user:pw@host/db --table orders \
       --target s3://bucket/lake --format parquet --partition-by placed_at --strategy s3
```

Strategies: `h1` declared-type rules, `h2` sample + precedent rules (both
offline, no LLM), `s1` zero-shot, `s2` fine-tuned, `s3` RAG + verification.
**Free option:** install [Ollama](https://ollama.com/download), run `ollama pull qwen2.5:7b`,
and the LLM strategies use that local model automatically (`--provider ollama`, model via
`--model` or `AMT_OLLAMA_MODEL`). No account or key; slower on a laptop CPU.
**Paid option:** the LLM strategies use Claude when you `pip install anthropic` and set an `ANTHROPIC_API_KEY` from
platform.claude.com (model via `--model` or `AMT_MODEL`, default `claude-opus-5-5`;
thinking effort via `AMT_EFFORT`, default `medium`).
S2 talks to any OpenAI-compatible endpoint serving your LoRA model
(`--ft-model`, `--ft-base-url`, e.g. vLLM or Ollama).

## For data engineers: large tables, job files, Delta Lake and Iceberg

```bash
python -m amt init-job --source sqlite:///shop.db --target C:/lake > shop.yaml
python -m amt run shop.yaml                 # full load, then incremental; exit 0 pass / 1 fail / 2 error
python -m amt verify shop.yaml              # re-check the output, write nothing
python -m amt run shop.yaml --rerun-failed  # rewrite only the partitions that failed
```

A streaming engine (`amt/stream.py`) copies and checks any table size in one pass with flat
memory: per-partition fingerprints, a database cross-check (the source recomputes counts and
sums in SQL), incremental loads with watermarks, Delta upserts, and output as Parquet, CSV,
Delta Lake or Apache Iceberg. 1,000,000 rows: 177 s, 471 MB, all checks passed.
Full guide: [DATA_ENGINEERS.md](DATA_ENGINEERS.md).

## Reproducing the paper's experiments

```bash
python -m unittest discover -s tests -v              # 10 tests
python -m eval.run_eval                              # offline baselines + rule-set study (~1 min)
python -m eval.run_eval --strategies s1,s3,s3-noverify,s1+verify   # LLM strategies + ablation
python -m eval.make_finetune_data --holdout support,logistics      # S2 training data (JSONL)
python -m eval.run_eval --strategies s2 --ft-model <name> --ft-base-url http://localhost:8000/v1
```

Results go to `results/results.md` and `results/results.json`. LLM responses
are cached in `results/llm_cache/`, so re-runs are free and reproducible.
Precedents are used leave-one-schema-out: a table never retrieves precedents
from its own schema.

## Current offline results (`results/results.md`)

* Rule-set study on gold migrations: generic row-count + schema checks detect
  0% of truncated decimals and 0% of timezone shifts (overall recall 0.61).
  Schema-aware typed rules detect 100% (recall 1.00), with 0/36 false positives.
* Declared-type mapping (H1) reaches MA F1 0.91, falling to 0.81 on Extra Hard
  tables. Sample-aware mapping (H2) reaches 0.99.
* The same evaluation on SQL Server (`results_sqlserver/results.md`) gives the same rule-set
  findings. H1 drops to 0.73 on Extra Hard tables there, because SQL Server stores JSON as
  `NVARCHAR(MAX)`, so declared types reveal less on that engine.
* H2 is a rule-based reference written by the same author as the corpus, so
  its near-perfect score is an upper reference, not an LLM result.
