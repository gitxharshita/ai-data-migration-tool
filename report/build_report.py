"""Builds the M.Tech project report by filling the IIT Jodhpur report template.

    python build_report.py <template.docx> <output.docx> [pages.json]

The Contents, List of Figures and List of Tables need page numbers, which are only
known after Word lays the document out. render_report.ps1 runs this script twice:
first without page numbers, then with the numbers found in the rendered PDF.
"""
import copy
import json
import re
import sys
from pathlib import Path

import docx
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt

HERE = Path(__file__).parent
TEMPLATE, OUT = sys.argv[1], sys.argv[2]
PAGES = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8")) if len(sys.argv) > 3 else {}

TITLE = "AI-Assisted Schema Mapping and Reconciliation for Relational-to-Cloud-Storage Data Migration"
STUDENT, ROLL = "Harshita Gupta", "M25DE2016"
DEGREE = "M.Tech. Data Engineering"
SUPERVISOR = "Dr. Aman Pathak"
TEXT_W = 11906 - 2 * 1080   # text width in twips: A4 minus the 0.75-in left and right margins of the guidelines

# =====================================================================================
# Content
# =====================================================================================

ABSTRACT = (
    "Copying tables from relational databases into cloud object storage is routine, yet type mismatches such as "
    "fixed-precision decimals, time zones and JSON can silently corrupt data, and most tools only compare row counts. "
    "This project builds an AI Data Migration Tool that produces the schema mapping and the reconciliation checks that "
    "verify it as one pipeline. A mapper grounded in sample values and past migrations proposes the mapping, a dry run "
    "tests it before any data moves, and a reconciliation engine recomputes every check from the source and the "
    "written files. On a 36-table fault-injection corpus, row-count checks missed every rounded decimal and time-zone "
    "shift, while schema-aware checks caught all 122 faults with no false alarms. Sample grounding raised mapping F1 "
    "from 0.91 to 0.99; a local language model matched this with retrieval when code chose the checks. A streaming "
    "engine verified one million rows in 177 seconds.")

ACK = [
    f"I am grateful to my supervisor, {SUPERVISOR}, for suggesting the direction of this work, for regular and "
    "patient guidance, and for encouraging me to evaluate the tool honestly, including the places where the AI "
    "methods did not help. I thank the faculty and staff of the School of Artificial Intelligence and Data Science, "
    "IIT Jodhpur, for the courses and facilities that made this project possible, and my classmates for their "
    "feedback on the dashboard. I thank my family for their constant support.",
    "AI-based assistants were used to help with software development, drafting and language editing. The research "
    "design, experiments and results were reviewed and verified by me, and I take full responsibility for the "
    "content of this report.",
]

# Each entry: (kind, ...). Kinds: ch (chapter), sec, p, bullets, table, fig, eq, code, refs
B = []
ch = lambda n, t: B.append(("ch", n, t))
sec = lambda n, t: B.append(("sec", n, t))
p = lambda t: B.append(("p", t))
bullets = lambda *items: B.append(("bullets", items))

# ---------------------------------------------------------------------------- 1
ch("1", "Introduction and background")
sec("1.1", "Background")
p("Organisations that adopt a cloud data platform regularly copy tables out of operational relational databases into "
  "cloud object storage, usually as Apache Parquet [1] or CSV files, and increasingly as open table formats such as "
  "Delta Lake [2] and Apache Iceberg [3]. Copying the bytes is easy. Copying them faithfully is not, because the "
  "source and target type systems do not line up. A DECIMAL(18,2) currency column written as a floating-point number "
  "can lose paise; a TIMESTAMP WITH TIME ZONE column can quietly become a timezone-naive value that is 5 h 30 min "
  "wrong; a nullable BOOLEAN can be coerced to text; and a JSON column can be flattened and lose its nested fields.")
p("These errors share one dangerous property: they do not change the number of rows. Most migration and ELT "
  "pipelines verify a load by comparing row counts and the column list, so every one of the errors above is signed "
  "off as a successful migration. The damage surfaces weeks later as a report that does not add up.")
sec("1.2", "Motivation")
p("A migration has three steps: mapping each source column to a target representation, executing the copy, and "
  "reconciling the output against the source. The first and third steps are usually done separately, and the third "
  "is usually done superficially. The idea behind this project is that the reconciliation checks should be derived "
  "from the same understanding of the schema as the mapping. If the tool knows that a column is money, it should "
  "check that the money still adds up to the last paisa; if it knows that a column holds instants in time, it should "
  "compare them as UTC instants. Large language models (LLMs) are a natural candidate for the mapping step, because "
  "they can read column names and sample values the way a data engineer does, but they also make confident "
  "mistakes, so how far they can be trusted has to be measured, not assumed.")
sec("1.3", "Contributions")
p("The project makes the following contributions:")
bullets(
    "**A working tool.** An AI Data Migration Tool with connectors for PostgreSQL, MySQL/MariaDB, SQL Server, Azure "
    "SQL Database and SQLite; output to local folders, Amazon S3 and MinIO, Azure Data Lake Storage and Google Cloud "
    "Storage as Parquet, CSV, Delta Lake or Iceberg; a three-stage verification process; and a reconciliation engine "
    "that recomputes every value it reports.",
    "**A joint mapping-and-checking design.** Schema-aware checks are generated from the mapping itself, and an "
    "evaluation shows that letting an AI propose the mapping but letting code choose the checks gives the best result.",
    "**An evaluation methodology for migration.** A 36-table fault-injection corpus with gold mappings, difficulty "
    "tiers and a hallucination taxonomy adapted from the text-to-SQL and LLM-agent literature, run on three storage "
    "back ends.",
    "**Features for practical use.** A multi-user web dashboard with an AI review of the mapping, a privacy check "
    "with masking and a plain-language summary; and, for data engineers, a streaming engine, job files, incremental "
    "loads and exit codes for schedulers.",
)
sec("1.4", "Organisation of the report")
p("Chapter 2 reviews related work. Chapter 3 defines the problem and the objectives. Chapter 4 describes the design "
  "and implementation of the tool. Chapter 5 presents the evaluation and its results. Chapter 6 summarises the work "
  "and outlines future plans.")

# ---------------------------------------------------------------------------- 2
ch("2", "Literature survey")
sec("2.1", "Schema matching")
p("Schema matching, finding correspondences between the elements of two schemas, has a long history. Rahm and "
  "Bernstein [4] classify matchers by whether they use schema-level information (names, declared types, "
  "constraints) or instance-level data (value distributions), and observe that combining both is most robust. "
  "Migration to files is a constrained form of the problem: target names usually mirror the source, but the type "
  "of every column has to be chosen again in a different type system. The two rule-based reference mappers in this "
  "work correspond to schema-level matching (H1) and combined schema- and instance-level matching (H2).")
sec("2.2", "Text-to-SQL and schema linking")
p("Spider [5] established cross-domain text-to-SQL with 10,181 questions over 200 databases, split so that no "
  "schema appears in both the training and the test set. Models of the time failed mostly on choosing the right "
  "columns, which made schema linking the recognised bottleneck. Mapping a whole schema to files is a related "
  "grounding problem without a question to anchor it: the evidence has to come from the schema and its data. This "
  "project adapts Spider's idea of difficulty tiers from query complexity to type-conversion complexity.")
sec("2.3", "Hallucinations in schema-grounded generation")
p("Qu et al. [6] divide text-to-SQL hallucinations into schema-based errors (schema contradiction, attribute "
  "overanalysis, value misrepresentation) and logic-based errors (join redundancy, clause abuse, mathematical "
  "delusion), and show that schema-grounding errors matter most. Reframed for migration, these become Mapping "
  "Contradiction (a lossy target type such as DECIMAL to FLOAT), Type Overanalysis (fields with no source column), "
  "Null-Convention Misrepresentation (NULL rewritten as an empty string), Redundant Transformation (a "
  "value-changing cast), Trivial-Rule Abuse (a check that cannot fail) and Reconciliation Math Delusion "
  "(miscomputed aggregates).")
sec("2.4", "LLM agents, retrieval and verification")
p("AutoML-Agent [7] combines retrieval-augmented planning with request, execution and implementation verification "
  "in a multi-agent pipeline. Retrieval-augmented generation [8] supplies a model with relevant examples alongside "
  "the question. This project maps both ideas onto migration: the migration request is the request, the mapping "
  "and checks are the plan, the written files are the implementation, and mappings from earlier migrations are the "
  "retrieved examples.")
sec("2.5", "Lakehouse formats and data validation")
p("Delta Lake [2] and Apache Iceberg [3] add transactions, schema enforcement and table versions on top of Parquet "
  "files in object storage, and are now the usual target of new data platforms. They guarantee that a write is "
  "atomic and that a column has the declared type, but not that the values are the ones in the source. Commercial "
  "migration and ELT tools automate extraction and loading but typically stop at row counts.")
sec("2.6", "Research gap")
p("No work reviewed here produces schema-specific reconciliation checks together with the mapping, or measures "
  "how reliable LLM-generated mappings and checks are for migration. Table 2.1 summarises the positioning.")
B.append(("table", "2.1", "Positioning of this work relative to prior work",
          ["Prior work", "Artifact", "Key mechanism", "Adapted here as"],
          [["Schema matching [4]", "Correspondences", "Schema- and instance-level matching", "H1 and H2 mappers"],
           ["Spider [5]", "SQL query", "Schema linking, hardness tiers", "Difficulty tiers, mapping F1"],
           ["TA-SQL [6]", "SQL query", "Hallucination taxonomy", "Migration taxonomy"],
           ["AutoML-Agent [7]", "ML pipeline", "Retrieval, three-stage verification", "Verification stages"],
           ["ETL tools", "Data transfer", "Post-load row counts", "Generic baseline"],
           ["This work", "Mapping and checks", "Grounded mapping, recomputed checks", "-"]],
          [1.35, 1.15, 1.95, 1.55]))

# ---------------------------------------------------------------------------- 3
ch("3", "Problem definition and Objective")
sec("3.1", "Problem formulation")
p("The input is a source schema S = (T, C, K, Δ) listing tables T, typed columns C, keys K and a sample of rows Δ, "
  "together with a target specification τ that names the destination, the file or table format and the "
  "partitioning. The tool must produce a mapping μ that links every source column to a target field and defines its "
  "type, conversion and NULL handling; execute the migration according to μ; generate reconciliation checks ρ that "
  "fit the schema and the mapping; and report the result of applying ρ to the written output.")
p("A migration is correct when μ represents S faithfully and the report is both sound (it never reports a match "
  "that is not there) and complete (it misses no discrepancy). Soundness is measured by false alarms on clean runs "
  "and completeness by the share of injected faults that are detected, as defined in Section 5.2.")
sec("3.2", "Objectives")
bullets(
    "**O1.** Build a tool that migrates relational tables to cloud storage and proves, column by column, that nothing "
    "was lost or changed.",
    "**O2.** Generate the reconciliation checks from the mapping, and show that they catch value-level faults that "
    "row-count checks miss, without false alarms.",
    "**O3.** Measure how much sample values and past migrations improve mapping accuracy over declared types alone.",
    "**O4.** Measure how reliable LLM-generated mappings and checks are, and find a safe way to use them.",
    "**O5.** Make the tool usable by non-specialists (a dashboard for 100 concurrent users) and by data engineers "
    "(large tables, schedulers, lakehouse formats).",
)
sec("3.3", "Scope")
p("The project covers one-way migration of relational tables to object storage. Warehouse-to-warehouse migration, "
  "continuous change-data capture and fine-tuning of a dedicated mapping model are outside its scope and are "
  "discussed as future work.")

# ---------------------------------------------------------------------------- 4
ch("4", "Methodology")
sec("4.1", "Architecture")
p("The tool has five parts (Figure 4.1). The Connector Layer reads schema metadata and sample rows from the source "
  "and writes the output to the target. The Schema Mapper proposes μ. The Migration Executor converts and writes the "
  "rows in partitions. The Validation and Reconciliation Engine generates and runs ρ. An Agent Manager coordinates "
  "them through three verification stages, and an optional AI assistant reviews the mapping, looks for personal "
  "data and summarises the result.")
B.append(("fig", "4.1", "Architecture of the AI Data Migration Tool and its three verification stages",
          HERE / "fig_architecture.png", 5.75, None))
sec("4.2", "Connector layer")
p("Source connectors list tables, read column types, keys and foreign keys from each engine's catalogue, take a "
  "sample of rows and stream the full table (Table 4.1). Large tables are sampled with evenly spaced OFFSET queries "
  "instead of a full scan, and PostgreSQL [9] and MySQL use server-side cursors so that rows arrive in batches. "
  "Azure SQL Database is reached through the SQL Server driver with an encrypted connection, using a SQL login or "
  "Microsoft Entra ID. Target connectors expose one small key-value interface, so the executor and the "
  "reconciliation engine never depend on where the bytes live.")
B.append(("table", "4.1", "Supported sources, destinations and output formats",
          ["Layer", "Supported", "Notes"],
          [["Sources", "PostgreSQL, MySQL / MariaDB, SQL Server, Azure SQL Database, SQLite",
            "SQLite and SQL Server tested end to end"],
           ["Destinations", "Local folder, Amazon S3, MinIO, Azure Data Lake Storage Gen2, Google Cloud Storage",
            "MinIO tested as an S3-compatible store"],
           ["Formats", "Parquet, CSV, Delta Lake (with upsert), Apache Iceberg (catalog-registered)",
            "Partitioned by month, day or value"]],
          [1.1, 3.05, 1.85]))
sec("4.3", "Schema mapper and grounding")
p("The mapper draws on two sources of evidence. The first is the schema itself and its sample values, summarised "
  "into value-shape profiles such as \u201cdecimal values with at most two places\u201d or \u201cintegers taking only "
  "the values 0 and 1\u201d. Sample values matter because declared types mislead: a SMALLINT may hold yes/no flags "
  "and a TEXT column may hold JSON or ISO-8601 timestamps. The second source is a precedent store of mappings from "
  "earlier migrations, retrieved by TF-IDF similarity over column-name tokens, declared-type family and value shape, "
  "so that an amount column in a new table can follow a currency column seen before.")
p("Four mapping strategies are implemented. H1 maps from declared types alone. H2 adds sample profiling, a vote over "
  "the eight most similar precedents and execution verification; it is the default in the dashboard. S1 asks an "
  "LLM to map the table from column names and declared types only (zero-shot). S3 gives the LLM the sample "
  "profiles and the retrieved precedents (retrieval-augmented generation [8]) and verifies its answer with a dry run. "
  "The LLM back end is pluggable: the evaluation uses the open-weight Qwen2.5 7B model [10] served locally by "
  "Ollama [11], so no paid service or data transfer is needed; Anthropic's Claude models can be used through the "
  "official SDK.")
sec("4.4", "Three-stage verification")
p("Request verification checks that the source table, the format and the partition column exist before any work "
  "starts. Execution verification dry-runs μ on the sample rows Δ: it catches conversion errors, lossy target types "
  "and NULL rewriting, and sends failures back to the mapper for one bounded self-repair round. Implementation "
  "verification is the full reconciliation of the written output against the source (Section 4.5).")
sec("4.5", "Validation and reconciliation engine")
p("Every migration receives deterministic checks (row count, schema conformity, primary-key uniqueness and "
  "per-partition row counts) and typed checks generated from the mapping, one or two per column according to its "
  "target type (Table 4.2). For example, a column mapped to decimal(18,2) receives an exact-sum check and a value "
  "fingerprint, and a nullable column additionally receives a NULL-count check.")
B.append(("table", "4.2", "Checks generated from the mapping, by target type",
          ["Column mapped to", "Checks", "Silent error caught"],
          [["Exact decimal", "Exact sum, value fingerprint", "Rounding or truncated digits"],
           ["Timestamp", "Every instant compared in UTC", "Time-zone shifts"],
           ["JSON", "Canonical round-trip fingerprint", "Flattened or altered documents"],
           ["True/false", "Share of true values", "Flipped or lost flags"],
           ["Number or date", "Minimum and maximum", "Out-of-range or changed values"],
           ["Text", "Number of distinct values", "Lost or altered values"],
           ["Any nullable column", "NULL count", "NULL turned into 0 or empty text"]],
          [1.6, 2.35, 2.05]))
p("The engine never trusts a value it has not computed. Every source and target value is recomputed from the "
  "database and from the written files, under the same canonical form (decimals normalised, timestamps as UTC "
  "instants, JSON with sorted keys). An LLM may propose checks, but an LLM-asserted value is only compared against, "
  "never substituted for, the recomputed one, which removes Reconciliation Math Delusion by construction. The "
  "evaluation in Section 5.5 led to one further design decision: when an LLM produces the mapping, the checks are "
  "still generated from that mapping by code (the hybrid design), because the checks the LLM wrote itself were "
  "weaker.")
sec("4.6", "AI assistant")
p("Because the rule-based mapper is already accurate on familiar data, the LLM is used where it adds something rules "
  "cannot: judgement and explanation. Three assistant features run in the background on the local model:")
bullets(
    "**Mapping review.** After the rule-based mapping, the model reviews every column and lists only the columns it "
    "would change, with a one-sentence reason. Suggestions naming unknown columns or invalid types are discarded; "
    "the user accepts or keeps each one, and an accepted suggestion is dry-run again before use.",
    "**Privacy check and masking.** Rules find personal data from column names and value patterns (e-mail, phone, "
    "PAN, Aadhaar and card numbers), and the model adds columns whose meaning only shows in the values, such as "
    "phone numbers inside free-text notes. The user is asked whether to mask each column; masking is deterministic "
    "and irreversible (for example `r***@example.com` or `******3210`), and the checks compare masked source values "
    "with the masked output. This supports compliance with India's Digital Personal Data Protection Act [12].",
    "**Migration summary.** The model writes a short plain-language summary from the reconciliation facts. Every "
    "number in it is checked against the report, and if one does not match, a template summary is shown instead.",
)
p("Asking the model to list only changes reduced its answer from 181 to 59 tokens, and keeping the model loaded "
  "between requests removed a 43-second reload; a review now takes about 20 to 40 seconds on a four-core laptop CPU.")
sec("4.7", "Streaming engine and job files")
p("The first executor and reconciliation engine hold a whole table in memory, which suits the evaluation corpus but "
  "not production tables. A second, streaming engine (Figure 4.2) reads the source once in chunks of 50,000 rows. "
  "Each row is converted and written and, in the same pass, folded into a source-side summary; the output is then "
  "read back file by file into a target-side summary. Summaries hold counts, exact decimal sums, minimum and maximum, "
  "and 64-bit value hashes. Value fingerprints are order-independent multiset hashes, eqn. (4.1), so they can be "
  "computed without sorting or storing the values:")
B.append(("eq", "F(c) = ( \u03a3 h(v) ) mod 2^64,   over all values v of column c", "(4.1)"))
p("where h is a 64-bit BLAKE2b hash of the canonical value. Each partition gets its own row fingerprint, so a "
  "failure is reported as, for example, \u201cupdated_at=2023-05 differs\u201d, and only that partition needs to be "
  "rewritten. As independent evidence that extraction itself lost nothing, the source database also recomputes "
  "COUNT(*), NULL counts and integer and decimal SUM, MIN and MAX with SQL, compared with the output.")
B.append(("fig", "4.2", "Single-pass streaming migration and verification",
          HERE / "fig_streaming.png", 6.0, None))
p("A job file in YAML describes a whole migration: source, target, format (Parquet, CSV, Delta Lake or Iceberg), "
  "tables, masks, overrides and incremental columns. Tables are processed parents first, following foreign keys. "
  "Incremental runs copy only rows above a saved watermark and advance it only after every check passes; Delta "
  "tables support upserts that replace changed rows by primary key. The command line returns exit code 0 when all "
  "tables pass, 1 when a check fails and 2 when the job cannot run, so schedulers such as Airflow or Azure Data "
  "Factory can stop downstream work on untrusted data. Appendix B shows an example.")
sec("4.8", "Dashboard and multi-user design")
p("A browser dashboard guides non-specialists through three steps: choose a database (their own, through a form "
  "for each engine, or one of six sample databases), plan the mapping, and choose a destination and migrate (Figure "
  "4.3). Misleading declared types are highlighted, every check is explained in plain language, and a \u201cTest the "
  "checks\u201d page damages a sample migration on purpose to show which checks notice (Figure 5.1). For shared use, "
  "heavy work runs as background jobs on a pool of worker processes, each visitor has a private session and job "
  "folder, results expire after 30 minutes, and a public mode allows only the sample databases.")
B.append(("fig", "4.3", "Dashboard after planning: the mapping, the generated checks, the AI review and the privacy check",
          HERE / "fig_dashboard_plan_full.png", 5.4, (0.515, 0.075)))
sec("4.9", "Implementation and testing")
p("The tool is written in Python with Apache Arrow [13] for file input and output, deltalake and pyiceberg for the "
  "lakehouse formats, and a standard-library web server for the dashboard. The code base has about 4,250 lines in "
  "the tool package and 6,270 lines including the evaluation harness, tools and 50 automated tests. The tests cover "
  "the pipeline, every output format, masking, the AI assistant (with a stand-in model), the job runner, connection "
  "strings for SQL Server and Azure SQL, and a live dashboard server.")

# ---------------------------------------------------------------------------- 5
ch("5", "Experimental findings")
sec("5.1", "Corpus and fault injection")
p("The corpus has six synthetic schemas (e-commerce, healthcare claims, HR and payroll, logistics, subscription "
  "billing and support ticketing) with 36 tables of 300 rows each. Columns exercise known sources of silent loss: "
  "DECIMAL currency, TIMESTAMP WITH TIME ZONE, nullable BOOLEAN, ENUM and JSON. Deceptive columns hide their meaning "
  "behind the declared type, and two trap columns are genuine counts that look like flags in a small sample. Tables "
  "fall into four difficulty tiers (Table 5.1), and each has a hand-written gold mapping. The corpus is generated "
  "deterministically and loaded into SQLite and SQL Server 2025 with engine-idiomatic types and identical rows.")
B.append(("table", "5.1", "Difficulty tiers of the 36-table corpus",
          ["Tier", "Criteria", "Tables", "Example"],
          [["Easy", "Primitive types only", "9", "Order items: integers and text"],
           ["Medium", "One edge case", "11", "Products with a DECIMAL(18,2) price"],
           ["Hard", "Two or more edge cases, or partitioned", "10", "Claims with ENUM status, nullable BOOLEAN"],
           ["Extra Hard", "Three or more edge cases including JSON", "6", "Tickets with JSON, time zones, 0/1 flags"]],
          [0.95, 2.05, 0.65, 2.35]))
p("Every table is migrated with its gold mapping, and from that output one clean copy and one copy per applicable "
  "fault are produced: a truncated decimal, a dropped row containing a NULL, a duplicated row, a time-zone shift in "
  "one file, an off-by-one partition boundary and a silently flattened JSON field. This gives 122 injected and 36 "
  "clean runs.")
sec("5.2", "Metrics")
p("A run counts as a detection when at least one check fails. Reconciliation recall R, eqn. (5.1), is the share of "
  "injected runs detected, and an alarm on a clean run is a false positive. Mapping Accuracy (MA) is the F1 score of "
  "the generated column mappings against the gold mappings, eqn. (5.2), where a column is correct when its target "
  "name, NULL handling and type match.")
B.append(("eq", "R = (injected runs detected) / (injected runs)", "(5.1)"))
B.append(("eq", "MA = 2 \u00b7 P \u00b7 Rc / (P + Rc)", "(5.2)"))
p("Here P is the share of generated column mappings that are correct and Rc the share of gold columns mapped "
  "correctly. Migration Validity Rate (MVR) is the share of migrations that run without error, and each "
  "hallucination category of Section 2.3 is detected automatically by comparison with the gold mapping.")
sec("5.3", "Reconciliation check sets")
p("Three check sets were run on the 158 runs (Table 5.2): generic checks (row count and schema, the level of most "
  "ETL tools), deterministic checks (adding primary-key uniqueness and per-partition counts) and the schema-aware "
  "checks generated from the mapping. The results were identical on SQLite, SQL Server and SQL Server with MinIO as "
  "the target.")
B.append(("table", "5.2", "Detection rate of three check sets on the injected faults",
          ["Fault (runs)", "Generic", "+ keys and partitions", "Schema-aware"],
          [["Truncated decimal (18)", "0.00", "0.00", "1.00"],
           ["Dropped-null row (10)", "1.00", "1.00", "1.00"],
           ["Duplicated row (36)", "1.00", "1.00", "1.00"],
           ["Time-zone shift (16)", "0.00", "0.00", "1.00"],
           ["Off-by-one partition (36)", "0.64", "1.00", "1.00"],
           ["Flattened JSON (6)", "1.00", "1.00", "1.00"],
           ["Overall recall (122)", "0.61", "0.72", "1.00"],
           ["False alarms (36 clean runs)", "0", "0", "0"],
           ["Alarm names the column", "0.00", "0.93", "1.00"]],
          [2.3, 1.0, 1.65, 1.05]))
p("Generic checks never detected a truncated decimal or a time-zone shift, because neither changes the row count or "
  "the schema, and they missed 36% of partition faults, where a row moves between partitions while the total stays "
  "correct. Partition counts fix that case but still miss every value-level fault. The schema-aware checks detected "
  "all 122 faults with no false alarm, and every alarm named the affected column. Figure 5.1 shows the same result "
  "live in the dashboard.")
B.append(("fig", "5.1", "\u201cTest the checks\u201d page: a time-zone shift passes the generic checks and is caught by the schema-aware timestamp check",
          HERE / "fig_dashboard_break_full.png", 5.4, (0.10, 0.395)))
sec("5.4", "Mapping accuracy on three back ends")
p("Table 5.3 gives the Mapping Accuracy of the two rule-based mappers by difficulty tier. Every migration ran without "
  "error (MVR 1.00). The checks generated from H1's mappings detected 96% of the faults on SQLite and 98% on SQL "
  "Server, and those from H2's mappings 100% on all back ends.")
B.append(("table", "5.3", "Mapping Accuracy (F1) of the rule-based mappers by tier",
          ["Mapper, back end", "Easy", "Medium", "Hard", "Extra Hard", "All"],
          [["H1, SQLite", "1.00", "0.96", "0.88", "0.81", "0.91"],
           ["H2, SQLite", "1.00", "1.00", "0.98", "0.97", "0.99"],
           ["H1, SQL Server", "1.00", "0.96", "0.92", "0.73", "0.91"],
           ["H2, SQL Server", "1.00", "1.00", "0.98", "0.97", "0.99"],
           ["H1, SQL Server to MinIO", "1.00", "0.96", "0.92", "0.73", "0.91"],
           ["H2, SQL Server to MinIO", "1.00", "1.00", "0.98", "0.97", "0.99"]],
          [2.2, 0.65, 0.8, 0.65, 1.0, 0.7]))
p("H1 mis-maps the deceptive columns: 0/1 flags stay integers, unconstrained NUMERIC currency becomes a float on "
  "SQLite, and timestamps or JSON stored as text stay strings, a Mapping Contradiction in 31% of tables. Its Extra "
  "Hard accuracy is lower on SQL Server (0.73 against 0.81) because SQL Server has no JSON type, so JSON columns are "
  "declared NVARCHAR(MAX) and only their values reveal them. H2 corrects all of these; its only errors are the two "
  "trap columns. Writing to MinIO gave exactly the same results as writing to a local folder, confirming that the "
  "checking logic does not depend on where the files are stored.")
sec("5.5", "LLM strategies and the hybrid design")
p("The LLM strategies were run on all 36 tables with Qwen2.5 7B served locally (Table 5.4). The two hybrid rows reuse "
  "the same model answers but generate the checks from the model's mapping with code.")
B.append(("table", "5.4", "LLM mapping strategies on SQLite (Qwen2.5 7B, local)",
          ["Strategy", "MVR", "MA (F1)", "Faults detected", "False alarms", "s / table"],
          [["H2 rules (reference)", "1.00", "0.99", "100%", "0 / 36", "0.3"],
           ["S1 zero-shot", "0.83", "0.67", "72%", "0 / 36", "137"],
           ["S1 + verification", "0.86", "0.68", "72%", "0 / 36", "137"],
           ["S1 mapping + code checks", "0.86", "0.68", "100%", "0 / 36", "137"],
           ["S3 retrieval + verification", "0.97", "0.99", "86%", "13 / 36", "228"],
           ["S3 mapping + code checks", "0.97", "0.99", "100%", "0 / 36", "228"]],
          [2.15, 0.6, 0.75, 1.0, 0.85, 0.65]))
p("Three findings stand out. First, context is decisive: asked cold, the model mapped only 67% of columns correctly "
  "and misrepresented NULL handling on every table, while with sample profiles and precedents it reached 0.99, equal "
  "to the best rule-based mapper, including 1.00 on the Extra Hard tier. Second, the checks the model wrote itself "
  "were weak: S3 detected only 67% of truncated decimals and 31% of time-zone shifts, and raised false alarms on 13 "
  "of 36 clean runs. Tracing each false alarm showed two causes: ten checks named no column, and five declared a "
  "repeated foreign key, such as order_id in order_items, unique. Third, separating the roles fixes this: with the "
  "model's mapping and code-generated checks, detection rose to 100% with no false alarms. Execution verification "
  "added little (S1 validity 0.83 to 0.86), and no Reconciliation Math Delusion occurred, because the model never "
  "computes the numbers. On this corpus the LLM did not beat the rules; its value lies in reasoning about "
  "unfamiliar schemas without new rules and in explanation, which is how the assistant features use it.")
sec("5.6", "Scalability and performance")
p("To check that the streaming engine detects exactly what the original engine detects, all 158 corpus runs were "
  "verified by both: all 158 verdicts agreed. Table 5.5 compares time and peak memory on synthetic order tables "
  "with every check and the database cross-check enabled.")
B.append(("table", "5.5", "Time and peak memory on large tables (4-core laptop, Parquet output by month)",
          ["Run", "Rows", "Time", "Peak memory", "Result"],
          [["In-memory engine", "200,000", "20 s", "424 MB", "Pass"],
           ["Streaming engine", "200,000", "26 s", "287 MB", "Pass"],
           ["Streaming engine", "1,000,000", "177 s", "471 MB", "Pass, database confirmed 6/6"]],
          [1.6, 0.95, 0.75, 1.1, 1.6]))
p("Both engines run at a similar speed of about 5,000 to 10,000 rows per second; the difference is memory, which "
  "grows with every row in the in-memory engine and stays bounded in the streaming engine apart from the "
  "per-distinct-value hashes of key and distinct-count checks. In an end-to-end job test, changing one amount by "
  "0.01 in one file was reported as a failure of exactly one month's partition, and re-running only that partition "
  "rewrote 860 rows and restored a passing verification. A Delta Lake upsert of 300 new and about 200 changed orders "
  "produced 20,900 rows with no duplicate keys, matching the source.")
p("For the multi-user dashboard, a load test simulated users who each opened the page, planned, migrated, downloaded "
  "the result and ran a fault-injection test. With 100 users arriving over 60 seconds, all 100 finished with no "
  "errors; the median job took 0.6 s to plan and 1.2 s to migrate. With all 100 users starting in the same second, "
  "all finished with no errors and a median migration time of 11 s. Throughput was 260 to 350 jobs per minute on "
  "the same laptop.")
sec("5.7", "Discussion")
p("Two conclusions follow. First, reconciliation must be schema-specific: the faults a generic check can never catch "
  "are exactly the value-level faults that matter to a data owner, and schema-aware checks catch them without false "
  "alarms. Second, correct type mapping needs evidence from sample values and precedents, not only declared types, "
  "and this holds for rules and LLMs alike; the safest use of an LLM is to propose the mapping while code generates "
  "and computes the checks. Sample-based inference has its own failure mode: a count column that takes only the "
  "values 0 and 1 in a 20-row sample is indistinguishable from a flag, which a larger sample or a clarifying "
  "question could resolve.")
sec("5.8", "Threats to validity")
p("The corpus is synthetic and has 36 tables, so the results are a case study rather than a general claim. H2 was "
  "designed by the author of the corpus and is therefore an optimistic reference. The LLM results use one 7-billion-"
  "parameter model; larger models may behave differently. PostgreSQL, MySQL, cloud object stores other than MinIO "
  "and the Glue, REST and Hive catalogs are implemented but were not tested in this environment.")

# ---------------------------------------------------------------------------- 6
ch("6", "Summary and Future plan of work")
sec("6.1", "Summary")
p("This project built an AI Data Migration Tool that generates a schema mapping and the schema-specific checks that "
  "verify it as one pipeline, recomputes every check deterministically, and is usable both through a multi-user "
  "dashboard and as a scheduled job. On a 36-table fault-injection corpus run on three back ends, row-count checks "
  "missed every rounded decimal and time-zone shift, while schema-aware checks caught all 122 faults with no false "
  "alarms. Grounding the mapper in sample values raised mapping F1 from 0.91 to 0.99. A local LLM reached the same "
  "accuracy with retrieval, but only the hybrid design, in which code generates the checks from the model's "
  "mapping, also reached full detection without false alarms. All five objectives of Section 3.2 were met.")
sec("6.2", "Future plan of work")
bullets(
    "Fine-tune a dedicated mapping model with LoRA [14] (strategy S2) on training data exported from held-out "
    "schemas, and compare it with S3.",
    "Extend the corpus with real anonymised schemas and evaluate PostgreSQL, MySQL and managed cloud destinations, "
    "including Delta Lake and Iceberg on cloud storage.",
    "Add an Oracle connector, warehouse-to-warehouse migration and continuous change-data capture.",
    "Speed up the streaming engine by vectorising conversion and canonicalisation with Arrow compute.",
    "Run a usability study of the dashboard with data engineers, and deploy the shared dashboard on a cloud server.",
)

PUBLICATIONS = [
    f"H. Gupta and A. Pathak, \u201cAI-assisted schema mapping and reconciliation for relational-to-cloud-storage data "
    "migration: A joint-pipeline evaluation,\u201d manuscript prepared in IEEE conference format, 2026."]

APPENDIX = [
    ("sec", "A", "Reproducing the results"),
    ("p", "All commands run in the ai-data-migration-tool folder. The corpus is generated deterministically, so the "
          "numbers in Chapter 5 are reproduced exactly; LLM answers are cached."),
    ("code", ["# H1, H2 and the check-set study (SQLite, then SQL Server)",
              "python -m eval.run_eval",
              "python -m eval.run_eval --mssql mssql://localhost/amt_corpus?instance=SQLEXPRESS",
              "# LLM strategies with the local model",
              "python -m eval.run_eval --provider ollama --strategies h2,s1,s1+code,s3,s3+code",
              "# the 50 automated tests, and the dashboard (http://localhost:8765)",
              "python -m unittest discover -s tests",
              "python -m amt ui"]),
    ("sec", "B", "Example job file"),
    ("p", "A job that copies two tables to Parquet, masks e-mail addresses and loads orders incrementally. It runs "
          "with python -m amt run shop.yaml."),
    ("code", ["name: shop-nightly",
              "source: sqlite:///shop.db",
              "target: C:/lake",
              "format: parquet         # parquet | csv | delta | iceberg",
              "tables:",
              "  - name: customers",
              "    mask: [email]",
              "  - name: orders",
              "    partition_by: updated_at",
              "    incremental: {column: updated_at, mode: append}"]),
]

REFERENCES = [
    "The Apache Software Foundation, \u201cApache Parquet documentation.\u201d [Online]. Available: https://parquet.apache.org/docs",
    "M. Armbrust et al., \u201cDelta Lake: High-performance ACID table storage over cloud object stores,\u201d Proc. VLDB "
    "Endowment, vol. 13, no. 12, pp. 3411\u20133424, 2020.",
    "The Apache Software Foundation, \u201cApache Iceberg documentation.\u201d [Online]. Available: https://iceberg.apache.org/docs/latest",
    "E. Rahm and P. A. Bernstein, \u201cA survey of approaches to automatic schema matching,\u201d VLDB J., vol. 10, no. 4, "
    "pp. 334\u2013350, 2001.",
    "T. Yu et al., \u201cSpider: A large-scale human-labeled dataset for complex and cross-domain semantic parsing and "
    "text-to-SQL task,\u201d in Proc. Conf. Empirical Methods in Natural Language Processing (EMNLP), 2018, pp. 3911\u20133921.",
    "G. Qu, J. Li, B. Li, B. Qin, N. Huo, C. Ma, and R. Cheng, \u201cBefore generation, align it! A novel and effective "
    "strategy for mitigating hallucinations in text-to-SQL generation,\u201d in Findings of the Association for "
    "Computational Linguistics: ACL 2024, 2024, pp. 5456\u20135471.",
    "P. Trirat, W. Jeong, and S. J. Hwang, \u201cAutoML-Agent: A multi-agent LLM framework for full-pipeline AutoML,\u201d "
    "in Proc. 42nd Int. Conf. Machine Learning (ICML), PMLR vol. 267, 2025.",
    "P. Lewis et al., \u201cRetrieval-augmented generation for knowledge-intensive NLP tasks,\u201d in Advances in Neural "
    "Information Processing Systems (NeurIPS), vol. 33, 2020, pp. 9459\u20139474.",
    "The PostgreSQL Global Development Group, \u201cPostgreSQL documentation.\u201d [Online]. Available: https://www.postgresql.org/docs",
    "Qwen Team, \u201cQwen2.5 technical report,\u201d arXiv:2412.15115, 2024.",
    "Ollama, \u201cOllama: Run large language models locally.\u201d [Online]. Available: https://ollama.com",
    "Government of India, \u201cThe Digital Personal Data Protection Act, 2023 (No. 22 of 2023),\u201d The Gazette of India, 2023.",
    "The Apache Software Foundation, \u201cApache Arrow documentation.\u201d [Online]. Available: https://arrow.apache.org/docs",
    "E. J. Hu et al., \u201cLoRA: Low-rank adaptation of large language models,\u201d in Proc. Int. Conf. Learning "
    "Representations (ICLR), 2022.",
]

# =====================================================================================
# Template surgery
# =====================================================================================

words = len(ABSTRACT.split())
assert words <= 150, f"abstract has {words} words (max 150)"

d = docx.Document(TEMPLATE)
body = d.element.body
kids = list(body.iterchildren())
sect_paras = [e for e in kids if e.tag == qn("w:p") and e.find(qn("w:pPr")) is not None
              and e.find(qn("w:pPr")).find(qn("w:sectPr")) is not None]
cover_end, front_end = sect_paras[0], sect_paras[1]
final_sect = kids[-1]
P = lambda el: docx.text.paragraph.Paragraph(el, d)


def set_text(par, text):
    for r in list(par.runs)[1:]:
        r._r.getparent().remove(r._r)
    par.runs[0].text = text


# ---- page layout from the institute guidelines (Annexure I): margins 1 / 1 / 0.75 / 0.75 in
for s in d.sections:
    s.top_margin = s.bottom_margin = Inches(1)
    s.left_margin = s.right_margin = Inches(0.75)

# ---- cover page (keeps the template's layout and logo)
set_text(P(kids[0]), TITLE)
set_text(P(kids[5]), f"{STUDENT} ({ROLL})")
set_text(P(kids[8]), DEGREE)

# ---- drop the template's sample front matter and main text
i0, i1 = kids.index(cover_end), kids.index(front_end)
for e in kids[i0 + 1:i1]:
    body.remove(e)
for e in kids[i1 + 1:-1]:
    body.remove(e)

INSERT = {"before": front_end}          # where new paragraphs go (front matter first, then main)


def place(el):
    INSERT["before"].addprevious(el)
    return el


def new_par():
    par = d.add_paragraph()          # python-docx appends before the final sectPr ...
    place(par._p)                    # ... then we move it where it belongs
    return par


def pf(par, after=200, before=0, line=360, align=WD_ALIGN_PARAGRAPH.JUSTIFY, keep=False):
    f = par.paragraph_format
    f.space_after, f.space_before = Pt(after / 20), Pt(before / 20)
    f.line_spacing = line / 240
    f.alignment = align
    f.keep_with_next = keep
    return par


MARK = re.compile(r"(\*\*.+?\*\*|\*[^*]+?\*|`[^`]+?`)")


def runs(par, text, size=None, bold=None):
    for part in MARK.split(text):
        if not part:
            continue
        if part.startswith("**"):
            r = par.add_run(part[2:-2])
            r.bold = True
        elif part.startswith("*"):
            r = par.add_run(part[1:-1])
            r.italic = True
        elif part.startswith("`"):
            r = par.add_run(part[1:-1])
            r.font.name = "Courier New"
            r.font.size = Pt(10.5)
        else:
            r = par.add_run(part)
            if bold:
                r.bold = True
        if size:
            r.font.size = Pt(size)
    return par


def para(text, **kw):
    return runs(pf(new_par(), **kw), text)


def page_break_before(par):
    par.paragraph_format.page_break_before = True


def heading1(text, page_break=True, after=300, before=0, align=WD_ALIGN_PARAGRAPH.LEFT):
    """Section heading per the guidelines: bold, 14 pt (numbered sections are written in capitals)."""
    par = new_par()
    par.style = d.styles["Heading 1"]
    if page_break:
        page_break_before(par)
    par.paragraph_format.space_after = Pt(after / 20)
    par.paragraph_format.space_before = Pt(before / 20)
    par.paragraph_format.keep_with_next = True
    par.alignment = align
    r = par.add_run(text)
    r.bold, r.font.size = True, Pt(14)
    return par


def heading2(text):
    """Sub-section heading per the guidelines: italic, e.g. '4.2 Experimental validations'."""
    par = new_par()
    par.style = d.styles["Heading 2"]
    pf(par, after=100, before=200, line=276, align=WD_ALIGN_PARAGRAPH.LEFT, keep=True)
    r = par.add_run(text)
    r.italic, r.bold, r.font.size = True, False, Pt(12)
    r.font.color.rgb = docx.shared.RGBColor(0, 0, 0)
    return par


def plain_title(text, size=14, page_break=True, after=400):
    """Front-matter title as in the guidelines' sample pages: bold, right-aligned, kept out of the outline."""
    par = new_par()
    if page_break:
        page_break_before(par)
    par.paragraph_format.space_after = Pt(after / 20)
    par.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    r = par.add_run(text)
    r.bold, r.font.size = True, Pt(size)
    return par


def bullet_list(items):
    for it in items:
        par = pf(new_par(), after=120)
        par.paragraph_format.left_indent = Inches(0.3)
        par.paragraph_format.first_line_indent = Inches(-0.2)
        runs(par, "\u2022\u00a0\u00a0" + it)


def caption(text):
    par = pf(new_par(), after=240, before=80, line=276, align=WD_ALIGN_PARAGRAPH.CENTER)
    r = par.add_run(text)
    r.font.size, r.italic = Pt(11), True
    return par


def cell_borders(cell, top, bottom):
    tcPr = cell._tc.get_or_add_tcPr()
    b = OxmlElement("w:tcBorders")
    for side, on in (("top", top), ("left", False), ("bottom", bottom), ("right", False)):
        e = OxmlElement(f"w:{side}")
        e.set(qn("w:val"), "single" if on else "none")
        e.set(qn("w:sz"), "8" if on else "0")
        e.set(qn("w:space"), "0")
        e.set(qn("w:color"), "000000" if on else "FFFFFF")
        b.append(e)
    tcPr.append(b)
    mar = OxmlElement("w:tcMar")
    for side, w in (("top", 50), ("left", 90), ("bottom", 50), ("right", 90)):
        e = OxmlElement(f"w:{side}")
        e.set(qn("w:w"), str(w))
        e.set(qn("w:type"), "dxa")
        mar.append(e)
    tcPr.append(mar)


def table(num, title, header, rows, widths):
    cap = caption(f"Table {num}: {title}")
    cap.paragraph_format.keep_with_next = True
    cap.paragraph_format.space_after = Pt(4)
    t = d.add_table(rows=1 + len(rows), cols=len(header))
    place(t._tbl)
    tblPr = t._tbl.tblPr
    jc = OxmlElement("w:jc")
    jc.set(qn("w:val"), "center")
    tblPr.append(jc)
    layout = OxmlElement("w:tblLayout")
    layout.set(qn("w:type"), "fixed")
    tblPr.append(layout)
    for gc, w in zip(t._tbl.tblGrid.findall(qn("w:gridCol")), widths):
        gc.set(qn("w:w"), str(int(w * 1440)))
    last = len(rows)
    for ri, row in enumerate([header] + rows):
        tr = t.rows[ri]
        tr._tr.get_or_add_trPr().append(OxmlElement("w:cantSplit"))
        if ri == 0:
            tr._tr.get_or_add_trPr().append(OxmlElement("w:tblHeader"))
        for ci, val in enumerate(row):
            c = tr.cells[ci]
            c.width = Inches(widths[ci])
            cell_borders(c, top=ri == 0, bottom=ri in (0, last))
            par = c.paragraphs[0]
            short = ci > 0 and len(val) <= 12
            pf(par, after=0, line=240, align=WD_ALIGN_PARAGRAPH.CENTER if short or ri == 0 and ci > 0
               else WD_ALIGN_PARAGRAPH.LEFT, keep=ri < last)     # keep the whole table on one page
            runs(par, val, size=10.5, bold=ri == 0)
    spacer = pf(new_par(), after=120, line=240)
    spacer.paragraph_format.space_before = Pt(0)


def figure(num, title, path, width, crop):
    par = pf(new_par(), after=0, before=120, line=240, align=WD_ALIGN_PARAGRAPH.CENTER, keep=True)
    shape = par.add_run().add_picture(str(path), width=Inches(width))
    if crop:
        top, bottom = crop
        bf = shape._inline.graphic.graphicData.pic.blipFill
        src = OxmlElement("a:srcRect")
        src.set("t", str(int(top * 100000)))
        src.set("b", str(int(bottom * 100000)))
        bf.insert(1, src)
        shape.height = int(shape.height * (1 - top - bottom))
    caption(f"Figure {num}: {title}")


def equation(expr, num):
    par = pf(new_par(), after=160, before=80, line=276, align=WD_ALIGN_PARAGRAPH.LEFT)
    tabs = par.paragraph_format.tab_stops
    tabs.add_tab_stop(docx.shared.Twips(TEXT_W // 2), docx.enum.text.WD_TAB_ALIGNMENT.CENTER)
    tabs.add_tab_stop(docx.shared.Twips(TEXT_W), docx.enum.text.WD_TAB_ALIGNMENT.RIGHT)
    r = par.add_run("\t" + expr)
    r.italic = True
    par.add_run("\t" + num)


def code_block(lines):
    for k, line in enumerate(lines):
        par = pf(new_par(), after=120 if k == len(lines) - 1 else 0, line=240, align=WD_ALIGN_PARAGRAPH.LEFT)
        par.paragraph_format.left_indent = Inches(0.25)
        r = par.add_run(line)
        r.font.name, r.font.size = "Courier New", Pt(8.5)


def toc_line(text, page, level=1, bold=False):
    par = new_par()
    pPr = par._p.get_or_add_pPr()
    tabs = OxmlElement("w:tabs")
    tab = OxmlElement("w:tab")
    tab.set(qn("w:val"), "right")
    tab.set(qn("w:leader"), "dot")
    tab.set(qn("w:pos"), str(TEXT_W))
    tabs.append(tab)
    pPr.insert(0, tabs)
    par.paragraph_format.space_after = Pt(2 if level == 1 else 0)
    par.paragraph_format.space_before = Pt(3 if level == 1 and bold else 0)
    if level == 2:
        par.paragraph_format.left_indent = Inches(0.35)
    r = par.add_run(text)
    r.bold = bold
    r2 = par.add_run("\t" + str(page))
    r2.bold = bold


def pg(key):
    return PAGES.get(key, "")


# ---- front matter
RIGHT = WD_ALIGN_PARAGRAPH.RIGHT
heading1("Declaration", page_break=False, after=400, align=RIGHT)
para(f"I hereby declare that the work presented in this Project Report titled **{TITLE}** submitted to the Indian "
     f"Institute of Technology Jodhpur in partial fulfilment of the requirements for the award of the degree of "
     f"{DEGREE}, is a bonafide record of the research work carried out under the supervision of {SUPERVISOR}. The "
     "contents of this Project Report in full or in parts, have not been submitted to, and will not be submitted by "
     "me to, any other Institute or University in India or abroad for the award of any degree or diploma.")
for k, t in enumerate(["Signature", STUDENT, ROLL]):
    par = para(("**%s**" % t) if k == 0 else t, align=WD_ALIGN_PARAGRAPH.RIGHT, after=0, before=600 if k == 0 else 0)

heading1("Certificate", after=400, align=RIGHT)
para(f"This is to certify that the Project Report titled **{TITLE}**, submitted by {STUDENT} ({ROLL}) to the Indian "
     f"Institute of Technology Jodhpur for the award of the degree of {DEGREE}, is a bonafide record of the research "
     "work done by the student under my supervision. To the best of my knowledge, the contents of this report, in "
     "full or in parts, have not been submitted to any other Institute or University for the award of any degree or "
     "diploma.")
for k, t in enumerate(["Signature", SUPERVISOR]):
    para(("**%s**" % t) if k == 0 else t, align=WD_ALIGN_PARAGRAPH.RIGHT, after=0, before=600 if k == 0 else 0)

heading1("Acknowledgements", after=400, align=RIGHT)
for t in ACK:
    para(t)

heading1("Abstract", after=400, align=RIGHT)
para(ABSTRACT)

plain_title("Contents", after=120)
hdr = pf(new_par(), after=60, line=240, align=RIGHT)
r = hdr.add_run("page")
r.italic = True
toc_line("Acknowledgements", pg("Acknowledgements"), bold=True)
toc_line("Abstract", pg("Abstract"), bold=True)
for kind, *rest in B:
    if kind == "ch":
        toc_line(f"{rest[0]}.  {rest[1]}", pg("ch:" + rest[0]), bold=True)
    elif kind == "sec":
        toc_line(f"{rest[0]}   {rest[1]}", pg("sec:" + rest[0]), level=2)
toc_line("Publications", pg("Publications"), bold=True)
toc_line("References", pg("References"), bold=True)
toc_line("Appendix", pg("Appendix"), bold=True)

plain_title("List of Figures")
for kind, *rest in B:
    if kind == "fig":
        toc_line(f"{rest[0]}   {rest[1]}", pg("fig:" + rest[0]))
plain_title("List of Tables", page_break=False, after=300)
for kind, *rest in B:
    if kind == "table":
        toc_line(f"{rest[0]}   {rest[1]}", pg("tab:" + rest[0]))

# ---- main text (after the front-matter section break)
INSERT["before"] = final_sect
first = True
for kind, *rest in B:
    if kind == "ch":
        # sections run on continuously (the guidelines expect about 10 pages, not one chapter per page)
        heading1(f"{rest[0]}. {rest[1].upper()}", page_break=False, before=0 if first else 360, after=160)
        first = False
    elif kind == "sec":
        heading2(f"{rest[0]}   {rest[1]}")
    elif kind == "p":
        para(rest[0])
    elif kind == "bullets":
        bullet_list(rest[0])
    elif kind == "table":
        table(*rest)
    elif kind == "fig":
        figure(*rest)
    elif kind == "eq":
        equation(*rest)

heading1("Publications", page_break=False, before=360, after=160)
for t in PUBLICATIONS:
    para(t)
heading1("References", after=160)
for k, ref in enumerate(REFERENCES, 1):
    par = pf(new_par(), after=100, line=300, align=WD_ALIGN_PARAGRAPH.JUSTIFY)
    par.paragraph_format.left_indent = Inches(0.375)
    par.paragraph_format.first_line_indent = Inches(-0.375)
    par.add_run(f"[{k}]\t{ref}")
    par.paragraph_format.tab_stops.add_tab_stop(Inches(0.375))
heading1("Appendix", after=160)
for kind, *rest in APPENDIX:
    if kind == "sec":
        heading2(f"{rest[0]}   {rest[1]}")
    elif kind == "p":
        para(rest[0])
    elif kind == "code":
        code_block(rest[0])

d.core_properties.title = TITLE
d.core_properties.author = STUDENT
d.save(OUT)
print("saved", OUT, f"(abstract {words} words)")
