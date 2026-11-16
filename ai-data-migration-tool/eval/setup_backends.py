"""Load the evaluation corpus into a real PostgreSQL server and prepare a
MinIO (S3-compatible) bucket, so experiments run against the real systems.

Connection details come from environment variables, never from code:

    AMT_PG_DSN             postgresql://postgres:<password>@localhost:5432/amt_corpus
    AMT_MSSQL_URI          mssql://localhost/amt_corpus?instance=SQLEXPRESS   (Windows login)
    AWS_ENDPOINT_URL       http://localhost:9000            (MinIO)
    AWS_ACCESS_KEY_ID      MinIO user
    AWS_SECRET_ACCESS_KEY  MinIO password

    python -m eval.setup_backends --check      # test both connections only
    python -m eval.setup_backends              # create database, load 36 tables, create bucket
    python -m eval.setup_backends --sqlserver --skip-postgres --skip-minio   # SQL Server only

Only databases whose name starts with "amt_" are ever created or emptied, so
existing databases on the same server are never touched.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse, urlunparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.connectors import SQLiteSource  # noqa: E402
from eval import corpus  # noqa: E402

BUCKET = "amt-lake"


def _guard(dbname: str) -> None:
    if not dbname.lower().startswith("amt_"):
        sys.exit(f"Refusing to load into database '{dbname}': use a dedicated database whose name "
                 "starts with 'amt_' (e.g. amt_corpus) so existing data is never overwritten.")


def _require(var: str) -> str:
    v = os.environ.get(var)
    if not v:
        sys.exit(f"Environment variable {var} is not set. See the instructions at the top of this file.")
    return v


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------

def ensure_database(dsn: str) -> None:
    """Create the target database (e.g. amt_corpus) if it does not exist yet."""
    import psycopg2
    from psycopg2 import sql
    u = urlparse(dsn)
    dbname = u.path.lstrip("/") or "amt_corpus"
    _guard(dbname)
    admin = psycopg2.connect(urlunparse(u._replace(path="/postgres", query="")))
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            print(f"  created database {dbname}")
    admin.close()


def load_postgres(dsn: str, seed: int = 7) -> None:
    """Copy all six corpus schemas into PostgreSQL with native types
    (NUMERIC, TIMESTAMPTZ, BOOLEAN, JSONB, real ENUM types). Each corpus schema
    becomes a PostgreSQL schema, so tables are e.g. ecommerce.orders."""
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    ensure_database(dsn)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    with tempfile.TemporaryDirectory() as tmp:
        for schema, tables in corpus.SCHEMAS.items():
            db = Path(tmp) / f"{schema}.db"
            corpus.build_schema_db(schema, db, seed)   # same rows as the SQLite runs
            src = SQLiteSource(str(db))
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
                cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
                for ts in tables:
                    defs = []
                    for c in ts.columns:
                        kind = corpus.column_kind(c)
                        decl, _, _, notnull, _, _ = corpus.KINDS[kind]
                        if decl == "ENUM":
                            enum = f"{ts.name}_{c[0]}_t"
                            cur.execute(sql.SQL("CREATE TYPE {} AS ENUM ({})").format(
                                sql.Identifier(enum), sql.SQL(", ").join(sql.Literal(v) for v in c[2].split("|"))))
                            col_type = sql.Identifier(enum)
                        else:
                            col_type = sql.SQL(decl)
                        d = sql.SQL("{} {}").format(sql.Identifier(c[0]), col_type)
                        if kind == "pk":
                            d = sql.SQL("{} PRIMARY KEY").format(d)
                        elif notnull:
                            d = sql.SQL("{} NOT NULL").format(d)
                        if c[1] == "fk":
                            d = sql.SQL("{} REFERENCES {}(id)").format(d, sql.Identifier(c[2]))
                        defs.append(d)
                    cur.execute(sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(ts.name), sql.SQL(", ").join(defs)))
                    table = src.describe(ts.name, 0)
                    names = [c.name for c in table.columns]
                    rows = [tuple(r[n] for n in names) for r in src.iter_rows_schema(table)]
                    execute_values(cur, sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                        sql.Identifier(ts.name), sql.SQL(", ").join(sql.Identifier(n) for n in names)).as_string(cur), rows)
            conn.commit()
            src.close()
            print(f"  loaded {schema}: {len(tables)} tables")
    conn.close()


def check_postgres(dsn: str) -> bool:
    try:
        import psycopg2
    except ImportError:
        print("[PostgreSQL] FAIL: the psycopg2 package is missing. Run: pip install psycopg2-binary")
        return False
    u = urlparse(dsn)
    try:
        c = psycopg2.connect(urlunparse(u._replace(path="/postgres", query="")))
        with c.cursor() as cur:
            cur.execute("SELECT version()")
            print("[PostgreSQL] OK:", cur.fetchone()[0].split(",")[0])
        c.close()
        return True
    except Exception as e:
        print(f"[PostgreSQL] FAIL: {e}".strip())
        print("  Is the PostgreSQL service running, and are the user/password in AMT_PG_DSN correct?")
        return False


# --------------------------------------------------------------------------
# Microsoft SQL Server
# --------------------------------------------------------------------------

# Corpus declared types -> idiomatic SQL Server types. SQL Server has no
# BOOLEAN, TIMESTAMPTZ, ENUM or JSONB, so the usual equivalents are used; this
# changes which columns look "deceptive" to a declared-type-only mapper.
MSSQL_TYPES = {
    "INTEGER": "INT", "SMALLINT": "SMALLINT", "DOUBLE PRECISION": "FLOAT", "TEXT": "NVARCHAR(MAX)",
    "DATE": "DATE", "TIMESTAMP": "DATETIME2", "BOOLEAN": "BIT", "TIMESTAMP WITH TIME ZONE": "DATETIMEOFFSET",
    "NUMERIC": "MONEY", "JSONB": "NVARCHAR(MAX)",
}


def _mssql_conn_str(cfg: dict) -> str:
    import pyodbc
    driver = cfg["driver"] or next((d for d in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server")
                                    if d in pyodbc.drivers()), "SQL Server")
    auth = f"UID={cfg['user']};PWD={cfg['password']}" if cfg["user"] else "Trusted_Connection=yes"
    return f"DRIVER={{{driver}}};SERVER={cfg['server']};{auth};TrustServerCertificate=yes"


def load_sqlserver(uri: str, seed: int = 7) -> None:
    import pyodbc
    from amt.connectors.sources import parse_mssql_uri
    cfg = parse_mssql_uri(uri)
    db = cfg["database"]
    _guard(db)
    base = _mssql_conn_str(cfg)
    admin = pyodbc.connect(base + ";DATABASE=master", autocommit=True)
    admin.execute(f"IF DB_ID(N'{db}') IS NOT NULL BEGIN "
                  f"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END")
    admin.execute(f"CREATE DATABASE [{db}]")
    admin.close()
    conn = pyodbc.connect(base + f";DATABASE={db}", autocommit=True)
    with tempfile.TemporaryDirectory() as tmp:
        for schema, tables in corpus.SCHEMAS.items():
            path = Path(tmp) / f"{schema}.db"
            corpus.build_schema_db(schema, path, seed)
            src = SQLiteSource(str(path))
            conn.execute(f"CREATE SCHEMA [{schema}]")
            for ts in tables:
                defs = []
                for c in ts.columns:
                    kind = corpus.column_kind(c)
                    decl, _, _, notnull, _, _ = corpus.KINDS[kind]
                    q = f"[{c[0]}]"
                    if decl == "ENUM":
                        vals = ", ".join("'" + v + "'" for v in c[2].split("|"))
                        d = f"{q} VARCHAR(20) CHECK ({q} IN ({vals}))"
                    else:
                        d = f"{q} {MSSQL_TYPES.get(decl, decl)}"
                        if decl == "JSONB":
                            d += f" CHECK (ISJSON({q}) = 1)"
                    if kind == "pk":
                        d += " PRIMARY KEY"
                    elif notnull:
                        d += " NOT NULL"
                    if c[1] == "fk":
                        d += f" REFERENCES [{schema}].[{c[2]}](id)"
                    defs.append(d)
                conn.execute(f"CREATE TABLE [{schema}].[{ts.name}] ({', '.join(defs)})")
                table = src.describe(ts.name, 0)
                names = [c.name for c in table.columns]
                rows = [tuple(r[n] for n in names) for r in src.iter_rows_schema(table)]
                cols = ", ".join(f"[{n}]" for n in names)
                marks = ", ".join("?" * len(names))
                conn.cursor().executemany(f"INSERT INTO [{schema}].[{ts.name}] ({cols}) VALUES ({marks})", rows)
            src.close()
            print(f"  loaded {schema}: {len(tables)} tables")
    conn.close()


def check_sqlserver(uri: str) -> bool:
    try:
        import pyodbc  # noqa: F401
        from amt.connectors.sources import SQLServerSource, parse_mssql_uri
        cfg = parse_mssql_uri(uri)
        s = SQLServerSource(**{**cfg, "database": "master"})
        v = next(s._query("SELECT @@VERSION AS v"))["v"].splitlines()[0]
        s.close()
        print(f"[SQL Server] OK: {v.strip()}")
        return True
    except ImportError:
        print("[SQL Server] FAIL: the pyodbc package is missing. Run: pip install pyodbc")
    except Exception as e:
        print(f"[SQL Server] FAIL: {e}")
        print("  Is the 'SQL Server (SQLEXPRESS)' service running, and is the instance name right?")
    return False


# --------------------------------------------------------------------------
# MinIO / S3
# --------------------------------------------------------------------------

def check_minio(create_bucket: bool) -> bool:
    try:
        import boto3
    except ImportError:
        print("[MinIO] FAIL: the boto3 package is missing. Run: pip install boto3")
        return False
    endpoint = _require("AWS_ENDPOINT_URL")
    _require("AWS_ACCESS_KEY_ID")
    _require("AWS_SECRET_ACCESS_KEY")
    try:
        s3 = boto3.client("s3", endpoint_url=endpoint, region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        names = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if BUCKET not in names and create_bucket:
            s3.create_bucket(Bucket=BUCKET)
            print(f"  created bucket {BUCKET}")
        if create_bucket:
            s3.put_object(Bucket=BUCKET, Key="_healthcheck.txt", Body=b"ok")
            assert s3.get_object(Bucket=BUCKET, Key="_healthcheck.txt")["Body"].read() == b"ok"
            s3.delete_object(Bucket=BUCKET, Key="_healthcheck.txt")
        print(f"[MinIO] OK: {endpoint}, buckets: {', '.join(names + ([BUCKET] if create_bucket and BUCKET not in names else [])) or '(none yet)'}")
        return True
    except Exception as e:
        print(f"[MinIO] FAIL: {e}")
        print("  Is `minio server` running in another terminal, and do the keys match its user/password?")
        return False


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="only test the connections")
    ap.add_argument("--skip-postgres", action="store_true")
    ap.add_argument("--skip-minio", action="store_true")
    ap.add_argument("--sqlserver", action="store_true", help="also load the corpus into SQL Server ($AMT_MSSQL_URI)")
    args = ap.parse_args(argv)
    ok = True
    if not args.skip_postgres:
        dsn = _require("AMT_PG_DSN")
        ok &= check_postgres(dsn)
        if ok and not args.check:
            print("Loading the 36-table corpus into PostgreSQL ...")
            load_postgres(dsn)
    if args.sqlserver:
        uri = _require("AMT_MSSQL_URI")
        ok &= check_sqlserver(uri)
        if ok and not args.check:
            print("Loading the 36-table corpus into SQL Server ...")
            load_sqlserver(uri)
    if not args.skip_minio:
        ok &= check_minio(create_bucket=not args.check)
    if ok and not args.check:
        s3 = "" if args.skip_minio else f" --s3 s3://{BUCKET}/eval"
        print("\nAll set. Run the evaluation against the real systems with, for example:")
        if not args.skip_postgres:
            print(f"  python -m eval.run_eval --pg-dsn $env:AMT_PG_DSN{s3}")
        if args.sqlserver:
            print(f"  python -m eval.run_eval --mssql $env:AMT_MSSQL_URI{s3}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
