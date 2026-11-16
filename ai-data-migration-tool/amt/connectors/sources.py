"""Source adapters: extract schema metadata, samples and rows from a relational DB.

SQLite needs no extra packages and is used by the tests and the evaluation
corpus. PostgreSQL (psycopg2) and MySQL (pymysql) adapters load their driver
lazily so the rest of the tool works without them installed.
"""
from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Iterator
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

from ..models import Column, ForeignKey, Table
from ..typesys import declared_family


LARGE_TABLE = 50_000     # above this, samples are fetched with OFFSET instead of a full scan


class SourceConnector(ABC):
    paramstyle = "?"

    @abstractmethod
    def list_tables(self) -> list[str]: ...

    @abstractmethod
    def _columns(self, table: str) -> list[Column]: ...

    @abstractmethod
    def _foreign_keys(self, table: str) -> list[ForeignKey]: ...

    @abstractmethod
    def _query(self, sql: str) -> Iterator[dict]: ...

    def _normalise(self, table: Table, row: dict) -> dict:
        return row

    def quote(self, ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    def table_ref(self, table: str) -> str:
        """How a table is named in a query (engines without a search path qualify it)."""
        return self.quote(table)

    def count(self, table: str) -> int:
        return next(iter(self._query(f"SELECT COUNT(*) AS n FROM {self.table_ref(table)}")))["n"]

    def describe(self, table: str, sample_size: int = 20) -> Table:
        t = Table(table, self._columns(table), self._foreign_keys(table))
        t.row_count = self.count(table)
        if t.columns and sample_size:
            step = max(1, t.row_count // sample_size)
            if t.row_count > LARGE_TABLE:
                # Large table: fetch evenly spaced rows inside the database instead of
                # streaming the whole table through Python just to keep 20 of them.
                t.sample = [self._normalise(t, r) for r in self._spread_sample(t, step, sample_size)]
            else:
                rows = self.iter_rows_schema(t)
                t.sample = [r for i, r in enumerate(rows) if i % step == 0][:sample_size]
        return t

    def _spread_sample(self, t: Table, step: int, n: int) -> list[dict]:
        order = self.quote(t.primary_key) if t.primary_key else "1"
        out = []
        for i in range(n):
            out += list(self._query(f"SELECT * FROM {self.table_ref(t.name)} ORDER BY {order} "
                                    f"LIMIT 1 OFFSET {i * step}"))
        return out

    def iter_rows_schema(self, t: Table) -> Iterator[dict]:
        order = f" ORDER BY {self.quote(t.primary_key)}" if t.primary_key else ""
        for row in self._query(f"SELECT * FROM {self.table_ref(t.name)}{order}"):
            yield self._normalise(t, row)

    def iter_rows(self, table: str) -> Iterator[dict]:
        return self.iter_rows_schema(Table(table, self._columns(table)))

    # -- large tables: streaming reads, incremental windows, database-side aggregates --
    def _stream(self, sql: str, params: tuple) -> Iterator[dict]:
        """Rows one by one without loading the result set into memory (overridden
        where the driver buffers everything by default)."""
        yield from self._query(sql, params)

    def window_sql(self, column: str | None, after=None, upto=None) -> tuple[str, tuple]:
        """WHERE clause for an incremental window: column > after AND column <= upto."""
        conds, params = [], []
        if column and after is not None:
            conds.append(f"{self.quote(column)} > {self.paramstyle}")
            params.append(after)
        if column and upto is not None:
            conds.append(f"{self.quote(column)} <= {self.paramstyle}")
            params.append(upto)
        return (" WHERE " + " AND ".join(conds) if conds else ""), tuple(params)

    def stream_rows(self, t: Table, column: str | None = None, after=None, upto=None) -> Iterator[dict]:
        where, params = self.window_sql(column, after, upto)
        order = f" ORDER BY {self.quote(t.primary_key)}" if t.primary_key else ""
        for row in self._stream(f"SELECT * FROM {self.table_ref(t.name)}{where}{order}", params):
            yield self._normalise(t, row)

    def aggregate(self, table: str, exprs: dict[str, str], column: str | None = None,
                  after=None, upto=None) -> dict:
        """Let the database compute aggregates (COUNT, SUM, MIN, MAX) itself."""
        where, params = self.window_sql(column, after, upto)
        sel = ", ".join(f"{e} AS {self.quote(k)}" for k, e in exprs.items())
        return next(iter(self._query(f"SELECT {sel} FROM {self.table_ref(table)}{where}", params)))

    def max_value(self, table: str, column: str):
        return self.aggregate(table, {"m": f"MAX({self.quote(column)})"})["m"]

    def close(self) -> None:
        pass


class SQLiteSource(SourceConnector):
    def __init__(self, path: str):
        # check_same_thread=False lets the web dashboard reuse one connection;
        # its API calls are serialised, so access is never concurrent.
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def list_tables(self) -> list[str]:
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        return [r[0] for r in cur]

    def _columns(self, table: str) -> list[Column]:
        cur = self.conn.execute(f"PRAGMA table_info({self.quote(table)})")
        return [Column(r["name"], r["type"], nullable=not r["notnull"] and not r["pk"],
                       primary_key=bool(r["pk"])) for r in cur]

    def _foreign_keys(self, table: str) -> list[ForeignKey]:
        cur = self.conn.execute(f"PRAGMA foreign_key_list({self.quote(table)})")
        return [ForeignKey(r["from"], r["table"], r["to"]) for r in cur]

    def _query(self, sql: str, params=()) -> Iterator[dict]:
        for r in self.conn.execute(sql, params):     # sqlite3 cursors already fetch lazily
            yield dict(r)

    def _normalise(self, table: Table, row: dict) -> dict:
        # SQLite stores BOOLEAN as 0/1 and DECIMAL as REAL; restore the
        # values a PostgreSQL driver would return so behaviour matches.
        for c in table.columns:
            v = row.get(c.name)
            if v is None:
                continue
            fam = declared_family(c.declared_type)
            if fam == "bool" and isinstance(v, int):
                row[c.name] = bool(v)
            elif fam == "decimal" and isinstance(v, (int, float)):
                row[c.name] = Decimal(repr(v)) if isinstance(v, float) else Decimal(v)
        return row

    def close(self) -> None:
        self.conn.close()


class PostgresSource(SourceConnector):
    paramstyle = "%s"

    def __init__(self, dsn: str, schema: str = "public"):
        import psycopg2
        import psycopg2.extras
        from psycopg2 import sql
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True          # read-only extraction, no open transactions
        self.schema = schema
        self._extras = psycopg2.extras
        with self.conn.cursor() as cur:      # unqualified table names resolve to this schema
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))

    def _query(self, sql: str, params=()) -> Iterator[dict]:
        with self.conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            yield from cur

    def _stream(self, sql: str, params: tuple) -> Iterator[dict]:
        # A named (server-side) cursor fetches 10,000 rows at a time instead of the
        # whole result; withhold=True allows it while the connection is in autocommit.
        import uuid
        cur = self.conn.cursor(name=f"amt_{uuid.uuid4().hex[:12]}", withhold=True,
                               cursor_factory=self._extras.RealDictCursor)
        cur.itersize = 10000
        try:
            cur.execute(sql, params)
            yield from cur
        finally:
            cur.close()

    def list_tables(self) -> list[str]:
        return [r["table_name"] for r in self._query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s "
            "AND table_type='BASE TABLE' ORDER BY table_name", (self.schema,))]

    def _columns(self, table: str) -> list[Column]:
        pk = {r["column_name"] for r in self._query(
            "SELECT kcu.column_name FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name "
            "AND tc.table_schema=kcu.table_schema WHERE tc.constraint_type='PRIMARY KEY' "
            "AND tc.table_schema=%s AND tc.table_name=%s", (self.schema, table))}
        cols = []
        for r in self._query(
                "SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale, is_nullable "
                "FROM information_schema.columns WHERE table_schema=%s AND table_name=%s "
                "ORDER BY ordinal_position", (self.schema, table)):
            dt = r["data_type"].upper()
            if dt == "NUMERIC" and r["numeric_precision"] is not None:
                dt = f"NUMERIC({r['numeric_precision']},{r['numeric_scale']})"
            elif dt == "USER-DEFINED":
                dt = "ENUM"
            cols.append(Column(r["column_name"], dt, r["is_nullable"] == "YES", r["column_name"] in pk))
        return cols

    def _foreign_keys(self, table: str) -> list[ForeignKey]:
        return [ForeignKey(r["column_name"], r["ref_table"], r["ref_column"]) for r in self._query(
            "SELECT kcu.column_name, ccu.table_name AS ref_table, ccu.column_name AS ref_column "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name "
            "JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name=ccu.constraint_name "
            "WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema=%s AND tc.table_name=%s",
            (self.schema, table))]

    def close(self) -> None:
        self.conn.close()


class MySQLSource(SourceConnector):
    paramstyle = "%s"

    def __init__(self, host, user, password, database, port=3306):
        import pymysql
        self.conn = pymysql.connect(host=host, user=user, password=password, database=database,
                                    port=port, cursorclass=pymysql.cursors.DictCursor)
        self.database = database

    def quote(self, ident: str) -> str:
        return "`" + ident.replace("`", "``") + "`"

    def _query(self, sql: str, params=()) -> Iterator[dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            yield from cur.fetchall()

    def _stream(self, sql: str, params: tuple) -> Iterator[dict]:
        import pymysql
        with self.conn.cursor(pymysql.cursors.SSDictCursor) as cur:   # unbuffered: rows arrive as read
            cur.execute(sql, params)
            yield from cur

    def list_tables(self) -> list[str]:
        return [r["TABLE_NAME"] for r in self._query(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s "
            "AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (self.database,))]

    def _columns(self, table: str) -> list[Column]:
        return [Column(r["COLUMN_NAME"], r["COLUMN_TYPE"].upper(), r["IS_NULLABLE"] == "YES",
                       r["COLUMN_KEY"] == "PRI") for r in self._query(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION", (self.database, table))]

    def _foreign_keys(self, table: str) -> list[ForeignKey]:
        return [ForeignKey(r["COLUMN_NAME"], r["REFERENCED_TABLE_NAME"], r["REFERENCED_COLUMN_NAME"])
                for r in self._query(
            "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM "
            "information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s "
            "AND REFERENCED_TABLE_NAME IS NOT NULL", (self.database, table))]

    def close(self) -> None:
        self.conn.close()


AZURE_AUTH = ("ActiveDirectoryInteractive", "ActiveDirectoryPassword", "ActiveDirectoryMsi",
              "ActiveDirectoryServicePrincipal", "ActiveDirectoryIntegrated")


def _odbc_value(v: str) -> str:
    """Brace-quote an ODBC connection-string value when it contains special characters."""
    return "{" + v.replace("}", "}}") + "}" if any(c in v for c in ";{}=") or v != v.strip() else v


def mssql_connection_string(server: str, database: str, user: str | None = None, password: str | None = None,
                            driver: str = "ODBC Driver 18 for SQL Server", authentication: str | None = None,
                            encrypt: bool | None = None) -> str:
    """Build the ODBC connection string for SQL Server or Azure SQL Database.

    * local SQL Server, no user: Windows login (Trusted_Connection)
    * user + password: SQL login
    * authentication=ActiveDirectory...: Microsoft Entra ID (Azure AD) sign-in
    Azure SQL (*.database.windows.net) always needs an encrypted connection with a
    verified certificate; a local server keeps TrustServerCertificate for its self-signed one."""
    azure = server.split(",")[0].lower().endswith((".database.windows.net", ".database.azure.com",
                                                   ".sql.azuresynapse.net", ".datawarehouse.fabric.microsoft.com"))
    if authentication and authentication not in AZURE_AUTH:
        raise ValueError(f"unknown authentication {authentication}; use one of {', '.join(AZURE_AUTH)}")
    parts = [f"DRIVER={{{driver}}}", f"SERVER={_odbc_value(server)}", f"DATABASE={_odbc_value(database)}"]
    if authentication:
        parts.append(f"Authentication={authentication}")
        if user:
            parts.append(f"UID={_odbc_value(user)}")
        if password and authentication in ("ActiveDirectoryPassword", "ActiveDirectoryServicePrincipal"):
            parts.append(f"PWD={_odbc_value(password)}")
    elif user:
        parts += [f"UID={_odbc_value(user)}", f"PWD={_odbc_value(password or '')}"]
    else:
        if azure:
            raise ValueError("Azure SQL needs a sign-in: a SQL login (user and password) or a Microsoft Entra method.")
        parts.append("Trusted_Connection=yes")
    if encrypt or (encrypt is None and azure):
        parts += ["Encrypt=yes", "TrustServerCertificate=no", "Connection Timeout=30"]
    else:
        parts.append("TrustServerCertificate=yes")
    return ";".join(parts) + ";"


class SQLServerSource(SourceConnector):
    """Microsoft SQL Server and Azure SQL Database via pyodbc. Without a user name
    it uses Windows authentication (Trusted_Connection), which is how SQL Server
    Express is usually set up on a laptop."""

    def __init__(self, server: str, database: str, schema: str = "dbo", user: str | None = None,
                 password: str | None = None, driver: str | None = None, authentication: str | None = None,
                 encrypt: bool | None = None):
        import pyodbc
        driver = driver or next((d for d in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server")
                                 if d in pyodbc.drivers()), "SQL Server")
        if authentication and driver == "SQL Server":
            raise RuntimeError("Microsoft Entra sign-in needs 'ODBC Driver 18 for SQL Server' (free from Microsoft).")
        self.conn = pyodbc.connect(mssql_connection_string(server, database, user, password, driver,
                                                           authentication, encrypt), autocommit=True)
        # pyodbc cannot decode DATETIMEOFFSET (ODBC type -155) on its own.
        self.conn.add_output_converter(-155, _datetimeoffset)
        self.schema = schema

    def quote(self, ident: str) -> str:
        return "[" + ident.replace("]", "]]") + "]"

    def table_ref(self, table: str) -> str:
        return f"{self.quote(self.schema)}.{self.quote(table)}"

    def _spread_sample(self, t: Table, step: int, n: int) -> list[dict]:
        order = self.quote(t.primary_key) if t.primary_key else "(SELECT NULL)"
        out = []
        for i in range(n):
            out += list(self._query(f"SELECT * FROM {self.table_ref(t.name)} ORDER BY {order} "
                                    f"OFFSET {i * step} ROWS FETCH NEXT 1 ROWS ONLY"))
        return out

    def _query(self, sql: str, params=()) -> Iterator[dict]:
        cur = self.conn.cursor()
        cur.execute(sql, *params)
        names = [d[0] for d in cur.description]
        for row in cur:
            yield dict(zip(names, row))

    def list_tables(self) -> list[str]:
        return [r["TABLE_NAME"] for r in self._query(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? "
            "AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (self.schema,))]

    def _columns(self, table: str) -> list[Column]:
        pk = {r["COLUMN_NAME"] for r in self._query(
            "SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME=kcu.CONSTRAINT_NAME "
            "AND tc.TABLE_SCHEMA=kcu.TABLE_SCHEMA WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY' "
            "AND tc.TABLE_SCHEMA=? AND tc.TABLE_NAME=?", (self.schema, table))}
        cols = []
        for r in self._query(
                "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH, "
                "IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? "
                "ORDER BY ORDINAL_POSITION", (self.schema, table)):
            dt = r["DATA_TYPE"].upper()
            if dt in ("DECIMAL", "NUMERIC"):
                dt = f"{dt}({r['NUMERIC_PRECISION']},{r['NUMERIC_SCALE']})"
            elif dt in ("VARCHAR", "NVARCHAR", "CHAR", "NCHAR") and r["CHARACTER_MAXIMUM_LENGTH"]:
                n = r["CHARACTER_MAXIMUM_LENGTH"]
                dt = f"{dt}({'MAX' if n == -1 else n})"
            cols.append(Column(r["COLUMN_NAME"], dt, r["IS_NULLABLE"] == "YES", r["COLUMN_NAME"] in pk))
        return cols

    def _foreign_keys(self, table: str) -> list[ForeignKey]:
        return [ForeignKey(r["col"], r["ref_table"], r["ref_col"]) for r in self._query(
            "SELECT pc.name AS col, rt.name AS ref_table, rc.name AS ref_col FROM sys.foreign_key_columns fkc "
            "JOIN sys.tables pt ON fkc.parent_object_id=pt.object_id "
            "JOIN sys.schemas s ON pt.schema_id=s.schema_id "
            "JOIN sys.columns pc ON pc.object_id=fkc.parent_object_id AND pc.column_id=fkc.parent_column_id "
            "JOIN sys.tables rt ON fkc.referenced_object_id=rt.object_id "
            "JOIN sys.columns rc ON rc.object_id=fkc.referenced_object_id AND rc.column_id=fkc.referenced_column_id "
            "WHERE s.name=? AND pt.name=?", (self.schema, table))]

    def close(self) -> None:
        self.conn.close()


def _datetimeoffset(raw: bytes):
    import struct
    from datetime import datetime, timedelta, timezone
    y, mo, d, h, mi, sec, ns, tzh, tzm = struct.unpack("<6hI2h", raw)
    return datetime(y, mo, d, h, mi, sec, ns // 1000, timezone(timedelta(hours=tzh, minutes=tzm)))


def parse_mssql_uri(uri: str) -> dict:
    """mssql://[user:pw@]host[:port]/database?instance=SQLEXPRESS&schema=dbo[&driver=...]
    Azure SQL: mssql://user:pw@myserver.database.windows.net:1433/mydb?schema=dbo
               [&authentication=ActiveDirectoryInteractive][&encrypt=yes]"""
    u = urlparse(uri)
    q = {k: v[0] for k, v in parse_qs(u.query).items()}
    server = u.hostname or "localhost"
    if q.get("instance"):
        server += "\\" + q["instance"]
    elif u.port:
        server += f",{u.port}"
    enc = q.get("encrypt")
    return {"server": server, "database": unquote(u.path.lstrip("/")) or "master", "schema": q.get("schema", "dbo"),
            "user": unquote(u.username) if u.username else None,
            "password": unquote(u.password) if u.password else None, "driver": q.get("driver"),
            "authentication": q.get("authentication") or None,
            "encrypt": None if enc is None else enc.lower() in ("yes", "true", "1", "mandatory")}


def split_schema(uri: str) -> tuple[str, str]:
    """postgresql://.../db?schema=sales -> (postgresql://.../db, "sales")"""
    u = urlparse(uri)
    q = parse_qs(u.query)
    schema = q.pop("schema", ["public"])[0]
    return urlunparse(u._replace(query=urlencode(q, doseq=True))), schema


def open_source(uri: str) -> SourceConnector:
    """sqlite:///path.db | postgresql://user:pw@host:5432/db[?schema=name] | mysql://user:pw@host/db
    | mssql://localhost/db?instance=SQLEXPRESS&schema=name (Windows login if no user given)"""
    u = urlparse(uri)
    if u.scheme == "sqlite":
        path = unquote(uri[len("sqlite:///"):]) if uri.startswith("sqlite:///") else u.path
        return SQLiteSource(path)
    if u.scheme in ("postgres", "postgresql"):
        return PostgresSource(*split_schema(uri))
    if u.scheme in ("mssql", "sqlserver", "azuresql"):
        return SQLServerSource(**parse_mssql_uri(uri))
    if u.scheme == "mysql":
        return MySQLSource(u.hostname, unquote(u.username or ""), unquote(u.password or ""),
                           u.path.lstrip("/"), u.port or 3306)
    raise ValueError(f"unsupported source URI: {uri}")
