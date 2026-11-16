"""Web dashboard for the AI Data Migration Tool (standard library only).

    python -m amt ui                                   # just you, opens your browser
    python -m amt ui --host 0.0.0.0 --public           # shared demo for many users

Design for many simultaneous users:
* heavy work runs as background jobs on worker processes (amt/ui/jobs.py);
  the page submits a job and polls its progress, so the server never blocks;
* every visitor gets a private session cookie and every job its own folder;
* demo databases, precedents and table descriptions are prepared once at
  startup and cached;
* public mode allows only the built-in demo databases and private local
  output: no file paths, database addresses or cloud credentials are exposed.
"""
from __future__ import annotations

import io
import json
import os
import re
import secrets
import sys
import tempfile
import threading
import time
import traceback
import webbrowser
import zipfile
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from amt.connectors import LocalTarget, SQLiteSource, open_source, open_target  # noqa: E402
from amt.retriever import PrecedentStore  # noqa: E402
from amt.typesys import declared_family, profile_values  # noqa: E402
from amt.ui.jobs import INJECTION_TEXT, JobManager, JobRejected, json_default  # noqa: E402
from eval import corpus  # noqa: E402
from eval.inject import INJECTIONS  # noqa: E402

STATIC = Path(__file__).parent / "index.html"
# Scratch data lives in the system temp folder, never inside OneDrive.
WORK = Path(os.environ.get("AMT_UI_WORKDIR") or Path(tempfile.gettempdir()) / "amt_ui")
SESSION_COOKIE = "amt_sid"

# (name shown to users, one-line description, uses AI, shown by default)
# Standard is the method; the AI helps as a reviewer (amt/assist.py). The other
# strategies exist for the research comparison and are tucked under
# "Compare with research methods" in the page.
STRATEGIES = {
    "h2": ("Standard (recommended)",
           "Reads real sample values and learns from past migrations. No AI, ready in about a second.", False, True),
    "h1": ("Baseline: declared types only (H1)",
           "Uses only the SQL type written in the schema. Shows what goes wrong without looking at the data.", False, False),
    "s1": ("Baseline: AI without context (S1)",
           "The AI sees only column names and types. Shows why the AI needs samples and examples.", True, False),
    "s3": ("Research: AI maps every column (S3)",
           "The AI makes the whole mapping from samples and past examples (about 4 minutes per table). "
           "As accurate as Standard in the experiments, but much slower.", True, False),
}


class App:
    """Shared, read-mostly application state (one instance per server)."""

    def __init__(self, public: bool, workers: int, ai_workers: int = 1):
        self.public = public
        self.connect_cache: dict[str, dict] = {}
        self._info_cache: tuple[float, dict] | None = None
        self._results_cache: tuple[float, dict] | None = None
        self.lock = threading.Lock()
        self._ai_missing = ["ollama"]
        self.warm()
        if not public:   # the shared demo never offers AI, so it never needs the check
            threading.Thread(target=self._watch_ai, daemon=True).start()
        self.jobs = JobManager(WORK, workers=workers, ai_workers=ai_workers, public=public)

    # -- startup ---------------------------------------------------------------
    def warm(self) -> None:
        """Build demo databases, the precedent store and table descriptions once."""
        (WORK / "db").mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        store = PrecedentStore()
        for schema, tables in corpus.SCHEMAS.items():
            db = WORK / "db" / f"{schema}.db"
            if not db.exists():
                corpus.build_schema_db(schema, db)
            src = SQLiteSource(str(db))
            self.connect_cache[f"demo:{schema}"] = describe_source(src, f"demo:{schema}")
            for ts in tables:
                table = src.describe(ts.name)
                for cm in corpus.gold_mapping(ts).columns:
                    col = table.column(cm.source)
                    store.add({"schema": schema, "table": ts.name, "column": cm.source,
                               "declared_type": col.declared_type,
                               "profile": profile_values([r.get(cm.source) for r in table.sample]),
                               "mapping": {"target_type": cm.target_type, "transform": cm.transform,
                                           "null_handling": cm.null_handling}})
            src.close()
        store.save(str(WORK / "precedents.jsonl"))
        print(f"  prepared {len(self.connect_cache)} demo databases in {time.time() - t0:.1f}s")

    def _watch_ai(self) -> None:
        """Refresh AI availability in the background, so page loads never wait on it.
        When a local model becomes available, load it into memory once (warm-up), so
        the user's first AI review does not also wait ~40 s for the model to load."""
        warmed = False
        while True:
            try:
                self._ai_missing = ai_missing()
            except Exception:
                self._ai_missing = ["ollama"]
            self._info_cache = None
            if not self._ai_missing and not warmed and not os.environ.get("ANTHROPIC_API_KEY"):
                warmed = True
                try:
                    from amt.llm import OllamaClient
                    OllamaClient(cache_dir=None).warm_up()
                except Exception:
                    warmed = False          # try again next round
            time.sleep(30)

    # -- API -------------------------------------------------------------------
    def info(self) -> dict:
        now = time.time()
        cached = self._info_cache
        if cached and now - cached[0] < 15:
            return cached[1]
        missing = self._ai_missing
        info = {
            "public": self.public,
            "demos": [{"id": f"demo:{s}", "name": s.replace("_", " ").title(), "tables": len(t)}
                      for s, t in corpus.SCHEMAS.items()],
            "strategies": [{"id": k, "name": v[0], "description": v[1], "needs_key": v[2], "main": v[3],
                            "missing": missing if v[2] else [],
                            "available": (not v[2]) or not missing}
                           for k, v in STRATEGIES.items()],
            "injections": [{"id": k, "description": INJECTION_TEXT[k]} for k in INJECTIONS],
            "destinations": destinations(self.public),
        }
        self._info_cache = (now, info)
        return info

    def connect(self, body: dict) -> dict:
        source = str(body.get("source") or "")
        if source in self.connect_cache:
            return self.connect_cache[source]
        if self.public or not source:
            raise JobRejected("This shared demo only allows the built-in demo databases.", 403)
        if "://" not in source and not Path(source).is_file():
            raise JobRejected(f"No SQLite file found at {source}. Check the path (for example C:\\data\\shop.db).", 400)
        try:
            src = open_source(source if "://" in source else f"sqlite:///{source}")
        except Exception as e:
            raise JobRejected(connect_error(source, e), 400) from e
        try:
            return describe_source(src, source)
        finally:
            src.close()

    def results(self) -> dict:
        # Prefer the run that includes the AI strategies (results_llm), when it exists.
        p = next((c for c in (ROOT / "results_llm" / "results.json", ROOT / "results" / "results.json")
                  if c.exists()), ROOT / "results" / "results.json")
        if not p.exists():
            return {"available": False}
        mtime = p.stat().st_mtime
        if self._results_cache and self._results_cache[0] == mtime:
            return self._results_cache[1]
        data = json.loads(p.read_text(encoding="utf-8"))
        for s in data.get("strategies", []):
            s.pop("per_table", None)
        out = {"available": True, **data}
        self._results_cache = (mtime, out)
        return out

    def test_destination(self, body: dict) -> dict:
        if self.public:
            raise JobRejected("Cloud destinations are turned off on this shared demo.", 403)
        try:
            uri = (body.get("target_uri") or "").strip()
            if body.get("destination", "local") == "local" or not uri:
                target, where = LocalTarget(str(WORK / "lake")), str(WORK / "lake")
            else:
                target, where = open_target(uri), uri.split("?")[0]
            key = "_amt_connection_test.txt"
            target.write_bytes(key, b"ok")
            ok = target.read_bytes(key) == b"ok"
            target.delete_prefix(key)
            return {"ok": ok, "message": f"Connected: wrote, read back and deleted a test file at {where}."}
        except ImportError as e:
            return {"ok": False, "message": f"Python package missing ({e.name}). Install it with pip, then restart the dashboard."}
        except Exception as e:
            return {"ok": False, "message": f"{type(e).__name__}: {e}"}

    def download(self, session: str, jid: str) -> bytes:
        job_dir = self.jobs.job_dir(session, jid)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for f in sorted(job_dir.rglob("*")):
                if f.is_file() and not f.name.startswith("progress."):
                    z.write(f, f.relative_to(job_dir).as_posix())
        return buf.getvalue()


def describe_source(src, source: str) -> dict:
    tables = []
    spec_by_name = {}
    if source.startswith("demo:"):
        spec_by_name = {t.name: t for t in corpus.SCHEMAS[source[5:]]}
    for name in src.list_tables():
        t = src.describe(name, 20)
        ts = spec_by_name.get(name)
        partition = ts.partition_by if ts else next(
            (c.name for c in t.columns if declared_family(c.declared_type) in ("timestamp", "timestamp_tz")), None)
        tables.append({
            "name": name, "row_count": t.row_count, "tier": ts.tier if ts else None, "partition_by": partition,
            "columns": [{**asdict(c), "family": declared_family(c.declared_type)} for c in t.columns],
            "foreign_keys": [asdict(f) for f in t.foreign_keys], "sample": t.sample[:5],
        })
    return json.loads(json.dumps({"source": source, "tables": tables}, default=json_default))


# --------------------------------------------------------------------------
# Environment checks (AI and destinations)
# --------------------------------------------------------------------------

def _has(module: str) -> bool:
    import importlib.util
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


DB_DRIVERS = {"psycopg2": ("PostgreSQL", "psycopg2-binary"), "pymysql": ("MySQL / MariaDB", "pymysql"),
              "pyodbc": ("SQL Server", "pyodbc")}


def connect_error(source: str, e: Exception) -> str:
    """A message a non-specialist can act on when connecting to a database fails."""
    if isinstance(e, ModuleNotFoundError) and e.name in DB_DRIVERS:
        db, pkg = DB_DRIVERS[e.name]
        return (f"To connect to {db}, this computer needs its free Python driver. In a terminal run "
                f"'pip install {pkg}', then restart the dashboard.")
    if isinstance(e, ValueError) and "unsupported source" in str(e):
        return "This database type is not supported. Choose SQLite, PostgreSQL, MySQL / MariaDB or SQL Server."
    text = str(e).split("\n")[0][:300]
    low = text.lower()
    if isinstance(e, ValueError) and "azure sql needs a sign-in" in low:
        return "Azure SQL needs a sign-in: choose a sign-in method and fill in the user name (and password for SQL login)."
    if "is not allowed to access the server" in low or "client with ip address" in low:
        return ("Azure blocked this computer's IP address. In the Azure portal open the SQL server, go to "
                "Networking, add your client IP address to the firewall rules and save; then try again. "
                f"(Details: {text})")
    if "aadsts" in low or "active directory" in low or "entra" in low:
        return ("Microsoft Entra sign-in failed. Check the email, that the account has access to this database, "
                f"and, for the password method, that the account does not require MFA. (Details: {text})")
    if "password" in low or "login failed" in low or "access denied" in low or "authentication" in low:
        hint = "The user name or password was not accepted."
    elif any(k in low for k in ("could not connect", "connection refused", "timeout", "timed out",
                                "can't connect", "server was not found", "not accessible", "unknown host")):
        hint = "The database server could not be reached. Check that it is running and the host, port or instance are right."
    elif "does not exist" in low or "unknown database" in low or "cannot open database" in low:
        hint = "The server was reached, but that database name does not exist."
    else:
        hint = "Could not connect to the database."
    return f"{hint} (Details: {type(e).__name__}: {text})"


def ai_missing() -> list[str]:
    """What this machine still lacks for the AI strategies (empty list = ready)."""
    from amt.llm import ollama_status
    if os.environ.get("ANTHROPIC_API_KEY") and _has("anthropic"):
        return []
    st = ollama_status()
    if st["running"] and st["has_model"]:
        return []
    return ["ollama"] if not st["running"] else ["ollama_model:" + st["model"]]


def destinations(public: bool) -> list[dict]:
    local = {"id": "local", "name": "Local folder" if not public else "Private download",
             "example": str(WORK / "lake") if not public else "your private download", "package": None,
             "package_ok": True, "creds_ok": True,
             "help": "Files are written to a folder on this computer. Nothing to set up." if not public else
                     "Your files are kept privately for 30 minutes; download them as a .zip after the migration."}
    if public:
        return [local]
    env = os.environ
    aws_creds = False
    if _has("boto3"):
        try:
            import boto3
            aws_creds = boto3.Session().get_credentials() is not None
        except Exception:
            aws_creds = False
    adc = Path(env.get("APPDATA", "")) / "gcloud" / "application_default_credentials.json"
    # Signed in = a storage key/connection string is set, or the Azure CLI has a logged-in
    # account (`az login`); merely having the azure-identity library installed is not enough.
    az_profile = Path.home() / ".azure" / "azureProfile.json"
    try:
        az_login = bool(json.loads(az_profile.read_text(encoding="utf-8-sig")).get("subscriptions"))
    except (OSError, ValueError):
        az_login = False
    azure_auth = any(env.get(k) for k in ("AZURE_STORAGE_CONNECTION_STRING", "AZURE_STORAGE_KEY",
                                          "AZURE_STORAGE_SAS_TOKEN")) or az_login
    return [
        local,
        {"id": "aws", "name": "Amazon S3 (AWS)", "example": "s3://your-bucket/migrations",
         "package": "boto3", "package_ok": _has("boto3"), "creds_ok": aws_creds,
         "help": "Needs AWS credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
                 "(and AWS_DEFAULT_REGION), or run `aws configure`, before starting the dashboard."},
        {"id": "minio", "name": "MinIO (S3-compatible, runs locally)",
         "example": "s3://amt-lake/migrations?endpoint=http://localhost:9000",
         "package": "boto3", "package_ok": _has("boto3"), "creds_ok": aws_creds,
         "help": "Start `minio server` first (see SETUP_POSTGRES_MINIO.md) and set AWS_ACCESS_KEY_ID / "
                 "AWS_SECRET_ACCESS_KEY to the MinIO user and password."},
        {"id": "azure", "name": "Azure Data Lake Storage Gen2 / Blob",
         "example": "abfss://container@youraccount.dfs.core.windows.net/migrations",
         "package": "azure-storage-blob azure-identity", "package_ok": _has("azure.storage.blob"),
         "creds_ok": azure_auth,
         "help": "Set AZURE_STORAGE_CONNECTION_STRING (from the Azure portal: Storage account > "
                 "Access keys), or AZURE_STORAGE_KEY, or sign in with `az login`."},
        {"id": "gcs", "name": "Google Cloud Storage", "example": "gs://your-bucket/migrations",
         "package": "google-cloud-storage", "package_ok": _has("google.cloud.storage"),
         "creds_ok": bool(env.get("GOOGLE_APPLICATION_CREDENTIALS")) or adc.exists(),
         "help": "Set GOOGLE_APPLICATION_CREDENTIALS to a service-account JSON key, "
                 "or run `gcloud auth application-default login`."},
    ]


# --------------------------------------------------------------------------
# HTTP layer
# --------------------------------------------------------------------------

JOB_PATH = re.compile(r"^/api/jobs/([0-9a-f]{16})(/download)?$")
MAX_BODY = 64 * 1024


class Handler(BaseHTTPRequestHandler):
    app: App = None  # set by make_server
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # keep the terminal quiet
        pass

    # -- helpers ---------------------------------------------------------------
    def _session(self) -> tuple[str, bool]:
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == SESSION_COOKIE and re.fullmatch(r"[0-9a-f]{32}", v):
                return v, False
        return secrets.token_hex(16), True

    def _send(self, code: int, body: bytes, ctype: str, extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        if self._new_session:
            self.send_header("Set-Cookie", f"{SESSION_COOKIE}={self._sid}; Path=/; HttpOnly; SameSite=Lax")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, data) -> None:
        self._send(code, json.dumps(data, default=json_default).encode(), "application/json")

    def _handle(self, fn) -> None:
        self._sid, self._new_session = self._session()
        try:
            fn()
        except JobRejected as e:
            self._json(e.status, {"error": str(e)})
        except (KeyError, ValueError) as e:
            self._json(400, {"error": f"Bad request: {e}"})
        except Exception as e:  # report errors to the page instead of crashing the server
            traceback.print_exc()
            self._json(500, {"error": f"Something went wrong ({type(e).__name__}): {e}"})

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if n > MAX_BODY:
            raise JobRejected("Request too large.", 413)
        data = json.loads(self.rfile.read(n) or b"{}")
        if not isinstance(data, dict):
            raise ValueError("expected a JSON object")
        return data

    # -- routes ----------------------------------------------------------------
    def do_GET(self):
        def route():
            path = self.path.split("?")[0]
            app = self.app
            if path in ("/", "/index.html"):
                self._send(200, STATIC.read_bytes(), "text/html; charset=utf-8")
            elif path == "/api/info":
                self._json(200, app.info())
            elif path == "/api/results":
                self._json(200, app.results())
            elif path == "/api/health":
                self._json(200, {"ok": True, **app.jobs.stats()})
            elif (m := JOB_PATH.match(path)):
                jid, dl = m.group(1), m.group(2)
                if dl:
                    data = app.download(self._sid, jid)
                    self._send(200, data, "application/zip",
                               {"Content-Disposition": f'attachment; filename="migration_{jid}.zip"'})
                else:
                    self._json(200, app.jobs.get(self._sid, jid))
            else:
                self._send(404, b"not found", "text/plain")
        self._handle(route)

    def do_POST(self):
        def route():
            path = self.path.split("?")[0]
            app = self.app
            body = self._body()
            if path == "/api/connect":
                self._json(200, app.connect(body))
            elif path == "/api/jobs":
                job_type = body.pop("type", "")
                self._json(202, {"job_id": app.jobs.submit(self._sid, job_type, body)})
            elif path == "/api/test_destination":
                self._json(200, app.test_destination(body))
            else:
                self._send(404, b"not found", "text/plain")
        self._handle(route)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 512


def make_server(host: str = "127.0.0.1", port: int = 8765, public: bool = False,
                workers: int | None = None, ai_workers: int = 1) -> Server:
    WORK.mkdir(parents=True, exist_ok=True)
    workers = workers or max(1, (os.cpu_count() or 2) - 1)
    app = App(public=public, workers=workers, ai_workers=ai_workers)
    handler = type("BoundHandler", (Handler,), {"app": app})
    httpd = Server((host, port), handler)
    httpd.app = app
    return httpd


def serve(port: int = 8765, open_browser: bool = True, host: str = "127.0.0.1", public: bool = False,
          workers: int | None = None) -> None:
    print("Starting the AI Data Migration Tool dashboard ...")
    httpd = make_server(host, port, public, workers)
    shown = "localhost" if host in ("127.0.0.1", "0.0.0.0", "") else host
    url = f"http://{shown}:{port}"
    mode = "PUBLIC shared demo (demo data only)" if public else "private (full features)"
    print(f"Dashboard running at {url}  |  mode: {mode}  |  workers: {httpd.app.jobs.workers}")
    if host == "0.0.0.0":
        print("Listening on all network interfaces: other computers can reach it via this machine's IP address.")
    print("Press Ctrl+C to stop.")
    if open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        httpd.app.jobs.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    serve()
