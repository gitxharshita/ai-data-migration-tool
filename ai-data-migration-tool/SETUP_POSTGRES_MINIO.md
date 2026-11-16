# Running the project on real PostgreSQL and MinIO (Windows)

By default the project uses SQLite files as the source and a local folder as the
target. These steps replace them with a real **PostgreSQL** database server and
**MinIO**, a free object store that speaks the same protocol as Amazon S3.

## A. Install PostgreSQL (about 10 minutes, one time)

1. Open https://www.postgresql.org/download/windows/ and click **Download the installer**
   (published by EDB). Choose the latest version for **Windows x86-64**.
2. Run the installer and accept the defaults, with these choices:
   * Components: keep **PostgreSQL Server**, **pgAdmin 4** (a viewer for your tables) and
     **Command Line Tools**. Stack Builder is not needed.
   * **Password**: choose one for the `postgres` user and write it down. Avoid the characters
     `@ : / ? #` in it, because they break database addresses.
   * Port: keep **5432**.
3. At the end, untick "Launch Stack Builder" and click Finish.
   PostgreSQL now runs in the background and starts with Windows automatically.

## B. Install MinIO (about 5 minutes, one time)

> **Already done on this laptop (Nov 2026):** MinIO is installed in `%USERPROFILE%\minio`
> with a start script and a random local password. Skip to step C.

MinIO no longer publishes Windows builds on its download site (the old link returns
"410 Gone"). The last free open-source build is on GitHub:

1. Create the folder `%USERPROFILE%\minio`.
2. From https://github.com/minio/minio/releases/tag/RELEASE.2025-09-07T16-13-09Z download
   `minio.windows-amd64.RELEASE.2025-09-07T16-13-09Z.exe` (about 108 MB), save it into
   that folder and rename it to `minio.exe`. Do not use the "AIStor" download: that is
   MinIO's paid product.

## C. Start MinIO (every time you want to use it)

Open a **new** terminal (PowerShell) and keep it open while you work:

```powershell
powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\minio\start_minio.ps1"
```

The script sets the user `amtadmin` and the password stored inside it. (If you
installed MinIO yourself, set `$env:MINIO_ROOT_USER` and `$env:MINIO_ROOT_PASSWORD`, then
run `minio.exe server data --console-address ":9001"`.)

Open http://localhost:9001 in your browser and log in with that user and password
to see your buckets and files.

## D. Install the Python drivers (one time)

```powershell
pip install psycopg2-binary boto3
```

## E. Tell the project where everything is (every new terminal)

In your normal VS Code terminal, inside the `ai-data-migration-tool` folder:

```powershell
$env:AMT_PG_DSN = "postgresql://postgres:YOUR_POSTGRES_PASSWORD@localhost:5432/amt_corpus"
$env:AWS_ENDPOINT_URL = "http://localhost:9000"
. "$env:USERPROFILE\minio\minio_env.ps1"   # sets the AWS_* keys to match MinIO
```

## F. Check, load and run

```powershell
python -m eval.setup_backends --check     # both lines should say OK
python -m eval.setup_backends             # creates database amt_corpus, loads 36 tables, creates bucket amt-lake
python -m eval.run_eval --pg-dsn $env:AMT_PG_DSN --s3 s3://amt-lake/eval
```

The results file then says `Backend: source: PostgreSQL; target: S3-compatible object store`.

## G. See it with your own eyes

* **Source tables:** open **pgAdmin 4**, then Servers > PostgreSQL > Databases > amt_corpus >
  Schemas > ecommerce > Tables. Right-click `orders` and choose View/Edit Data.
* **Migrate one table into MinIO and keep the files:**

  ```powershell
  python -m amt migrate --source "$($env:AMT_PG_DSN)?schema=ecommerce" --table orders --target s3://amt-lake/demo --partition-by placed_at
  ```

  Then open http://localhost:9001 and browse `amt-lake/demo/orders/`: one folder per month.
* **Dashboard:** choose "My own database..." and enter
  `postgresql://postgres:YOUR_POSTGRES_PASSWORD@localhost:5432/amt_corpus?schema=ecommerce`.

## SQL Server (already installed on this laptop)

SQL Server Express (`SQLEXPRESS`) is a second real source engine. It uses your Windows
login, so no password is needed. Install the driver once with `pip install pyodbc`, then:

```powershell
$env:AMT_MSSQL_URI = "mssql://localhost/amt_corpus?instance=SQLEXPRESS"
python -m eval.setup_backends --sqlserver --skip-postgres --skip-minio   # loads 36 tables into database amt_corpus
python -m eval.run_eval --mssql $env:AMT_MSSQL_URI --out results_sqlserver
```

Browse the tables in SQL Server Management Studio (or the VS Code "SQL Server" extension):
database `amt_corpus`, schemas `ecommerce`, `healthcare` and so on. In the dashboard, choose
"My own database..." and enter `mssql://localhost/amt_corpus?instance=SQLEXPRESS&schema=ecommerce`.
The loader only ever creates or replaces databases whose name starts with `amt_`; your
other databases are never touched.

## Troubleshooting

| Message | Fix |
|---|---|
| `password authentication failed` | Wrong password in `AMT_PG_DSN`. |
| `could not connect to server` | PostgreSQL service stopped: open the Services app and start "postgresql-x64-NN". |
| `Could not connect to the endpoint URL` | MinIO is not running. Start it again (step C). |
| `InvalidAccessKeyId` / `SignatureDoesNotMatch` | The AWS_* keys don't match the MinIO user and password. |
| `Environment variable ... is not set` | Run step E again; variables only last for one terminal window. |
