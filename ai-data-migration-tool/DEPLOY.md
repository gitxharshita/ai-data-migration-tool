# Running the dashboard for many users

## What changed for multi-user use

| Concern | How it is handled |
|---|---|
| Many people clicking at once | Heavy work runs as background jobs on worker processes (one per CPU core). The page shows live progress and queue position instead of freezing. |
| Users seeing each other's results | Every visitor gets a private session cookie; every job has its own folder. Other users get "not found". |
| Old results piling up | Finished jobs and their files are deleted after 30 minutes. |
| One user flooding the server | At most 3 running jobs per user, and at most 400 queued in total. Beyond that, a friendly "please wait" message. |
| Security on a shared server | `--public` mode allows only the built-in demo databases and private downloads: no file paths, database addresses or cloud credentials. |
| Slow page loads | Demo databases, table descriptions and past-migration examples are prepared once at startup and cached. |

## Measured capacity (this laptop: Intel i5-10310U, 4 cores, 16 GB RAM, 7 workers)

Each simulated user opens the page, connects, generates a mapping, runs a migration,
downloads the .zip and runs the fault-injection view (3 jobs per user).

| Scenario | Users finished | Errors | Page / API median | Job time median (p95) |
|---|---|---|---|---|
| 100 users arriving over 60 s | 100/100 | 0 | 0.01-0.02 s | plan 0.6 s (1.5), migrate 1.2 s (2.6), break 1.1 s (2.2) |
| 100 users at the same second | 100/100 | 0 | 0.1 s | plan 3 s (10), migrate 11 s (22), break 16 s (22) |

Throughput was 260-350 jobs per minute. Reproduce with `tools/loadtest.py`.

## Option A: a demo on the same network (quickest)

On the laptop, in the `ai-data-migration-tool` folder:

```powershell
python -m amt ui --host 0.0.0.0 --public --no-browser
```

Allow Python through Windows Firewall when asked (private networks). Find the laptop's
address with `ipconfig` (IPv4 Address, e.g. 172.16.4.25) and share
`http://172.16.4.25:8765`. Only people on the same network can reach it, and some
campus Wi-Fi networks block device-to-device traffic. Keep the laptop plugged in
and awake.

## Option B: a cloud server (for users anywhere)

On any Linux server with Docker (for example an Azure for Students virtual machine):

```bash
docker build -t amt-dashboard .
docker run -d --restart unless-stopped -p 8765:8765 amt-dashboard
```

Then open port 8765 in the server's firewall or network security group. For a link
you send to many people, put it behind HTTPS (for example Caddy:
`caddy reverse-proxy --from your-domain.example --to localhost:8765`). A 2-4 core
server handles the 100-user load above; more cores give more workers.

## Settings

| Option | Default | Meaning |
|---|---|---|
| `--host` | 127.0.0.1 | `0.0.0.0` makes it reachable from other computers |
| `--port` | 8765 | Port to listen on |
| `--public` | off | Shared-demo mode (demo data only, private downloads) |
| `--workers` | CPU cores - 1 | Parallel worker processes |
| `AMT_UI_WORKDIR` | system temp folder | Where demo databases and job files are kept |

`GET /api/health` returns queue and timing statistics, suitable for monitoring.
The AI strategies (S1/S3) are not offered in public mode: a local AI model answers
one request at a time, too slowly for 100 users.
