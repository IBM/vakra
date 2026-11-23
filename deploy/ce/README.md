# VAKRA on IBM Code Engine + COS

An **optional, admin-only** path to run the VAKRA benchmark environment on **IBM Code
Engine**, with the ~35 GB dataset living in **IBM COS**. It is **purely additive** — the
local `make` / docker-compose flow, the MCP servers, and existing CE/COS deployments are
untouched. All resources are named `vakra-*`.

- **4 capability apps** (`vakra-cap1..4`), one image, serving **MCP over HTTP** at `/mcp/<domain>`.
- **Data in COS** (bucket `vakra-benchmark-data-us-east`), mounted into the apps.
- **Benchmark from your laptop** with the same runner — only `--mcp-config` changes.
- Optional **hosted MCP Tools Explorer** (`vakra-explorer`).

## Which doc do I want?

| I want to… | Read |
|---|---|
| A **visual landing page** — what this is + links to try/deploy/benchmark | **[index.html](index.html)** (open in a browser) |
| **Reproduce the whole setup from scratch** (deploy data + apps), with every gotcha + fix | **[RUNBOOK.md](RUNBOOK.md)** ← authoritative |
| **Use the already-deployed apps** — call the MCP endpoints, run the benchmark, browse tools | **[USING.md](USING.md)** |
| A **one-page command cheatsheet** (deploy · use · operate) | **[CHEATSHEET.md](CHEATSHEET.md)** |

> RUNBOOK.md is the single source of truth for step detail. This README and the cheatsheet
> intentionally don't duplicate step specifics (so they can't drift) — they point to it.

---

## How it works (the two seams)

**Transport.** Locally, clients reach the MCP servers via `docker exec` (stdio). Code Engine
has no `docker exec`, so `docker/mcp_http_bridge.py` serves MCP over HTTP at `/mcp/<domain>`,
running only when the container starts with `SERVE_MODE=http`. Each domain gets one
long-lived, domain-pinned `mcp_dispatch.py` subprocess (same `MCP_DOMAIN` mechanism as
`docker exec -e`) that serves all of that domain's calls concurrently, so `list_tools()` is
scoped to exactly one domain — a client on `/mcp/california_schools` can't see another
domain's tools, and `verify_checksum` still passes. Tool lists are cached per domain, and
each backend reads a per-domain slice of the OpenAPI spec written once at start-up, so
many domains can run in parallel (see *Parallel runs & sizing* below).

**Data.** Default `DATA_SOURCE=mount` mounts the COS bucket into each app at the paths the
servers already expect (`/app/db`, `/app/environment/configs`, and for cap 4
`/app/retrievers/chroma_data` + `/app/retrievers/queries`). Fallback `DATA_SOURCE=sync`
downloads the data to the container's ephemeral disk at startup (bigger instances; use if
the s3fs mount is too slow or cap 4's ChromaDB needs writes).

```
your laptop:  benchmark_runner.py --mcp-config …ce.yaml   (reads data/test/ locally for inputs)
                     │  MCP over HTTPS  (/mcp/<domain>)
                     ▼
   4 Code Engine apps: vakra-cap1..4  (same image, SERVE_MODE=http, CAPABILITY_ID=1..4)
     mcp_http_bridge ──one per domain (MCP_DOMAIN)──► mcp_dispatch.py ──► FastAPI :8000 / :8001
     data mounted from COS pds  ◄── vakra-store ◄── bucket vakra-benchmark-data-us-east
```

Account conventions (reused, overridable in `config.sh`): region `us-east`, resource group
`routing`, CE project `ce-project-routing`, registry `icr.io/routing_namespace` (push secret
`icr-secret-1`), and a COS service-credential JSON via `COS_CREDS_JSON` (default `~/.cos_creds.json`; see RUNBOOK.md → Prerequisites for the required fields).

---

## Parallel runs & sizing

Measured on Code Engine (23 Nov 2026) by replaying real benchmark tool calls. One agent
in a parallel run makes roughly 2 tool calls/s; agents with a slower LLM make fewer.

| App | Size (`config.sh`) | Throughput | Full-speed parallel runs | Safe maximum |
|---|---|---|---|---|
| vakra-cap2 | 6 vCPU / 48 GB | ~37 calls/s | 17–20 | ~60 (slower, 0 errors at 68) |
| vakra-cap3 | 4 vCPU / 16 GB | ~14–22 calls/s | ~10 | ~40 (all 38 domains, ~2 s/call) |
| vakra-cap4 | 8 vCPU / 32 GB | ~22 SQL calls/s, ~3.5 searches/s | ~10 | ~40 (search slows to seconds–tens of seconds) |

Above full speed, calls queue and get slower; they fail only once a call waits longer than
`MCP_BACKEND_TIMEOUT`. cap 4 semantic search is CPU-bound and searches on the same Chroma
collection run one at a time; the first search on each collection after a restart can take
up to ~2 minutes while its index loads from the COS mount.

Runtime settings (set by `3_deploy_apps.sh` from `config.sh`):

| Variable | Default | Purpose |
|---|---|---|
| `M3_WORKERS` | 4 (image default 1) | uvicorn workers for the M3 SQL API; handlers block on sqlite, so one worker runs one query at a time |
| `M3_READY_TIMEOUT` | 300 | seconds to wait for the M3 API at start-up |
| `BRIDGE_PERSISTENT` | 1 | one long-lived backend per domain; `0` = new process per request (slow) |
| `BRIDGE_IDLE_SECONDS` | 900 | stop a domain's backend after this much idle time |
| `BRIDGE_MAX_PERSISTENT` | 64 | cap on live backends (~60 MB each); the least recently used idle one is evicted |
| `BRIDGE_MAX_BACKENDS` | 32 | cap on one-off backend processes running at once |
| `MCP_BACKEND_TIMEOUT` | 30 (cap 4: 120) | seconds a tool call waits for the FastAPI backend |

Load-test helpers (`parallel_load.py`, `replay_load.py`) live in the `vakra-demo` repo.
Check an app's traffic before redeploying it: an update restarts the app and drops any run
in progress.

---

## Files

| File | What it does |
|---|---|
| `config.sh` | shared config + `admin_guard` (sourced by the scripts; never run directly) |
| `0_push_data_to_cos.py` | reuses the HF download → mirrors `data/` + configs to COS (`--stream` for low disk) |
| `1_create_bucket_and_pds.sh` | bucket (reuse if owned) + HMAC secret + CE persistent data store |
| `2_build_push_image.sh` | cloud buildrun of `docker/Dockerfile.unified` → ICR |
| `3_deploy_apps.sh` | deploy `vakra-cap1..4` (delete+recreate); writes `.ce_urls.env` |
| `4_deploy_explorer.sh` | optional hosted explorer (`vakra-explorer`) |
| `5_test_ce.py` | health + MCP `list_tools` per capability (`--verify-checksums` optional) |
| `teardown.sh` | delete only `vakra-*` resources (`--delete-bucket` to also drop the bucket) |
| `Dockerfile.explorer` | image for the hosted explorer |
| `.ce_urls.env` | generated by step 3 — `source` it before using the runner/explorer |
| `../../docker/mcp_http_bridge.py` | stdio→HTTP MCP bridge (only when `SERVE_MODE=http`) |
| `../../docker/cos_sync.py` | COS→local sync (only when `DATA_SOURCE=sync`) |
| `../../benchmark/mcp_connection_config.ce.yaml` | HTTP config for the runner/explorer |

Everything is gated behind `VAKRA_CE_ADMIN=1` and prompts once (add `YES=1` or `-y` to skip).
