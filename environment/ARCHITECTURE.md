# Architecture

```
HOST
+------------------------------------------------------------+
|                    benchmark_runner.py                     |
|                                                            |
|  Loads domain questions  |  Runs LLM agent  |  Saves output|
+-------+----------+----------+----------+-------------------+
        |          |          |          |
        docker exec -i, MCP stdio (CAPABILITY_ID=N, MCP_DOMAIN=<domain>)
        |          |          |          |
- - - - - - - - - - - - host / container boundary - - - - - -
        |          |          |          |
        v          v          v          v
CONTAINERS (image: m3_environ)
+------------------+ +------------------+ +------------------+ +---------------------+
| capability_1_bi_apis_m3_environ| | capability_2_dashboard_apis_m3_environ| | capability_3_multihop_reasoning_m3_environ| | capability_4_multiturn_m3_environ   |
|                  | |                  | |                  | |  (mem_limit: 4 GB)  |
| mcp_dispatch.py  | | mcp_dispatch.py  | | mcp_dispatch.py  | | mcp_dispatch.py     |
|  (CAPABILITY_ID=1)     | |  (CAPABILITY_ID=2)     | |  (CAPABILITY_ID=3)     | |  (CAPABILITY_ID=5)        |
|  os.execv ->     | |  os.execv ->     | |  os.execv ->     | |  os.execv ->        |
| python_tools.mcp | | m3-rest/         | | bpo/             | | capability_4_mcp_server    |
| (Sel/Slot router)| | mcp_server.py    | | bpo_router.py    | | (M3 REST+Retriever)        |
|       |          | |       |          | |       |          | |     |         |     |
|   SQLite /db/    | | FastAPI :8000    | | FastAPI :8000    | | FastAPI   FastAPI   |
|                  | |       |          | |       |          | |  :8000     :8001    |
|                  | |   SQLite /db/    | |   SQLite /db/    | |   |           |    |
|                  | |                  | |                  | | SQLite    ChromaDB  |
+------------------+ +------------------+ +------------------+ +---------------------+
```

## Unified Docker Image

All four task containers (`capability_1_bi_apis_m3_environ` through `capability_4_multiturn_m3_environ`) run the same `m3_environ` image. The image starts two internal FastAPI services at boot (port 8000 and optionally 8001). MCP servers are launched on-demand via `docker exec`, routed by the `CAPABILITY_ID` environment variable through `/app/mcp_dispatch.py`.

| Task | Container | MCP Server |
|------|-----------|-----------|
| 1 | `capability_1_bi_apis_m3_environ` | `python -m apis.m3.python_tools.mcp` (Slot-fill/Selection router) |
| 2 | `capability_2_dashboard_apis_m3_environ` | `python /app/m3-rest/mcp_server.py` (M3 REST wrapper) |
| 3 | `capability_3_multihop_reasoning_m3_environ` | `python /app/environment/bpo/mcp/bpo_router.py` (BPO ↔ M3 REST router) |
| 4 | `capability_4_multiturn_m3_environ` | `python /app/retrievers/capability_4_mcp_server.py` (M3 REST + Retriever combined) |

All task containers are configured via `benchmark/mcp_connection_config.yaml` with `container_command: [python, /app/mcp_dispatch.py]` and the appropriate `CAPABILITY_ID`.

## Internal Services

### 1. M3 Tools Service (port 8000)

Database query tools for 60+ domains (hockey, airline, financial, etc.)

```
+-----------------------------------------------------------+
|          Container: task_1/2/3/5_m3_environ (shared)      |
|                                                           |
|  +-------------------+       +-------------------------+  |
|  |    MCP Server     | stdio  |     FastAPI Server      |  |
|  |  (mcp_server.py)  |<----->|       (app.py)          |  |
|  |                   |       |                         |  |
|  | - Fetches OpenAPI |  HTTP | - 60+ domain routers   |  |
|  |   spec on start   | :8000 | - GET endpoints for     |  |
|  | - Converts to     |       |   database queries      |  |
|  |   MCP tools       |       | - Prometheus metrics    |  |
|  | - MCP_DOMAIN env  |       |                         |  |
|  |   filters tools   |       |                         |  |
|  +-------------------+       +-----------+-------------+  |
|                                          |                |
|                              +-----------+-----------+    |
|                              |   SQLite Databases    |    |
|                              |  /db/hockey.sqlite    |    |
|                              |  /db/airline.sqlite   |    |
|                              |  /db/financial.sqlite |    |
|                              |  /db/...              |    |
|                              +-----------------------+    |
+-----------------------------------------------------------+
```

**Tools exposed:** Domain-specific SQL query wrappers
- `get_goalies_by_minutes_played(min_minutes)`
- `flight_count_by_date(date)`
- `get_movie_rating(title)` ...etc.

---

### 2. Retrievers Service (port 8001)

Semantic search (RAG) over 62 domain document collections. Only started when `chroma_data/` is mounted (Task 5 container only).

```
+-----------------------------------------------------------+
|               Container: capability_4_multiturn_m3_environ only            |
|                                                           |
|  +-------------------+       +-------------------------+  |
|  |    MCP Server     | stdio  |     FastAPI Server      |  |
|  |  (mcp_server.py)  |<----->|      (server.py)        |  |
|  |                   |       |                         |  |
|  | - Fetches OpenAPI |  HTTP | - /{domain}/query       |  |
|  |   spec on start   | :8001 | - /{domain}/chunks      |  |
|  | - Resolves $ref   |       | - /domains              |  |
|  |   in schemas      |       | - /health               |  |
|  | - Creates per-    |       |                         |  |
|  |   domain tools    |       | - Granite embeddings    |  |
|  |   (query_address, |       |   (ibm-granite/granite- |  |
|  |    query_hockey)  |       |    embedding-english-r2)|  |
|  +-------------------+       +-----------+-------------+  |
|                                          |                |
|                              +-----------+-----------+    |
|                              |      ChromaDB         |    |
|                              |  (PersistentClient)   |    |
|                              |                       |    |
|                              |  Collections:         |    |
|                              |   address (12k docs)  |    |
|                              |   hockey  (8k docs)   |    |
|                              |   ...62 domains       |    |
|                              +-----------------------+    |
+-----------------------------------------------------------+
```

**Tools exposed:** Semantic search per domain
- `query_address(question, n_results)` -> ranked document chunks
- `query_hockey(question, n_results)` -> ranked document chunks ...etc.

---

## Data Flow

```
User Question
      |
      v
+------------------+
| Benchmark Runner |
| (LLM Agent)      |---+
+------------------+   |
                       |  "Which hockey player was born in 1990?"
                       |
         +-------------+-------------+
         |                           |
         v                           v
  +--------------+           +---------------+
  | M3 Tools MCP |           | Retriever MCP |
  | (stdio)      |           | (stdio)       |
  +--------------+           +---------------+
         |                           |
         v                           v
  +--------------+           +---------------+
  | FastAPI :8000|           | FastAPI :8001  |
  +--------------+           +---------------+
         |                           |
         v                           v
  +--------------+           +---------------+
  | SQLite DB    |           | ChromaDB      |
  | (exact query)|           | (vector search)|
  +--------------+           +---------------+
         |                           |
         v                           v
  Structured rows              Ranked documents
  (name, birth_year,           (text chunks with
   country, ...)               similarity scores)
```

## MCP Dispatcher

All tasks use a single dispatcher entrypoint (`/app/mcp_dispatch.py`) inside the container. It reads `CAPABILITY_ID` and `os.execv()`s into the appropriate server — zero proxy overhead.

```
docker exec -i -e CAPABILITY_ID=2 -e MCP_DOMAIN=hockey capability_2_dashboard_apis_m3_environ python /app/mcp_dispatch.py
                                                                                    │
                                                                     reads CAPABILITY_ID, exec's:
                                                                     python /app/m3-rest/mcp_server.py
```

The original server scripts remain intact and can be called directly (e.g. in smoke tests).

## Shared MCP Pattern

Both FastAPI-backed services follow the same architecture:

1. **FastAPI** serves domain-specific REST endpoints
2. **MCP Server** wraps FastAPI by:
   - Fetching `/openapi.json` at startup
   - Converting endpoints to MCP tools
   - Filtering by `MCP_DOMAIN` env var
   - Proxying tool calls as HTTP requests
3. **Docker** runs both in a single unified container (FastAPI starts first at boot, MCP is spawned on-demand via `docker exec`)
4. **Benchmark Runner** connects via `docker exec -i` using MCP stdio protocol, with `CAPABILITY_ID` routing through `mcp_dispatch.py`

## Testing

Unified test runner for retrievers (`apis/retrievers/test_queries.py`):

```
python test_queries.py --mode fastapi address    # Direct HTTP to FastAPI
python test_queries.py --mode mcp address        # In-process MCP -> FastAPI
python test_queries.py --mode docker address     # MCP over stdio in container
```
