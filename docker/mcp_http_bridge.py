#!/usr/bin/env python3
"""
mcp_http_bridge.py — stdio → streamable-HTTP MCP bridge (OPT-IN, Code Engine only).

Why this exists
---------------
Locally, benchmark clients reach the MCP servers via `docker exec -i -e MCP_DOMAIN=<d>
<container> python /app/mcp_dispatch.py` — i.e. MCP over **stdio**, with the domain
injected as an env var into a freshly-spawned server process. Code Engine has no
`docker exec`, so the servers must be reachable over **HTTP**.

This bridge fronts the *unchanged* `mcp_dispatch.py`. It reproduces the exact
per-domain tool scoping:

    HTTP client connects to   ->  https://<app>/mcp/<domain>
    bridge spawns             ->  python /app/mcp_dispatch.py   (env MCP_DOMAIN=<domain>,
                                                                 CAPABILITY_ID inherited)
    bridge proxies            ->  initialize / list_tools / call_tool

A client that connects to /mcp/address gets ONLY the address tools — the subprocess
behind that path was launched with a single MCP_DOMAIN, so list_tools() is scoped
exactly as it is under docker exec. `verify_checksum(capability_id, domain, tools)`
on the client passes unchanged. No MCP server file is modified.

Only runs when the container is started with SERVE_MODE=http (see entrypoint-unified.sh).
The default (docker-exec / local) path is untouched.

Requires: mcp>=1.9 (StreamableHTTPSessionManager), starlette, uvicorn — all in the image.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
import time
from contextvars import ContextVar
from typing import Dict, Optional

import uvicorn
from starlette.types import Receive, Scope, Send

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.shared.exceptions import McpError
from mcp.types import CONNECTION_CLOSED
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

logging.basicConfig(
    level=os.environ.get("BRIDGE_LOG_LEVEL", "INFO"),
    format="%(asctime)s bridge[%(levelname)s] %(message)s",
)
log = logging.getLogger("mcp_http_bridge")

CAPABILITY_ID = os.environ.get("CAPABILITY_ID", "").strip()
# Overridable for local testing (point at a dummy stdio MCP server).
DISPATCH = os.environ.get("BRIDGE_DISPATCH", "/app/mcp_dispatch.py")

# Domain for the request currently being handled. Set by the ASGI wrapper before
# delegating to the session manager; read by the proxied list_tools/call_tool
# handlers. anyio copies the current context into the per-request task the session
# manager spawns, so the value propagates correctly.
_current_domain: ContextVar[str] = ContextVar("current_domain", default="")


# ---------------------------------------------------------------------------
# Downstream backend: a fresh stdio subprocess (mcp_dispatch.py) pinned to a
# single MCP_DOMAIN, created AND torn down within the calling request's own task
# scope. Used for one-off tool listing and when BRIDGE_PERSISTENT=0. (Entering a
# ClientSession in one task and exiting it in another trips anyio's cancel-scope
# bookkeeping -- "attempted to exit a cancel scope that isn't the current task's" --
# which is why the persistent backends below have a dedicated owner task.)
# ---------------------------------------------------------------------------
# Upper bound on concurrently running backend subprocesses. Each one is a full Python
# interpreter; without a cap a burst of parallel clients becomes a fork storm.
# Requests beyond the cap wait for a slot instead of failing. 0 disables the cap.
MAX_BACKENDS = int(os.environ.get("BRIDGE_MAX_BACKENDS", "32"))
_backend_slots: Optional[asyncio.Semaphore] = (
    asyncio.Semaphore(MAX_BACKENDS) if MAX_BACKENDS > 0 else None
)


@contextlib.asynccontextmanager
async def spawn_backend(domain: str):
    env = os.environ.copy()
    if domain:
        env["MCP_DOMAIN"] = domain
    params = StdioServerParameters(command=sys.executable, args=[DISPATCH], env=env)
    slot = _backend_slots if _backend_slots is not None else contextlib.nullcontext()
    async with slot:
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as sess:
                await sess.initialize()
                yield sess


# ---------------------------------------------------------------------------
# Per-domain tool-list cache.
#
# A domain's tool list is fixed for the life of the container, so it is fetched from
# a backend once and then served from memory. This matters twice over:
#   * client list_tools requests no longer spawn a subprocess, and
#   * the MCP SDK's call_tool wrapper (mcp>=1.10) looks the tool up in the Server's
#     _tool_cache and, on a miss, re-runs the list_tools handler. That cache is shared
#     by every domain and each refresh *replaces* it, so with N domains in parallel
#     almost every call_tool missed and spawned an extra backend just to re-list tools
#     (observed: ~4,900 hidden list_tools spawns for ~6,000 tool calls). With this
#     cache the refresh is a dict lookup.
# ---------------------------------------------------------------------------
_tools_by_domain: Dict[str, list] = {}
_tools_locks: Dict[str, asyncio.Lock] = {}


async def _domain_tools(domain: str) -> list:
    tools = _tools_by_domain.get(domain)
    if tools is not None:
        return tools
    lock = _tools_locks.setdefault(domain, asyncio.Lock())
    async with lock:  # concurrent cold requests for one domain share a single spawn
        tools = _tools_by_domain.get(domain)
        if tools is None:
            async with spawn_backend(domain) as sess:
                tools = (await sess.list_tools()).tools
            _tools_by_domain[domain] = tools
            log.info("list_tools domain=%r -> %d tools (cached)", domain, len(tools))
        return tools


# ---------------------------------------------------------------------------
# Persistent per-domain backends (default; BRIDGE_PERSISTENT=0 to disable).
#
# Spawning a backend per tool call costs a Python start-up, the imports, and
# building the domain's tool table from the multi-MB spec -- seconds of CPU per call,
# which caps throughput once many domains run in parallel. Instead each domain gets
# one long-lived subprocess that serves all its calls concurrently (MCP multiplexes
# requests by id).
#
# The anyio pitfall noted above only bites when a session's context managers are
# entered in one task and exited in another. Here a dedicated owner task enters
# them, parks until the backend is idle or dies, and exits them; request tasks only
# send requests over the open session, which is safe from any task.
# ---------------------------------------------------------------------------
PERSISTENT = os.environ.get("BRIDGE_PERSISTENT", "1") == "1"
IDLE_SECONDS = float(os.environ.get("BRIDGE_IDLE_SECONDS", "900"))
# Cap on live persistent backends (~60 MB each). Past the cap, the least-recently-used
# idle backend is stopped; if every backend is busy, the call runs on a one-off
# spawned backend instead. Size it to the container's memory (0 = no cap).
MAX_PERSISTENT = int(os.environ.get("BRIDGE_MAX_PERSISTENT", "64"))


class DomainBackend:
    def __init__(self, domain: str):
        self.domain = domain
        self.session: Optional[ClientSession] = None
        self.ready = asyncio.Event()
        self.dead = asyncio.Event()      # set when the subprocess connection is gone
        self.stopping = False            # set by LRU eviction
        self.wake = asyncio.Event()      # wakes the owner task (dead or stopping)
        self.error: Optional[BaseException] = None
        self.inflight = 0
        self.last_used = time.monotonic()
        self.task = asyncio.create_task(self._own(), name=f"backend:{domain}")

    @property
    def alive(self) -> bool:
        return (self.session is not None and not self.dead.is_set()
                and not self.stopping and not self.task.done())

    def stop(self) -> None:
        self.stopping = True
        self.wake.set()

    async def _own(self) -> None:
        env = os.environ.copy()
        if self.domain:
            env["MCP_DOMAIN"] = self.domain
        params = StdioServerParameters(command=sys.executable, args=[DISPATCH], env=env)
        try:
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as sess:
                    await sess.initialize()
                    self.session = sess
                    self.ready.set()
                    log.info("backend up: domain=%r", self.domain)
                    while True:
                        with contextlib.suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(self.wake.wait(), timeout=min(60.0, IDLE_SECONDS))
                        if self.dead.is_set():
                            log.warning("backend connection lost: domain=%r", self.domain)
                            break
                        if self.stopping:
                            if self.inflight == 0:
                                log.info("backend evicted (LRU): domain=%r", self.domain)
                                break
                            self.wake.clear()
                            await asyncio.sleep(1)  # let in-flight calls finish
                            self.wake.set()
                            continue
                        idle = time.monotonic() - self.last_used
                        if self.inflight == 0 and idle >= IDLE_SECONDS:
                            log.info("backend idle %.0fs, stopping: domain=%r", idle, self.domain)
                            break
        except Exception as e:  # noqa: BLE001
            self.error = e
            log.warning("backend for domain=%r exited: %r", self.domain, e)
        finally:
            self.session = None
            self.dead.set()
            self.wake.set()
            self.ready.set()
            if _backends.get(self.domain) is self:
                del _backends[self.domain]

    async def call_tool(self, name: str, arguments: dict):
        await self.ready.wait()
        if self.session is None:
            raise RuntimeError(f"backend for {self.domain!r} failed to start: {self.error!r}")
        self.inflight += 1
        self.last_used = time.monotonic()
        try:
            return await self.session.call_tool(name, arguments)
        except McpError as e:
            if e.error.code == CONNECTION_CLOSED:
                self.dead.set()
                self.wake.set()  # subprocess exited; owner task tears down, next call respawns
            raise
        finally:
            self.inflight -= 1
            self.last_used = time.monotonic()


_backends: Dict[str, DomainBackend] = {}


def _backend(domain: str) -> Optional[DomainBackend]:
    """The domain's live backend, starting one if needed; None if at capacity."""
    b = _backends.get(domain)
    if b is not None and (b.alive or not b.ready.is_set()) and not b.stopping:
        return b
    live = [x for x in _backends.values() if x is not b and not x.stopping]
    if MAX_PERSISTENT > 0 and len(live) >= MAX_PERSISTENT:
        idle = [x for x in live if x.inflight == 0 and x.ready.is_set()]
        if not idle:
            return None
        victim = min(idle, key=lambda x: x.last_used)
        victim.stop()
        del _backends[victim.domain]
    b = _backends[domain] = DomainBackend(domain)
    return b


async def _spawned_call(domain: str, name: str, arguments: dict):
    async with spawn_backend(domain) as sess:
        return await sess.call_tool(name, arguments)


async def _persistent_call(domain: str, name: str, arguments: dict):
    b = _backend(domain)
    if b is None:  # every persistent slot is busy: one-off backend for this call
        return await _spawned_call(domain, name, arguments)
    try:
        return await b.call_tool(name, arguments)
    except Exception:
        # The backend process died under us (crash, OOM, ...): start a fresh one and
        # retry once. A tool that merely fails returns isError content, not an
        # exception, so it is never retried here.
        if b.alive:
            raise
        log.warning("retrying %s on a fresh backend: domain=%r", name, domain)
        nb = _backend(domain)
        if nb is None:
            return await _spawned_call(domain, name, arguments)
        return await nb.call_tool(name, arguments)


# ---------------------------------------------------------------------------
# The single low-level proxy Server. Its handlers forward each request to the
# domain's backend (domain from the contextvar set per request).
# ---------------------------------------------------------------------------
proxy = Server(f"vakra-capability-{CAPABILITY_ID or '?'}")


@proxy.list_tools()
async def _list_tools():
    return await _domain_tools(_current_domain.get())


# validate_input=False: the SDK validates arguments against Server._tool_cache, which
# is keyed by tool name and shared by every domain this proxy serves. Dozens of tool
# names exist in several domains with different schemas (e.g. get_customer_names:
# books needs last_name_pattern, movie needs active+limit), so a valid call could be
# rejected -- or an invalid one accepted -- against another domain's schema. Each
# backend is pinned to its domain and validates against the right schema itself.
@proxy.call_tool(validate_input=False)
async def _call_tool(name: str, arguments: dict):
    domain = _current_domain.get()
    if PERSISTENT:
        result = await _persistent_call(domain, name, arguments or {})
    else:
        result = await _spawned_call(domain, name, arguments or {})
    if result.isError:
        # Keep the error flag: raising makes the SDK return isError=True with this text.
        raise RuntimeError("\n".join(
            getattr(c, "text", "") for c in result.content if getattr(c, "type", "") == "text"
        ) or f"tool {name} failed")
    # Forward the downstream content blocks verbatim.
    return result.content


session_manager = StreamableHTTPSessionManager(
    app=proxy,
    event_store=None,
    json_response=True,   # plain JSON responses — no SSE plumbing needed for the benchmark
    stateless=True,       # each request self-contained; domain is re-derived from the path
)


# ---------------------------------------------------------------------------
# Minimal ASGI app: /healthz + /mcp/<domain>/...  (domain -> contextvar -> manager)
# ---------------------------------------------------------------------------
async def _send_json(send: Send, status: int, body: bytes) -> None:
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [(b"content-type", b"application/json")],
    })
    await send({"type": "http.response.body", "body": body})


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] == "lifespan":
        # Keep the session manager's task group alive for the whole app lifetime,
        # and tear down any spawned subprocesses on shutdown.
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                try:
                    cm = session_manager.run()
                    await cm.__aenter__()
                    scope.setdefault("state", {})["_mgr_cm"] = cm
                    await send({"type": "lifespan.startup.complete"})
                except Exception as e:  # noqa: BLE001
                    await send({"type": "lifespan.startup.failed", "message": str(e)})
            elif message["type"] == "lifespan.shutdown":
                cm = scope.get("state", {}).get("_mgr_cm")
                if cm is not None:
                    with contextlib.suppress(Exception):
                        await cm.__aexit__(None, None, None)
                await send({"type": "lifespan.shutdown.complete"})
                return

    if scope["type"] != "http":
        return

    path = scope.get("path", "")

    if path in ("/healthz", "/health"):
        import json as _json
        body = _json.dumps({
            "status": "ok",
            "capability_id": CAPABILITY_ID,
        }).encode()
        await _send_json(send, 200, body)
        return

    # Expect /mcp/<domain>[/...]
    parts = [p for p in path.split("/") if p != ""]
    if len(parts) >= 2 and parts[0] == "mcp":
        domain = parts[1]
        token = _current_domain.set(domain)
        try:
            await session_manager.handle_request(scope, receive, send)
        finally:
            _current_domain.reset(token)
        return

    import json as _json
    await _send_json(
        send, 404,
        _json.dumps({
            "error": "not found",
            "hint": "connect to /mcp/<domain> (e.g. /mcp/address); health at /healthz",
        }).encode(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="VAKRA MCP stdio->HTTP bridge")
    parser.add_argument("--port", type=int, default=int(os.environ.get("BRIDGE_PORT", "8080")))
    parser.add_argument("--host", type=str, default=os.environ.get("BRIDGE_HOST", "0.0.0.0"))
    args = parser.parse_args()

    if CAPABILITY_ID not in {"1", "2", "3", "4"}:
        log.error("CAPABILITY_ID must be one of 1..4; got %r", CAPABILITY_ID)
        sys.exit(1)

    log.info("MCP HTTP bridge starting: capability=%s on %s:%d  (endpoint: /mcp/<domain>)",
             CAPABILITY_ID, args.host, args.port)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
