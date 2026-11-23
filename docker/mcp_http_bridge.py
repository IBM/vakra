#!/usr/bin/env python3
"""Domain-scoped MCP HTTP bridge with private, persistent backend workers.

X-Vakra-Session identifies a client connection. The evaluator generates a random
identifier and releases it with DELETE. Each worker owns its stdio context for
its entire lifetime in ONE asyncio task; requests are serialized through a queue.
Unmodified clients without the header retain the CE bridge's per-call behavior.
Run one replica: workers are process-local and must not migrate between replicas.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
import json
import logging
import os
import re
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
import uvicorn

log = logging.getLogger(__name__)
CAPABILITY_ID = os.environ.get("CAPABILITY_ID", "")
DISPATCH = os.environ.get("BRIDGE_DISPATCH", "/app/mcp_dispatch.py")
IDLE_TIMEOUT = float(os.environ.get("BRIDGE_IDLE_TIMEOUT", "900"))
MAX_SESSIONS = int(os.environ.get("BRIDGE_MAX_SESSIONS", "16"))
_domain = ContextVar("domain", default="")
_worker = ContextVar("worker", default=None)


@asynccontextmanager
async def spawn_backend(domain):
    env = dict(os.environ, MCP_DOMAIN=domain)
    params = StdioServerParameters(command=sys.executable, args=[DISPATCH], env=env)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


class BackendWorker:
    def __init__(self, domain):
        self.domain = domain
        self.queue = asyncio.Queue()
        self.task = None

    async def request(self, method, *args):
        if self.task is None:
            self.task = asyncio.create_task(self.run())
        if self.task.done():
            raise RuntimeError("Backend expired; start a new client session")
        future = asyncio.get_running_loop().create_future()
        self.queue.put_nowait((method, args, future))
        return await future

    async def run(self):
        failure = RuntimeError("Backend session closed or expired")
        pending = None
        try:
            async with spawn_backend(self.domain) as session:
                while True:
                    try:
                        method, args, pending = await asyncio.wait_for(self.queue.get(), IDLE_TIMEOUT)
                    except asyncio.TimeoutError:
                        return
                    if pending.cancelled():
                        pending = None
                        continue
                    try:
                        result = await getattr(session, method)(*args)
                    except Exception as exc:
                        if not pending.done():
                            pending.set_exception(exc)
                    else:
                        if not pending.done():
                            pending.set_result(result)
                    pending = None
        except Exception as exc:
            failure = RuntimeError(f"Could not run backend: {exc}")
            log.exception("Backend failed for %s", self.domain)
        finally:
            if pending is not None and not pending.done():
                pending.set_exception(failure)
            while not self.queue.empty():
                _, _, future = self.queue.get_nowait()
                if not future.done():
                    future.set_exception(failure)

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task


workers = {}
proxy = Server(f"vakra-capability-{CAPABILITY_ID or 'unknown'}")


async def forward(method, *args):
    worker = _worker.get()
    if worker is not None:
        return await worker.request(method, *args)
    # Compatibility for stateless explorer clients. Stateful capability 1
    # clients must supply X-Vakra-Session and use this updated deployment.
    async with spawn_backend(_domain.get()) as session:
        return await getattr(session, method)(*args)


@proxy.list_tools()
async def list_tools():
    return (await forward("list_tools")).tools


@proxy.call_tool(validate_input=False)
async def call_tool(name, arguments):
    # Preserve isError and structuredContent, as well as text blocks.
    return await forward("call_tool", name, arguments or {})


manager = StreamableHTTPSessionManager(app=proxy, stateless=True, json_response=True)


async def send_json(send, status, payload):
    await send({"type": "http.response.start", "status": status,
                "headers": [(b"content-type", b"application/json")]})
    await send({"type": "http.response.body", "body": json.dumps(payload).encode()})


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        # Enter and leave the manager context in the same owning task.
        async with manager.run():
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await asyncio.gather(*(worker.close() for worker in workers.values()))
            workers.clear()
            await send({"type": "lifespan.shutdown.complete"})
        return
    if scope["type"] != "http":
        return
    path = scope.get("path", "")
    if path in ("/health", "/healthz"):
        await send_json(send, 200, {"status": "ok", "capability_id": CAPABILITY_ID,
                                    "persistent_backends": True, "active_sessions": len(workers)})
        return
    parts = path.strip("/").split("/")
    if len(parts) != 2 or parts[0] != "mcp" or not re.fullmatch(r"[A-Za-z0-9_-]+", parts[1]):
        await send_json(send, 404, {"error": "Use /mcp/<domain>"})
        return
    domain = parts[1]
    headers = {k.lower(): v for k, v in scope.get("headers", [])}
    identifier = headers.get(b"x-vakra-session", b"").decode("ascii", errors="replace")
    worker = None
    if identifier:
        if not re.fullmatch(r"[a-f0-9]{32}", identifier):
            await send_json(send, 400, {"error": "Invalid X-Vakra-Session"})
            return
        if scope["method"] == "DELETE":
            worker = workers.get(identifier)
            if worker is not None and worker.domain != domain:
                await send_json(send, 409, {"error": "Session domain mismatch"})
                return
            if worker is not None:
                workers.pop(identifier)
                await worker.close()
            await send_json(send, 200, {"closed": True})
            return
        # Read/replay the body to recognize the initial MCP handshake. Unknown
        # identifiers on later calls fail explicitly (replica move / idle expiry).
        messages, body = [], bytearray()
        while True:
            message = await receive()
            messages.append(message)
            body.extend(message.get("body", b""))
            if len(body) > 4 * 1024 * 1024:
                await send_json(send, 413, {"error": "MCP request is too large"})
                return
            if message["type"] == "http.disconnect" or not message.get("more_body", False):
                break
        try:
            initialize = json.loads(body).get("method") == "initialize"
        except (ValueError, AttributeError):
            initialize = False
        worker = workers.get(identifier)
        if worker is not None and worker.task is not None and worker.task.done():
            workers.pop(identifier)
            worker = None
        if worker is None:
            if not initialize:
                await send_json(send, 409, {"error": "Backend session expired or reached another replica; restart the sample"})
                return
            # Reap idle workers before enforcing the limit.
            for key, value in list(workers.items()):
                if value.task is not None and value.task.done():
                    workers.pop(key)
            if len(workers) >= MAX_SESSIONS:
                await send_json(send, 503, {"error": "Backend session capacity reached"})
                return
            worker = BackendWorker(domain)
            worker.task = asyncio.create_task(worker.run())
            workers[identifier] = worker
        if worker.domain != domain:
            await send_json(send, 409, {"error": "Session domain mismatch"})
            return
        async def replay_receive():
            return messages.pop(0) if messages else await receive()
        request_receive = replay_receive
    else:
        request_receive = receive
    domain_token, worker_token = _domain.set(domain), _worker.set(worker)
    try:
        await manager.handle_request(scope, request_receive, send)
    finally:
        _domain.reset(domain_token)
        _worker.reset(worker_token)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    args = parser.parse_args()
    if CAPABILITY_ID not in {"1", "2", "3", "4"}:
        parser.error("CAPABILITY_ID must be 1, 2, 3 or 4")
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
