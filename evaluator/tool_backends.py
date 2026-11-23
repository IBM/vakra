"""Evaluator-only tool transports; benchmark and environment stay unchanged."""
from __future__ import annotations

import contextlib
from dataclasses import asdict, dataclass
import logging
import os
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlsplit
from uuid import uuid4

from mcp import ClientSession
from benchmark.mcp_client import MCPConnectionConfig, load_mcp_config
from benchmark.mcp_client import create_client_and_connect as connect_local

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONFIG = REPO_ROOT / 'benchmark' / 'mcp_connection_config.yaml'
CE_CONFIG = REPO_ROOT / 'benchmark' / 'mcp_connection_config.ce.yaml'
HTTP_MODES = ('http', 'streamable-http')


@dataclass
class EvaluatorMCPConnectionConfig(MCPConnectionConfig):
    capability_id: int = 0
    backend: str = 'local'
    requested_backend: str = 'local'
    require_persistent_backend: bool = False


def _load(path, capability_id):
    configs = load_mcp_config(str(path))
    if capability_id not in configs:
        raise ValueError(f'No MCP configuration for capability {capability_id} in {path}')
    return configs[capability_id]


def select_evaluator_mcp_config(capability_id: int, backend: str = 'local', config_path: Optional[str] = None):
    if backend not in ('local', 'code-engine'):
        raise ValueError(f'Unknown tool backend: {backend}')
    cfg = _load(config_path or (CE_CONFIG if backend == 'code-engine' else LOCAL_CONFIG), capability_id)
    if backend == 'code-engine' and cfg.mode not in HTTP_MODES:
        raise ValueError('code-engine requires an HTTP MCP configuration')
    effective = 'code-engine' if cfg.mode in HTTP_MODES else 'local'
    selected = EvaluatorMCPConnectionConfig(
        **asdict(cfg), capability_id=capability_id,
        backend=effective, requested_backend=backend,
        require_persistent_backend=capability_id == 1 and cfg.mode in HTTP_MODES,
    )
    if selected.server_url:
        selected.server_url = os.path.expandvars(selected.server_url)
    if selected.mode in HTTP_MODES:
        resolve_http_url(selected, 'validation')
    return selected


def resolve_http_url(cfg, domain: str) -> str:
    if not cfg.server_url or '$' in cfg.server_url:
        raise ValueError('Set the selected VAKRA_CAPn_URL environment variable or provide server_url in --mcp-config')
    if not domain:
        raise ValueError('HTTP MCP connections require a domain')
    base = cfg.server_url
    url = (base.replace('{domain}', quote(domain, safe='')) if '{domain}' in base
           else f"{base.rstrip('/')}/mcp/{quote(domain, safe='')}")
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        raise ValueError('server_url must be an absolute HTTP(S) URL')
    return url


@contextlib.asynccontextmanager
async def _http_session(url: str, domain: str, *, require_persistent_backend: bool = False):
    """One private bridge backend for this connection; release it on close."""
    headers = {"X-MCP-Domain": domain, "X-Vakra-Session": uuid4().hex}
    if require_persistent_backend:
        import httpx
        health_url = url.split("/mcp/", 1)[0] + "/healthz"
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(health_url)
            response.raise_for_status()
            if response.json().get("persistent_backends") is not True:
                raise RuntimeError("Capability 1 needs the session-preserving Code Engine bridge; redeploy docker/mcp_http_bridge.py before evaluating")
    # MCP 1.x and 2.x expose different HTTP transport constructors.
    import mcp.client.streamable_http as transport
    legacy_client = getattr(transport, "streamablehttp_client", None)
    try:
        async with contextlib.AsyncExitStack() as stack:
            if legacy_client is not None:
                connection = legacy_client(url, headers=headers, timeout=600, sse_read_timeout=1800)
            else:
                import httpx2
                client = await stack.enter_async_context(httpx2.AsyncClient(headers=headers, timeout=600))
                connection = transport.streamable_http_client(url, http_client=client)
            read, write, _ = await stack.enter_async_context(connection)
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            yield session
    finally:
        await _release_http_backend(url, headers)


async def _release_http_backend(url: str, headers: dict) -> None:
    import httpx
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.delete(url, headers=headers)
    except Exception:
        logger.warning("Could not release remote backend; bridge idle expiry will clean it up", exc_info=True)


@contextlib.asynccontextmanager
async def create_client_and_connect(cfg, domain=''):
    if cfg.mode in HTTP_MODES:
        async with _http_session(resolve_http_url(cfg, domain), domain,
                                 require_persistent_backend=getattr(cfg, 'require_persistent_backend', False) or getattr(cfg, 'capability_id', None) == 1) as session:
            yield session
    else:
        async with connect_local(cfg, domain) as session:
            yield session
