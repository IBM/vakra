"""Evaluator-only tool transports; benchmark and environment stay unchanged."""
from __future__ import annotations

import contextlib
from dataclasses import asdict, dataclass
import logging
import os
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlsplit

from mcp import ClientSession
from benchmark.mcp_client import MCPConnectionConfig, load_mcp_config
from benchmark.mcp_client import create_client_and_connect as connect_local

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONFIG = REPO_ROOT / 'benchmark' / 'mcp_connection_config.yaml'
CE_CONFIG = Path(__file__).with_name('mcp_connection_config.ce.yaml')
HTTP_MODES = ('http', 'streamable-http')


@dataclass
class EvaluatorMCPConnectionConfig(MCPConnectionConfig):
    capability_id: int = 0
    backend: str = 'local'
    requested_backend: str = 'local'


def _load(path, capability_id):
    configs = load_mcp_config(str(path))
    if capability_id not in configs:
        raise ValueError(f'No MCP configuration for capability {capability_id} in {path}')
    return configs[capability_id]


def _local_fallback(config_path=None):
    # A CE-only override may have no capability 1 entry, or reference an HTTP
    # endpoint with unset variables. Neither should prevent the local fallback.
    if config_path is not None:
        configs = load_mcp_config(str(config_path))
        candidate = configs.get(1)
        if candidate is not None and candidate.mode not in HTTP_MODES:
            return candidate
    return _load(LOCAL_CONFIG, 1)


def select_evaluator_mcp_config(capability_id: int, backend: str = 'local', config_path: Optional[str] = None):
    if backend not in ('local', 'code-engine'):
        raise ValueError(f'Unknown tool backend: {backend}')
    if capability_id == 1 and backend == 'code-engine':
        logger.warning(
            'Code Engine tool calling for capability 1 is not supported in this version; '
            'falling back to local execution. No Code Engine connection will be opened.'
        )
        cfg = _local_fallback(config_path)
    else:
        cfg = _load(config_path or (CE_CONFIG if backend == 'code-engine' else LOCAL_CONFIG), capability_id)
        # An explicit HTTP YAML must not bypass the capability 1 restriction.
        if capability_id == 1 and cfg.mode in HTTP_MODES:
            logger.warning(
                'Code Engine tool calling for capability 1 is not supported in this version; '
                'ignoring the HTTP configuration and falling back to local execution.'
            )
            cfg = _local_fallback()
        elif backend == 'code-engine' and cfg.mode not in HTTP_MODES:
            raise ValueError('code-engine requires an HTTP MCP configuration')
    effective = 'code-engine' if cfg.mode in HTTP_MODES else 'local'
    selected = EvaluatorMCPConnectionConfig(
        **asdict(cfg), capability_id=capability_id,
        backend=effective, requested_backend=backend,
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
async def _http_session(url, domain):
    import mcp.client.streamable_http as transport
    legacy = getattr(transport, 'streamablehttp_client', None)
    headers = {'X-MCP-Domain': domain}
    async with contextlib.AsyncExitStack() as stack:
        if legacy is not None:
            connection = legacy(url, headers=headers, timeout=600, sse_read_timeout=1800)
        else:
            # MCP 2.x changed the client constructor and HTTP client dependency.
            import httpx2
            client = await stack.enter_async_context(httpx2.AsyncClient(headers=headers, timeout=600))
            connection = transport.streamable_http_client(url, http_client=client)
        read, write, _ = await stack.enter_async_context(connection)
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        yield session


@contextlib.asynccontextmanager
async def create_client_and_connect(cfg, domain=''):
    if cfg.mode in HTTP_MODES and getattr(cfg, 'capability_id', None) == 1:
        # Protect programmatic callers too, not only CLI configuration selection.
        cfg = select_evaluator_mcp_config(1, 'code-engine')
    if cfg.mode in HTTP_MODES:
        async with _http_session(resolve_http_url(cfg, domain), domain) as session:
            yield session
    else:
        async with connect_local(cfg, domain) as session:
            yield session
