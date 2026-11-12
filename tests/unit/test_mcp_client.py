import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmark.mcp_client import (
    MCPConnectionConfig, load_mcp_config, resolve_http_url,
    select_evaluator_mcp_config, _http_session,
)


def test_backend_selection_and_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv('VAKRA_CAP2_URL', 'https://cap2.example')
    cfg = select_evaluator_mcp_config(2, 'code-engine')
    assert resolve_http_url(cfg, 'california_schools') == 'https://cap2.example/mcp/california_schools'
    assert select_evaluator_mcp_config(2).mode == 'stdio'
    monkeypatch.delenv('VAKRA_CAP2_URL')
    with pytest.raises(ValueError, match='VAKRA_CAPn_URL'):
        select_evaluator_mcp_config(2, 'code-engine')
    config = tmp_path / 'custom.yaml'
    config.write_text('tasks:\n  2:\n    mode: http\n    server_url: https://override.example/mcp/{domain}\n')
    assert select_evaluator_mcp_config(2, 'code-engine', str(config)).server_url.startswith('https://override')
    with pytest.raises(ValueError, match='capability 1'):
        select_evaluator_mcp_config(1, 'code-engine', str(config))
    with pytest.raises(ValueError, match='requires an HTTP'):
        select_evaluator_mcp_config(1, 'code-engine', 'benchmark/mcp_connection_config.yaml')


@pytest.mark.parametrize('mode', ['http', 'streamable-http'])
def test_http_url_validation(mode):
    assert resolve_http_url(MCPConnectionConfig(mode=mode, server_url='https://example.org'), 'a/b') == 'https://example.org/mcp/a%2Fb'
    with pytest.raises(ValueError, match='absolute HTTP'):
        resolve_http_url(MCPConnectionConfig(server_url='ftp://example.org'), 'test')


def test_remote_cleanup_on_tool_failure(monkeypatch):
    import benchmark.mcp_client as client
    import mcp.client.streamable_http as transport
    seen = []
    @asynccontextmanager
    async def connection(url, headers, **kwargs):
        seen.append(headers)
        yield None, None, lambda: None
    class Session:
        def __init__(self, *args): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def initialize(self): pass
    async def release(url, headers): seen.append(headers)
    monkeypatch.setattr(transport, 'streamablehttp_client', connection, raising=False)
    monkeypatch.setattr(client, 'ClientSession', Session)
    monkeypatch.setattr(client, '_release_http_backend', release)
    async def run():
        with pytest.raises(RuntimeError, match='deliberate'):
            async with _http_session('https://example.org/mcp/test', 'test'):
                raise RuntimeError('deliberate')
    asyncio.run(run())
    assert seen[0] == seen[1]
    assert len(seen[0]['X-Vakra-Session']) == 32


def test_capability_one_requires_a_persistent_remote_bridge(monkeypatch):
    import httpx
    monkeypatch.setenv('VAKRA_CAP1_URL', 'https://cap1.example')
    assert select_evaluator_mcp_config(1, 'code-engine').require_persistent_backend
    assert not select_evaluator_mcp_config(1).require_persistent_backend
    class Client:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, url):
            assert url == 'https://cap1.example/healthz'
            return httpx.Response(200, json={'status': 'ok'}, request=httpx.Request('GET', url))
    monkeypatch.setattr(httpx, 'AsyncClient', Client)
    async def run():
        with pytest.raises(RuntimeError, match='session-preserving'):
            async with _http_session('https://cap1.example/mcp/test', 'test', require_persistent_backend=True):
                pytest.fail('Old stateless bridge must not be used')
    asyncio.run(run())
