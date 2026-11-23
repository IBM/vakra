"""Verify capabilities 2–4 use equivalent local/HTTP MCP client semantics."""
import asyncio
from pathlib import Path
import sys

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'evaluator'), str(ROOT / 'tests' / 'fixtures')]
import tool_backends as backend
from evaluator_echo_backend import make_server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager


@pytest.mark.parametrize('capability', [2, 3, 4])
def test_local_http_parity(capability, monkeypatch):
    import mcp.client.streamable_http as transport
    manager = StreamableHTTPSessionManager(app=make_server(), stateless=True, json_response=True)
    async def app(scope, receive, send):
        await manager.handle_request(scope, receive, send)
    original = transport.streamablehttp_client
    def factory(headers=None, timeout=None, auth=None):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), headers=headers,
                                 timeout=timeout or 30, auth=auth)
    def connection(url, **kwargs):
        return original(url, httpx_client_factory=factory, **kwargs)
    monkeypatch.setattr(transport, 'streamablehttp_client', connection)
    async def run():
        local = backend.EvaluatorMCPConnectionConfig(command=sys.executable,
            args=[str(ROOT / 'tests' / 'fixtures' / 'evaluator_echo_backend.py')], capability_id=capability)
        remote = backend.EvaluatorMCPConnectionConfig(mode='http', server_url='http://localhost/mcp/{domain}',
                                                     capability_id=capability, backend='code-engine')
        async with backend.create_client_and_connect(local, 'test') as session:
            result = await session.call_tool('echo', {'value': f'capability-{capability}'})
        async with manager.run():
            async with backend.create_client_and_connect(remote, 'test') as session:
                hosted = await session.call_tool('echo', {'value': f'capability-{capability}'})
        assert result.model_dump() == hosted.model_dump()
    asyncio.run(run())
