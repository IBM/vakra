"""Real MCP HTTP -> persistent stdio round trips, without hosted data or sockets."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import sys

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'evaluator'))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'docker'))
from mcp_tools import replay_paired_tools
from benchmark.mcp_client import MCPConnectionConfig


def make_batches():
    predictions, truths = [], []
    for uuid in ('sample-a', 'sample-b'):
        init = {'name': 'get_data', 'arguments': {'tool_universe_id': uuid, 'reset_handles': True}, 'label': 'initial'}
        predictions.append([[init,
            {'name': 'transform_data_to_absolute_value', 'arguments': {'data_label': 'retrieved_data_1', 'key_name': 'value'}},
            {'name': 'get_values', 'arguments': {'data_label': 'transformed_data_1'}},
        ]])
        truths.append([[init,
            {'name': 'get_values', 'arguments': {'data_label': '$initial$'}, 'label': 'values'},
            {'name': 'get_loads', 'arguments': {'data_label': '$initial$'}},
        ]])
    return predictions, truths


def test_http_and_local_replay_have_identical_results(monkeypatch):
    import mcp_http_bridge as bridge
    import benchmark.mcp_client as client
    import mcp.client.streamable_http as transport
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

    fixture = str(Path(__file__).resolve().parents[1] / 'fixtures' / 'bi_backend.py')
    monkeypatch.setattr(bridge, 'DISPATCH', fixture)
    monkeypatch.setattr(bridge, 'workers', {})
    monkeypatch.setattr(bridge, 'manager', StreamableHTTPSessionManager(app=bridge.proxy, stateless=True, json_response=True))
    original_transport = transport.streamablehttp_client
    def factory(headers=None, timeout=None, auth=None):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), headers=headers,
                                 timeout=timeout or 30, auth=auth)
    def connection(url, headers, **kwargs):
        return original_transport(url, headers=headers, httpx_client_factory=factory)
    async def release(url, headers):
        async with factory(headers=headers) as session:
            response = await session.delete(url)
            assert response.status_code == 200
    monkeypatch.setattr(transport, 'streamablehttp_client', connection)
    monkeypatch.setattr(client, '_release_http_backend', release)

    async def run():
        pred, gt = make_batches()
        local = MCPConnectionConfig(command=sys.executable, args=[fixture])
        local_results = await replay_paired_tools(local, 'test', pred, gt, is_bi=True)
        remote = MCPConnectionConfig(mode='http', server_url='http://localhost/mcp/{domain}')
        async with bridge.manager.run():
            remote_results = await replay_paired_tools(remote, 'test', pred, gt, is_bi=True)
        assert not bridge.workers
        assert remote_results == local_results
        preds, truths = remote_results
        assert json.loads(preds[0][0][-1]) == [2, 3]
        assert json.loads(truths[0][0][1]) == [-2, 3]
        assert json.loads(truths[0][0][2]) == 1  # initial universe loaded once
        assert json.loads(truths[1][0][1]) == [-5, 7]
        assert json.loads(truths[1][0][2]) == 2  # default + requested universe
    asyncio.run(run())


def test_workers_keep_stdio_in_owner_task_and_expire(monkeypatch):
    import mcp_http_bridge as bridge
    closed = []
    class Session:
        async def list_tools(self):
            return asyncio.current_task()
    @asynccontextmanager
    async def backend(domain):
        owner = asyncio.current_task()
        try:
            yield Session()
        finally:
            assert asyncio.current_task() is owner
            closed.append(domain)
    monkeypatch.setattr(bridge, 'spawn_backend', backend)
    monkeypatch.setattr(bridge, 'IDLE_TIMEOUT', .03)
    async def run():
        worker = bridge.BackendWorker('test')
        first = await worker.request('list_tools')
        assert await worker.request('list_tools') is first
        await asyncio.wait_for(worker.task, 1)
        with pytest.raises(RuntimeError, match='expired'):
            await worker.request('list_tools')
        assert closed == ['test']
    asyncio.run(run())


def test_workers_isolate_simultaneous_client_connections(monkeypatch):
    import mcp_http_bridge as bridge
    closed = []
    class Session:
        def __init__(self): self.handles = []
        async def call_tool(self, name, value):
            if name == 'add': self.handles.append(value)
            return list(self.handles)
    @asynccontextmanager
    async def backend(domain):
        try:
            yield Session()
        finally:
            closed.append(domain)
    monkeypatch.setattr(bridge, 'spawn_backend', backend)
    async def run():
        left, right = bridge.BackendWorker('same-domain'), bridge.BackendWorker('same-domain')
        try:
            assert await left.request('call_tool', 'add', 'prediction') == ['prediction']
            assert await right.request('call_tool', 'read', None) == []
            assert await left.request('call_tool', 'read', None) == ['prediction']
        finally:
            await left.close()
            await right.close()
        assert len(closed) == 2
    asyncio.run(run())


def test_unknown_session_and_domain_mismatch_fail_without_new_backend(monkeypatch):
    import mcp_http_bridge as bridge
    monkeypatch.setattr(bridge, 'workers', {'a' * 32: bridge.BackendWorker('first-domain')})
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://localhost') as client:
            result = await client.post('/mcp/test', headers={'X-Vakra-Session': 'b' * 32},
                                      json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'})
            assert result.status_code == 409
            result = await client.post('/mcp/test', headers={'X-Vakra-Session': 'a' * 32}, json={})
            assert result.status_code == 409
            assert len(bridge.workers) == 1
    asyncio.run(run())
