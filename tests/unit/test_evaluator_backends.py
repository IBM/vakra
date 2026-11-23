import asyncio
from contextlib import asynccontextmanager
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'evaluator'))
import tool_backends as backend


@pytest.mark.parametrize('capability', [2, 3, 4])
def test_remote_selection(capability, monkeypatch):
    monkeypatch.setenv(f'VAKRA_CAP{capability}_URL', f'https://cap{capability}.example')
    cfg = backend.select_evaluator_mcp_config(capability, 'code-engine')
    assert cfg.backend == 'code-engine'
    assert cfg.capability_id == capability
    assert backend.resolve_http_url(cfg, 'test') == f'https://cap{capability}.example/mcp/test'
    monkeypatch.delenv(f'VAKRA_CAP{capability}_URL')
    with pytest.raises(ValueError, match='VAKRA_CAPn_URL'):
        backend.select_evaluator_mcp_config(capability, 'code-engine')










def test_explicit_remote_override_records_effective_backend(tmp_path):
    path = tmp_path / 'http.yaml'
    path.write_text('tasks:\n  2:\n    mode: http\n    server_url: https://override.example/mcp/{domain}\n')
    cfg = backend.select_evaluator_mcp_config(2, config_path=str(path))
    assert cfg.backend == 'code-engine'
    assert cfg.requested_backend == 'local'


def test_remote_rejects_stdio_config():
    with pytest.raises(ValueError, match='requires an HTTP'):
        backend.select_evaluator_mcp_config(2, 'code-engine', str(backend.LOCAL_CONFIG))


def test_unsupported_capability_is_configuration_error():
    with pytest.raises(ValueError, match='capability 5'):
        backend.select_evaluator_mcp_config(5, 'code-engine')


def test_http_transport_and_cleanup(monkeypatch):
    import mcp.client.streamable_http as transport
    events = []
    @asynccontextmanager
    async def connection(url, **kwargs):
        assert kwargs['headers']['X-MCP-Domain'] == 'test'
        assert len(kwargs['headers']['X-Vakra-Session']) == 32
        events.append('transport-open')
        try:
            yield None, None, lambda: None
        finally:
            events.append('transport-close')
    class Session:
        def __init__(self, *args): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): events.append('session-close')
        async def initialize(self): events.append('initialize')
    monkeypatch.setattr(transport, 'streamablehttp_client', connection, raising=False)
    monkeypatch.setattr(backend, 'ClientSession', Session)
    async def release(*args): events.append('release')
    monkeypatch.setattr(backend, '_release_http_backend', release)
    async def run():
        with pytest.raises(RuntimeError, match='test failure'):
            async with backend._http_session('https://example.org/mcp/test', 'test'):
                raise RuntimeError('test failure')
    asyncio.run(run())
    assert events == ['transport-open', 'initialize', 'session-close', 'transport-close', 'release']


@pytest.fixture
def evaluator():
    path = Path(__file__).resolve().parents[2] / 'evaluator' / 'evaluator.py'
    spec = importlib.util.spec_from_file_location('split_evaluator', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module





@pytest.mark.parametrize('previous', [None, {'backend': 'local', 'mode': 'stdio', 'server_url': None}])
def test_resume_rejects_mixed_backends(evaluator, tmp_path, previous):
    (tmp_path / 'domain.json').write_text('[]')
    results = {'domains': {}, 'groundedness_k': 1, 'metrics_version': 1}
    if previous is not None: results['tool_connection'] = previous
    output = tmp_path / 'results.txt'
    output.write_text(json.dumps(results))
    cfg = backend.EvaluatorMCPConnectionConfig(mode='http', server_url='https://example.org/mcp/{domain}', backend='code-engine')
    with pytest.raises(ValueError, match='backend'):
        evaluator.evaluate_capability('capability_dashboard_apis', tmp_path, tmp_path, output,
            {'capability_dashboard_apis': SimpleNamespace(groundedness_judge=SimpleNamespace(k=1))}, mcp_config=cfg, tool_backend=cfg.backend)


def test_capability_one_remote_is_enabled_on_stacked_branch(monkeypatch):
    monkeypatch.setenv('VAKRA_CAP1_URL', 'https://cap1.example')
    cfg = backend.select_evaluator_mcp_config(1, 'code-engine')
    assert cfg.backend == 'code-engine'
    assert cfg.require_persistent_backend
    assert not backend.select_evaluator_mcp_config(1).require_persistent_backend


def test_old_stateless_bridge_is_rejected(monkeypatch):
    import httpx
    class Client:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, url):
            return httpx.Response(200, json={'status':'ok'}, request=httpx.Request('GET', url))
    monkeypatch.setattr(httpx, 'AsyncClient', Client)
    async def run():
        with pytest.raises(RuntimeError, match='session-preserving'):
            async with backend._http_session('https://cap1.example/mcp/test', 'test', require_persistent_backend=True):
                pytest.fail('old bridge must be rejected')
    asyncio.run(run())


@pytest.mark.parametrize('k', [1, 3])
def test_cli_records_capability_one_remote_backend(evaluator, monkeypatch, tmp_path, k):
    seen = {}
    monkeypatch.setenv('VAKRA_CAP1_URL', 'https://cap1.example')
    monkeypatch.setattr(sys, 'argv', ['evaluator.py', '--capability_name', 'capability_bi_apis',
        '--gt_root', str(tmp_path), '--pred_root', str(tmp_path), '--tool-backend', 'code-engine', '--k', str(k)])
    def registry(k=1):
        seen['groundedness_k'] = k
        return {}
    monkeypatch.setattr(evaluator, 'build_default_capability_registry', registry)
    monkeypatch.setattr(evaluator, 'evaluate_capability', lambda **kwargs: seen.update(kwargs))
    evaluator.main()
    assert seen['groundedness_k'] == k
    assert seen['tool_backend'] == 'code-engine'
    assert seen['mcp_config'].require_persistent_backend
