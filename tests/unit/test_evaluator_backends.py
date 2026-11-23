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


def test_capability_one_falls_back_without_remote_url(monkeypatch, caplog):
    monkeypatch.delenv('VAKRA_CAP1_URL', raising=False)
    cfg = backend.select_evaluator_mcp_config(1, 'code-engine')
    assert cfg.backend == 'local'
    assert cfg.requested_backend == 'code-engine'
    assert cfg.mode == 'stdio'
    assert cfg.server_url is None
    assert 'falling back to local execution' in caplog.text
    seen = []
    @asynccontextmanager
    async def local(config, domain):
        seen.append((config.backend, domain))
        yield 'local-session'
    @asynccontextmanager
    async def remote(*args, **kwargs):
        pytest.fail('fallback must not open HTTP')
        yield
    monkeypatch.setattr(backend, 'connect_local', local)
    monkeypatch.setattr(backend, '_http_session', remote)
    async def run():
        async with backend.create_client_and_connect(cfg, 'college_completion') as session:
            assert session == 'local-session'
    asyncio.run(run())
    assert seen == [('local', 'college_completion')]


def test_fallback_with_ce_override_missing_capability_one(caplog):
    cfg = backend.select_evaluator_mcp_config(1, 'code-engine', str(backend.CE_CONFIG))
    assert cfg.mode == 'stdio'
    assert cfg.backend == 'local'
    assert 'falling back' in caplog.text


def test_http_override_cannot_bypass_capability_one_fallback(tmp_path, caplog):
    path = tmp_path / 'http.yaml'
    path.write_text('tasks:\n  1:\n    mode: http\n    server_url: ${UNSET_CAP1_URL}/mcp/{domain}\n')
    for requested in ('local', 'code-engine'):
        cfg = backend.select_evaluator_mcp_config(1, requested, str(path))
        assert cfg.mode == 'stdio'
        assert cfg.backend == 'local'
    assert 'falling back' in caplog.text


def test_fallback_honors_custom_local_config(tmp_path):
    path = tmp_path / 'local.yaml'
    path.write_text('tasks:\n  1:\n    mode: stdio\n    command: python\n    args: [custom.py]\n')
    cfg = backend.select_evaluator_mcp_config(1, 'code-engine', str(path))
    assert cfg.backend == 'local'
    assert cfg.args == ['custom.py']


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


@pytest.mark.parametrize('modern', [False, True])
@pytest.mark.parametrize('stream_count', [2, 3])
def test_http_transport_and_cleanup(monkeypatch, modern, stream_count):
    import mcp.client.streamable_http as transport
    events = []
    read, write = object(), object()
    @asynccontextmanager
    async def client(**kwargs):
        assert kwargs['headers'] == {'X-MCP-Domain': 'test'}
        yield 'http-client'
    @asynccontextmanager
    async def connection(url, **kwargs):
        if modern:
            assert kwargs['http_client'] == 'http-client'
        else:
            assert kwargs['headers'] == {'X-MCP-Domain': 'test'}
        events.append('transport-open')
        try:
            yield (read, write) if stream_count == 2 else (read, write, lambda: None)
        finally:
            events.append('transport-close')
    class Session:
        def __init__(self, *args): assert args == (read, write)
        async def __aenter__(self): return self
        async def __aexit__(self, *args): events.append('session-close')
        async def initialize(self): events.append('initialize')
    if modern:
        monkeypatch.delattr(transport, 'streamablehttp_client', raising=False)
        monkeypatch.setattr(transport, 'streamable_http_client', connection, raising=False)
        monkeypatch.setitem(sys.modules, 'httpx2', SimpleNamespace(AsyncClient=client))
    else:
        monkeypatch.setattr(transport, 'streamablehttp_client', connection, raising=False)
    monkeypatch.setattr(backend, 'ClientSession', Session)
    async def run():
        with pytest.raises(RuntimeError, match='test failure'):
            async with backend._http_session('https://example.org/mcp/test', 'test'):
                raise RuntimeError('test failure')
    asyncio.run(run())
    assert events == ['transport-open', 'initialize', 'session-close', 'transport-close']


@pytest.fixture
def evaluator():
    path = Path(__file__).resolve().parents[2] / 'evaluator' / 'evaluator.py'
    spec = importlib.util.spec_from_file_location('split_evaluator', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('k', [1, 3])
def test_cli_records_local_backend_after_fallback(evaluator, monkeypatch, tmp_path, caplog, k):
    seen = {}
    monkeypatch.setattr(sys, 'argv', ['evaluator.py', '--capability_name', 'capability_bi_apis',
        '--gt_root', str(tmp_path), '--pred_root', str(tmp_path), '--tool-backend', 'code-engine', '--k', str(k)])
    def registry(k=1):
        seen['groundedness_k'] = k
        return {}
    monkeypatch.setattr(evaluator, 'build_default_capability_registry', registry)
    monkeypatch.setattr(evaluator, 'evaluate_capability', lambda **kwargs: seen.update(kwargs))
    evaluator.main()
    assert seen['groundedness_k'] == k
    assert seen['tool_backend'] == 'local'
    assert seen['mcp_config'].mode == 'stdio'
    assert 'falling back' in caplog.text


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
