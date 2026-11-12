import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from benchmark.mcp_client import MCPConnectionConfig


@pytest.fixture
def evaluator_module():
    path = Path(__file__).resolve().parents[2] / 'evaluator' / 'evaluator.py'
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location('backend_evaluator', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_prepend_retains_initial_label_and_resets_handles(evaluator_module):
    batch = [[[{'name': 'get_data', 'arguments': {}, 'label': 'source'},
               {'name': 'get_values', 'arguments': {'data_label': '$source$'}}]]]
    output = evaluator_module._prepend_get_data_to_batch(batch, ['sample-a'], True)
    assert output[0][0][0] == {'name': 'get_data', 'label': 'source',
                                'arguments': {'tool_universe_id': 'sample-a', 'reset_handles': True}}
    assert batch[0][0][0]['arguments'] == {}


@pytest.mark.parametrize('old_connection', [None, {'backend': 'local', 'mode': 'stdio', 'server_url': None}])
def test_resume_rejects_different_backend(evaluator_module, tmp_path, old_connection):
    (tmp_path / 'domain.json').write_text('[]')
    results = {'domains': {}}
    if old_connection is not None:
        results['tool_connection'] = old_connection
    output = tmp_path / 'results.txt'
    output.write_text(json.dumps(results))
    with pytest.raises(ValueError, match='backend'):
        evaluator_module.evaluate_capability('capability_bi_apis', tmp_path, tmp_path, output,
            {'capability_bi_apis': object()},
            mcp_config=MCPConnectionConfig(mode='http', server_url='https://example.org/mcp/{domain}'),
            tool_backend='code-engine')


def test_paired_replay_rejects_old_server(monkeypatch):
    import asyncio
    from contextlib import asynccontextmanager
    from mcp_tools import replay_paired_tools
    import benchmark.mcp_client as client
    class Session:
        async def list_tools(self):
            return SimpleNamespace(tools=[SimpleNamespace(name='get_data', inputSchema={'properties': {}})])
    @asynccontextmanager
    async def connect(*args): yield Session()
    monkeypatch.setattr(client, 'create_client_and_connect', connect)
    with pytest.raises(RuntimeError, match='Rebuild'):
        asyncio.run(replay_paired_tools(None, 'test', [[[]]], [[[]]], is_bi=True))
