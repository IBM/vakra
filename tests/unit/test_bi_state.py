import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'fixtures'))
from bi_backend import TinyBIBackend
from mcp.types import CallToolRequest, CallToolRequestParams


def test_raw_transforms_copy_inputs_and_replays_reset_without_reload():
    async def run():
        backend = TinyBIBackend()
        async def call(name, **arguments):
            result = await backend.server.request_handlers[CallToolRequest](
                CallToolRequest(method='tools/call', params=CallToolRequestParams(name=name, arguments=arguments)))
            text = result.root.content[0].text
            try:
                return json.loads(text)
            except ValueError:
                return text
        first = await call('get_data', tool_universe_id='sample-a', reset_handles=True)
        source = first['handle']
        result = await call('transform_data_to_absolute_value', data_label=source, key_name='value')
        assert await call('get_values', data_label=result['handle']) == [2, 3]
        assert await call('get_values', data_label=source) == [-2, 3]
        failure = await call('transform_data_to_failure', data_label=source)
        assert 'deliberate failure' in failure
        assert await call('get_values', data_label=source) == [-2, 3]
        gt = await call('get_data', tool_universe_id='sample-a', reset_handles=True)
        assert gt['handle'] == source
        assert len(backend._handle_store) == 1
        assert await call('get_values', data_label=gt['handle']) == [-2, 3]
        assert backend.loads == 1
        await call('get_data', tool_universe_id='sample-b', reset_handles=True)
        assert backend.loads == 2
        assert backend.active_data['value'] == [-5, 7]
    asyncio.run(run())


def test_copy_includes_lists_and_dtype_metadata():
    backend = TinyBIBackend()
    data = {'value': [-2, 3], '_dtypes': {'value': 'int64'}}
    handle = backend._store_handle('get_data', data)
    copied = backend._resolve_handles({'data_label': handle}, copy_data=True)['data_label']
    copied['value'].append(999)
    copied['_dtypes']['value'] = 'changed'
    assert data == {'value': [-2, 3], '_dtypes': {'value': 'int64'}}
    assert backend._resolve_handles({'data_label': handle})['data_label'] is data
