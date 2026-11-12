"""Small in-memory backend using the production capability-1 request handlers."""
import asyncio
from copy import deepcopy
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from mcp.server import Server
from environment.m3.python_tools.mcp.mcp_server import LiveMCPServer
from environment.m3.python_tools.tools.pydantic_models import GetterInput, TransformDataToAbsoluteValueInput
from environment.m3.python_tools.tools.slot_filling_tools import transform_data_to_absolute_value


class TinyBIBackend(LiveMCPServer):
    def __init__(self):
        self.server = Server('tiny-bi-backend')
        self.config = SimpleNamespace(database_path='unused')
        self.shutdown_event = None
        self._handle_store = {}
        self._handle_counter = {}
        self._universe_lock = asyncio.Lock()
        self.loads = 0
        self.all_tool_configs = {key: {'init_args': {}} for key in ('sample-a', 'sample-b')}
        self.tool_input_models = {
            'get_values': GetterInput,
            'transform_data_to_absolute_value': TransformDataToAbsoluteValueInput,
            'transform_data_to_failure': GetterInput,
            'get_loads': GetterInput,
        }
        self.tool_docstrings = {'get_data': 'Get the initial dataset.'}
        self._load_tool_universe('sample-a')
        self._register_handlers()

    def _initialize_toolbox(self): pass
    def _get_tool_specs(self): return []
    def _on_universe_switch(self, universe_id): pass
    def _reload_active_data_for_universe(self, universe_id):
        self.loads += 1
        self.active_data = {'value': [-2, 3] if universe_id == 'sample-a' else [-5, 7]}

    def _get_available_tools(self):
        def fail(data):
            data['value'].append(999)
            raise ValueError('deliberate failure after mutation')
        return {
            'get_values': lambda data: list(data['value']),
            'get_loads': lambda data: self.loads,
            'transform_data_to_absolute_value': transform_data_to_absolute_value,
            'transform_data_to_failure': fail,
        }


if __name__ == '__main__':
    from mcp.server.stdio import stdio_server
    async def main():
        backend = TinyBIBackend()
        async with stdio_server() as (read, write):
            await backend.server.run(read, write, backend.server.create_initialization_options())
    asyncio.run(main())
