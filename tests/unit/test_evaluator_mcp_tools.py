"""Regression coverage for ground-truth replay of MCP responses."""

import asyncio
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evaluator"))
from mcp_tools import execute_tools_batch


@pytest.mark.parametrize(
    "response, expected",
    [
        ('["Oklahoma Panhandle State University", "Chandler-Gilbert Community College"]', {"data_label": "original", "input_value": "original"}),
        ('["handle"]', {"data_label": "original", "input_value": "original"}),
        ('["https://example.edu/handle/123"]', {"data_label": "original", "input_value": "original"}),
        ('"handle"', {"data_label": "original", "input_value": "original"}),
        ('Error: invalid handle', {"data_label": "original", "input_value": "original"}),
        ('{"error": "unknown handle"}', {"data_label": "original", "input_value": "original"}),
        ('{"handle": "fresh"}', {"data_label": "fresh", "input_value": "original"}),
        ('{"handle": "fresh", "results": [1]}', {"data_label": "fresh", "input_value": [1]}),
    ],
)
@pytest.mark.parametrize("string_args", [False, True])
def test_gt_chaining_checks_response_shape(response, expected, string_args):
    calls = []

    async def call_tool(name, arguments):
        calls.append((name, arguments))
        return SimpleNamespace(content=[SimpleNamespace(text=response if name == "first" else "done")])

    arguments = {"data_label": "original", "input_value": "original"}
    batch = [[[{"name": "first"}, {"name": "next", "arguments": json.dumps(arguments) if string_args else arguments}]]]
    original = copy.deepcopy(batch)
    results = asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call_tool), batch, {}, is_gt=True))

    assert calls[1] == ("next", expected)
    assert results == [[[response, "done"]]]
    assert batch == original


def test_prediction_arguments_are_not_chained():
    calls = []

    async def call_tool(name, arguments):
        calls.append(arguments)
        return SimpleNamespace(content=[SimpleNamespace(text='{"handle": "fresh"}')])

    batch = [[[ {"name": "first"}, {"name": "next", "arguments": {"data_label": "original"}} ]]]
    asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call_tool), batch, {}))
    assert calls[1] == {"data_label": "original"}


def test_labeled_replay_resolves_branches_and_array_values():
    from mcp_tools import extract_toolcalls_for_mcp

    names = ['Oklahoma Panhandle State University', 'Chandler-Gilbert Community College']
    tools = [
        {'name': 'get_data', 'arguments': {}, 'label': 'source'},
        {'name': 'filter', 'arguments': {'data_label': 'source'}, 'label': 'filtered'},
        {'name': 'names', 'arguments': {'data_label': '$filtered$'}, 'label': 'names_result'},
        {'name': 'sites', 'arguments': {'data_label': '$filtered$'}, 'label': 'sites_result'},
        {'name': 'unique', 'arguments': {'unique_array': '$names_result$'}},
        {'name': 'original', 'arguments': {'data_label': '$source$'}},
    ]
    dialogue = {'output': [{'sequence': {'tool_call': tools[:3]}}, {'sequence': {'tool_call': tools[3:]}}]}
    original = copy.deepcopy(dialogue)
    responses = [{'handle': 'live_source'}, {'handle': 'live_filtered'}, names, ['example.edu'], names, []]
    calls = []

    async def call_tool(name, arguments):
        response = responses[len(calls)]
        calls.append((name, arguments))
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(response))])

    batch = [extract_toolcalls_for_mcp(dialogue)]
    asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call_tool), batch, {}, is_gt=True))
    assert calls[1][1] == {'data_label': 'live_source'}
    assert calls[2][1] == calls[3][1] == {'data_label': 'live_filtered'}
    assert calls[4][1] == {'unique_array': names}
    assert calls[5][1] == {'data_label': 'live_source'}
    assert dialogue == original


def test_labels_are_isolated_between_dialogues_and_errors_are_not_values():
    tools = [[[
        {'name': 'source', 'label': 'source'},
        {'name': 'use', 'arguments': {'data_label': '$source$'}},
    ]], [[
        {'name': 'use', 'arguments': {'data_label': '$source$'}},
        {'name': 'failure', 'label': 'failed'},
        {'name': 'use', 'arguments': {'data_label': '$failed$'}},
    ]]]
    calls = []

    async def call_tool(name, arguments):
        calls.append((name, arguments))
        response = {'error': 'failed'} if name == 'failure' else {'handle': 'live_source'}
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(response))])

    asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call_tool), tools, {}, is_gt=True))
    assert calls[1][1]['data_label'] == 'live_source'
    assert calls[2][1]['data_label'] == '$source$'
    assert calls[4][1]['data_label'] == '$failed$'
