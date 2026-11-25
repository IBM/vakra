import asyncio
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'evaluator'))
from evaluation_errors import llm_error_context, build_error_report, write_error_report
from judge import GroundednessJudge, CorrectnessJudge, LLMJudge
from mcp_tools import execute_tools_batch
from utils import JudgeInput


def result(text, **flags):
    return SimpleNamespace(content=[SimpleNamespace(text=text)], **flags)


@pytest.mark.parametrize('kind', ['exception', 'isError', 'is_error', 'json', 'text', 'second_content', 'status', 'errors'])
@pytest.mark.parametrize('recovered', [True, False])
def test_gt_three_retries_and_recovery(monkeypatch, kind, recovered):
    calls, sleeps, errors, progress = [], [], [], []
    async def sleep(delay): sleeps.append(delay)
    monkeypatch.setattr('mcp_tools.asyncio.sleep', sleep)
    async def call(name, args):
        calls.append((name, args.copy()))
        if recovered and len(calls) == 4: return result('{"value": 42}')
        if kind == 'exception': raise RuntimeError('offline')
        if kind in ('isError', 'is_error'): return result('bad tool', **{kind: True})
        if kind == 'json': return result('{"error": "bad tool"}')
        if kind == 'text': return result('Error: bad tool')
        if kind == 'status': return result('{"status": "ERROR", "message": "bad tool"}')
        if kind == 'errors': return result('{"errors": ["bad tool"]}')
        return SimpleNamespace(content=[SimpleNamespace(text='normal'), SimpleNamespace(text='Error: bad tool')])
    batch = [[[{'name': 'test', 'arguments': {'x': '2'}}]]]
    original = json.dumps(batch)
    responses = asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call), batch,
        {'test': {'properties': {'x': {'type': 'integer'}}}}, gt_retries=3,
        error_callback=errors.append, progress_callback=progress.append))
    assert len(calls) == 4
    assert sleeps == [1, 2, 4]
    assert all(args == {'x': 2} for name, args in calls)
    assert json.dumps(batch) == original
    assert len(errors) == 1
    assert errors[0]['attempt_count'] == 4
    assert errors[0]['failed_attempt_count'] == (3 if recovered else 4)
    assert errors[0]['recovered'] is recovered
    assert len(errors[0]['attempt_errors']) == (3 if recovered else 4)
    assert errors[0]['tool_index'] == 0
    assert progress[-2]['error'] is (not recovered)
    if recovered: assert responses == [[['{"value": 42}']]]


def test_prediction_is_not_retried():
    calls = []
    async def call(name, args):
        calls.append(name)
        raise RuntimeError('offline')
    responses = asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call),
        [[[{'name': 'test'}]]], {}))
    assert calls == ['test']
    assert responses == [[['Error: offline']]]


def test_cancellation_is_not_retried():
    async def call(*args): raise asyncio.CancelledError
    errors = []
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call),
            [[[{'name': 'test'}]]], {}, gt_retries=3, error_callback=errors.append))
    assert not errors


def test_judge_request_and_parse_errors_are_sample_scoped(monkeypatch):
    monkeypatch.setattr(LLMJudge, '_build_llm', lambda *args: None)
    inp = JudgeInput('capability_dashboard_apis', 'alpha', 'q', '', 'answer', 'answer', [], [], [], [])
    groundedness = GroundednessJudge(k=3)
    groundedness.invoke = Mock(side_effect=[RuntimeError('offline'), 'invalid', 'yes'])
    correctness = CorrectnessJudge()
    correctness.invoke = Mock(side_effect=TimeoutError('provider timeout'))
    records = []
    with llm_error_context(records, domain='alpha', uuid='sample-1'):
        output = groundedness.judge(inp)
        correctness.judge(inp)
    assert output.pass_at_k == 1 and output.pass_power_k == 0
    assert len(records) == 3
    assert all(r['uuid'] == 'sample-1' and r['domain'] == 'alpha' for r in records)
    assert [r['phase'] for r in records] == ['request', 'response_parse', 'request']
    assert [r['trial'] for r in records] == [1, 2, None]
    assert 'TimeoutError: provider timeout' in records[-1]['message']
    correctness.judge(inp)
    assert len(records) == 3  # Scope was restored after the sample.


def test_async_sample_contexts_do_not_leak():
    from evaluation_errors import record_llm_error
    records = []
    async def run():
        async def one(uuid):
            with llm_error_context(records, domain='alpha', uuid=uuid):
                await asyncio.sleep(.01)
                record_llm_error(SimpleNamespace(), RuntimeError(uuid), phase='request')
        await asyncio.gather(one('a'), one('b'))
    asyncio.run(run())
    assert {r['uuid']: r['message'] for r in records} == {'a': 'RuntimeError: a', 'b': 'RuntimeError: b'}


def test_report_counts_and_legacy_coverage(tmp_path):
    records = [
        {'type': 'ground_truth_tool_call', 'uuid': 'a', 'message': 'offline',
         'recovered': False, 'failed_attempt_count': 4},
        {'type': 'ground_truth_tool_call', 'uuid': 'b', 'message': 'offline',
         'recovered': True, 'failed_attempt_count': 2},
        {'type': 'llm_call', 'uuid': 'a', 'message': 'timeout'},
        {'type': 'llm_call', 'uuid': 'a', 'message': 'invalid response'},
    ]
    results = {'capability_name': 'capability_dashboard_apis', 'domains': {'alpha': {}, 'legacy': {}},
               'evaluation_errors': {'alpha': records}}
    output = tmp_path / 'results.json'
    report = write_error_report(results, output)
    assert report['summary'] == {'error_count': 4, 'ground_truth_tool_call_errors': 2,
        'unresolved_ground_truth_calls': 1, 'recovered_ground_truth_calls': 1,
        'failed_ground_truth_attempts': 6, 'llm_call_errors': 2}
    assert report['domains']['alpha']['samples']['a']['error_count'] == 3
    assert report['domains']['alpha']['samples']['b']['error_count'] == 1
    assert report['domains_without_error_tracking'] == ['legacy']
    assert json.loads((tmp_path / 'results.errors.json').read_text()) == report
    assert build_error_report(json.loads(json.dumps(results)), output) == report


@pytest.mark.parametrize('labeled', [False, True])
@pytest.mark.parametrize('recovered', [False, True])
def test_cap1_chains_only_successful_retry_handles(monkeypatch, labeled, recovered):
    attempts, next_args, errors = [], [], []
    async def sleep(delay): pass
    monkeypatch.setattr('mcp_tools.asyncio.sleep', sleep)
    async def call(name, args):
        if name == 'source':
            attempts.append(name)
            if recovered and len(attempts) == 2:
                return result('{"handle": "fresh", "results": [42]}', is_error=False)
            return result('{"handle": "bad", "results": [0]}', is_error=True)
        next_args.append(args)
        return result('[]')
    source = {'name': 'source', 'arguments': {}}
    if labeled: source['label'] = 'source'
    batch = [[[source, {'name': 'next', 'arguments': {'data_label': 'source', 'input_value': 'source'}}]]]
    asyncio.run(execute_tools_batch(SimpleNamespace(call_tool=call), batch, {}, is_gt=True,
                                   gt_retries=3, error_callback=errors.append))
    assert len(attempts) == (2 if recovered else 4)
    assert next_args == [{'data_label': 'fresh' if recovered else 'source',
                          'input_value': [42] if recovered else 'source'}]
    assert len(errors) == 1
    assert errors[0]['recovered'] is recovered
