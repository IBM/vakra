import asyncio
import json
import multiprocessing
from pathlib import Path
import pickle
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'evaluator'), str(ROOT / 'tests' / 'fixtures')]
import domain_runner as runner
from scheduler_workers import tracked_worker
from parallel_judges import FixedJudge, FixedGroundedness
from mcp_tools import execute_tools_batch


def jobs(tmp_path, count=4, width=1):
    return [runner.DomainJob(f'domain_{i}', tmp_path / f'{i}.json', tmp_path / f'{i}.json',
            'capability_dashboard_apis', None, {'barrier_width': width}, None,
            tmp_path / f'{i}.log', tmp_path / f'{i}.result.json') for i in range(count)]


@pytest.mark.parametrize('limit,peak', [(1, 1), (2, 2), (20, 4)])
def test_real_process_limit_and_all_queued_domains(tmp_path, limit, peak):
    outcomes, events = [], []
    runner.run_domains(jobs(tmp_path, width=peak), limit, events.append, outcomes.append,
                       worker=tracked_worker)
    assert len(outcomes) == 4
    intervals = [r.data for r in outcomes]
    overlap = max(sum(r['started'] <= point < r['ended'] for r in intervals)
                  for point in [r['started'] for r in intervals])
    assert overlap == peak
    assert len({r['pid'] for r in intervals}) == 4
    assert {event['domain'] for event in events} == {r.domain for r in outcomes}


def test_domain_errors_and_crashes_do_not_stop_other_domains(tmp_path):
    batch = jobs(tmp_path, count=3)
    batch[0].policy_spec['fail'] = True
    batch[1].policy_spec['crash'] = True
    outcomes = []
    runner.run_domains(batch, 2, lambda event: None, outcomes.append, worker=tracked_worker)
    by_domain = {r.domain: r for r in outcomes}
    assert 'deliberate' in by_domain['domain_0'].error
    assert 'code 7' in by_domain['domain_1'].error
    assert by_domain['domain_2'].error is None


def test_interrupt_cleans_up_workers(tmp_path):
    outcomes = []
    def event(item):
        if item['event'] == 'sample_scored':
            raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        runner.run_domains(jobs(tmp_path), 2, event, outcomes.append, worker=tracked_worker)
    assert not [p for p in multiprocessing.active_children() if p.name.startswith('evaluate-')]


def test_atomic_write_keeps_previous_checkpoint_on_failure(tmp_path, monkeypatch):
    output = tmp_path / 'results.json'
    runner.atomic_write_json(output, {'completed': ['first']})
    def fail(*args): raise OSError('replace failed')
    monkeypatch.setattr(runner.os, 'replace', fail)
    with pytest.raises(OSError, match='replace failed'):
        runner.atomic_write_json(output, {'completed': ['second']})
    assert json.loads(output.read_text()) == {'completed': ['first']}
    assert list(tmp_path.glob('*.tmp')) == []


def test_policy_serialization_preserves_config_and_k_without_clients():
    policy = SimpleNamespace(dialogue_aggregate='mean', execute_mcp_tools=True,
        correctness_judge=FixedJudge({'setting': 'value'}), groundedness_judge=FixedGroundedness(k=5),
        exactmatch_judge=FixedJudge())
    spec = runner.pack_policy(policy)
    restored = runner.unpack_policy(pickle.loads(pickle.dumps(spec)))
    assert restored.groundedness_judge.k == 5
    assert restored.correctness_judge.model_config == {'setting': 'value'}
    assert restored.correctness_judge is not policy.correctness_judge
    assert 'llm' not in str(spec)


def test_replay_progress_does_not_change_outputs_or_arguments():
    batch = [[[{'name': 'echo', 'arguments': {'value': '1'}}],
              [{'name': 'failure', 'arguments': {}}]]]
    original = json.dumps(batch)
    async def call(name, args):
        if name == 'failure': raise RuntimeError('expected')
        return SimpleNamespace(content=[SimpleNamespace(text='{"value": 1}')], isError=False)
    async def run():
        session = SimpleNamespace(call_tool=call)
        baseline = await execute_tools_batch(session, batch, {})
        events = []
        result = await execute_tools_batch(session, batch, {}, progress_callback=events.append)
        assert result == baseline
        completed = [event for event in events if event['event'] == 'tool_completed']
        assert [e['completed'] for e in completed] == [1, 2]
        assert [e['error'] for e in completed] == [False, True]
        assert all(e['total'] == 2 for e in completed)
        assert json.dumps(batch) == original
    asyncio.run(run())
