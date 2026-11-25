import asyncio
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'evaluator'), str(ROOT / 'tests' / 'fixtures')]
import evaluator as entry
import domain_runner
from parallel_judges import FixedJudge, FixedGroundedness
from tool_backends import EvaluatorMCPConnectionConfig


def policy():
    return entry.CapabilityPolicy(execute_mcp_tools=False, correctness_judge=FixedJudge(),
        groundedness_judge=FixedGroundedness(), exactmatch_judge=FixedJudge())


@pytest.mark.parametrize('capability', ['capability_bi_apis', 'capability_dashboard_apis',
                                     'capability_multihop_reasoning', 'capability_multiturn'])
def test_local_execution_stays_sequential(capability, tmp_path, monkeypatch):
    for domain in ('alpha', 'beta'):
        (tmp_path / f'{domain}.json').write_text('[]')
    active = 0
    async def evaluate(domain, **kwargs):
        nonlocal active
        active += 1
        assert active == 1
        await asyncio.sleep(.01)
        active -= 1
        return {'domain': domain, 'n_paired': 0, 'missing_prediction_uuids': [],
                'extra_prediction_uuids': [], 'dialogues': [], 'summary': {'num_samples': 0}}, []
    monkeypatch.setattr(entry, 'evaluate_domain', evaluate)
    def forbidden(*args, **kwargs): pytest.fail('Local evaluation must not launch domain workers')
    monkeypatch.setattr(domain_runner, 'run_domains', forbidden)
    output = tmp_path / 'results.txt'
    logs = tmp_path / 'logs'
    result = entry.evaluate_capability(capability, tmp_path, tmp_path, output, {capability: policy()},
        tool_backend='local', num_domains=20, log_dir=logs)
    assert 'execution' not in result
    assert 'failed_domains' not in result
    assert not logs.exists()
    assert json.loads(output.read_text()) == result


@pytest.mark.parametrize('flag,value', [('--num-domains', '0'), ('--num_domains', '-2'), ('--num-domains', 'text')])
def test_cli_rejects_invalid_concurrency(flag, value, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, 'argv', ['evaluator.py', '--capability_name', 'capability_dashboard_apis',
        '--gt_root', str(tmp_path), '--pred_root', str(tmp_path), flag, value])
    with pytest.raises(SystemExit) as error: entry.main()
    assert error.value.code == 2


@pytest.mark.parametrize('options,expected', [([], 20), (['--num-domains', '2'], 2), (['--num_domains', '1'], 1)])
def test_cli_default_and_aliases(options, expected, monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setattr(sys, 'argv', ['evaluator.py', '--capability_name', 'capability_dashboard_apis',
        '--gt_root', str(tmp_path), '--pred_root', str(tmp_path), *options])
    monkeypatch.setattr(entry, 'build_default_capability_registry', lambda k=3: {})
    monkeypatch.setattr(entry, 'evaluate_capability', lambda **kwargs: captured.update(kwargs))
    entry.main()
    assert captured['num_domains'] == expected
    assert captured['tool_backend'] == 'local'


def test_partial_run_exits_nonzero(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, 'argv', ['evaluator.py', '--capability_name', 'capability_dashboard_apis',
        '--gt_root', str(tmp_path), '--pred_root', str(tmp_path)])
    monkeypatch.setattr(entry, 'build_default_capability_registry', lambda k=3: {})
    monkeypatch.setattr(entry, 'evaluate_capability', lambda **kwargs: {'execution': {'status': 'partial'}})
    with pytest.raises(SystemExit) as error: entry.main()
    assert error.value.code == 1


def test_interrupt_checkpoints_completed_domains(tmp_path, monkeypatch):
    gt = tmp_path / 'gt'
    gt.mkdir()
    for domain in ('alpha', 'beta'):
        (gt / f'{domain}.json').write_text('[]')
    def interrupt(jobs, limit, on_event, on_result):
        on_result(domain_runner.DomainOutcome('alpha', str(jobs[0].log_path),
            data={'domain': 'alpha', 'dialogues': [], 'n_paired': 0,
                  'missing_prediction_uuids': [], 'extra_prediction_uuids': [],
                  'summary': {'num_samples': 0}}, scores=[]))
        on_event({'event': 'domain_started', 'domain': 'beta', 'log_path': str(jobs[1].log_path)})
        raise KeyboardInterrupt
    monkeypatch.setattr(domain_runner, 'run_domains', interrupt)
    output = tmp_path / 'results.json'
    config = EvaluatorMCPConnectionConfig(mode='http', server_url='http://test/mcp/{domain}', backend='code-engine')
    with pytest.raises(KeyboardInterrupt):
        entry.evaluate_capability('capability_dashboard_apis', gt, gt, output,
            {'capability_dashboard_apis': policy()}, config, tool_backend='code-engine')
    result = json.loads(output.read_text())
    assert result['execution']['status'] == 'interrupted'
    assert result['execution']['completed_domains'] == ['alpha']
    assert result['execution']['pending_domains'] == ['beta']
    assert result['domain_progress']['beta']['status'] == 'interrupted'
    assert set(result['domains']) == {'alpha'}


def test_failed_worker_errors_are_checkpointed(tmp_path, monkeypatch):
    gt = tmp_path / 'gt'
    gt.mkdir()
    (gt / 'alpha.json').write_text('[]')
    error = {'type': 'ground_truth_tool_call', 'domain': 'alpha', 'uuid': 'sample-1',
             'tool': 'test', 'message': 'offline', 'recovered': False,
             'attempt_count': 4, 'failed_attempt_count': 4}
    def fail(jobs, limit, on_event, on_result):
        on_result(domain_runner.DomainOutcome('alpha', str(jobs[0].log_path),
                                             error='worker failed later', errors=[error]))
    monkeypatch.setattr(domain_runner, 'run_domains', fail)
    config = EvaluatorMCPConnectionConfig(mode='http', server_url='http://test/mcp/{domain}', backend='code-engine')
    output = tmp_path / 'results.json'
    result = entry.evaluate_capability('capability_dashboard_apis', gt, gt, output,
        {'capability_dashboard_apis': policy()}, config, tool_backend='code-engine')
    report = json.loads((tmp_path / 'results.errors.json').read_text())
    assert result['execution']['status'] == 'partial'
    assert result['evaluation_errors']['alpha'] == [error]
    assert report['summary']['unresolved_ground_truth_calls'] == 1
    assert report['domains']['alpha']['samples']['sample-1']['errors'] == [error]


def test_local_exception_preserves_collected_errors(tmp_path, monkeypatch):
    gt = tmp_path / 'gt'
    gt.mkdir()
    (gt / 'alpha.json').write_text('[]')
    error = {'type': 'llm_call', 'domain': 'alpha', 'uuid': 'sample-1',
             'judge': 'GroundednessJudge', 'message': 'provider offline'}
    async def fail(**kwargs):
        kwargs['error_records'].append(error)
        raise RuntimeError('domain failed later')
    monkeypatch.setattr(entry, 'evaluate_domain', fail)
    output = tmp_path / 'results.json'
    with pytest.raises(RuntimeError, match='domain failed later'):
        entry.evaluate_capability('capability_dashboard_apis', gt, gt, output,
            {'capability_dashboard_apis': policy()})
    report = json.loads((tmp_path / 'results.errors.json').read_text())
    assert report['summary']['llm_call_errors'] == 1
    assert report['domains']['alpha']['samples']['sample-1']['errors'] == [error]
