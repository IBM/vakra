"""Exercise the batch wrapper with actual subprocesses, without hosted APIs."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('batch_evaluator', ROOT / 'evaluator/evaluate_models.py')
batch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(batch)


@pytest.fixture
def layout(tmp_path, monkeypatch):
    repo = tmp_path / 'repo'
    (repo / 'evaluator').mkdir(parents=True)
    (repo / 'evaluator/evaluator.py').write_text('''
import argparse,json,os,sys,time
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--pred_root');p.add_argument('--output')
a,_=p.parse_known_args()
model=Path(a.pred_root)
mode=json.loads((model/'domain.json').read_text()).get('mode','ok')
events=model.parent/'events.jsonl'
def emit(event):
    with events.open('a') as f:f.write(json.dumps({'event':event,'model':model.name,'pid':os.getpid()})+'\\n')
emit('start')
print('evaluating '+model.name,flush=True)
try:
    if mode=='sleep':
        while True:time.sleep(.05)
    time.sleep(.04)
    out=Path(a.output);out.parent.mkdir(parents=True,exist_ok=True)
    resumed=out.exists()
    out.write_text(json.dumps({'summary':{'n_samples':1,'pass_at_k':1},'resumed':resumed,
        'arguments':sys.argv[1:],'test_env':os.environ.get('EVALUATOR_BATCH_TEST_VALUE')}))
    out.with_name(out.stem+'.errors.json').write_text('{}')
    emit('end')
    raise SystemExit(1 if mode=='fail' else 0)
except KeyboardInterrupt:
    emit('interrupt')
    raise SystemExit(130)
''')
    pred = tmp_path / 'predictions' / 'cap1'
    pred.mkdir(parents=True)
    gt = tmp_path / 'ground_truth' / 'capability_1_bi_apis' / 'output'
    gt.mkdir(parents=True)
    (gt / 'domain.json').write_text('[]')
    out = tmp_path / 'evaluation' / 'cap1'
    monkeypatch.setattr(batch, 'REPO_ROOT', repo)
    monkeypatch.setattr(batch, 'EVALUATION_ROOT', tmp_path / 'evaluation')
    monkeypatch.setattr(batch, 'EVALUATION_LOG_ROOT', tmp_path / 'evaluation_logs')
    policy = repo / 'policy_judge.py'
    policy.write_text('# test policy judge')
    monkeypatch.setattr(batch, 'DEFAULT_POLICY_JUDGE', policy)
    def add(name, mode='ok'):
        path = pred / name
        path.mkdir()
        (path / 'domain.json').write_text(json.dumps({'mode': mode}))
        return path
    return pred, gt, out, add


def options(layout, *extra):
    pred, gt, out, add = layout
    return [str(pred), '--gt-root', str(gt), '--output-dir', str(out), *extra]


def events(layout):
    return [json.loads(line) for line in (layout[0] / 'events.jsonl').read_text().splitlines()]


def test_models_run_sequentially_and_continue_after_failure(layout):
    pred, gt, out, add = layout
    add('z_model'); add('a model'); add('b_failure', 'fail')
    (pred / 'log.txt').write_text('ignored')
    (pred / 'empty').mkdir()
    add('.hidden')
    assert batch.main(options(layout)) == 1
    summary = json.loads((out / 'batch_summary.json').read_text())
    assert summary['model_order'] == ['a model', 'b_failure', 'z_model']
    assert summary['completed_models'] == ['a model', 'z_model']
    assert summary['failed_models'] == ['b_failure']
    assert summary['pending_models'] == []
    assert [e['event'] for e in events(layout)] == ['start', 'end'] * 3
    assert [e['model'] for e in events(layout)[::2]] == summary['model_order']
    for name, result in summary['models'].items():
        assert name in Path(result['log']).read_text()
        assert Path(result['error_report']).exists()
        assert result['checkpoint_summary']['pass_at_k'] == 1
    assert len(list((out / 'batch_runs').glob('*.json'))) == 1


def test_stop_on_error_leaves_remaining_models_pending(layout):
    pred, gt, out, add = layout
    add('a', 'fail'); add('b')
    assert batch.main(options(layout, '--stop-on-error')) == 1
    summary = json.loads((out / 'batch_summary.json').read_text())
    assert summary['failed_models'] == ['a']
    assert summary['pending_models'] == ['b']
    assert {e['model'] for e in events(layout)} == {'a'}


def test_rerun_reuses_results_and_preserves_each_run_log(layout):
    pred, gt, out, add = layout
    add('a')
    assert batch.main(options(layout)) == 0
    assert batch.main(options(layout)) == 0
    result = json.loads((out / 'a.json').read_text())
    assert result['resumed'] is True
    summary = json.loads((out / 'batch_summary.json').read_text())
    assert len(list(Path(summary['logs_dir']).glob('*/a.log'))) == 2
    assert len(list((out / 'batch_runs').glob('*.json'))) == 2


@pytest.mark.parametrize('capability', [1, 2, 3, 4])
def test_default_output_mapping_and_capability_inference(layout, capability):
    pred, gt, out, add = layout
    add('a')
    target = pred.with_name(f'cap{capability}')
    if target != pred: pred.rename(target)
    args, models = batch.parse_args([str(target), '--gt-root', str(gt), '--tool-backend', 'code-engine'])
    assert args.capability_id == capability
    assert args.output_dir == out.parent / f'cap{capability}' / 'code-engine_k3'
    assert args.logs_dir == pred.parent.parent / 'evaluation_logs' / f'cap{capability}' / 'code-engine_k3'
    if capability == 4:
        assert args.policy_judge_path == batch.DEFAULT_POLICY_JUDGE
        assert '--policy-judge-path' in batch.command(args, models[0])
    else:
        assert args.policy_judge_path is None
    assert models[0].name == 'a'


def test_flags_are_forwarded_and_env_file_respects_exports(layout, monkeypatch):
    pred, gt, out, add = layout
    add('a')
    config = gt / 'config.yaml'; config.write_text('tasks: {}')
    policy = gt / 'policy.py'; policy.write_text('')
    env_file = gt / '.env'; env_file.write_text('EVALUATOR_BATCH_TEST_VALUE=from-file\n')
    monkeypatch.setenv('EVALUATOR_BATCH_TEST_VALUE', 'exported')
    assert batch.main(options(layout, '--k', '5', '--tool-backend', 'code-engine',
        '--num_domains', '2', '--domains', 'domain', '--mcp-config', str(config),
        '--policy-judge-path', str(policy), '--env-file', str(env_file))) == 0
    result = json.loads((out / 'a.json').read_text())
    argv = result['arguments']
    assert argv[argv.index('--k') + 1] == '5'
    assert argv[argv.index('--num-domains') + 1] == '2'
    assert argv[argv.index('--tool-backend') + 1] == 'code-engine'
    assert argv[argv.index('--domains') + 1:] == ['domain']
    assert result['test_env'] == 'exported'


def test_model_selection_and_dry_run_have_no_side_effects(layout, capsys, monkeypatch):
    pred, gt, out, add = layout
    add('a'); add('b')
    def forbidden(*args, **kwargs): pytest.fail('dry-run started an evaluator')
    monkeypatch.setattr(batch.subprocess, 'Popen', forbidden)
    assert batch.main(options(layout, '--models', 'b', '--dry-run')) == 0
    assert not out.exists()
    assert not (pred / 'events.jsonl').exists()
    text = capsys.readouterr().out
    assert str(pred / 'b') in text
    assert str(pred / 'a') not in text


def test_ignore_prefix_directories_are_excluded(layout):
    pred, gt, out, add = layout
    add('a')
    add('IGNORE_old_model')
    add('IGNORE_another_model')
    args, models = batch.parse_args(options(layout))
    assert [p.name for p in models] == ['a']
    assert batch.main(options(layout)) == 0
    assert {e['model'] for e in events(layout)} == {'a'}
    summary = json.loads((out / 'batch_summary.json').read_text())
    assert summary['model_order'] == ['a']
    with pytest.raises(SystemExit) as error:
        batch.parse_args(options(layout, '--models', 'IGNORE_old_model'))
    assert error.value.code == 2


@pytest.mark.parametrize('extra', [['--models', 'missing'], ['--domains', 'missing'], ['--k', '0']])
def test_invalid_selection_is_rejected(layout, extra):
    layout[3]('a')
    with pytest.raises(SystemExit) as error: batch.parse_args(options(layout, *extra))
    assert error.value.code == 2


def test_output_cannot_overwrite_model_predictions(layout):
    pred, gt, out, add = layout
    model = add('a')
    with pytest.raises(SystemExit):
        batch.parse_args([str(pred), '--gt-root', str(gt), '--output-dir', str(model)])


def test_interrupt_stops_current_child_and_does_not_start_next(layout, monkeypatch):
    pred, gt, out, add = layout
    add('a', 'sleep'); add('b')
    original = batch.subprocess.Popen
    launched = []
    def launch(*args, **kwargs):
        process = original(*args, **kwargs)
        launched.append(process)
        wait = process.wait
        first = True
        def interrupt_wait(*args, **kwargs):
            nonlocal first
            if first:
                first = False
                deadline = time.monotonic() + 5
                while not (pred / 'events.jsonl').exists():
                    assert time.monotonic() < deadline
                    time.sleep(.01)
                raise KeyboardInterrupt
            return wait(*args, **kwargs)
        process.wait = interrupt_wait
        return process
    monkeypatch.setattr(batch.subprocess, 'Popen', launch)
    previous = signal.getsignal(signal.SIGTERM)
    assert batch.main(options(layout)) == 130
    assert len(launched) == 1 and launched[0].poll() is not None
    assert signal.getsignal(signal.SIGTERM) == previous
    summary = json.loads((out / 'batch_summary.json').read_text())
    assert summary['status'] == 'interrupted'
    assert summary['models']['a']['status'] == 'interrupted'
    assert summary['pending_models'] == ['b']
    assert [e['event'] for e in events(layout)] == ['start', 'interrupt']


def test_python_symlink_keeps_virtual_environment_path(layout, tmp_path):
    pred, gt, out, add = layout
    add('a')
    executable = tmp_path / 'venv-python'
    executable.symlink_to(sys.executable)
    args, _ = batch.parse_args(options(layout, '--python', str(executable)))
    assert args.python == str(executable)


def test_capability_four_uses_default_policy_in_child_command(layout):
    pred, gt, out, add = layout
    add('a')
    assert batch.main(options(layout, '--capability-id', '4')) == 0
    result = json.loads((out / 'a.json').read_text())
    argv = result['arguments']
    assert argv[argv.index('--capability_name') + 1] == 'capability_multiturn'
    assert argv[argv.index('--policy-judge-path') + 1] == str(batch.DEFAULT_POLICY_JUDGE)
    summary = json.loads((out / 'batch_summary.json').read_text())
    assert summary['policy_judge_path'] == str(batch.DEFAULT_POLICY_JUDGE)


def test_capability_four_can_explicitly_disable_policy(layout):
    layout[3]('a')
    args, models = batch.parse_args(options(layout, '--capability-id', '4', '--no-policy-judge'))
    assert args.policy_judge_path is None
    assert '--policy-judge-path' not in batch.command(args, models[0])


def test_missing_default_policy_fails_before_evaluation(layout, monkeypatch):
    layout[3]('a')
    monkeypatch.setattr(batch, 'DEFAULT_POLICY_JUDGE', layout[1] / 'missing_policy.py')
    with pytest.raises(SystemExit) as error:
        batch.parse_args(options(layout, '--capability-id', '4'))
    assert error.value.code == 2


def test_custom_policy_and_log_directory(layout):
    pred, gt, out, add = layout
    add('a')
    policy = gt / 'custom_policy.py'
    policy.write_text('# custom')
    logs = out.parent / 'custom_logs'
    assert batch.main(options(layout, '--capability-id', '4', '--policy-judge-path', str(policy),
                              '--logs-dir', str(logs))) == 0
    summary = json.loads((out / 'batch_summary.json').read_text())
    assert summary['policy_judge_path'] == str(policy)
    assert Path(summary['models']['a']['log']).is_relative_to(logs)
    argv = json.loads((out / 'a.json').read_text())['arguments']
    assert argv[argv.index('--log-dir') + 1] == str(logs / 'domain_logs' / 'a')
    assert argv[argv.index('--policy-judge-path') + 1] == str(policy)
