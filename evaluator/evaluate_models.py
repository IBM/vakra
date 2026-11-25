#!/usr/bin/env python3
"""Evaluate immediate model directories sequentially; domains may run concurrently.

Example:
  python evaluator/evaluate_models.py /root/results_v1/benchmark_runner/cap1 \
      --env-file /root/.env

Use --dry-run to preview commands. Evaluator checkpoints handle domain resume.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[1]
EVALUATION_ROOT = Path('/root/results_v1/evaluation')
EVALUATION_LOG_ROOT = Path('/root/results_v1/evaluation_logs')
DEFAULT_POLICY_JUDGE = Path('/root/vakra-internal-read-only/evaluator/policy_judge.py')
CAPABILITIES = {
    1: 'capability_bi_apis', 2: 'capability_dashboard_apis',
    3: 'capability_multihop_reasoning', 4: 'capability_multiturn',
}
GT_FOLDERS = {
    1: 'capability_1_bi_apis', 2: 'capability_2_dashboard_apis',
    3: 'capability_3_multihop_reasoning', 4: 'capability_4_multiturn',
}


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent, delete=False) as file:
            temporary = Path(file.name)
            json.dump(value, file, indent=2)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be positive')
    return number


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('prediction_dir', nargs='?', type=Path)
    parser.add_argument('--pred-root', '--pred_root', type=Path)
    parser.add_argument('--capability-id', type=int, choices=CAPABILITIES)
    parser.add_argument('--gt-root', '--gt_root', type=Path, help='Capability ground-truth output directory')
    parser.add_argument('--gt-base', type=Path, default=Path('/root/v14/internal/test'))
    parser.add_argument('--output-dir', type=Path,
                        help='Default: /root/results_v1/evaluation/cap<N>/<backend>_k<K>')
    parser.add_argument('--logs-dir', type=Path,
                        help='Default: /root/results_v1/evaluation_logs/cap<N>/<backend>_k<K>')
    parser.add_argument('--tool-backend', choices=['local', 'code-engine'], default='local')
    parser.add_argument('--k', type=positive, default=3)
    parser.add_argument('--num-domains', '--num_domains', type=positive, default=20)
    parser.add_argument('--models', nargs='+', help='Optional immediate model-directory names')
    parser.add_argument('--domains', nargs='+', help='Optional domain names forwarded to the evaluator')
    parser.add_argument('--mcp-config', type=Path)
    policy = parser.add_mutually_exclusive_group()
    policy.add_argument('--policy-judge-path', type=Path,
                        help='Capability 4 defaults to the internal evaluator/policy_judge.py')
    policy.add_argument('--no-policy-judge', action='store_true',
                        help='Explicitly disable the default capability 4 policy judge')
    parser.add_argument('--env-file', type=Path, help='Optional dotenv file; exported variables take precedence')
    parser.add_argument('--python', default=sys.executable, help='Evaluator Python (default: current interpreter)')
    parser.add_argument('--stop-on-error', action='store_true', help='Otherwise continue with remaining models')
    parser.add_argument('--dry-run', action='store_true', help='Print commands without creating files or evaluating')
    args = parser.parse_args(argv)
    if bool(args.prediction_dir) == bool(args.pred_root):
        parser.error('Provide a prediction directory either positionally or through --pred-root')
    args.pred_root = (args.pred_root or args.prediction_dir).expanduser().resolve()
    if not args.pred_root.is_dir():
        parser.error(f'Prediction directory does not exist: {args.pred_root}')
    if args.capability_id is None:
        candidates = [args.pred_root.name]
        if args.gt_root:
            candidates.append(args.gt_root.resolve().parent.name)
        for name in candidates:
            match = re.fullmatch(r'(?:cap|capability_)([1-4])(?:_.*)?', name)
            if match:
                args.capability_id = int(match.group(1))
                break
        if args.capability_id is None:
            parser.error('Cannot infer capability; pass --capability-id 1, 2, 3 or 4')
    args.gt_root = (args.gt_root or args.gt_base / GT_FOLDERS[args.capability_id] / 'output').expanduser().resolve()
    if not args.gt_root.is_dir() or not list(args.gt_root.glob('*.json')):
        parser.error(f'No domain JSON files found in ground-truth directory: {args.gt_root}')
    args.output_dir = (args.output_dir or EVALUATION_ROOT / f'cap{args.capability_id}' /
                       f'{args.tool_backend}_k{args.k}').expanduser().resolve()
    args.logs_dir = (args.logs_dir or EVALUATION_LOG_ROOT / f'cap{args.capability_id}' /
                     f'{args.tool_backend}_k{args.k}').expanduser().resolve()
    if args.capability_id == 4 and args.policy_judge_path is None and not args.no_policy_judge:
        args.policy_judge_path = DEFAULT_POLICY_JUDGE
    for name in ['mcp_config', 'policy_judge_path', 'env_file']:
        path = getattr(args, name)
        if path is not None:
            path = path.expanduser().resolve()
            if not path.is_file():
                parser.error(f'{name.replace("_", "-")} file does not exist: {path}')
            setattr(args, name, path)
    python = shutil.which(args.python)
    if python is None:
        parser.error(f'Python executable not found: {args.python}')
    # Make relative paths independent of child cwd without resolving venv symlinks.
    args.python = os.path.abspath(python)
    domains = {p.name for p in args.gt_root.glob('*.json')}
    if args.domains:
        unknown = set(args.domains) - {Path(name).stem for name in domains}
        if unknown:
            parser.error(f'Domains missing from ground truth: {sorted(unknown)}')
    models = sorted((p for p in args.pred_root.iterdir() if p.is_dir() and not p.name.startswith('.')
                     and not p.name.startswith('IGNORE_')
                     and p.name != '_evaluation' and not (p / 'batch_summary.json').is_file()
                     and any((p / domain).is_file() for domain in domains)),
                    key=lambda p: (p.name.casefold(), p.name))
    if args.models:
        unknown = set(args.models) - {p.name for p in models}
        if unknown:
            parser.error(f'Model directories missing or without matching domain JSON: {sorted(unknown)}')
        models = [p for p in models if p.name in args.models]
    if not models:
        parser.error('No model directories containing matching domain JSON files found')
    if any(root == p or p in root.parents for root in (args.output_dir, args.logs_dir) for p in models):
        parser.error('--output-dir and --logs-dir must be outside the model prediction directories')
    return args, models


def command(args, model):
    result = args.output_dir / f'{model.name}.json'
    cmd = [args.python, '-u', str(REPO_ROOT / 'evaluator/evaluator.py'),
           '--capability_name', CAPABILITIES[args.capability_id], '--gt_root', str(args.gt_root),
           '--pred_root', str(model), '--output', str(result), '--tool-backend', args.tool_backend,
           '--k', str(args.k), '--num-domains', str(args.num_domains),
           '--log-dir', str(args.logs_dir / 'domain_logs' / model.name)]
    for flag, value in [('--mcp-config', args.mcp_config), ('--policy-judge-path', args.policy_judge_path)]:
        if value is not None:
            cmd.extend([flag, str(value)])
    if args.domains:
        cmd.extend(['--domains', *args.domains])
    return cmd


def stop_child(process):
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        if os.name == 'posix':
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name == 'posix':
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
            process.wait()


def interrupt(signum, frame):
    raise KeyboardInterrupt


def main(argv=None):
    args, models = parse_args(argv)
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    print(f'{len(models)} models; capability={args.capability_id}, backend={args.tool_backend}, K={args.k}', flush=True)
    print(f'Results: {args.output_dir}; logs: {args.logs_dir}', flush=True)
    if args.dry_run:
        for model in models:
            print(shlex.join(command(args, model)))
        return 0
    env = os.environ.copy()
    if args.env_file:
        try:
            from dotenv import dotenv_values
        except ImportError:
            raise SystemExit('Using --env-file requires python-dotenv; run this script in the evaluator venv')
        env = {**{k: v for k, v in dotenv_values(args.env_file).items() if v is not None}, **env}
    env['PYTHONPATH'] = str(REPO_ROOT) + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
    summary = {'run_id': run_id, 'started_at': now(), 'status': 'running',
               'capability_name': CAPABILITIES[args.capability_id], 'gt_root': str(args.gt_root),
               'pred_root': str(args.pred_root), 'backend': args.tool_backend, 'k': args.k,
               'logs_dir': str(args.logs_dir),
               'policy_judge_path': str(args.policy_judge_path) if args.policy_judge_path else None,
               'model_order': [p.name for p in models], 'models': {}}
    def checkpoint():
        summary['pending_models'] = [p.name for p in models if p.name not in summary['models']]
        summary['completed_models'] = [n for n, r in summary['models'].items() if r['status'] == 'completed']
        summary['failed_models'] = [n for n, r in summary['models'].items() if r['status'] == 'failed']
        atomic_json(args.output_dir / 'batch_runs' / f'{run_id}.json', summary)
        atomic_json(args.output_dir / 'batch_summary.json', summary)
    previous_term = signal.signal(signal.SIGTERM, interrupt)
    try:
        checkpoint()
        for index, model in enumerate(models, 1):
            log = args.logs_dir / run_id / f'{model.name}.log'
            log.parent.mkdir(parents=True, exist_ok=True)
            entry = {'status': 'running', 'started_at': now(), 'log': str(log),
                     'results': str(args.output_dir / f'{model.name}.json'),
                     'error_report': str(args.output_dir / f'{model.name}.errors.json'),
                     'command': command(args, model)}
            summary['models'][model.name] = entry
            checkpoint()
            print(f'[{index}/{len(models)}] {model.name}; log={log}', flush=True)
            started, process = time.monotonic(), None
            try:
                with log.open('w', encoding='utf-8') as stream:
                    process = subprocess.Popen(entry['command'], cwd=REPO_ROOT, env=env,
                                               stdout=stream, stderr=subprocess.STDOUT,
                                               start_new_session=(os.name == 'posix'))
                    entry['exit_code'] = process.wait()
                entry['status'] = 'completed' if entry['exit_code'] == 0 else 'failed'
                try:
                    result = json.loads(Path(entry['results']).read_text())
                    entry['checkpoint_summary'] = result['summary']
                except (OSError, ValueError, KeyError, TypeError) as error:
                    entry['result_read_error'] = str(error)
                    entry['status'] = 'failed'
            except KeyboardInterrupt:
                if process is not None:
                    stop_child(process)
                    entry['exit_code'] = process.returncode
                entry['status'] = 'interrupted'
                raise
            except OSError as error:
                entry.update(status='failed', message=str(error))
            finally:
                entry.update(finished_at=now(), seconds=round(time.monotonic() - started, 3))
                checkpoint()
            print(f"{model.name}: {entry['status']} (exit={entry.get('exit_code')})", flush=True)
            if args.stop_on_error and entry['status'] == 'failed':
                break
        summary['status'] = 'partial' if summary['failed_models'] else 'completed'
    except KeyboardInterrupt:
        summary['status'] = 'interrupted'
    finally:
        summary['finished_at'] = now()
        checkpoint()
        signal.signal(signal.SIGTERM, previous_term)
    print(f"Batch {summary['status']}: completed={len(summary['completed_models'])}, "
          f"failed={len(summary['failed_models'])}; summary={args.output_dir / 'batch_summary.json'}", flush=True)
    return 130 if summary['status'] == 'interrupted' else int(bool(summary['failed_models']))


if __name__ == '__main__':
    raise SystemExit(main())
