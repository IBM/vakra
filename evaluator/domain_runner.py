"""Bounded Code Engine domain workers and atomic coordinator checkpoints.

Workers own their MCP event loop, judges and stdout/stderr. Only the parent
writes the aggregate result; workers return results through private JSON files.
"""
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
import importlib
import inspect
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import queue
import signal
import tempfile
import time
import traceback
from typing import Any, Callable


def atomic_write_json(path: Path, value: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix=f'.{path.name}.', suffix='.tmp', delete=False) as file:
            name = file.name
            json.dump(value, file, indent=2, ensure_ascii=False)
            file.flush()
            os.fsync(file.fileno())
        os.replace(name, path)
    finally:
        if name is not None:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(name)


def pack_policy(policy) -> dict:
    """Pass constructor settings to spawn workers, never live LLM clients."""
    judges = {}
    for name in ('correctness_judge', 'groundedness_judge', 'exactmatch_judge'):
        judge = getattr(policy, name)
        cls = type(judge)
        if '<locals>' in cls.__qualname__:
            raise ValueError(f'Parallel evaluation requires an importable judge class: {name}')
        parameters = inspect.signature(cls).parameters
        kwargs = {}
        if 'config' in parameters:
            kwargs['config'] = getattr(judge, 'model_config', {})
        if 'k' in parameters:
            kwargs['k'] = judge.k
        judges[name] = {'module': cls.__module__, 'class': cls.__qualname__, 'kwargs': kwargs}
    return {'dialogue_aggregate': policy.dialogue_aggregate,
            'execute_mcp_tools': policy.execute_mcp_tools, 'judges': judges}


def unpack_policy(spec):
    from evaluator import CapabilityPolicy
    judges = {}
    for name, definition in spec['judges'].items():
        cls = importlib.import_module(definition['module'])
        for component in definition['class'].split('.'):
            cls = getattr(cls, component)
        judges[name] = cls(**definition['kwargs'])
    return CapabilityPolicy(dialogue_aggregate=spec['dialogue_aggregate'],
                            execute_mcp_tools=spec['execute_mcp_tools'], **judges)


@dataclass
class DomainJob:
    domain: str
    gt_path: Path
    pred_path: Path
    capability_name: str
    mcp_config: Any
    policy_spec: dict
    policy_judge_path: str | None
    log_path: Path
    result_path: Path


@dataclass
class DomainOutcome:
    domain: str
    log_path: str
    data: dict | None = None
    scores: list | None = None
    error: str | None = None
    seconds: float = 0
    errors: list = field(default_factory=list)


def domain_worker(job: DomainJob, events, stop) -> None:
    """Top-level spawn target; all process-global redirection is private here."""
    signal.signal(signal.SIGINT, _interrupt)
    started = time.monotonic()
    job.log_path.parent.mkdir(parents=True, exist_ok=True)
    with job.log_path.open('w', encoding='utf-8', buffering=1) as log:
        # Capture stderr passed by already-imported MCP libraries too. Their
        # default errlog may still reference fd 2 rather than sys.stderr.
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            logging.basicConfig(level=logging.INFO, stream=log,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s', force=True)
            def emit(event):
                if stop.is_set():
                    raise asyncio.CancelledError('Domain evaluation interrupted')
                payload = {**event, 'domain': job.domain, 'capability': job.capability_name,
                           'time': datetime.now(timezone.utc).isoformat()}
                print(json.dumps(payload), flush=True)
                events.put(payload)
            outcome = DomainOutcome(job.domain, str(job.log_path))
            try:
                from evaluator import evaluate_domain
                policy = unpack_policy(job.policy_spec)
                emit({'event': 'stage', 'stage': 'loading'})
                outcome.data, outcome.scores = asyncio.run(evaluate_domain(
                    job.domain, job.gt_path, job.pred_path, policy, job.mcp_config,
                    job.capability_name, job.policy_judge_path, progress_callback=emit,
                    error_records=outcome.errors))
            except BaseException as exc:
                outcome.error = f'{type(exc).__name__}: {exc}'
                traceback.print_exc()
            outcome.seconds = round(time.monotonic() - started, 3)
            atomic_write_json(job.result_path, vars(outcome))


def _outcome(job, process) -> DomainOutcome:
    try:
        result = json.loads(job.result_path.read_text(encoding='utf-8'))
        outcome = DomainOutcome(**result)
        if outcome.domain != job.domain:
            raise ValueError('Worker returned a different domain')
        if outcome.error is None and (not isinstance(outcome.data, dict) or outcome.scores is None):
            raise ValueError('Worker returned an incomplete domain result')
        return outcome
    except (OSError, ValueError, TypeError) as exc:
        return DomainOutcome(job.domain, str(job.log_path),
                             error=f'Worker exited with code {process.exitcode}: {exc}')


def run_domains(jobs: list[DomainJob], limit: int, on_event: Callable, on_result: Callable,
                *, worker=domain_worker) -> None:
    """Start at most limit domain processes, continuing after domain failures."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError('num_domains must be a positive integer')
    context = mp.get_context('spawn')
    events, stop = context.Queue(), context.Event()
    pending = iter(jobs)
    active = {}
    exhausted = False
    def drain():
        while True:
            try:
                on_event(events.get_nowait())
            except queue.Empty:
                return
    def report_finished():
        for domain, (job, process) in list(active.items()):
            if process.exitcode is not None:
                process.join()
                on_result(_outcome(job, process))
                process.close()
                del active[domain]
    previous_term = None
    interrupted = False
    if mp.current_process().name == 'MainProcess':
        # SIGTERM gets the same checkpoint/cleanup path as Ctrl-C.
        try:
            previous_term = signal.signal(signal.SIGTERM, _interrupt)
        except ValueError:  # Programmatic invocation outside the main thread.
            pass
    try:
        while active or not exhausted:
            while len(active) < limit and not exhausted:
                try:
                    job = next(pending)
                except StopIteration:
                    exhausted = True
                    break
                process = context.Process(target=worker, args=(job, events, stop), name=f'evaluate-{job.domain}')
                try:
                    process.start()
                except Exception as exc:
                    process.close()
                    on_result(DomainOutcome(job.domain, str(job.log_path), error=f'Could not start worker: {exc}'))
                    continue
                active[job.domain] = job, process
                on_event({'event': 'domain_started', 'domain': job.domain, 'log_path': str(job.log_path),
                          'pid': process.pid})
            drain()
            report_finished()
            if active:
                time.sleep(.05)
        drain()
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        if active:
            stop.set()
            for _, process in active.values():
                if process.is_alive():
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(process.pid, signal.SIGINT)
            deadline = time.monotonic() + 3
            while any(p.is_alive() for _, p in active.values()) and time.monotonic() < deadline:
                # No callbacks during cleanup: a failing output writer must not
                # prevent worker termination or replace the original exception.
                with contextlib.suppress(queue.Empty, EOFError, OSError):
                    events.get_nowait()
                time.sleep(.05)
            for job, process in active.values():
                if process.is_alive():
                    process.terminate()
                process.join(timeout=3)
                if process.is_alive():
                    process.kill()
                    process.join()
                if interrupted:
                    outcome = _outcome(job, process)
                    if outcome.error:
                        outcome.error = 'Interrupted: ' + outcome.error
                    # Preserve the interrupt and finish stopping every worker
                    # even if the checkpoint destination becomes unavailable.
                    try:
                        on_result(outcome)
                    except Exception:
                        logging.exception('Could not checkpoint interrupted domain %s', job.domain)
                process.close()
        events.close()
        events.join_thread()
        if previous_term is not None:
            signal.signal(signal.SIGTERM, previous_term)


def _interrupt(signum, frame):
    raise KeyboardInterrupt
