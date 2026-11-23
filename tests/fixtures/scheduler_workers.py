"""Small spawn-safe workers for exercising the actual process scheduler."""
import os
from pathlib import Path
import time
from domain_runner import DomainOutcome, atomic_write_json


def tracked_worker(job, events, stop):
    ready = job.log_path.with_suffix('.ready')
    ready.parent.mkdir(parents=True, exist_ok=True)
    ready.touch()
    width = job.policy_spec.get('barrier_width', 1)
    deadline = time.monotonic() + 8
    while len(list(ready.parent.glob('*.ready'))) < width:
        if time.monotonic() > deadline:
            raise RuntimeError('Test worker barrier timed out')
        time.sleep(.01)
    started = time.monotonic()
    events.put({'event': 'sample_scored', 'domain': job.domain, 'completed': 1, 'total': 1})
    if job.policy_spec.get('crash'):
        os._exit(7)
    time.sleep(.25)
    data = {'started': started, 'ended': time.monotonic(), 'pid': os.getpid()}
    outcome = DomainOutcome(job.domain, str(job.log_path), data=data, scores=[1.0],
                            error='deliberate domain failure' if job.policy_spec.get('fail') else None)
    atomic_write_json(job.result_path, vars(outcome))
