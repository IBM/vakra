"""Sample-scoped judge diagnostics and the evaluator's separate error report."""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

_context = ContextVar('evaluation_error_context', default=None)


@contextmanager
def llm_error_context(records, **identity):
    token = _context.set((records, identity))
    try:
        yield
    finally:
        _context.reset(token)


def record_llm_error(judge, error, *, phase, trial=None):
    context = _context.get()
    if context is None:
        return
    records, identity = context
    records.append({**identity, 'type': 'llm_call', 'judge': type(judge).__name__,
                    'phase': phase, 'trial': trial, 'message': f'{type(error).__name__}: {error}',
                    'time': datetime.now(timezone.utc).isoformat()})


def error_report_path(output):
    output = Path(output)
    return output.with_name(output.stem + '.errors.json')


def _counts(records):
    gt = [r for r in records if r['type'] == 'ground_truth_tool_call']
    llm = [r for r in records if r['type'] == 'llm_call']
    return {'error_count': len(records), 'ground_truth_tool_call_errors': len(gt),
            'unresolved_ground_truth_calls': sum(not r['recovered'] for r in gt),
            'recovered_ground_truth_calls': sum(r['recovered'] for r in gt),
            'failed_ground_truth_attempts': sum(r['failed_attempt_count'] for r in gt),
            'llm_call_errors': len(llm)}


def build_error_report(results, output):
    domains, all_records = {}, []
    for domain, records in sorted(results.get('evaluation_errors', {}).items()):
        samples = {}
        for record in records:
            uuid = str(record.get('uuid'))
            samples.setdefault(uuid, []).append(record)
        domains[domain] = {**_counts(records), 'samples': {
            uuid: {**_counts(errors), 'errors': errors} for uuid, errors in sorted(samples.items())}}
        all_records.extend(records)
    # Older checkpoints cannot retrospectively supply errors they never recorded.
    unavailable = sorted(set(results.get('domains', {})) - set(results.get('evaluation_errors', {})))
    return {'capability_name': results.get('capability_name'), 'results_file': str(output),
            'summary': _counts(all_records), 'domains': domains,
            'domains_without_error_tracking': unavailable}


def write_error_report(results, output):
    from domain_runner import atomic_write_json
    path = error_report_path(output)
    report = build_error_report(results, output)
    atomic_write_json(path, report)
    return report
