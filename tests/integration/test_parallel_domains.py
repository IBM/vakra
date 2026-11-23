"""Real spawned evaluator domains -> HTTP MCP, with deterministic judge trials."""
import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
import json
from pathlib import Path
import socket
import sys
import threading
import time

import pytest
import uvicorn
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import Tool, TextContent

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'evaluator'), str(ROOT / 'tests' / 'fixtures')]
import evaluator as entry
from tool_backends import EvaluatorMCPConnectionConfig
from parallel_judges import FixedJudge, FixedGroundedness


def make_policy():
    return entry.CapabilityPolicy(correctness_judge=FixedJudge(), exactmatch_judge=FixedJudge(),
        groundedness_judge=FixedGroundedness({'trial_scores': [0.0, 1.0, 1.0]}, k=3))


def write_domain(folder, domain, turns=1):
    samples = []
    for i in range(2):
        output = []
        for t in range(turns):
            output.append({'turn_id': t, 'query': f'{domain} question', 'answer': 'answer',
                'sequence': {'tool_call': [{'name': 'echo', 'arguments': {'value': domain}}],
                             'tool_response': [json.dumps({'value': domain})]}})
        if folder.name == 'pred':
            output = output[-1:]  # Predictions contain only the final turn.
        samples.append({'uuid': f'{domain}-sample-{i}', 'domain': domain, 'output': output})
    (folder / f'{domain}.json').write_text(json.dumps(samples))


@contextmanager
def http_server():
    current = ContextVar('domain', default='')
    calls = []
    server = Server('parallel-domain-test')
    @server.list_tools()
    async def tools():
        return [Tool(name='echo', description='Echo', inputSchema={'type': 'object',
            'properties': {'value': {'type': 'string'}}, 'required': ['value']})]
    @server.call_tool()
    async def call(name, arguments):
        domain = current.get()
        assert arguments['value'] == domain
        calls.append(domain)
        await asyncio.sleep(.02)
        return [TextContent(type='text', text=json.dumps({'value': arguments['value']}))]
    manager = StreamableHTTPSessionManager(app=server, stateless=True, json_response=True)
    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            async with manager.run():
                await receive()
                await send({'type': 'lifespan.startup.complete'})
                await receive()
                await send({'type': 'lifespan.shutdown.complete'})
            return
        token = current.set(scope['path'].split('/')[-1])
        try:
            await manager.handle_request(scope, receive, send)
        finally:
            current.reset(token)
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    host = uvicorn.Server(uvicorn.Config(app, log_level='warning', access_log=False))
    thread = threading.Thread(target=host.run, kwargs={'sockets': [listener]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not host.started:
        if time.monotonic() > deadline:
            raise RuntimeError('Test HTTP server did not start')
        time.sleep(.01)
    try:
        yield f'http://127.0.0.1:{port}/mcp/{{domain}}', calls
    finally:
        host.should_exit = True
        thread.join(timeout=10)
        listener.close()
        assert not thread.is_alive()


@pytest.mark.parametrize('capability', ['capability_dashboard_apis', 'capability_multihop_reasoning', 'capability_multiturn'])
def test_parallel_metrics_failure_checkpoints_logs_and_resume(tmp_path, capability, capsys):
    gt, pred = tmp_path / 'gt', tmp_path / 'pred'
    gt.mkdir(); pred.mkdir()
    turns = 2 if capability == 'capability_multiturn' else 1
    for folder in (gt, pred):
        for domain in ('alpha', 'beta'):
            write_domain(folder, domain, turns)
    (gt / 'broken.json').write_text('{')
    output = tmp_path / 'results.json'
    with http_server() as (url, calls):
        config = EvaluatorMCPConnectionConfig(mode='http', server_url=url, backend='code-engine')
        expected = {}
        for domain in ('alpha', 'beta'):
            expected[domain], _ = asyncio.run(entry.evaluate_domain(domain, gt / f'{domain}.json',
                pred / f'{domain}.json', make_policy(), config, capability, progress_callback=lambda e: None))
        calls.clear()
        invalid_checkpoints = []
        stopped = threading.Event()
        def reader():
            while not stopped.is_set():
                try:
                    json.loads(output.read_text())
                except FileNotFoundError:
                    pass
                except json.JSONDecodeError as exc:
                    invalid_checkpoints.append(str(exc))
                stopped.wait(.005)
        watch = threading.Thread(target=reader)
        watch.start()
        try:
            result = entry.evaluate_capability(capability, gt, pred, output, {capability: make_policy()},
                config, tool_backend='code-engine', num_domains=2)
        finally:
            stopped.set(); watch.join()
        assert not invalid_checkpoints
        assert result['execution']['status'] == 'partial'
        assert result['execution']['max_workers'] == 2
        assert set(result['domains']) == {'alpha', 'beta'}
        assert set(result['failed_domains']) == {'broken'}
        assert 'JSONDecodeError' in result['failed_domains']['broken']['error']
        assert result['summary']['n_samples'] == 4
        assert result['summary']['mean_dialogue_score'] == 0
        assert result['summary']['pass_at_k'] == 1
        assert result['summary']['pass_power_k'] == 0
        assert result['groundedness_k'] == 3
        assert result['domains'] == expected
        for domain in ('alpha', 'beta'):
            assert calls.count(domain) == 2 * (1 + turns)  # final prediction + all GT turns
            text = Path(result['domain_logs'][domain]).read_text()
            assert text.count('JUDGE_CREATED') == 3
            assert f'"uuid": "{domain}-sample-0"' in text
            assert 'tool_completed' in text and 'sample_scored' in text
            assert '\r' not in text
            other = 'beta' if domain == 'alpha' else 'alpha'
            assert f'"domain": "{other}"' not in text
        assert 'Traceback' in Path(result['domain_logs']['broken']).read_text()
        first_logs = dict(result['domain_logs'])
        calls.clear()
        for folder in (gt, pred): write_domain(folder, 'broken', turns)
        resumed = entry.evaluate_capability(capability, gt, pred, output, {capability: make_policy()},
            config, tool_backend='code-engine', num_domains=1)
        assert resumed['execution']['status'] == 'completed'
        assert resumed['execution']['max_workers'] == 1
        assert resumed['failed_domains'] == {}
        assert set(resumed['domains']) == {'alpha', 'beta', 'broken'}
        assert resumed['summary']['n_samples'] == 6
        assert set(calls) == {'broken'}
        assert resumed['domain_logs']['alpha'] == first_logs['alpha']
        assert resumed['domain_logs']['broken'] != first_logs['broken']
        assert json.loads(output.read_text()) == resumed
    text = capsys.readouterr().out
    assert '[alpha]' in text and '[beta]' in text
    assert 'INCOMPLETE' in text
    assert 'Skipping already-completed domain: alpha' in text
