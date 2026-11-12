#!/usr/bin/env python3
"""Replay 10 documented samples/capability; preserve raw tool results and errors.

No LLM judge, no scoring, no output sorting or error-to-success normalization.
CE capability 1's guard is tested first; a separate diagnostic run may bypass
only that guard to expose the behavior of the currently deployed old backend.
"""
import asyncio
import copy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time

REPO = Path('/root/vakra')
sys.path[:0] = [str(REPO / 'evaluator'), str(REPO)]
from benchmark.mcp_client import MCPConnectionConfig, create_client_and_connect, load_mcp_config
from mcp_tools import extract_toolcalls_for_mcp, execute_tools_batch

ROOT = Path(__file__).resolve().parent
DATA = Path('/root/v14/internal/test')
NAMES = {1: 'bi_apis', 2: 'dashboard_apis', 3: 'multihop_reasoning', 4: 'multiturn'}
SELECTION = {
 1: [('beer_factory',0),('beer_factory',1),('book_publishing_company',2),('books',4),('books',10),('disney',4),('computer_student',0),('craftbeer',0),('craftbeer',1),('college_completion',1)],
 2: [(d,i) for d in ('california_schools','financial','formula_1','movie','superhero') for i in (0,1)],
 3: [('bpo',i) for i in (0,2,3,9,10)] + [('beer_factory',0),('beer_factory',3),('books',0),('books',1),('movie',0)],
 4: [('disney',0),('disney',5),('disney',6),('beer_factory',0),('beer_factory',17),('books',0),('books',6),('hockey',0),('hockey',7),('movie',4)],
}


def dump(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str))
    temporary.replace(path)


def unwrap_error(exc):
    if isinstance(exc, BaseExceptionGroup):
        return f'{type(exc).__name__}: ' + '; '.join(unwrap_error(e) for e in exc.exceptions)
    return f'{type(exc).__name__}: {exc}'


def parsed(text):
    try: return json.loads(text)
    except (TypeError, ValueError): return text


def has_error(value):
    if isinstance(value, dict):
        return bool(value.get('error')) or any(has_error(v) for v in value.values())
    if isinstance(value, list): return any(has_error(v) for v in value)
    return isinstance(value, str) and value.lower().startswith(('error:', 'input validation error', 'error calling tool', 'unknown tool'))


def semantic(result):
    if result is None: return None
    result = copy.deepcopy(result)
    for block in result.get('content', []):
        if block.get('type') == 'text': block['text'] = parsed(block.get('text', ''))
    return result


def diff(a, b, path='$'):
    if type(a) != type(b):
        # Numeric type differences are reported, not silently rounded/coerced.
        return {'path':path,'local':a,'code_engine':b,'reason':'different types'}
    if isinstance(a, dict):
        if set(a) != set(b):
            return {'path':path,'local_keys':sorted(a),'code_engine_keys':sorted(b),'reason':'different keys'}
        for k in a:
            d=diff(a[k],b[k],path+'.'+k)
            if d:return d
    elif isinstance(a,list):
        if len(a)!=len(b):return {'path':path,'local_length':len(a),'code_engine_length':len(b),'reason':'different lengths'}
        for i,(x,y) in enumerate(zip(a,b)):
            d=diff(x,y,f'{path}[{i}]')
            if d:return d
    elif a!=b:return {'path':path,'local':a,'code_engine':b,'reason':'different values'}
    return None


def make_tools(sample, cap):
    tools=copy.deepcopy(extract_toolcalls_for_mcp(sample))
    if cap==1 and tools:
        label=None
        if tools[0] and tools[0][0]['name'] in ('get_data','initialize_active_data'):
            label=tools[0].pop(0).get('label')
        initial={'name':'get_data','arguments':{'tool_universe_id':sample['uuid'],'reset_handles':True}}
        if label:initial['label']=label
        tools[0].insert(0,initial)
    return tools


class RecordingSession:
    def __init__(self,session,record,path):self.session,self.record,self.path=session,record,path
    async def call_tool(self,name,args):
        entry={'tool':name,'resolved_arguments':copy.deepcopy(args),'started_at':datetime.now(timezone.utc).isoformat()}
        self.record['calls'].append(entry)
        start=time.monotonic()
        try:
            result=await asyncio.wait_for(self.session.call_tool(name,args),300)
            payload=result.model_dump(by_alias=True,exclude_none=True)
            entry['result']=payload
            entry['semantic_result']=semantic(payload)
            entry['tool_error']=bool(payload.get('isError') or payload.get('is_error') or has_error(entry['semantic_result']))
            return result
        except BaseException as exc:
            entry['exception']=unwrap_error(exc)
            raise
        finally:
            entry['seconds']=round(time.monotonic()-start,3)
            dump(self.path,self.record)


async def run_backend(cap,sample,backend,config,tools):
    path=ROOT/f'cap{cap}_{sample["uuid"]}_{backend}.json'
    if path.exists():
        saved=json.loads(path.read_text())
        if saved.get('finished'):return saved
    record={'capability':cap,'domain':sample['domain'],'uuid':sample['uuid'],'backend':backend,
            'connection':asdict(config),'calls':[],'finished':False}
    start=time.monotonic()
    try:
        async with asyncio.timeout(1200):
            async with create_client_and_connect(config,sample['domain']) as session:
                listed=(await session.list_tools()).tools
                schemas={tool.name:getattr(tool,'input_schema',None) or getattr(tool,'inputSchema',{}) for tool in listed}
                record['listed_tools']=len(schemas)
                if cap==1 and backend=='local' and 'reset_handles' not in schemas.get('get_data',{}).get('properties',{}):
                    raise RuntimeError('Local capability 1 is missing reset_handles')
                recorder=RecordingSession(session,record,path)
                record['replay_responses']=await execute_tools_batch(recorder,[copy.deepcopy(tools)],schemas,is_gt=cap==1)
    except BaseException as exc:
        if isinstance(exc,KeyboardInterrupt):raise
        record['exception']=unwrap_error(exc)
    record['seconds']=round(time.monotonic()-start,3)
    record['finished']=True
    record['tool_errors']=sum(bool(c.get('tool_error') or c.get('exception')) for c in record['calls'])
    dump(path,record)
    return record


def compare(local,remote,tools):
    expected=sum(len(t) for t in tools)
    errors=[]
    matching_errors=[]
    for i in range(max(len(local['calls']),len(remote['calls']))):
        left=local['calls'][i] if i<len(local['calls']) else None
        right=remote['calls'][i] if i<len(remote['calls']) else None
        if left is None or right is None:
            errors.append({'call':i+1,'tool':(left or right)['tool'],'reason':'call not executed on one backend'})
            continue
        difference=diff(left.get('semantic_result'),right.get('semantic_result'))
        if difference:
            errors.append({'call':i+1,'tool':left['tool'],'difference':difference})
        elif left.get('tool_error') or right.get('tool_error') or left.get('exception') or right.get('exception'):
            matching_errors.append({'call':i+1,'tool':left['tool'],'reason':'both backends returned the same error'})
    complete=all(len(r['calls'])==expected and not r.get('exception') for r in (local,remote))
    passed=complete and not errors and not local['tool_errors'] and not remote['tool_errors']
    return {'status':'pass' if passed else 'matching_errors' if complete and not errors else 'blocked' if remote.get('exception') and not remote['calls'] else 'discrepancy',
            'expected_calls':expected,'local_calls':len(local['calls']),'code_engine_calls':len(remote['calls']),
            'local_errors':local['tool_errors'],'code_engine_errors':remote['tool_errors'],
            'local_exception':local.get('exception'),'code_engine_exception':remote.get('exception'),
            'differences':errors,'matching_errors':matching_errors,
            'local_tool_errors':[{'call':i+1,'tool':c['tool'],'result':c.get('semantic_result'),'exception':c.get('exception')} for i,c in enumerate(local['calls']) if c.get('tool_error') or c.get('exception')],
            'code_engine_tool_errors':[{'call':i+1,'tool':c['tool'],'result':c.get('semantic_result'),'exception':c.get('exception')} for i,c in enumerate(remote['calls']) if c.get('tool_error') or c.get('exception')],
            'raw_results_equal':complete and [c.get('result') for c in local['calls']]==[c.get('result') for c in remote['calls']]}


async def capability(cap,samples,preflight):
    locals=load_mcp_config(str(REPO/'benchmark/mcp_connection_config.yaml'))
    local=locals[cap]
    if cap==1:
        # Use current source and the exact DB/config directories mounted locally.
        local=MCPConnectionConfig(command=sys.executable,args=['-m','environment.m3.python_tools.mcp'])
    base=preflight['remote'][str(cap)]['base_url']
    remote=MCPConnectionConfig(mode='http',server_url=base+'/mcp/{domain}',require_persistent_backend=cap==1)
    for sample in samples:
        tools=make_tools(sample,cap)
        print(f'START cap={cap} {sample["domain"]} {sample["uuid"]} calls={sum(map(len,tools))}',flush=True)
        left=await run_backend(cap,sample,'local',local,tools)
        print(f'LOCAL cap={cap} {sample["uuid"]} calls={len(left["calls"])} errors={left["tool_errors"]}',flush=True)
        right=await run_backend(cap,sample,'code_engine',remote,tools)
        report={'capability':cap,'domain':sample['domain'],'uuid':sample['uuid'],**compare(left,right,tools)}
        if cap==1:
            diagnostic_config=copy.deepcopy(remote);diagnostic_config.require_persistent_backend=False
            diagnostic=await run_backend(cap,sample,'code_engine_diagnostic',diagnostic_config,tools)
            report['diagnostic_only']=compare(left,diagnostic,tools)
        dump(ROOT/f'cap{cap}_{sample["uuid"]}_comparison.json',report)
        print(f'DONE cap={cap} {sample["uuid"]} status={report["status"]} differences={len(report["differences"])}',flush=True)


def write_report(manifest):
    lines=['# Local versus Code Engine tool parity','',
      '10 selected samples per capability (40 total). Full original tool-call lists across every turn are replayed. '
      'JSON text is parsed for semantic comparison; list order, numeric types, error flags and all content blocks remain significant. '
      'Identical errors do not count as parity passes. Raw results are also retained.','',
      'Capability 1 local replay uses the current workspace source with the same database/config directories as Docker. '
      'Capabilities 2–4 use the configured running local Docker servers. Hosted services are tested as deployed; none are redeployed. '
      'Capability 1 diagnostic replay bypasses only the persistent-bridge health guard and never counts as an implementation pass.','',
      '| Capability | Domain | UUID | Calls | Result |','|---|---|---|---:|---|']
    reports=[]
    for item in manifest:
        p=ROOT/f'cap{item["capability"]}_{item["uuid"]}_comparison.json'
        r=json.loads(p.read_text()) if p.exists() else {**item,'status':'pending'}
        reports.append(r)
        lines.append(f'| {item["capability"]} | {item["domain"]} | `{item["uuid"]}` | {item["call_count"]} | {r["status"]} |')
    lines+=['','## Summary','', '| Capability | Successful match | Matching errors | Discrepancy | Blocked | Pending |','|---|---:|---:|---:|---:|---:|']
    for cap in range(1,5):
        subset=[r for r in reports if r['capability']==cap]
        counts=[sum(r['status']==s for r in subset) for s in ('pass','matching_errors','discrepancy','blocked','pending')]
        lines.append(f'| {cap} | '+' | '.join(map(str,counts))+' |')
    for r in reports:
        if r['status']=='pending' or (r['status']=='pass' and not r.get('serialization_differences')):continue
        lines+=['',f'## Capability {r["capability"]}: {r["domain"]} — {r["uuid"]}','',
                f'**{r["status"]}**. Local calls: {r.get("local_calls")}; Code Engine calls: {r.get("code_engine_calls")}.']
        for entry in r.get('serialization_differences',[]):
            lines.append('- Raw JSON serialization differs, but parsed values are identical: '+json.dumps(entry,ensure_ascii=False))
        for field in ('local_exception','code_engine_exception'):
            if r.get(field):lines.append(f'- {field}: `{r[field]}`')
        for entry in r.get('local_tool_errors',[]):
            lines.append('- Local tool error: '+json.dumps(entry,ensure_ascii=False)[:1600])
        for entry in r.get('code_engine_tool_errors',[]):
            lines.append('- Hosted tool error: '+json.dumps(entry,ensure_ascii=False)[:1600])
        for d in r.get('matching_errors',[]):
            lines.append('- '+json.dumps(d,ensure_ascii=False))
        for d in r.get('differences',[]):
            text=json.dumps(d,ensure_ascii=False)
            lines.append('- '+text[:1800]+(' … (full values in comparison JSON)' if len(text)>1800 else ''))
        if 'diagnostic_only' in r:
            for diagnostic_name in ('diagnostic_only','diagnostic_first_attempt'):
                if diagnostic_name not in r:continue
                d=r[diagnostic_name]
                lines.append(f'- Unguarded deployment {diagnostic_name}: {d["status"]}; local calls={d["local_calls"]}, hosted calls={d["code_engine_calls"]}, local errors={d["local_errors"]}, hosted errors={d["code_engine_errors"]}.')
                if d.get('code_engine_exception'):lines.append('  - Hosted exception: '+d['code_engine_exception'].replace('\n',' '))
                for detail in d['differences']:
                    text=json.dumps(detail,ensure_ascii=False)
                    lines.append('  - '+text[:1800])
            if r.get('diagnostic_retry'):
                lines.append('- Diagnostic retries: '+json.dumps(r['diagnostic_retry'],ensure_ascii=False))
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    dump(ROOT/'summary.json',reports)


async def main():
    os.environ['MCP_DB_ROOT']=str(REPO/'data/databases')
    os.environ['MCP_SERVER_TYPE']='router'
    manifest=[];selections={}
    for cap,choices in SELECTION.items():
        selected=[]
        for domain,index in choices:
            path=DATA/f'capability_{cap}_{NAMES[cap]}'/'output'/f'{domain}.json'
            sample=json.loads(path.read_text())[index]
            sample['domain']=domain
            selected.append(sample)
            manifest.append({'capability':cap,'domain':domain,'uuid':sample['uuid'],'sample_index':index,
                             'source_file':str(path),'source_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
                             'turn_count':len(sample['output']),'call_count':sum(map(len,make_tools(sample,cap)))})
        selections[cap]=selected
    dump(ROOT/'manifest.json',manifest)
    write_report(manifest)
    preflight=json.loads((ROOT/'preflight.json').read_text())
    tasks=[asyncio.create_task(capability(cap,samples,preflight)) for cap,samples in selections.items()]
    while not all(t.done() for t in tasks):
        await asyncio.wait([t for t in tasks if not t.done()],timeout=20)
        write_report(manifest)
    results=await asyncio.gather(*tasks,return_exceptions=True)
    for cap,result in enumerate(results,1):
        if isinstance(result,BaseException):
            print('CAPABILITY_FAILURE',cap,unwrap_error(result),flush=True)
    write_report(manifest)
    print('REPORT',ROOT/'REPORT.md',flush=True)

if __name__=='__main__':asyncio.run(main())
