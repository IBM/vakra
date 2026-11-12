import asyncio, importlib.util, json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('parity',ROOT/'run_parity.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
async def main():
 for path in sorted(list(ROOT.glob('*_code_engine.json'))+list(ROOT.glob('*_code_engine_diagnostic.json'))):
  r=json.loads(path.read_text())
  if not ('SSL' in r.get('exception','') or '502 Bad Gateway' in r.get('exception','')):continue
  if r.get('previous_attempt'):continue
  cap=r['capability'];uuid=r['uuid'];domain=r['domain']
  sample_path=m.DATA/f'capability_{cap}_{m.NAMES[cap]}'/'output'/f'{domain}.json'
  sample=next(x for x in json.loads(sample_path.read_text()) if x['uuid']==uuid);sample['domain']=domain
  backup=path.with_name(path.stem+'_attempt1.json')
  path.rename(backup)
  print('RETRY',cap,domain,uuid,flush=True)
  config=m.MCPConnectionConfig(**r['connection'])
  newer=await m.run_backend(cap,sample,r['backend'],config,m.make_tools(sample,cap))
  newer['previous_attempt']=backup.name
  m.dump(path,newer)
  print('RESULT',len(newer['calls']),newer.get('exception'),newer['tool_errors'],flush=True)
asyncio.run(main())
