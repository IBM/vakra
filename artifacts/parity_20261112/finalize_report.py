import csv
import importlib.util
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('parity',ROOT/'run_parity.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
manifest=json.loads((ROOT/'manifest.json').read_text())
rows=[]
for item in manifest:
 prefix=f'cap{item["capability"]}_{item["uuid"]}'
 lp,rp=ROOT/f'{prefix}_local.json',ROOT/f'{prefix}_code_engine.json'
 if not lp.exists() or not rp.exists():continue
 left,right=json.loads(lp.read_text()),json.loads(rp.read_text())
 if not left.get('finished') or not right.get('finished'):continue
 samples=json.loads(Path(item['source_file']).read_text())
 sample=samples[item['sample_index']];sample['domain']=item['domain']
 report={**item,**m.compare(left,right,m.make_tools(sample,item['capability']))}
 serialization=[]
 for i,(a,b) in enumerate(zip(left['calls'],right['calls'])):
  if a.get('result')!=b.get('result') and a.get('semantic_result')==b.get('semantic_result'):
   x=a.get('result',{}).get('content',[{}])[0].get('text','')
   y=b.get('result',{}).get('content',[{}])[0].get('text','')
   position=next((j for j,(v,w) in enumerate(zip(x,y)) if v!=w),min(len(x),len(y)))
   serialization.append({'call':i+1,'tool':a['tool'],'first_differing_character':position,
                         'local_excerpt':x[max(0,position-30):position+100],
                         'code_engine_excerpt':y[max(0,position-30):position+100]})
 report['serialization_differences']=serialization
 if item['capability']==1:

  dp=ROOT/f'{prefix}_code_engine_diagnostic.json'
  if dp.exists():
   d=json.loads(dp.read_text())
   if d.get('finished'):
    report['diagnostic_only']=m.compare(left,d,m.make_tools(sample,1))
    if d.get('previous_attempt'):
     first=json.loads((ROOT/d['previous_attempt']).read_text())
     report['diagnostic_first_attempt']=m.compare(left,first,m.make_tools(sample,1))
     report['diagnostic_retry']={'first_exception':first.get('exception'),'final_exception':d.get('exception'),'first_attempt_file':d['previous_attempt']}
 if right.get('previous_attempt'):
  original=json.loads((ROOT/right['previous_attempt']).read_text())
  report['transport_retry']={'attempt_1_exception':original.get('exception'),'attempt_1_file':right['previous_attempt'],
                              'attempt_2_exception':right.get('exception'),'recovered':not right.get('exception')}
 report['local_output_file']=lp.name;report['code_engine_output_file']=rp.name
 m.dump(ROOT/f'{prefix}_comparison.json',report)
 rows.append(report)
m.write_report(manifest)
with (ROOT/'samples.csv').open('w',newline='') as file:
 writer=csv.DictWriter(file,fieldnames=['capability','domain','uuid','sample_index','turn_count','call_count','status','raw_results_equal','local_calls','code_engine_calls','local_errors','code_engine_errors'])
 writer.writeheader()
 for item in manifest:
  r=next((r for r in rows if r['capability']==item['capability'] and r['uuid']==item['uuid']),{**item,'status':'pending'})
  writer.writerow({k:r.get(k) for k in writer.fieldnames})
retry_rows=[r for r in rows if r.get('transport_retry')]
extra=['','## Transport retries','',f'{len(retry_rows)} sample runs had an SSL transport error on the first attempt. Each was rerun once in a fresh session. Original attempts are preserved.']
for r in retry_rows:
 extra.append(f'- Capability {r["capability"]}, {r["domain"]}, `{r["uuid"]}`: recovered={r["transport_retry"]["recovered"]}, final result={r["status"]}.')
extra+=['','## Artifacts and reproduction','',
 '`manifest.json` records domain, UUID, original file/index, file SHA-256, turns and tool-call count for every sample. '
 '`samples.csv` is the inventory grouped by capability and domain. Each `*_local.json` and `*_code_engine.json` contains the full raw MCP results, parsed comparison values, resolved arguments and timing for every call. '
 'Capability 1 also has `*_code_engine_diagnostic.json` files. `*_comparison.json` provides per-call difference paths and backend errors.','',
 'Seven capability 4 samples have byte-level JSON serialization differences (for example, retriever metadata key ordering) with identical parsed values. These are listed per sample and call, and are not semantic discrepancies. No retriever document IDs, scores or content were removed for comparison.','',
 'No samples were replaced because of an error. No input arguments were repaired. Capability 1 label resolution, initialization and schema coercion use the evaluator implementation; other capabilities replay their calls unchanged except the evaluator\'s normal schema coercion. '
 'All turns are included, with no last-N truncation. Runtime get_data handles are compared as returned; arrays are not sorted and retriever scores/IDs are not discarded.','',
 'The hosted capability 1 health check lacks `persistent_backends: true`. All ten strict Code Engine runs stop at that compatibility check. '
 'The separately labeled diagnostic attempts bypass the guard only to replay the same calls against the old deployment; they do not validate the new bridge. '
 'The new bridge requires deployment before full capability 1 parity can be established.','',
 'Re-run from the repository root with the MCP 1.x environment: `python artifacts/parity_20261112/run_parity.py`, then `python artifacts/parity_20261112/retry_transport.py` for recorded SSL failures, then `python artifacts/parity_20261112/finalize_report.py`. '
 'The main runner resumes completed backend records; use a new artifact directory for an entirely fresh experiment.']
with (ROOT/'REPORT.md').open('a') as file:file.write('\n'.join(extra)+'\n')
print('Completed comparisons:',len(rows))
for cap in range(1,5):
 print(cap,{status:sum(r['capability']==cap and r['status']==status for r in rows) for status in ['pass','matching_errors','discrepancy','blocked']})
