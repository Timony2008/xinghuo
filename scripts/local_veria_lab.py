#!/usr/bin/env python3
"""Local synthetic targets + existing Veria runtime; never contacts TSecBench.

--check exercises real Docker/private member tools without a model.
--run uses real SDK/models and the existing supervisor against synthetic targets.
This tests orchestration and tool routing, not benchmark-solving performance.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
from pathlib import Path
import shlex
import sqlite3
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from xinghuo.config import Config
from xinghuo.core import Challenge, SubmissionReceipt
from xinghuo.store import Store, atomic_json


SERVER = r'''
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        state = json.loads(Path('/lab/fixture.json').read_text())
        status, body = 200, {'mode':'local_synthetic', 'challenge':state['code']}
        if self.path == '/health':
            body['ready'] = True
        elif self.path in ('/proof/one', '/proof/two'):
            if not state['active']:
                status, body = 410, {'error':'Local fixture is not active.'}
            elif not state['released']:
                status, body = 425, {'error':'Local concurrency barrier: wait for all configured challenges to start, then retry.'}
            else:
                body['flag'] = state['flags'][0 if self.path.endswith('one') else 1]
        elif self.path == '/':
            body['instructions'] = 'Synthetic diagnostic only. Read /proof/one and /proof/two; submit both distinct flags. HTTP 425 means wait for the concurrency barrier.'
        else:
            status, body = 404, {'error':'Unknown synthetic endpoint.'}
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)
    def log_message(self, *args):
        pass
ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
'''


async def docker(*args, check=True, timeout=60):
    process = await asyncio.create_subprocess_exec('docker', *map(str, args),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(process.communicate(), timeout)
    except BaseException:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        raise
    result = {'returncode':process.returncode, 'stdout':out.decode(errors='replace'),
              'stderr':err.decode(errors='replace')}
    if check and process.returncode:
        raise RuntimeError('Docker operation failed: ' + result['stderr'][-1200:])
    return result


class LocalPlatform:
    """An explicit synthetic judge; its flags never enter the model context."""
    def __init__(self, directory, run_id, parallel):
        self.directory, self.run_id, self.parallel = Path(directory), run_id, parallel
        self.rows = {f'lab-{n}': {'active':False, 'accepted':[],
            'flags':[f'flag{{local_{uuid.uuid4().hex}}}' for _ in range(2)]}
            for n in range(1, 4)}
        self.peak_active, self.released = 0, False
        self.network = 'xinghuo-lab-' + run_id
        self.resources = []
        self.network_id = None
        self.image = None
        self.flush()

    def address(self, code):
        return f'http://xinghuo-{self.run_id}-{code}:8080'

    def flush(self, codes=None):
        for code in codes if codes is not None else self.rows:
            row=self.rows[code]
            atomic_json(self.directory / code / 'fixture.json',
                {'code':code, 'flags':row['flags'], 'active':row['active'], 'released':self.released})

    async def list_challenges(self):
        return [Challenge(code, 'Local synthetic diagnostic, not a contest task. '
            'The two independent endpoints /proof/one and /proof/two each return one random flag. '
            'Connect to the supplied service and submit both immediately. HTTP 425 means the '
            'local concurrency barrier is waiting for other managed challenges; retry after a short delay. '
            'Do not attack any other service. This checks real SDK/tools/submission plumbing, not exploit skill.',
            completed=len(row['accepted'])==2, difficulty='synthetic', total_score=2,
            flag_count=2, correct_flag_count=len(row['accepted']),
            container_status='available' if row['active'] else 'stopped',
            container_addr=(self.address(code),) if row['active'] else ())
            for code, row in self.rows.items()]

    async def start(self, code):
        if sum(row['active'] for row in self.rows.values()) >= self.parallel and not self.rows[code]['active']:
            raise RuntimeError('Local synthetic concurrency limit exceeded')
        self.rows[code]['active'] = True
        self.peak_active = max(self.peak_active, sum(row['active'] for row in self.rows.values()))
        previous_release=self.released
        self.released |= self.peak_active >= self.parallel
        self.flush(None if self.released!=previous_release else [code])
        return (self.address(code),)

    async def submit(self, code, flag):
        row = self.rows[code]
        correct = row['active'] and self.released and flag in row['flags']
        duplicate = correct and flag in row['accepted']
        if correct and not duplicate:
            row['accepted'].append(flag)
        return SubmissionReceipt(correct=bool(correct), duplicate=bool(duplicate),
            awarded=1 if correct and not duplicate else 0, cumulative_score=len(row['accepted']),
            correct_flag_count=len(row['accepted']), total_flag_count=2,
            matched_flag_index=row['flags'].index(flag)+1 if correct else None)

    async def close(self, code):
        self.rows[code]['active'] = False
        self.flush([code])

    async def setup(self, image):
        inspection = await docker('image','inspect',image)
        self.image = json.loads(inspection['stdout'])[0]['Id']
        created = await docker('network','create','--label',f'xinghuo.local-lab={self.run_id}',self.network)
        self.network_id = created['stdout'].strip()
        for code in self.rows:
            print(json.dumps({'event':'local_target_starting','code':code}),flush=True)
            # Record the unique name before dispatch, so an interrupted CLI can
            # still be cleaned up after Docker actually created the container.
            name = f'xinghuo-{self.run_id}-{code}'
            self.resources.append(name)
            await docker('run','-d','--name',name,'--network',self.network,
                '--label',f'xinghuo.local-lab={self.run_id}', '--cpus','0.25','--memory','128m',
                '--pids-limit','64','--read-only','--cap-drop','ALL',
                '--security-opt','no-new-privileges',
                '--mount',f'type=bind,src={self.directory / code},dst=/lab,readonly',
                '--entrypoint','/opt/ctf/bin/python3',self.image,'-B','-u','-c',SERVER)
            await docker('exec',name,'/opt/ctf/bin/python3','-c',
                "import urllib.request,time\nfor n in range(50):\n try:\n  urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=1);break\n except OSError:\n  time.sleep(.1)\nelse: raise SystemExit('local fixture did not start')")

    async def cleanup(self):
        errors = []
        for name in self.resources:
            result = await docker('inspect',name,check=False)
            if result['returncode']:
                if 'No such' not in result['stderr']:
                    errors.append('Cannot inspect owned target: '+name)
                continue
            actual = json.loads(result['stdout'])[0]
            if actual.get('Config',{}).get('Labels',{}).get('xinghuo.local-lab') != self.run_id:
                errors.append('Ownership mismatch; left untouched: '+name)
                continue
            logs=await docker('logs',actual['Id'],check=False)
            code=name.rsplit('-',2)[-2]+'-'+name.rsplit('-',1)[-1]
            (self.directory/code/'server.log').write_text(logs['stdout']+logs['stderr'])
            removed = await docker('rm','-f',actual['Id'],check=False)
            if removed['returncode']:
                errors.append('Cannot remove owned target: '+name)
        if self.network_id:
            result = await docker('network','inspect',self.network_id,check=False)
            if result['returncode']==0:
                actual=json.loads(result['stdout'])[0]
                if actual.get('Labels',{}).get('xinghuo.local-lab')==self.run_id:
                    removed=await docker('network','rm',self.network_id,check=False)
                    if removed['returncode']:
                        errors.append('Owned network remains; inspect for remaining lab containers: '+self.network)
                else:
                    errors.append('Network ownership mismatch; left untouched')
            elif 'No such' not in result['stderr']:
                errors.append('Cannot inspect owned network')
        return errors


class Monitor:
    """Read existing journals without changing runtime/tool behavior."""
    def __init__(self, directory):
        self.directory=Path(directory)
        self.cursors={}
        self.members={}

    def scan(self):
        for database in (self.directory/'teams').glob('*/room.sqlite3'):
            code=database.parent.name
            try:
                with contextlib.closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=.1)) as db:
                    rows=db.execute('SELECT seq,type,sender,data,created_at FROM events WHERE seq>? ORDER BY seq LIMIT 1000',
                                    (self.cursors.get(code,0),)).fetchall()
            except sqlite3.OperationalError:
                continue
            for seq,event_type,member,raw,created_at in rows:
                self.cursors[code]=seq
                data=json.loads(raw)
                kind=data.get('kind') if event_type=='evidence' else event_type
                key=code+'/'+member
                record=self.members.setdefault(key,{'events':0,'tool_calls':0,'tool_results':0,'errors':0})
                record['events']+=1
                record['last_event_at']=created_at
                record['last_kind']=kind
                record['tool_calls']+=int(kind=='tool_call')
                record['tool_results']+=int(kind=='tool_execution_result')
                record['errors']+=int(kind in {'sdk_error','assistant_error','tool_routing_error','role_failed'})
                if kind in {'started','tool_call','tool_execution_result','turn_result','sdk_error',
                            'assistant_error','tool_routing_error','role_failed','candidate_flag','submission_result'}:
                    print(json.dumps({'event':'member_event','code':code,'member':member,'kind':kind,
                        'tool':data.get('metadata',{}).get('name'),'evidence_ref':data.get('evidence_ref')},ensure_ascii=False),flush=True)
        now=time.time()
        return {key:{**row,'seconds_since_event':round(now-row['last_event_at'],1)} for key,row in self.members.items()}


async def tools_check(platform, config, directory):
    from xinghuo.veria.environment import SharedDockerEnvironment
    environments=[]
    try:
        async def one(code):
            print(json.dumps({'event':'local_tools_check_starting','code':code,'private_members':6}),flush=True)
            env=SharedDockerEnvironment(directory/'teams'/code,platform.run_id,code,'local-check',config)
            environments.append(env)
            await env.start()
            source=("import json,requests; root="+repr(platform.address(code))+"; "
                "print(json.dumps([requests.get(root+p,timeout=10).json()['flag'] for p in ['/proof/one','/proof/two']]))")
            members=[f'{family}-{n}' for family in ('pro','flash') for n in (1,2,3)]
            outcomes=await asyncio.gather(*(env.execute(member,shlex.join(['python','-c',source]),timeout=60) for member in members),return_exceptions=True)
            results=[{'returncode':-1,'stdout':'','stderr':type(row).__name__+': '+str(row)}
                     if isinstance(row,BaseException) else row for row in outcomes]
            evidence=directory/'teams'/code/'local-tool-check.json'
            atomic_json(evidence,dict(zip(members,results)))
            if any(row['returncode'] for row in results):
                raise RuntimeError('A real private member tool request failed; see '+str(evidence))
            values=[json.loads(row['stdout']) for row in results]
            if any(value!=values[0] for value in values):
                raise RuntimeError('Members did not read the same synthetic target')
            first=await platform.submit(code,values[0][0])
            assert first.correct and first.correct_flag_count==1
            assert not next(row for row in await platform.list_challenges() if row.code==code).completed
            duplicate=await platform.submit(code,values[1][0])
            assert duplicate.duplicate and duplicate.awarded==0
            second=await platform.submit(code,values[0][1])
            assert second.correct and next(row for row in await platform.list_challenges() if row.code==code).completed
            await platform.close(code)
            print(json.dumps({'event':'local_tools_check_passed','code':code,'private_members':6}),flush=True)
            return {'code':code,'private_members':len(members),'distinct_flags':2,'duplicate_verified':True}
        results=[]
        codes=list(platform.rows)
        for offset in range(0,len(codes),platform.parallel):
            batch=codes[offset:offset+platform.parallel]
            for code in batch:
                await platform.start(code)
            outcomes=await asyncio.gather(*(one(code) for code in batch),return_exceptions=True)
            failure=next((value for value in outcomes if isinstance(value,BaseException)),None)
            if failure:
                raise failure
            results.extend(outcomes)
        return {'status':'passed','checks':results,'model_calls':0,'scope':'real Docker, member tools, HTTP and synthetic judge; no model or supervisor scheduling test'}
    finally:
        cleanup=await asyncio.gather(*(env.stop() for env in environments),return_exceptions=True)
        failures=[type(value).__name__ for value in cleanup if isinstance(value,BaseException)]
        if failures:
            raise RuntimeError('Local tool container cleanup failed: '+', '.join(failures))


async def run(args):
    directory=args.run_dir.resolve()
    if directory.exists() and any(directory.iterdir()):
        raise ValueError('Choose a fresh empty --run-dir; local lab does not resume old fixtures')
    if sys.version_info[:2]!=(3,14):
        raise ValueError('Use .venv-veria/bin/python (Python 3.14)')
    if args.run:
        from xinghuo.hosted import configure_model_environment
        if not os.environ.get('DEEPSEEK_API_KEY'):
            raise ValueError('Set DEEPSEEK_API_KEY in your terminal; do not put it in a file')
        os.environ['XINGHUO_DEPLOYMENT']='local'
        os.environ.setdefault('DEEPSEEK_BASE_URL','https://api.deepseek.com/anthropic')
        configure_model_environment(os.environ)
        # Driver client_environment reads these; credentials are never passed to
        # the local target containers or written into config/result artifacts.
    run_id=uuid.uuid4().hex[:16]
    platform=LocalPlatform(directory/'lab',run_id,args.parallel)
    config=Config(team_engine='veria',backend='claude',model='deepseek-v4-pro',parallel=args.parallel,
        budget=args.duration,veria_python=os.path.abspath(sys.executable),
        veria_tool_image=args.image,veria_tool_cpu_limit=1,veria_tool_memory_limit='2g',
        veria_tool_network=platform.network,cleanup_reserve=60).validate()
    result={'status':'failed','mode':'real_models' if args.run else 'no_model_tools_check',
        'platform':'local_synthetic','tsec_benchmark_calls':0,'parallel':args.parallel,'budget_seconds':args.duration}
    atomic_json(directory/'lab-config.json',{**result,'network':platform.network,'run_id':run_id,'image':args.image})
    try:
        await platform.setup(args.image)
        if args.check:
            result.update(await tools_check(platform,config,directory))
        else:
            from xinghuo.cli import _signals
            from xinghuo.supervisor import Supervisor
            store=Store(directory)
            supervisor=Supervisor(platform,store,config)
            _signals(supervisor)
            monitor=Monitor(directory)
            async def watch():
                while True:
                    members=monitor.scan()
                    rows=store.challenges()
                    progress={'event':'local_lab_progress','status':store.get_meta('status'),
                        'active':sum(row['active'] for row in platform.rows.values()),'peak_active':platform.peak_active,
                        'completed':sum(row['status']=='completed' for row in rows),'members':members}
                    atomic_json(directory/'live.json',progress)
                    print(json.dumps({k:v for k,v in progress.items() if k!='members'}),flush=True)
                    await asyncio.sleep(2)
            observer=asyncio.create_task(watch())
            try:
                outcome=await supervisor.run()
                monitor.scan()
                completed=sum(row['status']=='completed' for row in outcome['challenges'])
                result.update(status='passed' if outcome['status']=='completed' and completed==3 and platform.peak_active==args.parallel else 'not_passed',
                    supervisor_status=outcome['status'],completed=completed,members=monitor.members,
                    model_execution='real Claude Agent SDK / DeepSeek; consult evidence for calls, errors and usage')
            finally:
                observer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await observer
                store.close()
    except Exception as error:
        detail=str(error)
        for name in ('DEEPSEEK_API_KEY','ANTHROPIC_API_KEY','ANTHROPIC_AUTH_TOKEN','BENCHMARK_TOKEN'):
            if os.environ.get(name):
                detail=detail.replace(os.environ[name],'[REDACTED]')
        result.update(error_type=type(error).__name__,error=detail)
        raise RuntimeError(detail) from None
    finally:
        result['peak_active']=platform.peak_active
        result['cleanup_errors']=await platform.cleanup()
        if result['cleanup_errors']:
            result['status']='failed'
        atomic_json(directory/'result.json',result)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0 if result['status']=='passed' else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--check',action='store_true',help='Real Docker/tools check; no model calls')
    mode.add_argument('--run',action='store_true',help='Real model calls against only local synthetic targets')
    parser.add_argument('--run-dir',type=Path,default=ROOT/'runs'/('local-veria-lab-'+time.strftime('%Y%m%d-%H%M%S')))
    parser.add_argument('--parallel',type=int,choices=(1,2,3),default=3)
    parser.add_argument('--duration',type=float,default=600)
    parser.add_argument('--image',default='xinghuo-veria-tools:local')
    args=parser.parse_args()
    try:
        return asyncio.run(run(args))
    except (ValueError,RuntimeError) as error:
        print(f'{type(error).__name__}: {error}',file=sys.stderr)
        return 1


if __name__=='__main__':
    raise SystemExit(main())
