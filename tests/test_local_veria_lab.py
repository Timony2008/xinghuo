"""Synthetic lab contracts; no models, Docker, or benchmark connections."""
import asyncio
from contextlib import closing
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

from xinghuo.veria.state import VeriaRoom


ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('local_veria_lab',ROOT/'scripts/local_veria_lab.py')
lab=importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


class LocalJudgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrency_barrier_two_flag_completion_and_duplicate(self):
        with tempfile.TemporaryDirectory() as directory:
            platform=lab.LocalPlatform(Path(directory),'synthetic-test',3)
            await platform.start('lab-1')
            self.assertFalse(platform.released)
            flag=platform.rows['lab-1']['flags'][0]
            self.assertFalse((await platform.submit('lab-1',flag)).correct)
            await platform.start('lab-2')
            await platform.start('lab-3')
            self.assertTrue(platform.released)
            self.assertEqual(platform.peak_active,3)
            # A valid flag for another case cannot earn this case's score.
            self.assertFalse((await platform.submit('lab-2',flag)).correct)
            first=await platform.submit('lab-1',flag)
            self.assertEqual(first.awarded,1)
            self.assertFalse((await platform.list_challenges())[0].completed)
            duplicate=await platform.submit('lab-1',flag)
            self.assertTrue(duplicate.duplicate)
            self.assertEqual(duplicate.awarded,0)
            await platform.submit('lab-1',platform.rows['lab-1']['flags'][1])
            self.assertTrue((await platform.list_challenges())[0].completed)
            await platform.close('lab-1')
            self.assertFalse((await platform.submit('lab-1',flag)).correct)

    async def test_cleanup_refuses_unrelated_docker_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            platform=lab.LocalPlatform(Path(directory),'owned-test',3)
            platform.resources=['unique-target']
            platform.network_id='unique-network'
            calls=[]
            async def fake_docker(*args,**kwargs):
                calls.append(args)
                data=[{'Id':'other-id','Config':{'Labels':{'xinghuo.local-lab':'other-owner'}},
                       'Labels':{'xinghuo.local-lab':'other-owner'}}]
                return {'returncode':0,'stdout':json.dumps(data),'stderr':''}
            with patch.object(lab,'docker',side_effect=fake_docker):
                errors=await platform.cleanup()
            self.assertEqual(len(errors),2)
            self.assertFalse(any('rm' in call for call in calls))


class LocalFixtureTests(unittest.TestCase):
    def test_actual_http_service_obeys_activation_barrier_and_routes(self):
        with tempfile.TemporaryDirectory() as directory:
            platform=lab.LocalPlatform(Path(directory),'http-test',3)
            source=lab.SERVER.replace("Path('/lab/fixture.json')",f'Path({str(Path(directory)/"lab-1/fixture.json")!r})')
            source=source.replace("ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()",
                "server=ThreadingHTTPServer(('127.0.0.1',0),Handler)\nprint(server.server_port,flush=True)\nserver.serve_forever()")
            process=subprocess.Popen([sys.executable,'-u','-c',source],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                base='http://127.0.0.1:'+process.stdout.readline().strip()
                with closing(urlopen(base+'/health',timeout=3)) as response:
                    self.assertTrue(json.load(response)['ready'])
                asyncio.run(platform.start('lab-1'))
                with self.assertRaises(HTTPError) as error:
                    urlopen(base+'/proof/one',timeout=3)
                self.assertEqual(error.exception.code,425)
                error.exception.close()
                asyncio.run(platform.start('lab-2'))
                asyncio.run(platform.start('lab-3'))
                for route,expected in zip(('/proof/one','/proof/two'),platform.rows['lab-1']['flags']):
                    with closing(urlopen(base+route,timeout=3)) as response:
                        self.assertEqual(json.load(response)['flag'],expected)
                asyncio.run(platform.close('lab-1'))
                with self.assertRaises(HTTPError) as error:
                    urlopen(base+'/proof/one',timeout=3)
                self.assertEqual(error.exception.code,410)
                error.exception.close()
            finally:
                process.terminate()
                process.communicate(timeout=5)

    def test_monitor_reads_actual_veria_evidence_without_recounting(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            room=VeriaRoom(root/'teams/lab-1','lab-1','synthetic-attempt')
            try:
                room.record_evidence('pro-1','tool_call',{'command':'echo synthetic'},metadata={'name':'Bash'})
                room.record_evidence('pro-1','tool_execution_result',{'success':True})
                room.record_evidence('pro-1','sdk_error',{'error':'synthetic-failure'})
                monitor=lab.Monitor(root)
                with patch('builtins.print'):
                    first=monitor.scan()['lab-1/pro-1']
                    second=monitor.scan()['lab-1/pro-1']
                self.assertEqual(first['tool_calls'],1)
                self.assertEqual(first['tool_results'],1)
                self.assertEqual(first['errors'],1)
                self.assertEqual(first['events'],second['events'])
            finally:
                room.close()
