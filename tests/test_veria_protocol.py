"""Real child-process/JSONL acceptance with no model, Docker, or target I/O."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid


PROJECT = Path(__file__).resolve().parents[1]
FIXTURE = PROJECT / "tests/fixtures/veria_protocol_fixture.py"
VERIA_PYTHON = PROJECT / ".venv-veria/bin/python"


class JsonlProcess:
    def __init__(self, run_dir):
        self.run_dir = Path(run_dir)
        self.process = None
        self.pending = {}
        self.events = []
        self.lines = []
        self.errors = []
        self.changed = asyncio.Condition()
        self.reader = None
        self.stderr = None

    async def start(self):
        env = {key: value for key, value in os.environ.items()
               if not any(token in key.upper() for token in ("API_KEY", "AUTH_TOKEN", "SECRET", "PASSWORD"))}
        env["PYTHONPATH"] = str(PROJECT / "src")
        python = str(VERIA_PYTHON) if VERIA_PYTHON.is_file() else sys.executable
        self.process = await asyncio.create_subprocess_exec(python, "-u", str(FIXTURE),
            "--run-dir", str(self.run_dir), cwd=self.run_dir, env=env,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, limit=8 * 1024 * 1024)
        self.reader = asyncio.create_task(self.read())
        self.stderr = asyncio.create_task(self.process.stderr.read())
        return self

    async def read(self):
        try:
            while raw := await self.process.stdout.readline():
                self.lines.append(raw)
                item = json.loads(raw)
                if not isinstance(item, dict) or "type" not in item:
                    raise AssertionError("stdout contains a non-protocol record")
                if item.get("code") != "protocol-test" or item.get("attempt_id") != "attempt-protocol":
                    raise AssertionError("stdout protocol lost question/attempt identity")
                if item["type"] == "response":
                    future = self.pending.pop(item["id"], None)
                    if future is None:
                        raise AssertionError("Uncorrelated JSONL response")
                    if not future.done():
                        future.set_result(item["result"])
                else:
                    async with self.changed:
                        self.events.append(item)
                        self.changed.notify_all()
        except Exception as error:
            self.errors.append(error)
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(AssertionError("Fixture process ended before response"))

    async def request(self, command, payload=None, timeout=3):
        identifier = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[identifier] = future
        record = {"id": identifier, "command": command, "payload": payload or {}}
        self.process.stdin.write((json.dumps(record) + "\n").encode())
        await self.process.stdin.drain()
        result = await asyncio.wait_for(future, timeout)
        if result.get("error"):
            raise AssertionError(result["error"])
        return result

    async def event(self, kind, *, after=0, predicate=lambda event: True, timeout=3):
        def find():
            return next((event for event in self.events[after:] if event["type"] == kind and predicate(event)), None)
        async with self.changed:
            await asyncio.wait_for(self.changed.wait_for(lambda: find() is not None), timeout)
            return find()

    async def wait_exit(self):
        code = await asyncio.wait_for(self.process.wait(), 3)
        await self.reader
        stderr = (await self.stderr).decode(errors="replace")
        if code or self.errors:
            raise AssertionError(f"Fixture exit={code}, protocol errors={self.errors}, stderr={stderr}")
        return stderr

    async def cleanup(self):
        if self.process and self.process.returncode is None:
            self.process.kill()
            await self.process.wait()
        for task in (self.reader, self.stderr):
            if task:
                await asyncio.gather(task, return_exceptions=True)


class VeriaProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.temp.name)
        self.team_dir = self.run_dir / "teams/protocol-test"
        self.processes = []
        self.context = {"run_id": "protocol-offline-run", "attempt_id": "attempt-protocol",
            "deadline": 99999999999, "challenge": {"code": "protocol-test", "description": "Synthetic offline protocol acceptance only"},
            "addresses": ["https://offline.invalid/one", "tcp://offline.invalid:1234"],
            "platform": {"completed": False, "correct_flag_count": 0, "flag_count": 2}}

    async def asyncTearDown(self):
        for process in self.processes:
            await process.cleanup()
        self.temp.cleanup()

    async def launch(self):
        process = await JsonlProcess(self.run_dir).start()
        self.processes.append(process)
        return process

    async def poll(self, callback, predicate, timeout=3):
        async with asyncio.timeout(timeout):
            while True:
                value = await callback()
                if predicate(value):
                    return value
                await asyncio.sleep(.005)

    async def test_real_jsonl_submission_reporting_preserved_stop_and_resume(self):
        process = await self.launch()
        started = await process.request("start", self.context)
        self.assertTrue(started["started"])
        self.assertEqual(started["pid"], process.process.pid)
        candidate = await process.event("candidate_flag")
        self.assertEqual(candidate["flag"], "FLAG{jsonl-offline-candidate}")

        # The solver is waiting inside a submission callback for up to 15s.
        # A fresh heartbeat and correlated health response must arrive now.
        cursor = len(process.events)
        health = await process.request("health", timeout=1)
        self.assertTrue(health["responsive"])
        await process.event("heartbeat", after=cursor, timeout=1)
        observed = self.team_dir / "agents/pro-1/fixture-receipt-observed.json"
        self.assertFalse(observed.exists())
        receipt = {"correct": True, "duplicate": False, "awarded": 10,
            "correct_flag_count": 1, "total_flag_count": 2, "matched_flag_index": 0,
            "unknown": False, "raw": {"fixture_full_receipt": "preserved"}}
        recorded = await process.request("submission_result", {"request_id": candidate["request_id"],
            "receipt": receipt, "platform": {"completed": False, "correct_flag_count": 1, "flag_count": 2}})
        self.assertEqual(recorded["status"], "correct")
        await self.poll(lambda: process.request("snapshot"), lambda _: observed.exists())
        observation = json.loads(observed.read_text())
        self.assertEqual(observation["request_id"], candidate["request_id"])
        self.assertEqual(observation["receipt"], receipt)
        self.assertEqual(observation["status"], "correct")
        self.assertIsNone(process.process.returncode)

        # Reporter invocation is deliberately paused in the fixture; the real
        # runtime still freezes this request's watermark while later events flow.
        request = {"request_id": "protocol-audit-1", "round_id": 1}
        frozen = await process.request("request_report", request)
        await process.request("message", {"recipient": "leader", "body": "fixture finding: after first audit cutoff"})
        retried = await process.request("request_report", request)
        self.assertEqual(retried["required_through_event"], frozen["required_through_event"])
        (self.team_dir / "fixture-release-reporter").touch()
        status = await self.poll(lambda: process.request("report_status", request), lambda result: result["ready"])
        self.assertEqual(status["required_through_event"], frozen["required_through_event"])
        self.assertGreaterEqual(status["through_event"], frozen["required_through_event"])
        await process.event("report_published", predicate=lambda event: event["report_version"] >= status["report_version"])
        await process.request("message", {"recipient": "leader", "body": "fixture finding: later report material"})
        await self.poll(lambda: process.request("snapshot"), lambda result: result["report_version"] > status["report_version"])
        audit = await process.request("audit_snapshot", request)
        self.assertEqual(audit["report_request"]["report_version"], status["report_version"])

        before = await process.request("health")
        identities = {role["agent_id"]: role["session_id"] for role in before["roles"]}
        self.assertEqual(len({identities[member] for member in ("pro-1", "pro-2", "pro-3", "flash-1", "flash-2", "flash-3")}), 6)
        prepared = await process.request("prepare_stop")
        self.assertTrue(prepared["preparing_stop"])
        await process.request("cancel_stop")
        self.assertFalse(json.loads((self.team_dir / "fixture-environment.json").read_text())["quiescing"])
        await process.request("stop", {"preserve_environment": True})
        self.assertNotIn("Traceback", await process.wait_exit())
        environment = json.loads((self.team_dir / "fixture-environment.json").read_text())
        self.assertFalse(environment["closed"])
        self.assertTrue(set(identities) <= set(environment["cleaned"]))

        # A different real Python process reopens the same attempt and mock
        # sessions. The fixture generates fresh random IDs if resume is omitted,
        # so these assertions do not pass merely from deterministic naming.
        resumed = await self.launch()
        await resumed.request("start", {**self.context, "platform": {"completed": False, "correct_flag_count": 1, "flag_count": 2}})
        restored = await self.poll(lambda: resumed.request("health"), lambda result: all(role.get("session_id") for role in result["roles"]))
        self.assertNotEqual(process.process.pid, resumed.process.pid)
        for role in restored["roles"]:
            member = role["agent_id"]
            self.assertEqual(role["session_id"], identities[member])
            if member != "reporter":
                proof = json.loads((self.team_dir / "agents" / member / "fixture-driver-identity.json").read_text())
                self.assertEqual(proof["resumed_from"], identities[member])
        snapshot = await resumed.request("snapshot")
        self.assertFalse(snapshot["pending_candidates"])
        await resumed.request("health")
        self.assertFalse(any(event["type"] == "candidate_flag" for event in resumed.events))
        environment = json.loads((self.team_dir / "fixture-environment.json").read_text())
        self.assertEqual(environment["starts"], 2)
        await resumed.request("stop")
        self.assertNotIn("Traceback", await resumed.wait_exit())
        self.assertTrue(json.loads((self.team_dir / "fixture-environment.json").read_text())["closed"])
        for child in (process, resumed):
            self.assertFalse(child.errors)
            self.assertTrue(child.lines)


if __name__ == "__main__":
    unittest.main()
