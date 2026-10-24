import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import signal
import sqlite3
import tempfile
import time
import unittest
from types import SimpleNamespace

from xinghuo.runtime import TeamProcess
from xinghuo.room import Room
from xinghuo.team_runtime import RoleSlot, TeamRuntime


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.teams = []

    async def asyncTearDown(self):
        await asyncio.gather(*(team.stop() for team in self.teams), return_exceptions=True)
        self.temporary.cleanup()

    async def launch(self, *, mode="smoke", settings=None):
        team = TeamProcess(self.root, "test-team", "attempt-1", settings={
            "heartbeat_interval": .05, "role_turn_timeout": 8,
            "shutdown_timeout": 2, **(settings or {}),
        })
        self.teams.append(team)
        await team.start({"challenge": {"code": "test-team"}, "mode": mode,
                          "nonce": "runtime-test-nonce", "platform": {"correct_flag_count": 0}})
        return team

    async def wait_snapshot(self, team, predicate, timeout=8):
        end = time.monotonic() + timeout
        snapshot = {}
        while time.monotonic() < end:
            snapshot = await team.request("snapshot", timeout=2)
            if predicate(snapshot):
                return snapshot
            await asyncio.sleep(.025)
        self.fail(f"team did not reach expected state: {snapshot}")

    async def ready(self, team):
        return await self.wait_snapshot(team, lambda s: len(s["evidence_refs"]) == 2
            and not s["pending_reporter_messages"] and len(s["agents"]) == 4
            and all(a["turns"] > 0 and a["status"] == "idle" for a in s["agents"]))

    async def test_subprocess_delegation_report_and_acknowledgement(self):
        team = await self.launch()
        snapshot = await self.ready(team)
        self.assertNotEqual(team.pid, os.getpid())
        self.assertEqual(len({agent["pid"] for agent in snapshot["agents"]}), 4)
        self.assertEqual(len({agent["session_id"] for agent in snapshot["agents"]}), 4)
        self.assertTrue(all(agent["restarts"] == 0 for agent in snapshot["agents"]))
        reporter_messages = [m for m in snapshot["messages"] if m["recipient"] == "reporter"]
        self.assertTrue(reporter_messages)
        self.assertTrue(all(m["state"] == "acknowledged" for m in reporter_messages))
        self.assertIn("runtime-test-nonce", (team.directory / "report.md").read_text())
        self.assertEqual(json.loads((team.directory / "snapshot.json").read_text())["code"], "test-team")
        self.assertFalse(any(event["type"] == "candidate_flag" for event in team.poll_events()))
        with closing(sqlite3.connect(team.directory / "room.sqlite3")) as db:
            calls = [json.loads(row[0])["name"] for row in db.execute("SELECT data FROM events WHERE type='tool_called'")]
        self.assertEqual(calls.count("spawn_worker"), 2)
        self.assertIn("send_message", calls)
        self.assertIn("ack_message", calls)
        first = await team.request("audit_snapshot")
        second = await team.request("audit_snapshot")
        third = await team.request("audit_snapshot")
        self.assertEqual(len(third["review_windows"]), 2)
        self.assertEqual(second["review_windows"][-1]["window_id"], third["review_windows"][0]["window_id"])
        self.assertLess(first["review_windows"][0]["window_id"], third["review_windows"][-1]["window_id"])

    async def test_leader_report_request_is_published_and_survives_same_round_resume(self):
        team = await self.launch()
        before = await self.ready(team)
        request = {"request_id": "attempt-1:1", "round_id": 1}
        response = await team.request("request_report", request)
        self.assertEqual(response["baseline_version"], before["report_version"])
        async with asyncio.timeout(8):
            while not (status := await team.request("report_status", request))["ready"]:
                await asyncio.sleep(.025)
        after = await team.request("audit_snapshot", request)
        self.assertTrue(after["report_request"]["ready"])
        self.assertGreater(after["report_version"], before["report_version"])
        self.assertIn("leader_audit_report", json.dumps(after["recent_changes"]))
        health = await team.request("health")
        self.assertTrue(health["healthy"])
        self.assertTrue(all(a["process_alive"] for a in health["agents"]))
        await team.stop()
        recovered = await self.launch()
        retry = await recovered.request("request_report", request)
        self.assertTrue(retry["ready"])
        self.assertEqual(retry["draft_message_id"], status["draft_message_id"])
        snapshot = await recovered.request("snapshot")
        requests = [m for m in snapshot["messages"] if m["id"] == status["message_id"]]
        self.assertEqual(len(requests), 1)

    def test_unrelated_new_report_and_unpublished_leader_draft_do_not_complete_request(self):
        room = Room(self.root / "report-gate", "report-gate", "attempt-1")
        try:
            room.add_agent("leader", "leader")
            room.add_agent("reporter", "reporter")
            room.publish_report({"overview": "old"}, seen_through=room.last_event)
            state = room.request_audit_report("attempt-1:1", 1)
            request_id = state["message_id"]
            room.inject("leader", [request_id], "leader-turn")
            room.acknowledge("leader", [request_id])
            room.publish_report({"overview": "new but unrelated"}, seen_through=room.last_event)
            self.assertFalse(room.audit_report_status("attempt-1:1")["ready"])
            unrelated = room.send("leader", "reporter", "wrong request", reply_to="other-round")
            room.inject("reporter", [unrelated["id"]], "reporter-turn")
            room.acknowledge("reporter", [unrelated["id"]])
            room.publish_report({"overview": "wrong round"}, seen_through=room.last_event)
            self.assertFalse(room.audit_report_status("attempt-1:1")["ready"])
            draft = room.send("leader", "reporter", "current audit draft", reply_to=request_id)
            room.inject("reporter", [draft["id"]], "reporter-turn-2")
            room.acknowledge("reporter", [draft["id"]])
            self.assertFalse(room.audit_report_status("attempt-1:1")["ready"])
            room.publish_report({"overview": "includes leader draft"}, seen_through=room.last_event)
            self.assertTrue(room.audit_report_status("attempt-1:1")["ready"])
            with self.assertRaises(ValueError):
                room.request_audit_report("attempt-1:1", 2)
        finally:
            room.close()

    async def test_candidates_receive_authoritative_receipts_and_flush(self):
        team = await self.launch(mode="solve")
        candidates = []
        end = time.monotonic() + 8
        while len(candidates) < 2 and time.monotonic() < end:
            candidates.extend(event for event in team.poll_events() if event["type"] == "candidate_flag")
            await asyncio.sleep(.025)
        self.assertEqual({event["flag"] for event in candidates}, {"flag{test-team:1}", "flag{test-team:2}"})
        for index, candidate in enumerate(candidates, start=1):
            receipt = {"correct": True, "duplicate": False, "awarded": 10,
                       "cumulative_score": index * 10, "correct_flag_count": index,
                       "total_flag_count": 2, "matched_flag_index": index - 1}
            result = await team.request("submission_result", {"request_id": candidate["request_id"], "receipt": receipt})
            self.assertFalse(result["duplicate"])
            duplicate = await team.request("submission_result", {"request_id": candidate["request_id"], "receipt": receipt})
            self.assertTrue(duplicate["duplicate"])
        result = await team.request("prepare_stop", timeout=5)
        self.assertTrue(result["flushed"])
        self.assertEqual(result["snapshot"]["platform"]["correct_flag_count"], 2)
        await team.request("submission_result", {"request_id": candidates[0]["request_id"],
            "receipt": {"correct": True, "correct_flag_count": 1, "cumulative_score": 10}})
        self.assertEqual((await team.request("snapshot"))["platform"]["correct_flag_count"], 2)
        self.assertIn('"correct_flag_count": 2', (team.directory / "report.md").read_text())
        self.assertTrue(Path(result["checkpoint"]).exists())
        cancelled = await team.request("cancel_stop")
        self.assertFalse(cancelled["preparing_stop"])

    async def test_killed_worker_and_reporter_recover_locally(self):
        team = await self.launch()
        initial = await self.ready(team)
        killed = [next(a for a in initial["agents"] if a["role"] == "worker"),
                  next(a for a in initial["agents"] if a["role"] == "reporter")]
        retained = {a["agent_id"]: a["pid"] for a in initial["agents"] if a not in killed}
        for agent in killed:
            os.kill(agent["pid"], signal.SIGKILL)
        recovered = await self.wait_snapshot(team, lambda s: all(
            any(a["agent_id"] == old["agent_id"] and a["pid"] not in {None, old["pid"]}
                and a["restarts"] == 1 and a["status"] == "idle" for a in s["agents"])
            for old in killed))
        for old in killed:
            new = next(a for a in recovered["agents"] if a["agent_id"] == old["agent_id"])
            self.assertEqual(new["session_id"], old["session_id"])
        for agent_id, pid in retained.items():
            self.assertEqual(next(a["pid"] for a in recovered["agents"] if a["agent_id"] == agent_id), pid)
        owned_pids = [team.pid, *(a["pid"] for a in recovered["agents"] if a["pid"])]
        events = team.poll_events()
        self.assertGreaterEqual(sum(e["type"] == "worker_failed" for e in events), 2)
        await team.stop()
        for pid in owned_pids:
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    async def test_resume_preserves_ids_sessions_and_accepts_correction(self):
        first = await self.launch()
        before = await self.ready(first)
        old_sessions = {agent["agent_id"]: agent["session_id"] for agent in before["agents"]}
        await first.stop()
        second = await self.launch()
        response = await second.request("message", {"recipient": "leader", "body": "更正：此前假设需撤回，采用新的证据。"})
        after = await self.wait_snapshot(second, lambda s: any("此前假设需撤回" in str(item) for item in s["recent_changes"])
            and any(m["id"] == response["message_id"] and m["state"] == "acknowledged" for m in s["messages"]))
        self.assertEqual({agent["agent_id"] for agent in after["agents"]}, set(old_sessions))
        self.assertEqual({agent["agent_id"]: agent["session_id"] for agent in after["agents"]}, old_sessions)
        self.assertGreater(after["report_version"], before["report_version"])

    async def test_review_is_real_role_and_reports_completion(self):
        team = await self.launch()
        await self.ready(team)
        requests = await asyncio.gather(team.request("review", {"reason": "检查遗漏覆盖"}),
                                        team.request("review", {"reason": "合并同一轮"}))
        self.assertEqual(sum(bool(result.get("started")) for result in requests), 1)
        snapshot = await self.wait_snapshot(team, lambda s: bool(s["last_review"]))
        self.assertTrue(any(a["role"] == "reviewer" and a["pid"] for a in snapshot["agents"]))
        self.assertTrue(any(event["type"] == "review_completed" for event in team.poll_events()))

    async def test_reviewer_reads_old_receipts_and_full_evidence_index_without_leaking_to_leader(self):
        runtime = TeamRuntime(self.root, "review-materials", "attempt-1", "demo")
        try:
            for role in ("leader", "reporter", "reviewer"):
                runtime.room.add_agent(role, role)
            runtime.room.set_meta("context", {"platform": {"correct_flag_count": 1, "flag_count": 2}})
            runtime.room.set_meta("candidates", {"first": {"flag": "flag{private-fixture}"}})
            runtime.room.submission("first", {"correct": True, "duplicate": False, "matched_flag_index": 0})
            runtime.room.submission("same", {"correct": True, "duplicate": True, "matched_flag_index": 0})
            runtime.room.submission("wrong", {"correct": False, "duplicate": False})
            refs = [f"evidence/{index}.json" for index in range(55)]
            runtime.room.publish_report({"overview": "old report", "evidence_refs": refs}, seen_through=runtime.room.last_event)
            for index in range(125):
                runtime.room.event("old_noise", {"index": index})
            runtime.room.set_meta("review_request", {"review_id": "round-one", "output_path": "review.md"})
            slot = RoleSlot("reviewer", "reviewer", event_cursor=runtime.room.last_event)
            first = json.loads(runtime._prompt(slot))
            self.assertEqual(first["events"], [])
            material = first["review_context"]
            self.assertEqual([r["request_id"] for r in material["confirmed_submissions"]], ["first", "same"])
            self.assertEqual(material["confirmed_submissions"][0]["flag"], "flag{private-fixture}")
            self.assertTrue(material["confirmed_submissions"][1]["receipt"]["duplicate"])
            self.assertIsNone(material["confirmed_submissions"][1]["flag"])
            self.assertEqual(material["platform"]["correct_flag_count"], 1)
            self.assertEqual(material["evidence_refs"], refs)
            self.assertNotIn("review_context", json.loads(runtime._prompt(RoleSlot("leader", "leader"))))
            runtime.room.publish_report({"overview": "latest", "evidence_refs": ["new-proof.json"]}, seen_through=runtime.room.last_event)
            updated = await runtime.tool(slot, "get_report", {})
            self.assertEqual(updated["review_context"]["report_version"], material["report_version"] + 1)
            self.assertEqual(updated["review_context"]["evidence_refs"], ["new-proof.json"])
        finally:
            runtime.room.close()

    async def test_new_review_round_gets_new_artifact_and_preserves_previous_result(self):
        team = await self.launch()
        await self.ready(team)
        first = await team.request("review", {"reason": "first independent review"})
        await self.wait_snapshot(team, lambda s: (s.get("last_review") or {}).get("review_id") == first["review_id"]
                                 and all(a["status"] == "idle" for a in s["agents"]))
        second = await team.request("review", {"reason": "new evidence, second review"})
        self.assertTrue(second["started"])
        self.assertNotEqual(first["review_id"], second["review_id"])
        self.assertNotEqual(first["output_path"], second["output_path"])
        self.assertTrue(Path(second["output_path"]).parent.is_dir())
        self.assertEqual(second["previous_review"]["review_id"], first["review_id"])
        snapshot = await self.wait_snapshot(team, lambda s: (s.get("last_review") or {}).get("review_id") == second["review_id"])
        self.assertEqual(snapshot["last_review"]["review_id"], second["review_id"])
        with closing(sqlite3.connect(team.directory / "room.sqlite3")) as db:
            reviews = [json.loads(row[0])["summary"] for row in db.execute("SELECT data FROM events WHERE type='progress' AND sender='reviewer'")]
        self.assertEqual(len(reviews), 2)

    async def test_resume_queues_review_saved_before_message_delivery(self):
        directory = self.root / "teams" / "pending-review"
        room = Room(directory, "pending-review", "attempt-1")
        request = {"review_id": "persisted-review", "reason": "crash before message delivery",
                   "output_path": str(directory / "agents/reviewer/reviews/persisted-review.md"),
                   "previous_review": None}
        room.set_meta("review_request", request)
        room.close()
        team = TeamProcess(self.root, "pending-review", "attempt-1", settings={"heartbeat_interval": .05})
        self.teams.append(team)
        await team.start({"mode": "smoke", "nonce": "pending-review-fixture", "challenge": {"code": "pending-review"}})
        snapshot = await self.wait_snapshot(team, lambda s: (s.get("last_review") or {}).get("review_id") == "persisted-review")
        messages = [m for m in snapshot["messages"] if m["id"] == "review-request:persisted-review"]
        self.assertEqual(len(messages), 1)
        self.assertTrue(Path(request["output_path"]).parent.is_dir())

    async def test_new_attempt_archives_old_room_and_uses_new_sessions(self):
        first = await self.launch()
        before = await self.ready(first)
        await first.stop()
        second = TeamProcess(self.root, "test-team", "attempt-2", settings={"heartbeat_interval": .05})
        self.teams.append(second)
        await second.start({"challenge": {"code": "test-team"}, "mode": "smoke", "nonce": "fresh-attempt"})
        after = await self.ready(second)
        self.assertEqual(after["attempt_id"], "attempt-2")
        old_leader = next(a for a in before["agents"] if a["role"] == "leader")
        new_leader = next(a for a in after["agents"] if a["role"] == "leader")
        self.assertNotEqual(old_leader["session_id"], new_leader["session_id"])
        self.assertEqual(len([a for a in after["agents"] if a["role"] == "worker"]), 2)
        archives = list((second.directory / "attempts").iterdir())
        self.assertEqual(len(archives), 1)
        self.assertTrue((archives[0] / "room.sqlite3").exists())
        self.assertTrue((archives[0] / "report.md").exists())
        self.assertTrue((archives[0] / "agents").is_dir())
        self.assertIn("fresh-attempt", (second.directory / "report.md").read_text())

    async def test_prepare_stop_is_bounded_when_reporter_cannot_recover(self):
        team = await self.launch(settings={"shutdown_timeout": .25})
        snapshot = await self.ready(team)
        reporter = next(a for a in snapshot["agents"] if a["role"] == "reporter")
        for failure in (1, 2, 3):
            os.kill(reporter["pid"], signal.SIGKILL)
            snapshot = await self.wait_snapshot(team, lambda s: any(a["role"] == "reporter"
                and a["restarts"] == failure and ((a["pid"] and a["status"] == "idle")
                if failure <= 2 else a["status"] == "failed") for a in s["agents"]))
            reporter = next(a for a in snapshot["agents"] if a["role"] == "reporter")
        await team.request("message", {"recipient": "reporter", "body": "报告尚未处理的新证据"})
        start = time.monotonic()
        result = await team.request("prepare_stop", timeout=2)
        self.assertFalse(result["flushed"])
        self.assertLess(time.monotonic() - start, 1)
        self.assertTrue(team.alive)

    async def test_machine_limit_idempotency_and_editable_role_policy(self):
        policy_dir = self.root / "policies"
        policy_dir.mkdir()
        (policy_dir / "leader.md").write_text("CUSTOM EDITABLE LEADER POLICY", encoding="utf-8")
        runtime = TeamRuntime(self.root, "capacity", "a1", "demo", settings={"max_agents_per_team": 5})
        try:
            runtime.room.add_agent("leader", "leader")
            runtime.room.add_agent("reporter", "reporter")
            leader = RoleSlot("leader", "leader")
            runtime.slots = {"leader": leader, "reporter": RoleSlot("reporter", "reporter")}
            def launch_without_process(agent_id, role):
                slot = RoleSlot(agent_id, role)
                runtime.slots[agent_id] = slot
                return slot
            runtime._launch_slot = launch_without_process
            first = await runtime.tool(leader, "spawn_worker", {"task": "first", "request_key": "first"})
            await runtime.tool(leader, "spawn_worker", {"task": "second", "request_key": "second"})
            duplicate = await runtime.tool(leader, "spawn_worker", {"task": "first", "request_key": "first"})
            self.assertEqual(first["agent_id"], duplicate["agent_id"])
            with self.assertRaisesRegex(ValueError, "capacity"):
                await runtime.tool(leader, "spawn_worker", {"task": "too many", "request_key": "third"})
            prompt = json.loads(runtime._prompt(leader))
            self.assertIn("CUSTOM EDITABLE LEADER POLICY", prompt["policy"])
            self.assertNotIn("messages", prompt["report"])
            self.assertNotIn("windows", prompt["report"])
        finally:
            runtime.room.close()

    async def test_batched_reporter_does_not_lose_old_messages_or_self_wake_forever(self):
        team = await self.launch()
        await self.ready(team)
        replies = await asyncio.gather(*(team.request("message", {"recipient": "reporter", "body": f"batch-evidence-{index}"})
                                        for index in range(70)))
        ids = {reply["message_id"] for reply in replies}
        snapshot = await self.wait_snapshot(team, lambda s: not s["pending_reporter_messages"]
            and all(any(m["id"] == message_id and m["state"] == "acknowledged" for m in s["messages"]) for message_id in ids)
            and s["through_event"] >= s["evidence_watermark"])
        with closing(sqlite3.connect(team.directory / "room.sqlite3")) as db:
            historical_reports = [json.loads(row[0]) for row in db.execute("SELECT data FROM reports")]
        merged = {change for report in historical_reports for change in report["recent_changes"]}
        self.assertTrue({f"batch-evidence-{index}" for index in range(70)} <= merged)
        await asyncio.sleep(.15)
        version = (await team.request("snapshot"))["report_version"]
        await asyncio.sleep(.15)
        self.assertEqual((await team.request("snapshot"))["report_version"], version)

    async def test_tool_effect_persists_session_before_turn_result(self):
        runtime = TeamRuntime(self.root, "session-early", "a1", "demo")
        runtime.room.add_agent("reporter", "reporter")
        reporter = RoleSlot("reporter", "reporter", driver=SimpleNamespace(session_id="session-before-final-result"))
        reporter.seen_through = runtime.room.last_event
        async def discard_output(*args, **kwargs):
            pass
        runtime.emit = discard_output
        try:
            await runtime.tool(reporter, "publish_report", {"overview": "Published while turn remains in progress", "through_event": reporter.seen_through})
            self.assertEqual(runtime.room.agent("reporter")["session_id"], "session-before-final-result")
            self.assertEqual(runtime.room.agent("reporter")["turns"], 0)
        finally:
            runtime.room.close()
        reopened = TeamRuntime(self.root, "session-early", "a1", "demo")
        try:
            self.assertEqual(reopened.room.agent("reporter")["session_id"], "session-before-final-result")
        finally:
            reopened.room.close()

    async def test_tool_loop_suspicion_is_deduplicated_until_pattern_changes(self):
        runtime = TeamRuntime(self.root, "loop", "a1", "demo")
        runtime.started = True
        for agent_id, role in (("leader", "leader"), ("reporter", "reporter"), ("worker-x", "worker")):
            runtime.room.add_agent(agent_id, role)
            runtime.slots[agent_id] = RoleSlot(agent_id, role)
        events = []
        async def emit(event_type, **data):
            events.append({"type": event_type, **data})
        runtime.emit = emit
        def launch_without_process(agent_id, role):
            slot = RoleSlot(agent_id, role)
            runtime.slots[agent_id] = slot
            return slot
        runtime._launch_slot = launch_without_process
        slot = runtime.slots["worker-x"]
        def observation(result="same"):
            return {"phase": "completed", "tool_name": "Read", "tool_use_id": "use", "input_fingerprint": "input-hash", "result_fingerprint": result, "is_error": False}
        try:
            slot.driver = SimpleNamespace(poll_observations=lambda: [observation()] * 4)
            await runtime._observe_tools(slot)
            self.assertEqual(sum(e["type"] == "worker_suspected_loop" for e in events), 1)
            self.assertTrue(runtime.room.meta("tool_patterns")["worker-x"]["suspected"])
            slot.driver = SimpleNamespace(poll_observations=lambda: [observation("changed")])
            await runtime._observe_tools(slot)
            self.assertFalse(runtime.room.meta("tool_patterns")["worker-x"]["suspected"])
            slot.driver = SimpleNamespace(poll_observations=lambda: [observation()] * 3)
            await runtime._observe_tools(slot)
            self.assertEqual(sum(e["type"] == "worker_suspected_loop" for e in events), 2)
            self.assertIn("reviewer", runtime.slots)
        finally:
            runtime.room.close()

    async def test_killed_runtime_resumes_without_orphan_roles(self):
        first = await self.launch()
        before = await self.ready(first)
        previous_pids = [agent["pid"] for agent in before["agents"] if agent["pid"]]
        os.kill(first.pid, signal.SIGKILL)
        await first.process.wait()
        await first.stop()
        second = await self.launch()
        after = await self.wait_snapshot(second, lambda s: all(
            agent["session_id"] for agent in s["agents"]) and s["report_version"] > before["report_version"])
        self.assertEqual({agent["agent_id"]: agent["session_id"] for agent in after["agents"]},
                         {agent["agent_id"]: agent["session_id"] for agent in before["agents"]})
        end = time.monotonic() + 3
        alive = previous_pids
        while alive and time.monotonic() < end:
            remaining = []
            for pid in alive:
                try:
                    os.kill(pid, 0)
                    remaining.append(pid)
                except ProcessLookupError:
                    pass
            alive = remaining
            await asyncio.sleep(.025)
        self.assertEqual(alive, [])

    async def test_completed_worker_notifies_leader_and_can_take_new_task_in_same_session(self):
        team = await self.launch()
        await self.ready(team)
        initial = await self.wait_snapshot(team, lambda s: len([m for m in s["messages"]
            if m["recipient"] == "leader" and m["id"].startswith("task-complete:") and m["state"] == "acknowledged"]) == 2)
        worker = next(agent for agent in initial["agents"] if agent["role"] == "worker")
        assignment = {"type": "assign_task", "worker_id": worker["agent_id"], "request_key": "reuse-test",
            "task": json.dumps({"code": "test-team", "index": 3, "nonce": "REUSED-NONCE", "smoke": True,
                                "objective": "Produce new evidence without repeating the completed assignment"})}
        await team.request("message", {"recipient": "leader", "body": json.dumps(assignment)})
        after = await self.wait_snapshot(team, lambda s: "demo://test-team/worker-3" in s["evidence_refs"]
            and any(agent["agent_id"] == worker["agent_id"] and agent["status"] == "idle" for agent in s["agents"])
            and len([task for task in s["tasks"] if task["agent_id"] == worker["agent_id"] and task["status"] == "completed"]) == 2)
        reused = next(agent for agent in after["agents"] if agent["agent_id"] == worker["agent_id"])
        self.assertEqual(reused["session_id"], worker["session_id"])
        self.assertEqual(reused["pid"], worker["pid"])
        self.assertEqual(len(after["agents"]), len(initial["agents"]))
        self.assertNotEqual(reused["task_id"], worker["task_id"])
        self.assertTrue(any("REUSED-NONCE" in str(change) for change in after["recent_changes"]))
        self.assertFalse(any(event["type"] == "candidate_flag" for event in team.poll_events()))


class TransportDeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_deadline_covers_write_lock(self):
        writes = []
        team = TeamProcess(Path("/tmp"), "blocked-lock", "attempt")
        team.process = SimpleNamespace(returncode=None, stdin=SimpleNamespace(write=writes.append))
        await team._write_lock.acquire()
        try:
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(team.request("prepare_stop", timeout=.02), .5)
            self.assertEqual(writes, [])
            self.assertEqual(team._pending, {})
        finally:
            team._write_lock.release()

    async def test_request_deadline_covers_pipe_backpressure(self):
        async def drain():
            await asyncio.Event().wait()

        writes = []
        team = TeamProcess(Path("/tmp"), "blocked-pipe", "attempt")
        team.process = SimpleNamespace(returncode=None,
            stdin=SimpleNamespace(write=writes.append, drain=drain))
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(team.request("stop", timeout=.02), .5)
        self.assertEqual(len(writes), 1)
        self.assertEqual(team._pending, {})
        self.assertFalse(team._write_lock.locked())


if __name__ == "__main__":
    unittest.main()
