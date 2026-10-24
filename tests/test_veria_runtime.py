"""Offline integration tests: no SDK connections, Docker or platform network."""
import asyncio
import json
import os
from unittest.mock import patch
from pathlib import Path
import tempfile
import unittest

from xinghuo.config import VERIA_MODELS, VERIA_MIXED4_MODELS, VERIA_MIXED4_PROVIDERS, VERIA_MIXED4_SOLVERS
from xinghuo.veria_runtime import SOLVERS, VeriaRuntime, VeriaSlot, ReviewDeadlineReached, safe_error
from xinghuo.veria.prompts import continuation_prompt


class FakeEnvironment:
    def __init__(self, directory, run_id, code, attempt_id, settings):
        self.closed = False
        self.quiescing = False
        self.cleaned = []

    async def start(self):
        return {"image_id": "sha256:fixture", "container_id": "fixture", "description": {},
                "recovered": False, "volatile_state_lost": False}

    def quiesce(self, value):
        self.quiescing = value

    async def health(self):
        return {"healthy": not self.closed}

    async def stop_member(self, agent_id):
        self.cleaned.append(agent_id)

    async def stop(self):
        self.closed = True


class FakeDriver:
    instances = []

    def __init__(self, agent_id, role, model_id, workspace, context, environment,
                 on_tool, on_session=None, on_event=None, session_id=None):
        self.agent_id, self.role, self.model_id = agent_id, role, model_id
        self.workspace = workspace
        self.context = context
        self.on_tool, self.on_session, self.on_event = on_tool, on_session, on_event
        self.session_id = session_id or "fixture-session-" + agent_id
        self.gate = asyncio.Event()
        self.prompts = []
        self.alive = False
        self.closed = False
        self.instances.append(self)

    async def start(self):
        self.alive = True
        await self.on_session(self.session_id)

    async def turn(self, prompt):
        data = json.loads(prompt)
        self.prompts.append(data)
        if self.role == "solver":
            await self.gate.wait()
            self.gate.clear()
        if self.role == "reporter":
            return {"result": json.dumps({"overview": "offline fixture", "recent_changes": [],
                "short_history": [], "next_steps": [], "evidence_refs": []}), "session_id": self.session_id}
        if self.role == "reviewer":
            await self.on_tool("record_reproductions", {"reproductions": []})
            return {"result": json.dumps({"summary": "no known flags", "reproductions": [],
                "sections": {key: "unchecked: offline fixture" for key in
                    ("lateral_opportunities", "coverage", "missed_routes", "prior_rejections", "member_health")}}),
                    "session_id": self.session_id}
        return {"result": "", "session_id": self.session_id}

    async def close(self):
        self.alive = False
        self.closed = True


async def eventually(predicate, timeout=2):
    end = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= end:
            raise AssertionError("offline condition did not become true")
        await asyncio.sleep(.005)


class VeriaRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        FakeDriver.instances = []
        self.temp = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.temp.name)
        self.events = []
        self.settings = {"veria_models": VERIA_MODELS, "veria_tool_image": "fixture:local",
            "veria_python": "/fixture/python", "veria_report_interval": 3600,
            "veria_continuation_delay": .01, "role_turn_timeout": 60,
            "operation_timeout": .02, "shutdown_timeout": .02, "max_retries": 2}
        self.runtime = self.make_runtime()
        await self.runtime.start({"run_id": "fixture-run", "deadline": 99999999999,
            "challenge": {"code": "test", "description": "synthetic test only"},
            "platform": {"flag_count": 2, "correct_flag_count": 0, "completed": False}})
        await eventually(lambda: all(self.runtime.slots[x].driver for x in SOLVERS))

    def make_runtime(self):
        runtime = VeriaRuntime(self.run_dir, "test", "attempt-1", settings=dict(self.settings),
            environment_factory=FakeEnvironment, driver_factory=FakeDriver)

        async def emit(kind, **data):
            self.events.append({"type": kind, **data})

        runtime.emit = emit
        return runtime

    async def asyncTearDown(self):
        await self.runtime.shutdown()
        self.temp.cleanup()

    def events_of(self, kind):
        return [row for row in self.events if row["type"] == kind]

    async def candidate(self, member="pro-1", flag="FLAG{first}"):
        return await self.runtime.tool(self.runtime.slots[member], "submit_flag", {"flag": flag})

    async def test_three_mixed4_teams_have_exactly_twelve_solvers_and_routed_context(self):
        teams = []
        settings = {**self.settings, "veria_models": VERIA_MIXED4_MODELS,
            "veria_team_profile": "mixed4", "veria_solver_ids": list(VERIA_MIXED4_SOLVERS),
            "veria_providers": VERIA_MIXED4_PROVIDERS}
        async def emit(kind, **data):
            return None
        try:
            for index in range(3):
                runtime = VeriaRuntime(self.run_dir, f"mixed-{index}", "mixed-attempt", settings=settings,
                    environment_factory=FakeEnvironment, driver_factory=FakeDriver)
                runtime.emit = emit
                teams.append(runtime)
                await runtime.start({"run_id": "mixed-test", "deadline": 99999999999, "challenge": {}})
            await eventually(lambda: all(all(team.slots[x].driver for x in VERIA_MIXED4_SOLVERS) for team in teams))
            self.assertEqual(sum(sum(slot.role == "solver" for slot in team.slots.values()) for team in teams), 12)
            for team in teams:
                self.assertEqual(set(team.slots), {*VERIA_MIXED4_SOLVERS, "coordinator", "reporter"})
                for member in VERIA_MIXED4_SOLVERS:
                    self.assertEqual(team.slots[member].driver.context["provider"], VERIA_MIXED4_PROVIDERS[member])
                broadcast = await team.tool(team.slots["coordinator"], "broadcast", {"message": "four only"})
                self.assertEqual(set(broadcast["recipients"]), set(VERIA_MIXED4_SOLVERS))
                with self.assertRaisesRegex(ValueError, "configured solver"):
                    await team.tool(team.slots["coordinator"], "bump_agent", {"solver_id": "pro-1", "insights": "invalid"})
                self.assertEqual(team.room.meta("veria_contract")["providers"], VERIA_MIXED4_PROVIDERS)
                review_id = f"mixed-glm-review-{team.code}"
                await team.command("review", {"review_id": review_id})
                await eventually(lambda: team.slots["reviewer"].task.done())
                reviewers = [driver for driver in FakeDriver.instances if driver.role == "reviewer"
                             and (driver.context.get("review") or {}).get("review_id") == review_id]
                self.assertEqual(len(reviewers), 1)
                reviewer = reviewers[0]
                self.assertEqual(reviewer.model_id, "glm-5.3")
                self.assertEqual(reviewer.context["provider"], "glm")
                self.assertEqual(team.room.review(review_id)["status"], "completed")
        finally:
            for runtime in teams:
                await runtime.shutdown()

    async def test_review_fixed_total_deadline_phase_handoffs_and_incremental_evidence(self):
        import time
        started = time.time()
        known = [{"request_id": "first", "flag": "FLAG{first}"}, {"request_id": "second", "flag": "FLAG{second}"}]
        request = {"review_id": "phases", "started_at": started, "confirmed_submissions": known,
                   **self.runtime._new_review_schedule(started)}
        self.runtime.room.start_review("phases", request)
        slot = VeriaSlot("reviewer", "reviewer", review_id="phases")
        self.runtime._advance_review_phase(slot)
        partial = {"request_id": "first", "status": "blocked", "reason": "shared service cannot be reset", "evidence_refs": []}
        await self.runtime._record_review_progress(slot, {"reproductions": [partial]})
        with patch("xinghuo.veria_runtime.time.time", return_value=started + 601):
            self.runtime._advance_review_phase(slot)
        review = self.runtime.room.review("phases")
        checkpoint = review["reproduction_checkpoint"]
        self.assertEqual(review["phase"], "coverage")
        self.assertEqual([item["status"] for item in checkpoint["reproductions"]], ["blocked", "unchecked"])
        evidence = self.runtime.room.record_evidence("reviewer", "tool_execution_result", {"result": "checked an independent route"},
            metadata={"review_id": "phases", "success": True})
        await self.runtime._record_review_progress(slot, {"sections": {"missed_routes": "route checked"},
            "audit_evidence_refs": [evidence["event_seq"]]})
        self.assertTrue(self.runtime.room.review("phases")["phase_evidence_complete"])
        fixed_deadline = self.runtime._review_schedule(slot)["deadline"]
        self.runtime.settings["veria_review_budget_seconds"] = 9000
        self.runtime.room.recover_review("phases")
        self.assertEqual(self.runtime._review_schedule(VeriaSlot("reviewer", "reviewer", review_id="phases"))["deadline"], fixed_deadline)
        with patch("xinghuo.veria_runtime.time.time", return_value=started + 1681):
            self.runtime._advance_review_phase(slot)
        self.assertEqual(self.runtime.room.review("phases")["phase"], "finalizing")
        with patch("xinghuo.veria_runtime.time.time", return_value=started + 1801):
            with self.assertRaises(ReviewDeadlineReached):
                self.runtime._advance_review_phase(slot)

    async def test_no_flag_review_checkpoint_is_immediate_and_cancel_retains_partial_evidence(self):
        class WaitingReviewDriver(FakeDriver):
            async def turn(self, prompt):
                if self.role == "reviewer":
                    await asyncio.Event().wait()
                return await super().turn(prompt)
        self.runtime.driver_factory = WaitingReviewDriver
        result = await self.runtime.command("review", {"review_id": "cancel-me"})
        self.assertTrue(result["started"])
        review = self.runtime.room.review("cancel-me")
        self.assertEqual(review["reproduction_checkpoint"]["status"], "not_applicable")
        self.assertEqual(review["phase"], "coverage")
        self.assertEqual(result["deadline"] - result["started_at"], 1800)
        await eventually(lambda: self.runtime.slots["reviewer"].running)
        cancelled = await self.runtime.command("cancel_review", {"reason": "supervisor switched after persisted phase evidence"})
        self.assertTrue(cancelled["review"]["result"]["partial"])
        self.assertEqual(cancelled["review"]["status"], "cancelled")
        self.assertIsNone(self.runtime.room.meta("last_review"))
        self.assertNotIn("reviewer", self.runtime._role_health()[1])
        replay = await self.runtime.command("review", {"review_id": "cancel-me"})
        self.assertFalse(replay["started"])
        self.assertEqual(replay["status"], "cancelled")

    async def test_balance_failure_is_unhealthy_and_controlled_recovery_is_provider_scoped_idempotent(self):
        self.runtime.providers = {member: "deepseek" for member in self.runtime.models}
        slot = self.runtime.slots["pro-1"]
        slot.task.cancel()
        await asyncio.gather(slot.task, return_exceptions=True)
        self.assertFalse(await self.runtime._failure(slot, RuntimeError("402 InsufficientBalance")))
        health = await self.runtime.command("health", {})
        self.assertFalse(health["healthy"])
        failed = next(member for member in health["roles"] if member["agent_id"] == "pro-1")
        self.assertEqual(failed["failure_category"], "balance")
        session = self.runtime.room.agent("pro-1")["session_id"]
        deadline = self.runtime.room.meta("context")["deadline"]
        payload = {"request_id": "recovery-once", "provider": "deepseek", "reason": "balance replenished", "members": ["pro-1"]}
        receipt = await self.runtime.command("recover_members", payload)
        self.assertEqual(receipt["recovered"], ["pro-1"])
        self.assertEqual(receipt["deadline"], deadline)
        self.assertEqual(await self.runtime.command("recover_members", payload), receipt)
        await eventually(lambda: self.runtime.slots["pro-1"].driver is not None)
        self.assertEqual(self.runtime.room.agent("pro-1")["session_id"], session)
        with self.assertRaisesRegex(ValueError, "reused"):
            await self.runtime.command("recover_members", {**payload, "reason": "changed"})

    async def test_six_independent_sessions_and_fixed_roles(self):
        self.assertEqual(len(self.runtime.slots), 8)
        sessions = [self.runtime.room.agent(member)["session_id"] for member in SOLVERS]
        self.assertEqual(len(set(sessions)), 6)
        self.assertEqual(self.runtime.slots["pro-1"].driver.model_id, "deepseek-v4-pro")
        self.assertEqual(self.runtime.slots["flash-3"].driver.model_id, "deepseek-flash")
        self.assertNotIn("leader", self.runtime.slots)

    async def test_first_correct_flag_keeps_all_solvers_and_container_alive(self):
        pending = await self.candidate()
        await self.runtime.command("submission_result", {"request_id": pending["request_id"],
            "receipt": {"correct": True, "duplicate": False, "awarded": 10,
                        "correct_flag_count": 1, "total_flag_count": 2},
            "platform": {"completed": False, "correct_flag_count": 1, "flag_count": 2}})
        self.assertFalse(self.runtime.stopping.is_set())
        self.assertFalse(self.runtime.environment.closed)
        self.assertTrue(all(self.runtime.slots[member].running for member in SOLVERS))
        duplicate = await self.candidate("flash-1")
        self.assertEqual(duplicate["status"], "correct")
        self.assertEqual(len(self.events_of("candidate_flag")), 1)
        second = await self.candidate(flag="FLAG{second}")
        self.assertNotEqual(second["request_id"], pending["request_id"])
        driver = self.runtime.slots["pro-1"].driver
        bump = self.runtime.room.queue_bump("pro-1", "remaining objective")
        driver.gate.set()
        await eventually(lambda: len(driver.prompts) == 2)
        self.assertEqual(driver.prompts[1]["instruction"], continuation_prompt(
            has_confirmed=True, insights=driver.prompts[1]["guidance"]))
        self.assertIn(bump["id"], driver.prompts[1]["instruction"])
        self.assertNotIn("previous attempt did not find the flag", driver.prompts[1]["instruction"])

    async def test_inconsistent_platform_completion_does_not_stop_continuation(self):
        pending = await self.candidate()
        await self.runtime.command("submission_result", {"request_id": pending["request_id"],
            "receipt": {"correct": True, "correct_flag_count": 1, "total_flag_count": 3},
            "platform": {"completed": True, "is_completed": True,
                         "correct_flag_count": 1, "flag_count": 3}})
        platform = self.runtime.room.meta("context")["platform"]
        self.assertFalse(platform["completed"])
        self.assertFalse(platform["is_completed"])
        self.assertFalse(self.runtime._completed())
        self.assertTrue(self.runtime._work_enabled.is_set())
        driver = self.runtime.slots["pro-1"].driver
        driver.gate.set()
        await eventually(lambda: len(driver.prompts) == 2)
        self.assertFalse(self.runtime.environment.closed)

    async def test_verified_completion_survives_stale_update_but_new_total_resumes_work(self):
        self.runtime._update_platform({"completed": True, "correct_flag_count": 2, "flag_count": 2})
        self.assertTrue(self.runtime._completed())
        self.assertFalse(self.runtime._work_enabled.is_set())
        self.runtime._update_platform({"completed": False, "correct_flag_count": 0, "flag_count": 2})
        self.assertTrue(self.runtime._completed())
        self.assertFalse(self.runtime._work_enabled.is_set())
        self.runtime._update_platform({"flag_count": 3})
        self.assertFalse(self.runtime._completed())
        self.assertFalse(self.runtime.room.meta("context")["platform"]["completed"])
        self.assertTrue(self.runtime._work_enabled.is_set())

    async def test_start_normalizes_inconsistent_completion_before_launching_roles(self):
        runtime = VeriaRuntime(self.run_dir, "inconsistent-start", "attempt-2", settings=dict(self.settings),
            environment_factory=FakeEnvironment, driver_factory=FakeDriver)

        async def emit(kind, **data):
            return None

        runtime.emit = emit
        try:
            await runtime.start({"run_id": "fixture-run", "deadline": 99999999999,
                "challenge": {"code": "inconsistent-start", "description": "synthetic only"},
                "platform": {"completed": True, "correct_flag_count": 1, "flag_count": 3}})
            await eventually(lambda: all(runtime.slots[x].driver for x in SOLVERS))
            self.assertFalse(runtime.room.meta("context")["platform"]["completed"])
            self.assertTrue(runtime._work_enabled.is_set())
            self.assertTrue(all(runtime.slots[x].running for x in SOLVERS))
            saved = json.loads((runtime.directory / "inputs" / "challenge.json").read_text())
            self.assertFalse(saved["platform"]["completed"])
        finally:
            await runtime.shutdown()

    async def test_concurrent_same_candidate_waits_one_id_without_blocking_protocol(self):
        one = asyncio.create_task(self.candidate("pro-1"))
        two = asyncio.create_task(self.candidate("pro-2"))
        await eventually(lambda: bool(self.events_of("candidate_flag")))
        event = self.events_of("candidate_flag")[0]
        health = await self.runtime.command("health", {})
        self.assertTrue(health["responsive"])
        await self.runtime.command("submission_result", {"request_id": event["request_id"],
                                                        "receipt": {"correct": True}})
        results = await asyncio.gather(one, two)
        self.assertEqual(results[0]["request_id"], results[1]["request_id"])
        self.assertEqual(len(self.events_of("candidate_flag")), 1)
        self.assertEqual(len(results[-1]["sources"]), 2)

    async def test_unknown_does_not_cooldown_and_late_result_recovers(self):
        pending = await self.candidate()
        await self.runtime.command("submission_result", {"request_id": pending["request_id"],
            "receipt": {"unknown": True, "correct": False}})
        self.assertEqual(self.runtime.room.candidate(pending["request_id"])["status"], "unknown")
        self.assertEqual(self.runtime.room.meta("submission_cooldowns", {}), {})
        await self.runtime.command("submission_result", {"request_id": pending["request_id"],
            "receipt": {"correct": True, "awarded": 1}})
        self.assertEqual(self.runtime.room.candidate(pending["request_id"])["status"], "correct")

    async def test_notice_is_not_broadcast_and_bumps_wait_for_next_turn(self):
        first = self.runtime.slots["pro-1"]
        driver = first.driver
        self.runtime.room.notice("pro-2", "private coordinator notice")
        self.assertFalse(self.runtime.room.mailbox("pro-1"))
        self.runtime.room.broadcast("coordinator", "reusable finding", recipients=list(SOLVERS))
        messages = await self.runtime.tool(first, "check_messages", {})
        self.assertEqual(len(messages["messages"]), 1)
        a = self.runtime.room.queue_bump("pro-1", "first correction")
        b = self.runtime.room.queue_bump("pro-1", "second correction")
        self.assertEqual(driver.prompts[0]["guidance"], [])
        self.assertEqual(driver.prompts[0]["instruction"], continuation_prompt(has_confirmed=False, first=True))
        driver.gate.set()
        await eventually(lambda: len(driver.prompts) == 2)
        self.assertEqual([x["id"] for x in driver.prompts[1]["guidance"]], [a["id"], b["id"]])
        self.assertEqual(driver.prompts[1]["instruction"], continuation_prompt(
            has_confirmed=False, insights=driver.prompts[1]["guidance"]))
        self.assertIn("Insights from other agents:", driver.prompts[1]["instruction"])
        self.assertIs(self.runtime.slots["pro-1"].driver, driver)

    async def test_quiesce_can_be_cancelled_without_destroying_members(self):
        driver = self.runtime.slots["pro-1"].driver
        await self.runtime.command("prepare_stop", {})
        self.assertTrue(self.runtime.environment.quiescing)
        self.assertFalse(driver.closed)
        driver.gate.set()
        await asyncio.sleep(.04)
        self.assertEqual(len(driver.prompts), 1)
        await self.runtime.command("cancel_stop", {})
        await eventually(lambda: len(driver.prompts) == 2)
        self.assertFalse(self.runtime.environment.quiescing)
        self.assertIs(self.runtime.slots["pro-1"].driver, driver)

    async def test_reporter_is_incremental_and_audit_cutoff_stays_fixed(self):
        slot = self.runtime.slots["reporter"]
        slot.task.cancel()
        await asyncio.gather(slot.task, return_exceptions=True)
        self.runtime.settings["veria_report_interval"] = .02
        slot = self.runtime._launch("reporter", "reporter")
        self.runtime.room.notice("pro-1", "first finding")
        request = await self.runtime.command("request_report", {"request_id": "round-one", "round_id": 1})
        self.runtime.room.notice("pro-2", "after-cutoff finding")
        await eventually(lambda: self.runtime.room.audit_report_status("round-one")["ready"])
        frozen = self.runtime.room.audit_report_status("round-one")
        calls = len(slot.driver.prompts)
        await asyncio.sleep(.08)
        self.assertEqual(len(slot.driver.prompts), calls)
        self.assertEqual(frozen["required_through_event"], request["required_through_event"])
        self.runtime.room.notice("pro-3", "new report evidence")
        await eventually(lambda: len(slot.driver.prompts) > calls)
        self.assertEqual(self.runtime.room.audit_report_status("round-one")["report_version"], frozen["report_version"])

    async def test_same_attempt_recovers_candidate_and_session_identity(self):
        candidate = await self.candidate()
        self.runtime.room.notice("pro-1", "survives restart", message_id="persistent-notice")
        old_session = self.runtime.room.agent("pro-1")["session_id"]
        await self.runtime.command("stop", {"preserve_environment": True})
        environment = self.runtime.environment
        await self.runtime.shutdown()
        self.assertFalse(environment.closed)
        self.assertIn("pro-1", environment.cleaned)
        self.runtime = self.make_runtime()
        await self.runtime.start({"run_id": "fixture-run", "deadline": 99999999999,
            "platform": {"completed": False}})
        await eventually(lambda: self.runtime.slots["pro-1"].driver is not None)
        self.assertEqual(self.runtime.room.agent("pro-1")["session_id"], old_session)
        self.assertEqual(self.events_of("candidate_flag")[-1]["request_id"], candidate["request_id"])
        self.assertIsNotNone(self.runtime.room.veria_message("persistent-notice"))

    async def test_reviewer_does_not_reenter_and_new_review_has_separate_workspace(self):
        first = await self.runtime.command("review", {"review_id": "review-first",
            "platform": {"correct_flag_count": 1}})
        self.assertTrue(first["started"])
        await eventually(lambda: self.runtime.room.meta("last_review", {}).get("review_id") == "review-first")
        await eventually(lambda: self.runtime.slots["reviewer"].task.done())
        second = await self.runtime.command("review", {"review_id": "review-second"})
        self.assertTrue(second["started"])
        await eventually(lambda: self.runtime.room.meta("last_review", {}).get("review_id") == "review-second")
        reviewer_drivers = [d for d in FakeDriver.instances if d.role == "reviewer"]
        self.assertNotEqual(reviewer_drivers[0].workspace, reviewer_drivers[1].workspace)
        self.assertEqual(reviewer_drivers[0].prompts[0]["platform"]["correct_flag_count"], 1)
        await eventually(lambda: self.runtime.slots["reviewer"].task.done())
        self.assertNotIn("reviewer", self.runtime._role_health()[1])

    async def test_coordinator_broadcast_reaches_active_reviewer_and_retains_coordination_sources(self):
        entered, resume = asyncio.Event(), asyncio.Event()
        class HoldingReviewerDriver(FakeDriver):
            async def turn(self, prompt):
                if self.role == "reviewer":
                    entered.set()
                    await resume.wait()
                return await super().turn(prompt)
        self.runtime.driver_factory = HoldingReviewerDriver
        await self.runtime.command("review", {"review_id": "review-coordination"})
        await asyncio.wait_for(entered.wait(), timeout=2)
        reviewer = self.runtime.slots["reviewer"]
        notice = await self.runtime.tool(reviewer, "notify_coordinator", {"message": "Reproduction requires resetting shared service; please broadcast and coordinate."})
        sources = [notice["message_id"]]
        broadcast = await self.runtime.tool(self.runtime.slots["coordinator"], "broadcast", {
            "message": "Shared reset proposed; wait until affected members are ready.", "source_message_ids": sources})
        self.assertEqual(set(broadcast["recipients"]), {*SOLVERS, "reviewer"})
        received = await self.runtime.tool(reviewer, "check_messages", {})
        coordination = next(item for item in received["messages"] if item["id"] == broadcast["id"])
        self.assertEqual(coordination["source_message_ids"], sources)
        self.assertEqual(self.runtime.room.evidence_record(broadcast["event_seq"])["data"]["source_message_ids"], sources)
        self.assertTrue(reviewer.running)
        self.assertFalse(self.runtime.environment.cleaned)
        self.assertFalse(self.runtime.environment.closed)
        resume.set()
        await eventually(lambda: reviewer.task.done())
        following = await self.runtime.tool(self.runtime.slots["coordinator"], "broadcast", {"message": "review finished"})
        self.assertNotIn("reviewer", following["recipients"])

    async def test_health_exposes_unexpected_loop_exit_and_disconnected_idle_client(self):
        reporter = self.runtime.slots["reporter"]
        reporter.task.cancel()
        await asyncio.gather(reporter.task, return_exceptions=True)
        coordinator = self.runtime.slots["coordinator"]
        await eventually(lambda: coordinator.driver is not None and not coordinator.running)
        coordinator.driver.alive = False
        roles, failures = self.runtime._role_health()
        self.assertIn("reporter", failures)
        self.assertIn("coordinator", failures)
        self.assertTrue(coordinator.wake.is_set())
        self.assertEqual(next(r for r in roles if r["agent_id"] == "reporter")["degraded_reason"],
            "role loop exited")

    async def test_completed_platform_prevents_new_continuations(self):
        driver = self.runtime.slots["pro-1"].driver
        await self.runtime.command("snapshot", {"platform": {"completed": True,
            "correct_flag_count": 2, "flag_count": 2}})
        driver.gate.set()
        await asyncio.sleep(.05)
        self.assertEqual(len(driver.prompts), 1)

    async def test_stale_attempt_command_and_external_evidence_rejected(self):
        with self.assertRaisesRegex(ValueError, "stale attempt"):
            await self.runtime.command("message", {"attempt_id": "old", "body": "stale"})
        with self.assertRaisesRegex(ValueError, "inside"):
            await self.runtime.tool(self.runtime.slots["coordinator"], "read_evidence", {"path": "/etc/passwd"})

    async def test_significant_evidence_rejects_fabricated_and_reporter_sources(self):
        slot = self.runtime.slots["reporter"]
        with self.assertRaisesRegex(ValueError, "existing original"):
            await self.runtime.tool(slot, "significant_evidence", {"evidence_id": "made-up"})
        own = self.runtime.room.record_evidence("reporter", "visible_text", "my summary")
        with self.assertRaisesRegex(ValueError, "existing original"):
            await self.runtime.tool(slot, "significant_evidence", {"evidence_id": own["id"]})
        source = self.runtime.room.record_evidence("pro-1", "tool_execution_result", {"stdout": "capability proof"})
        await self.runtime.tool(slot, "significant_evidence", {"evidence_id": source["id"], "summary": "important"})
        duplicate = await self.runtime.tool(slot, "significant_evidence", {"source_event_id": source["event_seq"], "summary": "same reworded"})
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(len(self.events_of("significant_evidence")), 1)

    async def test_repeated_incorrect_receipt_counts_once_per_sender(self):
        candidate = await self.candidate()
        self.runtime.room.save_candidate("pro-1", candidate["flag"], evidence_refs=["another source"])
        await self.runtime.command("submission_result", {"request_id": candidate["request_id"],
                                                        "receipt": {"correct": False}})
        await self.runtime.command("submission_result", {"request_id": candidate["request_id"],
                                                        "receipt": {"correct": False, "awarded": 0}})
        self.assertEqual(self.runtime.room.meta("submission_cooldowns")["pro-1"]["wrong_count"], 1)

    async def test_old_log_cannot_prove_reproduction(self):
        from xinghuo.veria_runtime import VeriaSlot
        candidate = self.runtime.room.save_candidate("pro-1", "FLAG{known}")
        self.runtime.room.apply_receipt(candidate["request_id"], {"correct": True})
        self.runtime.room.register_member("reviewer", "reviewer", VERIA_MODELS["reviewer"])
        self.runtime.room.start_review("review-proof", {"review_id": "review-proof"})
        slot = VeriaSlot("reviewer", "reviewer", review_id="review-proof", tool_executions=100)
        old = self.runtime.room.record_evidence("reviewer", "tool_execution_result", "old successful run",
            metadata={"success": True, "review_id": "previous-review"})
        payload = {"summary": "claim", "reproductions": [{"request_id": candidate["request_id"],
            "status": "reproduced", "evidence_refs": [old["path"]]}]}
        with self.assertRaisesRegex(ValueError, "this review"):
            await self.runtime._complete_review(slot, payload)
        payload["reproductions"][0]["status"] = "not_applicable"
        with self.assertRaisesRegex(ValueError, "known flags"):
            await self.runtime._complete_review(slot, payload)


    async def test_review_checkpoint_rejects_unrelated_and_echoed_output_and_enforces_order(self):
        candidate = self.runtime.room.save_candidate("pro-1", "FLAG{known}")
        self.runtime.room.apply_receipt(candidate["request_id"], {"correct": True})
        self.runtime.room.start_review("proof", {"review_id": "proof"})
        slot = VeriaSlot("reviewer", "reviewer", review_id="proof")
        item = {"request_id": candidate["request_id"], "status": "reproduced", "acquisition_steps": "rerun acquisition script"}
        async def record(command, result):
            evidence = self.runtime.room.record_evidence("reviewer", "tool_execution_result",
                {"command": command, "result": {"stdout": result}}, metadata={"review_id": "proof", "success": True})
            item["evidence_refs"] = [evidence["path"]]
            return evidence
        await record("true", "")
        with self.assertRaisesRegex(ValueError, "fresh acquisition"):
            await self.runtime.tool(slot, "record_reproductions", {"reproductions": [item]})
        await record("echo 'FLAG{known}'", "FLAG{known}")
        with self.assertRaisesRegex(ValueError, "copied command"):
            await self.runtime.tool(slot, "record_reproductions", {"reproductions": [item]})
        acquisition = await record("python acquire.py", "FLAG{known}")
        payload = {"reproductions": [item], "sections": {key: "unchecked: fixture has no extra routes" for key in
            ("lateral_opportunities", "coverage", "missed_routes", "prior_rejections", "member_health")}}
        with self.assertRaisesRegex(ValueError, "record_reproductions"):
            await self.runtime._complete_review(slot, payload)
        checkpoint = await self.runtime.tool(slot, "record_reproductions", payload)
        self.assertEqual(await self.runtime.tool(slot, "record_reproductions", payload), checkpoint)
        payload["audit_evidence_refs"] = [acquisition["path"]]
        with self.assertRaisesRegex(ValueError, "after the reproduction"):
            await self.runtime._complete_review(slot, payload)
        audit = self.runtime.room.record_evidence("reviewer", "tool_execution_result",
            {"result": "no additional route"}, metadata={"review_id": "proof", "success": True})
        payload["audit_evidence_refs"] = [audit["path"]]
        self.assertTrue((await self.runtime._complete_review(slot, payload))["completed"])

    async def test_review_blocked_requires_reason_and_can_complete_bounded_review(self):
        candidate = self.runtime.room.save_candidate("pro-1", "FLAG{known}")
        self.runtime.room.apply_receipt(candidate["request_id"], {"correct": True})
        self.runtime.room.start_review("blocked", {"review_id": "blocked"})
        slot = VeriaSlot("reviewer", "reviewer", review_id="blocked")
        item = {"request_id": candidate["request_id"], "status": "blocked"}
        with self.assertRaisesRegex(ValueError, "concrete reason"):
            await self.runtime._record_reproductions(slot, {"reproductions": [item]})
        item["reason"] = "One-shot instance consumed; coordinator cannot safely reset before deadline"
        await self.runtime._record_reproductions(slot, {"reproductions": [item]})
        with self.assertRaisesRegex(ValueError, "sections must cover"):
            await self.runtime._complete_review(slot, {"reproductions": [item]})
        result = await self.runtime._complete_review(slot, {"reproductions": [item], "sections": {
            key: "blocked: one-shot instance unavailable" for key in
            ("lateral_opportunities", "coverage", "missed_routes", "prior_rejections", "member_health")}})
        self.assertTrue(result["completed"])

    async def test_review_freezes_known_flags_and_empty_audit_cannot_claim_success(self):
        self.runtime.room.start_review("frozen", {"review_id": "frozen", "confirmed_submissions": []})
        slot = VeriaSlot("reviewer", "reviewer", review_id="frozen")
        await self.runtime._record_reproductions(slot, {"reproductions": []})
        later = self.runtime.room.save_candidate("pro-1", "FLAG{later}")
        self.runtime.room.apply_receipt(later["request_id"], {"correct": True})
        sections = {key: "all checked" for key in
            ("lateral_opportunities", "coverage", "missed_routes", "prior_rejections", "member_health")}
        with self.assertRaisesRegex(ValueError, "without audit evidence"):
            await self.runtime._complete_review(slot, {"reproductions": [], "sections": sections})
        sections = {key: {"status": "unchecked", "reason": "deadline reached"} for key in sections}
        self.assertTrue((await self.runtime._complete_review(slot, {"reproductions": [], "sections": sections}))["completed"])

    async def test_changed_acquisition_evidence_rejected(self):
        candidate = self.runtime.room.save_candidate("pro-1", "FLAG{known}")
        self.runtime.room.apply_receipt(candidate["request_id"], {"correct": True})
        self.runtime.room.start_review("changed", {"review_id": "changed"})
        slot = VeriaSlot("reviewer", "reviewer", review_id="changed")
        evidence = self.runtime.room.record_evidence("reviewer", "tool_execution_result",
            {"command": "python acquire.py", "result": "FLAG{known}"}, metadata={"review_id": "changed", "success": True})
        Path(evidence["path"]).write_text("tampered")
        with self.assertRaisesRegex(ValueError, "changed after recording"):
            await self.runtime._record_reproductions(slot, {"reproductions": [{"request_id": candidate["request_id"],
                "status": "reproduced", "acquisition_steps": "reran script", "evidence_refs": [evidence["path"]]}]})

    async def test_activity_extends_turn_timeout_and_success_resets_consecutive_failures(self):
        runtime = self.runtime
        class ActiveDriver(FakeDriver):
            async def turn(self, prompt):
                for _ in range(6):
                    await asyncio.sleep(.02)
                    await self.on_event("tool_execution_result", {"success": True, "result": "new evidence"})
                runtime.stopping.set()
                return {"result": "finished a long useful turn"}
        slot = VeriaSlot("pro-1", "solver")
        self.runtime.driver_factory = ActiveDriver
        self.runtime.settings["role_turn_timeout"] = .05
        self.runtime.room.update_agent("pro-1", restarts=2)
        await self.runtime._role_loop(slot)
        self.assertGreater(self.runtime.room.agent("pro-1")["turns"], 0)
        self.assertEqual(self.runtime.room.agent("pro-1")["restarts"], 0)
        await self.runtime._close_driver(slot)

    def test_safe_error_redacts_non_sk_provider_and_platform_secrets(self):
        secrets = {name: "fixture.secret." + name for name in
            ("GLM_API_KEY", "DEEPSEEK_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "BENCHMARK_TOKEN")}
        with patch.dict(os.environ, secrets):
            result = safe_error(RuntimeError("connection failed: " + " ".join(secrets.values())))
        self.assertTrue(all(secret not in result for secret in secrets.values()))
        self.assertIn("connection failed", result)
        self.assertEqual(result.count("[redacted]"), len(secrets))

    async def test_invalid_review_completion_exhausts_consecutive_retries(self):
        class InvalidReviewDriver(FakeDriver):
            async def turn(self, prompt):
                return {"result": json.dumps({"reproductions": []})}
        self.runtime.driver_factory = InvalidReviewDriver
        self.runtime.settings["max_retries"] = 1
        self.runtime._delay = lambda delay: asyncio.sleep(0)
        await self.runtime.command("review", {"review_id": "invalid"})
        await eventually(lambda: self.runtime.slots["reviewer"].task.done())
        self.assertEqual(self.runtime.room.agent("reviewer")["restarts"], 2)
        self.assertEqual(self.runtime.room.agent("reviewer")["status"], "failed")
        self.assertIsNone(self.runtime.room.meta("last_review"))

    async def test_new_review_rearms_its_own_retries_after_failed_review_without_changing_old_evidence(self):
        class InvalidReviewDriver(FakeDriver):
            async def turn(self, prompt):
                return {"result": json.dumps({"reproductions": []})}
        self.runtime.driver_factory = InvalidReviewDriver
        self.runtime.settings["max_retries"] = 1
        self.runtime._delay = lambda delay: asyncio.sleep(0)
        await self.runtime.command("review", {"review_id": "old-failed"})
        await eventually(lambda: self.runtime.slots["reviewer"].task.done())
        old_review = self.runtime.room.review("old-failed")
        self.assertEqual(old_review["status"], "failed")
        self.assertEqual(self.runtime.room.agent("reviewer")["restarts"], 2)
        # Retrying a terminal identity cannot reset it or allocate new time.
        same = await self.runtime.command("review", {"review_id": "old-failed"})
        self.assertFalse(same["started"])
        self.assertEqual(self.runtime.room.agent("reviewer")["restarts"], 2)
        self.runtime.driver_factory = FakeDriver
        fresh = await self.runtime.command("review", {"review_id": "new-healthy"})
        self.assertTrue(fresh["started"])
        await eventually(lambda: self.runtime.room.meta("last_review", {}).get("review_id") == "new-healthy")
        self.assertTrue(any(driver.role == "reviewer" and "new-healthy" in str(driver.workspace)
                            for driver in FakeDriver.instances))
        self.assertEqual(self.runtime.room.agent("reviewer")["restarts"], 0)
        self.assertEqual(self.runtime.room.review("old-failed"), old_review)
        self.assertEqual(self.runtime.room.review("new-healthy")["request"]["deadline"], fresh["deadline"])

    async def test_silent_turn_times_out_and_cleanup_failure_does_not_skip_bounded_recovery(self):
        class SilentDriver(FakeDriver):
            async def turn(self, prompt):
                await asyncio.Event().wait()
        slot = VeriaSlot("pro-1", "solver")
        self.runtime.driver_factory = SilentDriver
        self.runtime.settings.update(role_turn_timeout=.02, max_retries=1)
        with self.assertRaisesRegex(TimeoutError, "inactive"):
            await self.runtime._turn(slot)
        async def broken_cleanup(agent_id):
            raise RuntimeError("fixture cleanup unavailable")
        self.runtime.environment.stop_member = broken_cleanup
        self.runtime._delay = lambda delay: asyncio.sleep(0)
        self.assertTrue(await self.runtime._failure(slot, TimeoutError("inactive")))
        self.assertFalse(await self.runtime._failure(slot, TimeoutError("inactive")))
        self.assertEqual(self.runtime.room.agent("pro-1")["status"], "failed")


if __name__ == "__main__":
    unittest.main()
