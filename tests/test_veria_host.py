"""Veria host protocol acceptance, using only fakes and local executables."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from xinghuo.config import (Config, VERIA_MODELS, veria_environment_cleanup_timeout,
                           veria_stop_grace,
                           veria_supervisor_cleanup_timeout)
from xinghuo.core import SubmissionReceipt, completion_confirmed
from xinghuo.runtime import TeamProcess
from xinghuo.store import Store
from xinghuo.strategy import ReviewDecision
from xinghuo.supervisor import Supervisor
from test_supervisor import Clock, FakePlatform, FakeStrategy, FakeTeam


def veria_config(**changes):
    return Config(team_engine="veria", backend="claude", veria_python=sys.executable,
                  veria_tool_image="local/test:offline", veria_tool_cpu_limit=2,
                  veria_tool_memory_limit="4g", **changes).validate()


class VeriaFakeTeam(FakeTeam):
    async def stop(self, *, preserve_environment=False):
        self.preserve_environment = preserve_environment
        await super().stop()


class VeriaConfigTests(unittest.TestCase):
    def test_completion_requires_platform_confirmation_and_consistent_counts(self):
        for total in (1, 2, 4, 6):
            with self.subTest(total=total):
                self.assertFalse(completion_confirmed({"completed": False,
                    "flag_count": total, "correct_flag_count": total}))
                self.assertFalse(completion_confirmed({"completed": True,
                    "flag_count": total, "correct_flag_count": total - 1}))
                self.assertTrue(completion_confirmed({"is_completed": True,
                    "flag_count": total, "correct_flag_count": total}))
        self.assertTrue(completion_confirmed({"completed": True, "flag_count": 0}))
        for total in (-1, None, "2", True):
            self.assertFalse(completion_confirmed({"completed": True,
                "flag_count": total, "correct_flag_count": 2}))

    def test_old_configuration_defaults_to_native(self):
        self.assertEqual(Config(**{"backend": "demo"}).validate().team_engine, "native")

    def test_invalid_or_implicit_veria_configuration_is_rejected(self):
        base = veria_config().to_dict()
        for change in ({"backend": "demo"}, {"team_engine": "typo"}, {"veria_python": "python"},
                       {"veria_python": "/missing/python"}, {"veria_tool_image": None},
                       {"veria_tool_cpu_limit": None}, {"veria_tool_cpu_limit": float("nan")},
                       {"veria_tool_memory_limit": "0g"}, {"veria_tool_memory_limit": "4g --privileged"},
                       {"veria_tool_network": ""}, {"veria_tool_network": "bridge other"},
                       {"veria_report_interval": 0}, {"veria_models": {"pro-1": "other"}},
                       {"veria_upstream_commit": "not-the-pinned-revision"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                Config(**{**base, **change}).validate()

    def test_model_overrides_preserve_fixed_identities_and_defaults_are_not_shared(self):
        config = veria_config()
        config.veria_models["pro-1"] = "configured-pro"
        config.validate()
        self.assertEqual(set(config.veria_models), set(VERIA_MODELS))
        self.assertEqual(veria_config().veria_models["pro-1"], "deepseek-v4-pro")
        self.assertNotIn("api_key", config.to_dict())

    def test_explicit_network_is_saved_and_reaches_runtime_settings(self):
        config = veria_config(veria_tool_network="none")
        self.assertEqual(Config(**config.to_dict()).validate().veria_tool_network, "none")
        team = TeamProcess(Path("/tmp"), "offline-network", "attempt", backend="claude", settings=config.to_dict())
        self.assertEqual(team.settings["veria_tool_network"], "none")

    def test_old_run_cannot_be_resumed_as_veria(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                store.set_meta("config", {"backend": "claude"})
                with self.assertRaisesRegex(ValueError, "cannot change"):
                    Supervisor(FakePlatform(), store, veria_config(), strategy=FakeStrategy())
            finally:
                store.close()


class VeriaHostTests(unittest.IsolatedAsyncioTestCase):
    async def test_checkpoint_rpc_leaves_time_after_runtime_drain_deadline(self):
        original = self.team.request
        self.sup.config.shutdown_timeout = .02
        async def request(command, payload=None, **kwargs):
            if command == "prepare_stop":
                async def drain():
                    await asyncio.sleep(.04)
                    return {"flushed": False, "in_flight": ["pro-1"]}
                return await asyncio.wait_for(drain(), kwargs["timeout"])
            return await original(command, payload, **kwargs)
        self.team.request = request
        await self.sup.finish("A", "shutdown")
        events = list(self.store.db.execute("SELECT kind,body FROM events WHERE kind LIKE 'checkpoint_%'"))
        self.assertEqual([kind for kind, _ in events], ["checkpoint_ready"])
        self.assertEqual(json.loads(events[0][1]), {"flushed": False, "in_flight": ["pro-1"]})
        self.assertEqual(self.platform.close_calls, ["A"])

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        self.platform, self.strategy, self.clock = FakePlatform(), FakeStrategy(), Clock()
        self.config = veria_config(epsilon=0, operation_timeout=.2, shutdown_timeout=.2)
        self.sup = Supervisor(self.platform, self.store, self.config, strategy=self.strategy,
                              team_factory=VeriaFakeTeam, clock=self.clock)
        await self.sup.initialize()
        await self.sup.start_challenge("A", "test-selection")
        self.team = self.sup.teams["A"]
        self.attempt = self.store.challenge("A")["attempt_id"]
        self.report = {"request_id": f"{self.attempt}:1", "ready": True,
                       "report_mode": "reporter_snapshot", "required_through_event": 5, "report_version": 2}
        self.snapshot = {"report": {"summary": "固定截点报告", "evidence_refs": []},
                         "report_mode": "reporter_snapshot", "required_through_event": 5,
                         "report_version": 2, "through_event": 5, "evidence_watermark": 500,
                         "pending_after_cutoff": 495, "windows": []}
        async def report(payload):
            return dict(self.report, request_id=payload["request_id"])
        async def snapshot(payload):
            return dict(self.snapshot)
        self.team.handlers.update(request_report=report, audit_snapshot=snapshot)

    async def asyncTearDown(self):
        await self.sup.shutdown()
        self.store.close()
        self.temp.cleanup()

    def event(self, kind, **values):
        return {"type": kind, "code": "A", "attempt_id": self.attempt, "source": "reporter", **values}

    async def settle(self):
        if self.sup.jobs:
            await asyncio.gather(*list(self.sup.jobs.values()))
            self.sup._reap()

    async def test_report_fixed_watermark_survives_continued_solver_events(self):
        await self.sup.audit_one("A", 1)
        self.assertEqual(len(self.strategy.calls), 1)
        packet = self.strategy.calls[0]
        self.assertEqual(packet["report_mode"], "reporter_snapshot")
        self.assertEqual(packet["current"]["required_through_event"], 5)
        self.assertEqual(packet["current"]["evidence_watermark"], 500)
        self.assertNotIn("leader_report_request", packet["current"])
        self.assertTrue(packet["current"]["reporter_report_request"]["ready"])
        self.assertEqual([kind for kind, _ in self.team.requests][:3],
                         ["request_report", "health", "audit_snapshot"])

    async def test_version_or_cutoff_mismatch_routes_material_gap_to_reporter(self):
        for change in ({"report_version": 3}, {"required_through_event": 6}, {"through_event": 4},
                       {"report_mode": "leader_report"}):
            old = dict(self.snapshot)
            self.snapshot.update(change)
            await self.sup.audit_one("A", 1)
            self.assertEqual(self.strategy.calls, [])
            self.assertEqual(self.team.requests[-1][0], "message")
            self.assertEqual(self.team.requests[-1][1]["recipient"], "reporter")
            self.snapshot = old

    async def test_strategy_question_routes_to_coordinator(self):
        async def decide(packet):
            return ReviewDecision("need_info", "请确认此路线是否已验证")
        self.strategy.decide = decide
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.team.requests[-1][1]["recipient"], "coordinator")

    async def test_health_failure_cannot_count_as_switch_recommendation(self):
        async def health(payload):
            return {"healthy": False, "recoverable": True, "core_failures": ["reporter"]}
        self.team.handlers["health"] = health
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.strategy.calls, [])
        row = self.store.challenge("A")
        self.assertEqual(row["confirmation"]["streak"], 0)
        self.assertNotIn("recover:A", self.sup.jobs)

    async def test_usable_runtime_can_be_reviewed_with_explicit_degraded_roles(self):
        async def health(payload):
            return {"healthy": True, "responsive": True, "needs_restart": False,
                    "degraded_roles": ["pro-1", "reviewer"], "core_failures": ["pro-1", "reviewer"]}
        self.team.handlers["health"] = health
        await self.sup.audit_one("A", 1)
        self.assertEqual(len(self.strategy.calls), 1)
        health = self.strategy.calls[0]["current"]["harness_health"]["runtime"]
        self.assertEqual(health["degraded_roles"], ["pro-1", "reviewer"])
        self.assertNotIn("recover:A", self.sup.jobs)

    async def test_start_context_contains_identity_absolute_deadline_and_recovery(self):
        context = self.team.context
        self.assertEqual(context["attempt_id"], self.attempt)
        self.assertEqual(context["run_id"], self.store.get_meta("run_id"))
        self.assertEqual(context["deadline"], self.sup.deadline)
        self.assertEqual(context["addresses"], ["A.lab:80"])
        self.assertEqual(context["challenge"]["description"], "题目A")
        self.assertEqual(context["platform"]["flag_count"], 3)
        self.assertIn("submissions", context["recovery"])
        self.assertEqual(self.sup.status()["team_engine"], "veria")

    async def test_stale_or_unidentified_events_cannot_submit_or_invalidate_decision(self):
        before = self.store.challenge("A")["revision"]
        for kind in ("candidate_flag", "significant_evidence", "heartbeat"):
            self.assertFalse(self.sup.handle_team_event("A", self.event(kind, attempt_id="old", flag="bad")))
        self.assertFalse(self.sup.handle_team_event("A", {"type": "candidate_flag", "flag": "bad"}))
        await self.settle()
        self.assertEqual(self.platform.submit_calls, [])
        self.assertEqual(self.store.challenge("A")["revision"], before)

    async def test_significant_evidence_is_deduplicated_by_source_identity(self):
        before = self.store.challenge("A")["revision"]
        event = self.event("significant_evidence", evidence_id="review:17:path:3")
        self.assertTrue(self.sup.handle_team_event("A", event))
        self.assertFalse(self.sup.handle_team_event("A", dict(event, source="coordinator")))
        self.assertFalse(self.sup.handle_team_event("A", self.event("significant_evidence")))
        self.assertEqual(self.store.challenge("A")["revision"], before + 1)

    async def test_equal_candidates_keep_both_sources_and_actual_success_receipt(self):
        receipt = SubmissionReceipt(correct=True, awarded=10, correct_flag_count=1,
                                    total_flag_count=3, cumulative_score=10, matched_flag_index=0)
        self.platform.receipts["FLAG{Case Sensitive}"] = receipt
        self.sup.handle_team_event("A", self.event("candidate_flag", flag=" FLAG{Case Sensitive} ",
                                                  request_id="r1", sender="pro-1"))
        self.sup.handle_team_event("A", self.event("candidate_flag", flag="FLAG{Case Sensitive}",
                                                  request_id="r2", sender="flash-1"))
        await self.settle()
        self.assertEqual(self.platform.submit_calls, [("A", "FLAG{Case Sensitive}")])
        operation = self.store.operations("submit")[0]
        self.assertEqual(operation["attempt_id"], self.attempt)
        self.assertEqual(len(operation["sources"]), 2)
        self.assertEqual(operation["request_id"], "r1")
        deliveries = [payload for kind, payload in self.team.requests if kind == "submission_result"]
        self.assertEqual(len(deliveries), 2)
        self.assertEqual(deliveries[1]["receipt"], receipt.to_dict())
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertFalse(self.store.challenge("A")["challenge"]["completed"])
        self.assertTrue(self.team.alive)

    async def test_unknown_receipt_remains_unknown_without_progress(self):
        self.platform.receipts["uncertain"] = SubmissionReceipt(unknown=True)
        await self.sup.submit("A", "uncertain", "r1", "pro-1", self.attempt)
        operation = self.store.operations("submit")[0]
        self.assertEqual(operation["status"], "unknown")
        self.assertEqual(self.store.challenge("A")["challenge"]["correct_flag_count"], 0)
        self.assertTrue(self.team.requests[-1][1]["receipt"]["unknown"])

    async def test_all_flag_counts_wait_for_explicit_platform_completion(self):
        self.platform.receipts["last"] = SubmissionReceipt(correct=True, correct_flag_count=3, total_flag_count=3)
        await self.sup.submit("A", "last", "r1", "pro-1", self.attempt)
        row = self.store.challenge("A")
        self.assertEqual(row["challenge"]["correct_flag_count"], 3)
        self.assertFalse(row["challenge"]["completed"])
        self.assertTrue(self.team.alive)
        self.assertNotIn("finish:A", self.sup.jobs)
        # FakePlatform has now independently confirmed completed in list_challenges.
        await self.sup.reconcile()
        await self.settle()
        self.assertEqual(self.store.challenge("A")["status"], "completed")

    async def test_inconsistent_completion_cannot_close_after_an_incorrect_candidate(self):
        self.platform.update("A", completed=True, correct_flag_count=1, flag_count=3)
        await self.sup.reconcile()
        row = self.store.challenge("A")
        self.assertFalse(row["challenge"]["completed"])
        self.assertFalse(self.sup.platform_card(row)["completed"])
        self.assertNotIn("finish:A", self.sup.jobs)
        self.platform.receipts["wrong"] = SubmissionReceipt(correct=False)
        await self.sup.submit("A", "wrong", "r1", "pro-1", self.attempt)
        await self.settle()
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertTrue(self.team.alive)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.store.db.execute(
            "SELECT count(*) FROM events WHERE kind='completion_inconsistent'").fetchone()[0], 1)

    async def test_unknown_receipt_cannot_close_inconsistent_saved_completion(self):
        row = self.store.challenge("A")
        row["challenge"].update(completed=True, correct_flag_count=1, flag_count=3)
        self.store.save_challenge(row)
        self.assertFalse(self.sup.platform_card(row)["completed"])
        self.platform.receipts["unknown"] = SubmissionReceipt(unknown=True)
        await self.sup.submit("A", "unknown", "r1", "pro-1", self.attempt)
        await self.settle()
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertFalse(self.store.challenge("A")["challenge"]["completed"])
        self.assertTrue(self.team.alive)
        self.assertEqual(self.platform.close_calls, [])

    async def test_receipt_increasing_total_invalidates_earlier_completion(self):
        row = self.store.challenge("A")
        row["challenge"].update(completed=True, correct_flag_count=1, flag_count=1)
        self.store.save_challenge(row)
        self.platform.receipts["new-total"] = SubmissionReceipt(correct=False,
            correct_flag_count=1, total_flag_count=3)
        await self.sup.submit("A", "new-total", "r1", "pro-1", self.attempt)
        await self.settle()
        row = self.store.challenge("A")
        self.assertFalse(row["challenge"]["completed"])
        self.assertEqual(row["status"], "active")
        self.assertEqual(self.platform.close_calls, [])

    async def test_verified_completion_survives_stale_platform_list(self):
        row = self.store.challenge("A")
        row["challenge"].update(completed=True, correct_flag_count=3, flag_count=3)
        self.store.save_challenge(row)
        self.platform.update("A", completed=False, correct_flag_count=0, flag_count=3)
        await self.sup.reconcile()
        await self.settle()
        row = self.store.challenge("A")
        self.assertTrue(row["challenge"]["completed"])
        self.assertEqual(row["challenge"]["correct_flag_count"], 3)
        self.assertEqual(row["status"], "completed")
        self.assertEqual(self.platform.close_calls, ["A"])

    async def test_inconsistent_saved_completion_does_not_stick_on_reconcile(self):
        row = self.store.challenge("A")
        row["challenge"].update(completed=True, correct_flag_count=1, flag_count=3)
        self.store.save_challenge(row)
        self.platform.update("A", completed=False, correct_flag_count=0, flag_count=3)
        await self.sup.reconcile()
        await self.settle()
        row = self.store.challenge("A")
        self.assertFalse(row["challenge"]["completed"])
        self.assertEqual(row["challenge"]["correct_flag_count"], 1)
        self.assertTrue(self.team.alive)
        self.assertEqual(self.platform.close_calls, [])

    async def test_finish_rechecks_completion_after_checkpoint(self):
        row = self.store.challenge("A")
        row["challenge"].update(completed=True, correct_flag_count=1, flag_count=1)
        self.store.save_challenge(row)

        async def checkpoint(payload):
            row = self.store.challenge("A")
            row["challenge"]["flag_count"] = 3
            self.store.save_challenge(row)
            return {"flushed": True}

        self.team.handlers["prepare_stop"] = checkpoint
        await self.sup.finish("A", "completed")
        self.assertTrue(self.team.alive)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.team.requests[-1][0], "cancel_stop")

    async def test_corrected_total_reopens_previously_completed_stopped_challenge(self):
        row = self.store.challenge("A")
        row.update(status="completed", close_intent="completed")
        row["challenge"].update(completed=True, correct_flag_count=1, flag_count=1)
        self.store.save_challenge(row)
        self.platform.update("A", completed=False, correct_flag_count=1,
                             flag_count=3, container_status="stopped", container_addr=())
        await self.sup.reconcile()
        row = self.store.challenge("A")
        self.assertEqual(row["status"], "paused")
        self.assertFalse(row["challenge"]["completed"])
        self.assertIn("A", {candidate["code"] for candidate in self.sup.eligible()})

    async def test_submission_timeout_is_delivered_as_unknown_and_retried_with_original_identity(self):
        self.platform.receipts["uncertain"] = TimeoutError("local fake timeout")
        await self.sup.submit("A", "uncertain", "r1", "pro-1", self.attempt)
        operation = self.store.operations("submit")[0]
        self.assertEqual(operation["status"], "unknown")
        self.assertTrue(self.team.requests[-1][1]["receipt"]["unknown"])
        self.platform.receipts["uncertain"] = SubmissionReceipt(correct=True, correct_flag_count=1)
        await self.sup.submit("A", "uncertain", "r1", "pro-1", self.attempt)
        confirmed = self.store.operations("submit")[0]
        self.assertEqual(confirmed["id"], operation["id"])
        self.assertEqual(confirmed["request_id"], "r1")
        self.assertEqual(confirmed["status"], "confirmed")

    async def test_team_process_selects_isolated_interpreter_and_explicit_module_path(self):
        calls = []
        class Process:
            returncode = None
            pid = 12345
            async def communicate(self):
                self.returncode = 0
                return b"3.14\n", b""
            async def wait(self):
                self.returncode = 0
                return 0
        async def spawn(*args, **kwargs):
            calls.append((args, kwargs))
            return Process()
        team = TeamProcess(Path(self.temp.name), "launch-check", "attempt", backend="claude",
                           settings=self.config.to_dict())
        with patch("xinghuo.runtime.asyncio.create_subprocess_exec", side_effect=spawn), \
             patch.object(team, "request", new=AsyncMock(return_value={})), \
             patch.object(team, "_read_stdout", new=AsyncMock()):
            await team.start({})
            await team.stop()
        args, kwargs = calls[1]
        self.assertEqual(args[:3], (self.config.veria_python, "-m", "xinghuo.veria_runtime"))
        # Source checkouts and installed wheels must both launch the same
        # package that owns TeamProcess, rather than an unrelated test folder.
        package_root = Path(sys.modules[TeamProcess.__module__].__file__).resolve().parents[1]
        self.assertIn(str(package_root), kwargs["env"]["PYTHONPATH"])
        self.assertTrue(kwargs["start_new_session"])

    async def test_late_receipt_is_journaled_but_cannot_change_new_attempt(self):
        async def delayed_submit(code, flag):
            row = self.store.challenge(code)
            row["attempt_id"] = "new-attempt"
            self.store.save_challenge(row)
            return SubmissionReceipt(correct=True, awarded=10, correct_flag_count=1)
        self.platform.submit = delayed_submit
        await self.sup.submit("A", "late", "r1", "pro-1", self.attempt)
        operation = self.store.operations("submit")[0]
        self.assertEqual(operation["status"], "received")
        self.assertEqual(self.store.challenge("A")["challenge"]["correct_flag_count"], 0)
        self.assertFalse(any(kind == "submission_result" for kind, _ in self.team.requests))

    async def test_saved_models_cannot_be_changed_on_resume(self):
        config = replace(self.config, veria_models={**self.config.veria_models, "pro-1": "changed"})
        with self.assertRaisesRegex(ValueError, "veria_models"):
            Supervisor(self.platform, self.store, config, strategy=self.strategy, team_factory=FakeTeam)

    async def test_same_attempt_recovery_preserves_environment_deadline_and_receipt_context(self):
        self.platform.receipts["saved"] = SubmissionReceipt(correct=True, correct_flag_count=1)
        await self.sup.submit("A", "saved", "r1", "pro-1", self.attempt)
        deadline = self.sup.deadline
        await self.sup.recover_runtime("A")
        self.assertTrue(self.team.preserve_environment)
        resumed = self.sup.teams["A"]
        self.assertEqual(resumed.attempt_id, self.attempt)
        self.assertEqual(resumed.context["deadline"], deadline)
        recovered = resumed.context["recovery"]["submissions"]
        self.assertEqual(len(recovered), 1)
        self.assertTrue(recovered[0]["receipt"]["correct"])
        self.assertEqual(recovered[0]["request_id"], "r1")

    async def test_new_attempt_has_distinct_submission_identity_and_final_stop_destroys_environment(self):
        self.platform.receipts["same-candidate"] = SubmissionReceipt(correct=False)
        await self.sup.submit("A", "same-candidate", "r1", "pro-1", self.attempt)
        old = self.store.operations("submit")[0]
        await self.sup.finish("A", "pause")
        self.assertFalse(self.team.preserve_environment)
        await self.sup.start_challenge("A", "new-selection")
        current = self.store.challenge("A")["attempt_id"]
        self.assertNotEqual(current, self.attempt)
        await self.sup.submit("A", "same-candidate", "r2", "pro-1", current)
        operations = self.store.operations("submit")
        self.assertEqual(len(operations), 2)
        self.assertEqual({op["attempt_id"] for op in operations}, {self.attempt, current})
        self.assertEqual(len({op["id"] for op in operations}), 2)
        before = len(self.platform.submit_calls)
        await self.sup.submit("A", "late-old", "r3", "pro-1", self.attempt)
        self.assertEqual(len(self.platform.submit_calls), before)

    async def test_environment_health_failure_requests_technical_recovery(self):
        async def health(payload):
            return {"healthy": False, "needs_restart": True,
                    "environment": {"healthy": False, "reason": "container_missing"}}
        self.team.handlers["health"] = health
        result = await self.sup._audit_health("A", self.team)
        self.assertFalse(result["healthy"])
        self.assertIn("recover:A", self.sup.jobs)
        await self.settle()
        self.assertTrue(self.team.preserve_environment)

    async def test_failed_review_request_keeps_original_id_and_does_not_skip_review_window(self):
        row = self.store.challenge("A")
        row.update(active_seconds=2000, last_review_work=10)
        self.store.save_challenge(row)
        async def unavailable(payload):
            raise TimeoutError("fake lost response")
        self.team.handlers["review"] = unavailable
        await self.sup.request_review("A")
        saved = self.store.challenge("A")
        self.assertEqual(saved["last_review_work"], 10)
        request = saved["pending_review_request"]
        self.assertTrue(request["review_id"])
        self.clock.now += self.config.poll_interval
        async def accepted(payload):
            self.assertEqual(payload["review_id"], request["review_id"])
            return {"already_running": True, "review_id": payload["review_id"]}
        self.team.handlers["review"] = accepted
        await self.sup.request_review("A")
        row = self.store.challenge("A")
        self.assertEqual(row["last_review_work"], 2000)
        self.assertIsNone(row["pending_review_request"])
        self.assertEqual(row["review_id"], request["review_id"])

    async def test_wrong_python_version_fails_without_starting_runtime(self):
        if sys.version_info[:2] == (3, 14):
            self.skipTest("host uses 3.14; wrong-version test requires another host interpreter")
        team = TeamProcess(Path(self.temp.name), "version-check", "attempt", backend="claude",
                           settings=self.config.to_dict())
        with self.assertRaisesRegex(RuntimeError, "Python 3.14"):
            await team.start({})
        self.assertIsNone(team.process)
        self.assertFalse(team.directory.exists())


class VeriaCrossLayerTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_host_runtime_room_protocol_with_only_external_dependencies_faked(self):
        from test_veria_runtime import FakeDriver, FakeEnvironment
        from xinghuo.veria_runtime import VeriaRuntime

        class DirectTeam:
            def __init__(self, run_dir, code, attempt_id, backend, model, settings):
                self.code, self.attempt_id, self.pid = code, attempt_id, 12345
                self.alive = False
                self.runtime = VeriaRuntime(run_dir, code, attempt_id, backend, model,
                    {**settings, "veria_report_interval": .01},
                    environment_factory=FakeEnvironment, driver_factory=FakeDriver)
                async def emit(kind, **values):
                    supervisor.handle_team_event(code, {"type": kind, "code": code,
                                                        "attempt_id": attempt_id, **values})
                self.runtime.emit = emit

            async def start(self, context):
                self.alive = True
                return await self.runtime.start(context)

            async def request(self, command, payload, timeout=1):
                result = await asyncio.wait_for(self.runtime.command(command, payload), timeout)
                if command == "request_report":
                    self.runtime.room.record_evidence("pro-2", "tool_result", "post-cutoff observation")
                return result

            def poll_events(self):
                return []

            async def stop(self, *, preserve_environment=False):
                if self.alive:
                    await self.runtime.command("stop", {"preserve_environment": preserve_environment})
                    await self.runtime.shutdown()
                    self.alive = False

        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            platform, strategy = FakePlatform(), FakeStrategy()
            supervisor = Supervisor(platform, store,
                veria_config(operation_timeout=.5, shutdown_timeout=.1, audit_report_timeout=1),
                strategy=strategy, team_factory=DirectTeam, clock=Clock())
            try:
                await supervisor.initialize()
                await supervisor.start_challenge("A", "cross-layer")
                runtime = supervisor.teams["A"].runtime
                runtime.room.record_evidence("pro-1", "tool_result", "local synthetic observation")
                await supervisor.audit_one("A", 1)
                self.assertEqual(len(strategy.calls), 1, store.reviews(store.challenge("A")["attempt_id"]))
                current = strategy.calls[0]["current"]
                self.assertEqual(current["report_mode"], "reporter_snapshot")
                self.assertGreaterEqual(current["through_event"], current["required_through_event"])
                self.assertTrue(current["reporter_report_request"]["ready"])
                platform.receipts["FLAG{offline-cross-layer}"] = SubmissionReceipt(
                    correct=True, awarded=10, correct_flag_count=1, total_flag_count=3)
                result = await runtime.tool(runtime.slots["pro-1"], "submit_flag",
                                            {"flag": "FLAG{offline-cross-layer}"})
                self.assertEqual(result["status"], "correct")
                self.assertEqual(result["receipt"]["awarded"], 10)
                self.assertEqual(platform.submit_calls, [("A", "FLAG{offline-cross-layer}")])
                self.assertFalse(runtime._completed())
                original_environment = runtime.environment
                await supervisor.recover_runtime("A")
                resumed = supervisor.teams["A"].runtime
                self.assertFalse(original_environment.closed)
                self.assertEqual(resumed.room.candidate(result["request_id"])["status"], "correct")
                self.assertEqual(resumed.room.meta("context")["deadline"], supervisor.deadline)
            finally:
                await supervisor.shutdown()
                store.close()


class VeriaStopBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def fake_stop(self, *, engine="veria", preserve=False, timeout_count=0):
        class Process:
            returncode = None
            pid = 123456
            async def wait(self):
                self.returncode = 0
                return 0
        waits, signals = [], []
        async def wait_for(awaitable, timeout):
            waits.append(timeout)
            if len(waits) <= timeout_count:
                awaitable.close()
                raise TimeoutError("synthetic cleanup still running")
            return await awaitable
        with tempfile.TemporaryDirectory() as temp:
            team = TeamProcess(Path(temp), "budget", "attempt", settings={
                "team_engine": engine, "shutdown_timeout": 20})
            team.process = Process()
            team.request = AsyncMock(return_value={})
            with patch("xinghuo.runtime.asyncio.wait_for", new=wait_for), \
                 patch("xinghuo.runtime.os.killpg", side_effect=lambda pid, sig: signals.append((pid, sig))):
                await team.stop(preserve_environment=preserve)
            saved = None
            if engine == "veria":
                saved = json.loads((team.directory / "runtime-cleanup.json").read_text())
                self.assertEqual(saved, team.cleanup_result)
            return waits, signals, saved, team.request

    async def test_grace_covers_parallel_roles_then_serial_container_cleanup(self):
        waits, signals, saved, request = await self.fake_stop()
        self.assertEqual(veria_environment_cleanup_timeout(), 55)
        self.assertEqual(veria_stop_grace(20), 80)
        self.assertEqual(waits, [80])
        self.assertEqual(signals, [])
        self.assertEqual(saved["cleanup_status"], "runtime_exited")
        self.assertFalse(saved["needs_environment_takeover"])
        self.assertNotIn("cleanup_complete", saved)
        self.assertEqual(request.call_args.kwargs["timeout"], 5)

    async def test_recovery_waits_for_roles_and_member_cleanup_then_retains_environment(self):
        waits, signals, saved, _ = await self.fake_stop(preserve=True)
        self.assertEqual(veria_environment_cleanup_timeout(preserve_environment=True), 40)
        self.assertEqual(veria_stop_grace(20, preserve_environment=True), 65)
        self.assertEqual(waits, [65])
        self.assertEqual(saved["cleanup_status"], "environment_retained")
        self.assertTrue(saved["needs_environment_takeover"])

    async def test_escalation_records_incomplete_cleanup_even_after_process_exits(self):
        waits, signals, saved, _ = await self.fake_stop(timeout_count=2)
        self.assertEqual(waits, [80, 3, 3])
        self.assertEqual(len(signals), 2)
        self.assertTrue(saved["process_exit_confirmed"])
        self.assertEqual(saved["cleanup_status"], "incomplete")
        self.assertTrue(saved["needs_environment_takeover"])
        self.assertEqual(saved["attempt_id"], "attempt")

    async def test_native_keeps_original_waits_and_supervisor_budget_contains_all_phases(self):
        waits, signals, saved, request = await self.fake_stop(engine="native", timeout_count=1)
        self.assertEqual(waits, [4, 3])
        self.assertIsNone(saved)
        self.assertEqual(request.call_args.kwargs["timeout"], 12)
        self.assertGreaterEqual(veria_supervisor_cleanup_timeout(20, 30),
                                20 + 5 + veria_stop_grace(20) + 6 + 60)


if __name__ == "__main__":
    unittest.main()
