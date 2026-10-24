"""Lifecycle acceptance tests with real Store/Supervisor and no model or network."""
import asyncio
import hashlib
import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from xinghuo.config import Config
from xinghuo.core import Challenge, SubmissionReceipt
from xinghuo.store import Store
from xinghuo.strategy import ReviewDecision
from xinghuo.supervisor import Supervisor


class Clock:
    def __init__(self, now=1000.):
        self.now = now

    def __call__(self):
        return self.now


class FakePlatform:
    def __init__(self):
        self.rows = {code: Challenge(code, description=f"题目{code}", flag_count=3)
                     for code in "ABCDE"}
        self.start_calls = []
        self.close_calls = []
        self.submit_calls = []
        self.pending_starts = set()
        self.unknown_closes = set()
        self.receipts = {}
        self.peak = 0

    def update(self, code, **changes):
        self.rows[code] = replace(self.rows[code], **changes)
        self.peak = max(self.peak, sum(row.container_status != "stopped"
                                      for row in self.rows.values()))

    async def list_challenges(self):
        return list(self.rows.values())

    async def start(self, code):
        self.start_calls.append(code)
        if code in self.pending_starts:
            self.update(code, container_status="pending")
            raise TimeoutError("start response lost while platform is starting")
        self.update(code, container_status="available", container_addr=(f"{code}.lab:80",))
        return self.rows[code].container_addr

    async def submit(self, code, flag):
        self.submit_calls.append((code, flag))
        receipt = self.receipts[flag]
        if isinstance(receipt, BaseException):
            raise receipt
        if receipt.correct_flag_count is not None:
            count = max(self.rows[code].correct_flag_count, receipt.correct_flag_count)
            self.update(code, correct_flag_count=count,
                        completed=count >= self.rows[code].flag_count)
        return receipt

    async def close(self, code):
        self.close_calls.append(code)
        if code in self.unknown_closes:
            self.update(code, container_status="stop_pending", container_addr=())
            raise TimeoutError("close response lost")
        self.update(code, container_status="stopped", container_addr=())


class FakeTeam:
    def __init__(self, run_dir, code, attempt_id, **kwargs):
        self.code, self.attempt_id = code, attempt_id
        self.alive = False
        self.pid = 12345
        self.requests = []
        self.handlers = {}
        self.events = []
        self.stop_count = 0

    async def start(self, context):
        self.context = context
        self.alive = True

    async def request(self, kind, payload, **kwargs):
        self.requests.append((kind, payload))
        if kind in self.handlers:
            return await self.handlers[kind](payload)
        if kind == "request_report":
            return {"request_id": payload["request_id"], "round_id": payload["round_id"], "ready": True}
        if kind == "health":
            return {"healthy": True, "responsive": True, "core_failures": [], "agents": []}
        if kind == "audit_snapshot":
            return {"report": {"summary": "当前下一步仍待验证", "evidence_refs": ["route.json"]},
                    "through_event": 5, "last_event": 5, "report_version": 1, "windows": []}
        return {"ok": True}

    def poll_events(self):
        result, self.events = self.events, []
        return result

    async def stop(self):
        self.stop_count += 1
        self.alive = False


class FakeStrategy:
    def __init__(self):
        self.calls = []
        self.on_review = None

    async def decide(self, packet):
        self.calls.append(packet)
        if packet.get("mode") == "select":
            return ReviewDecision("switch", "测试推荐首个可选题",
                                  replacement=packet["candidates"][0]["code"])
        if self.on_review:
            await self.on_review(packet)
        return ReviewDecision("switch", "最近未跨过关键障碍且下一窗口缺乏依据", replacement="D")


class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        self.clock = Clock()
        self.platform = FakePlatform()
        self.strategy = FakeStrategy()
        self.config = Config(backend="demo", epsilon=0, budget=100,
                             operation_timeout=.2, shutdown_timeout=.2)
        self.supervisors = []
        self.sup = self.make_supervisor()
        await self.sup.initialize()

    async def asyncTearDown(self):
        for supervisor in reversed(self.supervisors):
            await supervisor.shutdown()
        self.store.close()
        self.temp.cleanup()

    def make_supervisor(self, config=None):
        supervisor = Supervisor(self.platform, self.store, config or self.config,
                                strategy=self.strategy, team_factory=FakeTeam, clock=self.clock)
        self.supervisors.append(supervisor)
        return supervisor

    async def start(self, *codes):
        for code in codes:
            await self.sup.start_challenge(code, "test-selection:" + code)

    async def settle_jobs(self):
        if self.sup.jobs:
            await asyncio.gather(*list(self.sup.jobs.values()))
            self.sup._reap()

    def events(self, kind):
        return [json.loads(row[0]) for row in self.store.db.execute(
            "SELECT body FROM events WHERE kind=?", (kind,))]

    async def test_confirmed_score_gain_skips_model_and_keeps_scene(self):
        await self.start("A")
        self.platform.receipts["first"] = SubmissionReceipt(
            correct=True, awarded=10, cumulative_score=10,
            correct_flag_count=1, total_flag_count=3, matched_flag_index=0)
        await self.sup.submit("A", "first")
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.strategy.calls, [])
        row = self.store.challenge("A")
        self.assertEqual(row["confirmation"]["decision"]["action"], "continue")
        self.assertEqual(row["confirmation"]["streak"], 0)
        self.assertEqual(row["status"], "active")
        self.assertNotIn("A", self.platform.close_calls)
        self.assertEqual([kind for kind, _ in self.sup.teams["A"].requests][:3],
                         ["submission_result", "request_report", "health"])

    async def test_audit_requests_leader_report_then_checks_health_before_strategy(self):
        await self.start("A")

        async def inspect(packet):
            self.assertEqual([kind for kind, _ in self.sup.teams["A"].requests],
                             ["request_report", "health", "audit_snapshot"])
            self.assertTrue(packet["current"]["leader_report_request"]["ready"])
            self.assertTrue(packet["current"]["harness_health"]["healthy"])

        self.strategy.on_review = inspect
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 1)
        kinds = [row[0] for row in self.store.db.execute("SELECT kind FROM events ORDER BY seq")]
        self.assertLess(kinds.index("audit_report_ready"), kinds.index("audit_health_checked"))
        self.assertLess(kinds.index("audit_health_checked"), kinds.index("audit_decision"))

    async def test_leader_report_timeout_still_checks_health_and_breaks_switch_confirmation(self):
        await self.start("A")
        await self.sup.audit_one("A", 1)
        self.sup.config.audit_report_timeout = .03

        async def pending(payload):
            return {"request_id": payload["request_id"], "ready": False}

        team = self.sup.teams["A"]
        team.handlers["request_report"] = pending
        team.handlers["report_status"] = pending
        await self.sup.audit_one("A", 2)
        confirmation = self.store.challenge("A")["confirmation"]
        self.assertEqual(confirmation["decision"]["action"], "need_info")
        self.assertEqual(confirmation["streak"], 0)
        self.assertEqual(len(self.strategy.calls), 1)
        self.assertEqual(len(self.events("audit_health_checked")), 2)
        self.assertEqual(self.platform.close_calls, [])

    async def test_failed_core_role_is_a_health_problem_not_a_switch_recommendation(self):
        await self.start("A")
        await self.sup.audit_one("A", 1)

        async def unhealthy(payload):
            return {"healthy": False, "responsive": True, "core_failures": ["reporter"],
                    "agents": [{"agent_id": "reporter", "status": "failed"}]}

        self.sup.teams["A"].handlers["health"] = unhealthy
        await self.sup.audit_one("A", 2)
        row = self.store.challenge("A")
        self.assertFalse(row["last_audit_health"]["healthy"])
        self.assertEqual(row["confirmation"]["decision"]["action"], "need_info")
        self.assertEqual(row["confirmation"]["streak"], 0)
        self.assertEqual(len(self.strategy.calls), 1)
        self.assertEqual(self.platform.close_calls, [])

    async def test_dead_runtime_is_recovered_after_report_request_without_closing_target(self):
        await self.start("A", "B", "C")
        attempt = self.store.challenge("A")["attempt_id"]
        self.sup.teams["A"].alive = False
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.strategy.calls, [])
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 0)
        await self.settle_jobs()
        self.assertTrue(self.sup.teams["A"].alive)
        self.assertEqual(self.store.challenge("A")["attempt_id"], attempt)
        self.assertTrue(self.sup.teams["B"].alive and self.sup.teams["C"].alive)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.sup.reserve_count(), 3)

    async def test_slow_leader_does_not_block_another_challenges_audit(self):
        await self.start("A", "B")
        requested, release = asyncio.Event(), asyncio.Event()

        async def slow(payload):
            requested.set()
            await release.wait()
            return {"request_id": payload["request_id"], "ready": True}

        async def inspect(packet):
            if packet["challenge"]["code"] == "B":
                await asyncio.wait_for(requested.wait(), 1)
                self.assertFalse(release.is_set())
                release.set()

        self.sup.teams["A"].handlers["request_report"] = slow
        self.strategy.on_review = inspect
        await asyncio.wait_for(self.sup.audit_round(1), 2)
        self.assertTrue(release.is_set())
        self.assertEqual({p["challenge"]["code"] for p in self.strategy.calls}, {"A", "B"})

    async def test_two_regular_rounds_required_before_switching(self):
        await self.start("A", "B", "C")
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 1)
        self.assertEqual(self.platform.close_calls, [])
        self.assertTrue(self.sup.teams["B"].alive and self.sup.teams["C"].alive)
        await self.sup.audit_one("A", 2)
        self.assertEqual(self.store.challenge("A")["status"], "paused")
        self.assertTrue(self.store.challenge("A")["close_confirmed"])
        self.assertEqual(self.store.challenge("D")["status"], "active")
        self.assertEqual(self.platform.close_calls, ["A"])
        self.assertLessEqual(self.platform.peak, 3)

    async def test_score_arriving_during_model_audit_revokes_switch(self):
        await self.start("A", "B", "C")
        await self.sup.audit_one("A", 1)
        self.platform.receipts["breakthrough"] = SubmissionReceipt(
            correct=True, awarded=10, cumulative_score=10,
            correct_flag_count=1, total_flag_count=3)

        async def new_score(packet):
            await self.sup.submit("A", "breakthrough")

        self.strategy.on_review = new_score
        await self.sup.audit_one("A", 2)
        row = self.store.challenge("A")
        self.assertEqual(row["confirmation"]["decision"]["action"], "continue")
        self.assertEqual(row["confirmation"]["streak"], 0)
        self.assertEqual(self.platform.close_calls, [])
        self.assertNotIn("D", self.platform.start_calls)

    async def test_new_flag_without_known_score_during_audit_invalidates_old_streak(self):
        await self.start("A", "B", "C")
        await self.sup.audit_one("A", 1)
        self.platform.receipts["flag-no-score"] = SubmissionReceipt(
            correct=True, correct_flag_count=1, total_flag_count=3)

        async def new_flag(packet):
            await self.sup.submit("A", "flag-no-score")

        self.strategy.on_review = new_flag
        await self.sup.audit_one("A", 2)
        row = self.store.challenge("A")
        self.assertEqual(row["progress_revision"], 1)
        self.assertEqual(row["score_revision"], 0)
        self.assertFalse(row["confirmation"]["execute"])
        self.assertEqual(self.platform.close_calls, [])

    async def test_unknown_close_reserves_slot_while_other_teams_continue(self):
        await self.start("A", "B", "C")
        self.platform.unknown_closes.add("A")
        await self.sup.audit_one("A", 1)
        await self.sup.audit_one("A", 2)
        self.assertEqual(self.store.challenge("A")["status"], "closing")
        self.assertFalse(self.store.challenge("A")["close_confirmed"])
        self.assertEqual(self.sup.reserve_count(), 3)
        await self.sup.fill_one()
        self.assertNotIn("D", self.platform.start_calls)
        self.assertTrue(self.sup.teams["B"].alive and self.sup.teams["C"].alive)
        self.platform.update("A", container_status="stopped")
        await self.sup.reconcile()
        await self.sup.fill_one()
        self.assertTrue(self.store.challenge("A")["close_confirmed"])
        self.assertEqual(self.platform.start_calls.count("D"), 1)
        self.assertLessEqual(self.platform.peak, 3)

    async def test_selection_is_persisted_before_start_and_not_redrawn_on_recovery(self):
        await self.start("A", "B", "C")
        self.sup.config.epsilon = 1
        selected = await self.sup.choose("replace:persistent", recommended="D", excluding="A")
        self.assertEqual(selected, "E")
        saved_rng = self.store.get_meta("rng_state")
        recovered = self.make_supervisor()
        other = await recovered.choose("replace:persistent", recommended="D", excluding="A")
        self.assertEqual(other, selected)
        self.assertEqual(self.store.get_meta("rng_state"), saved_rng)
        self.assertEqual(self.store.operation("replace:persistent")["choice"]["selected"], "E")
        self.assertEqual(self.strategy.calls, [])

    async def test_duplicate_submission_never_resets_stagnation_or_adds_score(self):
        await self.start("A")
        row = self.store.challenge("A")
        row.update(active_seconds=2000, last_flag_work=100, last_review_work=500)
        self.store.save_challenge(row)
        self.platform.receipts["already-known"] = SubmissionReceipt(correct=True, duplicate=True)
        await self.sup.submit("A", "already-known")
        await self.sup.submit("A", "already-known")
        changed = self.store.challenge("A")
        self.assertEqual(changed["last_flag_work"], 100)
        self.assertEqual(changed["last_review_work"], 500)
        self.assertEqual(changed["active_seconds"], 2000)
        self.assertEqual(changed["progress_revision"], 0)
        self.assertEqual(changed["score_revision"], 0)
        self.assertIsNone(changed["score"])
        self.assertEqual(self.platform.submit_calls, [("A", "already-known")])

    async def test_received_receipt_is_replayed_without_resubmitting_or_double_counting(self):
        await self.start("A")
        flag = "receipt-journaled-before-derived-state"
        operation_id = "submit:" + hashlib.sha256(("A\0" + flag).encode()).hexdigest()
        receipt = SubmissionReceipt(correct=True, awarded=25, cumulative_score=25,
                                    correct_flag_count=1, total_flag_count=3,
                                    matched_flag_index=0)
        self.store.save_operation(operation_id, "submit", "A", "received", flag=flag,
                                  attempts=1, receipt=receipt.to_dict())
        await self.sup.submit("A", flag)
        applied = self.store.challenge("A")
        self.assertEqual(self.platform.submit_calls, [])
        self.assertEqual(applied["score"], 25)
        self.assertEqual(applied["score_revision"], 1)
        self.assertEqual(applied["progress_revision"], 1)
        self.assertEqual(applied["challenge"]["correct_flag_count"], 1)
        self.assertEqual(applied["applied_receipts"].count(operation_id), 1)
        self.assertEqual(self.store.operation(operation_id)["status"], "confirmed")
        # Also cover a crash after derived state was committed but before the
        # journal's received -> confirmed transition was saved.
        self.store.save_operation(operation_id, "submit", "A", "received", flag=flag,
                                  attempts=1, receipt=receipt.to_dict())
        await self.sup.submit("A", flag)
        await self.sup.submit("A", flag)
        replayed = self.store.challenge("A")
        self.assertEqual(self.platform.submit_calls, [])
        self.assertEqual(replayed["score_revision"], 1)
        self.assertEqual(replayed["progress_revision"], 1)
        self.assertEqual(replayed["applied_receipts"].count(operation_id), 1)
        self.assertEqual(len(self.events("score_gained")), 1)

    async def test_audit_history_contains_decision_summaries_not_recursive_packets(self):
        await self.start("A")

        class ContinuingStrategy:
            def __init__(self):
                self.packets = []

            async def decide(self, packet):
                self.packets.append(packet)
                return ReviewDecision("continue", "下一实验有可验证依据",
                                      progress="关键步骤已复现", next_breakthrough="验证最后前提")

        strategy = ContinuingStrategy()
        self.sup.strategy = strategy
        for round_id in range(1, 13):
            await self.sup.audit_one("A", round_id)
        self.assertEqual(len(strategy.packets), 12)
        attempt = self.store.challenge("A")["attempt_id"]
        saved = self.store.reviews(attempt, limit=12)
        self.assertTrue(all("packet" in review for review in saved))
        last = strategy.packets[-1]
        self.assertEqual([review["round_id"] for review in last["history"]], [9, 10, 11])
        self.assertTrue(all(set(review) == {"round_id", "decision", "streak"}
                            for review in last["history"]))
        self.assertNotIn('"packet"', json.dumps(last["history"]))
        sizes = [len(json.dumps(packet, ensure_ascii=False)) for packet in strategy.packets[4:]]
        self.assertLess(max(sizes) - min(sizes), 200)
        self.assertLess(max(sizes), 10_000)

    async def test_report_behind_evidence_watermark_is_unknown_not_a_stop_confirmation(self):
        await self.start("A")

        async def stale_report(payload):
            return {"report": {"summary": "旧报告"}, "report_version": 1,
                    "through_event": 4, "last_event": 8, "evidence_watermark": 7}

        self.sup.teams["A"].handlers["audit_snapshot"] = stale_report
        await self.sup.audit_one("A", 1)
        row = self.store.challenge("A")
        self.assertEqual(self.strategy.calls, [])
        self.assertEqual(row["confirmation"]["decision"]["action"], "need_info")
        self.assertEqual(row["confirmation"]["streak"], 0)
        self.assertEqual(self.platform.close_calls, [])

    async def test_restart_keeps_absolute_deadline_even_if_new_budget_is_larger(self):
        original_deadline = self.sup.deadline
        await self.sup.shutdown()
        self.clock.now += 40
        recovered = self.make_supervisor(replace(self.config, budget=1000))
        await recovered.initialize()
        self.assertEqual(recovered.deadline, original_deadline)
        self.assertEqual(recovered.status()["remaining_seconds"], 60)
        await recovered.shutdown()
        self.clock.now = original_deadline + 1
        expired = self.make_supervisor(replace(self.config, budget=1000))
        result = await expired.run()
        self.assertEqual(result["deadline"], original_deadline)
        self.assertEqual(result["status"], "deadline")
        self.assertEqual(self.platform.start_calls, [])

    async def test_pending_start_uses_slot_and_available_reconciliation_attaches_runtime(self):
        await self.start("A", "B")
        self.platform.pending_starts.add("C")
        await self.start("C")
        self.assertEqual(self.store.challenge("C")["status"], "unknown")
        self.assertEqual(self.sup.reserve_count(), 3)
        await self.sup.reconcile()
        await self.sup.fill_one()
        self.assertNotIn("D", self.platform.start_calls)
        self.platform.update("C", container_status="available", container_addr=("C.lab:80",))
        await self.sup.reconcile()
        await self.settle_jobs()
        self.assertEqual(self.store.challenge("C")["status"], "active")
        self.assertIn("C", self.sup.teams)
        self.assertEqual(self.platform.start_calls.count("C"), 1)
        self.assertLessEqual(self.platform.peak, 3)

    async def test_significant_evidence_arriving_during_audit_rejects_old_stop(self):
        await self.start("A", "B", "C")
        await self.sup.audit_one("A", 1)

        async def evidence_change(packet):
            row = self.store.challenge("A")
            row["revision"] += 1
            self.store.save_challenge(row)

        self.strategy.on_review = evidence_change
        await self.sup.audit_one("A", 2)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertTrue(self.events("stop_decision_stale"))

    async def test_unconfirmed_submission_discovered_during_checkpoint_prevents_close(self):
        await self.start("A", "B", "C")
        self.platform.receipts["last-candidate"] = TimeoutError("submission response lost")

        async def checkpoint(payload):
            await self.sup.submit("A", "last-candidate")
            return {"checkpoint": "last.json"}

        self.sup.teams["A"].handlers["prepare_stop"] = checkpoint
        await self.sup.audit_one("A", 1)
        await self.sup.audit_one("A", 2)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertNotIn("D", self.platform.start_calls)

    async def test_reconciliation_does_not_bypass_exhausted_runtime_recovery_limit(self):
        await self.start("A")
        self.sup.config.max_retries = 0
        await self.sup.recover_runtime("A")
        self.assertNotIn("A", self.sup.teams)
        self.assertTrue(self.events("runtime_recovery_exhausted"))
        await self.sup.reconcile()
        await self.settle_jobs()
        self.assertNotIn("A", self.sup.teams)

    async def test_saved_selection_cannot_retry_exhausted_start_forever(self):
        self.sup.config.max_retries = 0
        self.platform.pending_starts.add("A")
        selected = await self.sup.choose("fill:exhausted", recommended="A")
        await self.sup.start_challenge(selected, "fill:exhausted")
        self.platform.update("A", container_status="stopped")
        await self.sup.reconcile()
        self.assertNotIn("A", [row["code"] for row in self.sup.eligible()])
        await self.sup.fill_one()
        self.assertEqual(self.platform.start_calls.count("A"), 1)
        self.assertTrue(any(code != "A" for code in self.platform.start_calls))


    async def test_late_report_resumes_original_round_once_and_preserves_adjacent_confirmation(self):
        await self.start("A", "B", "C")
        await self.sup.audit_one("A", 1)
        team = self.sup.teams["A"]
        ready = False

        async def report(payload):
            return {"request_id": payload["request_id"], "round_id": 2, "ready": ready}

        team.handlers.update(request_report=report, report_status=report)
        self.sup.config.audit_report_timeout = .01
        await self.sup.audit_one("A", 2)
        attempt = self.store.challenge("A")["attempt_id"]
        self.assertEqual([review["round_id"] for review in self.store.reviews(attempt)], [1])
        operation_id = self.sup._audit_operation_id(attempt, 2)
        operation = self.store.operation(operation_id)
        self.assertEqual(operation["status"], "pending")
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 0)
        ready = True
        self.sup.handle_team_event("A", {"type": "report_published", "attempt_id": attempt})
        await self.settle_jobs()
        self.assertEqual(self.store.operation(operation_id)["status"], "applied")
        self.assertEqual(self.store.challenge("A")["status"], "paused")
        self.assertEqual(self.platform.close_calls, ["A"])
        self.assertEqual([p["mode"] for p in self.strategy.calls], ["review", "review", "select"])
        self.assertEqual([review["round_id"] for review in self.store.reviews(attempt)], [1, 2])
        await self.sup.resume_pending_audits()
        await self.sup.audit_one("A", 2)
        self.assertEqual(len(self.strategy.calls), 3)
        request_ids = {payload["request_id"] for kind, payload in team.requests
                       if kind == "request_report" and payload["round_id"] == 2}
        self.assertEqual(request_ids, {f"{attempt}:2"})

    async def test_pending_report_recovery_retains_original_revision_and_request_id(self):
        await self.start("A")
        team = self.sup.teams["A"]
        self.sup.config.audit_report_timeout = .01

        async def pending(payload):
            return {"request_id": payload["request_id"], "ready": False}

        team.handlers.update(request_report=pending, report_status=pending)
        await self.sup.audit_one("A", 1)
        row = self.store.challenge("A")
        operation_id = self.sup._audit_operation_id(row["attempt_id"], 1)
        captured = self.store.operation(operation_id)
        recovered = self.make_supervisor()
        recovered.teams = self.sup.teams
        recovered.last_heartbeat = self.sup.last_heartbeat
        team.handlers.clear()
        await recovered.resume_pending_audits()
        applied = self.store.operation(operation_id)
        self.assertEqual(applied["revision"], captured["revision"])
        self.assertEqual(applied["request_id"], captured["request_id"])
        self.assertEqual(applied["created_at"], captured["created_at"])
        self.assertEqual(applied["status"], "applied")
        calls = len(self.strategy.calls)
        await recovered.audit_one("A", 1)
        self.assertEqual(len(self.strategy.calls), calls)

    async def test_two_pending_cutoffs_are_consumed_in_round_order(self):
        await self.start("A", "B", "C")
        team = self.sup.teams["A"]
        ready = False

        async def report(payload):
            return {"request_id": payload["request_id"], "ready": ready}

        team.handlers.update(request_report=report, report_status=report)
        self.sup.config.audit_report_timeout = .01
        await self.sup.audit_one("A", 1)
        await self.sup.audit_one("A", 2)
        self.assertEqual(self.strategy.calls, [])
        ready = True
        await self.sup.resume_pending_audits()
        self.assertEqual(self.platform.close_calls, ["A"])
        attempt = self.store.challenge("A")["attempt_id"]
        self.assertEqual([review["round_id"] for review in self.store.reviews(attempt)], [1, 2])
        self.assertEqual([item["streak"] for item in self.events("audit_decision")], [1, 2])

    async def test_material_overflow_is_not_misclassified_as_report_gap(self):
        await self.start("A")

        async def huge_report(payload):
            return {"report": {"summary": "证" * 270000}, "report_version": 1,
                    "through_event": 5, "evidence_watermark": 5}

        self.sup.teams["A"].handlers["audit_snapshot"] = huge_report
        await self.sup.audit_one("A", 1)
        attempt = self.store.challenge("A")["attempt_id"]
        review = self.store.reviews(attempt)[0]
        self.assertEqual(review["packet"]["material_error_type"], "material_too_large")
        self.assertIn("material_too_large", review["decision"]["reason"])
        self.assertNotIn("报告尚未覆盖", review["decision"]["reason"])
        self.assertEqual(self.strategy.calls, [])
        self.assertEqual(self.events("audit_material_error")[0]["error_type"], "material_too_large")
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 0)

    async def test_report_plus_complete_delta_enters_real_audit_call(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        team = self.sup.teams["A"]

        async def status(payload):
            return {"request_id": payload["request_id"], "ready": True,
                "report_mode": "report_plus_delta", "required_through_event": 9,
                "report_version": 1, "through_event": 9, "coverage_complete": True}

        async def snapshot(payload):
            return {**await status(payload), "report": {"overview": "完整旧报告", "through_event": 5},
                "delta_events": [{"seq": 9, "data": {"stdout": "固定截点完整增量"}}]}

        team.handlers.update(request_report=status, audit_snapshot=snapshot)
        await self.sup.audit_one("A", 1)
        packet = self.strategy.calls[0]
        self.assertEqual(packet["report_mode"], "report_plus_delta")
        self.assertEqual(packet["current"]["report"]["through_event"], 5)
        self.assertEqual(packet["current"]["through_event"], 9)
        self.assertEqual(packet["current"]["delta_events"][0]["data"]["stdout"], "固定截点完整增量")
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 1)

    async def test_incomplete_delta_cannot_count_toward_switch(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        team = self.sup.teams["A"]

        async def status(payload):
            return {"request_id": payload["request_id"], "ready": True,
                "report_mode": "report_plus_delta", "required_through_event": 9,
                "report_version": 1, "through_event": 9}

        async def snapshot(payload):
            return {**await status(payload), "report": {"overview": "旧报告", "through_event": 5},
                    "coverage_complete": False, "delta_events": []}

        team.handlers.update(request_report=status, audit_snapshot=snapshot)
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.strategy.calls, [])
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 0)
        attempt = self.store.challenge("A")["attempt_id"]
        self.assertEqual(self.store.operation(self.sup._audit_operation_id(attempt, 1))["status"], "pending")

    async def test_reviewer_partial_evidence_required_before_early_switch_confirmation(self):
        await self.start("A", "B", "C")
        review = {"status": "running", "reproduction_checkpoint": {"event": 1},
                  "phase_evidence_complete": False, "investigation_evidence_refs": []}

        async def snapshot(payload):
            return {"report": {"summary": "无进展"}, "report_version": 1,
                    "through_event": 5, "last_event": 5, "active_review": review}

        self.sup.teams["A"].handlers["audit_snapshot"] = snapshot
        await self.sup.audit_one("A", 1)
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 0)
        review.update(phase_evidence_complete=True, investigation_evidence_refs=["post-checkpoint-execution.txt"])
        await self.sup.audit_one("A", 2)
        self.assertEqual(self.store.challenge("A")["confirmation"]["streak"], 1)
        await self.sup.audit_one("A", 3)
        self.assertEqual(self.platform.close_calls, ["A"])
        self.assertEqual(self.strategy.calls[-1]["mode"], "select")

    async def test_already_running_reviewer_does_not_reset_review_work_clock(self):
        await self.start("A")
        row = self.store.challenge("A")
        row.update(last_review_work=123, active_seconds=2000)
        self.store.save_challenge(row)

        async def review(payload):
            return {"already_running": True, "review_id": "original-review"}

        self.sup.teams["A"].handlers["review"] = review
        await self.sup.request_review("A")
        self.assertEqual(self.store.challenge("A")["last_review_work"], 123)
        self.assertEqual(self.store.challenge("A")["review_id"], "original-review")
        self.assertEqual(self.events("reviewer_requested"), [])
        self.assertEqual(self.events("reviewer_already_running")[0]["review_id"], "original-review")

    async def test_operator_recovery_queue_is_scoped_idempotent_and_keeps_deadline(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        row = self.store.challenge("A")
        deadline = self.sup.deadline
        request = {"request_id": "recover-test", "run_id": self.store.get_meta("run_id"),
                   "code": "A", "attempt_id": row["attempt_id"], "provider": "glm", "reason": "topup"}
        path = self.store.run_dir / "control" / "recovery-requests" / "recover-test.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(request))
        await self.sup.process_recovery_requests()
        self.assertFalse(path.exists())
        rpc = [payload for kind, payload in self.sup.teams["A"].requests if kind == "recover_members"]
        self.assertEqual(len(rpc), 1)
        self.assertEqual(rpc[0]["provider"], "glm")
        receipt_path = self.store.run_dir / "control" / "recovery-receipts" / path.name
        receipt = json.loads(receipt_path.read_text())
        self.assertEqual(receipt["status"], "confirmed")
        self.assertEqual(receipt["deadline"], deadline)
        path.write_text(json.dumps(request))
        await self.sup.process_recovery_requests()
        self.assertEqual(len([kind for kind, _ in self.sup.teams["A"].requests if kind == "recover_members"]), 1)
        invalid = {**request, "request_id": "wrong-attempt", "attempt_id": "obsolete"}
        bad_path = path.with_name("wrong-attempt.json")
        bad_path.write_text(json.dumps(invalid))
        await self.sup.process_recovery_requests()
        bad_receipt = json.loads(receipt_path.with_name(bad_path.name).read_text())
        self.assertEqual(bad_receipt["status"], "refused")
        self.assertEqual(self.sup.deadline, deadline)
        self.assertEqual(len([kind for kind, _ in self.sup.teams["A"].requests if kind == "recover_members"]), 1)


    async def test_oversized_delta_retries_reporter_only_and_applies_late_round_once(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        team = self.sup.teams["A"]
        full_report = False

        async def status(payload):
            if full_report:
                self.assertFalse(payload["allow_delta"])
            return {"request_id": payload["request_id"], "ready": True,
                "report_mode": "reporter_snapshot" if full_report else "report_plus_delta",
                "required_through_event": 9, "report_version": 2 if full_report else 1,
                "through_event": 9, "coverage_complete": True}

        async def snapshot(payload):
            return {**await status(payload), "report": {"overview": "完整报告"},
                **({} if full_report else {"delta_evidence_refs": [{"path": "large.txt", "content": "证" * 270000}]})}

        team.handlers.update(request_report=status, audit_snapshot=snapshot)
        await self.sup.audit_one("A", 1)
        attempt = self.store.challenge("A")["attempt_id"]
        operation_id = self.sup._audit_operation_id(attempt, 1)
        operation = self.store.operation(operation_id)
        self.assertEqual(operation["status"], "pending")
        self.assertTrue(operation["prefer_reporter"])
        self.assertEqual(operation["last_material_error"]["error_type"], "material_too_large")
        self.assertEqual(self.strategy.calls, [])
        self.assertEqual(self.store.reviews(attempt), [])
        full_report = True
        await self.sup.resume_pending_audits()
        self.assertEqual(self.store.operation(operation_id)["status"], "applied")
        self.assertEqual(len(self.strategy.calls), 1)
        self.assertEqual(self.strategy.calls[0]["report_mode"], "reporter_snapshot")
        await self.sup.resume_pending_audits()
        self.assertEqual(len(self.strategy.calls), 1)

    async def test_preallocation_delta_guard_is_material_error_not_report_missing(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        self.sup.config.audit_report_timeout = .01
        team = self.sup.teams["A"]

        async def too_large(payload):
            return {"request_id": payload["request_id"], "ready": False,
                    "report_mode": "report_plus_delta", "required_through_event": 9,
                    "report_version": 1, "through_event": 9,
                    "coverage_complete": False, "delta_error": {"error_type": "material_too_large", "bytes": 999999}}

        team.handlers.update(request_report=too_large, report_status=too_large)
        await self.sup.audit_one("A", 1)
        attempt = self.store.challenge("A")["attempt_id"]
        operation = self.store.operation(self.sup._audit_operation_id(attempt, 1))
        self.assertTrue(operation["prefer_reporter"])
        self.assertEqual(operation["status"], "pending")
        self.assertEqual(self.events("audit_material_error")[0]["error_type"], "material_too_large")
        self.assertEqual(self.events("audit_deferred")[0]["reason"], "material_too_large")
        self.assertEqual(self.strategy.calls, [])

    async def test_lost_review_ack_preserves_original_work_position_not_retry_position(self):
        await self.start("A")
        row = self.store.challenge("A")
        row.update(active_seconds=2000, last_review_work=10)
        self.store.save_challenge(row)

        async def lost(payload):
            raise TimeoutError("first request accepted but response was lost")

        self.sup.teams["A"].handlers["review"] = lost
        await self.sup.request_review("A")
        row = self.store.challenge("A")
        original_id = row["pending_review_request"]["review_id"]
        row["active_seconds"] = 2500
        self.store.save_challenge(row)
        self.clock.now += self.config.poll_interval

        async def acknowledged(payload):
            return {"already_running": True, "review_id": original_id}

        self.sup.teams["A"].handlers["review"] = acknowledged
        await self.sup.request_review("A")
        self.assertEqual(self.store.challenge("A")["last_review_work"], 2000)
        self.assertEqual(len(self.events("reviewer_requested")), 1)


    def recovery_request(self, request_id):
        row = self.store.challenge("A")
        request = {"request_id": request_id, "run_id": self.store.get_meta("run_id"),
                   "code": "A", "attempt_id": row["attempt_id"], "provider": "glm", "reason": "topup"}
        path = self.store.run_dir / "control" / "recovery-requests" / (request_id + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(request))
        return path

    async def test_lost_operator_recovery_response_retries_original_id_without_rearming_twice(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        path = self.recovery_request("lost-recovery")
        applied, calls = {}, []

        async def recover(payload):
            calls.append(payload["request_id"])
            if payload["request_id"] not in applied:
                applied[payload["request_id"]] = {"recovered": ["solver-3"], "refused": {}, "deadline": self.sup.deadline}
                raise TimeoutError("runtime applied and cached receipt before reply was lost")
            return applied[payload["request_id"]]

        self.sup.teams["A"].handlers["recover_members"] = recover
        await self.sup.process_recovery_requests()
        operation_id = "operator-recovery:" + path.stem
        operation = self.store.operation(operation_id)
        self.assertEqual(operation["status"], "unknown")
        self.assertEqual(operation["attempts"], 1)
        self.assertTrue(path.exists())
        self.assertEqual(operation["receipt"]["status"], "unknown")
        await self.sup.process_recovery_requests()
        self.assertEqual(len(calls), 1)
        self.clock.now += self.config.poll_interval
        await self.sup.process_recovery_requests()
        self.assertEqual(calls, ["lost-recovery", "lost-recovery"])
        self.assertEqual(len(applied), 1)
        self.assertFalse(path.exists())
        operation = self.store.operation(operation_id)
        self.assertEqual(operation["status"], "confirmed")
        self.assertEqual(operation["attempts"], 2)
        self.assertEqual(operation["receipt"]["result"]["recovered"], ["solver-3"])

    async def test_repeated_unknown_recovery_is_parked_after_three_attempts(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        path = self.recovery_request("never-confirmed")
        calls = []

        async def unavailable(payload):
            calls.append(payload["request_id"])
            raise TimeoutError("reply lost")

        self.sup.teams["A"].handlers["recover_members"] = unavailable
        for index in range(3):
            await self.sup.process_recovery_requests()
            self.clock.now += self.config.poll_interval * (2 ** index)
        self.assertEqual(calls, ["never-confirmed"] * 3)
        self.assertFalse(path.exists())
        parked = self.store.run_dir / "control" / "recovery-parked" / path.name
        self.assertTrue(parked.exists())
        operation = self.store.operation("operator-recovery:" + path.stem)
        self.assertEqual(operation["status"], "needs_attention")
        self.assertEqual(operation["receipt"]["status"], "unknown")
        self.assertTrue(operation["receipt"]["needs_attention"])
        await self.sup.process_recovery_requests()
        self.assertEqual(len(calls), 3)

    async def test_crash_during_third_recovery_rpc_cannot_send_fourth_attempt(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        path = self.recovery_request("crash-third-attempt")
        request = json.loads(path.read_text())
        self.store.save_operation("operator-recovery:" + path.stem, "operator_recovery", "A", "pending",
                                  request=request, attempts=3, next_retry_at=0)
        await self.sup.process_recovery_requests()
        self.assertFalse(path.exists())
        self.assertTrue((self.store.run_dir / "control" / "recovery-parked" / path.name).exists())
        self.assertEqual([kind for kind, _ in self.sup.teams["A"].requests if kind == "recover_members"], [])
        self.assertEqual(self.store.operation("operator-recovery:" + path.stem)["status"], "needs_attention")

    async def test_operator_recovery_waits_for_runtime_without_consuming_attempt(self):
        await self.start("A")
        self.sup.config.team_engine = "veria"
        path = self.recovery_request("wait-runtime")
        self.sup.teams["A"].alive = False
        await self.sup.process_recovery_requests()
        operation = self.store.operation("operator-recovery:" + path.stem)
        self.assertEqual(operation["status"], "pending")
        self.assertEqual(operation["attempts"], 0)
        self.assertEqual(operation["receipt"]["reason"], "waiting_runtime")
        self.assertTrue(path.exists())
        self.sup.teams["A"].alive = True
        self.clock.now += self.config.poll_interval
        await self.sup.process_recovery_requests()
        self.assertFalse(path.exists())
        self.assertEqual(self.store.operation("operator-recovery:" + path.stem)["status"], "confirmed")

    async def test_committed_audit_switch_is_automatically_replayed_after_crash(self):
        await self.start("A", "B", "C")
        await self.sup.audit_one("A", 1)
        original_switch = self.sup.switch

        async def crash(*args, **kwargs):
            raise RuntimeError("simulated crash after atomic audit commit")

        self.sup.switch = crash
        with self.assertRaisesRegex(RuntimeError, "atomic audit commit"):
            await self.sup.audit_one("A", 2)
        self.sup.switch = original_switch
        row = self.store.challenge("A")
        operation = self.store.operation(self.sup._audit_operation_id(row["attempt_id"], 2))
        self.assertEqual(operation["status"], "applied")
        self.assertTrue(row["confirmation"]["execute"])
        self.assertEqual(self.platform.close_calls, [])
        recovered = self.make_supervisor()
        recovered.teams = self.sup.teams
        recovered.last_heartbeat = self.sup.last_heartbeat
        await recovered.resume_pending_audits()
        self.assertEqual(self.platform.close_calls, ["A"])
        self.assertEqual([packet["mode"] for packet in self.strategy.calls], ["review", "review", "select"])
        await recovered.resume_pending_audits()
        self.assertEqual(self.platform.close_calls, ["A"])
        self.assertEqual(len(self.strategy.calls), 3)

    async def test_old_attempt_pending_audit_cannot_create_round_in_new_attempt(self):
        await self.start("A")
        row = self.store.challenge("A")
        old_attempt = "obsolete-attempt"
        old_operation = self.sup._audit_operation_id(old_attempt, 2)
        self.store.save_operation(old_operation, "audit_request", "A", "pending", attempt_id=old_attempt,
            round_id=2, revision=0, score_at_start=0, request_id=old_attempt + ":2")
        await self.sup.resume_pending_audits()
        self.assertEqual(self.store.operation(old_operation)["status"], "superseded")
        self.assertIsNone(self.store.operation(self.sup._audit_operation_id(row["attempt_id"], 2)))
        self.assertEqual(self.strategy.calls, [])

    async def test_old_hosted_config_without_provider_loads_and_resumes_unambiguous_service(self):
        from xinghuo.hosted import _saved_config
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            try:
                config = Config(team_engine="veria", backend="claude", veria_environment="hosted",
                    veria_python=sys.executable, veria_tool_image="offline-immutable",
                    veria_tool_cpu_limit=8, veria_tool_memory_limit="16g").validate()
                saved = config.to_dict()
                for field in ("veria_provider", "veria_team_profile", "veria_solver_ids", "veria_providers"):
                    saved.pop(field)
                store.set_meta("config", saved)
                store.set_meta("run_id", "legacy-hosted")
                store.set_meta("started_at", 1000)
                store.set_meta("deadline", 29800)
                loaded = _saved_config(Path(directory), {})
                self.assertEqual(loaded.veria_provider, "deepseek")
                resumed = Supervisor(None, store, loaded, strategy=FakeStrategy(), team_factory=FakeTeam)
                self.assertEqual(resumed.deadline, 29800)
                self.assertEqual(resumed.config.veria_team_profile, "legacy6")
                with self.assertRaisesRegex(ValueError, "veria_provider"):
                    Supervisor(None, store, replace(loaded, veria_provider="glm"),
                               strategy=FakeStrategy(), team_factory=FakeTeam)
            finally:
                store.close()


class SupervisorTimingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        self.clock = Clock()
        self.platform = FakePlatform()
        self.config = Config(backend="demo", epsilon=0, budget=900, cleanup_reserve=180,
                             operation_timeout=.2, shutdown_timeout=.2, tick=.001)
        self.events = []

    async def asyncTearDown(self):
        self.store.close()
        self.temp.cleanup()

    def supervisor(self, *, config=None, callback=None, team_factory=FakeTeam):
        return Supervisor(self.platform, self.store, config or self.config, clock=self.clock,
                          strategy=FakeStrategy(), team_factory=team_factory,
                          on_lifecycle=callback or self.events.append)

    async def test_uninitialized_run_has_no_invented_actual_schedule(self):
        supervisor = self.supervisor()
        self.assertTrue(all(value is None for value in supervisor.timing_status().values()))
        self.assertIsNone(supervisor.status()["exit_reason"])

    async def test_short_budget_stops_at_work_deadline_before_final_deadline(self):
        calls = []
        original_list = self.platform.list_challenges

        async def list_challenges():
            calls.append("reconcile")
            self.assertEqual(self.events[0]["event"], "initialized")
            return await original_list()

        def lifecycle(event):
            self.events.append(event)
            calls.append(event["event"])
            if event["event"] == "initialized":
                self.clock.now = event["work_deadline"]

        self.platform.list_challenges = list_challenges
        supervisor = self.supervisor(callback=lifecycle)
        result = await supervisor.run()
        self.assertEqual(calls, ["initialized", "reconcile", "stopping"])
        initial, stopped = self.events
        self.assertFalse(initial["resumed"])
        self.assertEqual(initial["started_at"], 1000)
        self.assertEqual(initial["deadline"], 1900)
        self.assertEqual(initial["work_deadline"], 1810)
        self.assertEqual(initial["budget_seconds"], 900)
        self.assertEqual(initial["cleanup_reserve_seconds"], 90)
        self.assertEqual(initial["work_remaining_seconds"], 810)
        self.assertEqual(stopped["exit_reason"], "work_deadline_reached")
        self.assertEqual(result["status"], "deadline")
        self.assertEqual(result["elapsed_seconds"], 810)
        self.assertEqual(result["remaining_seconds"], 90)
        self.assertEqual(result["deadline_remaining_seconds"], 90)
        self.assertEqual(result["work_remaining_seconds"], 0)
        self.assertEqual(self.platform.start_calls, [])
        self.assertEqual(json.loads((self.store.run_dir / "status.json").read_text()), result)

    async def test_eight_hour_schedule_preserves_three_minute_cleanup_reserve(self):
        supervisor = self.supervisor(config=replace(self.config, budget=28800))
        await supervisor.initialize()
        try:
            timing = supervisor.timing_status()
            self.assertEqual(timing["budget_seconds"], 28800)
            self.assertEqual(timing["work_remaining_seconds"], 28620)
            self.assertEqual(timing["cleanup_reserve_seconds"], 180)
            self.assertEqual(timing["deadline"], 29800)
            self.assertEqual(timing["work_deadline"], 29620)
            self.clock.now += 3600
            self.assertEqual(supervisor.status()["elapsed_seconds"], 3600)
            self.assertEqual(supervisor.status()["remaining_seconds"], 25200)
            self.assertEqual(supervisor.status()["work_remaining_seconds"], 25020)
        finally:
            await supervisor.shutdown()

    async def test_resume_uses_persisted_budget_and_work_deadline(self):
        original = self.supervisor()
        await original.initialize()
        original_run_id = self.store.get_meta("run_id")
        await original.shutdown()
        self.clock.now += 100
        resumed = self.supervisor(config=replace(self.config, budget=21600, cleanup_reserve=900))
        await resumed.initialize()
        try:
            self.assertEqual(self.store.get_meta("run_id"), original_run_id)
            self.assertTrue(self.events[-1]["resumed"])
            self.assertEqual(resumed.status()["budget_seconds"], 900)
            self.assertEqual(resumed.status()["deadline"], 1900)
            self.assertEqual(resumed.status()["work_deadline"], 1810)
            self.assertEqual(resumed.status()["work_remaining_seconds"], 710)
            self.assertEqual(resumed.status()["cleanup_reserve_seconds"], 90)
        finally:
            await resumed.shutdown()

    async def test_legacy_resume_derives_work_deadline_from_saved_cleanup_policy(self):
        original = self.supervisor()
        await original.initialize()
        await original.shutdown()
        self.store.set_meta("work_deadline", None)
        resumed = self.supervisor(config=replace(self.config, budget=21600, cleanup_reserve=900))
        await resumed.initialize()
        try:
            self.assertEqual(resumed.work_deadline, 1810)
            self.assertEqual(self.store.get_meta("work_deadline"), 1810)
        finally:
            await resumed.shutdown()

    async def test_expired_resume_reconciles_and_closes_target_without_starting_runtime(self):
        original = self.supervisor()
        await original.initialize()
        # Simulate a process crash after the target became available. No
        # graceful shutdown occurred and the new owner must close that target.
        self.platform.update("A", container_status="available", container_addr=("A.lab:80",))
        row = self.store.challenge("A")
        row.update(status="active", attempt_id="interrupted-attempt")
        self.store.save_challenge(row)
        self.platform.update("B", container_status="available", container_addr=("B.lab:80",))
        closing = self.store.challenge("B")
        closing.update(status="closing", attempt_id="interrupted-close", close_intent="shutdown")
        self.store.save_challenge(closing)
        original.lock.close()
        original.lock = None
        self.clock.now = 1811  # Final budget has 89s left; work has already ended.
        constructed = []

        def unexpected_team(*args, **kwargs):
            constructed.append(args)
            raise AssertionError("expired resume must not construct a runtime")

        resumed = self.supervisor(config=replace(self.config, budget=21600), team_factory=unexpected_team)
        result = await resumed.run()
        self.assertEqual(result["exit_reason"], "work_deadline_reached")
        self.assertEqual(result["deadline"], 1900)
        self.assertEqual(constructed, [])
        self.assertEqual(self.platform.start_calls, [])
        self.assertCountEqual(self.platform.close_calls, ["A", "B"])
        self.assertEqual(self.store.challenge("A")["status"], "paused")
        self.assertTrue(self.store.challenge("A")["close_confirmed"])
        self.assertTrue(self.store.challenge("B")["close_confirmed"])

    async def test_requested_stop_is_distinct_from_deadline(self):
        supervisor = self.supervisor()
        supervisor.stop_requested = True
        result = await supervisor.run()
        self.assertEqual(result["status"], "stopped")
        self.assertEqual(result["exit_reason"], "stop_requested")
        self.assertEqual(result["remaining_seconds"], 900)
        self.assertEqual(self.events[-1]["exit_reason"], "stop_requested")
        self.assertEqual(self.platform.start_calls, [])

    async def test_all_completed_is_distinct_from_deadline(self):
        for code in self.platform.rows:
            self.platform.update(code, completed=True, correct_flag_count=3)
        result = await self.supervisor().run()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["exit_reason"], "all_challenges_completed")
        self.assertEqual(self.events[-1]["exit_reason"], "all_challenges_completed")
        self.assertEqual(self.platform.start_calls, [])

    async def test_runtime_error_is_persisted_and_original_exception_propagates(self):
        failure = RuntimeError("test reconciliation failure")

        async def failing_list():
            raise failure

        self.platform.list_challenges = failing_list
        supervisor = self.supervisor()
        with self.assertRaises(RuntimeError) as raised:
            await supervisor.run()
        self.assertIs(raised.exception, failure)
        self.assertEqual(self.events[-1]["exit_reason"], "runtime_error")
        self.assertEqual(supervisor.status()["status"], "error")
        self.assertEqual(supervisor.status()["exit_reason"], "runtime_error")
        self.assertEqual(json.loads((self.store.run_dir / "status.json").read_text())["exit_reason"],
                         "runtime_error")
        self.assertIsNone(supervisor.lock)

    async def test_lock_failure_cannot_overwrite_or_clean_up_the_live_run(self):
        owner = self.supervisor()
        await owner.initialize()
        await owner.start_challenge("A", "selection-A")
        second = self.supervisor()
        try:
            with self.assertRaises(RuntimeError):
                await second.run()
            self.assertEqual(self.store.get_meta("status"), "running")
            self.assertIsNone(self.store.get_meta("exit_reason"))
            self.assertEqual(self.platform.close_calls, [])
            self.assertTrue(owner.teams["A"].alive)
            self.assertEqual(len(self.events), 1)
        finally:
            await owner.shutdown()

    async def test_cleanup_failure_cannot_replace_primary_runtime_exception(self):
        primary = ValueError("primary reconciliation failure")
        secondary = RuntimeError("secondary cleanup failure")

        async def failing_list():
            raise primary

        async def failing_shutdown():
            raise secondary

        self.platform.list_challenges = failing_list
        supervisor = self.supervisor()
        supervisor.shutdown = failing_shutdown
        with self.assertRaises(ValueError) as raised:
            await supervisor.run()
        self.assertIs(raised.exception, primary)
        self.assertEqual(supervisor.status()["exit_reason"], "runtime_error")
        failures = [json.loads(row[0]) for row in self.store.db.execute(
            "SELECT body FROM events WHERE kind='supervisor_cleanup_failed'")]
        self.assertEqual(failures, [{"error_type": "RuntimeError", "primary_error_type": "ValueError"}])
        self.assertIsNone(supervisor.lock)

    async def test_cleanup_failure_after_success_is_still_a_runtime_error(self):
        for code in self.platform.rows:
            self.platform.update(code, completed=True, correct_flag_count=3)
        failure = RuntimeError("cleanup failure after completion")

        async def failing_shutdown():
            raise failure

        supervisor = self.supervisor()
        supervisor.shutdown = failing_shutdown
        with self.assertRaises(RuntimeError) as raised:
            await supervisor.run()
        self.assertIs(raised.exception, failure)
        self.assertEqual(supervisor.status()["status"], "error")
        self.assertEqual(self.events[-1]["exit_reason"], "runtime_error")
        failure_record = json.loads(self.store.db.execute(
            "SELECT body FROM events WHERE kind='supervisor_cleanup_failed'").fetchone()[0])
        self.assertEqual(failure_record, {"error_type": "RuntimeError", "primary_error_type": None})
        self.assertIsNone(supervisor.lock)


if __name__ == "__main__":
    unittest.main()


class RuntimeStartupRaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        self.platform = FakePlatform()
        self.sup = Supervisor(self.platform, self.store,
            Config(backend='demo', epsilon=0, budget=10, tick=.002, poll_interval=.01,
                   operation_timeout=.2, shutdown_timeout=.2),
            strategy=FakeStrategy(), team_factory=FakeTeam)

    async def asyncTearDown(self):
        await self.sup.shutdown()
        self.store.close()
        self.temp.cleanup()

    async def test_watchdog_does_not_recover_before_start_has_a_process(self):
        entered, release = asyncio.Event(), asyncio.Event()
        teams = []
        class SlowTeam(FakeTeam):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                teams.append(self)
            async def start(self, context):
                entered.set()
                await release.wait()
                await super().start(context)
        self.sup.team_factory = SlowTeam
        running = asyncio.create_task(self.sup.run())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            # Exercise several actual supervisor ticks and reconciliations while
            # the process is absent, just like TeamProcess's interpreter check.
            await asyncio.sleep(.04)
            self.assertEqual(len(teams), 1)
            self.assertEqual(teams[0].stop_count, 0)
            self.assertEqual(self.store.challenge('A')['runtime_restarts'], 0)
            release.set()
            async with asyncio.timeout(1):
                while sum(t.alive for t in self.sup.teams.values()) != 3:
                    await asyncio.sleep(.005)
            self.assertEqual(len(teams), 3)
            self.assertEqual(self.platform.peak, 3)
            self.assertTrue(all(self.store.challenge(c)['runtime_restarts'] == 0 for c in 'ABC'))
        finally:
            release.set()
            self.sup.stop_requested = True
            await asyncio.wait_for(running, 2)

    async def test_reconciliation_waits_until_old_runtime_finishes_stopping(self):
        entered, release = asyncio.Event(), asyncio.Event()
        class SlowStopTeam(FakeTeam):
            async def stop(self):
                entered.set()
                await release.wait()
                await super().stop()
        self.sup.team_factory = SlowStopTeam
        await self.sup.initialize()
        await self.sup.start_challenge('A', 'test-start')
        old = self.sup.teams['A']
        recovery = asyncio.create_task(self.sup.recover_runtime('A'))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await self.sup.reconcile()
            self.assertNotIn('runtime:A', self.sup.jobs)
            await self.sup.start_runtime('A')
            self.assertNotIn('A', self.sup.teams)
            release.set()
            await asyncio.wait_for(recovery, 1)
            self.assertIsNot(self.sup.teams['A'], old)
            self.assertEqual(old.stop_count, 1)
            self.assertEqual(self.store.challenge('A')['runtime_restarts'], 1)
        finally:
            release.set()
            await asyncio.gather(recovery, return_exceptions=True)
