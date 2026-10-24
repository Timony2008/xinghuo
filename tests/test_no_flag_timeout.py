"""Hard no-flag rotation uses a deterministic clock and no paid services."""
import asyncio
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from xinghuo.config import Config
from xinghuo.core import Challenge, SubmissionReceipt
from xinghuo.store import Store
from xinghuo.supervisor import Supervisor
from tests.test_supervisor import Clock, FakePlatform, FakeStrategy, FakeTeam


class NoFlagTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        self.clock = Clock()
        self.platform = FakePlatform()
        self.strategy = FakeStrategy()
        self.config = Config(backend="demo", budget=100000, parallel=3,
                             no_flag_timeout=900, operation_timeout=.05,
                             shutdown_timeout=.05, epsilon=1)
        self.sup = self.make_supervisor()
        await self.sup.initialize()

    def make_supervisor(self, config=None):
        return Supervisor(self.platform, self.store, config or self.config,
                          strategy=self.strategy, team_factory=FakeTeam, clock=self.clock)

    async def asyncTearDown(self):
        await self.sup.shutdown()
        self.store.close()
        self.temp.cleanup()

    async def settle(self):
        for _ in range(10):
            jobs = list(self.sup.jobs.values())
            if not jobs:
                return
            results = await asyncio.gather(*jobs, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise result
            self.sup._reap()
        self.fail("jobs did not settle")

    async def advance(self, seconds):
        self.clock.now += seconds
        self.sup.check_no_flag_timeouts()
        await self.settle()

    async def start(self, *codes):
        for code in codes:
            await self.sup.start_challenge(code, "test:" + code)

    async def flag(self, code="A", *, flag="new-flag", count=1):
        self.platform.receipts[flag] = SubmissionReceipt(correct=True,
            correct_flag_count=count, total_flag_count=3)
        await self.sup.submit(code, flag)

    async def test_899_keeps_scene_900_skips_without_audits_or_reviewer(self):
        await self.start("A", "B", "C")
        old = self.sup.teams["A"]
        row = self.store.challenge("A")
        row.update(confirmation={"streak": 0}, pending_review_request={"review_id": "busy"})
        self.store.save_challenge(row)
        await self.advance(899)
        self.assertEqual(self.platform.close_calls, [])
        await self.advance(1)
        self.assertEqual(set(self.platform.close_calls), set("ABC"))
        self.assertEqual(self.store.challenge("A")["close_intent"], "no_flag_timeout")
        self.assertEqual(old.stop_count, 1)
        self.assertEqual(self.strategy.calls, [])
        await self.sup.fill_one()
        self.assertEqual(self.platform.start_calls[-1], "D")

    async def test_only_new_confirmed_count_resets_900_second_window(self):
        await self.start("A")
        await self.advance(600)
        await self.flag()
        self.assertEqual(self.store.challenge("A")["last_flag_work"], 600)
        await self.advance(899)
        self.assertEqual(self.platform.close_calls, [])
        await self.advance(1)
        self.assertEqual(self.platform.close_calls, ["A"])

    async def test_score_evidence_wrong_duplicate_and_replayed_receipt_do_not_reset(self):
        await self.start("A")
        await self.flag()
        await self.advance(800)
        for flag, receipt in {
            "wrong": SubmissionReceipt(correct=False),
            "duplicate": SubmissionReceipt(correct=True, duplicate=True, correct_flag_count=1),
            "score-only": SubmissionReceipt(awarded=10, cumulative_score=10),
        }.items():
            self.platform.receipts[flag] = receipt
            await self.sup.submit("A", flag)
        await self.sup.submit("A", "new-flag")
        self.sup.handle_team_event("A", {"type": "significant_evidence", "evidence_id": "new-route"})
        await self.sup.audit_one("A", 100)
        self.assertEqual(self.store.challenge("A")["last_flag_work"], 0)
        self.assertEqual(self.strategy.calls, [])
        await self.advance(100)
        self.assertEqual(self.platform.close_calls, ["A"])

    async def test_flag_during_prepare_stop_cancels_skip_and_unquiesces(self):
        await self.start("A")
        team = self.sup.teams["A"]
        attempt = self.store.challenge("A")["attempt_id"]

        async def final_flag(payload):
            await self.flag()
            return {"flushed": True}

        team.handlers["prepare_stop"] = final_flag
        await self.advance(900)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertEqual(self.store.challenge("A")["attempt_id"], attempt)
        self.assertEqual(self.store.challenge("A")["last_flag_work"], 900)
        self.assertIn(("cancel_stop", {}), team.requests)
        self.assertEqual(team.stop_count, 0)

    async def test_final_candidate_event_is_submitted_before_closing(self):
        await self.start("A")
        team = self.sup.teams["A"]
        self.platform.receipts["final"] = SubmissionReceipt(correct=True,
            correct_flag_count=1, total_flag_count=3)

        async def flush(payload):
            team.events.append({"type": "candidate_flag", "flag": "final"})
            return {"flushed": True}

        team.handlers["prepare_stop"] = flush
        await self.advance(900)
        self.assertEqual(self.platform.submit_calls, [("A", "final")])
        self.assertEqual(self.platform.close_calls, [])
        self.assertTrue(team.alive)

    async def test_flag_during_runtime_stop_restores_same_attempt(self):
        await self.start("A")
        old = self.sup.teams["A"]
        attempt = old.attempt_id

        async def stop_with_flag():
            await self.flag()
            await FakeTeam.stop(old)

        old.stop = stop_with_flag
        await self.advance(900)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertIsNot(self.sup.teams["A"], old)
        self.assertEqual(self.sup.teams["A"].attempt_id, attempt)
        self.assertTrue(self.sup.teams["A"].alive)

    async def test_new_flag_in_platform_reconciliation_revokes_timeout(self):
        await self.start("A")
        original = self.platform.list_challenges

        async def updated():
            self.platform.update("A", correct_flag_count=1)
            return await original()

        self.platform.list_challenges = updated
        await self.advance(900)
        self.assertEqual(self.platform.close_calls, [])
        self.assertEqual(self.store.challenge("A")["last_flag_work"], 900)

    async def test_confirmation_between_ticks_anchors_window_to_current_work_time(self):
        await self.start("A")
        self.clock.now += 899  # No supervisor tick has accrued this interval.
        await self.flag()
        self.assertEqual(self.store.challenge("A")["last_flag_work"], 899)
        await self.advance(1)
        self.assertEqual(self.platform.close_calls, [])
        await self.advance(899)
        self.assertEqual(self.platform.close_calls, ["A"])

    async def test_unresolved_submission_is_retained_without_indefinite_skip_gate(self):
        await self.start("A")
        attempt = self.store.challenge("A")["attempt_id"]
        self.store.save_operation("submit:unknown", "submit", "A", "unknown",
            flag="candidate", attempt_id=attempt, attempts=3, receipt={"unknown": True})
        await self.advance(900)
        self.assertEqual(self.platform.close_calls, ["A"])
        self.assertEqual(self.store.operation("submit:unknown")["status"], "unknown")
        self.assertEqual(self.store.operation("submit:unknown")["attempt_id"], attempt)

    async def test_actual_start_recovery_and_resume_keep_correct_clock(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class SlowTeam(FakeTeam):
            async def start(inner, context):
                entered.set()
                await release.wait()
                await super().start(context)

        self.sup.team_factory = SlowTeam
        starting = asyncio.create_task(self.sup.start_challenge("A", "slow"))
        await entered.wait()
        self.clock.now += 1000
        self.sup.check_no_flag_timeouts()
        self.assertEqual(self.store.challenge("A")["active_seconds"], 0)
        release.set()
        await starting
        await self.advance(400)
        attempt = self.store.challenge("A")["attempt_id"]
        self.sup.team_factory = FakeTeam
        await self.sup.recover_runtime("A")
        self.assertEqual(self.store.challenge("A")["active_seconds"], 400)
        self.assertEqual(self.store.challenge("A")["attempt_id"], attempt)
        # Simulate supervisor loss without a graceful scene close.
        await self.sup.teams["A"].stop()
        self.sup.teams.clear()
        self.sup.lock.close()
        self.sup.lock = None
        self.clock.now += 10000
        self.sup = self.make_supervisor()
        await self.sup.initialize()
        await self.settle()
        self.assertEqual(self.store.challenge("A")["active_seconds"], 400)
        await self.advance(499)
        self.assertEqual(self.platform.close_calls, [])
        await self.advance(1)
        self.assertEqual(self.platform.close_calls, ["A"])

    async def test_crash_during_checkpoint_resumes_same_expired_attempt(self):
        await self.start("A")
        attempt = self.store.challenge("A")["attempt_id"]
        row = self.store.challenge("A")
        row.update(status="draining", active_seconds=900)
        self.store.save_challenge(row)
        await self.sup.teams["A"].stop()
        self.sup.teams.clear()
        self.sup.lock.close()
        self.sup.lock = None
        self.clock.now += 10000
        self.sup = self.make_supervisor()
        await self.sup.initialize()
        await self.settle()
        self.assertEqual(self.store.challenge("A")["attempt_id"], attempt)
        await self.advance(0)
        self.assertEqual(self.platform.close_calls, ["A"])

    async def test_initial_startup_failure_still_gets_bounded_idle_window(self):
        class BrokenTeam(FakeTeam):
            async def start(inner, context):
                raise RuntimeError("interpreter launch failed")

        self.sup.team_factory = BrokenTeam
        with self.assertRaises(RuntimeError):
            await self.start("A")
        row = self.store.challenge("A")
        row["runtime_restarts"] = self.config.max_retries + 1
        self.store.save_challenge(row)
        self.assertNotIn("A", self.sup.teams)
        await self.advance(899)
        self.assertEqual(self.platform.close_calls, [])
        await self.advance(1)
        self.assertEqual(self.platform.close_calls, ["A"])

    async def test_live_checkpoint_is_not_resurrected_as_a_crashed_checkpoint(self):
        await self.start("A")
        await self.sup.teams["A"].stop()
        self.sup.teams.clear()
        row = self.store.challenge("A")
        row.update(status="draining", active_seconds=900, close_intent=None)
        self.store.save_challenge(row)
        lock = self.sup._finish_locks.setdefault("A", asyncio.Lock())
        async with lock:
            await self.sup.reconcile()
        self.assertEqual(self.store.challenge("A")["status"], "draining")
        self.assertNotIn("runtime:A", self.sup.jobs)

    async def test_timeout_cancels_recovery_start_before_scene_close(self):
        await self.start("A")
        entered, release = asyncio.Event(), asyncio.Event()
        created = []

        class SlowReplacement(FakeTeam):
            def __init__(inner, *args, **kwargs):
                super().__init__(*args, **kwargs)
                created.append(inner)

            async def start(inner, context):
                entered.set()
                await release.wait()
                await super().start(context)

        self.sup.team_factory = SlowReplacement
        self.sup._job("recover:A", self.sup.recover_runtime("A"))
        await entered.wait()
        self.clock.now += 900
        self.sup.check_no_flag_timeouts()
        await self.settle()
        release.set()
        await asyncio.sleep(0)
        self.assertEqual(self.platform.close_calls, ["A"])
        self.assertNotIn("A", self.sup.teams)
        self.assertTrue(all(not team.alive for team in created))

    async def test_pending_existing_scene_becomes_available_then_starts(self):
        self.platform.update("A", container_status="pending")
        await self.sup.reconcile()
        self.assertEqual(self.store.challenge("A")["status"], "unknown")
        self.platform.update("A", container_status="available", container_addr=("A.lab:80",))
        await self.sup.reconcile()
        await self.settle()
        self.assertEqual(self.store.challenge("A")["status"], "active")
        self.assertIn("A", self.sup.teams)
        self.assertEqual(self.platform.start_calls, [])

    async def test_fresh_attempt_gets_new_window_and_queue_visits_untouched_first(self):
        await self.start("A")
        first_attempt = self.store.challenge("A")["attempt_id"]
        await self.advance(900)
        for _ in range(3):
            await self.sup.fill_one()
        self.assertEqual(self.platform.start_calls, list("ABCD"))
        self.assertNotEqual(self.store.challenge("B")["attempt_id"], first_attempt)
        await self.advance(900)
        for _ in range(2):
            await self.sup.fill_one()
        self.assertEqual(self.platform.start_calls[-2:], list("EA"))
        second = self.store.challenge("A")
        self.assertNotEqual(second["attempt_id"], first_attempt)
        self.assertEqual(second["last_flag_work"], second["active_seconds"])
        await self.advance(899)
        self.assertEqual(self.platform.close_calls.count("A"), 1)

    async def test_fifty_available_scenes_only_admit_three_and_rotate_forward(self):
        self.platform.rows = {f"Q{i:02d}": Challenge(f"Q{i:02d}", flag_count=3,
            container_status="available", container_addr=(f"Q{i:02d}.lab:80",)) for i in range(50)}
        await self.sup.reconcile()
        await self.settle()
        self.assertEqual(len(self.sup.teams), 3)
        self.assertEqual(self.sup.reserve_count(), 3)
        self.assertEqual(set(self.sup.teams), {"Q00", "Q01", "Q02"})
        self.assertEqual(self.platform.start_calls, [])
        for batch in range(4):
            expired = set(self.sup.teams)
            await self.advance(900)
            await self.sup.reconcile()
            await self.settle()
            for _ in range(3):
                await self.sup.fill_one()
            self.assertEqual(len(self.sup.teams), 3)
            self.assertEqual(self.sup.reserve_count(), 3)
            self.assertTrue(expired.isdisjoint(self.sup.teams))
            self.assertEqual(set(self.sup.teams), {f"Q{i:02d}" for i in range(3 * (batch + 1), 3 * (batch + 2))})
        self.assertEqual(len(self.platform.close_calls), 12)
        self.assertEqual(self.platform.start_calls, [])

    async def test_resume_cannot_change_saved_no_flag_rule(self):
        with self.assertRaisesRegex(ValueError, "no_flag_timeout"):
            self.make_supervisor(replace(self.config, no_flag_timeout=0))


if __name__ == "__main__":
    unittest.main()
