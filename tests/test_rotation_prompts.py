"""The no-flag rotation rule reaches SDK prompts without changing raw upstream text."""
import json
from pathlib import Path
import tempfile
import time
import unittest

from xinghuo.config import (VERIA_FLASH4_MODELS, VERIA_FLASH4_PROVIDERS,
                            VERIA_MIXED4_SOLVERS)
from xinghuo.veria import upstream_baseline as upstream
from xinghuo.veria.prompts import (build_role_document, continuation_document,
                                   continuation_prompt, rotation_policy_appendix)
from xinghuo.veria_runtime import VeriaRuntime, VeriaSlot


POLICY = {"mode": "no_flag_timeout", "timeout_seconds": 900}
ROLES = ("solver", "coordinator", "reporter", "reviewer")


class RotationPromptTests(unittest.TestCase):
    def test_policy_states_confirmation_clock_and_stop_boundaries_without_a_fake_countdown(self):
        appendix = rotation_policy_appendix(POLICY)
        for requirement in (
                "900 seconds (15 minutes)",
                "configured interval, not the time remaining",
                "new increase in the authoritative platform-confirmed correct flag count",
                "Incorrect submissions, duplicate or reproduced flags, score-only changes",
                "promising evidence, report publication, tool activity, coordinator bumps and reviewer work",
                "do not reset or extend it",
                "No two-review confirmation or reviewer approval is required",
                "Pausing an attempt is not platform confirmation that the challenge is completed",
                "do not infer an exact countdown or stop early on your own",
                "Submit every new candidate immediately",
                "promptly preserve",
                "Any reviewer schedule is a maximum review budget",
                "never grants extra challenge time or postpones rotation",
                "prioritizes untried questions",
                "Keep the runtime-assigned provider and model"):
            with self.subTest(requirement=requirement):
                self.assertIn(requirement, appendix)
        self.assertIn("120 seconds (2 minutes)", rotation_policy_appendix(
            {"mode": "no_flag_timeout", "timeout_seconds": 120}))

    def test_every_role_receives_one_policy_appendix_and_keeps_its_original_baseline(self):
        appendix = rotation_policy_appendix(POLICY)
        self.assertTrue(appendix.strip())
        context = {"solver_ids": VERIA_MIXED4_SOLVERS, "challenge": {"category": "pwn"}}
        for role in ROLES:
            with self.subTest(role=role):
                legacy = build_role_document(role, role, context, {})
                active = build_role_document(role, role,
                    {**context, "rotation_policy": POLICY}, {})
                self.assertEqual(active.baseline, legacy.baseline)
                self.assertEqual(active.audit()["baseline_sha256"], legacy.audit()["baseline_sha256"])
                self.assertEqual(active.text.count(appendix), 1)
                self.assertTrue(active.appendix.endswith(appendix))

    def test_active_role_adaptations_remove_only_the_conflicting_indefinite_rules(self):
        context = {"rotation_policy": POLICY}
        solver = build_role_document("solver", "solver-1", context, {})
        coordinator = build_role_document("coordinator", "coordinator", context, {})
        self.assertIn("rotation-solver-stop", {item.rule for item in solver.patches})
        self.assertNotIn("2. Keep using tools until the platform confirms all challenge objectives complete.",
                         solver.adapted)
        self.assertIn("For RSA: use `RsaCtfTool`, sage ECM, or `cado-nfs`.", solver.text)
        self.assertIn("Be creative and thorough", solver.text)
        self.assertTrue({"rotation-coordinator-bounded", "rotation-coordinator-cost"}
                        <= {item.rule for item in coordinator.patches})
        self.assertNotIn("NEVER kill a swarm", coordinator.adapted)
        self.assertNotIn("Solvers will keep trying indefinitely", coordinator.adapted)
        self.assertNotIn("Cost is not a concern", coordinator.adapted)
        self.assertEqual(coordinator.baseline, upstream.COORDINATOR_PROMPT)
        reviewer = build_role_document("reviewer", "reviewer", context, {})
        self.assertNotIn("The total budget is 30 minutes", reviewer.text)
        self.assertIn("The persisted review schedule is only a maximum review budget", reviewer.text)
        self.assertIn("an earlier supervisor stop ends this review as partial", reviewer.text)
        legacy_review = build_role_document("reviewer", "reviewer", {}, {})
        self.assertIn("The total budget is 30 minutes", legacy_review.text)

    def test_continuation_first_regular_guidance_and_confirmed_paths_all_obey_the_policy(self):
        appendix = rotation_policy_appendix(POLICY)
        for first in (False, True):
            for confirmed in (False, True):
                for insights in ([], [{"id": "guidance", "insights": "inspect nonce again"}]):
                    with self.subTest(first=first, confirmed=confirmed, insights=insights):
                        kwargs = {"first": first, "has_confirmed": confirmed, "insights": insights}
                        original = continuation_document(**kwargs)
                        active = continuation_document(**kwargs, rotation_policy=POLICY)
                        self.assertEqual(active.baseline, original.baseline)
                        self.assertEqual(active.text.count(appendix), 1)
                        self.assertEqual(active.text, continuation_prompt(**kwargs, rotation_policy=POLICY))
                        self.assertNotIn("Only authoritative platform completion ends the challenge.", active.text)
                        if confirmed:
                            self.assertIn("remaining objectives", active.text)
                        if insights:
                            self.assertIn('"id": "guidance"', active.text)
                            self.assertIn("inspect nonce again", active.text)

    def test_absent_or_disabled_policy_preserves_the_legacy_prompt_contract(self):
        self.assertEqual(continuation_prompt(has_confirmed=False, first=True), upstream.FIRST_PROMPT)
        self.assertEqual(continuation_prompt(has_confirmed=False), upstream.CONTINUE_PROMPT)
        invalid_timeouts = (0, -1, False, True, "900", float("inf"), float("nan"))
        for policy in (None, {}, {"mode": "review", "timeout_seconds": 900},
                       *({"mode": "no_flag_timeout", "timeout_seconds": value}
                         for value in invalid_timeouts)):
            with self.subTest(policy=policy):
                self.assertEqual(rotation_policy_appendix(policy), "")
                self.assertEqual(continuation_prompt(has_confirmed=False, rotation_policy=policy),
                                 upstream.CONTINUE_PROMPT)
        legacy = build_role_document("coordinator", "coordinator", {}, {})
        self.assertIn("NEVER kill a swarm", legacy.text)
        self.assertIn("Cost is not a concern", legacy.text)


class RecordingDriver:
    """Capture the actual driver inputs while avoiding SDK and background turns."""
    def __init__(self, agent_id, role, model_id, workspace, context, environment,
                 on_tool, on_session=None, on_event=None, session_id=None):
        self.context, self.role = context, role
        self.on_session = on_session

    async def start(self):
        await self.on_session("offline-session-" + self.role)


class RotationPromptRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.runtimes = []

    async def asyncTearDown(self):
        for runtime in self.runtimes:
            await runtime.shutdown()
        self.temp.cleanup()

    def runtime(self, timeout=900):
        runtime = VeriaRuntime(Path(self.temp.name), "policy-" + str(len(self.runtimes)), "attempt",
            settings={"no_flag_timeout": timeout, "veria_team_profile": "flash4",
                      "veria_models": VERIA_FLASH4_MODELS,
                      "veria_solver_ids": list(VERIA_MIXED4_SOLVERS),
                      "veria_providers": VERIA_FLASH4_PROVIDERS},
            driver_factory=RecordingDriver)
        self.runtimes.append(runtime)
        runtime.room.set_meta("context", {"run_id": "offline", "deadline": time.time() + 10000,
                                         "challenge": {}, "platform": {}})
        for member, role in [(member, "solver") for member in VERIA_MIXED4_SOLVERS] + [
                (role, role) for role in ("coordinator", "reporter", "reviewer")]:
            runtime.room.register_member(member, role, VERIA_FLASH4_MODELS[member],
                                          workspace=str(runtime.directory / "agents" / member), run_id="offline")
            runtime.slots[member] = VeriaSlot(member, role)
        request = {"review_id": "offline-review", "started_at": time.time(),
                   "confirmed_submissions": []}
        runtime.room.set_meta("review_request", request)
        runtime.room.start_review("offline-review", request)
        runtime.slots["reviewer"].review_id = "offline-review"
        return runtime

    async def test_driver_context_and_every_turn_include_the_same_authoritative_policy(self):
        runtime = self.runtime()
        context = runtime.room.meta("context")
        context["rotation_policy"] = {"mode": "no_flag_timeout", "timeout_seconds": 1800}
        runtime.room.set_meta("context", context)
        appendix = rotation_policy_appendix(POLICY)
        for member, slot in runtime.slots.items():
            with self.subTest(member=member):
                driver = await runtime._new_driver(slot)
                self.assertEqual(driver.context["rotation_policy"], POLICY)
                self.assertEqual(driver.context["provider"], "deepseek")
                payload = json.loads(runtime._prompt(slot))
                self.assertEqual(payload["rotation_policy"], POLICY)
                self.assertEqual(payload["instruction"].count(appendix), 1)
                self.assertNotIn("remaining_seconds", payload["rotation_policy"])
                self.assertNotIn("deadline", payload["rotation_policy"])
        reviewer = json.loads(runtime._prompt(runtime.slots["reviewer"]))
        self.assertIn("record_reproductions", reviewer["instruction"])
        self.assertIn("review_context", reviewer)

    async def test_disabled_runtime_does_not_invent_a_rotation_rule(self):
        runtime = self.runtime(timeout=0)
        context = runtime.room.meta("context")
        context["rotation_policy"] = POLICY
        runtime.room.set_meta("context", context)
        for slot in runtime.slots.values():
            driver = await runtime._new_driver(slot)
            self.assertEqual(rotation_policy_appendix(driver.context.get("rotation_policy")), "")
            payload = json.loads(runtime._prompt(slot))
            self.assertEqual(rotation_policy_appendix(payload.get("rotation_policy")), "")


if __name__ == "__main__":
    unittest.main()
