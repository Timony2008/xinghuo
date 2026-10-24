"""Hosted run identity and observable deadlines, with no network/model calls."""
import asyncio
import contextlib
from dataclasses import replace
from datetime import datetime
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from xinghuo import hosted
from xinghuo.store import Store
from xinghuo.supervisor import Supervisor


def saved_run(directory, *, budget=900, parallel=1, started_at=1792159353.0):
    config = hosted.hosted_config({"XINGHUO_TEAM_PROFILE": "legacy6", "XINGHUO_BUDGET_SECONDS": str(budget),
                                   "XINGHUO_PARALLEL": str(parallel)})
    store = Store(directory)
    try:
        for name, value in {"config": config.to_dict(), "deadline": started_at + budget,
                            "started_at": started_at, "run_id": "saved-run-fixture",
                            "status": "stopped"}.items():
            store.set_meta(name, value)
    finally:
        store.close()
    return config


class HostedLifecycleDefaultsTests(unittest.TestCase):
    def test_eight_hour_three_challenge_defaults(self):
        config = hosted.hosted_config({})
        self.assertEqual(config.budget, 8 * 60 * 60)
        self.assertEqual(config.parallel, 3)
        self.assertEqual(config.cleanup_reserve, 180)


class HostedRunIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.directory = self.root / "run"
        self.addCleanup(self.temp.cleanup)

    def prepare(self, **values):
        return hosted.prepare_hosted_run({"XINGHUO_RUN_DIR": str(self.directory), **values},
                                         work_root=self.root)

    def test_new_uses_defaults_and_does_not_rewrite_environment(self):
        env = {"XINGHUO_RUN_DIR": str(self.directory)}
        before = dict(env)
        with hosted.prepare_hosted_run(env, work_root=self.root) as run:
            self.assertEqual(run.mode, "new")
            self.assertEqual(run.directory, self.directory)
            self.assertEqual((run.config.budget, run.config.parallel), (28800, 3))
            self.assertEqual((run.budget_source, run.parallel_source), ("default", "default"))
            self.assertIsNone(run.store.get_meta("deadline"))
        self.assertEqual(env, before)

    def test_new_rejects_old_run_without_mutating_saved_database(self):
        saved_run(self.directory)
        database = self.directory / "state.sqlite3"
        before = database.read_bytes()
        with self.assertRaises(ValueError):
            with self.prepare():
                self.fail("new silently reused the old deadline")
        self.assertEqual(database.read_bytes(), before)

    def test_new_rejects_nonempty_directory_without_creating_state(self):
        self.directory.mkdir()
        evidence = self.directory / "previous-evidence.json"
        evidence.write_text('{"preserve": true}')
        before = evidence.read_bytes()
        with self.assertRaises(ValueError):
            with self.prepare():
                self.fail("new adopted an existing evidence directory")
        self.assertEqual(evidence.read_bytes(), before)
        self.assertFalse((self.directory / "state.sqlite3").exists())

    def test_resume_requires_existing_state_without_creating_database(self):
        with self.assertRaises(ValueError):
            with self.prepare(XINGHUO_RUN_MODE="resume"):
                self.fail("resume created a new run")
        self.assertFalse((self.directory / "state.sqlite3").exists())

    def test_resume_inherits_original_budget_parallel_and_deadline(self):
        saved_run(self.directory)
        with self.prepare(XINGHUO_RUN_MODE="resume") as run:
            self.assertEqual(run.mode, "resume")
            self.assertEqual((run.config.budget, run.config.parallel), (900, 1))
            self.assertEqual((run.budget_source, run.parallel_source), ("saved_config", "saved_config"))
            self.assertEqual(run.store.get_meta("deadline"), 1792159353.0 + 900)
            self.assertEqual(run.store.get_meta("run_id"), "saved-run-fixture")

    def test_resume_rejects_explicit_budget_parallel_and_model_conflicts_read_only(self):
        saved_run(self.directory)
        database = self.directory / "state.sqlite3"
        before = database.read_bytes()
        for override in ({"XINGHUO_BUDGET_SECONDS": "21600"}, {"XINGHUO_PARALLEL": "3"},
                         {"XINGHUO_STRATEGY_MODEL": "different-model"}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                with self.prepare(XINGHUO_RUN_MODE="resume", **override):
                    self.fail("resume accepted changed runtime policy")
            self.assertEqual(database.read_bytes(), before)

    def test_resume_accepts_identical_explicit_budget_and_parallel(self):
        saved_run(self.directory)
        with self.prepare(XINGHUO_RUN_MODE="resume", XINGHUO_BUDGET_SECONDS="900",
                          XINGHUO_PARALLEL="1", XINGHUO_STRATEGY_MODEL="deepseek-v4-pro") as run:
            self.assertEqual((run.config.budget, run.config.parallel), (900, 1))
            self.assertEqual(run.store.get_meta("deadline"), 1792159353.0 + 900)

    def test_resume_inherits_custom_aliases_when_only_credentials_are_supplied(self):
        config = saved_run(self.directory)
        config.veria_provider = "glm"
        config.veria_providers = {member: "glm" for member in config.veria_models}
        config.veria_models = hosted.hosted_config({"XINGHUO_TEAM_PROFILE": "legacy6", "XINGHUO_PROVIDER": "glm", "XINGHUO_STRONG_MODEL": "relay-strong",
                                                   "XINGHUO_FAST_MODEL": "relay-fast"}).veria_models
        store = Store(self.directory)
        store.set_meta("config", config.to_dict())
        store.close()
        database = self.directory / "state.sqlite3"
        before = database.read_bytes()
        with self.prepare(XINGHUO_RUN_MODE="resume", XINGHUO_PROVIDER="glm",
                          GLM_API_KEY="rotated-synthetic-key") as run:
            self.assertEqual(run.config.veria_models, config.veria_models)
            self.assertEqual(run.config.veria_provider, "glm")
        self.assertEqual(database.read_bytes(), before)
        for override in ({"XINGHUO_STRONG_MODEL": "glm-5.3"},
                         {"XINGHUO_PROVIDER": "deepseek"}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                with self.prepare(XINGHUO_RUN_MODE="resume", **override):
                    self.fail("resume accepted changed model/provider")
        self.assertEqual(database.read_bytes(), before)

    def test_rejects_invalid_mode_and_outside_work_root(self):
        for overrides in ({"XINGHUO_RUN_MODE": "automatic"},
                          {"XINGHUO_RUN_DIR": str(self.root.parent / "outside-run")}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                with self.prepare(**overrides):
                    self.fail("invalid hosted identity accepted")

    def test_concurrent_entrypoints_cannot_open_same_run(self):
        saved_run(self.directory)
        with self.prepare(XINGHUO_RUN_MODE="resume"):
            with self.assertRaises((ValueError, RuntimeError)):
                with self.prepare(XINGHUO_RUN_MODE="resume"):
                    self.fail("two hosted entrypoints acquired the same run")

    def test_timestamp_log_uses_actual_persisted_schedule_in_one_timezone(self):
        saved_run(self.directory)
        snapshot = {"run_id": "saved-run-fixture", "status": "deadline",
                    "started_at": 1792159353.0, "deadline": 1792160253.0,
                    "work_deadline": 1792160163.0, "budget_seconds": 900.0,
                    "cleanup_reserve_seconds": 90.0, "elapsed_seconds": 815.0,
                    "remaining_seconds": 85.0, "deadline_remaining_seconds": 85.0,
                    "work_remaining_seconds": 0.0, "exit_reason": "work_deadline_reached",
                    "challenges": [{"status": "completed"}, {"status": "active"}]}
        with self.prepare(XINGHUO_RUN_MODE="resume") as run:
            event = hosted.hosted_event("hosted_stopping", run, snapshot, now=1792160168.0)
        self.assertEqual(event["event"], "hosted_stopping")
        self.assertEqual(event["run_mode"], "resume")
        self.assertEqual(event["budget_seconds"], 900.0)
        self.assertEqual(event["configured_budget_seconds"], 900.0)
        self.assertEqual(event["parallel"], 1)
        self.assertEqual(event["exit_reason"], "work_deadline_reached")
        self.assertEqual(event["timezone"], "Asia/Shanghai")
        for field in ("started_at", "work_deadline", "deadline"):
            self.assertEqual(event[field], snapshot[field])
            self.assertTrue(event[field + "_iso"].endswith("+08:00"))
            self.assertEqual(datetime.fromisoformat(event[field + "_iso"]).timestamp(), snapshot[field])
        self.assertEqual(datetime.fromisoformat(event["observed_at_iso"]).timestamp(), 1792160168.0)


class NoModelStrategy:
    async def decide(self, packet):
        raise AssertionError("This lifecycle test must not call a strategy model")


class OfflineClient:
    def __init__(self, *, vpn_error=None, list_error=None, close_error=None,
                 close_block=False, completed=True):
        self.vpn_error, self.list_error, self.completed = vpn_error, list_error, completed
        self.close_error, self.close_block = close_error, close_block
        self.calls = []
        self.close_count = 0
        self.closed = False
        self.close_cancelled = False

    async def check_vpn(self):
        self.calls.append("check_vpn")
        if self.vpn_error:
            raise self.vpn_error

    async def list_challenges(self):
        self.calls.append("list_challenges")
        if self.list_error:
            raise self.list_error
        return [SimpleNamespace(unique_code="fixture", description="offline", is_completed=self.completed,
                    difficulty="normal", level=1, total_score=10, flag_count=1,
                    correct_flag_count=int(self.completed), container_status="stopped", container_addr=[])]

    async def aclose(self):
        self.close_count += 1
        if self.close_error:
            raise self.close_error
        if self.close_block:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.close_cancelled = True
                raise
        self.closed = True


class HostedEntrypointLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.directory = self.root / "run"
        self.addCleanup(self.temp.cleanup)
        self.secrets = ("synthetic-model-secret", "synthetic-platform-secret")
        self.env = {"BENCHMARK_BASE_URL": "https://benchmark.invalid", "BENCHMARK_TOKEN": self.secrets[1],
                    "DEEPSEEK_API_KEY": self.secrets[0], "GLM_API_KEY": "synthetic-glm-secret",
                    "GLM_BASE_URL": "http://console.cloudrouter.com.cn.tsecbench.gw",
                    "DEEPSEEK_BASE_URL": "http://api.deepseek.com.tsecbench.gw/anthropic",
                    "XINGHUO_RUN_DIR": str(self.directory)}

    async def execute(self, client, *, now=1792159353.0, stop=False, expected_error=False):
        out, err = io.StringIO(), io.StringIO()
        supervisor_instances = []
        def make_supervisor(platform, store, config, **kwargs):
            instance = Supervisor(platform, store, config, strategy=NoModelStrategy(), clock=lambda: now, **kwargs)
            supervisor_instances.append(instance)
            return instance
        def signals(supervisor):
            supervisor.stop_requested = stop
        sdk = SimpleNamespace(TSecBenchmarkAsync=lambda **_: client)
        with patch.dict(os.environ, self.env, clear=True), \
             patch.dict(sys.modules, {"tsec_benchmark": sdk}), \
             patch.object(hosted, "offline_check", return_value={"ok": True}), \
             patch("xinghuo.supervisor.Supervisor", side_effect=make_supervisor), \
             patch("xinghuo.cli._signals", side_effect=signals), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            if expected_error:
                with self.assertRaises(hosted.HostedRunError):
                    await hosted.run(work_root=self.root)
            else:
                await hosted.run(work_root=self.root)
        text = out.getvalue() + err.getvalue()
        for secret in self.secrets:
            self.assertNotIn(secret, text)
        events = [json.loads(line) for line in text.splitlines() if line.strip()]
        self.assertEqual(client.close_count, 1)
        self.assertEqual(client.closed, not (bool(client.close_error) or client.close_block))
        return events, supervisor_instances

    def assert_schedule(self, events, *, started_at, budget, reserve, parallel):
        for event in events:
            if event["event"] not in {"hosted_start", "hosted_stopping", "hosted_finished"}:
                continue
            self.assertEqual(event["budget_seconds"], budget)
            self.assertEqual(event["parallel"], parallel)
            self.assertEqual(event["started_at"], started_at)
            self.assertEqual(event["deadline"], started_at + budget)
            self.assertEqual(event["work_deadline"], started_at + budget - reserve)
            for field in ("started_at", "deadline", "work_deadline"):
                self.assertTrue(event[field + "_iso"].endswith("+08:00"))
                self.assertEqual(datetime.fromisoformat(event[field + "_iso"]).timestamp(), event[field])

    async def test_actual_run_reports_eight_hour_schedule_and_completion(self):
        client = OfflineClient()
        events, instances = await self.execute(client)
        sequence = [event["event"] for event in events]
        self.assertLess(sequence.index("hosted_start"), sequence.index("hosted_stopping"))
        self.assertLess(sequence.index("hosted_stopping"), sequence.index("hosted_finished"))
        self.assert_schedule(events, started_at=1792159353.0, budget=28800, reserve=180, parallel=3)
        finished = next(event for event in events if event["event"] == "hosted_finished")
        self.assertEqual(finished["exit_reason"], "all_challenges_completed")
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(client.calls, ["check_vpn", "list_challenges"])
        self.assertEqual(len(instances), 1)

    async def test_resume_logs_old_schedule_and_work_deadline_exit_without_extending_run(self):
        started_at = 1792159353.0
        saved_run(self.directory, started_at=started_at)
        self.env["XINGHUO_RUN_MODE"] = "resume"
        client = OfflineClient(completed=False)
        events, _ = await self.execute(client, now=started_at + 815)
        self.assert_schedule(events, started_at=started_at, budget=900, reserve=90, parallel=1)
        for event in events:
            if event["event"] in {"hosted_start", "hosted_stopping", "hosted_finished"}:
                self.assertEqual(event["run_mode"], "resume")
                self.assertEqual(event["budget_source"], "saved_config")
        finished = next(event for event in events if event["event"] == "hosted_finished")
        self.assertEqual(finished["exit_reason"], "work_deadline_reached")
        self.assertEqual(finished["status"], "deadline")

    async def test_short_test_overrides_are_reported_explicitly(self):
        self.env.update(XINGHUO_BUDGET_SECONDS="900", XINGHUO_PARALLEL="1")
        events, _ = await self.execute(OfflineClient())
        warnings = [event for event in events if event["event"] == "hosted_configuration_warning"]
        self.assertEqual(len(warnings), 1)
        self.assertEqual((warnings[0]["budget_seconds"], warnings[0]["parallel"]), (900, 1))
        self.assert_schedule(events, started_at=1792159353.0, budget=900, reserve=90, parallel=1)
        start = next(event for event in events if event["event"] == "hosted_start")
        self.assertEqual((start["budget_source"], start["parallel_source"]), ("environment", "environment"))

    async def test_new_existing_directory_is_rejected_before_client_or_model_setup(self):
        saved_run(self.directory)
        sdk = SimpleNamespace(TSecBenchmarkAsync=lambda **_: self.fail("client created for rejected new run"))
        with patch.dict(os.environ, self.env, clear=True), \
             patch.dict(sys.modules, {"tsec_benchmark": sdk}), \
             patch.object(hosted, "offline_check", return_value={"ok": True}), \
             patch.object(hosted, "configure_model_environment") as model_setup:
            with self.assertRaises(ValueError):
                await hosted.run(work_root=self.root)
        model_setup.assert_not_called()

    async def test_explicit_stop_has_distinct_exit_reason(self):
        events, _ = await self.execute(OfflineClient(completed=False), stop=True)
        finished = next(event for event in events if event["event"] == "hosted_finished")
        self.assertEqual(finished["exit_reason"], "stop_requested")
        self.assertEqual(finished["status"], "stopped")

    async def test_vpn_failure_is_closed_and_reported_without_fabricated_deadline_or_secrets(self):
        client = OfflineClient(vpn_error=RuntimeError("response " + " ".join(self.secrets)))
        events, instances = await self.execute(client, expected_error=True)
        error = next(event for event in events if event["event"] == "hosted_error")
        self.assertTrue(error.get("exit_reason"))
        self.assertIsNone(error.get("deadline"))
        self.assertNotIn("hosted_finished", [event["event"] for event in events])
        self.assertEqual(client.calls, ["check_vpn"])

    async def test_runtime_failure_logs_stopping_error_and_preserves_real_deadline(self):
        client = OfflineClient(list_error=RuntimeError("response " + " ".join(self.secrets)))
        events, _ = await self.execute(client, expected_error=True)
        stopping = next(event for event in events if event["event"] == "hosted_stopping")
        self.assertEqual(stopping["exit_reason"], "runtime_error")
        self.assertEqual(stopping["status"], "error")
        error = next(event for event in events if event["event"] == "hosted_error")
        self.assertEqual(error["deadline"], 1792159353.0 + 28800)
        self.assertEqual(error["exit_reason"], "runtime_error")

    async def test_client_close_failure_reports_runtime_error_before_any_finished_event(self):
        client = OfflineClient(close_error=RuntimeError("close response " + " ".join(self.secrets)))
        events, _ = await self.execute(client, expected_error=True)
        errors = [event for event in events if event["event"] == "hosted_error"]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["stage"], "client_close")
        self.assertEqual(errors[0]["exit_reason"], "runtime_error")
        self.assertEqual(errors[0]["deadline"], 1792159353.0 + 28800)
        self.assertNotIn("hosted_finished", [event["event"] for event in events])
        self.assertNotIn("hosted_cleanup_warning", [event["event"] for event in events])

    async def test_client_close_failure_preserves_primary_startup_error_and_emits_one_warning(self):
        client = OfflineClient(vpn_error=ValueError("vpn response " + " ".join(self.secrets)),
                               close_error=RuntimeError("close response " + " ".join(self.secrets)))
        events, _ = await self.execute(client, expected_error=True)
        errors = [event for event in events if event["event"] == "hosted_error"]
        warnings = [event for event in events if event["event"] == "hosted_cleanup_warning"]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["stage"], "vpn_check")
        self.assertEqual(errors[0]["exit_reason"], "startup_error")
        self.assertEqual(errors[0]["error_type"], "ValueError")
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["stage"], "client_close")
        self.assertEqual(warnings[0]["error_type"], "RuntimeError")
        self.assertNotIn("hosted_finished", [event["event"] for event in events])

    async def test_client_close_timeout_cancels_close_and_has_no_false_finished_event(self):
        client = OfflineClient(close_block=True)
        make_config = hosted.hosted_config
        with patch.object(hosted, "hosted_config",
                          side_effect=lambda env: replace(make_config(env), operation_timeout=.02)):
            events, _ = await self.execute(client, expected_error=True)
        error = next(event for event in events if event["event"] == "hosted_error")
        self.assertEqual(error["stage"], "client_close")
        self.assertEqual(error["exit_reason"], "runtime_error")
        self.assertEqual(error["error_type"], "TimeoutError")
        self.assertTrue(client.close_cancelled)
        self.assertNotIn("hosted_finished", [event["event"] for event in events])

