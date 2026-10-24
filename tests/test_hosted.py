"""Deployment contracts without model calls or network."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from xinghuo.config import Config
from xinghuo.hosted import configure_model_environment, hosted_config
from xinghuo.preflight import check_api
from xinghuo.veria.hosted_environment import HostedProcessEnvironment
from xinghuo.veria.prompts import build_solver_prompt


class HostedConfigTests(unittest.TestCase):
    def test_gateway_applies_to_strategy_and_all_sdk_default_models_without_saving_secret(self):
        env = {"DEEPSEEK_API_KEY": "synthetic-secret", "DEEPSEEK_BASE_URL": "http://api.deepseek.com.tsecbench.gw/anthropic"}
        report = configure_model_environment(env)
        self.assertEqual(env["ANTHROPIC_BASE_URL"], env["DEEPSEEK_BASE_URL"])
        self.assertEqual(env["ANTHROPIC_DEFAULT_HAIKU_MODEL"], "deepseek-flash")
        self.assertNotIn("synthetic-secret", json.dumps(report))
        config = hosted_config(env)
        self.assertEqual(config.veria_environment, "hosted")
        self.assertEqual(config.budget, 28800)
        self.assertNotIn("synthetic-secret", json.dumps(config.to_dict()))
        self.assertEqual(hosted_config({"XINGHUO_BUDGET_SECONDS": "28800"}).budget, 28800)

    def test_no_public_provider_fallback_in_hosted_mode(self):
        for base in ("https://api.deepseek.com/anthropic", "http://evil.invalid/anthropic",
                     "http://api.deepseek.com.tsecbench.gw/anthropic?token=hidden", ""):
            with self.assertRaises(ValueError):
                configure_model_environment({"DEEPSEEK_API_KEY": "fixture", "DEEPSEEK_BASE_URL": base})
        env = {"XINGHUO_DEPLOYMENT": "local", "DEEPSEEK_API_KEY": "fixture",
               "DEEPSEEK_BASE_URL": "https://api.deepseek.com/anthropic"}
        self.assertEqual(configure_model_environment(env)["deployment"], "local")

    def test_original_default_and_observed_hosted_paths(self):
        self.assertEqual(Config().veria_environment, "docker")
        prompt = build_solver_prompt("pro-1", {}, {"execution_mode": "hosted_process",
            "paths": {"members": "/work/run/teams/A/members", "inputs": "/work/run/teams/A/inputs", "shared": "/work/run/teams/A/shared"}})
        self.assertIn("/work/run/teams/A/members/pro-1/work", prompt)
        self.assertNotIn("/challenge/", prompt)
        self.assertIn("share kernel/network/resource", prompt)


class HostedEnvironmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_refuses_unprepared_host_and_wrong_work_root(self):
        with tempfile.TemporaryDirectory() as temp:
            env = HostedProcessEnvironment(temp, "run", "A", "attempt", {})
            with patch("xinghuo.veria.hosted_environment.sys.platform", "darwin"), \
                 self.assertRaisesRegex(RuntimeError, "prepared Linux"):
                await env.start()
            root = Path(temp)
            marker, helper = root / "marker.json", root / "helper.py"
            marker.write_text("{}")
            helper.write_text("# fixture")
            with patch("xinghuo.veria.hosted_environment.sys.platform", "linux"), \
                 patch.object(HostedProcessEnvironment, "marker", marker), \
                 patch.object(HostedProcessEnvironment, "helper", helper), \
                 patch.object(HostedProcessEnvironment, "work_root", root / "allowed"), \
                 self.assertRaisesRegex(RuntimeError, "under /work"):
                await env.start()

    async def test_environment_adapter_uses_helper_without_nested_docker_and_rejects_changed_image(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            marker, helper = root / "image.json", root / "helper.py"
            marker.write_text('{"build":1}')
            helper.write_text("# fixture")
            calls = []
            async def runner(argv, timeout=0):
                calls.append(argv)
                return {"returncode": 0, "stdout": json.dumps({"paths": {}}) if "describe" in argv else "", "stderr": ""}
            with patch("xinghuo.veria.hosted_environment.sys.platform", "linux"), \
                 patch.object(HostedProcessEnvironment, "marker", marker), \
                 patch.object(HostedProcessEnvironment, "helper", helper), \
                 patch.object(HostedProcessEnvironment, "work_root", root):
                env = HostedProcessEnvironment(root / "attempt", "run", "A", "attempt", {}, runner=runner)
                result = await env.start()
                self.assertIsNone(result["container_id"])
                command = await env.prepare_command("pro-1", "echo ok", "unique")
                self.assertNotIn("docker", command)
                self.assertIn("XINGHUO_TOOL_ROOT=", command)
                env.quiesce(True)
                with self.assertRaisesRegex(RuntimeError, "paused"):
                    await env.prepare_command("pro-2", "echo no")
                env.quiesce(False)
                await env.stop()
                self.assertEqual(env.commands, {})
                self.assertTrue(env.command_archive_path.is_file())
                marker.write_text('{"build":2}')
                resumed = HostedProcessEnvironment(root / "attempt", "run", "A", "attempt", {}, runner=runner)
                with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
                    await resumed.start()


class ApiPreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_credentials_never_construct_client(self):
        def factory(**kwargs):
            self.fail("API client constructed without credentials")
        result = await check_api(environ={}, client_factory=factory)
        self.assertFalse(result["ok"])
        self.assertEqual(result["platform_mutations"], 0)

    async def test_real_sdk_vpn_then_list_only_and_full_lifecycle_remains_available(self):
        try:
            import httpx
            from tsec_benchmark import TSecBenchmarkAsync
        except ImportError:
            self.skipTest("benchmark optional dependency missing")
        calls = []
        def handler(request):
            calls.append((request.method, request.url.host, request.url.path))
            if request.url.host == "10.0.100.58":
                return httpx.Response(200, json={"status": "ok", "client_ip": "fixture", "time": "fixture"})
            return httpx.Response(200, json=[{"unique_code": "offline", "description": "fixture",
                "difficulty": "normal", "level": 1, "total_score": 20, "flag_count": 2,
                "correct_flag_count": 1, "is_completed": False, "container_status": "stopped", "container_addr": []}])
        def factory(**kwargs):
            return TSecBenchmarkAsync(**kwargs, transport=httpx.MockTransport(handler))
        result = await check_api(environ={"BENCHMARK_BASE_URL": "https://benchmark.invalid", "BENCHMARK_TOKEN": "fixture"}, client_factory=factory)
        self.assertTrue(result["ok"])
        self.assertEqual(result["challenges"][0]["correct_flag_count"], 1)
        self.assertEqual(calls, [("GET", "10.0.100.58", "/"), ("GET", "benchmark.invalid", "/openapi/v1/challenges")])

    async def test_api_errors_are_redacted_and_closed(self):
        class Client:
            closed = False
            async def check_vpn(self):
                raise RuntimeError("secret-from-response")
            async def aclose(self):
                self.closed = True
        client = Client()
        result = await check_api(environ={"BENCHMARK_BASE_URL": "https://benchmark.invalid", "BENCHMARK_TOKEN": "fixture"}, client_factory=lambda **_: client)
        self.assertNotIn("secret-from-response", json.dumps(result))
        self.assertTrue(client.closed)
