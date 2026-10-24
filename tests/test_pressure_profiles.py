"""Offline acceptance must exercise the selected release's service contract."""
from contextlib import contextmanager, redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pressure = load_script("verify_mixed4_pressure")
runner = load_script("run_mixed4_pressure")
prompt_audit = load_script("audit_veria_prompts")


class PressureProfileTests(unittest.TestCase):
    def test_flash_release_needs_only_deepseek_endpoint(self):
        keys = []
        @contextmanager
        def fake_endpoint(key):
            keys.append(key)
            yield "http://127.0.0.1:1234", []
        with patch.object(pressure, "endpoint", fake_endpoint):
            with pressure.pressure_endpoints("flash4") as (env, wire):
                self.assertEqual(keys, ["synthetic-deepseek"])
                self.assertEqual(set(wire), {"deepseek"})
                self.assertNotIn("GLM_API_KEY", env)
                self.assertNotIn("GLM_BASE_URL", env)
                self.assertEqual(env["XINGHUO_TEAM_PROFILE"], "flash4")
        contract = pressure.pressure_contract("flash4")
        self.assertEqual(len(contract["solver_ids"]), 4)
        self.assertEqual(len(contract["models"]), 7)
        self.assertEqual(set(contract["models"].values()), {"deepseek-flash"})
        self.assertEqual(set(contract["providers"].values()), {"deepseek"})

    def test_explicit_mixed_profile_retains_separate_authenticated_services(self):
        with pressure.pressure_endpoints("mixed4") as (env, wire):
            self.assertEqual(set(wire), {"deepseek", "glm"})
            self.assertNotEqual(env["DEEPSEEK_BASE_URL"], env["GLM_BASE_URL"])
            self.assertNotEqual(env["DEEPSEEK_API_KEY"], env["GLM_API_KEY"])
        self.assertEqual(pressure.pressure_contract("mixed4")["providers"]["solver-3"], "glm")

    def test_flash_acceptance_rejects_accidental_model_or_provider_fallback(self):
        for field, value in (("models", "deepseek-v4-pro"), ("providers", "glm")):
            contract = {"models": {"solver-1": "deepseek-flash"},
                        "providers": {"solver-1": "deepseek"}}
            contract[field]["solver-1"] = value
            with self.subTest(field=field), patch.object(pressure, "profile_contract", return_value=contract):
                with self.assertRaises(RuntimeError):
                    pressure.pressure_contract("flash4")

    def test_loopback_fixture_checks_service_key_and_captures_flash_wire_model(self):
        with pressure.endpoint("synthetic-deepseek") as (base, requests):
            data = json.dumps({"model": "deepseek-flash", "tools": []}).encode()
            request = Request(base + "/v1/messages", data=data,
                              headers={"Content-Type": "application/json", "x-api-key": "wrong"})
            with self.assertRaises(HTTPError) as caught:
                urlopen(request, timeout=5)
            self.assertEqual(caught.exception.code, 401)
            caught.exception.close()
            request.headers["X-api-key"] = "synthetic-deepseek"
            with urlopen(request, timeout=5) as response:
                self.assertIn(b"text/event-stream", response.headers["Content-Type"].encode())
                self.assertIn(b"deepseek-flash", response.read())
            self.assertEqual([item["auth_ok"] for item in requests], [False, True])
            self.assertFalse(any(item["rotation_policy_present"] for item in requests))
            for system in (pressure.ROTATION_MARKER + "\n900 seconds", [
                    {"type": "text", "text": "SDK preamble"},
                    {"type": "text", "text": pressure.ROTATION_MARKER + "\n900 seconds",
                     "cache_control": {"type": "ephemeral"}}]):
                request.data = json.dumps({"model": "deepseek-flash", "tools": [], "system": system}).encode()
                with urlopen(request, timeout=5) as response:
                    response.read()
            self.assertTrue(all(item["rotation_policy_present"] for item in requests[2:]))

    def test_prompt_audit_exports_actual_flash_policy_for_every_role_and_continuation(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            manifest = prompt_audit.export(output)
            flash = [item for item in manifest["scenarios"] if item["name"].startswith("flash4-hard900-")]
            self.assertEqual(len(flash), 7)
            self.assertEqual({item["role"] for item in flash}, {"solver", "coordinator", "reporter", "reviewer"})
            self.assertTrue(all(item["rotation_policy"] == pressure.ROTATION_POLICY
                                and item["rotation_policy_present"] for item in flash))
            continuations = json.loads((output / "continuations.json").read_text())["documents"]
            flash_continuations = [value for name, value in continuations.items()
                                   if name.startswith("flash4_hard900_")]
            self.assertEqual(len(flash_continuations), 6)
            self.assertTrue(all(pressure.ROTATION_MARKER in item["system_prompt"] for item in flash_continuations))

    def test_runner_passes_default_flash_profile_to_owned_offline_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "pressure.json"
            calls = []
            def check_output(command, **kwargs):
                if command[1] == "info":
                    return json.dumps({"MemTotal": 20 * 1024**3, "NCPU": 8})
                if command[1] == "image":
                    return "sha256:test\n"
                return json.dumps([{"State": {"ExitCode": 0, "OOMKilled": False}}])
            def run(command, **kwargs):
                calls.append(command)
                if command[1] == "cp":
                    output.write_text(json.dumps({"status": "passed"}))
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            with patch.object(runner.subprocess, "check_output", side_effect=check_output), \
                    patch.object(runner.subprocess, "run", side_effect=run), \
                    patch("sys.argv", ["pressure", "--image", "test-image", "--output", str(output)]), \
                    redirect_stdout(io.StringIO()):
                self.assertEqual(runner.main(), 0)
            command = next(item for item in calls if item[1] == "run")
            self.assertEqual(command[command.index("--profile") + 1], "flash4")
            self.assertEqual(command[command.index("--network") + 1], "none")
            self.assertEqual(command[command.index("--memory") + 1], "16g")
            self.assertNotIn("--env", command)
            self.assertNotIn("--env-file", command)
            self.assertEqual(json.loads(output.read_text())["requested_profile"], "flash4")


if __name__ == "__main__":
    unittest.main()
