import asyncio
import importlib.util
import io
import json
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import patch

from xinghuo.veria.driver import SpecialCommandError, parse_special_command
from xinghuo.veria.environment import EnvironmentError, SharedDockerEnvironment


class SpecialCommandsTests(unittest.TestCase):
    def test_quoted_candidate_preserves_internal_content(self):
        self.assertEqual(parse_special_command("submit_flag ' Flag{a ; $b \'\' c} '"),
                         ("submit_flag", " Flag{a ; $b  c} "))
        self.assertEqual(parse_special_command('notify_coordinator "two words"'), ("notify_coordinator", "two words"))
        self.assertIsNone(parse_special_command("python solve.py > result.txt"))

    def test_mixed_commands_and_substitution_are_rejected(self):
        for command in ["submit_flag flag; echo bad", "echo begin && submit_flag flag", "submit_flag flag | cat",
                        "submit_flag flag > out", "submit_flag flag\necho more", "env submit_flag flag",
                        'submit_flag "$(cat flag.txt)"', "submit_flag 'unterminated", "submit_flag one two"]:
            with self.subTest(command=command), self.assertRaises(SpecialCommandError):
                parse_special_command(command)


class FakeDocker:
    def __init__(self):
        self.calls = []
        self.containers = {}
        self.running = 0
        self.max_running = 0
        self.image_id = "sha256:fixture-image"

    async def __call__(self, argv, timeout=120):
        self.calls.append(argv)
        if argv[:2] == ["docker", "inspect"]:
            value = self.containers.get(argv[2])
            if argv[2] in ("image:test", self.image_id):
                value = {"Id": self.image_id}
            return {"returncode": 0 if value else 1, "stdout": json.dumps([value]) if value else "",
                    "stderr": "" if value else "Error: No such object"}
        if argv[:2] == ["docker", "run"]:
            labels = dict(arg.split("=", 1) for index, arg in enumerate(argv) if index and argv[index - 1] == "--label")
            value = {"Id": "container-fixture", "Image": self.image_id,
                     "Config": {"Labels": labels}, "State": {"Running": True}}
            self.containers[value["Id"]] = value
            self.containers[argv[argv.index("--name") + 1]] = value
            return {"returncode": 0, "stdout": value["Id"] + "\n", "stderr": ""}
        if "describe" in argv:
            return {"returncode": 0, "stdout": json.dumps({"architecture": "fixture", "python": "3.14", "tools": {"python3": "/usr/bin/python3"}}), "stderr": ""}
        if argv[:2] == ["docker", "exec"] and "run" in argv:
            self.running += 1
            self.max_running = max(self.running, self.max_running)
            await asyncio.sleep(.02)
            self.running -= 1
        return {"returncode": 0, "stdout": "ok", "stderr": ""}


class EnvironmentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.docker = FakeDocker()
        self.settings = {"veria_tool_image": "image:test", "veria_tool_cpu_limit": 2, "veria_tool_memory_limit": "2g"}
        self.env = SharedDockerEnvironment(self.temp.name, "run", "question", "attempt", self.settings, runner=self.docker)

    async def test_parallel_members_have_independent_commands(self):
        await self.env.start()
        results = await asyncio.gather(self.env.execute("pro-1", "sleep .01"), self.env.execute("pro-2", "sleep .01"))
        self.assertEqual(self.docker.max_running, 2)
        self.assertNotEqual(results[0]["command_id"], results[1]["command_id"])
        self.assertEqual({entry["agent_id"] for entry in self.env.commands.values()}, {"pro-1", "pro-2"})
        await self.env.stop_member("pro-1")
        self.assertFalse(any(call[:2] == ["docker", "rm"] for call in self.docker.calls))
        self.assertTrue((Path(self.temp.name) / "members").is_dir())

    async def test_resume_checks_image_generation_and_cleans_old_descendants(self):
        await self.env.start()
        await self.env.prepare_command("pro-1", "sleep 100", "command-1")
        resumed = SharedDockerEnvironment(self.temp.name, "run", "question", "attempt", self.settings, runner=self.docker)
        result = await resumed.start()
        self.assertTrue(result["recovered"])
        self.assertTrue(result["volatile_state_lost"])
        self.assertEqual(result["retired_execution_count"], 1)
        archived = [json.loads(line) for line in resumed.command_archive_path.read_text().splitlines()]
        self.assertNotIn(resumed.command_id("pro-1", "command-1"), resumed.commands)
        self.assertEqual(archived[-1]["status"], "cleaned")
        self.assertTrue(any("cleanup" in call for call in self.docker.calls))
        self.docker.containers[resumed.container_id]["Config"]["Labels"]["xinghuo.attempt"] = "different"
        with self.assertRaises(EnvironmentError):
            await resumed.stop()

    async def test_quiesce_is_reversible_and_retains_container(self):
        await self.env.start()
        self.env.quiesce(True)
        self.assertFalse(json.loads((Path(self.temp.name) / "control/dispatch.json").read_text())["allowed"])
        with self.assertRaises(EnvironmentError):
            await self.env.prepare_command("pro-1", "true")
        self.env.quiesce(False)
        self.assertTrue(json.loads((Path(self.temp.name) / "control/dispatch.json").read_text())["allowed"])
        rewritten = await self.env.prepare_command("pro-1", "printf '%s' 'a;b'")
        self.assertEqual(shlex.split(rewritten)[-1], "printf '%s' 'a;b'")
        await self.env.stop()
        self.assertFalse((await self.env.health())["healthy"])

    async def test_missing_container_rebuild_records_volatile_loss(self):
        await self.env.start()
        self.docker.containers.clear()
        resumed = SharedDockerEnvironment(self.temp.name, "run", "question", "attempt", self.settings, runner=self.docker)
        result = await resumed.start()
        self.assertTrue(result["volatile_state_lost"])
        self.assertFalse(result["recovered"])

    async def test_failed_inspect_does_not_claim_container_missing(self):
        await self.env.start()
        async def disconnected(argv, timeout=120):
            return {"returncode": 1, "stdout": "", "stderr": "Cannot connect to Docker daemon"}
        self.env.runner = disconnected
        with self.assertRaises(EnvironmentError):
            await self.env.health()

    async def test_created_container_recovered_when_crash_preceded_id_save(self):
        await self.env.start()
        record = json.loads(self.env.state_path.read_text())
        record["container_id"] = ""
        self.env.state_path.write_text(json.dumps(record))
        resumed = SharedDockerEnvironment(self.temp.name, "run", "question", "attempt", self.settings, runner=self.docker)
        result = await resumed.start()
        self.assertTrue(result["recovered"])
        self.assertEqual(sum(call[:2] == ["docker", "run"] for call in self.docker.calls), 1)

    async def test_only_cleaned_commands_leave_active_index_and_history_is_append_only(self):
        await self.env.start()
        await self.env.prepare_command("pro-1", "background-one &", "one")
        await self.env.prepare_command("pro-2", "background-two &", "two")
        await self.env.prepare_command("pro-3", "still-registering", "three")
        self.env.complete_command("one")
        self.env.complete_command("two")
        await self.env.stop_member("pro-1")
        first = self.env.command_archive_path.read_bytes()
        archived = json.loads(first)
        self.assertEqual(archived["agent_id"], "pro-1")
        self.assertEqual(archived["status"], "cleaned")
        self.assertTrue(Path(archived["dispatch_record"]).is_file())
        self.assertEqual({entry["status"] for entry in self.env.commands.values()}, {"shell_returned", "registered"})
        snapshot = json.loads(self.env.state_path.read_text())
        self.assertEqual(len(snapshot["commands"]), 2)
        self.assertEqual(snapshot["commands_archive"]["path"], str(self.env.command_archive_path))
        await self.env.stop_member("pro-2")
        self.assertTrue(self.env.command_archive_path.read_bytes().startswith(first))
        self.assertEqual(len(self.env.command_archive_path.read_text().splitlines()), 2)
        self.assertEqual(len(self.env.commands), 1)
        await self.env.stop()
        self.assertFalse(self.env.commands)
        self.assertEqual(len(self.env.command_archive_path.read_text().splitlines()), 3)

    async def test_cleanup_failure_keeps_ownership_in_active_index(self):
        await self.env.start()
        await self.env.prepare_command("pro-1", "background &", "live")
        self.env.complete_command("live")
        async def failed_cleanup(argv, timeout=120):
            if "cleanup" in argv:
                return {"returncode": 1, "stdout": "", "stderr": "fixture cleanup failed"}
            return await self.docker(argv, timeout=timeout)
        self.env.runner = failed_cleanup
        with self.assertRaises(EnvironmentError):
            await self.env.stop_member("pro-1")
        self.assertEqual(len(self.env.commands), 1)
        self.assertEqual(next(iter(self.env.commands.values()))["status"], "shell_returned")
        self.assertFalse(self.env.command_archive_path.exists())

    async def test_legacy_cleaned_entries_are_archived_during_recovery(self):
        await self.env.start()
        await self.env.prepare_command("pro-1", "already-cleaned", "old")
        identifier = self.env.command_id("pro-1", "old")
        snapshot = json.loads(self.env.state_path.read_text())
        snapshot["commands"][identifier]["status"] = "cleaned"
        self.env.state_path.write_text(json.dumps(snapshot))
        recovered = SharedDockerEnvironment(self.temp.name, "run", "question", "attempt", self.settings, runner=self.docker)
        await recovered.start()
        self.assertFalse(recovered.commands)
        archived = json.loads(recovered.command_archive_path.read_text())
        self.assertEqual(archived["command_id"], identifier)
        self.assertEqual(archived["archive_id"], f"{recovered.generation}:{identifier}")


class ContainerDispatchGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        path = Path(__file__).resolve().parents[1] / "scripts/veria_container_exec.py"
        spec = importlib.util.spec_from_file_location("veria_container_gate_test", path)
        self.helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.helper)
        self.helper.CONTROL_DIR = Path(self.temp.name)

    def write(self, name, value):
        (Path(self.temp.name) / name).write_text(json.dumps(value))

    def test_missing_gate_wrong_generation_and_retired_command_fail_closed(self):
        self.assertFalse(self.helper.dispatch_allowed("pro-1", "gen", "cmd"))
        self.write("dispatch.json", {"allowed": True, "generation": "gen"})
        permit = {"allowed": True, "generation": "gen", "member": "pro-1", "command_id": "cmd"}
        self.write("dispatch-cmd.json", permit)
        self.assertTrue(self.helper.dispatch_allowed("pro-1", "gen", "cmd"))
        self.assertFalse(self.helper.dispatch_allowed("pro-2", "gen", "cmd"))
        self.assertFalse(self.helper.dispatch_allowed("pro-1", "other", "cmd"))
        self.write("dispatch.json", {"allowed": False, "generation": "gen"})
        self.assertFalse(self.helper.dispatch_allowed("pro-1", "gen", "cmd"))
        self.write("dispatch.json", {"allowed": True, "generation": "gen"})
        self.assertTrue(self.helper.dispatch_allowed("pro-1", "gen", "cmd"))
        self.write("dispatch-cmd.json", {**permit, "allowed": False})
        self.assertFalse(self.helper.dispatch_allowed("pro-1", "gen", "cmd"))

    def test_closed_gate_never_invokes_host_command_or_environment_prepare(self):
        argv = ["helper", "run", "--member", "pro-1", "--generation", "gen", "--command-id", "cmd", "--", "must-never-execute"]
        with patch.object(self.helper.sys, "argv", argv), patch.object(self.helper.sys, "stderr", io.StringIO()), patch.object(self.helper, "prepare") as prepare, patch.object(self.helper.subprocess, "Popen") as popen:
            self.assertEqual(self.helper.main(), 75)
        prepare.assert_not_called()
        popen.assert_not_called()

    def test_gate_is_rechecked_after_member_preparation(self):
        self.write("dispatch.json", {"allowed": True, "generation": "gen"})
        self.write("dispatch-cmd.json", {"allowed": True, "generation": "gen", "member": "pro-1", "command_id": "cmd"})
        def prepare(_):
            self.write("dispatch.json", {"allowed": False, "generation": "gen"})
            return Path(self.temp.name)
        argv = ["helper", "run", "--member", "pro-1", "--generation", "gen", "--command-id", "cmd", "--", "must-never-execute"]
        with patch.object(self.helper.sys, "argv", argv), patch.object(self.helper.sys, "stderr", io.StringIO()), patch.object(self.helper, "prepare", side_effect=prepare), patch.object(self.helper.subprocess, "Popen") as popen:
            self.assertEqual(self.helper.main(), 75)
        popen.assert_not_called()
