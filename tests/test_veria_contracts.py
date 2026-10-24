"""Cross-layer regression contracts. All SDK and Docker I/O is synthetic."""
import asyncio
import json
from pathlib import Path
import shlex
import tempfile
from types import SimpleNamespace
import unittest

from xinghuo.veria.driver import VeriaRoleDriver
from xinghuo.veria.environment import EnvironmentError, SharedDockerEnvironment
from xinghuo.veria_runtime import parse_object

from test_veria_tools import FakeDocker


class VeriaCrossLayerContracts(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.docker = FakeDocker()
        self.settings = {"veria_tool_image": "image:test", "veria_tool_cpu_limit": 2,
                         "veria_tool_memory_limit": "2g"}

    def environment(self):
        return SharedDockerEnvironment(self.directory, "run", "question", "attempt",
                                       self.settings, runner=self.docker)

    async def test_final_report_json_is_not_prefixed_by_intermediate_assistant_text(self):
        final = json.dumps({"overview": "checked evidence", "recent_changes": []})
        messages = [
            type("AssistantMessage", (), {"model": "deepseek-flash", "error": None,
                "content": [SimpleNamespace(text="I will inspect the new evidence first.")]})(),
            type("AssistantMessage", (), {"model": "deepseek-flash", "error": None,
                "content": [SimpleNamespace(text=final)]})(),
            type("ResultMessage", (), {"session_id": "report-session", "result": final,
                "is_error": False, "structured_output": None})(),
        ]
        class Stream:
            async def receive_messages(self):
                for message in messages:
                    yield message
                await asyncio.Event().wait()
        observed = []
        driver = VeriaRoleDriver("reporter", "reporter", "deepseek-flash", self.directory,
            {}, None, None, on_event=lambda kind, data: observed.append((kind, data)))
        driver.client = Stream()
        driver._active = asyncio.get_running_loop().create_future()
        reader = asyncio.create_task(driver._receive())
        try:
            result = await asyncio.wait_for(asyncio.shield(driver._active), 1)
            self.assertEqual(parse_object(result["result"])["overview"], "checked evidence")
            self.assertTrue(any(kind == "assistant_text" and "inspect" in data["text"]
                                for kind, data in observed))
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def test_recovery_discloses_retired_background_services(self):
        original = self.environment()
        await original.start()
        await original.prepare_command("pro-1", "python server.py &", "server-command")
        original.complete_command("server-command")
        recovered = self.environment()
        result = await recovered.start()
        self.assertTrue(result["recovered"])
        self.assertFalse(recovered.commands)
        archived = json.loads(recovered.command_archive_path.read_text())
        self.assertEqual(archived["command_id"], recovered.command_id("pro-1", "server-command"))
        self.assertEqual(archived["status"], "cleaned")
        self.assertTrue(result["volatile_state_lost"],
                        "Retiring registered background services loses volatile state even when the container survives")

    async def test_quiesce_during_member_preparation_prevents_new_command_registration(self):
        environment = self.environment()
        await environment.start()
        preparing, release = asyncio.Event(), asyncio.Event()
        async def delayed_prepare(_):
            preparing.set()
            await release.wait()
        environment.ensure_member = delayed_prepare
        pending = asyncio.create_task(environment.prepare_command("pro-1", "mutate-shared-state", "late-command"))
        await preparing.wait()
        environment.quiesce(True)
        release.set()
        with self.assertRaises(EnvironmentError):
            await pending
        self.assertNotIn("late-command", environment.commands)

    async def test_same_sdk_tool_id_in_distinct_members_does_not_overwrite_ownership(self):
        environment = self.environment()
        await environment.start()
        first = shlex.split(await environment.prepare_command("pro-1", "sleep 20", "same-provider-tool-id"))
        second = shlex.split(await environment.prepare_command("pro-2", "sleep 20", "same-provider-tool-id"))
        self.assertEqual(len(environment.commands), 2)
        self.assertEqual({entry["agent_id"] for entry in environment.commands.values()}, {"pro-1", "pro-2"})
        self.assertNotEqual(first[first.index("--command-id") + 1], second[second.index("--command-id") + 1])


if __name__ == "__main__":
    unittest.main()
