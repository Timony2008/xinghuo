import argparse
import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from xinghuo.agent_runner import ROLE_TOOLS, TOOL_SPECS, Runner
from xinghuo.sdk_driver import AgentProcess


class DriverTelemetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_observation_does_not_consume_or_change_turn_result(self):
        driver = AgentProcess("worker", "worker-test", Path.cwd())
        reader = asyncio.StreamReader()
        observation = {
            "type": "tool_observation", "phase": "completed", "tool_use_id": "use-1",
            "tool_name": "Bash", "input_fingerprint": "a" * 64,
            "result_fingerprint": "b" * 64, "is_error": True,
            "raw_input": "this field must be discarded",
        }
        result = {"type": "turn_result", "id": "turn-1", "result": "done", "session_id": "session-1", "is_error": False}
        for record in (observation, result):
            reader.feed_data((json.dumps(record) + "\n").encode())
        reader.feed_eof()

        async def wait():
            return 0

        driver.process = SimpleNamespace(stdout=reader, wait=wait)
        driver._closing = True
        future = asyncio.get_running_loop().create_future()
        driver._pending["turn-1"] = future
        await driver._read_stdout()
        self.assertEqual(await future, result)
        self.assertEqual(driver.session_id, "session-1")
        observations = driver.poll_observations()
        self.assertEqual(len(observations), 1)
        self.assertNotIn("raw_input", observations[0])
        self.assertEqual(observations[0]["result_fingerprint"], "b" * 64)
        self.assertEqual(driver.poll_observations(), [])

    @unittest.skipUnless(importlib.util.find_spec("claude_agent_sdk"), "optional real SDK package unavailable")
    async def test_sdk_blocks_produce_only_hashes_and_correlate_tool_results(self):
        from claude_agent_sdk import AssistantMessage, ToolResultBlock, ToolUseBlock, UserMessage

        runner = Runner(argparse.Namespace(role="worker", agent_id="worker-test", session_id=None))
        emitted = []

        async def emit(record):
            emitted.append(record)

        runner.emit = emit
        secret = "secret-raw-command-do-not-record"
        messages = [
            AssistantMessage(content=[ToolUseBlock("use-1", "Bash", {"command": secret, "cwd": "/tmp"})], model="fake-model"),
            UserMessage(content=[ToolResultBlock("use-1", "private-result-body", True)]),
            AssistantMessage(content=[ToolUseBlock("use-2", "Bash", {"cwd": "/tmp", "command": secret})], model="fake-model"),
            UserMessage(content=[ToolResultBlock("use-2", "private-result-body", True)]),
            AssistantMessage(content=[ToolUseBlock("mcp-use", "mcp__xinghuo__report_progress", {"summary": secret})], model="fake-model"),
            UserMessage(content=[ToolResultBlock("mcp-use", secret, False)]),
            AssistantMessage(content=[ToolUseBlock("evidence-use", "mcp__xinghuo__read_evidence", {"path": "/private/evidence.txt"})], model="fake-model"),
            UserMessage(content=[ToolResultBlock("evidence-use", secret, False)]),
        ]

        class FakeClient:
            async def receive_messages(self):
                for message in messages:
                    yield message

        runner.client = FakeClient()
        await runner.receive_sdk()
        self.assertEqual(len(emitted), 4)
        self.assertEqual([event["phase"] for event in emitted], ["started", "completed", "started", "completed"])
        self.assertEqual(emitted[0]["input_fingerprint"], emitted[2]["input_fingerprint"])
        self.assertEqual(emitted[1]["result_fingerprint"], emitted[3]["result_fingerprint"])
        self.assertEqual(emitted[1]["tool_use_id"], "use-1")
        self.assertTrue(emitted[1]["is_error"])
        serialized = json.dumps(emitted)
        self.assertNotIn(secret, serialized)
        self.assertNotIn("private-result-body", serialized)
        self.assertNotIn("mcp__xinghuo", serialized)

    async def test_strategy_routes_only_read_and_search_evidence_to_parent(self):
        runner = Runner(argparse.Namespace(role="strategy", agent_id="strategy-test", session_id=None, backend="claude"))
        emitted = []

        async def emit(record):
            emitted.append(record)
            if record["type"] == "tool_call":
                runner.pending_tools[record["id"]].set_result({"text": "allowed evidence", "truncated": False})

        runner.emit = emit
        self.assertEqual(ROLE_TOOLS["strategy"], ["read_evidence", "search_evidence"])
        result = await runner.tool_call("read_evidence", {"path": "/allowed/evidence.txt", "max_chars": 100})
        self.assertEqual(result["text"], "allowed evidence")
        result = await runner.tool_call("search_evidence", {"path": "/allowed/evidence.txt", "query": "needle"})
        self.assertEqual(result["text"], "allowed evidence")
        denied = await runner.tool_call("spawn_worker", {"task": "not permitted"})
        self.assertTrue(denied["is_error"])
        self.assertEqual(len(emitted), 2)
        self.assertEqual(emitted[0]["name"], "read_evidence")
        self.assertEqual(emitted[1]["name"], "search_evidence")
        self.assertNotIn("tool_observation", json.dumps(emitted))

    async def test_completed_demo_worker_only_acknowledges_new_message(self):
        args = argparse.Namespace(role="worker", agent_id="worker-test", session_id="demo-session", backend="demo")
        runner = Runner(args)
        calls = []

        async def tool_call(name, arguments):
            calls.append((name, arguments))
            return {"ok": True}

        runner.tool_call = tool_call
        result = await runner.demo_turn(json.dumps({"task_completed": True, "context": {"code": "X", "index": 1}, "messages": [{"id": "message-1", "body": "submission receipt"}]}))
        self.assertFalse(result["is_error"])
        self.assertEqual(calls, [("ack_message", {"message_id": "message-1"})])

    async def test_spawn_worker_schema_and_demo_leader_support_session_reuse(self):
        description, schema = TOOL_SPECS["spawn_worker"]
        self.assertEqual(schema["properties"]["worker_id"], {"type": "string"})
        self.assertNotIn("worker_id", schema["required"])
        self.assertIn("reuse", description)
        runner = Runner(argparse.Namespace(role="leader", agent_id="leader", session_id="demo-leader", backend="demo"))
        calls = []

        async def tool_call(name, arguments):
            calls.append((name, arguments))
            return {"worker_id": arguments.get("worker_id"), "ok": True}

        runner.tool_call = tool_call
        assignment = {"type": "assign_task", "worker_id": "worker-existing", "task": '{"index":3,"smoke":true}', "request_key": "reuse-test"}
        result = await runner.demo_turn(json.dumps({"messages": [{"id": "reuse-message", "body": json.dumps(assignment)}]}))
        self.assertFalse(result["is_error"])
        self.assertEqual(calls, [
            ("spawn_worker", {"worker_id": "worker-existing", "task": assignment["task"], "request_key": "reuse-test"}),
            ("ack_message", {"message_id": "reuse-message"}),
        ])


if __name__ == "__main__":
    unittest.main()
