import asyncio
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from xinghuo.veria.driver import DriverError, HUMAN_INTERACTION_TOOLS, VeriaRoleDriver, client_environment
from xinghuo.veria.prompts import build_role_prompt, build_solver_prompt, continuation_prompt
from xinghuo.veria.upstream_baseline import solver_output_json_schema


def message(name, **kwargs):
    return type(name, (), kwargs)()


class FakeEnvironment:
    _description = {"architecture": "fixture", "tools": {"python3": "/bin/python3"}}
    def __init__(self):
        self.commands = []
        self.stopped = []
        self.abandoned = []
    async def prepare_command(self, agent_id, command, tool_id):
        self.commands.append((agent_id, command, tool_id))
        return "docker exec fixture true"
    def complete_command(self, tool_id, response):
        pass
    async def stop_member(self, agent_id):
        self.stopped.append(agent_id)
    def abandon_command(self, tool_id):
        self.abandoned.append(tool_id)


class FakeClient:
    def __init__(self, options):
        self.options = options
        self.queue = asyncio.Queue()
        self.readers = 0
        self.max_readers = 0
        self.queries = []
        self.active = 0
        self.max_active = 0
    async def connect(self):
        await self.queue.put(message("SystemMessage", data={"session_id": "session-early"}))
    async def receive_messages(self):
        self.readers += 1
        self.max_readers = max(self.max_readers, self.readers)
        try:
            while True:
                item = await self.queue.get()
                if item is None:
                    break
                yield item
        finally:
            self.readers -= 1
    async def query(self, prompt):
        self.active += 1
        self.max_active = max(self.active, self.max_active)
        self.queries.append(prompt)
        await asyncio.sleep(.01)
        await self.queue.put(message("AssistantMessage", model=self.options.model, content=[SimpleNamespace(text=prompt)], error=None))
        await self.queue.put(message("ResultMessage", session_id="session-early", is_error=False, result=prompt, structured_output=None, usage={}, total_cost_usd=0))
        self.active -= 1
    async def interrupt(self):
        await self.queue.put(message("ResultMessage", session_id="session-early", is_error=False, result="interrupted"))
    async def disconnect(self):
        await self.queue.put(None)


class DriverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.environment = FakeEnvironment()
        self.events = []
        self.calls = []
        self.sessions = []
        async def on_tool(name, args):
            self.calls.append((name, args))
            return {"status": "correct", "request_id": "candidate-1"} if name == "submit_flag" else {"messages": [{"id": "broadcast-1"}]}
        self.driver = VeriaRoleDriver("pro-1", "solver", "deepseek-v4-pro", self.temp.name, {}, self.environment,
            on_tool, self.sessions.append, lambda kind, data: self.events.append((kind, data)), client_factory=FakeClient)

    async def test_solver_structured_output_tool_uses_upstream_schema_without_shell_execution(self):
        args = {"type": "flag_found", "flag": "flag{offline}", "method": "offline fixture"}
        result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "StructuredOutput", "tool_input": args}, "structured")
        self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "allow")
        self.assertEqual(self.environment.commands, [])
        for malformed in ({**args, "extra": "unexpected"}, {**args, "flag": 7}, {**args, "type": "other"}, {}):
            result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "StructuredOutput", "tool_input": malformed}, "bad-structured")
            self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")
        self.driver.role = "coordinator"
        result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "StructuredOutput", "tool_input": args}, "wrong-role")
        self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")

    async def test_full_builder_output_is_the_actual_verbatim_sdk_prompt_file_without_argv_content(self):
        from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport
        for role in ("solver", "reviewer", "coordinator", "reporter"):
            with self.subTest(role=role):
                context = {"agent_id": role, "review": {"review_id": "fixture-review"},
                           "challenge": {"category": "pwn", "connection_info": "nc 192.0.2.42 80",
                                         "description": "curl http://192.0.2.42/ is the fixture target"}}
                driver = VeriaRoleDriver(role, role, "deepseek-v4-pro", Path(self.temp.name) / role,
                    context, self.environment, self.driver.on_tool, client_factory=FakeClient)
                await driver.start()
                try:
                    expected = build_role_prompt(role, driver.tool_member, context, self.environment._description)
                    prompt_path = driver.workspace / "sdk-system-prompt.txt"
                    self.assertEqual(driver.client.options.system_prompt,
                                     {"type": "file", "path": str(prompt_path)})
                    self.assertTrue(driver.client.options.verbatim_prompts)
                    self.assertEqual(prompt_path.read_text(), expected)
                    # Exercise the pinned SDK's actual argument builder, not a
                    # parallel fixture that could accidentally hide an extra
                    # --system-prompt/append layer.
                    transport = SubprocessCLITransport(prompt="", options=driver.client.options)
                    command = transport._build_command()
                    self.assertEqual(command.count("--system-prompt-file"), 1)
                    self.assertEqual(command[command.index("--system-prompt-file") + 1], str(prompt_path))
                    self.assertNotIn("--system-prompt", command)
                    self.assertNotIn("--append-system-prompt", command)
                    self.assertNotIn(expected, command)
                    self.assertNotIn("192.0.2.42", " ".join(command))
                    if role == "solver":
                        self.assertEqual(driver.client.options.output_format,
                            {"type": "json_schema", "schema": solver_output_json_schema()})
                    else:
                        self.assertIsNone(driver.client.options.output_format)
                    if role == "reviewer":
                        self.assertIn(f"/members/{driver.tool_member}/work", expected)
                        self.assertIn("**pyghidra** is installed", expected)
                finally:
                    await driver.close()

    async def test_actual_sdk_prompt_exposes_remote_description_image_and_archive_as_unstaged(self):
        from xinghuo.core import Challenge
        self.driver.context = {"challenge": Challenge("attachment-fixture", description=
            "![image](https://materials.invalid/figure.png) [attachment](https://materials.invalid/binary.zip) "
            "[service](https://target.invalid/)").to_dict(), "distfile_names": []}
        await self.driver.start()
        try:
            text = Path(self.driver.client.options.system_prompt["path"]).read_text()
            self.assertIn("`image` — remote/not_staged; source: https://materials.invalid/figure.png", text)
            self.assertIn("`attachment` — remote/not_staged; source: https://materials.invalid/binary.zip", text)
            self.assertIn("After obtaining the image: **IMAGE: use `exiftool`", text)
            self.assertNotIn("/challenge/inputs/figure.png", text)
            self.assertNotIn("/challenge/inputs/binary.zip", text)
        finally:
            await self.driver.close()

    async def test_one_reader_serial_turns_and_early_session_persistence(self):
        await self.driver.start()
        await asyncio.sleep(.01)
        self.assertEqual(self.driver.client.options.permission_mode, "dontAsk")
        self.assertNotIn("Bash", self.driver.client.options.allowed_tools)
        self.assertEqual(self.sessions, ["session-early"])
        saved = json.loads((Path(self.temp.name) / "sdk-session.json").read_text())
        self.assertEqual(saved["session_id"], "session-early")
        results = await asyncio.gather(self.driver.turn("first"), self.driver.turn("second"))
        self.assertEqual([r["result"] for r in results], ["first", "second"])
        self.assertEqual(self.driver.client.max_readers, 1)
        self.assertEqual(self.driver.client.max_active, 1)
        self.assertNotEqual(results[0]["turn_id"], results[1]["turn_id"])
        await self.driver.close()
        self.assertEqual(self.environment.stopped, ["pro-1"])

    async def test_fail_closed_on_routing_exception_and_native_tools(self):
        async def broken(*args):
            raise RuntimeError("redirect unavailable")
        self.environment.prepare_command = broken
        result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": "whoami"}}, "one")
        self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")
        for name in ["Read", "Write", "Edit", "Agent", "Task", "WebSearch", "WebFetch", "mcp__foreign__read"]:
            result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": name, "tool_input": {}}, name)
            self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")

    async def test_unattended_roles_cannot_request_human_or_plan_approval(self):
        for role in ("solver", "reviewer", "coordinator", "reporter"):
            with self.subTest(role=role):
                driver = VeriaRoleDriver(role, role, "glm-5.3", Path(self.temp.name) / role,
                    {}, self.environment, self.driver.on_tool, client_factory=FakeClient)
                await driver.start()
                try:
                    self.assertEqual(driver.client.options.permission_mode, "dontAsk")
                    self.assertIsNone(driver.client.options.max_turns)
                    self.assertIsNone(driver.client.options.max_budget_usd)
                    for name in HUMAN_INTERACTION_TOOLS:
                        self.assertNotIn(name, driver.client.options.tools)
                        self.assertIn(name, driver.client.options.disallowed_tools)
                        result = await driver.pre_tool({"hook_event_name": "PreToolUse",
                            "tool_name": name, "tool_input": {}}, name)
                        output = result["hookSpecificOutput"]
                        self.assertEqual(output["permissionDecision"], "deny")
                        self.assertIn("no human answer", output["permissionDecisionReason"])
                finally:
                    await driver.close()

    async def test_sdk_failure_diagnostics_survive_even_without_result_text(self):
        class FailedClient(FakeClient):
            async def query(self, prompt):
                await self.queue.put(message("ResultMessage", session_id="session-early", is_error=True,
                    result=None, errors=["authentication failed token=offline-secret"], subtype="error_during_execution",
                    api_error_status=401, terminal_reason="api_error", permission_denials=[]))
        self.driver._secrets.append("offline-secret")
        self.driver.client_factory = FailedClient
        try:
            result = await self.driver.turn("work")
            self.assertTrue(result["is_error"])
            self.assertEqual(result["api_error_status"], 401)
            self.assertIn("authentication failed", result["error"])
            self.assertIn("401", result["error"])
            self.assertNotIn("offline-secret", result["error"])
            self.assertNotIn("offline-secret", json.dumps(result["errors"]))
            self.assertEqual(result["terminal_reason"], "api_error")
            self.assertEqual([data for kind, data in self.events if kind == "turn_result"][-1]["error"], result["error"])
        finally:
            await self.driver.close()

    async def test_plain_text_request_for_confirmation_is_a_completed_turn_not_a_ui_wait(self):
        first = await self.driver.turn("May I read the challenge files?")
        self.assertFalse(first["is_error"])
        second = await self.driver.turn("Continue authorized challenge work autonomously.")
        self.assertEqual(second["result"], "Continue authorized challenge work autonomously.")
        self.assertEqual(len(self.driver.client.queries), 2)
        await self.driver.close()

    async def test_sdk_terminal_error_cannot_be_mistaken_for_success(self):
        class FailedClient(FakeClient):
            async def query(self, prompt):
                await self.queue.put(message("ResultMessage", is_error=False, result="partial progress",
                    subtype="error_max_turns", terminal_reason="max_turns", errors=["maximum turns reached"]))
        self.driver.client_factory = FailedClient
        try:
            result = await self.driver.turn("work")
            self.assertTrue(result["is_error"])
            self.assertIn("maximum turns reached", result["error"])
        finally:
            await self.driver.close()

    async def test_local_web_capabilities_are_real_sdk_tools_and_hook_permissions(self):
        self.environment._description = {**self.environment._description,
            "web_tools": ["WebFetch", "WebSearch"]}
        await self.driver.start()
        try:
            for name in ("WebFetch", "WebSearch"):
                self.assertIn(name, self.driver.client.options.tools)
                self.assertNotIn(name, self.driver.client.options.disallowed_tools)
                self.assertNotIn(name, self.driver.client.options.allowed_tools)
                result = await self.driver.pre_tool({"hook_event_name": "PreToolUse",
                    "tool_name": name, "tool_input": {"url": "https://offline.invalid"}}, name)
                self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "allow")
            self.assertFalse(self.environment.commands)
        finally:
            await self.driver.close()

    async def test_hosted_mode_cannot_enable_native_web_even_with_stale_capability(self):
        self.environment._description = {"execution_mode": "hosted_process",
            "web_tools": ["WebFetch", "WebSearch"]}
        self.driver.context = {"challenge": {"connection_info": "https://offline.invalid"}}
        await self.driver.start()
        try:
            text = Path(self.driver.client.options.system_prompt["path"]).read_text()
            self.assertNotIn("or use `WebFetch`.", text)
            self.assertIn("Native WebFetch is unavailable in this environment", text)
            self.assertIn('"web_tools": []', text)
            for name in ("WebFetch", "WebSearch"):
                self.assertNotIn(name, self.driver.client.options.tools)
                self.assertIn(name, self.driver.client.options.disallowed_tools)
                result = await self.driver.pre_tool({"hook_event_name": "PreToolUse",
                    "tool_name": name, "tool_input": {}}, name)
                self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")
        finally:
            await self.driver.close()

    async def test_network_none_denies_native_web_and_prompt_uses_effective_capabilities(self):
        self.environment._description = {"network_mode": "none", "web_tools": ["WebFetch", "WebSearch"]}
        self.driver.context = {"challenge": {"connection_info": "https://offline.invalid"}}
        await self.driver.start()
        try:
            for name in ("WebFetch", "WebSearch"):
                self.assertNotIn(name, self.driver.client.options.tools)
                self.assertIn(name, self.driver.client.options.disallowed_tools)
                result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": name, "tool_input": {}}, name)
                self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")
            text = Path(self.driver.client.options.system_prompt["path"]).read_text()
            self.assertNotIn("or use `WebFetch`.", text)
            self.assertIn('"web_tools": []', text)
        finally:
            await self.driver.close()

    async def test_coordinator_shell_is_routed_through_member_environment(self):
        self.driver.role, self.driver.agent_id, self.driver.tool_member = "coordinator", "coordinator", "coordinator"
        await self.driver.start()
        try:
            self.assertIn("Bash", self.driver.client.options.tools)
            self.assertNotIn("Bash", self.driver.client.options.allowed_tools)
            result = await self.driver.pre_tool({"hook_event_name": "PreToolUse",
                "tool_name": "Bash", "tool_input": {"command": "printf offline"}}, "coordinator-command")
            self.assertEqual(result["hookSpecificOutput"]["updatedInput"]["command"], "docker exec fixture true")
            self.assertEqual(self.environment.commands, [("coordinator", "printf offline", "coordinator-command")])
        finally:
            await self.driver.close()

    async def test_web_hook_cancellation_never_defaults_to_allow(self):
        self.environment._description = {"web_tools": ["WebSearch"]}
        async def cancelled(*args):
            raise asyncio.CancelledError()
        self.driver.on_event = cancelled
        result = await self.driver.pre_tool({"hook_event_name": "PreToolUse",
            "tool_name": "WebSearch", "tool_input": {"query": "offline"}}, "cancelled")
        self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")

    async def test_correct_flag_continues_and_every_five_tools_injects_messages(self):
        result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": "submit_flag 'flag{first}'"}}, "flag")
        self.assertIn("printf", result["hookSpecificOutput"]["updatedInput"]["command"])
        self.assertEqual(self.calls[0], ("submit_flag", {"flag": "flag{first}"}))
        for index in range(1, 6):
            result = await self.driver.post_tool({"tool_name": "Bash", "tool_response": "ok"}, str(index))
            self.assertEqual(bool(result), index == 5)
        self.assertEqual(self.calls[-1][0], "check_messages")
        self.assertFalse(self.environment.stopped)

    async def test_active_reviewer_receives_coordinator_notices_every_five_tool_boundaries(self):
        self.driver.role, self.driver.agent_id, self.driver.tool_member = "reviewer", "reviewer", "reviewer-fixture"
        for index in range(1, 6):
            result = await self.driver.post_tool({"hook_event_name": "PostToolUse", "tool_name": "Bash", "tool_response": "ok"}, str(index))
            self.assertEqual(bool(result), index == 5)
        self.assertEqual(self.calls, [("check_messages", {})])
        self.assertIn("broadcast-1", result["hookSpecificOutput"]["additionalContext"])
        self.assertEqual([kind for kind, _ in self.events].count("messages_injected"), 1)

    async def test_reporter_has_no_shell_even_hook_misroute(self):
        self.driver.role = "reporter"
        result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": "true"}}, "one")
        self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")

    async def test_reporter_has_no_native_web_even_when_local_tools_available(self):
        self.driver.role = "reporter"
        self.environment._description = {"web_tools": ["WebSearch", "WebFetch"]}
        await self.driver.start()
        try:
            self.assertEqual(self.driver.client.options.tools, [])
            for name in ("WebSearch", "WebFetch"):
                result = await self.driver.pre_tool({"hook_event_name": "PreToolUse",
                    "tool_name": name, "tool_input": {}}, name)
                self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")
        finally:
            await self.driver.close()

    async def test_execution_result_excludes_special_and_denied_commands(self):
        await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": "submit_flag 'flag{x}'"}}, "special")
        await self.driver.post_tool({"hook_event_name": "PostToolUse", "tool_name": "Bash", "tool_response": "correct"}, "special")
        await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": "python solve.py"}}, "actual")
        await self.driver.post_tool({"hook_event_name": "PostToolUseFailure", "tool_name": "Bash", "tool_response": {"stderr": "failed", "exitCode": 1}}, "actual")
        execution = [data for kind, data in self.events if kind == "tool_execution_result"]
        self.assertEqual(len(execution), 1)
        self.assertEqual(execution[0]["command_id"], "actual")
        self.assertFalse(execution[0]["success"])

    async def test_review_rounds_have_distinct_container_members(self):
        one = VeriaRoleDriver("reviewer", "reviewer", "deepseek-v4-pro", self.temp.name, {"review": {"review_id": "round/one"}}, self.environment, self.driver.on_tool)
        two = VeriaRoleDriver("reviewer", "reviewer", "deepseek-v4-pro", self.temp.name, {"review": {"review_id": "round/two"}}, self.environment, self.driver.on_tool)
        self.assertNotEqual(one.tool_member, two.tool_member)
        self.assertTrue(one.tool_member.startswith("reviewer-"))

    async def test_evidence_failure_after_registration_refuses_dispatch(self):
        def failing(kind, data):
            if kind == "tool_execution_registered":
                raise OSError("disk full")
        self.driver.on_event = failing
        result = await self.driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": "sleep 100"}}, "registered")
        self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")
        self.assertEqual(self.environment.abandoned, ["registered"])
        self.assertFalse(self.driver._container_commands)

    async def test_cancelled_turn_drains_old_result_before_next_query(self):
        class SlowFirst(FakeClient):
            async def query(self, prompt):
                if prompt == "slow":
                    self.queries.append(prompt)
                    return
                await super().query(prompt)
        self.driver.client_factory = SlowFirst
        task = asyncio.create_task(self.driver.turn("slow"))
        for _ in range(100):
            if self.driver.client and self.driver.client.queries:
                break
            await asyncio.sleep(.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        result = await self.driver.turn("next")
        self.assertEqual(result["result"], "next")
        self.assertEqual(self.driver.client.max_readers, 1)
        await self.driver.close()

    async def test_interrupt_timeout_closes_client_without_recursive_wait(self):
        class NoInterrupt(FakeClient):
            async def interrupt(self):
                await asyncio.Event().wait()
        self.driver.client_factory = NoInterrupt
        await self.driver.start()
        await asyncio.wait_for(self.driver.interrupt(timeout=.01), .5)
        self.assertFalse(self.driver.alive)
        self.assertTrue(self.driver._reader.done())

    async def test_cleanup_error_still_closes_reader_and_pending_turn(self):
        await self.driver.start()
        async def fail(_):
            raise RuntimeError("container unavailable")
        self.environment.stop_member = fail
        with self.assertRaises(DriverError):
            await self.driver.close()
        self.assertTrue(self.driver._reader.done())
        self.assertFalse(self.driver.alive)

    async def test_final_result_json_takes_precedence_over_progress_text(self):
        class ReportingClient(FakeClient):
            async def query(self, prompt):
                await self.queue.put(message("AssistantMessage", model=self.options.model, content=[SimpleNamespace(text="I am inspecting the evidence.")], error=None))
                await self.queue.put(message("ResultMessage", session_id="session-early", is_error=False, result='{"overview":"done"}'))
        self.driver.client_factory = ReportingClient
        result = await self.driver.turn("report")
        self.assertEqual(json.loads(result["result"]), {"overview": "done"})
        self.assertIn("inspecting", result["visible_text"])
        await self.driver.close()

    async def test_unrecoverable_session_is_lineage_not_false_resume(self):
        self.driver.session_id = "missing-session"
        await self.driver.start()
        self.assertIsNone(self.driver.client.options.resume)
        self.assertTrue(any(kind == "session_lineage" for kind, _ in self.events))
        await self.driver.close()

    async def test_existing_session_file_passes_resume_without_model_change(self):
        self.driver.session_id = "existing-session"
        self.driver.config_dir.mkdir()
        (self.driver.config_dir / "existing-session.jsonl").write_text('{"type":"test"}\n')
        await self.driver.start()
        self.assertEqual(self.driver.client.options.resume, "existing-session")
        self.assertEqual(self.driver.client.options.model, "deepseek-v4-pro")
        self.assertIsNone(self.driver.client.options.fallback_model)
        await self.driver.close()


class ModelPromptTests(unittest.TestCase):
    def test_per_client_environment_never_mutates_global_defaults(self):
        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "offline-test-key", "ANTHROPIC_MODEL": "wrong"}):
            one = client_environment("deepseek-v4-pro", Path("/tmp/pro"))
            two = client_environment("deepseek-flash", Path("/tmp/flash"))
            self.assertEqual(os.environ["ANTHROPIC_MODEL"], "wrong")
            self.assertEqual(one["ANTHROPIC_DEFAULT_HAIKU_MODEL"], "deepseek-v4-pro")
            self.assertEqual(two["ANTHROPIC_DEFAULT_OPUS_MODEL"], "deepseek-flash")
            self.assertNotEqual(one["CLAUDE_CONFIG_DIR"], two["CLAUDE_CONFIG_DIR"])

    def test_prompt_preserves_solver_intent_without_false_environment_claims(self):
        prompt = build_solver_prompt("pro-2", {"challenge": {"name": "name", "connection_info": "use the two URLs", "urls": ["https://a.invalid", "https://b.invalid"]}}, {"architecture": "test", "tools": {"python3": "/bin/python3"}})
        self.assertIn("Be creative and thorough", prompt)
        self.assertIn("https://a.invalid", prompt)
        self.assertIn("https://b.invalid", prompt)
        self.assertNotIn("Run: `use the two URLs`", prompt)
        self.assertIn("pyghidra** is installed", prompt)
        self.assertNotIn("previous attempt did not find", continuation_prompt(has_confirmed=True))
        self.assertIn("remaining objectives", continuation_prompt(has_confirmed=True))
