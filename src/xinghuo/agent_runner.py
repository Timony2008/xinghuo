"""One supervised agent role, controlled over stdin/stdout JSONL.

Only protocol records go to stdout.  ``--backend demo`` is a deterministic
simulation using the same parent tool roundtrips; it never imports the SDK.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any
import uuid


log = logging.getLogger("xinghuo.agent_runner")


def _schema(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required or [], "additionalProperties": False}


STR = {"type": "string"}
REFS = {"type": "array", "items": STR}
EVIDENCE_PAGE = {"path": STR, "max_chars": {"type": "integer", "minimum": 1},
                 "offset": {"type": "integer", "minimum": 0},
                 "expected_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"}}
TOOL_SPECS: dict[str, tuple[str, dict[str, Any]]] = {
    "read_evidence": ("Read a Unicode-offset page of evidence referenced by the review packet. Continue with next_offset and expected_sha256 from the first page; a changed file is refused. The host enforces the packet allowlist and output budget. Only complete_file means the entire file was returned.", _schema(EVIDENCE_PAGE, ["path"])),
    "search_evidence": ("Search literal case-sensitive text throughout referenced evidence. Matching snippets and pagination remain within the host's output budget. Continue with next_offset and expected_sha256; search results never mean the entire file was read.", _schema({**EVIDENCE_PAGE, "query": STR}, ["path", "query"])),
    "spawn_worker": ("Assign a bounded task and return immediately. Supply worker_id to reuse an existing idle worker and its session; omit worker_id to create a new managed worker.", _schema({"task": STR, "worker_id": STR, "evidence_refs": REFS, "request_key": STR}, ["task"])),
    "send_message": ("Queue a message for a managed teammate, including the fixed recipient reporter.", _schema({"recipient": STR, "body": STR, "evidence_refs": REFS, "reply_to": STR}, ["recipient", "body"])),
    "report_progress": ("Record evidence-backed progress; the reporter will receive this event.", _schema({"summary": STR, "evidence_refs": REFS, "kind": {"type": "string", "enum": ["finding", "negative", "blocked", "correction", "review"]}}, ["summary"])),
    "get_report": ("Read the current report, pending evidence events and platform state.", _schema({})),
    "submit_flag": ("Submit a flag candidate immediately; do not wait for report publication.", _schema({"flag": STR}, ["flag"])),
    "publish_report": ("Atomically publish the reporter's evidence-backed current report.", _schema({"overview": STR, "recent_changes": REFS, "short_history": REFS, "next_steps": REFS, "evidence_refs": REFS, "through_event": {"type": "integer", "minimum": 0}}, ["overview", "recent_changes", "short_history", "next_steps", "evidence_refs", "through_event"])),
    "ack_message": ("Acknowledge that this role has processed a received message.", _schema({"message_id": STR}, ["message_id"])),
}
ROLE_TOOLS = {
    "leader": ["spawn_worker", "send_message", "report_progress", "get_report", "submit_flag", "ack_message"],
    "worker": ["send_message", "report_progress", "get_report", "submit_flag", "ack_message"],
    "reporter": ["send_message", "report_progress", "get_report", "publish_report", "ack_message"],
    "reviewer": ["send_message", "report_progress", "get_report", "submit_flag", "ack_message"],
    "strategy": ["read_evidence", "search_evidence"],
}


class Runner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.session_id: str | None = args.session_id
        self.turns: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.pending_tools: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self.stop = asyncio.Event()
        self.write_lock = asyncio.Lock()
        self.client: Any = None
        self.sdk_receiver: asyncio.Task[None] | None = None
        self.active_result: asyncio.Future[dict[str, Any]] | None = None
        self.active_text: list[str] = []
        self.sdk_error: Exception | None = None
        self.initial_dispatched = bool(args.session_id)
        self.tool_uses: dict[str, dict[str, Any]] = {}

    @staticmethod
    def fingerprint(value: Any) -> str:
        """Hash canonical data in memory; never log or persist the original."""
        canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()

    async def emit(self, item: dict[str, Any]) -> None:
        async with self.write_lock:
            sys.stdout.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
            sys.stdout.flush()

    async def set_session(self, session_id: str | None) -> None:
        if session_id and session_id != self.session_id:
            self.session_id = session_id
            await self.emit({"type": "session", "session_id": session_id})

    async def tool_call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name not in ROLE_TOOLS[self.args.role]:
            return {"is_error": True, "error": f"Role {self.args.role} cannot call {name}"}
        call_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        self.pending_tools[call_id] = future
        try:
            await self.emit({"type": "tool_call", "id": call_id, "name": name, "args": arguments})
            result = await future
            if self.args.backend == "demo" and (result.get("is_error") or result.get("isError")):
                raise RuntimeError(f"Demo tool {name} failed: {result.get('error', result)}")
            return result
        finally:
            self.pending_tools.pop(call_id, None)

    async def read_input(self) -> None:
        reader = asyncio.StreamReader(limit=4 * 1024 * 1024)
        transport = None
        try:
            transport, _ = await asyncio.get_running_loop().connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(reader), sys.stdin
            )
            while line := await reader.readline():
                try:
                    item = json.loads(line)
                    if not isinstance(item, dict):
                        raise ValueError("protocol record must be an object")
                except (json.JSONDecodeError, ValueError) as exc:
                    log.error("Invalid parent input: %s", exc)
                    continue
                kind = item.get("type")
                if kind == "turn":
                    if not isinstance(item.get("prompt"), str):
                        await self.emit({"type": "turn_result", "id": item.get("id"), "result": "", "session_id": self.session_id, "is_error": True, "error": "prompt must be a string"})
                    else:
                        await self.turns.put(item)
                elif kind == "tool_result":
                    future = self.pending_tools.get(str(item.get("id")))
                    if future and not future.done():
                        result = item.get("result", {})
                        future.set_result(result if isinstance(result, dict) else {"result": result})
                elif kind == "stop":
                    break
                else:
                    log.error("Unknown parent record type: %r", kind)
        finally:
            if transport:
                transport.close()
            self.stop.set()

    def system_prompt(self) -> str:
        base = (
            f"You are the {self.args.role} of one managed challenge team. Your managed agent ID is {self.args.agent_id}. "
            "Use only the challenge scope and evidence supplied in your inputs. "
            "Managed teammates include leader and reporter. Use the xinghuo MCP tools for delegation, communication, progress, and flag submission. "
            "A queued message is not proof the receiver processed it. Acknowledge received messages with ack_message after handling them. "
            "Distinguish verified evidence, hypotheses, and unchecked areas. Refer to evidence files explicitly. "
            "Use absolute paths for all evidence files so teammates can locate them from their own working directories. "
            "If task_completed is true, your earlier assignment is complete: handle and acknowledge the new messages without repeating that assignment or resubmitting previous flags. "
            "Do not invoke native agents or create independent Claude processes. End each bounded assignment with a concise factual result; more inputs may follow. "
        )
        if self.args.role == "leader":
            return base + "Delegate independent work through spawn_worker. Start at least two workers when the input requests a parallel team. Do not block other work waiting for a single worker. Send important changes and corrections to reporter."
        if self.args.role == "reporter":
            return base + "You alone maintain the human-readable report. Read get_report to obtain current state and pending events. Merge stable challenge facts, recent changes and short history; publish_report through the highest event actually processed. Include corrections and evidence provenance. Never invent progress, flags, or missing context. Use short factual sentences, merge repeated facts and avoid filler or raw log dumps. Keep essential evidence paths, blockers, uncertainty and complete watermark coverage; concise wording is not permission to omit required facts."
        if self.args.role == "reviewer":
            return base + "Read the current report first. Reproduce every known flag acquisition path before new exploration, then audit lateral opportunities, scan coverage, missed vulnerabilities, untried routes and previously rejected methods. Record evidence or explicit failed/blocked/unchecked status for every section. Inspect worker health, write the full review to review_context.output_path, report_progress(kind='review') with evidence references and message the leader. Never claim replay from old receipts or summaries alone."
        if self.args.role == "strategy":
            return "You are an independent conservative strategy reviewer. You may use only the read-only read_evidence and search_evidence MCP tools to verify evidence references in the review packet; the host enforces their scope and shared output budget. Read further pages with next_offset and expected_sha256; search snippets do not prove full-file coverage. Follow the JSON response specification in the input exactly. Evaluate recent key progress and evidence of a breakthrough in the next review window. Your final response must contain JSON only."
        return base + "Complete the assigned task, immediately submit any flag, and report evidence directly to reporter and report_progress."

    async def start_sdk(self) -> None:
        import claude_agent_sdk
        from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, create_sdk_mcp_server, tool

        bundled_cli = Path(claude_agent_sdk.__file__).resolve().parent / "_bundled" / ("claude.exe" if os.name == "nt" else "claude")
        if not bundled_cli.is_file():
            raise RuntimeError("claude-agent-sdk bundled CLI is missing; install the platform wheel instead of using a global CLI")
        tools = []
        for name in ROLE_TOOLS[self.args.role]:
            description, schema = TOOL_SPECS[name]

            def make_handler(tool_name: str):
                async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
                    result = await self.tool_call(tool_name, arguments)
                    response: dict[str, Any] = {"content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False, default=str)}]}
                    if result.get("is_error") or result.get("isError"):
                        response["isError"] = True
                    return response
                return handler

            tools.append(tool(name, description, schema)(make_handler(name)))

        builtins = [] if self.args.role == "strategy" else ["Read", "Glob", "Grep"]
        if self.args.role in {"leader", "worker", "reviewer"}:
            builtins += ["Write", "Edit", "Bash"]
        server = create_sdk_mcp_server("xinghuo", tools=tools) if tools else None
        # Sanitize after the SDK merges its environment with the parent. The
        # strategy process otherwise inherits both services' raw credentials.
        from .veria.driver import write_sdk_launcher, client_environment
        launcher = Path(self.args.cwd) / "launch-sdk"
        write_sdk_launcher(launcher, bundled_cli, own_session=False)
        model = self.args.model or os.environ.get("ANTHROPIC_MODEL") or None
        sdk_env = {"CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS": "1"}
        if self.args.role == "strategy" and os.environ.get("XINGHUO_TEAM_PROFILE") in {"mixed4", "flash4"}:
            sdk_env = client_environment(model, Path(self.args.cwd) / ".claude", provider="deepseek")
        options = ClaudeAgentOptions(
            cwd=self.args.cwd,
            cli_path=str(launcher),
            model=self.args.model or os.environ.get("ANTHROPIC_MODEL") or None,
            resume=self.args.session_id,
            tools=builtins,
            allowed_tools=builtins + [f"mcp__xinghuo__{name}" for name in ROLE_TOOLS[self.args.role]],
            disallowed_tools=["Agent", "Task", "SendMessage", "Workflow", "CronCreate", "CronDelete", "CronList"],
            mcp_servers={"xinghuo": server} if server else {},
            strict_mcp_config=True,
            setting_sources=[],
            permission_mode="dontAsk",
            system_prompt=self.system_prompt(),
            env=sdk_env,
            stderr=lambda line: log.info("SDK: %s", line),
            verbatim_prompts=True,
        )
        # Resolve the packaged CLI explicitly; never fall back to a global CLI.
        self.client = ClaudeSDKClient(options=options)
        await self.client.connect()
        self.sdk_receiver = asyncio.create_task(self.receive_sdk(), name="sdk-output-reader")

    async def receive_sdk(self) -> None:
        from claude_agent_sdk import AssistantMessage, ResultMessage, SystemMessage, TextBlock, ToolResultBlock, ToolUseBlock, UserMessage

        try:
            async for message in self.client.receive_messages():
                session_id = getattr(message, "session_id", None)
                if isinstance(message, SystemMessage):
                    session_id = session_id or message.data.get("session_id")
                await self.set_session(session_id)
                if isinstance(message, AssistantMessage):
                    self.active_text.extend(block.text for block in message.content if isinstance(block, TextBlock))
                    for block in message.content:
                        if isinstance(block, ToolUseBlock) and not block.name.startswith("mcp__"):
                            observation = {"type": "tool_observation", "phase": "started", "tool_use_id": block.id, "tool_name": block.name, "input_fingerprint": self.fingerprint(block.input), "result_fingerprint": None, "is_error": None}
                            self.tool_uses[block.id] = observation
                            if len(self.tool_uses) > 1000:
                                self.tool_uses.pop(next(iter(self.tool_uses)))
                            await self.emit(observation)
                    if message.error:
                        log.error("SDK assistant error: %s", message.error)
                elif isinstance(message, UserMessage) and isinstance(message.content, list):
                    for block in message.content:
                        if isinstance(block, ToolResultBlock):
                            started = self.tool_uses.pop(block.tool_use_id, None)
                            if started:
                                await self.emit({**started, "phase": "completed", "result_fingerprint": self.fingerprint(block.content), "is_error": bool(block.is_error)})
                elif isinstance(message, ResultMessage):
                    is_error = bool(message.is_error or message.subtype.startswith("error_") or getattr(message, "terminal_reason", None) in {"api_error", "aborted_streaming", "aborted_tools"})
                    result: dict[str, Any] = {
                        "result": message.result or "\n".join(self.active_text),
                        "session_id": self.session_id,
                        "is_error": is_error,
                        "subtype": message.subtype,
                        "terminal_reason": getattr(message, "terminal_reason", None),
                    }
                    if message.structured_output is not None:
                        result["structured_output"] = message.structured_output
                    if is_error:
                        result["error"] = str(message.result or getattr(message, "errors", None) or getattr(message, "api_error_status", None) or message.subtype)
                    if self.active_result and not self.active_result.done():
                        self.active_result.set_result(result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.sdk_error = exc
            if self.active_result and not self.active_result.done():
                self.active_result.set_exception(exc)
            log.exception("SDK output stream failed")
        finally:
            if not self.stop.is_set() and self.active_result and not self.active_result.done():
                self.active_result.set_exception(RuntimeError("SDK output stream closed before turn result"))

    async def sdk_turn(self, prompt: str) -> dict[str, Any]:
        if self.client is None:
            await self.start_sdk()
        if self.sdk_error:
            raise self.sdk_error
        if self.sdk_receiver and self.sdk_receiver.done():
            raise RuntimeError("SDK output reader is no longer running")
        self.active_text = []
        self.active_result = asyncio.get_running_loop().create_future()
        try:
            await self.client.query(prompt)
            return await self.active_result
        finally:
            self.active_result = None

    @staticmethod
    def parse_context(prompt: str) -> dict[str, Any]:
        try:
            data = json.loads(prompt)
            return data if isinstance(data, dict) else {"text": prompt}
        except json.JSONDecodeError:
            return {"text": prompt}

    async def demo_turn(self, prompt: str) -> dict[str, Any]:
        if not self.session_id:
            await self.set_session("demo-" + str(uuid.uuid4()))
        payload = self.parse_context(prompt)
        context = {**payload.get("initial_context", {}), **payload.get("context", {})}
        challenge = context.get("challenge") or {}
        if not isinstance(challenge, dict):
            challenge = {"code": str(challenge)}
        code = str(context.get("code") or challenge.get("code") or payload.get("code") or "DEMO")
        index = context.get("index") or payload.get("index") or 1
        nonce = str(context.get("nonce") or payload.get("nonce") or "demo")
        smoke = bool(context.get("smoke") or payload.get("smoke") or context.get("mode") == "smoke")
        task = payload.get("task") or context.get("task")
        if isinstance(task, str):
            task_data = self.parse_context(task)
            context.update(task_data)
            code, index, nonce = str(context.get("code", code)), context.get("index", index), str(context.get("nonce", nonce))
            smoke = bool(context.get("smoke", smoke))
        messages = list(payload.get("messages") or [])
        events = list(payload.get("events") or [])
        if self.args.role == "strategy":
            return {"result": json.dumps({"decision": "continue", "reason": "Explicit demo strategy; no live model review", "milestone": "Collect the next evidence report"}), "session_id": self.session_id, "is_error": False}
        if self.args.role == "leader":
            assignments = []
            audit_requests = []
            for message in messages:
                body = message.get("body")
                assignment = self.parse_context(body) if isinstance(body, str) else body
                if isinstance(assignment, dict) and assignment.get("type") == "assign_task":
                    assignments.append(assignment)
                elif isinstance(assignment, dict) and assignment.get("type") == "audit_report_request":
                    audit_requests.append((message, assignment))
            for message, request in audit_requests:
                current = payload.get("report") or {}
                body = json.dumps({"type": "leader_audit_report", "round_id": request["round_id"],
                    "progress": current.get("overview", ""), "promise_result": "合成演练，已记录结果见证据。",
                    "next_breakthrough": "继续验证已有证据中的具体下一步；本条不代表真实比赛进展。",
                    "blockers": "无新增合成证据时明确未知。"}, ensure_ascii=False)
                await self.tool_call("send_message", {"recipient": "reporter", "body": body,
                    "reply_to": message["id"], "evidence_refs": current.get("evidence_refs", [])})
            correction = payload.get("correction")
            if not correction:
                correction = next((m.get("body") for m in messages if "correction" in str(m.get("body", "")).lower() or "更正" in str(m.get("body", ""))), None)
            if correction:
                await self.tool_call("report_progress", {"summary": str(correction), "kind": "correction", "evidence_refs": []})
                await self.tool_call("send_message", {"recipient": "reporter", "body": str(correction), "evidence_refs": []})
            elif assignments:
                for assignment in assignments:
                    await self.tool_call("spawn_worker", {key: assignment[key] for key in ("worker_id", "task", "request_key", "evidence_refs") if key in assignment})
            elif not self.initial_dispatched:
                for worker_index in (1, 2):
                    worker_task = json.dumps({"code": code, "index": worker_index, "nonce": nonce, "smoke": smoke, "objective": "Produce independent evidence and report directly to reporter"}, ensure_ascii=False)
                    await self.tool_call("spawn_worker", {"task": worker_task, "evidence_refs": [], "request_key": f"initial-worker-{worker_index}"})
                self.initial_dispatched = True
                await self.tool_call("report_progress", {"summary": f"Explicit demo leader dispatched two independent workers for {code}; nonce={nonce}", "kind": "finding", "evidence_refs": []})
        elif self.args.role == "worker" and not (payload.get("task_completed") or context.get("task_completed")):
            summary = f"Explicit demo worker-{index} verified evidence for {code}; nonce={nonce}"
            refs = [f"demo://{code}/worker-{index}"]
            await self.tool_call("report_progress", {"summary": summary, "kind": "finding", "evidence_refs": refs})
            await self.tool_call("send_message", {"recipient": "reporter", "body": summary, "evidence_refs": refs})
            if not smoke:
                await self.tool_call("submit_flag", {"flag": f"flag{{{code}:{index}}}"})
        elif self.args.role == "reviewer":
            summary = "Explicit demo review checked worker state, coverage and existing evidence; verify one alternative route next."
            await self.tool_call("report_progress", {"summary": summary, "kind": "review", "evidence_refs": []})
            await self.tool_call("send_message", {"recipient": "leader", "body": summary, "evidence_refs": []})
            await self.tool_call("send_message", {"recipient": "reporter", "body": summary, "evidence_refs": []})
        elif self.args.role == "reporter":
            report = await self.tool_call("get_report", {})
            source = report.get("report") if isinstance(report.get("report"), dict) else report
            recent = list(source.get("recent_changes") or [])
            history = list(source.get("short_history") or [])
            refs = list(source.get("evidence_refs") or [])
            for event in [*report.get("events", []), *events]:
                event_data = event.get("data") or event.get("payload") or {}
                if isinstance(event_data, dict):
                    text = event_data.get("summary") or event_data.get("body")
                    if text:
                        recent.append(str(text))
                    refs.extend(event_data.get("evidence_refs") or [])
            reporter_messages = [*report.get("messages", []), *messages]
            for message in reporter_messages:
                if message.get("body"):
                    recent.append(str(message["body"]))
                refs.extend(message.get("evidence_refs") or [])
            through = max([int(payload.get("through_event") or 0), int(report.get("through_event") or 0), int(source.get("through_event") or 0), *[int(e.get("seq") or 0) for e in events + list(report.get("events") or [])]])
            overview = str(source.get("overview") or context.get("overview") or f"Explicit demo report for {code}; nonce={nonce}. Verified evidence is recorded below.")
            for message_id in dict.fromkeys(str(m["id"]) for m in reporter_messages if m.get("id")):
                await self.tool_call("ack_message", {"message_id": message_id})
            await self.tool_call("publish_report", {"overview": overview, "recent_changes": list(dict.fromkeys(recent))[-30:], "short_history": history[-15:], "next_steps": ["Compare the latest evidence and verify an actionable next step."], "evidence_refs": list(dict.fromkeys(refs)), "through_event": through})
        for message in messages if self.args.role != "reporter" else []:
            if message.get("id"):
                await self.tool_call("ack_message", {"message_id": str(message["id"])})
        return {"result": f"Explicit demo {self.args.role} turn completed", "session_id": self.session_id, "is_error": False}

    async def process_turns(self) -> None:
        try:
            while True:
                item = await self.turns.get()
                try:
                    response = await (self.demo_turn(item["prompt"]) if self.args.backend == "demo" else self.sdk_turn(item["prompt"]))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception("Role turn failed")
                    response = {"result": "", "session_id": self.session_id, "is_error": True, "error": f"{type(exc).__name__}: {exc}"}
                await self.emit({"type": "turn_result", "id": item["id"], **response})
                self.turns.task_done()
        finally:
            # ClaudeSDKClient owns an AnyIO task-group cancel scope. Connect
            # and disconnect must run in this same owner task, not wait_for's
            # new task or Runner.run's task.
            if self.client is not None:
                with contextlib.suppress(Exception):
                    async with asyncio.timeout(3):
                        await self.client.disconnect()
            if self.sdk_receiver:
                self.sdk_receiver.cancel()
                await asyncio.gather(self.sdk_receiver, return_exceptions=True)

    async def run(self) -> None:
        if self.session_id:
            await self.emit({"type": "session", "session_id": self.session_id})
        input_task = asyncio.create_task(self.read_input(), name="parent-input-reader")
        turn_task = asyncio.create_task(self.process_turns(), name="serial-turn-worker")
        try:
            await self.stop.wait()
        finally:
            self.stop.set()
            turn_task.cancel()
            await asyncio.gather(turn_task, return_exceptions=True)
            for future in self.pending_tools.values():
                if not future.done():
                    future.cancel()
            input_task.cancel()
            await asyncio.gather(input_task, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=list(ROLE_TOOLS))
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--session-id")
    parser.add_argument("--model")
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument("--backend", choices=["demo", "claude"], default="demo")
    args = parser.parse_args()
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(Runner(args).run())


if __name__ == "__main__":
    main()
