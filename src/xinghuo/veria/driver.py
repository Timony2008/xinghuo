"""One managed Claude Agent SDK client per persistent Veria role.

No provider fallback, retry loop, poller or platform client lives here.  The
runtime owns continuation and finite recovery; this adapter owns tool routing,
one response reader, serial turns, early sessions and bounded local cleanup.
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import shlex
import signal
import sys
import time
from typing import Any, Awaitable, Callable
import uuid

from .loop_detect import LoopDetector, LOOP_WARNING_MESSAGE
from .prompts import build_role_prompt
from .upstream_baseline import solver_output_json_schema


class DriverError(RuntimeError):
    pass


class SpecialCommandError(ValueError):
    pass


SPECIAL_COMMANDS = {"submit_flag", "notify_coordinator", "record_acquisition"}


def parse_special_command(command: str) -> tuple[str, str] | None:
    """Parse a standalone shell builtin without evaluating shell syntax.

    Quoted metacharacters are legitimate candidate/message data. Unquoted shell
    operators, substitutions, comments and wrappers around a reserved command
    are refused. Ordinary shell commands remain ordinary Docker work.
    """
    lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|<>()\n")
    lexer.whitespace_split = True
    lexer.commenters = ""  # '#' belongs to a flag only when it is within its argument.
    try:
        tokens = list(lexer)
    except ValueError as exc:
        if re.search(r"\b(?:submit_flag|notify_coordinator|record_acquisition)\b", command):
            raise SpecialCommandError("Unbalanced quotes in standalone special command") from exc
        return None
    mentioned = any(token in SPECIAL_COMMANDS for token in tokens)
    if not mentioned:
        return None
    # Explicit shell syntax is accepted only inside quotes (shlex removes those,
    # so use a quote-aware scan to distinguish data from execution syntax).
    quote = None
    escaped = False
    unsafe = False
    for char in command.strip():
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote != "'":
            escaped = True
            continue
        if quote:
            if char == quote:
                quote = None
            elif quote == '"' and char in "$`":
                unsafe = True
            continue
        if char in "'\"":
            quote = char
        elif char in ";&|<>()\n\r`$":
            unsafe = True
    if unsafe or len(tokens) != 2 or tokens[0] not in SPECIAL_COMMANDS or not tokens[1].strip():
        raise SpecialCommandError("Use exactly submit_flag 'FLAG', notify_coordinator 'MESSAGE', or record_acquisition 'JSON_OBJECT' as a standalone command; shell combinations are refused")
    return tokens[0], tokens[1]


STR = {"type": "string"}
STRINGS = {"type": "array", "items": STR}
EVIDENCE_PAGE = {"path": STR, "max_chars": {"type": "integer", "minimum": 1},
                 "offset": {"type": "integer", "minimum": 0},
                 "expected_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"}}
ACQUISITION_FIELDS = {"request_id": STR, "entrypoints": STRINGS, "prerequisites": STRINGS,
                      "replay_commands": STRINGS, "script_paths": STRINGS, "notes": STR,
                      "evidence_refs": {"type": "array", "items": {"anyOf": [STR, {"type": "integer", "minimum": 1}]}}}


def _schema(properties=None, required=None, *, extra=False):
    return {"type": "object", "properties": properties or {}, "required": required or [], "additionalProperties": extra}


TOOL_SPECS = {
    "check_messages": ("Read pending messages with stable delivery IDs.", _schema()),
    "ack_messages": ("Explicitly acknowledge messages you have processed; delivery alone is not understanding.", _schema({"message_ids": STRINGS}, ["message_ids"])),
    "get_solve_status": ("Read authoritative progress for this question only.", _schema()),
    "check_swarm_status": ("Read configured member states, health and pending bump guidance.", _schema()),
    "read_solver_trace": ("Read visible SDK/tool trace evidence for a solver by member ID; continue from scanned_through.", _schema({"solver_id": STR, "after": {"type": "integer", "minimum": 0}}, ["solver_id"])),
    "bump_agent": ("Persist guidance for the solver's next natural continuation. Does not interrupt it.", _schema({"solver_id": STR, "insights": STR}, ["solver_id", "insights"])),
    "broadcast": ("Publish selected technical guidance and shared-operation coordination to solver and active reviewer tool-boundary inboxes.", _schema({"message": STR, "source_message_ids": STRINGS}, ["message"])),
    "submit_flag": ("Immediately queue a candidate through the supervisor and return its actual receipt or pending state.", _schema({"flag": STR}, ["flag"])),
    "notify_coordinator": ("Persist a private technical message for the current coordinator.", _schema({"message": STR, "evidence_refs": STRINGS}, ["message"])),
    "get_report": ("Read current report, bounded material batch and authoritative platform state.", _schema()),
    "read_evidence": ("Read a page of a referenced UTF-8 evidence file. Unicode offset defaults to zero; use next_offset and expected_sha256=the first page's sha256 for every later page. eof ends this page sequence; only complete_file proves the entire file was returned. Files remain intact.", _schema(EVIDENCE_PAGE, ["path"])),
    "search_evidence": ("Find literal case-sensitive text anywhere in a referenced evidence file, including beyond its preview. Returns bounded matching snippets and next_offset; supply the returned sha256 as expected_sha256 for subsequent pages. Search snippets never mean the entire evidence was read.", _schema({**EVIDENCE_PAGE, "query": STR}, ["path", "query"])),
    "record_acquisition": ("After immediate flag submission, attach or update acquisition instructions for its request_id. Include entrypoints, prerequisites, exact replay_commands, script_paths and evidence_refs; unknown fields remain pending. This never executes commands and never proves independent reproduction. An id-only call reads current state.", _schema(ACQUISITION_FIELDS, ["request_id"])),
    "read_acquisition": ("Read versioned acquisition material for a locally confirmed flag, or list available manifests. Large manifests return an explicit index and manifest_path for read_evidence/search_evidence. These historical instructions are not fresh reviewer reproduction evidence.", _schema({"request_id": STR, "version": {"type": "integer", "minimum": 1}})),
    "publish_report": ("Atomically publish one report through the invocation's fixed input watermark.", _schema({"overview": STR, "recent_changes": STRINGS, "short_history": STRINGS, "next_steps": STRINGS, "evidence_refs": STRINGS, "through_event": {"type": "integer", "minimum": 0}, "blockers": STRINGS, "missing_material": STRINGS}, ["overview", "recent_changes", "short_history", "next_steps", "evidence_refs", "through_event"])),
    "significant_evidence": ("Report new material evidence with its concrete source ID; repeated wording is not new evidence.", _schema({"source_event_id": {"type": "integer"}, "summary": STR, "evidence_refs": STRINGS}, ["source_event_id", "summary"])),
    "record_reproductions": ("Record every known flag reproduction before broader review; each item needs request_id, status (reproduced/failed/blocked/unchecked), evidence_refs, and acquisition_steps (reproduced) or reason (others). Reproduced requires current-review successful acquisition tool output containing the known flag, never echo/copy of an earlier flag. The result includes a checkpoint event. Then inspect missed routes before complete_review.", _schema({"reproductions": {"type": "array", "items": {"type": "object"}}}, ["reproductions"])),
    "record_review_progress": ("Persist individual reproduction paths as they finish, then audit sections and actual execution evidence after the reproduction checkpoint; publish before complete_review so interrupted reviews remain usable.", _schema({"sections": {"type": "object"}, "audit_evidence_refs": STRINGS, "reproductions": {"type": "array", "items": {"type": "object"}}})),
    "complete_review": ("After record_reproductions, publish independent reproduction outcomes and concrete corrections with evidence. Each reproduction includes request_id, status (reproduced/failed/blocked/unchecked/not_applicable), and evidence_refs. Cover every requested audit section in sections with results or explicit reasons for unchecked areas; reference actual subsequent audit executions in audit_evidence_refs.", _schema({"review_id": STR, "summary": STR, "evidence_refs": STRINGS, "reproductions": {"type": "array", "items": {"type": "object"}}, "corrections": STRINGS, "output_path": STR, "sections": {"type": "object"}, "results": {"type": "object"}, "audit_evidence_refs": STRINGS}, ["summary", "evidence_refs", "reproductions"])),
}
ROLE_TOOLS = {
    "solver": [],
    "coordinator": ["check_messages", "ack_messages", "get_solve_status", "check_swarm_status", "read_solver_trace", "bump_agent", "broadcast", "submit_flag", "get_report", "read_evidence", "search_evidence", "read_acquisition", "record_acquisition", "significant_evidence"],
    "reporter": ["check_messages", "ack_messages", "get_report", "read_evidence", "search_evidence", "read_acquisition", "publish_report", "significant_evidence"],
    "reviewer": ["check_messages", "ack_messages", "get_report", "read_evidence", "search_evidence", "read_acquisition", "record_acquisition", "submit_flag", "notify_coordinator", "record_reproductions", "record_review_progress", "complete_review", "significant_evidence"],
}
HUMAN_INTERACTION_TOOLS = ("AskUserQuestion", "EnterPlanMode", "ExitPlanMode")
BLOCKED_TOOLS = ["Read", "Write", "Edit", "MultiEdit", "NotebookEdit", "Glob", "Grep", "Agent", "Task", "TaskCreate", "TaskUpdate", "TaskGet", "TaskList", "TaskOutput", "TaskStop", "SendMessage", "WebFetch", "WebSearch", "Workflow", "CronCreate", "CronDelete", "CronList", *HUMAN_INTERACTION_TOOLS]
WEB_TOOLS = ("WebFetch", "WebSearch")


def client_environment(model_id: str, config_dir: Path, *, provider: str | None = None, environ=None) -> dict[str, str]:
    from ..model_connection import connection
    try:
        _, key, base = connection(os.environ if environ is None else environ, provider=provider)
    except ValueError as error:
        raise DriverError(str(error)) from None
    env = {"ANTHROPIC_BASE_URL": base,
           "ANTHROPIC_API_KEY": key, "ANTHROPIC_AUTH_TOKEN": key,
           "ANTHROPIC_MODEL": model_id, "ANTHROPIC_DEFAULT_OPUS_MODEL": model_id,
           "ANTHROPIC_DEFAULT_SONNET_MODEL": model_id, "ANTHROPIC_DEFAULT_HAIKU_MODEL": model_id,
           "ANTHROPIC_SMALL_FAST_MODEL": model_id, "CLAUDE_CODE_SUBAGENT_MODEL": model_id,
           "CLAUDE_CONFIG_DIR": str(config_dir), "CLAUDECODE": "",
           "CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS": "1",
           "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
           "CLAUDE_CODE_USE_BEDROCK": "", "CLAUDE_CODE_USE_VERTEX": "", "CLAUDE_CODE_USE_FOUNDRY": ""}
    return env


SDK_OS_ENV = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL", "TZ",
              "SYSTEMROOT", "WINDIR", "SSL_CERT_FILE", "SSL_CERT_DIR", "NODE_EXTRA_CA_CERTS",
              "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy")
SDK_CLIENT_ENV = ("ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_MODEL",
                  "ANTHROPIC_DEFAULT_OPUS_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL", "ANTHROPIC_DEFAULT_HAIKU_MODEL",
                  "ANTHROPIC_SMALL_FAST_MODEL", "CLAUDE_CODE_SUBAGENT_MODEL", "CLAUDE_CONFIG_DIR", "CLAUDECODE",
                  "CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
                  "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY")


def write_sdk_launcher(path: Path, bundled_cli: Path, *, own_session: bool = True) -> None:
    """Sanitize at exec, after the SDK merges options.env with os.environ.

    An options.env mapping cannot remove inherited keys in the pinned SDK.
    Only OS essentials and this client's selected Anthropic aliases survive.
    No raw provider credentials or benchmark token reaches the CLI process.
    """
    cli = str(bundled_cli)
    code = ("import os,sys; " + ("os.setsid(); " if own_session else "")
            + "env={k:v for k,v in os.environ.items() if k in " + repr(SDK_OS_ENV + SDK_CLIENT_ENV) + "}; "
            "os.execve(" + repr(cli) + ", [" + repr(cli) + ", *sys.argv[1:]], env)")
    path.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + " -c " + shlex.quote(code) + ' "$@"\n')
    path.chmod(0o700)


async def _maybe(callback, *args):
    if callback:
        result = callback(*args)
        return await result if inspect.isawaitable(result) else result
    return None


async def _processes() -> dict[int, tuple[int, str, int]]:
    proc = await asyncio.create_subprocess_exec("ps", "-axo", "pid=,ppid=,pgid=,lstart=,comm=", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    output, _ = await proc.communicate()
    result = {}
    for line in output.decode(errors="replace").splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and all(p.isdigit() for p in parts[:3]):
            result[int(parts[0])] = (int(parts[1]), parts[3], int(parts[2]))
    return result


class VeriaRoleDriver:
    def __init__(self, agent_id: str, role: str, model_id: str, workspace: str | Path,
                 context: dict[str, Any], environment: Any, on_tool: Callable,
                 on_session: Callable | None = None, on_event: Callable | None = None,
                 session_id: str | None = None, *, client_factory: Callable | None = None) -> None:
        if role not in ROLE_TOOLS:
            raise ValueError(f"Unknown Veria role: {role}")
        self.agent_id, self.role, self.model_id = agent_id, role, model_id
        self.provider = context.get("provider")
        self.workspace = Path(workspace).resolve()
        self.context, self.environment = context, environment
        review_id = (context.get("review") or {}).get("review_id") if isinstance(context.get("review"), dict) else None
        self.tool_member = ("reviewer-" + hashlib.sha256(str(review_id).encode()).hexdigest()[:16]
                            if role == "reviewer" and review_id else agent_id)
        self.on_tool, self.on_session, self.on_event = on_tool, on_session, on_event
        self.session_id, self.client_factory = session_id, client_factory
        self.client = None
        self.generation = str(uuid.uuid4())
        self.turn_id: str | None = None
        self._turn_lock = asyncio.Lock()
        self._reader: asyncio.Task | None = None
        self._active: asyncio.Future | None = None
        self._active_text: list[str] = []
        self._error: BaseException | None = None
        self._closing = False
        self._steps = 0
        self._post_steps = 0
        self._tool_steps: dict[str, int] = {}
        self._container_commands: dict[str, dict[str, str]] = {}
        self._loop = LoopDetector()
        self._owned_processes: dict[int, str] = {}
        self._process_group: int | None = None
        self._secrets = [v for k in ("GLM_API_KEY", "DEEPSEEK_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "BENCHMARK_TOKEN") if (v := os.environ.get(k))]

    @property
    def alive(self) -> bool:
        return bool(self.client and not self._closing and self._error is None and self._reader and not self._reader.done())

    @property
    def config_dir(self) -> Path:
        return self.workspace / ".claude"

    @property
    def web_tools(self) -> list[str]:
        """Native network tools are explicit environment capabilities.

        An unknown or hosted environment must never silently inherit the host's
        public network. SDK web tools do not execute inside the tool container.
        """
        description = getattr(self.environment, "_description", {}) or {}
        if (self.role not in {"solver", "reviewer", "coordinator"}
                or description.get("execution_mode") == "hosted_process"
                or description.get("network_mode") == "none"):
            return []
        declared = description.get("web_tools", [])
        return [name for name in WEB_TOOLS if name in declared] if isinstance(declared, list) else []

    def session_files_available(self, session_id: str | None = None) -> bool:
        session_id = session_id or self.session_id
        return bool(session_id and any(path.stat().st_size > 0 for path in self.config_dir.rglob(f"{session_id}.jsonl")))

    def _redact(self, value: Any) -> Any:
        if isinstance(value, str):
            for secret in self._secrets:
                value = value.replace(secret, "[REDACTED]")
            return value
        if isinstance(value, dict):
            return {k: "[REDACTED]" if re.search(r"api.?key|auth.?token|authorization", str(k), re.I) else self._redact(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._redact(v) for v in value]
        return value

    async def _event(self, kind: str, payload: dict[str, Any]) -> Any:
        return await _maybe(self.on_event, kind, self._redact({"agent_id": self.agent_id, "model_id": self.model_id,
                        "provider": self.provider, "generation": self.generation, "turn_id": self.turn_id, **payload}))

    def _save_session(self) -> None:
        self.workspace.mkdir(parents=True, exist_ok=True)
        record = {"agent_id": self.agent_id, "role": self.role, "model_id": self.model_id, "provider": self.provider,
                  "generation": self.generation, "session_id": self.session_id,
                  "config_dir": str(self.config_dir), "processes": self._owned_processes,
                  "process_group": self._process_group}
        path = self.workspace / "sdk-session.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, ensure_ascii=False, indent=2))
        os.replace(temporary, path)

    async def _set_session(self, value: str | None) -> None:
        if value and value != self.session_id:
            self.session_id = value
            self._save_session()
            await _maybe(self.on_session, value)
            await self._event("session", {"session_id": value, "config_dir": str(self.config_dir)})

    async def _remember_processes(self) -> None:
        transport = getattr(self.client, "_transport", None)
        process = getattr(transport, "_process", None)
        pid = getattr(process, "pid", None)
        if not pid:
            return
        processes = await _processes()
        owned = {pid}
        while True:
            children = {p for p, (parent, _, _) in processes.items() if parent in owned}
            if children <= owned:
                break
            owned |= children
        for p in owned:
            if p in processes:
                self._owned_processes[p] = processes[p][1]
        if pid in processes and processes[pid][2] == pid:
            self._process_group = pid
        self._save_session()

    async def _cleanup_processes(self, owned: dict[int, str] | None = None,
                                 group: int | None = None) -> None:
        owned = self._owned_processes if owned is None else owned
        group = self._process_group if group is None else group
        # Stored birth+executable identity must still match before each signal.
        for sig in (signal.SIGTERM, signal.SIGKILL):
            processes = await _processes()
            valid_group = bool(group and group != os.getpgrp() and any(
                pid in processes and processes[pid][1] == identity and processes[pid][2] == group
                for pid, identity in owned.items()))
            if valid_group:
                try:
                    os.killpg(group, sig)
                except ProcessLookupError:
                    pass
            for pid, identity in owned.items():
                if pid in processes and processes[pid][1] == identity and pid != os.getpid():
                    try:
                        os.kill(pid, sig)
                    except ProcessLookupError:
                        pass
            if sig == signal.SIGTERM:
                await asyncio.sleep(0.05)

    async def _recover(self) -> None:
        path = self.workspace / "sdk-session.json"
        if path.exists():
            old = json.loads(path.read_text())
            if (old.get("agent_id") != self.agent_id or old.get("model_id") != self.model_id
                    or old.get("provider", self.provider) != self.provider):
                raise DriverError("Saved role identity/model/provider does not match this attempt")
            await self._cleanup_processes({int(p): identity for p, identity in old.get("processes", {}).items()}, old.get("process_group"))
            self.session_id = self.session_id or old.get("session_id")
        if self.session_id and not self.session_files_available():
            previous = self.session_id
            self.session_id = None
            await self._event("session_lineage", {"previous_session_id": previous, "reason": "session_files_unavailable", "resumed": False})

    @staticmethod
    def _deny(reason: str) -> dict[str, Any]:
        return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": reason}}

    async def pre_tool(self, input_data, tool_use_id, context=None):
        try:
            if input_data.get("hook_event_name") != "PreToolUse":
                return self._deny("Unexpected hook event; tool execution refused")
            name = str(input_data.get("tool_name", ""))
            arguments = input_data.get("tool_input", {})
            if not isinstance(arguments, dict):
                return self._deny("Invalid tool input")
            self._steps += 1
            self._tool_steps[str(tool_use_id)] = self._steps
            await self._event("tool_call", {"tool_use_id": tool_use_id, "name": name, "arguments": arguments, "step": self._steps})
            if name in HUMAN_INTERACTION_TOOLS:
                return self._deny("This competition runs unattended: no human answer or plan approval is available. "
                                  "Continue authorized challenge work autonomously. Route team coordination through "
                                  "the coordinator; record any real blocker and continue other useful work.")
            if name == "StructuredOutput" and self.role == "solver":
                # The pinned CLI exposes the upstream JSON output schema as a
                # synthetic tool. It performs no shell/filesystem operation;
                # denying it makes valid completion exhaust five SDK retries.
                schema = solver_output_json_schema()
                if (set(arguments) != set(schema["required"])
                        or arguments.get("type") != "flag_found"
                        or any(not isinstance(arguments.get(key), str) for key in schema["required"])):
                    return self._deny("StructuredOutput does not match the pinned solver output schema")
                return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "allow"}}
            allowed_mcp = {f"mcp__veria__{item}" for item in ROLE_TOOLS[self.role]}
            if name in allowed_mcp:
                return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "allow"}}
            if name in self.web_tools and self.role in {"solver", "reviewer", "coordinator"}:
                loop_status = self._loop.check(name, arguments)
                if loop_status == "break":
                    return self._deny("Loop detected — try a different approach.")
                result = {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "allow"}}
                if loop_status == "warn":
                    result["systemMessage"] = LOOP_WARNING_MESSAGE
                return result
            if name != "Bash" or self.role not in {"solver", "reviewer", "coordinator"}:
                return self._deny(f"{name} is unavailable for this role and environment; use container Bash for filesystem work")
            command = arguments.get("command")
            if not isinstance(command, str):
                return self._deny("Bash command must be a string")
            special = parse_special_command(command)
            if special:
                tool, value = special
                if tool == "record_acquisition":
                    payload = json.loads(value)
                    if not isinstance(payload, dict):
                        raise SpecialCommandError("record_acquisition requires one JSON object")
                else:
                    payload = {"flag" if tool == "submit_flag" else "message": value}
                response = await asyncio.wait_for(self.on_tool(tool, payload), timeout=90)
                await self._event("tool_result", {"tool_use_id": tool_use_id, "name": tool, "result": response})
                # Only a quoted constant is executed on the host. No user shell
                # syntax or platform call is ever interpolated into this command.
                rewritten = "printf '%s\\n' " + shlex.quote(json.dumps(response, ensure_ascii=False, default=str))
            else:
                loop_status = self._loop.check(name, arguments)
                if loop_status == "break":
                    return self._deny("Loop detected — try a different approach.")
                rewritten = await self.environment.prepare_command(self.tool_member, command, str(tool_use_id))
                command_id = (self.environment.command_id(self.tool_member, str(tool_use_id))
                              if hasattr(self.environment, "command_id") else str(tool_use_id))
                self._container_commands[str(tool_use_id)] = {"command": command, "command_id": command_id}
                await self._event("tool_execution_registered", {"tool_use_id": tool_use_id, "command_id": command_id, "name": "Bash", "command": command, "tool_member": self.tool_member})
            result = {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "allow", "updatedInput": {**arguments, "command": rewritten}}}
            if not special and loop_status == "warn":
                result["systemMessage"] = LOOP_WARNING_MESSAGE
            await self._remember_processes()
            if not special and not getattr(self.environment, "accepting_commands", True):
                raise DriverError("Tool dispatch paused before the rewritten command was released")
            return result
        except asyncio.CancelledError:
            registration = self._container_commands.pop(str(tool_use_id), {})
            if hasattr(self.environment, "abandon_command"):
                try:
                    self.environment.abandon_command(registration.get("command_id", str(tool_use_id)))
                except Exception:
                    pass
            return self._deny("Tool routing cancelled; host execution refused")
        except Exception as exc:
            registration = self._container_commands.pop(str(tool_use_id), {})
            if hasattr(self.environment, "abandon_command"):
                try:
                    self.environment.abandon_command(registration.get("command_id", str(tool_use_id)))
                except Exception:
                    pass
            try:
                await self._event("tool_routing_error", {"error": f"{type(exc).__name__}: {exc}"})
            except Exception:
                pass
            return self._deny(self._redact(f"Tool routing failed closed: {type(exc).__name__}: {exc}"))

    async def post_tool(self, input_data, tool_use_id, context=None):
        try:
            additional = []
            response = input_data.get("tool_response")
            await self._remember_processes()
            registration = self._container_commands.pop(str(tool_use_id), None)
            if registration is not None:
                self.environment.complete_command(registration["command_id"], response)
                failed = input_data.get("hook_event_name") != "PostToolUse" or response is None
                if isinstance(response, dict):
                    failed = failed or bool(response.get("is_error") or response.get("isError") or response.get("interrupted"))
                    failed = failed or response.get("returncode", response.get("exit_code", response.get("exitCode", 0))) != 0
                record = await self._event("tool_execution_result", {"tool_use_id": tool_use_id,
                    "command_id": registration["command_id"], "command": registration["command"], "result": response,
                    "success": not failed, "tool_member": self.tool_member})
                if self.role == "reviewer" and record:
                    additional.append("This actual tool execution was persisted as evidence (reference its path/ID for the relevant reproduction; execution success alone does not prove flag acquisition): " + json.dumps(record, ensure_ascii=False, default=str))
            await self._event("tool_result", {"tool_use_id": tool_use_id, "name": input_data.get("tool_name"), "result": response, "step": self._tool_steps.pop(str(tool_use_id), None)})
            self._post_steps += 1
            if self.role in {"solver", "reviewer"} and self._post_steps % 5 == 0:
                messages = await self.on_tool("check_messages", {})
                if messages:
                    await self._event("messages_injected", {"messages": messages})
                    additional.append(json.dumps(messages, ensure_ascii=False, default=str))
            if additional:
                return {"hookSpecificOutput": {"hookEventName": input_data.get("hook_event_name", "PostToolUse"), "additionalContext": "\n\n".join(additional)}}
        except Exception as exc:
            try:
                await self._event("post_tool_error", {"error": f"{type(exc).__name__}: {exc}"})
            except Exception:
                pass
        return {}

    async def start(self) -> None:
        if self.client:
            if self.alive:
                return
            raise DriverError("Create a new driver after a closed or failed SDK client")
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(exist_ok=True)
        await self._recover()
        from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, HookMatcher, create_sdk_mcp_server, tool
        import claude_agent_sdk
        registered = []
        for name in ROLE_TOOLS[self.role]:
            description, schema = TOOL_SPECS[name]
            def handler_for(tool_name):
                async def handler(args):
                    response = await self.on_tool(tool_name, args)
                    result = {"content": [{"type": "text", "text": json.dumps(response, ensure_ascii=False, default=str)}]}
                    if isinstance(response, dict) and (response.get("is_error") or response.get("isError")):
                        result["isError"] = True
                    return result
                return handler
            registered.append(tool(name, description, schema)(handler_for(name)))
        server = create_sdk_mcp_server("veria", tools=registered) if registered else None
        bundled_cli = Path(claude_agent_sdk.__file__).parent / "_bundled" / ("claude.exe" if os.name == "nt" else "claude")
        if not bundled_cli.is_file():
            raise DriverError("Pinned SDK bundled CLI is missing; no global CLI fallback")
        # The SDK transport does not create a process group. This tiny launcher
        # execs only the explicitly resolved bundled CLI into an owned session,
        # so stop can terminate its host Bash descendants without broad pkill.
        launcher = self.workspace / "launch-sdk"
        write_sdk_launcher(launcher, bundled_cli)
        env = client_environment(self.model_id, self.config_dir, provider=self.provider) if self.client_factory is None else {
            "CLAUDE_CONFIG_DIR": str(self.config_dir), "ANTHROPIC_MODEL": self.model_id}
        builtins = ["Bash", *self.web_tools] if self.role in {"solver", "reviewer", "coordinator"} else []
        effective_environment = dict(getattr(self.environment, "_description", {}) or {})
        if "web_tools" in effective_environment:
            effective_environment["web_tools"] = self.web_tools
        system_prompt = build_role_prompt(self.role, self.tool_member, self.context,
                                         effective_environment)
        # The pinned SDK reads this exact replacement prompt with
        # --system-prompt-file. Keeping the full challenge out of CLI argv also
        # prevents ordinary `pkill -f 'curl.*TARGET'` cleanup from matching and
        # killing every SDK process whose prompt mentions that target.
        # No default/append prompt or second shortening layer is introduced.
        prompt_path = self.workspace / "sdk-system-prompt.txt"
        prompt_path.write_text(system_prompt, encoding="utf-8")
        await self._event("system_prompt", {"path": str(prompt_path),
            "sha256": hashlib.sha256(system_prompt.encode()).hexdigest(),
            "characters": len(system_prompt), "builtin_tools": builtins,
            "transport": "system_prompt_file"})
        options = ClaudeAgentOptions(
            model=self.model_id, fallback_model=None, cwd=str(self.workspace), cli_path=str(launcher),
            resume=self.session_id, env=env, tools=builtins,
            # Bash is deliberately NOT auto-allowed. Only a successful
            # PreToolUse hook may grant its rewritten input. If the hook times
            # out or disappears, dontAsk denies the original host command.
            allowed_tools=[f"mcp__veria__{name}" for name in ROLE_TOOLS[self.role]],
            disallowed_tools=[name for name in BLOCKED_TOOLS if name not in builtins],
            mcp_servers={"veria": server} if server else {},
            strict_mcp_config=True, setting_sources=[], permission_mode="dontAsk",
            system_prompt={"type": "file", "path": str(prompt_path)},
            output_format={"type": "json_schema", "schema": solver_output_json_schema()}
                if self.role == "solver" else None,
            hooks={"PreToolUse": [HookMatcher(hooks=[self.pre_tool], timeout=120)],
                   "PostToolUse": [HookMatcher(hooks=[self.post_tool], timeout=120)],
                   "PostToolUseFailure": [HookMatcher(hooks=[self.post_tool], timeout=120)]},
            stderr=lambda line: None, verbatim_prompts=True)
        self.client = self.client_factory(options) if self.client_factory else ClaudeSDKClient(options=options)
        try:
            await self.client.connect()
        finally:
            await self._remember_processes()
        self._reader = asyncio.create_task(self._receive(), name=f"veria-sdk-reader:{self.agent_id}")
        await self._event("started", {"requested_model": self.model_id, "session_id": self.session_id})

    async def _receive(self) -> None:
        try:
            async for message in self.client.receive_messages():
                kind = type(message).__name__
                data = getattr(message, "data", {}) or {}
                await self._set_session(getattr(message, "session_id", None) or (data.get("session_id") if isinstance(data, dict) else None))
                if kind == "AssistantMessage":
                    observed_model = getattr(message, "model", None)
                    for block in getattr(message, "content", []):
                        text = getattr(block, "text", None)
                        if isinstance(text, str):
                            self._active_text.append(text)
                            await self._event("assistant_text", {"text": text, "returned_model": observed_model})
                    if getattr(message, "error", None):
                        await self._event("assistant_error", {"error": str(message.error), "returned_model": observed_model})
                elif kind == "ResultMessage":
                    subtype = str(getattr(message, "subtype", "") or "")
                    terminal_reason = getattr(message, "terminal_reason", None)
                    is_error = bool(getattr(message, "is_error", False) or subtype.startswith("error_")
                                    or terminal_reason in {"api_error", "aborted_streaming", "aborted_tools"})
                    result = {"result": getattr(message, "result", "") or "\n".join(self._active_text),
                              "visible_text": "\n".join(self._active_text),
                              "session_id": self.session_id, "turn_id": self.turn_id,
                              "is_error": is_error,
                              "structured_output": getattr(message, "structured_output", None),
                              "usage": getattr(message, "usage", None), "total_cost_usd": getattr(message, "total_cost_usd", None)}
                    # Preserve the SDK's termination diagnostics. In particular,
                    # API errors can have no result text; dropping errors/status
                    # here would make recovery and hosted logs report only a
                    # generic failure instead of authentication/rate-limit facts.
                    for field in ("subtype", "terminal_reason", "stop_reason", "api_error_status", "errors", "permission_denials", "num_turns"):
                        result[field] = self._redact(getattr(message, field, None))
                    if result["is_error"]:
                        details = [str(item) for item in (result["errors"] or [])]
                        details.extend(f"{field}={result[field]}" for field in
                                       ("subtype", "terminal_reason", "api_error_status") if result[field] is not None)
                        if not details:
                            details = [result["result"] or "SDK turn failed"]
                        result["error"] = self._redact("; ".join(details))
                    await self._event("turn_result", result)
                    if self._active and not self._active.done():
                        self._active.set_result(result)
            if not self._closing:
                raise DriverError("SDK output reader ended")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error = exc
            if self._active and not self._active.done():
                self._active.set_exception(DriverError(self._redact(str(exc))))
            await self._event("sdk_error", {"error": f"{type(exc).__name__}: {exc}"})

    async def turn(self, prompt: str) -> dict[str, Any]:
        async with self._turn_lock:
            if not self.client:
                await self.start()
            if not self.alive:
                raise DriverError("SDK client cannot accept another turn")
            self.turn_id = str(uuid.uuid4())
            self._active_text = []
            self._active = asyncio.get_running_loop().create_future()
            try:
                await self.client.query(prompt)
                return await asyncio.shield(self._active)
            except asyncio.CancelledError:
                await self.interrupt()
                raise
            finally:
                self._active = None

    async def interrupt(self, timeout: float = 4) -> None:
        if not self.client:
            return
        try:
            await asyncio.wait_for(self.client.interrupt(), timeout)
            if self._active and not self._active.done():
                # Drain the old ResultMessage before any later query.
                await asyncio.wait_for(asyncio.shield(self._active), timeout)
        except Exception:
            await self.close()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        errors = []
        try:
            try:
                await asyncio.wait_for(self._remember_processes(), 2)
            except Exception as exc:
                errors.append(f"process inventory: {exc}")
            if self.client:
                try:
                    await asyncio.wait_for(self.client.disconnect(), 4)
                except Exception as exc:
                    errors.append(f"SDK disconnect: {exc}")
        finally:
            try:
                await asyncio.wait_for(self._cleanup_processes(), 3)
            except Exception as exc:
                errors.append(f"SDK process cleanup: {exc}")
            try:
                if self.environment:
                    await asyncio.wait_for(self.environment.stop_member(self.tool_member), 8)
            except Exception as exc:
                errors.append(f"container member cleanup: {exc}")
            if self._reader and self._reader is not asyncio.current_task():
                self._reader.cancel()
                await asyncio.gather(self._reader, return_exceptions=True)
            if self._active and not self._active.done():
                self._active.set_exception(DriverError("Role closed"))
                self._active.exception()  # Mark observed even if caller was cancelled.
            self._save_session()
            await self._event("closed", {"session_id": self.session_id, "cleanup_errors": errors})
            if errors:
                raise DriverError(self._redact("; ".join(errors)))
