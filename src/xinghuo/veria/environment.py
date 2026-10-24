"""Attempt-owned Docker tools; member handles never own the shared container.

All subprocess calls use argv.  Parallel commands only share a short state-file
write, never an execution lock.  Files survive container removal and recovery.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import time
from typing import Any, Awaitable, Callable
import uuid

from ..config import VERIA_CONTAINER_INSPECT_TIMEOUT, VERIA_CONTAINER_REMOVE_TIMEOUT, VERIA_MEMBER_CLEANUP_TIMEOUT


class EnvironmentError(RuntimeError):
    pass


Runner = Callable[..., Awaitable[dict[str, Any]]]


async def _run(argv: list[str], timeout: float = 120) -> dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    return {"returncode": process.returncode, "stdout": stdout.decode(errors="replace"),
            "stderr": stderr.decode(errors="replace")}


def _get(settings: Any, name: str, default: Any = None) -> Any:
    return settings.get(name, default) if isinstance(settings, dict) else getattr(settings, name, default)


def _member(agent_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", agent_id):
        raise EnvironmentError("Invalid member identifier")
    return agent_id


class SharedDockerEnvironment:
    def __init__(self, directory: str | Path, run_id: str, code: str,
                 attempt_id: str, settings: Any, *, runner: Runner | None = None) -> None:
        self.directory = Path(directory).resolve()
        self.run_id, self.code, self.attempt_id = str(run_id), str(code), str(attempt_id)
        self.settings = settings
        self.runner = runner or _run
        self.image = _get(settings, "veria_tool_image")
        self.cpu = _get(settings, "veria_tool_cpu_limit")
        self.memory = _get(settings, "veria_tool_memory_limit")
        self.network = _get(settings, "veria_tool_network", "bridge")
        self.labels = {"xinghuo.run": self.run_id, "xinghuo.challenge": self.code,
                       "xinghuo.attempt": self.attempt_id, "xinghuo.engine": "veria"}
        self.state_path = self.directory / "environment.json"
        self.command_archive_path = self.directory / "command-history.jsonl"
        self.container_id = ""
        self.image_id = ""
        self.generation = str(uuid.uuid4())
        self.commands: dict[str, dict[str, Any]] = {}
        self._quiescing = False
        self._closed = False
        self._members: set[str] = set()
        self._member_locks: dict[str, asyncio.Lock] = {}
        self._description: dict[str, Any] = {}

    def _save(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        self._archive_cleaned()
        data = {"version": 1, "labels": self.labels, "generation": self.generation,
                "container_id": self.container_id, "image_id": self.image_id,
                "commands": self.commands, "description": self._description,
                "commands_archive": {"path": str(self.command_archive_path),
                                     "format": "jsonl", "dedup_key": "archive_id"}}
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        os.replace(temporary, self.state_path)

    def _archive_cleaned(self) -> None:
        """Retire only proven-clean ownership, preserving full append-only history.

        shell_returned is intentionally still active: its detached descendants
        may be running. Append+fsync precedes index removal. A crash before the
        environment snapshot replaces can repeat an archive record on recovery;
        archive_id is stable so readers can deduplicate that at-least-once case.
        """
        cleaned = [(identifier, entry) for identifier, entry in self.commands.items()
                   if entry.get("status") == "cleaned"]
        if not cleaned:
            return
        with self.command_archive_path.open("a", encoding="utf-8") as stream:
            for identifier, entry in cleaned:
                archived = {**entry, "archive_id": f"{entry['generation']}:{identifier}",
                            "archived_at": time.time(), "labels": self.labels,
                            "control_record": str(self.directory / "control" / (identifier + ".json")),
                            "dispatch_record": str(self.directory / "control" / ("dispatch-" + identifier + ".json"))}
                stream.write(json.dumps(archived, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        for identifier, _ in cleaned:
            del self.commands[identifier]

    def _control_write(self, name: str, payload: dict[str, Any]) -> None:
        directory = self.directory / "control"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload))
        os.replace(temporary, path)

    def _command_permit(self, command_id: str, allowed: bool) -> None:
        entry = self.commands[command_id]
        self._control_write("dispatch-" + command_id + ".json", {
            "generation": entry["generation"], "member": entry["agent_id"],
            "command_id": command_id, "allowed": allowed})

    async def _inspect(self, identifier: str) -> dict[str, Any] | None:
        result = await self.runner(["docker", "inspect", identifier], timeout=VERIA_CONTAINER_INSPECT_TIMEOUT)
        if result["returncode"]:
            # A disconnected daemon is not evidence that a container vanished.
            missing = "No such object" in result.get("stderr", "") or "No such container" in result.get("stderr", "")
            if missing:
                return None
            raise EnvironmentError("Docker inspect failed: " + result.get("stderr", "")[-1000:])
        values = json.loads(result["stdout"])
        return values[0] if values else None

    def _verify_owned(self, info: dict[str, Any]) -> None:
        actual = info.get("Config", {}).get("Labels") or {}
        if any(actual.get(k) != v for k, v in self.labels.items()):
            raise EnvironmentError("Container ownership mismatch; refusing to use or remove it")
        if actual.get("xinghuo.generation") != self.generation:
            raise EnvironmentError("Container generation mismatch")
        if self.image_id and info.get("Image") != self.image_id:
            raise EnvironmentError("Container image identity changed")

    @property
    def ready(self) -> bool:
        return bool(self.container_id)

    def helper_argv(self, *args: str, interactive: bool = False) -> list[str]:
        return ["docker", "exec", *(["-i"] if interactive else []), self.container_id,
                "python3", "/usr/local/bin/xinghuo_exec.py", *args]

    async def ensure_owned_environment(self) -> bool:
        if not self.container_id:
            return False
        info = await self._inspect(self.container_id)
        if not info:
            return False
        self._verify_owned(info)
        return True

    async def start(self) -> dict[str, Any]:
        if not self.image or not self.cpu or not self.memory:
            raise EnvironmentError("Veria tool image, CPU and memory limits must be configured")
        self.directory.mkdir(parents=True, exist_ok=True)
        for name in ("inputs", "members", "shared", "control"):
            (self.directory / name).mkdir(exist_ok=True)
        previous = json.loads(self.state_path.read_text()) if self.state_path.exists() else None
        if previous:
            if previous.get("labels") != self.labels:
                raise EnvironmentError("Saved tool environment belongs to another attempt")
            self.container_id = previous.get("container_id", "")
            self.image_id = previous.get("image_id", "")
            self.generation = previous["generation"]
            self.commands = previous.get("commands", {})
        # Close the dispatch gate before examining/recovering an old container.
        # Previously granted SDK commands must not start during reconciliation.
        self.quiesce(True)
        recovered = False
        if previous:
            existing = await self._inspect(self.container_id or f"xinghuo-veria-{self.generation[:16]}")
            if existing:
                self._verify_owned(existing)
                if not existing.get("State", {}).get("Running"):
                    removed = await self.runner(["docker", "rm", existing["Id"]], timeout=20)
                    if removed["returncode"]:
                        raise EnvironmentError("Cannot remove stopped owned container")
                    self.container_id = ""
                else:
                    self.container_id = existing["Id"]
                    recovered = True
            else:
                self.container_id = ""
        if not self.container_id:
            image_info = await self._inspect(str(self.image))
            if not image_info:
                raise EnvironmentError("Tool image is missing; prepare it before starting Veria")
            resolved_image_id = image_info["Id"]
            if self.image_id and resolved_image_id != self.image_id:
                raise EnvironmentError("Saved image is unavailable; refusing silent image replacement")
            self.image_id = resolved_image_id
            self.generation = str(uuid.uuid4())
            args = ["docker", "run", "-d", "--init", "--name", f"xinghuo-veria-{self.generation[:16]}",
                    "--cpus", str(self.cpu), "--memory", str(self.memory), "--network", self.network,
                    "--security-opt", "no-new-privileges", "--cap-drop", "ALL"]
            for key, value in {**self.labels, "xinghuo.generation": self.generation}.items():
                args += ["--label", f"{key}={value}"]
            for name in ("inputs", "members", "shared", "control"):
                args += ["--mount", f"type=bind,src={self.directory / name},dst=/challenge/{name}" + (",readonly" if name == "inputs" else "")]
            args += [self.image_id, "sleep", "infinity"]
            self._save()  # Ownership exists before launch; name enables crash recovery.
            result = await self.runner(args, timeout=60)
            if result["returncode"]:
                raise EnvironmentError("Docker start failed: " + result.get("stderr", "")[-1000:])
            self.container_id = result["stdout"].strip()
            self._save()
        description = await self.runner(["docker", "exec", self.container_id, "python3", "/usr/local/bin/xinghuo_exec.py", "describe"], timeout=20)
        if description["returncode"]:
            raise EnvironmentError("Tool image lacks the Veria execution helper")
        self._description = json.loads(description["stdout"])
        self._description.update({"network_mode": self.network,
                                  "cpu_limit": self.cpu, "memory_limit": self.memory,
                                  "internet_verified": False,
                                  "web_tools": [] if self.network == "none" else ["WebFetch", "WebSearch"],
                                  "web_tool_execution": "SDK host; availability is not proof of public connectivity",
                                  "shared_container_is_security_isolation": False})
        self._closed = False
        self._save()
        retired_execution_count = 0
        if recovered:
            # Do not assume a departed SDK Bash process stopped its background
            # services. Retain files, but retire old registered execution trees.
            retired = [entry for entry in self.commands.values()
                       if entry.get("status") not in {"cleaned", "not_dispatched"}]
            retired_execution_count = len(retired)
            for member in sorted({entry["agent_id"] for entry in retired}):
                await self._cleanup(member)
        self.quiesce(False)
        return {"container_id": self.container_id, "image_id": self.image_id,
                "description": self._description, "recovered": recovered,
                "retired_execution_count": retired_execution_count,
                "volatile_state_lost": bool((previous and not recovered) or retired_execution_count)}

    async def ensure_member(self, agent_id: str) -> None:
        agent_id = _member(agent_id)
        if agent_id in self._members:
            return
        lock = self._member_locks.setdefault(agent_id, asyncio.Lock())
        async with lock:
            if agent_id in self._members:
                return
            result = await self.runner(self.helper_argv("prepare", agent_id), timeout=60)
            if result["returncode"]:
                raise EnvironmentError("Member private environment preparation failed")
            self._members.add(agent_id)

    async def prepare_command(self, agent_id: str, command: str, tool_use_id: str | None = None) -> str:
        if self._quiescing or self._closed:
            raise EnvironmentError("Ordinary tools are paused while the attempt is quiescing or closed")
        if not self.ready:
            raise EnvironmentError("Tool container has not started")
        await self.ensure_member(agent_id)
        if not self.accepting_commands:
            raise EnvironmentError("Tool dispatch paused during private environment preparation")
        tool_use_id = str(tool_use_id or uuid.uuid4())
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", tool_use_id):
            raise EnvironmentError("Invalid tool command identifier")
        command_id = self.command_id(agent_id, tool_use_id)
        self.commands[command_id] = {"command_id": command_id, "agent_id": agent_id,
                                     "tool_use_id": tool_use_id,
                                     "generation": self.generation, "started_at": time.time(),
                                     "status": "registered"}
        self._command_permit(command_id, True)
        self._save()
        argv = self.helper_argv("run", "--member", agent_id, "--command-id", command_id,
                                "--generation", self.generation, "--", command, interactive=True)
        return shlex.join(argv)

    @property
    def accepting_commands(self) -> bool:
        return not self._quiescing and not self._closed

    def command_id(self, agent_id: str, tool_use_id: str) -> str:
        identity = json.dumps([self.generation, _member(agent_id), str(tool_use_id)], separators=(",", ":"))
        return hashlib.sha256(identity.encode()).hexdigest()

    def _resolve_command_id(self, identifier: str) -> str:
        if identifier in self.commands:
            return identifier
        # Backwards compatible for callers with an unambiguous old SDK ID.
        matches = [key for key, entry in self.commands.items() if entry.get("tool_use_id") == identifier]
        if len(matches) > 1:
            raise EnvironmentError("Ambiguous provider tool ID; use the member-scoped command ID")
        return matches[0] if matches else identifier

    def complete_command(self, command_id: str, result: Any = None) -> None:
        command_id = self._resolve_command_id(command_id)
        if command_id in self.commands:
            self.commands[command_id].update(status="shell_returned", finished_at=time.time())
            # Keep the ownership record: background descendants may still live.
            self._save()

    def abandon_command(self, command_id: str) -> None:
        """A hook failed before returning the rewritten command to the SDK."""
        command_id = self._resolve_command_id(command_id)
        if command_id in self.commands:
            self.commands[command_id].update(status="not_dispatched", finished_at=time.time())
            self._command_permit(command_id, False)
            self._save()

    async def execute(self, agent_id: str, command: str, timeout: float = 120) -> dict[str, Any]:
        tool_use_id = str(uuid.uuid4())
        rewritten = await self.prepare_command(agent_id, command, tool_use_id)
        command_id = self.command_id(agent_id, tool_use_id)
        try:
            result = await self.runner(shlex.split(rewritten), timeout=timeout)
            self.complete_command(command_id, result)
            return {**result, "command_id": command_id}
        except (asyncio.TimeoutError, asyncio.CancelledError):
            await self._cleanup(agent_id, command_id)
            raise

    def quiesce(self, value: bool) -> None:
        self._quiescing = bool(value)
        self._control_write("dispatch.json", {"generation": self.generation,
                            "allowed": not self._quiescing and not self._closed})

    async def _cleanup(self, agent_id: str, command_id: str | None = None) -> None:
        for identifier, entry in self.commands.items():
            if entry["agent_id"] == agent_id and (not command_id or identifier == command_id):
                self._command_permit(identifier, False)
        if not await self.ensure_owned_environment():
            return
        argv = self.helper_argv("cleanup", "--member", _member(agent_id), "--generation", self.generation)
        if command_id:
            argv += ["--command-id", command_id]
        result = await self.runner(argv, timeout=VERIA_MEMBER_CLEANUP_TIMEOUT)
        if result["returncode"]:
            raise EnvironmentError("Member descendant cleanup failed")
        for entry in self.commands.values():
            if entry["agent_id"] == agent_id and (not command_id or entry["command_id"] == command_id):
                entry.update(status="cleaned", cleaned_at=time.time(), cleanup_reason="member_descendants_retired")
        self._save()

    async def stop_member(self, agent_id: str) -> None:
        members = {agent_id}
        if agent_id == "reviewer":
            members.update(entry["agent_id"] for entry in self.commands.values()
                           if entry["agent_id"].startswith("reviewer-"))
        results = await asyncio.gather(*(self._cleanup(member) for member in sorted(members)),
                                       return_exceptions=True)
        failures = [str(result) for result in results if isinstance(result, BaseException)]
        if failures:
            raise EnvironmentError("Member descendant cleanup incomplete: " + "; ".join(failures))

    async def stop(self) -> None:
        self.quiesce(True)
        if self.container_id:
            info = await self._inspect(self.container_id)
            if info:
                self._verify_owned(info)
                result = await self.runner(["docker", "rm", "-f", self.container_id], timeout=VERIA_CONTAINER_REMOVE_TIMEOUT)
                if result["returncode"]:
                    raise EnvironmentError("Failed to remove owned container")
        self._closed = True
        for entry in self.commands.values():
            entry.update(status="cleaned", cleaned_at=time.time(), cleanup_reason="container_removed_or_absent")
        self._save()

    async def health(self) -> dict[str, Any]:
        if not self.container_id or self._closed:
            return {"alive": False, "healthy": False, "running": False, "container_id": self.container_id, "quiescing": self._quiescing}
        info = await self._inspect(self.container_id)
        if info:
            self._verify_owned(info)
        running = bool(info and info.get("State", {}).get("Running"))
        return {"alive": running, "healthy": running, "running": running,
                "container_id": self.container_id, "image_id": self.image_id,
                "quiescing": self._quiescing, "description": self._description}
