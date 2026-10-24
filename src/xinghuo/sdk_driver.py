"""A small process boundary around one managed Claude Agent SDK session.

The subprocess protocol is also used by the explicit demo backend.  An
``AgentProcess`` never claims that a demo turn was a Claude SDK invocation.
"""

from __future__ import annotations

import asyncio
from collections import deque
import json
import os
from pathlib import Path
import signal
import sys
from typing import Any, Awaitable, Callable
import uuid


ToolHandler = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]


class AgentProcessError(RuntimeError):
    """The role process died or violated its wire protocol."""


class AgentProcess:
    """Own one role process and serialize turns on its SDK session.

    Business timeouts belong to the caller.  Cancelling a turn invalidates the
    process: call ``close()`` and create a new instance, optionally with its last
    ``session_id``, before sending more work.
    """

    def __init__(
        self,
        role: str,
        agent_id: str,
        cwd: str | Path,
        backend: str = "demo",
        model: str | None = None,
        session_id: str | None = None,
        on_tool: ToolHandler | None = None,
    ) -> None:
        self.role = role
        self.agent_id = agent_id
        self.cwd = Path(cwd).resolve()
        self.backend = backend
        self.model = model
        self.session_id = session_id
        self.on_tool = on_tool
        self.process: asyncio.subprocess.Process | None = None
        self.stderr_tail: deque[str] = deque(maxlen=80)
        self.observations: deque[dict[str, Any]] = deque(maxlen=1000)
        self._reader: asyncio.Task[None] | None = None
        self._stderr_reader: asyncio.Task[None] | None = None
        self._tool_tasks: set[asyncio.Task[None]] = set()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._turn_lock = asyncio.Lock()
        self._closing = False
        self._invalid = False

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.process else None

    @property
    def alive(self) -> bool:
        return bool(self.process and self.process.returncode is None and not self._invalid)

    def poll_observations(self) -> list[dict[str, Any]]:
        """Consume metadata-only tool telemetry without reading SDK output again."""
        observations = list(self.observations)
        self.observations.clear()
        return observations

    async def start(self) -> None:
        if self.process is not None:
            if self.alive:
                return
            raise AgentProcessError("Create a new AgentProcess after a stopped process")
        if not self.cwd.is_dir():
            raise AgentProcessError(f"Agent working directory does not exist: {self.cwd}")
        command = [
            sys.executable, "-u", "-m", "xinghuo.agent_runner",
            "--role", self.role, "--agent-id", self.agent_id,
            "--cwd", str(self.cwd), "--backend", self.backend,
        ]
        if self.model:
            command.extend(["--model", self.model])
        if self.session_id:
            command.extend(["--session-id", self.session_id])
        # Keep editable/source checkouts importable after changing cwd.  Do not
        # rewrite credential or user-home environment variables.
        env = os.environ.copy()
        package_root = str(Path(__file__).resolve().parent.parent)
        env["PYTHONPATH"] = os.pathsep.join(
            p for p in [package_root, env.get("PYTHONPATH", "")] if p
        )
        self.process = await asyncio.create_subprocess_exec(
            *command,
            cwd=self.cwd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=os.name == "posix",
            limit=4 * 1024 * 1024,
        )
        self._reader = asyncio.create_task(self._read_stdout(), name=f"agent-read:{self.agent_id}")
        self._stderr_reader = asyncio.create_task(self._read_stderr(), name=f"agent-log:{self.agent_id}")

    async def _send(self, payload: dict[str, Any]) -> None:
        if not self.process or self.process.returncode is not None or not self.process.stdin:
            raise AgentProcessError(f"Agent {self.agent_id} is not running")
        data = (json.dumps(payload, ensure_ascii=False, default=str) + "\n").encode()
        async with self._write_lock:
            try:
                self.process.stdin.write(data)
                await self.process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                raise AgentProcessError(f"Agent {self.agent_id} input closed") from exc

    async def turn(self, prompt: str) -> dict[str, Any]:
        async with self._turn_lock:
            if self.process is None:
                await self.start()
            if not self.alive:
                raise AgentProcessError(f"Agent {self.agent_id} cannot accept another turn")
            turn_id = str(uuid.uuid4())
            future = asyncio.get_running_loop().create_future()
            self._pending[turn_id] = future
            try:
                await self._send({"id": turn_id, "type": "turn", "prompt": prompt})
                return await future
            except asyncio.CancelledError:
                self._invalid = True
                # A cancelled await must not leave an unowned SDK/tool process.
                await self.cancel()
                raise
            finally:
                self._pending.pop(turn_id, None)

    def _fail_pending(self, error: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)

    async def _read_stdout(self) -> None:
        assert self.process and self.process.stdout
        try:
            while line := await self.process.stdout.readline():
                try:
                    item = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    raise AgentProcessError(f"Invalid JSONL from {self.agent_id}: {line[:180]!r}") from exc
                kind = item.get("type")
                if kind == "session":
                    self.session_id = item.get("session_id") or self.session_id
                elif kind == "tool_call":
                    task = asyncio.create_task(self._handle_tool(item))
                    self._tool_tasks.add(task)
                    task.add_done_callback(self._tool_tasks.discard)
                elif kind == "tool_observation":
                    # Deliberately discard any unknown fields; raw tool inputs
                    # or outputs must never enter the telemetry store.
                    fields = ("type", "phase", "tool_use_id", "tool_name", "input_fingerprint", "result_fingerprint", "is_error")
                    self.observations.append({key: item.get(key) for key in fields})
                elif kind == "turn_result":
                    self.session_id = item.get("session_id") or self.session_id
                    future = self._pending.get(str(item.get("id")))
                    if future and not future.done():
                        future.set_result(item)
                else:
                    raise AgentProcessError(f"Unexpected runner message type: {kind!r}")
            code = await self.process.wait()
            if not self._closing:
                detail = "\n".join(self.stderr_tail)[-4000:]
                self._invalid = True
                self._fail_pending(AgentProcessError(f"Agent {self.agent_id} exited {code}: {detail}"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._invalid = True
            self._fail_pending(exc if isinstance(exc, AgentProcessError) else AgentProcessError(str(exc)))

    async def _read_stderr(self) -> None:
        assert self.process and self.process.stderr
        while line := await self.process.stderr.readline():
            self.stderr_tail.append(line.decode(errors="replace").rstrip())

    async def _handle_tool(self, item: dict[str, Any]) -> None:
        try:
            if self.on_tool is None:
                result = {"is_error": True, "error": "This role has no tool handler"}
            else:
                result = await self.on_tool(str(item["name"]), dict(item.get("args") or {}))
                if not isinstance(result, dict):
                    result = {"result": result}
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = {"is_error": True, "error": f"{type(exc).__name__}: {exc}"}
        try:
            await self._send({"type": "tool_result", "id": item["id"], "result": result})
        except AgentProcessError:
            pass

    async def cancel(self) -> None:
        """Cancel this process and all its descendants; it cannot be reused."""
        self._invalid = True
        await self.close()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        process = self.process
        if process and process.returncode is None:
            try:
                await self._send({"type": "stop"})
                if process.stdin:
                    process.stdin.close()
                await asyncio.wait_for(process.wait(), timeout=4)
            except (AgentProcessError, asyncio.TimeoutError, BrokenPipeError):
                pass
        if process:
            # Also clean descendants when the runner crashed before close().
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            elif process.returncode is None:
                process.terminate()
            if process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), timeout=2)
                except asyncio.TimeoutError:
                    if os.name != "posix":
                        process.kill()
            if os.name == "posix":
                # The group can outlive its leader and contain a tool ignoring
                # SIGTERM. A final group kill gives shutdown a bounded end.
                await asyncio.sleep(0.05)
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            await process.wait()
        for task in [*self._tool_tasks, self._reader, self._stderr_reader]:
            if task and task is not asyncio.current_task() and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in [*self._tool_tasks, self._reader, self._stderr_reader]
              if task and task is not asyncio.current_task()),
            return_exceptions=True,
        )
        self._fail_pending(AgentProcessError(f"Agent {self.agent_id} closed"))

    async def __aenter__(self) -> "AgentProcess":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
