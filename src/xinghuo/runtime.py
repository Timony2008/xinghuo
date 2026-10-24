"""Host-side process boundary for a challenge team."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
import time
import uuid
from collections import deque
from pathlib import Path


class TeamProcess:
    """One independently supervised team process using correlated JSONL RPC.

    ``poll_events`` is deliberately synchronous and never blocks. Reading stdout
    has a single owner; requests await futures populated by that reader.
    """
    def __init__(self, run_dir: Path, code: str, attempt_id: str,
                 backend: str = "demo", model: str | None = None,
                 settings: dict | None = None):
        if not code or code in {".", ".."} or any(c in code for c in ("/", "\\", "\x00")):
            raise ValueError("challenge code must be a safe directory name")
        self.run_dir = Path(run_dir).resolve()
        self.code, self.attempt_id, self.backend, self.model = code, attempt_id, backend, model
        self.settings = settings or {}
        self.team_engine = self.settings.get("team_engine", "native")
        if self.team_engine not in {"native", "veria"}:
            raise ValueError("unknown team engine")
        self.process: asyncio.subprocess.Process | None = None
        self._reader: asyncio.Task | None = None
        self._pending: dict[str, asyncio.Future] = {}
        self._events: deque[dict] = deque()
        self._write_lock = asyncio.Lock()
        self._stderr = None
        self._stopping = False
        self.cleanup_result: dict | None = None

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.process else None

    @property
    def alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    @property
    def directory(self) -> Path:
        return self.run_dir / "teams" / self.code

    async def start(self, context: dict) -> None:
        if self.alive:
            raise RuntimeError("team is already running")
        executable, module = sys.executable, "xinghuo.team_runtime"
        if self.team_engine == "veria":
            from .config import Config
            config = Config(**{**self.settings, "backend": self.backend}).validate()
            executable, module = config.veria_python, "xinghuo.veria_runtime"
            check = await asyncio.create_subprocess_exec(executable, "-I", "-c",
                "import sys; print('%s.%s' % sys.version_info[:2])",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                output, _ = await asyncio.wait_for(check.communicate(), timeout=5)
            except BaseException:
                with contextlib.suppress(ProcessLookupError):
                    check.kill()
                await check.wait()
                raise
            if check.returncode != 0 or output.strip() != b"3.14":
                raise RuntimeError("Veria requires Python 3.14; interpreter version check failed")
        self.directory.mkdir(parents=True, exist_ok=True)
        self._stopping = False
        self.cleanup_result = None
        self._stderr = (self.directory / "runtime.stderr.log").open("ab")
        args = [executable, "-m", module, "--run-dir", str(self.run_dir),
                "--code", self.code, "--attempt-id", self.attempt_id, "--backend", self.backend,
                "--settings-json", json.dumps(self.settings, ensure_ascii=False)]
        if self.model:
            args += ["--model", self.model]
        env = dict(os.environ)
        source_root = str(Path(__file__).resolve().parents[1])
        env["PYTHONPATH"] = source_root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        try:
            self.process = await asyncio.create_subprocess_exec(*args, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=self._stderr, env=env, start_new_session=True,
                limit=8 * 1024 * 1024)
            self._reader = asyncio.create_task(self._read_stdout(), name=f"team-reader-{self.code}")
            response = await self.request("start", context, timeout=float(self.settings.get("start_timeout", 30)))
            if response.get("error"):
                raise RuntimeError(response["error"])
        except BaseException:
            await self.stop(preserve_environment=self.team_engine == "veria")
            raise

    async def _read_stdout(self) -> None:
        assert self.process and self.process.stdout
        try:
            while line := await self.process.stdout.readline():
                try:
                    row = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    self._events.append({"type": "runtime_protocol_error", "code": self.code,
                                         "detail": line[:200].decode(errors="replace")})
                    continue
                request_id = row.get("id")
                if row.get("type") == "response" and request_id in self._pending:
                    future = self._pending.pop(request_id)
                    if not future.done():
                        future.set_result(row.get("result", {}))
                elif row.get("type") != "response":
                    self._events.append(row)
        except (asyncio.CancelledError, ConnectionError):
            pass
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(RuntimeError(f"team {self.code} disconnected"))
            self._pending.clear()

    async def request(self, command: str, payload: dict | None = None, timeout: float = 30) -> dict:
        if not self.alive or not self.process or not self.process.stdin:
            raise RuntimeError(f"team {self.code} is not running")
        request_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        message = {"type": "request", "id": request_id, "command": command, "payload": payload or {}}
        try:
            # A stalled writer must not prevent a deadline or challenge switch
            # from reaching the process-group cleanup fallback.
            async with asyncio.timeout(timeout):
                async with self._write_lock:
                    self.process.stdin.write((json.dumps(message, ensure_ascii=False) + "\n").encode())
                    await self.process.stdin.drain()
                return await future
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    def poll_events(self) -> list[dict]:
        result = list(self._events)
        self._events.clear()
        return result

    async def stop(self, *, preserve_environment: bool = False) -> None:
        if self._stopping:
            return
        if self.cleanup_result is not None and not self.alive:
            return
        self._stopping = True
        process = self.process
        veria = self.team_engine == "veria"
        from .config import VERIA_STOP_RPC_TIMEOUT, VERIA_STOP_SIGNAL_TIMEOUT, veria_stop_grace
        grace = veria_stop_grace(float(self.settings.get("shutdown_timeout", 20)),
                                preserve_environment=preserve_environment) if veria else 4
        forced, cancelled, termination = False, False, "graceful"
        try:
            if process and process.returncode is None:
                try:
                    await self.request("stop", {"preserve_environment": preserve_environment},
                        timeout=VERIA_STOP_RPC_TIMEOUT if veria else float(self.settings.get("stop_timeout", 12)))
                except (TimeoutError, RuntimeError, BrokenPipeError, ConnectionError):
                    pass
                stages = [(grace, signal.SIGTERM), (VERIA_STOP_SIGNAL_TIMEOUT, signal.SIGKILL),
                          (VERIA_STOP_SIGNAL_TIMEOUT if veria else None, None)]
                for timeout, next_signal in stages:
                    try:
                        if timeout is None:
                            await process.wait()
                        else:
                            await asyncio.wait_for(process.wait(), timeout=timeout)
                        break
                    except TimeoutError:
                        forced = True
                        termination = "unconfirmed" if next_signal is None else signal.Signals(next_signal).name
                        if next_signal is not None:
                            with contextlib.suppress(ProcessLookupError):
                                os.killpg(process.pid, next_signal)
        except asyncio.CancelledError:
            cancelled = True
            if veria and process and process.returncode is None:
                forced, termination = True, "SIGKILL"
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(process.wait(), timeout=VERIA_STOP_SIGNAL_TIMEOUT)
            raise
        finally:
            if veria and process:
                from .store import atomic_json
                exited = process.returncode is not None
                self.cleanup_result = {"team_engine": "veria", "code": self.code,
                    "attempt_id": self.attempt_id, "pid": process.pid, "recorded_at": time.time(),
                    "preserve_environment": preserve_environment, "grace_seconds": grace,
                    "process_exit_confirmed": exited, "termination": termination,
                    "cleanup_status": "incomplete" if forced or cancelled or not exited else
                        "environment_retained" if preserve_environment else "runtime_exited",
                    "needs_environment_takeover": bool(preserve_environment or forced or cancelled or not exited),
                    "note": "Process exit alone does not independently verify Docker cleanup."}
                atomic_json(self.directory / "runtime-cleanup.json", self.cleanup_result)
            if self._reader:
                self._reader.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._reader
                self._reader = None
            if self._stderr:
                self._stderr.close()
                self._stderr = None
            self._stopping = False
