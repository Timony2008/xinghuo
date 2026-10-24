from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
import sys
import time
import uuid

from .config import Config
from .store import Store


async def execute(args):
    if args.command == "recover":
        # The owning supervisor applies this durable command at a tool boundary.
        # Never reset room retry counters or reopen a stopped run here.
        import sqlite3
        directory = Path(args.run_dir).resolve()
        database = directory / "state.sqlite3"
        if not database.is_file():
            raise ValueError("recovery requires an existing running supervisor")
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as db:
            metadata = {key: json.loads(body) for key, body in db.execute("SELECT key,body FROM meta")}
            found = db.execute("SELECT body FROM challenges WHERE code=?", (args.code,)).fetchone()
        row = json.loads(found[0]) if found else {}
        if metadata.get("status") != "running" or row.get("status") != "active":
            raise ValueError("recovery is only available for an active challenge in a running run")
        if time.time() >= metadata.get("deadline", 0):
            raise ValueError("original run deadline has expired; recovery cannot extend it")
        request_id = uuid.uuid4().hex
        payload = {"request_id": request_id, "run_id": metadata["run_id"],
                   "code": args.code, "attempt_id": row["attempt_id"],
                   "provider": args.provider, "reason": args.reason, "requested_at": time.time()}
        target = directory / "control/recovery-requests" / (request_id + ".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(payload, stream, ensure_ascii=False)
            stream.flush(); os.fsync(stream.fileno())
        temporary.replace(target)
        print(json.dumps({"status": "queued", "request_id": request_id,
            "receipt": str(directory / "control/recovery-receipts" / (request_id + ".json")),
            "deadline": metadata["deadline"]}, ensure_ascii=False))
        return 0
    if args.command == "api-check":
        from .preflight import check_api
        result = await check_api()
        output = json.dumps(result, ensure_ascii=False, indent=2)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(output + "\n")
        print(output)
        return 0 if result["ok"] else 1
    if args.command == "sdk-smoke":
        from .smoke import sdk_smoke
        result = await sdk_smoke(args.run_dir, model=args.model or os.getenv("ANTHROPIC_MODEL"),
                                 timeout=args.timeout, backend=args.backend)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["passed"] else 1
    if args.command == "status":
        import sqlite3
        path = (Path(args.run_dir) / "state.sqlite3").resolve()
        if not path.is_file():
            raise ValueError("run directory has no supervisor database")
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            metadata = {key: json.loads(body) for key, body in db.execute("SELECT key,body FROM meta")}
            saved = metadata.get("config") or {}
            engine = saved.get("team_engine", "native")
            result = {"status": metadata.get("status"), "deadline": metadata.get("deadline"),
                      "team_engine": engine,
                      "role_models": saved.get("veria_models", {}) if engine == "veria" else {"default": saved.get("model")},
                      "team_profile": saved.get("veria_team_profile", "legacy6") if engine == "veria" else None,
                      "solver_ids": saved.get("veria_solver_ids", []) if engine == "veria" else [],
                      "role_providers": saved.get("veria_providers", {}) if engine == "veria" else {},
                      "veria_upstream_commit": saved.get("veria_upstream_commit") if engine == "veria" else None,
                      "round_id": metadata.get("round_id", 0),
                      "challenges": [json.loads(row[0]) for row in db.execute("SELECT body FROM challenges ORDER BY code")]}
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        finally:
            db.close()
    if args.command == "resume" and not (Path(args.run_dir) / "state.sqlite3").is_file():
        raise ValueError("run directory has no saved configuration")
    if args.command in {"run", "demo"} and (Path(args.run_dir) / "state.sqlite3").is_file():
        raise ValueError("run directory already exists; use resume or choose a fresh directory")
    store = Store(args.run_dir)
    try:
        if args.command == "resume":
            saved = store.get_meta("config")
            if not saved:
                raise ValueError("run directory has no saved configuration")
            config = Config(**saved).validate()
        elif args.command == "demo":
            config = Config(backend="demo", budget=args.duration, audit_interval=.5,
                            review_interval=1, poll_interval=.2, tick=.02).validate()
        else:
            config = Config.load(args.config)
            if config.backend != "claude":
                raise ValueError("run requires backend=claude; use demo for synthetic execution")
        from .supervisor import Supervisor
        if config.backend == "demo":
            from .demo import DemoPlatform
            platform = DemoPlatform(store)
            supervisor = Supervisor(platform, store, config)
            _signals(supervisor)
            result = await supervisor.run()
        else:
            from tsec_benchmark import TSecBenchmarkAsync
            from .benchmark import BenchmarkPlatform
            base, token = os.getenv("BENCHMARK_BASE_URL"), os.getenv("BENCHMARK_TOKEN")
            if not base or not token:
                raise ValueError("set BENCHMARK_BASE_URL and BENCHMARK_TOKEN; this program does not create evaluation tasks")
            async with TSecBenchmarkAsync(base_url=base, token=token, timeout=config.operation_timeout) as client:
                supervisor = Supervisor(BenchmarkPlatform(client), store, config)
                _signals(supervisor)
                result = await supervisor.run()
        print(json.dumps({"status": result["status"], "run_dir": str(Path(args.run_dir).resolve()),
                          "completed": sum(r["status"] == "completed" for r in result["challenges"]),
                          "total": len(result["challenges"]), "demo_peak": result["demo_peak"]}, ensure_ascii=False))
        return 0
    finally:
        store.close()


def _signals(supervisor):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: setattr(supervisor, "stop_requested", True))


def main():
    parser = argparse.ArgumentParser(description="星火不熄 Python harness")
    commands = parser.add_subparsers(dest="command", required=True)
    api_check = commands.add_parser("api-check", help="Read-only VPN and API listing; never starts a challenge")
    api_check.add_argument("--output", type=Path)
    recover = commands.add_parser("recover", help="Queue bounded member recovery in the existing run; never extends the deadline")
    recover.add_argument("--run-dir", type=Path, required=True)
    recover.add_argument("--code", required=True)
    recover.add_argument("--provider", choices=("deepseek", "glm"))
    recover.add_argument("--reason", default="operator requested recovery after resolving provider/resource failure")
    for name in ("demo", "run", "resume", "status", "sdk-smoke"):
        child = commands.add_parser(name)
        child.add_argument("--run-dir", type=Path, required=name in {"resume", "status"},
                           default=Path("runs") / (name + "-" + time.strftime("%Y%m%d-%H%M%S")))
        if name == "run":
            child.add_argument("--config", type=Path, default=Path("config.example.toml"))
        if name == "demo":
            child.add_argument("--duration", type=float, default=20)
        if name == "sdk-smoke":
            child.add_argument("--model")
            child.add_argument("--timeout", type=float, default=300)
            child.add_argument("--backend", choices=("claude", "demo"), default="claude")
    args = parser.parse_args()
    try:
        return_code = asyncio.run(execute(args))
    except (ValueError, RuntimeError, ImportError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return_code = 1
    except KeyboardInterrupt:
        return_code = 130
    raise SystemExit(return_code)
