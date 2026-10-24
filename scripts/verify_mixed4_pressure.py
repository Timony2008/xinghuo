#!/usr/bin/env python3
"""Real SDK/CLI + private tool execution pressure on loopback, no paid calls.

Run inside the hosted image with --network none --cpus 8 --memory 16g.
Three synthetic teams hold all 18 normal clients and three on-demand reviewers
concurrently. This is an offline resource/transport check, not a contest score
or evidence that either external model service is authenticated and available.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager, ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import uuid

from xinghuo.model_connection import profile_contract
from xinghuo.resource_health import cgroup_memory
from xinghuo.veria.driver import VeriaRoleDriver
from xinghuo.veria.hosted_environment import HostedProcessEnvironment


ROTATION_MARKER = "## Supervisor-owned challenge rotation"
ROTATION_POLICY = {"mode": "no_flag_timeout", "timeout_seconds": 900}


def system_text(body):
    system = body.get("system", [])
    if isinstance(system, str):
        return system
    return "\n".join(block.get("text", "") for block in system
                     if isinstance(block, dict) and isinstance(block.get("text"), str))


def pressure_contract(profile):
    contract = profile_contract({"XINGHUO_TEAM_PROFILE": profile})
    if profile == "flash4":
        if set(contract["models"].values()) != {"deepseek-flash"}:
            raise RuntimeError("Flash acceptance requires deepseek-flash for every role")
        if set(contract["providers"].values()) != {"deepseek"}:
            raise RuntimeError("Flash acceptance requires DeepSeek routing for every role")
    return contract


@contextmanager
def pressure_endpoints(profile):
    """Create only services used by this profile; synthetic credentials only."""
    with ExitStack() as stack:
        ds, ds_requests = stack.enter_context(endpoint("synthetic-deepseek"))
        runtime_env = {"DEEPSEEK_API_KEY": "synthetic-deepseek", "DEEPSEEK_BASE_URL": ds,
                       "XINGHUO_TEAM_PROFILE": profile, "XINGHUO_PROVIDER": "deepseek"}
        wire = {"deepseek": ds_requests}
        if profile == "mixed4":
            glm, glm_requests = stack.enter_context(endpoint("synthetic-glm"))
            runtime_env.update(GLM_API_KEY="synthetic-glm", GLM_BASE_URL=glm)
            wire["glm"] = glm_requests
        yield runtime_env, wire


@contextmanager
def endpoint(expected_key):
    captured = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            if self.path.split("?")[0] != "/v1/messages":
                raw = b"{}"; self.send_response(200)
                self.send_header("Content-Length", str(len(raw))); self.end_headers()
                self.wfile.write(raw); return
            key = self.headers.get("x-api-key") or self.headers.get("Authorization", "").removeprefix("Bearer ")
            captured.append({"path": self.path, "model": body["model"],
                             "auth_ok": key == expected_key,
                             "rotation_policy_present": ROTATION_MARKER in system_text(body)})
            if key != expected_key:
                raw = b'{"type":"error","error":{"type":"authentication_error","message":"wrong synthetic service key"}}'
                self.send_response(401); self.send_header("Content-Length", str(len(raw)))
                self.end_headers(); self.wfile.write(raw); return
            # The CLI may append a system cache/token hint after a tool result.
            # Ignore that hint, but only inspect the latest conversational
            # message so an old turn's result cannot skip the next turn's tool.
            conversation = [message for message in body.get("messages", []) if message.get("role") != "system"]
            last = (conversation[-1] if conversation else {}).get("content", [])
            has_result = isinstance(last, list) and any(item.get("type") == "tool_result" for item in last)
            names = [tool.get("name", "") for tool in body.get("tools", [])]
            exercise_bash = "StructuredOutput" in names or any(item.endswith("__record_reproductions") for item in names)
            name = "Bash" if exercise_bash else next((item for item in names if item.endswith("__get_report")), "Bash")
            structured = has_result and "StructuredOutput" in names and not any(
                item.get("tool_use_id", "").startswith("pressure_struct_") for item in last)
            if structured:
                block = {"type": "tool_use", "id": "pressure_struct_" + uuid.uuid4().hex,
                         "name": "StructuredOutput", "input": {}}
                delta = {"type": "input_json_delta", "partial_json": json.dumps({
                    "type": "flag_found", "flag": "", "method": "offline-pressure-ok"})}
            elif has_result:
                block = {"type": "text", "text": ""}
                delta = {"type": "text_delta", "text": "offline-pressure-ok"}
            else:
                # Vary the fixed harmless command per turn, so the loop detector
                # itself is not the reason subsequent tools stop executing.
                arguments = {} if name != "Bash" else {"command": "printf 'offline-pressure-%s\\n' " + str(len(captured))}
                block = {"type": "tool_use", "id": "pressure_" + uuid.uuid4().hex, "name": name, "input": {}}
                delta = {"type": "input_json_delta", "partial_json": json.dumps(arguments)}
            events = [("message_start", {"type": "message_start", "message": {
                "id": "msg_" + uuid.uuid4().hex, "type": "message", "role": "assistant", "model": body["model"],
                "content": [], "stop_reason": None, "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 0}}}),
                ("content_block_start", {"type": "content_block_start", "index": 0, "content_block": block}),
                ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": delta}),
                ("content_block_stop", {"type": "content_block_stop", "index": 0}),
                ("message_delta", {"type": "message_delta", "delta": {
                    "stop_reason": "end_turn" if has_result and not structured else "tool_use", "stop_sequence": None},
                    "usage": {"output_tokens": 1}}), ("message_stop", {"type": "message_stop"})]
            raw = "".join(f"event: {name}\ndata: {json.dumps(value)}\n\n" for name, value in events).encode()
            self.send_response(200); self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", captured
    finally:
        server.shutdown(); server.server_close(); thread.join()


async def run(args):
    started = time.monotonic()
    baseline = cgroup_memory()
    if baseline.get("limit_bytes") != 16 * 1024**3:
        raise RuntimeError("Pressure acceptance requires the actual 16GiB cgroup limit")
    if os.getuid() != 10001:
        raise RuntimeError("Pressure acceptance requires the hosted UID10001")
    if not isinstance(baseline.get("events", {}).get("oom_kill"), int):
        raise RuntimeError("Pressure acceptance requires an observable cgroup OOM kill counter")
    contract = pressure_contract(args.profile)
    report = {"status": "running", "external_model_calls": 0, "platform_calls": 0,
              "profile": contract, "normal_clients": 18, "peak_clients": 21,
              "rotation_policy": dict(ROTATION_POLICY), "required_system_marker": ROTATION_MARKER,
              "pressure_seconds_requested": args.seconds, "baseline": baseline,
              "samples": [], "members": {}, "errors": []}
    with pressure_endpoints(args.profile) as (runtime_env, wire), \
            tempfile.TemporaryDirectory(prefix=args.profile + "-pressure-", dir="/work") as temporary:
        # Test-process-only environment, fixed before concurrency starts. No real
        # key is read, mounted or inherited by the Docker invocation.
        os.environ.update(runtime_env)
        root = Path(temporary)
        environments, drivers = [], []
        functional_task = None
        events = []
        try:
            for team in range(3):
                directory = root / f"team-{team}"
                environment = HostedProcessEnvironment(directory, "offline-pressure", str(team), "attempt",
                    {"veria_tool_image": "embedded", "veria_tool_cpu_limit": 8, "veria_tool_memory_limit": "16g"})
                await environment.start(); environments.append(environment)
                for member in (*contract["solver_ids"], "coordinator", "reporter", "reviewer"):
                    role = "solver" if member in contract["solver_ids"] else member
                    identity = f"{team}/{member}"
                    async def on_tool(name, values, identity=identity):
                        report["members"][identity]["mcp_calls"] += 1
                        return {"messages": [], "report": {"overview": "offline fixed evidence", "evidence_refs": []}}
                    async def on_event(kind, data, identity=identity):
                        events.append((identity, kind, data.get("success")))
                    context = {"provider": contract["providers"][member],
                        "team_profile": args.profile, "solver_ids": contract["solver_ids"],
                        "veria_solver_ids": contract["solver_ids"],
                        "rotation_policy": dict(ROTATION_POLICY),
                        "challenge": {"description": "Synthetic offline pressure test. Only run the fixed local command."},
                        "platform": {"flag_count": 2, "correct_flag_count": 0, "completed": False}}
                    if role == "reviewer":
                        context["review"] = {"review_id": f"pressure-{team}", "confirmed_submissions": []}
                    driver = VeriaRoleDriver(member, role, contract["models"][member], directory / "agents" / member,
                        context, environment, on_tool, on_event=on_event)
                    drivers.append((identity, driver))
                    report["members"][identity] = {"model": driver.model_id, "provider": context["provider"],
                        "role": role, "turns": 0, "mcp_calls": 0}
            await asyncio.gather(*(driver.start() for _, driver in drivers))
            loaded = time.monotonic()
            # Run all offline functional tool cases (including Ghidra, angr,
            # Sage/RSA/lattice and torch/Keras) through a real private member
            # while every SDK client remains live. Installation-only checks
            # would not exercise these tools' resource cost.
            functional_task = asyncio.create_task(environments[0].execute("solver-1",
                "python /opt/xinghuo/veria_tool_acceptance.py --phase functional --expect-uid 10001", timeout=300))
            while time.monotonic() - loaded < args.seconds or not functional_task.done():
                results = await asyncio.gather(*(asyncio.wait_for(driver.turn("Execute the next fixed offline pressure step, then end this turn."), 90)
                                                for _, driver in drivers), return_exceptions=True)
                for (identity, driver), value in zip(drivers, results):
                    valid_output = (isinstance(value, dict) and (value.get("result") == "offline-pressure-ok"
                        or (value.get("structured_output") or {}).get("method") == "offline-pressure-ok"))
                    if isinstance(value, BaseException) or value.get("is_error") or not valid_output:
                        report["errors"].append({"member": identity, "error": str(value)[:500]})
                    else:
                        report["members"][identity]["turns"] += 1
                sample = cgroup_memory()
                sample["elapsed_seconds"] = round(time.monotonic() - loaded, 3)
                sample["alive_clients"] = sum(driver.alive for _, driver in drivers)
                report["samples"].append(sample)
                if report["errors"]:
                    break
                await asyncio.sleep(5)
            report["loaded_seconds"] = round(time.monotonic() - loaded, 3)
            functional = await functional_task
            try:
                report["functional_under_pressure"] = json.loads(functional["stdout"])
            except ValueError:
                report["errors"].append({"functional_output": functional["stdout"][-1000:]})
            if functional["returncode"] != 0:
                report["errors"].append({"functional_returncode": functional["returncode"],
                    "stderr": functional["stderr"][-1500:]})
            for identity in report["members"]:
                report["members"][identity]["successful_bash_executions"] = sum(
                    who == identity and kind == "tool_execution_result" and success is True for who, kind, success in events)
                failures = sum(who == identity and (kind in {"tool_routing_error", "sdk_error", "assistant_error"}
                    or kind == "tool_execution_result" and success is False) for who, kind, success in events)
                if failures:
                    report["errors"].append({"member": identity, "failed_events": failures})
        except BaseException as error:
            report["errors"].append({"exception": type(error).__name__, "error": str(error)[:1000]})
        finally:
            if functional_task and not functional_task.done():
                functional_task.cancel()
                await asyncio.gather(functional_task, return_exceptions=True)
            closes = await asyncio.gather(*(driver.close() for _, driver in drivers), return_exceptions=True)
            stops = await asyncio.gather(*(env.stop() for env in environments), return_exceptions=True)
            report["cleanup_errors"] = [str(item)[:500] for item in (*closes, *stops) if isinstance(item, BaseException)]
        report["wire"] = wire
        for provider, requests in wire.items():
            allowed_models = {model for member, model in contract["models"].items()
                              if contract["providers"][member] == provider}
            if not requests or any(item["model"] not in allowed_models for item in requests):
                report["errors"].append({"wire_provider": provider, "error": "missing requests or unexpected model"})
            if any(not item["rotation_policy_present"] for item in requests):
                report["errors"].append({"wire_provider": provider,
                    "error": "supervisor rotation policy absent from an actual SDK system message"})
            if args.profile == "flash4" and any(item["model"] != "deepseek-flash" for item in requests):
                report["errors"].append({"wire_provider": provider, "error": "non-Flash model on Flash acceptance wire"})
        report["wire_rotation_policy"] = {"requests": sum(len(rows) for rows in wire.values()),
            "requests_with_policy": sum(item["rotation_policy_present"] for rows in wire.values() for item in rows)}
    final = cgroup_memory()
    final_oom = final.get("events", {}).get("oom_kill")
    report.update(final=final, elapsed_seconds=round(time.monotonic() - started, 3),
                  oom_kill_delta=final_oom - baseline["events"]["oom_kill"] if isinstance(final_oom, int) else None)
    good = (not report["errors"] and not report["cleanup_errors"] and report["oom_kill_delta"] == 0
            and report.get("loaded_seconds", 0) >= args.seconds
            and report.get("functional_under_pressure", {}).get("status") in {"passed", "passed_with_restrictions"}
            and all(item["turns"] >= 2 and (item["successful_bash_executions"] >= 2
                if item["role"] in {"solver", "reviewer"} else item["mcp_calls"] >= 2)
                for item in report["members"].values())
            and all(item["auth_ok"] and item["rotation_policy_present"]
                    for rows in report["wire"].values() for item in rows))
    report["status"] = "passed" if good else "failed"
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("status", "elapsed_seconds", "peak_clients", "oom_kill_delta", "errors", "cleanup_errors")}))
    return 0 if good else 1


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("flash4", "mixed4"), default="flash4")
    parser.add_argument("--seconds", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    return parser


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(argument_parser().parse_args())))
