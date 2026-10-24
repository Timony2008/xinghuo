#!/usr/bin/env python3
"""Real SDK/CLI + runtime evidence/acquisition tools against synthetic loopback SSE."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shlex
import tempfile
import threading
from unittest.mock import patch

from verify_glm_sdk_wire import OfflineEnvironment
from xinghuo.config import VERIA_MIXED4_MODELS, VERIA_MIXED4_PROVIDERS, VERIA_MIXED4_SOLVERS
from xinghuo.veria_runtime import VeriaRuntime, VeriaSlot


MARKER = "WIRE_LATE_EVIDENCE_CLUE"
REPORTER_MODEL = VERIA_MIXED4_MODELS["reporter"]
SOLVER_MODEL = VERIA_MIXED4_MODELS["solver-3"]


class FixtureEnvironment(OfflineEnvironment):
    """Only the inherited, fixed harmless printf can enter the Bash bridge."""

    async def stop(self):
        return None


def result_json(block):
    if block.get("is_error"):
        raise AssertionError("Real SDK returned an unsuccessful fixture tool result")
    content = block.get("content", "")
    texts = [content] if isinstance(content, str) else [
        item.get("text", "") for item in content if isinstance(item, dict)]
    for text in texts:
        if not isinstance(text, str):
            continue
        index = text.find("{")
        if index >= 0:
            try:
                value, _ = json.JSONDecoder().raw_decode(text[index:])
                if isinstance(value, dict):
                    return value
            except ValueError:
                pass
    raise AssertionError("Fixture could not decode the real tool-result JSON")


def tool_results(body):
    return {block["tool_use_id"]: block for message in body.get("messages", [])
            if isinstance(message.get("content"), list) for block in message["content"]
            if block.get("type") == "tool_result"}


@contextmanager
def fixture_server(state):
    captured = []

    def next_action(body):
        results = tool_results(body)
        if body["model"] == REPORTER_MODEL:
            if "report-search" not in results:
                return "report-search", "mcp__veria__search_evidence", {
                    "path": state["long_path"], "query": MARKER, "max_chars": 80}
            found = result_json(results["report-search"])
            assert len(found["matches"]) == 1 and found["matches"][0]["offset"] > 24_000
            assert found["complete_file"] is False
            if "report-read" not in results:
                return "report-read", "mcp__veria__read_evidence", {
                    "path": state["long_path"], "offset": found["matches"][0]["offset"],
                    "expected_sha256": found["sha256"], "max_chars": len(MARKER)}
            read = result_json(results["report-read"])
            assert read["text"] == MARKER and read["sha256"] == found["sha256"]
            assert read["complete_file"] is False
            if "report-acquisition" not in results:
                return "report-acquisition", "mcp__veria__read_acquisition", {"request_id": state["request_id"]}
            acquisition = result_json(results["report-acquisition"])
            assert acquisition["manifest"] and acquisition["job"]["state"] == "ready"
            assert acquisition["manifest"]["fresh_reproduction"] is False
            if "report-manifest" not in results:
                return "report-manifest", "mcp__veria__read_evidence", {
                    "path": acquisition["manifest"]["manifest_path"], "max_chars": 24_000}
            manifest_page = result_json(results["report-manifest"])
            assert state["request_id"] in manifest_page["text"]
            state["reporter_verified"] = True
            return None
        assert body["model"] == SOLVER_MODEL, "Unexpected model in loopback fixture"
        if "solver-printf" not in results:
            return "solver-printf", "Bash", {"command": "printf offline-wire-tool"}
        assert not results["solver-printf"].get("is_error")
        if "solver-record" not in results:
            fields = {"request_id": state["request_id"], "entrypoints": ["offline ordinary fixture"],
                      "prerequisites": [], "replay_commands": ["printf offline-wire-tool"],
                      "script_paths": [], "notes": "SDK hook fixture; never independent reproduction"}
            command = "record_acquisition " + shlex.quote(json.dumps(fields))
            state["record_command"] = command
            return "solver-record", "Bash", {"command": command}
        recorded = result_json(results["solver-record"])
        assert recorded["draft"]["payload"]["notes"].startswith("SDK hook fixture")
        for index in range(6):
            identifier = f"solver-status-{index}"
            if identifier not in results:
                return identifier, "Bash", {"command": "record_acquisition " + shlex.quote(
                    json.dumps({"request_id": state["request_id"]}))}
            status = result_json(results[identifier])
            if status["manifest"] and status["job"]["state"] == "ready":
                assert status["manifest"]["draft_revision"] == 1
                assert status["manifest"]["fresh_reproduction"] is False
                state["solver_verified"] = True
                if "solver-output" not in results:
                    return "solver-output", "StructuredOutput", {"type": "flag_found", "flag": "",
                        "method": "offline ordinary fixture; no live flag acquired"}
                return None
        raise AssertionError("Background acquisition did not become ready within six real SDK status calls")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            captured.append({"path": self.path, "body": body,
                             "auth": self.headers.get("x-api-key") or self.headers.get("authorization")})
            if self.path.split("?")[0] not in {"/v1/messages", "/anthropic/v1/messages"}:
                data = b"{}"
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            try:
                action = next_action(body)
            except BaseException as error:
                state["server_error"] = f"{type(error).__name__}: {error}"
                action = None
            events = [("message_start", {"type": "message_start", "message": {
                "id": "msg_offline_evidence", "type": "message", "role": "assistant",
                "model": body["model"], "content": [], "stop_reason": None, "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 0}}})]
            if action:
                identifier, name, arguments = action
                events += [("content_block_start", {"type": "content_block_start", "index": 0,
                    "content_block": {"type": "tool_use", "id": identifier, "name": name, "input": {}}}),
                    ("content_block_delta", {"type": "content_block_delta", "index": 0,
                    "delta": {"type": "input_json_delta", "partial_json": json.dumps(arguments)}})]
            else:
                events += [("content_block_start", {"type": "content_block_start", "index": 0,
                    "content_block": {"type": "text", "text": ""}}),
                    ("content_block_delta", {"type": "content_block_delta", "index": 0,
                    "delta": {"type": "text_delta", "text": "offline-evidence-ok"}})]
            events += [("content_block_stop", {"type": "content_block_stop", "index": 0}),
                ("message_delta", {"type": "message_delta", "delta": {
                    "stop_reason": "tool_use" if action else "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": 1}}), ("message_stop", {"type": "message_stop"})]
            data = "".join(f"event: {name}\ndata: {json.dumps(value)}\n\n" for name, value in events).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", captured
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


async def verify(endpoint, captured, state, root):
    environment = FixtureEnvironment()
    settings = {"veria_models": VERIA_MIXED4_MODELS, "veria_solver_ids": list(VERIA_MIXED4_SOLVERS),
                "veria_providers": VERIA_MIXED4_PROVIDERS, "veria_team_profile": "mixed4",
                "veria_environment": "hosted"}
    runtime = VeriaRuntime(root, "offline-fixture", "offline-attempt", settings=settings)
    runtime.environment = environment
    runtime.room.set_meta("context", {"run_id": "offline-sdk-fixture", "deadline": 99_999_999_999,
        "challenge": {"description": "Synthetic evidence tool fixture; no external targets"},
        "platform": {"completed": False, "correct_flag_count": 1, "flag_count": 2}})
    runtime.room.set_meta("environment", {"description": environment._description})
    long_path = runtime.directory / "shared/long-evidence.txt"
    long_path.parent.mkdir(exist_ok=True)
    long_path.write_text("证" * 26_000 + MARKER + "尾", encoding="utf-8")
    candidate = runtime.room.save_candidate("solver-3", "FLAG{fixture}")
    runtime.room.apply_receipt(candidate["request_id"], {"correct": True, "duplicate": False})
    state.update(long_path=str(long_path), request_id=candidate["request_id"])
    clients = []
    results = []

    async def emit(*args, **kwargs):
        return None

    runtime.emit = emit
    try:
        for member, role in (("solver-3", "solver"), ("reporter", "reporter")):
            slot = VeriaSlot(member, role)
            runtime.slots[member] = slot
            runtime.room.register_member(member, role, runtime.models[member],
                workspace=str(runtime.directory / "agents" / member), run_id="offline-sdk-fixture")
            driver = await asyncio.wait_for(runtime._new_driver(slot), 30)
            clients.append(driver)
            result = await asyncio.wait_for(driver.turn("Execute only the synthetic fixture tool sequence."), 90)
            assert not result["is_error"], result
            assert "server_error" not in state, state.get("server_error")
            if role == "solver":
                assert result["structured_output"]["type"] == "flag_found"
                assert result["structured_output"]["flag"] == ""
            else:
                assert result["result"] == "offline-evidence-ok"
            results.append({"member": member, "role": role, "model": runtime.models[member],
                            "actual_runtime_handler": True, "real_sdk_cli": True, "successful_turn": True})
            await driver.close()
        assert state.get("solver_verified") and state.get("reporter_verified")
        assert environment.executed == ["printf offline-wire-tool"], environment.executed
        draft = runtime.room.acquisition_status(candidate["request_id"])
        assert draft["job"]["state"] == "ready" and draft["manifest"]["draft_revision"] == 1
        assert draft["manifest"]["fresh_reproduction"] is False
        requests = []
        role_schemas = {}
        for request in captured:
            if request["path"].split("?")[0] not in {"/v1/messages", "/anthropic/v1/messages"}:
                continue
            body = request["body"]
            role = "reporter" if body["model"] == REPORTER_MODEL else "solver"
            expected_key = "synthetic-evidence-ds" if role == "reporter" else "synthetic-evidence-glm"
            assert request["auth"] in {expected_key, "Bearer " + expected_key}
            assert body.get("stream") is True
            tools = {tool["name"]: tool.get("input_schema", {}) for tool in body.get("tools", [])}
            role_schemas[role] = tools
            requests.append({"role": role, "model": body["model"], "path": request["path"],
                             "stream": True, "synthetic_auth_matches": True,
                             "tool_result_ids": list(tool_results(body)), "tool_names": list(tools)})
        schemas = role_schemas["reporter"]
        assert {"path", "offset", "max_chars", "expected_sha256"} <= set(
            schemas["mcp__veria__read_evidence"]["properties"])
        assert "query" in schemas["mcp__veria__search_evidence"]["properties"]
        assert "mcp__veria__read_acquisition" in schemas
        assert "Bash" not in schemas and "mcp__veria__record_acquisition" not in schemas
        assert "Bash" in role_schemas["solver"]
        return {"passed": True, "real_model_calls": 0, "platform_calls": 0,
                "endpoint_scope": "127.0.0.1 only; synthetic Anthropic SSE",
                "runtime_started_full_team": False, "clients": results, "requests": requests,
                "role_tool_schemas": role_schemas, "reporter_late_clue_offset": 26_000,
                "reporter_search_then_hash_bound_read": True, "reporter_read_acquisition": True,
                "reporter_manifest_read": True, "solver_real_hook_record_and_status": True,
                "acquisition_manifest_ready": True, "acquisition_fresh_reproduction": False,
                "only_fixed_benign_environment_command": environment.executed,
                "standalone_record_command_never_sent_to_shell": True,
                "private_environment_inheritance_disabled": True,
                "gateway_and_paid_provider_access_verified": False}
    finally:
        # Also close a driver whose start failed before _new_driver returned.
        for slot in runtime.slots.values():
            if slot.driver is not None:
                await slot.driver.close()
        await runtime.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    state = {}
    env = {name: os.environ[name] for name in ("PATH", "USER", "TMPDIR", "LANG") if name in os.environ}
    with tempfile.TemporaryDirectory(prefix="evidence-sdk-wire-") as temporary, fixture_server(state) as (endpoint, captured):
        root = Path(temporary)
        isolated_home = root / "isolated-home"
        isolated_home.mkdir()
        env.update({"HOME": str(isolated_home), "XINGHUO_TEAM_PROFILE": "mixed4",
                    "DEEPSEEK_API_KEY": "synthetic-evidence-ds", "DEEPSEEK_BASE_URL": endpoint + "/anthropic",
                    "GLM_API_KEY": "synthetic-evidence-glm", "GLM_BASE_URL": endpoint,
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "CLAUDECODE": ""})
        with patch.dict(os.environ, env, clear=True):
            try:
                report = asyncio.run(asyncio.wait_for(verify(endpoint, captured, state, root), 240))
            except BaseException as error:
                report = {"passed": False, "real_model_calls": 0, "platform_calls": 0,
                          "error": f"{type(error).__name__}: {error}", "fixture_error": state.get("server_error")}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items()
                      if key not in {"requests", "role_tool_schemas"}}, ensure_ascii=False))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
