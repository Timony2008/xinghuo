"""Test-only real JSONL process with deterministic injected role/environment I/O.

This is not a public backend or a model/target simulation exposed by the CLI.
Network connections and child commands are prohibited inside this fixture.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import uuid

from xinghuo.config import VERIA_MODELS
from xinghuo.room import atomic_write
from xinghuo.veria_runtime import VeriaRuntime


def reject_external_io(event, args):
    if event in {"socket.connect", "socket.connect_ex", "subprocess.Popen", "os.system", "os.posix_spawn"}:
        raise AssertionError("Offline protocol fixture forbids external I/O: " + event)


class FixtureEnvironment:
    def __init__(self, directory, run_id, code, attempt_id, settings):
        self.directory = Path(directory)
        self.path = self.directory / "fixture-environment.json"
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {
            "container_id": "fixture-" + uuid.uuid4().hex, "closed": False, "starts": 0,
            "cleaned": [], "attempt_id": attempt_id}
        if self.state["attempt_id"] != attempt_id:
            raise AssertionError("Fixture cannot silently change attempts")

    def save(self):
        atomic_write(self.path, json.dumps(self.state))

    async def start(self):
        recovered = self.state["starts"] > 0 and not self.state["closed"]
        self.state.update(starts=self.state["starts"] + 1, closed=False, quiescing=False)
        self.save()
        return {"image_id": "sha256:offline-fixture", "container_id": self.state["container_id"],
                "description": {"fixture": True}, "recovered": recovered, "volatile_state_lost": False}

    def quiesce(self, value):
        self.state["quiescing"] = bool(value)
        self.save()

    async def health(self):
        return {"healthy": not self.state["closed"], "fixture": True}

    async def stop_member(self, agent_id):
        self.state["cleaned"].append(agent_id)
        self.save()

    async def stop(self):
        self.state["closed"] = True
        self.save()


class FixtureDriver:
    def __init__(self, agent_id, role, model_id, workspace, context, environment,
                 on_tool, on_session=None, on_event=None, session_id=None):
        self.agent_id, self.role, self.model_id = agent_id, role, model_id
        self.workspace = Path(workspace)
        self.environment = environment
        self.on_tool, self.on_session, self.on_event = on_tool, on_session, on_event
        self.resumed_from = session_id
        self.session_id = session_id or "fixture-session-" + uuid.uuid4().hex
        self.alive = False
        self.turns = 0

    async def start(self):
        self.alive = True
        self.workspace.mkdir(parents=True, exist_ok=True)
        atomic_write(self.workspace / "fixture-driver-identity.json", json.dumps({
            "agent_id": self.agent_id, "model_id": self.model_id,
            "session_id": self.session_id, "resumed_from": self.resumed_from}))
        await self.on_session(self.session_id)

    async def turn(self, prompt):
        self.turns += 1
        data = json.loads(prompt)
        if self.role == "solver":
            if self.agent_id == "pro-1" and self.turns == 1:
                receipt = await self.on_tool("submit_flag", {"flag": "FLAG{jsonl-offline-candidate}"})
                atomic_write(self.workspace / "fixture-receipt-observed.json", json.dumps(receipt))
                await self.on_tool("notify_coordinator", {"message": "fixture receipt observed: " + receipt["status"]})
            # A pending model turn remains indefinitely; start/health/report/stop
            # can only pass if the production protocol stays asynchronous.
            await asyncio.Event().wait()
        if self.role == "coordinator":
            messages = data.get("messages", [])
            for item in messages:
                body = item.get("body", item.get("message", ""))
                if str(body).startswith("fixture finding:"):
                    await self.on_tool("broadcast", {"message": body, "reply_to": item["id"]})
            if messages:
                await self.on_tool("ack_messages", {"message_ids": [item["id"] for item in messages]})
        if self.role == "reporter":
            while not (self.environment.directory / "fixture-release-reporter").exists():
                await asyncio.sleep(.005)
            await self.on_tool("publish_report", {"overview": "Offline protocol evidence batch",
                "recent_changes": ["Observed the fixture's original events."], "short_history": [],
                "next_steps": [], "evidence_refs": [], "through_event": data["batch"]["through_event"]})
        return {"result": "", "session_id": self.session_id, "is_error": False}

    async def close(self):
        self.alive = False


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    # Make accidental real SDK construction a hard failure as well as denying
    # network and subprocess audit events. No credentials are needed or read.
    sys.modules["claude_agent_sdk"] = None
    sys.addaudithook(reject_external_io)
    runtime = VeriaRuntime(args.run_dir, "protocol-test", "attempt-protocol", settings={
        "veria_models": VERIA_MODELS, "veria_python": sys.executable,
        "veria_tool_image": "fixture:no-network", "veria_report_interval": .04,
        "veria_continuation_delay": .01, "role_turn_timeout": 60,
        "operation_timeout": 15, "shutdown_timeout": .1, "max_retries": 0,
        "heartbeat_timeout": .15,
    }, environment_factory=FixtureEnvironment, driver_factory=FixtureDriver)
    await runtime.serve()


if __name__ == "__main__":
    asyncio.run(main())
