"""Explicit execution inside the platform's outer container, without Docker-in-Docker.

Each attempt has private paths and generation-scoped process ownership. Teams
share the outer sandbox's kernel, network and resource limits. This does not
claim per-team container or hostile-member security isolation.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
import sys

from .environment import EnvironmentError, SharedDockerEnvironment
from ..resource_health import ResourceMonitor


class HostedProcessEnvironment(SharedDockerEnvironment):
    marker = Path("/opt/xinghuo/HOSTED_IMAGE.json")
    helper = Path("/usr/local/bin/xinghuo_exec.py")
    work_root = Path("/work")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._active = False
        self._resources = ResourceMonitor(state_path=self.directory / "resource-state.json")
        self.labels["xinghuo.environment"] = "hosted"

    def _validate_sandbox(self):
        if sys.platform != "linux" or not self.marker.is_file() or not self.helper.is_file():
            raise EnvironmentError("Hosted tools require the prepared Linux hosted image; no host fallback")
        if not self.directory.is_relative_to(self.work_root.resolve()):
            raise EnvironmentError("Hosted attempt directory must stay under /work")
        return "hosted-sha256:" + hashlib.sha256(self.marker.read_bytes()).hexdigest()

    @property
    def ready(self):
        return self._active and not self._closed

    def helper_argv(self, *args, interactive=False):
        self._validate_sandbox()
        return ["env", f"XINGHUO_TOOL_ROOT={self.directory}", sys.executable, str(self.helper), *args]

    async def ensure_owned_environment(self):
        if self._validate_sandbox() != self.image_id:
            raise EnvironmentError("Hosted image identity changed within an attempt")
        return self.ready

    async def start(self):
        identity = self._validate_sandbox()
        self.directory.mkdir(parents=True, exist_ok=True)
        for name in ("inputs", "members", "shared", "control"):
            (self.directory / name).mkdir(exist_ok=True)
        previous = json.loads(self.state_path.read_text()) if self.state_path.exists() else None
        if previous:
            if previous.get("labels") != self.labels or previous.get("image_id") != identity:
                raise EnvironmentError("Hosted environment ownership or image identity mismatch")
            self.generation = previous["generation"]
            self.commands = previous.get("commands", {})
        self.image_id = identity
        self._active, self._closed = True, False
        self.quiesce(True)
        retired = [entry for entry in self.commands.values()
                   if entry.get("status") not in {"cleaned", "not_dispatched"}]
        results = await asyncio.gather(*(self._cleanup(member) for member in
            sorted({entry["agent_id"] for entry in retired})), return_exceptions=True)
        if any(isinstance(result, BaseException) for result in results):
            raise EnvironmentError("Could not retire previous hosted tool processes")
        description = await self.runner(self.helper_argv("describe"), timeout=20)
        if description["returncode"]:
            raise EnvironmentError("Hosted tool inventory failed")
        self._description = json.loads(description["stdout"])
        self._description.update({"execution_mode": "hosted_process",
            "network_mode": "platform-managed outer sandbox", "internet_verified": False,
            "web_tools": [],
            "web_tool_execution": "disabled: hosted platform has no public Internet",
            "resource_scope": "all teams share outer container limits; no per-team cgroup",
            "shared_container_is_security_isolation": False})
        self._save()
        self.quiesce(False)
        return {"environment_id": f"hosted:{self.generation}", "container_id": None,
                "image_id": self.image_id, "description": self._description,
                "recovered": bool(previous), "retired_execution_count": len(retired),
                "volatile_state_lost": bool(previous)}

    async def stop(self):
        self.quiesce(True)
        members = sorted({entry["agent_id"] for entry in self.commands.values()})
        results = await asyncio.gather(*(self._cleanup(member) for member in members),
                                       return_exceptions=True)
        if any(isinstance(result, BaseException) for result in results):
            raise EnvironmentError("Hosted owned-process cleanup incomplete")
        self._closed, self._active = True, False
        self._save()

    async def health(self):
        healthy = await self.ensure_owned_environment()
        return {"healthy": healthy, "alive": healthy, "running": healthy,
                "execution_mode": "hosted_process", "container_id": None,
                "image_id": self.image_id, "quiescing": self._quiescing,
                "resources": self._resources.snapshot(),
                "description": self._description}
