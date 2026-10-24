from __future__ import annotations

import os
import math
import re
import tomllib
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path


VERIA_UPSTREAM_COMMIT = "3366d569557c4fda3fd153040632de65e255396d"
VERIA_CONTAINER_INSPECT_TIMEOUT = 20.0
VERIA_CONTAINER_REMOVE_TIMEOUT = 30.0
VERIA_MEMBER_CLEANUP_TIMEOUT = 15.0
VERIA_STOP_RPC_TIMEOUT = 5.0
VERIA_STOP_SIGNAL_TIMEOUT = 3.0


def veria_environment_cleanup_timeout(*, preserve_environment: bool = False) -> float:
    # All owned members (including review workspaces) clean in parallel.
    action = VERIA_MEMBER_CLEANUP_TIMEOUT if preserve_environment else VERIA_CONTAINER_REMOVE_TIMEOUT
    return VERIA_CONTAINER_INSPECT_TIMEOUT + action + 5.0


def veria_stop_grace(shutdown_timeout: float, *, preserve_environment: bool = False) -> float:
    """Parallel role-close budget, environment cleanup, then IPC margin."""
    return shutdown_timeout + veria_environment_cleanup_timeout(preserve_environment=preserve_environment) + 5.0


def veria_supervisor_cleanup_timeout(shutdown_timeout: float, operation_timeout: float) -> float:
    # prepare_stop, stop RPC, runtime drain and signals, platform close + reconciliation.
    return (shutdown_timeout + VERIA_STOP_RPC_TIMEOUT + veria_stop_grace(shutdown_timeout)
            + 2 * VERIA_STOP_SIGNAL_TIMEOUT + 2 * operation_timeout + 5.0)


VERIA_MODELS = {
    **{f"pro-{n}": "deepseek-v4-pro" for n in range(1, 4)},
    **{f"flash-{n}": "deepseek-flash" for n in range(1, 4)},
    "coordinator": "deepseek-v4-pro", "reporter": "deepseek-flash",
    "reviewer": "deepseek-v4-pro",
}

VERIA_LEGACY_SOLVERS = tuple(f"{family}-{n}" for family in ("pro", "flash") for n in range(1, 4))
VERIA_MIXED4_SOLVERS = tuple(f"solver-{n}" for n in range(1, 5))
VERIA_SUPPORT_ROLES = ("coordinator", "reporter", "reviewer")
VERIA_MIXED4_MODELS = {
    "solver-1": "deepseek-v4-pro", "solver-2": "deepseek-flash",
    "solver-3": "glm-5.3", "solver-4": "glm-5.3-flash",
    "coordinator": "glm-5.3", "reporter": "deepseek-flash", "reviewer": "glm-5.3",
}
VERIA_MIXED4_PROVIDERS = {member: ("glm" if member in {"solver-3", "solver-4", "coordinator", "reviewer"} else "deepseek")
                          for member in VERIA_MIXED4_MODELS}
VERIA_FLASH4_MODELS = {member: "deepseek-flash" for member in (*VERIA_MIXED4_SOLVERS, *VERIA_SUPPORT_ROLES)}
VERIA_FLASH4_PROVIDERS = {member: "deepseek" for member in VERIA_FLASH4_MODELS}


@dataclass
class Config:
    team_engine: str = "native"
    backend: str = "claude"
    model: str | None = None
    parallel: int = 3
    budget: float = 36000
    audit_interval: float = 900
    audit_report_timeout: float = 180
    review_interval: float = 1800
    no_flag_timeout: float = 0
    poll_interval: float = 15
    tick: float = 0.1
    epsilon: float = 0.1
    seed: int = 20261015
    operation_timeout: float = 30
    strategy_timeout: float = 180
    shutdown_timeout: float = 20
    heartbeat_timeout: float = 30
    role_turn_timeout: float = 900
    cleanup_reserve: float = 60
    max_retries: int = 2
    max_agents_per_team: int = 12
    veria_python: str | None = None
    veria_tool_image: str | None = None
    veria_provider: str | None = None
    veria_team_profile: str = "legacy6"
    veria_solver_ids: list[str] = field(default_factory=list)
    veria_providers: dict[str, str] = field(default_factory=dict)
    veria_models: dict[str, str] = field(default_factory=lambda: dict(VERIA_MODELS))
    veria_report_interval: float = 60
    veria_tool_cpu_limit: float | None = None
    veria_tool_memory_limit: str | None = None
    veria_tool_network: str = "bridge"
    veria_environment: str = "docker"
    veria_upstream_commit: str = VERIA_UPSTREAM_COMMIT

    def validate(self):
        if self.veria_team_profile not in {"legacy6", "mixed4", "flash4"}:
            raise ValueError("veria_team_profile must be legacy6/mixed4/flash4")
        expected_solvers = VERIA_LEGACY_SOLVERS if self.veria_team_profile == "legacy6" else VERIA_MIXED4_SOLVERS
        if not self.veria_solver_ids:
            self.veria_solver_ids = list(expected_solvers)
        if not isinstance(self.veria_solver_ids, (list, tuple)) or tuple(self.veria_solver_ids) != expected_solvers:
            raise ValueError("veria_solver_ids must exactly match the selected team profile")
        self.veria_solver_ids = list(self.veria_solver_ids)
        if self.veria_team_profile == "mixed4" and self.veria_models == VERIA_MODELS:
            self.veria_models = dict(VERIA_MIXED4_MODELS)
        if self.veria_team_profile == "flash4":
            if self.veria_models == VERIA_MODELS:
                self.veria_models = dict(VERIA_FLASH4_MODELS)
            if self.veria_models != VERIA_FLASH4_MODELS or self.model not in {None, "deepseek-flash"}:
                raise ValueError("flash4 requires deepseek-flash for every member and strategy")
            self.model = "deepseek-flash"
        expected_members = {*expected_solvers, *VERIA_SUPPORT_ROLES}
        if not isinstance(self.veria_models, dict) or set(self.veria_models) != expected_members:
            raise ValueError("veria_models must map every configured member ID")
        if any(not isinstance(value, str) or not value.strip() for value in self.veria_models.values()):
            raise ValueError("veria_models values must be nonempty model IDs")
        inferred_provider = self.veria_provider or (
            "glm" if self.veria_models and all(value.startswith("glm-") for value in self.veria_models.values())
            else "deepseek")
        if not self.veria_providers:
            self.veria_providers = (dict(VERIA_MIXED4_PROVIDERS) if self.veria_team_profile == "mixed4"
                                    else {member: inferred_provider for member in expected_members})
        if (not isinstance(self.veria_providers, dict) or set(self.veria_providers) != expected_members
                or any(provider not in {"deepseek", "glm"} for provider in self.veria_providers.values())):
            raise ValueError("veria_providers must bind every configured member to deepseek/glm")
        if self.veria_team_profile == "mixed4" and self.veria_providers != VERIA_MIXED4_PROVIDERS:
            raise ValueError("mixed4 providers must match the fixed four-solver roster")
        if self.veria_team_profile == "mixed4" and self.veria_provider not in {None, "deepseek"}:
            raise ValueError("mixed4 strategy provider must be deepseek")
        if self.veria_team_profile == "flash4":
            if self.veria_providers != VERIA_FLASH4_PROVIDERS or self.veria_provider not in {None, "deepseek"}:
                raise ValueError("flash4 requires the deepseek provider for every member and strategy")
            self.veria_provider = "deepseek"
        if self.veria_provider not in {None, "glm", "deepseek"}:
            raise ValueError("veria_provider must be glm/deepseek or unset for legacy runs")
        if self.team_engine not in {"native", "veria"}:
            raise ValueError("team_engine must be native/veria")
        if self.backend not in {"demo", "claude"} or not 1 <= self.parallel <= 3:
            raise ValueError("backend must be demo/claude; parallel must be 1..3")
        if not 0 <= self.epsilon <= 1 or self.max_retries < 0 or self.max_agents_per_team < 3 or self.cleanup_reserve < 0:
            raise ValueError("invalid exploration/recovery/agent limit")
        if (isinstance(self.no_flag_timeout, bool) or not isinstance(self.no_flag_timeout, (int, float))
                or not math.isfinite(self.no_flag_timeout) or self.no_flag_timeout < 0):
            raise ValueError("no_flag_timeout must be finite and nonnegative; zero keeps review-based switching")
        for key in ("budget", "audit_interval", "audit_report_timeout", "review_interval", "poll_interval", "tick",
                    "operation_timeout", "strategy_timeout", "shutdown_timeout", "heartbeat_timeout", "role_turn_timeout",
                    "veria_report_interval"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{key} must be positive")
        if not isinstance(self.veria_models, dict) or set(self.veria_models) != expected_members:
            raise ValueError("veria_models must map every configured member ID")
        if any(not isinstance(value, str) or not value.strip() for value in self.veria_models.values()):
            raise ValueError("veria_models values must be nonempty model IDs")
        if self.team_engine == "veria":
            if self.veria_environment not in {"docker", "hosted"}:
                raise ValueError("veria_environment must be docker/hosted")
            if self.backend != "claude":
                raise ValueError("Veria requires backend=claude; offline tests use injected adapters")
            if not isinstance(self.veria_python, str) or not Path(self.veria_python).is_absolute():
                raise ValueError("veria_python must be an absolute Python 3.14 interpreter path")
            if not Path(self.veria_python).is_file() or not os.access(self.veria_python, os.X_OK):
                raise ValueError("veria_python does not name an executable interpreter")
            if not isinstance(self.veria_tool_image, str) or not self.veria_tool_image.strip():
                raise ValueError("Veria requires an explicitly prepared veria_tool_image")
            cpu = self.veria_tool_cpu_limit
            if isinstance(cpu, bool) or not isinstance(cpu, (int, float)) or not math.isfinite(cpu) or cpu <= 0:
                raise ValueError("veria_tool_cpu_limit must be explicitly set to a positive number")
            memory = self.veria_tool_memory_limit
            if not isinstance(memory, str) or not re.fullmatch(r"[1-9][0-9]*(?:[bBkKmMgG])?", memory):
                raise ValueError("veria_tool_memory_limit must be an explicit positive Docker memory limit, e.g. 4g")
            if not isinstance(self.veria_tool_network, str) or not self.veria_tool_network or any(c.isspace() for c in self.veria_tool_network):
                raise ValueError("veria_tool_network must be a nonempty Docker network name without whitespace")
            if self.veria_upstream_commit != VERIA_UPSTREAM_COMMIT:
                raise ValueError("Veria upstream commit differs from this adapter's pinned revision")
        return self

    def to_dict(self):
        return asdict(self)

    @classmethod
    def load(cls, path: str | Path | None = None, overrides: dict | None = None):
        data = {}
        if path:
            data = tomllib.loads(Path(path).read_text()).get("harness", {})
        data.update({k: v for k, v in (overrides or {}).items() if v is not None})
        allowed = {f.name for f in fields(cls)}
        if set(data) - allowed:
            raise ValueError(f"unknown config fields: {sorted(set(data) - allowed)}")
        if not data.get("model"):
            data["model"] = os.getenv("ANTHROPIC_MODEL") or None
        return cls(**data).validate()
