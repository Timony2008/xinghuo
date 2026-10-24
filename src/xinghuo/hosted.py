"""Self-starting hosted Agent entrypoint. Secrets are runtime environment only."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import sqlite3
import sys
import time
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from .config import Config
from .model_connection import connection, profile_contract, provider_name
from .resource_health import host_capacity
from .store import RunLock, Store


class HostedRunError(RuntimeError):
    """The run already emitted a redacted hosted_error event."""


@dataclass
class HostedRun:
    directory: Path
    mode: str
    config: Config
    store: Store
    budget_source: str
    parallel_source: str


def configure_model_environment(env, config=None):
    deployment = env.get("XINGHUO_DEPLOYMENT", "hosted")
    if deployment not in {"hosted", "local"}:
        raise ValueError("XINGHUO_DEPLOYMENT must be hosted/local")
    scoped_env = {**env, "XINGHUO_DEPLOYMENT": deployment}
    contract = profile_contract(env, default_profile="flash4" if deployment == "hosted" else "legacy6") if config is None else {
        "team_profile": config.veria_team_profile, "models": config.veria_models,
        "providers": config.veria_providers}
    if contract["team_profile"] == "flash4":
        profile_contract({**env, "XINGHUO_TEAM_PROFILE": "flash4"})
    # Validate every selected service before opening a platform client. This is
    # configuration validation only; authenticated network checks are separate.
    endpoints = {}
    if contract["team_profile"] in {"mixed4", "flash4"}:
        for selected in sorted(set(contract["providers"].values())):
            endpoints[selected] = connection(scoped_env, provider=selected)
        provider, key, base = endpoints["deepseek"]
    else:
        provider, key, base = connection(scoped_env)
        endpoints[provider] = (provider, key, base)
    parsed = urlsplit(base)
    if provider == "deepseek":
        expected = ("http", "api.deepseek.com.tsecbench.gw") if deployment == "hosted" else ("https", "api.deepseek.com")
        if ((parsed.scheme, parsed.hostname) != expected or parsed.port
                or parsed.path.rstrip("/") != "/anthropic"):
            raise ValueError("DEEPSEEK_BASE_URL does not match this deployment's DeepSeek Anthropic endpoint")
    if deployment == "hosted" and "glm" in endpoints and contract["team_profile"] == "mixed4":
        glm = urlsplit(endpoints["glm"][2])
        if (glm.scheme, glm.hostname, glm.port, glm.path.rstrip("/")) != ("http", "console.cloudrouter.com.cn.tsecbench.gw", None, ""):
            raise ValueError("GLM_BASE_URL does not match the hosted CloudRouter Anthropic root")
    model = env.get("XINGHUO_STRATEGY_MODEL", contract["models"]["solver-1"] if contract["team_profile"] == "mixed4" else contract["models"]["coordinator"])
    env.update({"ANTHROPIC_BASE_URL": base, "ANTHROPIC_API_KEY": key,
        "ANTHROPIC_AUTH_TOKEN": key, "ANTHROPIC_MODEL": model,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "CLAUDECODE": "",
        "CLAUDE_CODE_USE_BEDROCK": "", "CLAUDE_CODE_USE_VERTEX": "", "CLAUDE_CODE_USE_FOUNDRY": ""})
    for name in ("ANTHROPIC_DEFAULT_OPUS_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL",
                 "ANTHROPIC_DEFAULT_HAIKU_MODEL", "ANTHROPIC_SMALL_FAST_MODEL", "CLAUDE_CODE_SUBAGENT_MODEL"):
        env[name] = model
    return {"provider": provider, "gateway_host": parsed.hostname, "strategy_model": model,
            "deployment": deployment, "team_profile": contract["team_profile"],
            "provider_hosts": {name: urlsplit(value[2]).hostname for name, value in endpoints.items()}}


def hosted_config(env):
    contract = profile_contract(env, default_profile="flash4")
    return Config(team_engine="veria", backend="claude", veria_environment="hosted",
        veria_provider="deepseek" if contract["team_profile"] in {"mixed4", "flash4"} else provider_name(env),
        veria_team_profile=contract["team_profile"], veria_solver_ids=contract["solver_ids"],
        veria_providers=contract["providers"],
        veria_python=sys.executable, veria_tool_image="embedded-hosted-image",
        veria_tool_cpu_limit=8, veria_tool_memory_limit="16g", veria_tool_network="platform-managed",
        budget=float(env.get("XINGHUO_BUDGET_SECONDS", "28800")),
        no_flag_timeout=float(env.get("XINGHUO_NO_FLAG_TIMEOUT", "900")),
        parallel=int(env.get("XINGHUO_PARALLEL", "3")), cleanup_reserve=180,
        model=env.get("XINGHUO_STRATEGY_MODEL", contract["models"]["solver-1"] if contract["team_profile"] == "mixed4" else contract["models"]["coordinator"]),
        veria_models=contract["models"]).validate()


def _saved_config(directory, env):
    """Validate resume read-only, before opening Store or changing any state."""
    database = directory / "state.sqlite3"
    if not database.is_file():
        raise ValueError("resume requires an existing state.sqlite3; use new with a fresh run directory")
    try:
        with contextlib.closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as db:
            metadata = {key: json.loads(body) for key, body in db.execute("SELECT key,body FROM meta")}
        config = Config(**metadata["config"]).validate()
        started, deadline = metadata["started_at"], metadata["deadline"]
        if (not metadata.get("run_id") or any(isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) for value in (started, deadline)) or deadline <= started):
            raise ValueError("invalid saved clock")
    except (sqlite3.Error, KeyError, TypeError, ValueError):
        raise ValueError("resume requires valid saved configuration and absolute deadline; choose a fresh directory for new") from None
    if config.team_engine != "veria" or config.backend != "claude" or config.veria_environment != "hosted":
        raise ValueError("resume requires a saved hosted Veria run")
    for name, field, parse in (("XINGHUO_BUDGET_SECONDS", "budget", float),
                              ("XINGHUO_PARALLEL", "parallel", int),
                              ("XINGHUO_NO_FLAG_TIMEOUT", "no_flag_timeout", float),
                              ("XINGHUO_STRATEGY_MODEL", "model", str)):
        if name in env:
            try:
                requested = parse(env[name])
            except (TypeError, ValueError):
                raise ValueError(f"invalid {name} for resume") from None
            if requested != getattr(config, field):
                raise ValueError(f"resume cannot change {name}; remove the override to use saved settings, "
                                 "or choose new with a fresh run directory")
    if "XINGHUO_TEAM_PROFILE" in env and env["XINGHUO_TEAM_PROFILE"] != config.veria_team_profile:
        raise ValueError("resume cannot change team profile/solver roster; choose new with a fresh run directory")
    saved_provider = config.veria_provider
    if saved_provider is None:
        models = list(config.veria_models.values())
        saved_provider = next((name for name in ("glm", "deepseek")
                               if all(model.startswith(name + "-") for model in models)), None)
    if config.veria_team_profile == "flash4":
        profile_contract({**env, "XINGHUO_TEAM_PROFILE": "flash4"})
        overrides = ()
    elif config.veria_team_profile == "mixed4":
        if env.get("XINGHUO_PROVIDER") not in {None, "", "deepseek"}:
            raise ValueError("resume cannot change the mixed4 strategy provider")
        overrides = (("XINGHUO_STRONG_MODEL", ("solver-1",)),
                     ("XINGHUO_FAST_MODEL", ("solver-2", "reporter")),
                     ("DEEPSEEK_STRONG_MODEL", ("solver-1",)),
                     ("DEEPSEEK_FAST_MODEL", ("solver-2", "reporter")),
                     ("GLM_STRONG_MODEL", ("solver-3", "coordinator", "reviewer")), ("GLM_FAST_MODEL", ("solver-4",)))
    else:
        explicit_provider = any(env.get(name) for name in ("XINGHUO_PROVIDER", "GLM_API_KEY", "DEEPSEEK_API_KEY"))
        if explicit_provider and saved_provider and provider_name(env) != saved_provider:
            raise ValueError("resume cannot change provider; choose new with a fresh run directory")
        overrides = (("XINGHUO_STRONG_MODEL", ("pro-1", "pro-2", "pro-3", "coordinator", "reviewer")),
                     ("XINGHUO_FAST_MODEL", ("flash-1", "flash-2", "flash-3", "reporter")))
    for name, roles in overrides:
        if name in env and any(config.veria_models[role] != env[name] for role in roles):
            raise ValueError(f"resume cannot change {name}; remove the override to inherit saved models")
    config.veria_provider = saved_provider
    return config


@contextlib.contextmanager
def prepare_hosted_run(env, *, work_root=Path("/work")):
    """Explicit new/resume, serialized before inspecting or opening a journal."""
    root = Path(work_root).resolve()
    directory = Path(env.get("XINGHUO_RUN_DIR", str(root / "run"))).resolve()
    if not directory.is_relative_to(root) or directory == root:
        raise ValueError("XINGHUO_RUN_DIR must be a directory under /work")
    mode = env.get("XINGHUO_RUN_MODE", "new")
    if mode not in {"new", "resume"}:
        raise ValueError("XINGHUO_RUN_MODE must be new/resume")
    if mode == "resume" and not (directory / "state.sqlite3").is_file():
        raise ValueError("resume requires an existing state.sqlite3; use new with a fresh run directory")
    lock_name = ".hosted-entrypoint.lock"
    lock = RunLock(directory / lock_name)
    store = None
    try:
        if mode == "new":
            if any(path.name != lock_name for path in directory.iterdir()):
                raise ValueError("new requires an empty run directory; saved data is preserved. "
                                 "Choose a fresh XINGHUO_RUN_DIR or explicitly set XINGHUO_RUN_MODE=resume")
            config = hosted_config(env)
            budget_source = "environment" if "XINGHUO_BUDGET_SECONDS" in env else "default"
            parallel_source = "environment" if "XINGHUO_PARALLEL" in env else "default"
        else:
            config = _saved_config(directory, env)
            budget_source = parallel_source = "saved_config"
        store = Store(directory)
        yield HostedRun(directory, mode, config, store, budget_source, parallel_source)
    finally:
        if store is not None:
            store.close()
        lock.close()


def hosted_event(event, run, snapshot, *, now=None, connection=None):
    """Format one actual persisted schedule, with explicit local-zone timestamps."""
    now = time.time() if now is None else now
    zone = ZoneInfo("Asia/Shanghai")
    def iso(value):
        return datetime.fromtimestamp(value, zone).isoformat(timespec="seconds") if value is not None else None
    fields = ("run_id", "status", "resumed", "exit_reason", "started_at", "deadline", "work_deadline",
              "budget_seconds", "cleanup_reserve_seconds", "elapsed_seconds", "remaining_seconds",
              "deadline_remaining_seconds", "work_remaining_seconds", "round_id")
    result = {name: snapshot[name] for name in fields if name in snapshot}
    result.update(event=event, run_mode=run.mode, run_dir=str(run.directory),
                  configured_budget_seconds=run.config.budget, parallel=run.config.parallel,
                  no_flag_timeout_seconds=run.config.no_flag_timeout,
                  budget_source=run.budget_source, parallel_source=run.parallel_source,
                  timezone="Asia/Shanghai", observed_at=now, observed_at_iso=iso(now),
                  resources=host_capacity())
    for name in ("started_at", "work_deadline", "deadline"):
        result[name + "_iso"] = iso(snapshot.get(name))
    if connection is not None:
        result["connection"] = connection
    rows = snapshot.get("challenges", [])
    result.update(completed=sum(row["status"] == "completed" for row in rows),
                  active=sum(row["status"] == "active" for row in rows), total=len(rows))
    if event == "hosted_start":
        result["message"] = "已按实际保存的截止时间开始运行；平台页面倒计时不会延长本程序预算。"
    elif event == "hosted_stopping":
        result["message"] = "解题调度已停止，正在保存状态并关闭题目实例。"
    elif event == "hosted_finished":
        result.update(finished_at=now, finished_at_iso=iso(now),
                      message="本程序已结束解题并完成收尾流程；平台页面仍在计时也不会继续解题。")
    return result


def _error_detail(error, env):
    detail = str(error) if type(error) is ValueError else "Check runtime connectivity and platform logs"
    for name in ("GLM_API_KEY", "DEEPSEEK_API_KEY", "BENCHMARK_TOKEN", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY"):
        if env.get(name):
            detail = detail.replace(env[name], "[REDACTED]")
    return detail


def _emit(payload):
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def offline_check():
    from claude_agent_sdk import __file__ as sdk_file
    cli = Path(sdk_file).parent / "_bundled/claude"
    marker = Path("/opt/xinghuo/HOSTED_IMAGE.json")
    required = [marker, Path("/usr/local/bin/xinghuo_exec.py"), cli]
    ok = (sys.platform == "linux" and sys.version_info[:2] == (3, 14)
          and all(path.is_file() for path in required) and os.access(cli, os.X_OK))
    return {"ok": ok, "mode": "offline-image-check", "python": platform.python_version(),
        "architecture": platform.machine(), "sdk": importlib.metadata.version("claude-agent-sdk"),
        "benchmark_sdk": importlib.metadata.version("tsec-benchmark"),
        "missing_files": [str(path) for path in required if not path.is_file()],
        "tool_execution": "hosted_process", "nested_docker_required": False,
        "hosted_interface_version": 3, "default_run_mode": "new",
        "default_budget_seconds": 28800, "default_parallel": 3,
        "default_provider": "deepseek", "default_team_profile": "flash4",
        "default_no_flag_timeout_seconds": 900,
        "default_solver_ids": profile_contract({}, default_profile="flash4")["solver_ids"],
        "default_role_models": profile_contract({}, default_profile="flash4")["models"],
        "model_calls": 0, "platform_calls": 0}


async def run(*, work_root=Path("/work")):
    missing = [name for name in ("BENCHMARK_BASE_URL", "BENCHMARK_TOKEN")
               if not os.environ.get(name)]
    if missing:
        raise ValueError("Missing runtime variables: " + ", ".join(missing))
    if not offline_check()["ok"]:
        raise ValueError("Hosted image preflight failed")
    from tsec_benchmark import TSecBenchmarkAsync
    from .benchmark import BenchmarkPlatform
    from .cli import _signals
    from .supervisor import Supervisor
    with prepare_hosted_run(os.environ, work_root=work_root) as prepared:
        client = progress = supervisor = None
        stage = "model_configuration"
        try:
            model_env = dict(os.environ)
            if prepared.config.veria_provider:
                model_env["XINGHUO_PROVIDER"] = prepared.config.veria_provider
                os.environ["XINGHUO_PROVIDER"] = prepared.config.veria_provider
            model_env["XINGHUO_STRATEGY_MODEL"] = prepared.config.model
            model_env["XINGHUO_TEAM_PROFILE"] = prepared.config.veria_team_profile
            os.environ["XINGHUO_TEAM_PROFILE"] = prepared.config.veria_team_profile
            connection = configure_model_environment(model_env, prepared.config)
            os.environ.update({key: value for key, value in model_env.items()
                               if key.startswith("ANTHROPIC_") or key.startswith("CLAUDE_") or key == "CLAUDECODE"})
            resources = host_capacity()
            _emit({"event": "hosted_boot", "run_mode": prepared.mode, "run_dir": str(prepared.directory),
                   "configured_budget_seconds": prepared.config.budget, "parallel": prepared.config.parallel,
                   "no_flag_timeout_seconds": prepared.config.no_flag_timeout,
                   "role_models": prepared.config.veria_models, "role_providers": prepared.config.veria_providers,
                   "team_profile": prepared.config.veria_team_profile, "solver_ids": prepared.config.veria_solver_ids,
                   "resources": resources,
                   "message": "正在检查运行环境；实际截止时间将在 hosted_start 中输出。"})
            if resources["nominal_limit_exceeds_host"]:
                _emit({"event": "hosted_resource_warning", "resources": resources,
                       "message": "容器名义内存上限高于实际主机容量；有效上限受主机内存限制。"})
            if prepared.mode == "new" and (prepared.config.budget < 28800 or prepared.config.parallel < 3):
                _emit({"event": "hosted_configuration_warning", "budget_seconds": prepared.config.budget,
                       "parallel": prepared.config.parallel,
                       "message": "当前显式配置少于 8 小时或三题并发；将按该配置运行，不会自动扩大预算。"})
            stage = "vpn_check"
            client = TSecBenchmarkAsync(base_url=os.environ["BENCHMARK_BASE_URL"],
                token=os.environ["BENCHMARK_TOKEN"], timeout=prepared.config.operation_timeout)
            await client.check_vpn()
            def lifecycle(snapshot):
                event = {"initialized": "hosted_start", "stopping": "hosted_stopping"}[snapshot["event"]]
                current = supervisor.status()
                current.update(snapshot)
                _emit(hosted_event(event, prepared, current, connection=connection))
            stage = "supervisor"
            supervisor = Supervisor(BenchmarkPlatform(client), prepared.store, prepared.config, on_lifecycle=lifecycle)
            _signals(supervisor)
            async def report_progress():
                while True:
                    await asyncio.sleep(30)
                    _emit(hosted_event("hosted_progress", prepared, supervisor.status()))
            progress = asyncio.create_task(report_progress())
            result = await supervisor.run()
            stage = "client_close"
            closing_client, client = client, None
            await asyncio.wait_for(closing_client.aclose(), timeout=prepared.config.operation_timeout)
            result = supervisor.status()
            _emit(hosted_event("hosted_finished", prepared, result))
            return result
        except Exception as error:
            snapshot = supervisor.status() if supervisor is not None else {}
            snapshot.update(status="error", exit_reason="runtime_error" if supervisor is not None else "startup_error")
            payload = hosted_event("hosted_error", prepared, snapshot)
            payload.update(stage=stage, error_type=type(error).__name__, message=_error_detail(error, os.environ))
            _emit(payload)
            raise HostedRunError("Hosted run failed; see hosted_error") from None
        finally:
            if progress:
                progress.cancel()
                await asyncio.gather(progress, return_exceptions=True)
            if client:
                try:
                    await asyncio.wait_for(client.aclose(), timeout=prepared.config.operation_timeout)
                except Exception as error:
                    _emit({"event": "hosted_cleanup_warning", "run_mode": prepared.mode,
                           "stage": "client_close", "error_type": type(error).__name__,
                           "message": "关闭平台连接失败；此前退出原因保持不变。"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true", help="Offline image check; no credentials or network")
    modes.add_argument("--api-check", action="store_true", help="Read-only VPN and challenge listing")
    args = parser.parse_args()
    try:
        if args.check:
            result = offline_check()
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return 0 if result["ok"] else 1
        if args.api_check:
            from .preflight import check_api
            result = asyncio.run(check_api())
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return 0 if result["ok"] else 1
        asyncio.run(run())
        return 0
    except HostedRunError:
        return 1
    except Exception as error:
        # Never echo provider response bodies or arbitrary SDK exception detail.
        print(json.dumps({"event": "hosted_error", "exit_reason": "startup_error",
            "error_type": type(error).__name__, "message": _error_detail(error, os.environ)}, ensure_ascii=False), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
