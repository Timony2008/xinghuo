#!/usr/bin/env python3
"""Exercise real SDK/CLI on two loopback services; never contacts a provider."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

from verify_glm_sdk_wire import OfflineEnvironment, fixture_server
from xinghuo.model_connection import profile_contract
from xinghuo.veria.driver import VeriaRoleDriver, client_environment, DriverError
from xinghuo.veria.prompts import build_role_prompt


async def run_member(member, provider, model, root, role="solver"):
    environment = OfflineEnvironment()
    context = {"provider": provider, "solver_ids": [f"solver-{n}" for n in range(1, 5)],
               "challenge": {"description": "Synthetic offline loopback tool routing fixture"}}
    async def tool(*_):
        return {"messages": []}
    # Use the production solver role, including its pinned JSON output schema.
    driver = VeriaRoleDriver(member, role, model, root / member, context, environment, tool)
    try:
        await driver.start()
        prompt = build_role_prompt(role, member, context, environment._description)
        result = await asyncio.wait_for(driver.turn("Run fixed offline fixture then return offline-ok."), 45)
        assert not result["is_error"], result
        if role == "solver":
            assert result["structured_output"] == {"type": "flag_found", "flag": "",
                                                    "method": "offline fixture; no acquired flag"}, result
        else:
            assert result["result"] == "offline-ok", result
        assert environment.executed == ["printf offline-wire-tool"]
        return {"member_id": member, "role": role, "provider": provider, "model": model,
                "tool_continuation_passed": True, "production_output_schema_passed": role == "solver", "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest()}
    finally:
        await driver.close()


async def verify(ds_url, ds_captured, glm_url, glm_captured):
    contract = profile_contract({"XINGHUO_TEAM_PROFILE": "mixed4"})
    # Freeze fixture environment before clients start. Each SDK independently
    # binds its explicit provider; global aliases deliberately point elsewhere.
    env = {name: os.environ[name] for name in ("PATH", "HOME", "USER", "TMPDIR", "LANG") if name in os.environ}
    env.update({"XINGHUO_TEAM_PROFILE": "mixed4", "DEEPSEEK_API_KEY": "synthetic-ds-key",
                "DEEPSEEK_BASE_URL": ds_url + "/anthropic", "GLM_API_KEY": "synthetic-glm-key",
                "GLM_BASE_URL": glm_url, "BENCHMARK_TOKEN": "synthetic-platform-token",
                "ANTHROPIC_API_KEY": "synthetic-stale-key", "ANTHROPIC_AUTH_TOKEN": "synthetic-stale-key",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "CLAUDECODE": ""})
    with tempfile.TemporaryDirectory(prefix="mixed4-wire-") as temp, patch.dict(os.environ, env, clear=True):
        root = Path(temp)
        members = [run_member(member, contract["providers"][member], contract["models"][member], root)
                   for member in contract["solver_ids"]]
        # Same opaque alias on both services catches inference based on model ID.
        members += [run_member("same-alias-ds", "deepseek", "same-routing-alias", root),
                    run_member("same-alias-glm", "glm", "same-routing-alias", root)]
        members.append(run_member("coordinator", contract["providers"]["coordinator"],
                                  contract["models"]["coordinator"], root, role="coordinator"))
        members.append(run_member("reviewer", contract["providers"]["reviewer"],
                                  contract["models"]["reviewer"], root, role="reviewer"))
        runs = await asyncio.gather(*members)
        try:
            client_environment("glm-5.3", root, provider="glm", environ=env | {"GLM_API_KEY": ""})
        except DriverError as error:
            assert "Missing GLM_API_KEY" in str(error)
        else:
            raise AssertionError("Missing GLM key silently borrowed another credential")
    requests = []
    for provider, captured, path, key, models in (
        ("deepseek", ds_captured, "/anthropic/v1/messages", "synthetic-ds-key",
         {"deepseek-v4-pro", "deepseek-flash", "same-routing-alias"}),
        ("glm", glm_captured, "/v1/messages", "synthetic-glm-key",
         {"glm-5.3", "glm-5.3-flash", "same-routing-alias"})):
        captured = [item for item in captured if item["path"].split("?")[0] == path]
        assert len(captured) >= 6, (provider, len(captured))
        seen = set()
        for request in captured:
            body = request["body"]
            assert request["auth"] in {key, "Bearer " + key}, "Wrong service credential"
            assert body["model"] in models, "Model crossed service boundary"
            assert body["stream"] is True
            tool_results = sum(block.get("type") == "tool_result" for message in body["messages"]
                               if isinstance(message.get("content"), list) for block in message["content"])
            if tool_results:
                seen.add(body["model"])
            requests.append({"provider": provider, "path": path, "model": body["model"],
                             "auth_matches_selected_service": True, "stream": True,
                             "tool_result_count": tool_results})
        assert seen == models, (provider, "Missing tool continuation", seen)
    return {"passed": True, "real_model_calls": 0, "platform_calls": 0,
            "endpoint_scope": "two independent 127.0.0.1 HTTP servers",
            "frozen_environment_before_concurrency": True, "runs": runs, "requests": requests,
            "same_alias_different_provider_passed": True, "missing_key_rejected": True,
            "gateway_and_account_access_verified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with fixture_server("/anthropic/v1/messages", structured=True) as (ds_url, ds_captured), fixture_server(structured=True) as (glm_url, glm_captured):
        try:
            report = asyncio.run(verify(ds_url, ds_captured, glm_url, glm_captured))
        except Exception as error:
            report = {"passed": False, "error_type": type(error).__name__, "message": str(error),
                      "real_model_calls": 0, "platform_calls": 0}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key not in {"runs", "requests"}}, ensure_ascii=False))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
