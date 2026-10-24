"""Run inside the built image under --network none. Never queries a model."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import time

from xinghuo.hosted import offline_check
from xinghuo.model_connection import profile_contract
from xinghuo.veria.driver import VeriaRoleDriver
from xinghuo.veria.hosted_environment import HostedProcessEnvironment


async def main():
    started = time.monotonic()
    check = offline_check()
    assert check["ok"]
    assert check["default_team_profile"] == "flash4", check
    assert check["default_no_flag_timeout_seconds"] == 900, check
    assert check["default_budget_seconds"] == 28800 and check["default_parallel"] == 3, check
    assert set(check["default_role_models"].values()) == {"deepseek-flash"}, check
    contract = profile_contract({"XINGHUO_TEAM_PROFILE": "flash4"})
    assert set(contract["models"].values()) == {"deepseek-flash"}, contract
    assert set(contract["providers"].values()) == {"deepseek"}, contract
    with tempfile.TemporaryDirectory(prefix="verify-", dir="/work") as temp:
        env = HostedProcessEnvironment(Path(temp)/"one", "offline", "one", "attempt", {})
        sibling = HostedProcessEnvironment(Path(temp)/"two", "offline", "two", "attempt", {})
        await env.start()
        await sibling.start()
        driver = None
        try:
            os.environ["DEEPSEEK_API_KEY"] = "offline-sentinel"
            os.environ["BENCHMARK_TOKEN"] = "offline-sentinel"
            os.environ["DEEPSEEK_BASE_URL"] = "http://127.0.0.1:9/anthropic"
            command = "python -c 'import os,json,requests,Crypto,sympy,capstone,pwn; from pathlib import Path; Path(\"private.txt\").write_text(os.environ[\"XINGHUO_MEMBER\"]); print(json.dumps({\"cwd\":os.getcwd(),\"python\":os.sys.executable,\"secret_inherited\":any(k in os.environ for k in (\"DEEPSEEK_API_KEY\",\"BENCHMARK_TOKEN\"))}))'"
            members = contract["solver_ids"]
            results = await asyncio.gather(*(env.execute(member, command, timeout=180) for member in members))
            assert all(result["returncode"] == 0 for result in results), results
            rows = [json.loads(result["stdout"]) for result in results]
            assert len({row["cwd"] for row in rows}) == 4
            assert len({row["python"] for row in rows}) == 4
            assert not any(row["secret_inherited"] for row in rows)
            await env.execute("solver-1", "sleep 120 >/dev/null 2>&1 &")
            await sibling.execute("solver-1", "sleep 120 >/dev/null 2>&1 & echo $! > sibling.pid")
            await env.stop_member("solver-1")
            still_alive = await sibling.execute("solver-1", "kill -0 $(cat sibling.pid)")
            assert still_alive["returncode"] == 0
            env.quiesce(True)
            try:
                await env.execute("solver-2", "echo forbidden")
                raise AssertionError("quiesce admitted command")
            except RuntimeError:
                pass
            env.quiesce(False)
            assert (await env.execute("solver-2", "cat private.txt"))["stdout"].strip() == "solver-2"
            async def on_tool(name, args):
                return {"ok": True}
            driver = VeriaRoleDriver("solver-1", "solver", contract["models"]["solver-1"], Path(temp)/"sdk",
                {"provider": contract["providers"]["solver-1"], "team_profile": "flash4", "solver_ids": members,
                 "challenge": {"description": "offline initialization only"}}, env, on_tool)
            # Real bundled Linux CLI and MCP initialization only: no query/turn.
            await asyncio.wait_for(driver.start(), timeout=90)
            assert driver.alive
            await asyncio.wait_for(driver.close(), timeout=30)
            driver = None
            return {**offline_check(), "profile": contract, "checks": ["all_roles_use_deepseek_flash",
                "four_private_workspaces_and_venvs", "tool_imports",
                "secrets_not_in_tool_environment", "cross_attempt_process_cleanup",
                "reversible_quiesce", "real_sdk_cli_and_mcp_initialization_without_query"],
                "seconds": round(time.monotonic()-started, 3), "members": rows}
        finally:
            if driver:
                await driver.close()
            await env.stop()
            await sibling.stop()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main()), ensure_ascii=False, indent=2))
