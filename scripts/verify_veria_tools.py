#!/usr/bin/env python3
"""Offline container self-check. No model, platform, target or network request."""
import argparse
import asyncio
import json
from pathlib import Path
import tempfile
import time

from xinghuo.veria.environment import SharedDockerEnvironment


async def verify(image):
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="xinghuo-veria-tools-") as directory:
        settings = {"veria_tool_image": image, "veria_tool_cpu_limit": 2,
                    "veria_tool_memory_limit": "2g", "veria_tool_network": "none"}
        environment = SharedDockerEnvironment(Path(directory), "offline-tools-check", "synthetic", "one", settings)
        try:
            ready = await environment.start()
            command = "python -c 'import os,json,requests,Crypto,sympy,capstone,pwn; from pathlib import Path; Path(\"private-proof.txt\").write_text(os.environ[\"XINGHUO_MEMBER\"]); print(json.dumps({\"cwd\":os.getcwd(),\"python\":os.sys.executable,\"member\":os.environ[\"XINGHUO_MEMBER\"]}))'"
            ids = [f"{family}-{n}" for family in ("pro", "flash") for n in (1, 2, 3)]
            results = await asyncio.gather(*(environment.execute(agent, command) for agent in ids))
            failures = [r for r in results if r["returncode"]]
            if failures:
                raise RuntimeError("Tool import/private environment check failed: " + str(failures))
            members = [json.loads(result["stdout"].strip()) for result in results]
            assert len({result["cwd"] for result in members}) == 6
            assert len({result["python"] for result in members}) == 6
            background = await environment.execute("pro-1", "sleep 120 >/dev/null 2>&1 &")
            assert background["returncode"] == 0
            await environment.stop_member("pro-1")
            health = await environment.health()
            assert health["healthy"]
            environment.quiesce(True)
            try:
                await environment.execute("pro-2", "echo must-not-run")
                raise AssertionError("quiesce allowed a new ordinary command")
            except RuntimeError as error:
                if isinstance(error, AssertionError):
                    raise
            environment.quiesce(False)
            continued = await environment.execute("pro-2", "cat private-proof.txt")
            assert continued["returncode"] == 0 and continued["stdout"].strip() == "pro-2"
            return {"kind": "offline_docker_self_check", "model_calls": 0, "platform_calls": 0,
                    "network": "none", "image_id": ready["image_id"],
                    "description": ready["description"], "members": members,
                    "checks": ["six_parallel_private_venvs", "installed_library_imports",
                               "member_cleanup_preserves_container", "quiesce_and_cancel"],
                    "seconds": round(time.monotonic() - started, 3)}
        finally:
            await environment.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="xinghuo-veria-tools:local")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(verify(args.image))
    text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    print(text)
