#!/usr/bin/env python3
"""Run complete tools acceptance through the actual member execution helper.

The image is started with no external network and no mounted challenge inputs.
The hosted default UID remains 10001; the local tools default UID remains 0.
No model or platform query occurs. This intentionally does not submit an image.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid


BOOTSTRAP = r'''
import json, os, pathlib, shlex, subprocess, sys, tempfile
mode, phase = sys.argv[1:]
root = pathlib.Path(tempfile.mkdtemp(prefix="full-tools-acceptance-", dir="/work" if mode == "hosted" else "/challenge"))
try:
    control = root / "control"
    control.mkdir()
    (control / "dispatch.json").write_text(json.dumps({"allowed": True, "generation": "offline-acceptance"}))
    (control / "dispatch-offline-acceptance.json").write_text(json.dumps({"allowed": True,
        "generation": "offline-acceptance", "member": "pro-1", "command_id": "offline-acceptance"}))
    command = shlex.join(["python", "/opt/xinghuo/veria_tool_acceptance.py", "--phase", phase,
                          "--expect-uid", "10001" if mode == "hosted" else "0"])
    result = subprocess.run([sys.executable, "/usr/local/bin/xinghuo_exec.py", "run",
        "--member", "pro-1", "--generation", "offline-acceptance", "--command-id", "offline-acceptance",
        "--", command], env={**os.environ, "XINGHUO_TOOL_ROOT": str(root)})
    raise SystemExit(result.returncode)
finally:
    import shutil
    subprocess.run([sys.executable, "/usr/local/bin/xinghuo_exec.py", "cleanup", "--member", "pro-1",
        "--generation", "offline-acceptance"], env={**os.environ, "XINGHUO_TOOL_ROOT": str(root)},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    shutil.rmtree(root)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--mode", choices=("hosted", "tools"), required=True)
    parser.add_argument("--phase", choices=("all", "inventory", "functional"), default="all")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpus", default="4")
    parser.add_argument("--memory", default="8g")
    args = parser.parse_args()
    started = time.monotonic()
    inspection = subprocess.run(["docker", "image", "inspect", args.image], capture_output=True, text=True, check=True)
    image = json.loads(inspection.stdout)[0]
    # Supplying the bootstrap on stdin avoids host mounts or altering the image.
    container_name = "xinghuo-offline-acceptance-" + uuid.uuid4().hex[:16]
    argv = ["docker", "run", "--name", container_name, "--rm", "-i", "--network", "none", "--cpus", args.cpus,
            "--memory", args.memory, "--entrypoint", "/opt/sdk/bin/python", args.image,
            "-", args.mode, args.phase]
    try:
        result = subprocess.run(argv, input=BOOTSTRAP, capture_output=True, text=True, timeout=2700)
    except subprocess.TimeoutExpired as error:
        result = subprocess.CompletedProcess(argv, 124, error.stdout or "", error.stderr or "")
        if isinstance(result.stdout, bytes):
            result.stdout = result.stdout.decode("utf-8", errors="replace")
        if isinstance(result.stderr, bytes):
            result.stderr = result.stderr.decode("utf-8", errors="replace")
        result.stderr += "\nOffline acceptance timed out after 2700 seconds."
    finally:
        # Killing a Docker CLI after a timeout does not reliably kill its
        # container. Remove exactly the container created by this verification.
        subprocess.run(["docker", "rm", "-f", container_name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=30)
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError:
        report = {"kind": "veria_full_tool_acceptance", "status": "failed",
                  "bootstrap_error": result.stderr[-24000:], "stdout": result.stdout[-24000:]}
    report.update({"image": args.image, "image_id": image["Id"], "image_architecture": image["Architecture"],
                   "image_size_bytes": image["Size"], "mode": args.mode, "phase": args.phase,
                   "docker_network": "none", "docker_limits": {"cpus": args.cpus, "memory": args.memory},
                   "runner_returncode": result.returncode, "runner_seconds": round(time.monotonic() - started, 3),
                   "acceptance_script_sha256": hashlib.sha256(Path(__file__).with_name("veria_tool_acceptance.py").read_bytes()).hexdigest()})
    if result.returncode:
        report["status"] = "failed"
        report["runner_stderr"] = result.stderr[-24000:]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"status": report["status"], "image": args.image, "output": str(args.output.resolve()),
                      "failed_cases": [name for name, item in report.get("functional", {}).items() if item["status"] == "failed"]}, ensure_ascii=False))
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
