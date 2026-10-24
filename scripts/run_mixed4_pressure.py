#!/usr/bin/env python3
"""Run resource acceptance in exactly one owned offline 8CPU/16GiB container."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=180)
    parser.add_argument("--profile", choices=("flash4", "mixed4"), default="flash4")
    args = parser.parse_args()
    info = json.loads(subprocess.check_output(["docker", "info", "--format", "{{json .}}"], text=True))
    if info["MemTotal"] < 19 * 1024**3 or info["NCPU"] < 8:
        raise SystemExit("Actual Docker VM must provide at least 19GiB/8CPU (configure 20GiB for overhead)")
    image_id = subprocess.check_output(["docker", "image", "inspect", "--format", "{{.Id}}", args.image], text=True).strip()
    name = "xinghuo-" + args.profile + "-pressure-" + uuid.uuid4().hex[:12]
    started = time.monotonic()
    report = {"status": "failed"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(["docker", "run", "--name", name, "-i", "--network", "none", "--cpus", "8",
            "--memory", "16g", "--memory-swap", "16g", "--entrypoint", "/opt/sdk/bin/python", args.image,
            "-", "--profile", args.profile, "--seconds", str(args.seconds), "--output", "/work/offline-pressure.json"],
            input=Path(__file__).with_name("verify_mixed4_pressure.py").read_text(),
            text=True, capture_output=True, timeout=max(600, args.seconds + 300))
        copied = subprocess.run(["docker", "cp", name + ":/work/offline-pressure.json", str(args.output)], capture_output=True, text=True)
        if copied.returncode == 0:
            report = json.loads(args.output.read_text())
        else:
            report["copy_error"] = copied.stderr[-1500:]
        state = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]["State"]
        report.update(container_exit_code=state["ExitCode"], container_oom_killed=state["OOMKilled"],
                      runner_returncode=result.returncode)
        if result.returncode or state["OOMKilled"]:
            report["status"] = "failed"
        report["runner_stdout"] = result.stdout[-3000:]
        report["runner_stderr"] = result.stderr[-5000:]
    except (subprocess.TimeoutExpired, ValueError, subprocess.CalledProcessError) as error:
        report.update(status="failed", runner_error=type(error).__name__ + ": " + str(error)[:1000])
    finally:
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
    report.update(image=args.image, image_id=image_id, docker_vm_memory_bytes=info["MemTotal"],
                  requested_profile=args.profile,
                  limits={"cpus": 8, "memory_bytes": 16 * 1024**3, "swap_bytes": 0},
                  duration_seconds=round(time.monotonic() - started, 3), network="none", paid_model_calls=0)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: report.get(key) for key in ("status", "image", "duration_seconds", "oom_kill_delta", "errors", "runner_error")}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
