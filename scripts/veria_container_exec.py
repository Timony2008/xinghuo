#!/usr/bin/env python3
"""Container-only execution helper. Never import or execute this on the host.

Process environment markers survive ordinary daemonization. A member cleanup
scans all descendants bearing our exact generation/member/command identity.
This is cooperative process ownership, not isolation between malicious members.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import time


ROOT = Path(os.environ.get("XINGHUO_TOOL_ROOT", "/challenge")).resolve()
CONTROL_DIR = ROOT / "control"
MANIFEST_PATH = Path("/opt/xinghuo/tool-manifest.json")
TOOL_ENV_KEYS = {"JAVA_HOME", "GHIDRA_INSTALL_DIR", "KERAS_BACKEND", "GEM_PATH", "UV_PYTHON_INSTALL_DIR"}


def tool_manifest():
    manifest = json.loads(MANIFEST_PATH.read_text())
    interpreter = manifest.get("tool_python", "/opt/ctf/bin/python3")
    if not Path(interpreter).is_file():
        raise RuntimeError("Complete CTF tool interpreter is missing: " + interpreter)
    return manifest


def python_inventory(manifest):
    # Query the tool interpreter, never the SDK interpreter running this helper.
    code = """import importlib.util,importlib.metadata as m,json,sys,sysconfig
modules=json.loads(sys.argv[1]); packages={}
for distribution,module in modules.items():
    try:
        packages[distribution]={'version':m.version(distribution),'module':module,'available':importlib.util.find_spec(module) is not None}
    except (m.PackageNotFoundError,ImportError,ValueError):
        packages[distribution]={'version':None,'module':module,'available':False}
print(json.dumps({'version':sys.version.split()[0],'packages':packages,'site':sysconfig.get_path('purelib')}))
"""
    result = subprocess.run([manifest.get("tool_python", "/opt/ctf/bin/python3"), "-c", code,
                             json.dumps(manifest.get("python_modules", {}))],
                            check=True, capture_output=True, text=True, timeout=20)
    return json.loads(result.stdout)


def describe():
    manifest = tool_manifest()
    inventory = python_inventory(manifest)
    tool_python = manifest.get("tool_python", "/opt/ctf/bin/python3")
    search_path = str(Path(tool_python).parent) + ":" + os.environ.get("PATH", "")
    commands = manifest.get("commands", [])
    tools = {name: shutil.which(name, path=search_path) for name in commands}
    return {"architecture": platform.machine(), "python": inventory["version"],
            "tool_python": tool_python, "sdk_python": sys.version.split()[0],
            "tools": tools, "python_packages": [name for name, item in inventory["packages"].items() if item["available"]],
            "package_inventory": inventory["packages"],
            "missing_commands": [name for name, path in tools.items() if path is None],
            "missing_packages": [name for name, item in inventory["packages"].items() if not item["available"]],
            "tool_manifest": str(MANIFEST_PATH),
            "architecture_exclusions": manifest.get("architecture_exclusions", {}),
            # Finding an executable/module proves installation only. Functional
            # checks run through a member and produce a separate signed-off report.
            "functional_validation": {"status": "not_run_in_inventory",
                                      "report": "separate offline acceptance report"},
            "paths": {name: str(ROOT / name) for name in ("inputs", "shared", "members")}}


def dispatch_allowed(member, generation, command_id):
    """Fail closed when quiescing, reconciling, or retiring an old command.

    A global pause is reversible; command-specific retirement is permanent.
    Thus a stale pre-recovery SDK allow cannot run after cancel_stop reopens the
    global gate. Cleanup and environment inventory deliberately bypass this.
    """
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", command_id):
        return False
    try:
        gate = json.loads((CONTROL_DIR / "dispatch.json").read_text())
        permit = json.loads((CONTROL_DIR / ("dispatch-" + command_id + ".json")).read_text())
        return bool(gate.get("allowed") is True and gate.get("generation") == generation
                    and permit.get("allowed") is True and permit.get("generation") == generation
                    and permit.get("member") == member and permit.get("command_id") == command_id)
    except (OSError, ValueError, TypeError):
        return False


def member_path(member):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", member):
        raise ValueError("Invalid member")
    return ROOT / "members" / member


def prepare(member):
    base = member_path(member)
    for name in ("work", "cache", "home", "runtime"):
        (base / name).mkdir(parents=True, exist_ok=True)
    (base / "runtime").chmod(0o700)
    manifest = tool_manifest()
    tool_python = manifest.get("tool_python", "/opt/ctf/bin/python3")
    if not (base / "venv/bin/python").is_file():
        subprocess.run([tool_python, "-m", "venv", "--system-site-packages", str(base / "venv")], check=True)
    # venv-of-venv does NOT inherit the parent's site-packages. Refresh this even
    # for a recovered member whose venv predates the full tool image. A sibling's
    # custom packages still cannot overwrite the shared read-only tool install.
    tool_site = subprocess.check_output([tool_python, "-c",
        "import sysconfig; print(sysconfig.get_path('purelib'))"], text=True).strip()
    member_site = subprocess.check_output([str(base / "venv/bin/python"), "-c",
        "import sysconfig; print(sysconfig.get_path('purelib'))"], text=True).strip()
    Path(member_site, "xinghuo_ctf_base.pth").write_text(tool_site + "\n")
    return base


def member_environment(base, member, generation, command_id):
    env = {key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL", "TERM", "TZ") if key in os.environ}
    manifest = tool_manifest()
    for key in TOOL_ENV_KEYS:
        value = manifest.get("environment", {}).get(key, os.environ.get(key))
        if value is not None:
            env[key] = str(value)
    env.update({"XINGHUO_MEMBER": member, "XINGHUO_GENERATION": generation,
                "XINGHUO_COMMAND_ID": command_id, "VIRTUAL_ENV": str(base / "venv"),
                "PATH": str(base / "venv/bin") + ":" + str(Path(manifest.get("tool_python", "/opt/ctf/bin/python3")).parent) + ":" + os.environ.get("PATH", ""),
                "HOME": str(base / "home"), "XDG_CACHE_HOME": str(base / "cache"),
                "XDG_RUNTIME_DIR": str(base / "runtime"),
                "PIP_CACHE_DIR": str(base / "cache/pip"), "npm_config_cache": str(base / "cache/npm"),
                "UV_CACHE_DIR": str(base / "cache/uv"),
                "CARGO_HOME": str(base / "cache/cargo"), "GEM_HOME": str(base / "cache/gems")})
    env["PATH"] = str(base / "cache/gems/bin") + ":" + env["PATH"]
    return env


def tagged_pids(member, generation, command_id=None):
    expected = [f"XINGHUO_MEMBER={member}".encode(), f"XINGHUO_GENERATION={generation}".encode()]
    if command_id:
        expected.append(f"XINGHUO_COMMAND_ID={command_id}".encode())
    matches = []
    for path in Path("/proc").glob("[0-9]*/environ"):
        try:
            pid = int(path.parent.name)
            if pid != os.getpid() and all(item in path.read_bytes().split(b"\0") for item in expected):
                # PID birth ticks prevent killing a recycled PID during cleanup.
                stat = (path.parent / "stat").read_text().rsplit(")", 1)[1].split()
                matches.append((pid, stat[19]))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return matches


def kill_tagged(member, generation, command_id=None):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        matches = tagged_pids(member, generation, command_id)
        for pid, birth in matches:
            try:
                stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
                if stat[19] == birth:
                    os.kill(pid, sig)
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
        if sig == signal.SIGTERM:
            time.sleep(0.2)
    return len(tagged_pids(member, generation, command_id))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("describe")
    sub.add_parser("prepare").add_argument("member")
    for name in ("run", "cleanup"):
        child = sub.add_parser(name)
        child.add_argument("--member", required=True)
        child.add_argument("--generation", required=True)
        child.add_argument("--command-id", required=name == "run")
        if name == "run":
            child.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.action == "describe":
        print(json.dumps(describe()))
        return 0
    if args.action == "prepare":
        prepare(args.member)
        return 0
    if args.action == "cleanup":
        remaining = kill_tagged(args.member, args.generation, args.command_id)
        print(json.dumps({"remaining": remaining}))
        return 1 if remaining else 0
    if not dispatch_allowed(args.member, args.generation, args.command_id):
        print("Veria tool dispatch paused or retired; command was not started.", file=sys.stderr)
        return 75
    base = prepare(args.member)
    command = args.command[1:] if args.command and args.command[0] == "--" else args.command
    if len(command) != 1:
        raise ValueError("Exactly one shell command is required")
    if not dispatch_allowed(args.member, args.generation, args.command_id):
        print("Veria tool dispatch paused during preparation; command was not started.", file=sys.stderr)
        return 75
    # Hosted tools share the outer sandbox: never copy model/platform credentials
    # into command environments. This is not a hostile-code security boundary.
    env = member_environment(base, args.member, args.generation, args.command_id)
    if not dispatch_allowed(args.member, args.generation, args.command_id):
        print("Veria tool dispatch paused during preparation; command was not started.", file=sys.stderr)
        return 75
    process = subprocess.Popen(["bash", "--noprofile", "--norc", "-c", command[0]], cwd=base / "work", env=env, start_new_session=True)
    record = CONTROL_DIR / (args.command_id + ".json")
    record.write_text(json.dumps({"pid": process.pid, "member": args.member, "generation": args.generation, "command_id": args.command_id, "started_at": time.time()}))
    def terminate(signum, _frame):
        kill_tagged(args.member, args.generation, args.command_id)
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
