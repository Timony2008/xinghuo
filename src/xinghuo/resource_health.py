"""Read real Linux cgroup limits and changes, without guessing OOM victims.

The outer hosted cgroup is shared by all teams. A counter increment proves an
OOM happened in that cgroup, not which SDK process was killed. Runtime process
exit events and member identities provide the separate attribution evidence.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import time


def host_capacity(meminfo: Path = Path("/proc/meminfo")) -> dict:
    total = None
    try:
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                total = int(line.split()[1]) * 1024
                break
    except (OSError, ValueError):
        pass
    memory = cgroup_memory()
    limit = memory.get("limit_bytes")
    return {"host_memory_bytes": total, "host_cpus": os.cpu_count(), "cgroup": memory,
            "nominal_limit_exceeds_host": bool(total is not None and limit is not None and limit > total),
            "effective_memory_upper_bound_bytes": min(value for value in (total, limit) if value is not None)
                if total is not None or limit is not None else None}


def _number(path: Path):
    try:
        value = path.read_text().strip()
        return None if value == "max" else int(value)
    except (OSError, ValueError):
        return None


def cgroup_memory(root: Path = Path("/sys/fs/cgroup")) -> dict:
    """Support cgroup v2 and old v1 mounts; missing metrics stay unknown."""
    root = Path(root)
    if (root / "memory.current").exists():
        events = {}
        try:
            events = {key: int(value) for key, value in
                      (line.split() for line in (root / "memory.events").read_text().splitlines())}
        except (OSError, ValueError):
            pass
        return {"available": True, "version": 2,
                "current_bytes": _number(root / "memory.current"),
                "peak_bytes": _number(root / "memory.peak"),
                "limit_bytes": _number(root / "memory.max"), "events": events}
    legacy = root / "memory" if (root / "memory").is_dir() else root
    if (legacy / "memory.usage_in_bytes").exists():
        limit = _number(legacy / "memory.limit_in_bytes")
        return {"available": True, "version": 1,
                "current_bytes": _number(legacy / "memory.usage_in_bytes"),
                "peak_bytes": _number(legacy / "memory.max_usage_in_bytes"),
                "limit_bytes": limit if limit is not None and limit < 2**60 else None,
                "events": {"limit_failures": _number(legacy / "memory.failcnt")},
                "oom_kill_counter_available": False}
    return {"available": False, "version": None, "current_bytes": None,
            "peak_bytes": None, "limit_bytes": None, "events": {}}


def process_memory(pid: int, proc_root: Path = Path("/proc")) -> dict:
    """Only numeric identity/RSS; never read environment or command arguments."""
    result = {"pid": int(pid), "exists": False, "rss_bytes": None}
    try:
        fields = dict(line.split(":", 1) for line in
                      (Path(proc_root) / str(pid) / "status").read_text().splitlines() if ":" in line)
        result.update(exists=True, rss_bytes=int(fields["VmRSS"].split()[0]) * 1024
                      if "VmRSS" in fields else None)
    except (OSError, ValueError):
        pass
    return result


class ResourceMonitor:
    def __init__(self, root: Path = Path("/sys/fs/cgroup"), state_path: Path | None = None):
        self.root, self.state_path = Path(root), state_path
        self.previous = None
        if state_path:
            try:
                self.previous = json.loads(state_path.read_text())
            except (OSError, ValueError):
                pass

    def snapshot(self) -> dict:
        result = {"sampled_at": time.time(), **cgroup_memory(self.root)}
        old = (self.previous or {}).get("events", {})
        events = result["events"]
        result["event_deltas"] = {key: max(0, value - old[key])
            for key, value in events.items()
            if isinstance(value, int) and isinstance(old.get(key), int)}
        result["oom_kill_delta"] = result["event_deltas"].get("oom_kill")
        result["baseline_sample"] = self.previous is None
        result["attribution"] = "shared_cgroup; correlate with member process_exit events"
        self.previous = result
        if self.state_path:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.state_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(result, sort_keys=True) + "\n")
            temporary.replace(self.state_path)
        return result
