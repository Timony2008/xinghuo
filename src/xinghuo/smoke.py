"""Small real SDK acceptance run; deliberately has no benchmark client."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path

from .runtime import TeamProcess
from .store import atomic_json


def report_payload(snapshot):
    return snapshot.get("report") or {key: snapshot.get(key) for key in
        ("overview", "recent_changes", "short_history", "next_steps", "evidence_refs", "report_version", "through_event")}


async def sdk_smoke(run_dir: Path, *, model=None, timeout=300, backend="claude"):
    run_dir = Path(run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    nonce = uuid.uuid4().hex
    attempt = "smoke-" + nonce
    result = {"backend": backend, "nonce": nonce, "passed": False, "stages": [], "started_at": time.time()}
    settings = {"role_turn_timeout": min(timeout, 120), "shutdown_timeout": 15,
                "max_agents_per_team": 6, "heartbeat_interval": 1}
    context = {"mode": "smoke", "nonce": nonce,
        "challenge": {"code": "sdk-smoke", "description": "Local messaging acceptance; no target or network actions.",
                      "flag_count": 0, "correct_flag_count": 0, "completed": False},
        "addresses": [], "platform": {},
        "leader_prompt": f"""这是通信验收，不是解题。你的本轮工作只有：
1. 用spawn_worker派发且仅派发两个独立worker，request_key分别为smoke-A、smoke-B。
2. worker A必须将原文标记 {nonce}:evidence-A 同时用report_progress和send_message发送给reporter；worker B同理标记 {nonce}:evidence-B。
3. 工人任务必须明确要求处理后ack收到的消息；不要联网、不要调用submit_flag、无需创建文件。
4. 派完立即结束本轮，不等待工人。reporter会独立收到消息并publish_report。
5. 后续收到correction消息时，把完整correction原文send_message给reporter，要求保留原证据并发布包含更正的新版本；ack原消息。
6. 如果已有任务，不重复派发。"""}
    team = None
    snapshots = []
    async def wait_until(predicate):
        while True:
            snapshot = await team.request("snapshot", timeout=15)
            if snapshot.get("error"):
                raise RuntimeError(snapshot["error"])
            if predicate(snapshot):
                return snapshot
            await asyncio.sleep(.4)
    try:
        async with asyncio.timeout(timeout):
            team = TeamProcess(run_dir, "sdk-smoke", attempt, backend=backend, model=model, settings=settings)
            await team.start(context)
            def complete(snapshot):
                workers = [a for a in snapshot.get("agents", []) if a.get("role") == "worker" and a.get("session_id")]
                report = json.dumps(report_payload(snapshot), ensure_ascii=False)
                if backend == "demo":
                    markers = ["worker-1", "worker-2", nonce]
                else:
                    markers = [nonce + ":evidence-A", nonce + ":evidence-B"]
                processed = [m for m in snapshot.get("messages", []) if m.get("recipient") == "reporter"
                             and m.get("sender") in {a["agent_id"] for a in workers}
                             and m.get("state") == "acknowledged"]
                core = [a for a in snapshot.get("agents", []) if a.get("role") in {"leader", "reporter"}]
                return (len(core) == 2 and all(a.get("session_id") for a in core)
                        and len(workers) >= 2 and len(processed) >= 2 and all(mark in report for mark in markers))
            initial = await wait_until(complete)
            snapshots.append(initial)
            result["stages"].append("leader_spawned_two_workers_reporter_processed_evidence")
            atomic_json(run_dir / "initial-report.json", report_payload(initial))
            original_sessions = {a["agent_id"]: a.get("session_id") for a in initial["agents"] if a.get("session_id")}
            await team.stop()
            result["stages"].append("first_runtime_stopped")
            team = TeamProcess(run_dir, "sdk-smoke", attempt, backend=backend, model=model, settings=settings)
            await team.start(context)
            correction = f"correction: {nonce}:corrected 原证据应保留，增加这条更正并发布新版报告。"
            await team.request("message", {"recipient": "leader", "body": correction})
            corrected = await wait_until(lambda snapshot:
                nonce + ":corrected" in json.dumps(report_payload(snapshot), ensure_ascii=False)
                and snapshot.get("report_version", 0) > initial.get("report_version", 0)
                and all(a.get("session_id") for a in snapshot.get("agents", []) if a.get("role") in {"leader", "reporter"}))
            snapshots.append(corrected)
            actual_sessions = {a["agent_id"]: a.get("session_id") for a in corrected["agents"]}
            if any(actual_sessions.get(key) != value for key, value in original_sessions.items()):
                raise RuntimeError("session identities changed across same-attempt resume")
            result["stages"].extend(["same_sessions_resumed", "leader_correction_published_in_new_report"])
            result.update(passed=True, session_ids=actual_sessions,
                          report_versions=[s["report_version"] for s in snapshots])
            atomic_json(run_dir / "corrected-report.json", report_payload(corrected))
    except Exception as exc:
        result.update(error_type=type(exc).__name__, error=str(exc)[:1200])
    finally:
        if team:
            await team.stop()
        result["finished_at"] = time.time()
        result["elapsed_seconds"] = result["finished_at"] - result["started_at"]
        atomic_json(run_dir / "result.json", result)
    return result
