"""One challenge's durable agent room, isolated from the host supervisor."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import signal
import sys
import time
import uuid

from .room import Room
from .sdk_driver import AgentProcess


@dataclass
class RoleSlot:
    agent_id: str
    role: str
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    driver: AgentProcess | None = None
    task: asyncio.Task | None = None
    running: bool = False
    seen_through: int = 0
    event_cursor: int = 0
    turn_id: str = ""
    recovering: bool = False
    backlog_pending: bool = False


class TeamRuntime:
    def __init__(self, run_dir: Path, code: str, attempt_id: str, backend: str,
                 model: str | None = None, settings: dict | None = None):
        self.directory = Path(run_dir).resolve() / "teams" / code
        self.room = Room(self.directory, code, attempt_id)
        self.code, self.attempt_id, self.backend, self.model = code, attempt_id, backend, model
        self.settings = settings or {}
        self.max_agents = int(self.settings.get("max_agents_per_team", 12))
        if self.max_agents < 3:
            raise ValueError("max_agents_per_team must leave room for leader, reporter and reviewer (minimum 3)")
        self.slots: dict[str, RoleSlot] = {}
        self.stopping = asyncio.Event()
        self.started = False
        self.preparing_stop = False
        self._output_lock = asyncio.Lock()
        self._background: list[asyncio.Task] = []
        self._input_transport = None
        self._request_tasks: set[asyncio.Task] = set()

    async def emit(self, event_type: str, **data) -> None:
        row = {"type": event_type, "code": self.code, "attempt_id": self.attempt_id, **data}
        async with self._output_lock:
            sys.stdout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            sys.stdout.flush()

    async def response(self, request_id: str, result: dict) -> None:
        await self.emit("response", id=request_id, result=result)

    async def start(self, context: dict) -> dict:
        if self.started:
            return {"started": True, "pid": os.getpid(), "snapshot": self.room.snapshot()}
        previous_context = self.room.meta("context", {})
        self.room.set_meta("context", {**previous_context, **context})
        if self.room.archived_attempt:
            context_with_archive = self.room.meta("context", {})
            context_with_archive["previous_attempt_archive"] = self.room.archived_attempt
            self.room.set_meta("context", context_with_archive)
        await self._cleanup_stale_roles()
        self.room.set_meta("runtime_pid", os.getpid())
        self.started = True
        self.preparing_stop = False
        for agent_id, role in (("leader", "leader"), ("reporter", "reporter")):
            if not self.room.agent(agent_id):
                self.room.add_agent(agent_id, role)
        pending_review = self.room.meta("review_request")
        if pending_review and (self.room.meta("last_review") or {}).get("review_id") != pending_review["review_id"]:
            self._queue_review_request(pending_review)
        for agent in self.room.agents():
            if int(agent["restarts"]) <= 2:
                task = self.room.task(agent["agent_id"])
                if agent["role"] == "worker" and agent["status"] == "stopped" and task and task["status"] == "completed":
                    self.room.update_agent(agent["agent_id"], status="idle")
                self._launch_slot(agent["agent_id"], agent["role"])
        for agent_id in ("leader", "reporter"):
            if agent_id in self.slots:
                self.slots[agent_id].wake.set()
        # Interrupted workers and queued messages survive a runtime restart.
        for agent_id, slot in self.slots.items():
            record = self.room.agent(agent_id) or {}
            task = self.room.task(agent_id)
            if (record.get("status") in {"running", "queued", "recovering"}
                    or (task and task.get("status") in {"queued", "running"})
                    or self.room.messages(agent_id, pending=True)):
                slot.wake.set()
        self.room.event("runtime_started", {"pid": os.getpid(), "backend": self.backend})
        self.room.publish_snapshot()
        self._background.append(asyncio.create_task(self._heartbeat(), name="team-heartbeat"))
        for candidate in self.room.meta("candidates", {}).values():
            if candidate.get("status") == "pending":
                await self.emit("candidate_flag", **{k: v for k, v in candidate.items() if k != "status"})
        return {"started": True, "pid": os.getpid(), "snapshot": self.room.snapshot()}

    async def _cleanup_stale_roles(self) -> None:
        # A killed runtime can leave role process groups behind. Confirm command
        # identity before signalling a persisted PID, guarding against PID reuse.
        for agent in [*self.room.stale_agents, *self.room.agents()]:
            pid = agent.get("pid")
            if not pid or pid == os.getpid():
                continue
            try:
                process = await asyncio.create_subprocess_exec("ps", "-p", str(pid), "-o", "command=",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
                output, _ = await process.communicate()
                command = output.decode(errors="replace")
                if ("xinghuo.agent_runner" in command and f"--agent-id {agent['agent_id']}" in command
                        and str(self.directory) in command and os.getpgid(pid) == pid):
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(pid, signal.SIGTERM)
                    await asyncio.sleep(.05)
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(pid, signal.SIGKILL)
            except (ProcessLookupError, FileNotFoundError):
                pass
            self.room.update_agent(agent["agent_id"], pid=None)

    def _launch_slot(self, agent_id: str, role: str) -> RoleSlot:
        if agent_id in self.slots:
            return self.slots[agent_id]
        slot = RoleSlot(agent_id, role)
        self.slots[agent_id] = slot
        slot.task = asyncio.create_task(self._agent_loop(slot), name=f"role-{agent_id}")
        return slot

    async def _new_driver(self, slot: RoleSlot) -> AgentProcess:
        agent = self.room.agent(slot.agent_id) or {}
        agent_cwd = self.directory / "agents" / slot.agent_id
        agent_cwd.mkdir(parents=True, exist_ok=True)

        async def on_tool(name: str, arguments: dict) -> dict:
            return await self.tool(slot, name, arguments)

        driver = AgentProcess(slot.role, slot.agent_id, agent_cwd, backend=self.backend,
                              model=self.model, session_id=agent.get("session_id"), on_tool=on_tool)
        await driver.start()
        slot.driver = driver
        self.room.update_agent(slot.agent_id, pid=driver.pid, status="idle")
        self.room.event("agent_process_started", {"agent_id": slot.agent_id, "pid": driver.pid}, slot.agent_id)
        return driver

    def _prompt(self, slot: RoleSlot) -> str:
        agent = self.room.agent(slot.agent_id) or {}
        context = self.room.meta("context", {})
        task = self.room.task(slot.agent_id)
        task_body = task["body"] if task else ""
        task_context = {}
        if task_body:
            with contextlib.suppress(ValueError):
                parsed = json.loads(task_body)
                if isinstance(parsed, dict):
                    task_context = parsed
        events, last_seen = self._event_batch(slot.event_cursor)
        slot.backlog_pending = last_seen < self.room.last_event
        slot.seen_through = max(slot.seen_through, last_seen)
        messages = [message for message in self.room.messages(slot.agent_id, pending=True, limit=20)
                    if message["event_seq"] <= slot.seen_through]
        slot.turn_id = uuid.uuid4().hex
        self.room.inject(slot.agent_id, [message["id"] for message in messages], slot.turn_id)
        snapshot = self._compact_snapshot()
        role_instructions = {
            "leader": "负责本题组织：异步派发独立 worker，保持任务有界；进展与更正直接告诉 reporter。根据证据推进，不等待一个成员无限重试。",
            "worker": "完成分配任务，及时提交候选 flag，附证据向 reporter 汇报；收到消息处理后明确 ack。",
            "reporter": "你是报告唯一作者。get_report 查阅事件；处理消息后 ack_message，再 publish_report。水位不得超过实际收到并处理的事件。保留历史及证据，明确未知。",
            "reviewer": "先get_report审核当前报告；首先逐个实际复现所有已知flag，再逐项复查横向、扫描覆盖、遗漏漏洞、未尝试路线及旧否定结论，检查成员健康。每项记录证据与失败/阻塞/未检查原因；完整记录写入review_context.output_path，以report_progress(kind='review')回报并通知leader。",
        }
        payload = {"role": slot.role, "agent_id": slot.agent_id, "initial": agent.get("turns", 0) == 0,
                   "initial_context": context, "context": {**context, **task_context, "code": self.code,
                    "smoke": context.get("mode") == "smoke", "overview": snapshot["overview"]},
                   "task": task_body, "messages": messages, "events": events,
                   "through_event": slot.seen_through, "report": snapshot,
                   "instructions": role_instructions[slot.role]}
        payload["task_completed"] = bool(task and task.get("status") == "completed")
        payload["context"]["task_completed"] = payload["task_completed"]
        if slot.role == "reviewer":
            payload["review_context"] = self._review_context()
        if payload["task_completed"] and slot.role == "worker":
            payload["instructions"] += "\n原任务已经完成。本轮只处理新消息和确认平台结果；不要重复原任务、重新提交旧flag或重新声称同一发现。只有明确的新任务才需要新实验。"
        if slot.role == "leader" and context.get("leader_prompt"):
            payload["instructions"] += "\n" + str(context["leader_prompt"])
        policy = self._policy(slot.role)
        if policy:
            payload["policy"] = policy
        payload["event_backlog_pending"] = slot.backlog_pending
        return json.dumps(payload, ensure_ascii=False, default=str)

    def _review_context(self) -> dict:
        context = self.room.meta("context", {})
        report = self.room.latest_report()
        request = self.room.meta("review_request", {})
        return {**request,
                "report_path": str(self.directory / "report.md"),
                "report_version": report["report_version"], "through_event": report["through_event"],
                "platform": context.get("platform", {}), "challenge": context.get("challenge", {}),
                "confirmed_submissions": self.room.confirmed_submissions(),
                "evidence_refs": report["evidence_refs"],
                "previous_review": request.get("previous_review", self.room.meta("last_review"))}

    def _queue_review_request(self, request: dict) -> dict:
        Path(request["output_path"]).parent.mkdir(parents=True, exist_ok=True)
        if not self.room.agent("reviewer"):
            self.room.add_agent("reviewer", "reviewer", "每轮先读报告，逐个复现全部已知flag，再复查横向、扫描覆盖、遗漏漏洞、未尝试路线和旧否定结论；写完整证据记录并提出具体纠偏。")
        return self.room.send("host", "reviewer", json.dumps(request, ensure_ascii=False),
                              message_id="review-request:" + request["review_id"])

    def _policy(self, role: str) -> str:
        directories = []
        if self.settings.get("policy_dir"):
            directories.append(Path(self.settings["policy_dir"]))
        directories.extend([self.directory.parents[1] / "policies", Path.cwd() / "policies",
                            Path(__file__).parent / "policies", Path(__file__).parents[2] / "policies"])
        for directory in directories:
            path = directory / f"{role}.md"
            if path.is_file():
                return path.read_text(encoding="utf-8")
        return ""

    def _event_batch(self, after: int) -> tuple[list[dict], int]:
        raw = self.room.events(after=after, limit=100)
        relevant = {"progress", "submission_result", "candidate_flag", "worker_failed", "task_completed", "task_assigned",
                    "runtime_started", "agent_created", "message_queued", "messages_acknowledged",
                    "worker_suspected_loop", "suspected_loop_cleared"}
        return ([event for event in raw if event["type"] in relevant], raw[-1]["seq"] if raw else after)

    def _compact_snapshot(self) -> dict:
        full = self.room.snapshot()
        return {"code": self.code, "attempt_id": self.attempt_id, "overview": full["overview"],
                "recent_changes": full["recent_changes"][-12:], "short_history": full["short_history"][-6:],
                "next_steps": full["next_steps"][-10:], "evidence_refs": full["evidence_refs"][-40:],
                "report_version": full["report_version"], "through_event": full["through_event"],
                "last_event": full["last_event"], "platform": full["platform"],
                "agents": [{key: agent.get(key) for key in ("agent_id", "role", "status", "restarts", "task_id")}
                           for agent in full["agents"]],
                "report_path": str(self.directory / "report.md"), "full_snapshot_path": str(self.directory / "snapshot.json"),
                "tool_health": full["tool_health"],
                "summary_limits": {"recent_changes": 12, "short_history": 6, "evidence_refs": 40}}

    async def _agent_loop(self, slot: RoleSlot) -> None:
        try:
            while not self.stopping.is_set():
                await slot.wake.wait()
                slot.wake.clear()
                if self.stopping.is_set():
                    break
                if slot.recovering:
                    await asyncio.sleep(.05)
                    slot.wake.set()
                    continue
                record = self.room.agent(slot.agent_id) or {}
                if int(record.get("restarts", 0)) > 2:
                    continue
                try:
                    slot.running = True
                    driver = slot.driver
                    if driver is not None and not driver.alive:
                        raise RuntimeError("role process exited before queued turn")
                    if driver is None:
                        driver = await self._new_driver(slot)
                    prompt = self._prompt(slot)
                    original_task = self.room.task(slot.agent_id)
                    work_turn = bool(original_task and original_task.get("status") != "completed")
                    self.room.update_agent(slot.agent_id, status="running")
                    if work_turn:
                        self.room.task_status(slot.agent_id, "running")
                    result = await asyncio.wait_for(driver.turn(prompt), timeout=float(self.settings.get("role_turn_timeout", 180)))
                    if result.get("is_error"):
                        raise RuntimeError(str(result.get("error") or result.get("result") or "role turn failed"))
                    slot.running = False
                    self.room.update_agent(slot.agent_id, status="idle", session_id=driver.session_id,
                                           turns=int(record.get("turns", 0)) + 1, last_error=None)
                    if work_turn and slot.role == "worker":
                        completion = self.room.complete_worker_task(slot.agent_id, str(result.get("result", "")))
                        if completion and "leader" in self.slots:
                            self.slots["leader"].wake.set()
                        if completion and "reporter" in self.slots:
                            self.slots["reporter"].wake.set()
                    elif work_turn:
                        self.room.task_status(slot.agent_id, "completed")
                    self.room.event("agent_turn_completed", {"agent_id": slot.agent_id, "result": str(result.get("result", ""))[:4000]}, slot.agent_id)
                    slot.event_cursor = slot.seen_through
                    if slot.backlog_pending or any(message["state"] == "queued" for message in self.room.messages(slot.agent_id, pending=True, limit=10000)):
                        slot.wake.set()
                    self.room.publish_snapshot()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    slot.running = False
                    await self._failure(slot, exc)
        finally:
            slot.running = False
            if slot.driver:
                if slot.driver.session_id:
                    self.room.update_agent(slot.agent_id, session_id=slot.driver.session_id)
                with contextlib.suppress(Exception):
                    await slot.driver.close()
                slot.driver = None
            self.room.update_agent(slot.agent_id, pid=None, status="stopped")

    async def _failure(self, slot: RoleSlot, error: BaseException) -> None:
        if slot.recovering:
            return
        slot.recovering = True
        record = self.room.agent(slot.agent_id) or {}
        restarts = int(record.get("restarts", 0)) + 1
        if slot.driver:
            if slot.driver.session_id:
                self.room.update_agent(slot.agent_id, session_id=slot.driver.session_id)
            with contextlib.suppress(Exception):
                await slot.driver.close()
            slot.driver = None
        self.room.update_agent(slot.agent_id, restarts=restarts, status="recovering" if restarts <= 2 else "failed",
                               pid=None, last_error=f"{type(error).__name__}: {error}")
        data = {"agent_id": slot.agent_id, "role": slot.role, "error": f"{type(error).__name__}: {error}",
                "restarts": restarts, "will_retry": restarts <= 2}
        self.room.event("worker_failed", data, slot.agent_id)
        await self.emit("worker_failed", **data)
        if slot.agent_id != "leader" and "leader" in self.slots:
            self.room.send("runtime", "leader", json.dumps(data, ensure_ascii=False))
            self.slots["leader"].wake.set()
        if restarts <= 2 and not self.stopping.is_set():
            await asyncio.sleep(min(.1 * restarts, 1))
            slot.wake.set()
        self.room.publish_snapshot()
        slot.recovering = False

    async def _heartbeat(self) -> None:
        interval = max(.05, float(self.settings.get("heartbeat_interval", 1)))
        while not self.stopping.is_set():
            for slot in list(self.slots.values()):
                await self._observe_tools(slot)
                if slot.driver and slot.driver.session_id:
                    record = self.room.agent(slot.agent_id) or {}
                    if record.get("session_id") != slot.driver.session_id:
                        self.room.update_agent(slot.agent_id, session_id=slot.driver.session_id)
                if slot.driver and not slot.running and not slot.driver.alive:
                    await self._failure(slot, RuntimeError("idle role process exited"))
            snapshot = self.room.publish_snapshot()
            await self.emit("heartbeat", pid=os.getpid(), last_event=snapshot["last_event"],
                            report_version=snapshot["report_version"], agents=snapshot["agents"], time=time.time())
            try:
                await asyncio.wait_for(self.stopping.wait(), interval)
            except TimeoutError:
                pass

    async def _observe_tools(self, slot: RoleSlot) -> None:
        poll = getattr(slot.driver, "poll_observations", None)
        if not poll or slot.role == "reporter":
            return
        observations = poll()
        patterns = self.room.meta("tool_patterns", {})
        pattern = patterns.get(slot.agent_id, {})
        recent = self.room.meta(f"tool_observations:{slot.agent_id}", [])
        for observation in observations:
            if observation.get("phase", "completed") != "completed":
                continue
            name = observation.get("tool_name") or observation.get("name")
            if not name or str(name).startswith("mcp__"):
                continue
            summary = {key: observation.get(key) for key in ("tool_use_id", "input_fingerprint", "result_fingerprint", "is_error")}
            summary["tool_name"] = name
            self.room.event("tool_observation", summary, slot.agent_id)
            recent.append(summary)
            key = [name, summary["input_fingerprint"], summary["result_fingerprint"], summary["is_error"]]
            if pattern.get("key") == key:
                pattern["count"] = int(pattern.get("count", 0)) + 1
            else:
                if pattern.get("suspected"):
                    self.room.event("suspected_loop_cleared", {"agent_id": slot.agent_id}, slot.agent_id)
                pattern = {"key": key, "count": 1, "suspected": False}
            if pattern["count"] >= 3 and not pattern.get("suspected"):
                pattern["suspected"] = True
                data = {"agent_id": slot.agent_id, "role": slot.role, "tool_name": name,
                        "repeat_count": pattern["count"], "input_fingerprint": summary["input_fingerprint"],
                        "result_fingerprint": summary["result_fingerprint"], "is_error": summary["is_error"]}
                self.room.event("worker_suspected_loop", data, slot.agent_id)
                await self.emit("worker_suspected_loop", **data)
                body = "疑似重复循环（不是已证实死循环）：" + json.dumps(data, ensure_ascii=False) + "。请核查前提和路线，勿直接关闭靶场。"
                for recipient in ("leader", "reporter"):
                    if recipient in self.slots:
                        self.room.send("runtime", recipient, body)
                        self.slots[recipient].wake.set()
                if not self.preparing_stop:
                    await self.command("review", {"reason": body})
        if observations:
            patterns[slot.agent_id] = pattern
            self.room.set_meta("tool_patterns", patterns)
            self.room.set_meta(f"tool_observations:{slot.agent_id}", recent[-50:])

    async def tool(self, slot: RoleSlot, name: str, arguments: dict) -> dict:
        # SDK sessions are established before their first MCP request. Persist
        # that ID before committing effects, rather than waiting for the final
        # ResultMessage, which may arrive much later or be interrupted by stop.
        if slot.driver and slot.driver.session_id:
            record = self.room.agent(slot.agent_id) or {}
            if record.get("session_id") != slot.driver.session_id:
                self.room.update_agent(slot.agent_id, session_id=slot.driver.session_id)
        self.room.event("tool_called", {"name": name, "arguments": arguments, "agent_id": slot.agent_id}, slot.agent_id)
        if name == "spawn_worker":
            if slot.role != "leader":
                raise ValueError("only the team leader can create workers")
            if self.preparing_stop:
                raise ValueError("team is preparing to stop")
            task = str(arguments.get("task", "")).strip()
            if not task:
                raise ValueError("worker task is required")
            request_key = arguments.get("request_key")
            keys = self.room.meta("spawn_keys", {})
            key = f"{slot.agent_id}:{request_key}" if request_key else None
            if key and key in keys:
                saved = keys[key]
                agent_id = saved["agent_id"] if isinstance(saved, dict) else saved
                return {"agent_id": agent_id, "worker_id": agent_id, "task_id": saved.get("task_id") if isinstance(saved, dict) else None, "duplicate": True}
            worker_id = arguments.get("worker_id")
            if worker_id:
                agent_id = str(worker_id)
                worker = self.slots.get(agent_id)
                if not worker or worker.role != "worker" or worker.running or worker.recovering:
                    raise ValueError("new tasks require an idle managed worker")
                assignment = self.room.assign_task(agent_id, task)
                if key:
                    keys[key] = {"agent_id": agent_id, "task_id": assignment["task_id"]}
                    self.room.set_meta("spawn_keys", keys)
                worker.wake.set()
                return {"agent_id": agent_id, "worker_id": agent_id, "task_id": assignment["task_id"], "status": "queued", "reused": True}
            reserved_reviewer = 0 if "reviewer" in self.slots else 1
            if len(self.slots) >= self.max_agents - reserved_reviewer:
                raise ValueError("team machine capacity reached; finish or reuse an existing worker")
            agent_id = "worker-" + uuid.uuid4().hex[:10]
            self.room.add_agent(agent_id, "worker", task)
            worker = self._launch_slot(agent_id, "worker")
            assignment = self.room.task(agent_id) or {}
            if key:
                keys[key] = {"agent_id": agent_id, "task_id": assignment.get("task_id")}
                self.room.set_meta("spawn_keys", keys)
            worker.wake.set()
            return {"agent_id": agent_id, "worker_id": agent_id, "task_id": assignment.get("task_id"), "status": "queued"}
        if name == "send_message":
            recipient = str(arguments.get("recipient", ""))
            message = self.room.send(slot.agent_id, recipient, str(arguments.get("body", "")),
                evidence_refs=arguments.get("evidence_refs"), reply_to=arguments.get("reply_to"),
                message_id=arguments.get("message_id"))
            if recipient in self.slots:
                self.slots[recipient].wake.set()
            return {"message_id": message["id"], "id": message["id"], "state": message["state"]}
        if name in {"ack_message", "ack_messages"}:
            ids = arguments.get("message_ids") if name == "ack_messages" else [arguments.get("message_id")]
            acknowledged = self.room.acknowledge(slot.agent_id, [str(message_id) for message_id in ids or []])
            if slot.role == "reporter" and acknowledged and self.room.latest_report()["through_event"] < slot.seen_through:
                slot.wake.set()
            return {"acknowledged": acknowledged}
        if name == "report_progress":
            summary = str(arguments.get("summary", "")).strip()
            if not summary:
                raise ValueError("progress summary is required")
            if len(summary) > 8000:
                raise ValueError("progress summary is too long; link the full evidence file")
            kind = str(arguments.get("kind", "finding"))
            data = {"summary": summary, "kind": kind, "evidence_refs": arguments.get("evidence_refs", []), "agent_id": slot.agent_id}
            if kind == "review" and slot.role == "reviewer":
                data["review_id"] = self.room.meta("review_request", {}).get("review_id")
            seq = self.room.event("progress", data, slot.agent_id)
            if slot.agent_id != "reporter":
                self.room.send(slot.agent_id, "reporter", summary, evidence_refs=data["evidence_refs"])
                self.slots["reporter"].wake.set()
            if kind == "review" and slot.role == "reviewer":
                self.room.set_meta("last_review", {**data, "event_seq": seq, "completed_at": time.time()})
                await self.emit("review_completed", **data, event_seq=seq)
            elif kind in {"finding", "correction"}:
                await self.emit("significant_evidence", **data, event_seq=seq)
            return {"recorded": True, "event_seq": seq}
        if name == "get_report":
            events, last_seen = self._event_batch(slot.event_cursor)
            slot.seen_through = max(slot.seen_through, last_seen)
            slot.backlog_pending = last_seen < self.room.last_event
            messages = [message for message in self.room.messages(slot.agent_id, pending=True, limit=20)
                        if message["event_seq"] <= slot.seen_through]
            self.room.inject(slot.agent_id, [message["id"] for message in messages], slot.turn_id)
            result = {"report": self._compact_snapshot(), "events": events, "messages": messages,
                      "through_event": slot.seen_through, "event_backlog_pending": slot.backlog_pending}
            if slot.role == "reviewer":
                result["review_context"] = self._review_context()
            return result
        if name == "publish_report":
            if slot.role != "reporter":
                raise ValueError("only reporter can publish report")
            report = self.room.publish_report(arguments, seen_through=slot.seen_through)
            await self.emit("report_published", report_version=report["report_version"],
                            through_event=report["through_event"], path=str(self.directory / "report.md"))
            return {"published": True, "report_version": report["report_version"], "through_event": report["through_event"]}
        if name == "submit_flag":
            flag = str(arguments.get("flag", ""))
            if not 1 <= len(flag) <= 4096:
                raise ValueError("flag length must be between 1 and 4096")
            candidates = self.room.meta("candidates", {})
            for candidate in candidates.values():
                if candidate["flag"] == flag:
                    return {"request_id": candidate["request_id"], "status": candidate["status"], "duplicate_candidate": True}
            request_id = "flag-" + uuid.uuid4().hex
            candidate = {"request_id": request_id, "flag": flag, "sender": slot.agent_id, "status": "pending"}
            candidates[request_id] = candidate
            self.room.set_meta("candidates", candidates)
            seq = self.room.event("candidate_flag", candidate, slot.agent_id)
            await self.emit("candidate_flag", flag=flag, request_id=request_id, sender=slot.agent_id, event_seq=seq)
            return {"request_id": request_id, "status": "pending"}
        raise ValueError(f"unknown team tool: {name}")

    async def command(self, command: str, payload: dict) -> dict:
        if command == "start":
            return await self.start(payload)
        if not self.started:
            raise RuntimeError("start the team before issuing commands")
        if command in {"snapshot", "audit_snapshot", "request_report"}:
            if payload.get("platform") is not None:
                context = self.room.meta("context", {})
                context["platform"] = payload["platform"]
                self.room.set_meta("context", context)
            if command == "request_report":
                status = self.room.request_audit_report(str(payload["request_id"]), int(payload["round_id"]))
                if not status["ready"]:
                    self.slots["leader"].wake.set()
                return status
            snapshot = self.room.audit_snapshot() if command == "audit_snapshot" else self.room.publish_snapshot()
            if command == "audit_snapshot" and payload.get("request_id"):
                snapshot["report_request"] = self.room.audit_report_status(str(payload["request_id"]))
                if not snapshot["report_request"]["ready"]:
                    snapshot["report_unknown"] = True
            return snapshot
        if command == "report_status":
            return self.room.audit_report_status(str(payload["request_id"]))
        if command == "health":
            agents = []
            for record in self.room.agents():
                slot = self.slots.get(record["agent_id"])
                process_alive = bool(slot and slot.driver and slot.driver.alive)
                agents.append({key: record.get(key) for key in ("agent_id", "role", "status", "restarts")}
                              | {"process_alive": process_alive})
            core_failures = [agent["agent_id"] for agent in agents if agent["role"] in {"leader", "reporter"}
                and (agent["status"] in {"failed", "recovering", "stopped"}
                     or (agent["status"] in {"idle", "running"} and not agent["process_alive"]))]
            return {"healthy": not core_failures and not self.stopping.is_set(), "runtime_pid": os.getpid(),
                    "responsive": True, "core_failures": core_failures, "agents": agents}
        if command == "message":
            recipient = str(payload.get("recipient", "leader"))
            message = self.room.send("host", recipient, str(payload.get("body", "")),
                evidence_refs=payload.get("evidence_refs"), reply_to=payload.get("reply_to"),
                message_id=payload.get("message_id"))
            if recipient in self.slots:
                self.slots[recipient].wake.set()
            return {"message_id": message["id"], "state": message["state"]}
        if command == "review":
            reviewer = self.slots.get("reviewer")
            if reviewer and (reviewer.running or reviewer.wake.is_set()):
                return {"started": False, "already_running": True}
            review_id = "review-" + uuid.uuid4().hex
            request = {"review_id": review_id,
                       "reason": str(payload.get("reason") or "30分钟没有新的平台确认flag，请独立复核。"),
                       "output_path": str(self.directory / "agents" / "reviewer" / "reviews" / f"{review_id}.md"),
                       "previous_review": self.room.meta("last_review")}
            self.room.set_meta("review_request", request)
            message = self._queue_review_request(request)
            reviewer = self._launch_slot("reviewer", "reviewer")
            reviewer.wake.set()
            return {"started": True, "agent_id": "reviewer", "message_id": message["id"], **request}
        if command == "submission_result":
            request_id = str(payload["request_id"])
            receipt = payload.get("receipt", {})
            if not isinstance(receipt, dict):
                raise ValueError("receipt must be an object")
            if isinstance(payload.get("platform"), dict):
                context = self.room.meta("context", {})
                context["platform"] = {**context.get("platform", {}), **payload["platform"]}
                self.room.set_meta("context", context)
            is_new = self.room.submission(request_id, receipt)
            if not is_new:
                self.room.publish_snapshot()
                return {"recorded": True, "duplicate": True}
            candidates = self.room.meta("candidates", {})
            candidate = candidates.get(request_id)
            if candidate:
                candidate["status"] = "confirmed" if receipt.get("correct") or receipt.get("duplicate") else "rejected"
                candidate["receipt"] = receipt
                self.room.set_meta("candidates", candidates)
            context = self.room.meta("context", {})
            platform = context.setdefault("platform", {})
            for key in ("cumulative_score", "correct_flag_count", "total_flag_count", "is_completed", "completed"):
                if receipt.get(key) is not None:
                    value = receipt[key]
                    if isinstance(value, (int, float)) and not isinstance(value, bool) and isinstance(platform.get(key), (int, float)):
                        value = max(platform[key], value)
                    platform[key] = value
            self.room.set_meta("context", context)
            if is_new:
                body = "平台提交结果：" + json.dumps({"request_id": request_id, "receipt": receipt}, ensure_ascii=False)
                recipients = {"leader", "reporter"}
                if candidate:
                    recipients.add(candidate["sender"])
                for recipient in recipients:
                    if self.room.agent(recipient):
                        self.room.send("platform", recipient, body)
                        if recipient in self.slots:
                            self.slots[recipient].wake.set()
            self.room.publish_snapshot()
            return {"recorded": True, "duplicate": not is_new}
        if command == "prepare_stop":
            self.preparing_stop = True
            self.room.event("prepare_stop", payload)
            self.room.send("host", "leader", "准备停止本题：保存证据和检查点，提交已取得flag，停止派发新任务并报告最后状态。")
            self.slots["leader"].wake.set()
            self.slots["reporter"].wake.set()
            end = time.monotonic() + float(self.settings.get("shutdown_timeout", 20))
            flushed = False
            while time.monotonic() < end:
                active = any(slot.running or slot.wake.is_set() or slot.recovering for slot in self.slots.values())
                unread = self.room.messages("reporter", pending=True, limit=10000)
                report = self.room.latest_report()
                if (not active and not unread and report["report_version"] > 0
                        and report["through_event"] >= self.room.evidence_watermark):
                    flushed = True
                    break
                reporter = self.slots.get("reporter")
                if reporter and not reporter.running and not reporter.wake.is_set():
                    record = self.room.agent("reporter") or {}
                    if int(record.get("restarts", 0)) <= 2:
                        reporter.wake.set()
                await asyncio.sleep(.05)
            snapshot = self.room.publish_snapshot()
            return {"preparing_stop": True, "flushed": flushed, "snapshot": snapshot,
                    "checkpoint": str(self.directory / "snapshot.json"),
                    "report_path": str(self.directory / "report.md")}
        if command == "cancel_stop":
            self.preparing_stop = False
            self.room.event("stop_cancelled", payload)
            self.room.send("host", "leader", "停题决定已撤销。保留本题现场，恢复解题和派发；根据最新证据继续推进。")
            self.slots["leader"].wake.set()
            return {"preparing_stop": False, "snapshot": self.room.publish_snapshot()}
        if command == "stop":
            self.stopping.set()
            return {"stopping": True}
        raise ValueError(f"unknown host command: {command}")

    async def _read_commands(self) -> None:
        reader = asyncio.StreamReader(limit=8 * 1024 * 1024)
        transport, _ = await asyncio.get_running_loop().connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        self._input_transport = transport
        try:
            while line := await reader.readline():
                try:
                    request = json.loads(line)
                    if not isinstance(request, dict):
                        raise ValueError("request must be an object")
                    task = asyncio.create_task(self._handle_request(request))
                    self._request_tasks.add(task)
                    task.add_done_callback(self._request_tasks.discard)
                except Exception as exc:
                    await self.response("", {"error": f"{type(exc).__name__}: {exc}"})
                if self.stopping.is_set():
                    break
        finally:
            self.stopping.set()

    async def _handle_request(self, request: dict) -> None:
        request_id = str(request.get("id", ""))
        try:
            result = await self.command(str(request.get("command", "")), request.get("payload") or {})
            await self.response(request_id, result)
        except Exception as exc:
            await self.response(request_id, {"error": f"{type(exc).__name__}: {exc}"})

    async def shutdown(self) -> None:
        self.stopping.set()
        for task in self._request_tasks:
            task.cancel()
        for task in self._background:
            task.cancel()
        for slot in self.slots.values():
            if slot.task:
                slot.task.cancel()
        tasks = [*self._request_tasks, *self._background] + [slot.task for slot in self.slots.values() if slot.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.room.event("runtime_stopped", {"pid": os.getpid()})
        self.room.publish_snapshot()
        if self._input_transport:
            self._input_transport.close()
        self.room.close()

    async def serve(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stopping.set)
        commands = asyncio.create_task(self._read_commands(), name="host-commands")
        try:
            await self.stopping.wait()
        finally:
            commands.cancel()
            await asyncio.gather(commands, return_exceptions=True)
            await self.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--backend", default="demo", choices=["demo", "claude"])
    parser.add_argument("--model")
    parser.add_argument("--settings-json", default="{}")
    args = parser.parse_args()
    runtime = TeamRuntime(args.run_dir, args.code, args.attempt_id, args.backend, args.model, json.loads(args.settings_json))
    asyncio.run(runtime.serve())


if __name__ == "__main__":
    main()
