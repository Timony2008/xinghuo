"""A single Veria challenge team behind the host's correlated JSONL protocol.

The public entrypoint only runs the real Claude SDK backend. Offline tests inject
drivers and a tool environment in-process; there is deliberately no Veria demo.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import time
import uuid

from .config import VERIA_MODELS, VERIA_UPSTREAM_COMMIT, veria_environment_cleanup_timeout
from .core import completion_confirmed
from .evidence_access import (is_private_evidence_path, read_evidence as read_evidence_page,
                              search_evidence as search_evidence_page)
from .room import atomic_write
from .team_runtime import TeamRuntime
from .veria.state import StaleAcquisitionJob, VeriaRoom
from .veria.prompts import continuation_prompt, rotation_policy_appendix


# Kept as the legacy import for historical callers; each runtime owns its roster.
SOLVERS = tuple(f"{family}-{number}" for family in ("pro", "flash") for number in (1, 2, 3))
REVIEW_SECTIONS = {"lateral_opportunities", "coverage", "missed_routes", "prior_rejections", "member_health"}


class ReviewPhaseChanged(Exception):
    """A normal bounded handoff; it does not consume the technical retry budget."""


class ReviewDeadlineReached(TimeoutError):
    pass


def safe_error(error: BaseException) -> str:
    text = str(error)
    for name in ("GLM_API_KEY", "DEEPSEEK_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "BENCHMARK_TOKEN"):
        secret = os.environ.get(name, "")
        if secret:
            text = text.replace(secret, "[redacted]")
    text = re.sub(r"sk-[A-Za-z0-9_-]+", "[redacted]", text)
    return f"{type(error).__name__}: {text[:1500]}"


def parse_object(value) -> dict:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if text.startswith("```json\n") and text.endswith("```"):
        text = text[8:-3].strip()
    result = json.loads(text)
    if not isinstance(result, dict):
        raise ValueError("expected a JSON object")
    return result


@dataclass
class VeriaSlot:
    agent_id: str
    role: str
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None
    driver: object | None = None
    running: bool = False
    turn_id: str = ""
    generation: str = field(default_factory=lambda: uuid.uuid4().hex)
    batch: dict | None = None
    continuation: int = 0
    review_id: str | None = None
    published_in_turn: bool = False
    tool_executions: int = 0
    last_activity: float = 0.0


class VeriaRuntime(TeamRuntime):
    """Owns each role, the shared container and the only writer to VeriaRoom."""

    def __init__(self, run_dir: Path, code: str, attempt_id: str, backend: str = "claude",
                 model: str | None = None, settings: dict | None = None, *,
                 environment_factory=None, driver_factory=None):
        if backend != "claude":
            raise ValueError("Veria supports backend=claude only")
        if not code or code in {".", ".."} or any(c in code for c in ("/", "\\", "\x00")):
            raise ValueError("challenge code must be a safe directory name")
        self.run_dir = Path(run_dir).resolve()
        self.directory = self.run_dir / "teams" / code
        self.code, self.attempt_id, self.backend, self.model = code, attempt_id, backend, model
        self.settings = settings or {}
        self.models = dict(self.settings.get("veria_models") or VERIA_MODELS)
        self.solver_ids = tuple(self.settings.get("veria_solver_ids") or
            [member for member in self.models if member not in {"coordinator", "reporter", "reviewer"}])
        if set(self.models) != {*self.solver_ids, "coordinator", "reporter", "reviewer"}:
            raise ValueError("Veria requires its configured solver roster and three named support roles")
        self.providers = dict(self.settings.get("veria_providers") or {})
        self.team_profile = str(self.settings.get("veria_team_profile") or ("mixed4" if self.solver_ids != SOLVERS else "legacy6"))
        if self.providers and set(self.providers) != set(self.models):
            raise ValueError("every Veria member requires an immutable provider binding")
        self.room = VeriaRoom(self.directory, code, attempt_id, solver_ids=self.solver_ids)
        self.environment_factory, self.driver_factory = environment_factory, driver_factory
        self.environment = None
        self.slots: dict[str, VeriaSlot] = {}
        self.started = False
        self.preparing_stop = False
        self.stopping = asyncio.Event()
        self._work_enabled = asyncio.Event()
        self._output_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._background: list[asyncio.Task] = []
        self._request_tasks: set[asyncio.Task] = set()
        self._input_transport = None
        self._candidate_waiters: dict[str, asyncio.Event] = {}
        self._sent_candidates: set[str] = set()
        self._shutdown_done = False
        self._preserve_environment = False
        self._environment_failures = 0
        self._acquisition_task: asyncio.Task | None = None

    def _workspace(self, slot: VeriaSlot) -> Path:
        base = self.directory / "agents" / slot.agent_id
        return base / "reviews" / slot.review_id if slot.review_id else base

    def _completed(self) -> bool:
        platform = self.room.meta("context", {}).get("platform", {})
        return completion_confirmed(platform)

    def _role_health(self) -> tuple[list[dict], list[str]]:
        roles, failures = [], []
        for record in self.room.members():
            slot = self.slots.get(record["agent_id"])
            disconnected = bool(slot and slot.driver and not slot.driver.alive)
            review_complete = bool(slot and slot.role == "reviewer" and
                (self.room.review(slot.review_id) or {}).get("status") in
                    {"completed", "partial", "cancelled", "timed_out", "failed"})
            unexpected_exit = bool(slot and slot.task and slot.task.done() and
                not self.stopping.is_set() and not review_complete)
            failed = record["status"] in {"failed", "recovering"} or disconnected or unexpected_exit
            if failed:
                failures.append(record["agent_id"])
            failure = self.room.meta("role_failure:" + record["agent_id"], {})
            processes = getattr(slot.driver, "_owned_processes", {}) if slot and slot.driver else {}
            try:
                from .resource_health import process_memory
                process_resources = [process_memory(int(pid)) for pid in processes]
            except (ImportError, OSError, ValueError):
                process_resources = []
            roles.append({**record, "provider": self.providers.get(record["agent_id"]),
                "failure_category": failure.get("category") if failed else None,
                "failure_at": failure.get("failed_at") if failed else None,
                "sdk_process_resources": process_resources,
                "running": bool(slot and slot.running),
                "client_initialized": bool(slot and slot.driver),
                "client_alive": bool(slot and slot.driver and slot.driver.alive),
                "degraded_reason": "role loop exited" if unexpected_exit else
                    "SDK client disconnected" if disconnected else record.get("last_error")})
            if disconnected and slot and slot.role != "reporter" and not slot.running:
                # The role loop performs the same bounded recovery as a turn
                # failure. Health never resets that role's retry budget.
                slot.wake.set()
        return roles, failures

    def _update_platform(self, platform: dict | None) -> None:
        if platform is None:
            return
        if not isinstance(platform, dict):
            raise ValueError("platform must be an object")
        context = self.room.meta("context", {})
        previous = context.get("platform", {})
        updated = {**previous, **platform}
        if (isinstance(previous.get("correct_flag_count"), int)
                and isinstance(updated.get("correct_flag_count"), int)):
            updated["correct_flag_count"] = max(previous["correct_flag_count"], updated["correct_flag_count"])
        reported_complete = bool(updated.get("completed") or updated.get("is_completed"))
        updated["completed"] = bool(reported_complete or completion_confirmed(previous))
        complete = completion_confirmed(updated)
        updated["completed"] = complete
        if "is_completed" in updated:
            updated["is_completed"] = complete
        if reported_complete and not complete:
            self.room.event("platform_completion_inconsistent", {
                "correct_flag_count": updated.get("correct_flag_count"),
                "flag_count": updated.get("flag_count", updated.get("total_flag_count"))})
        context["platform"] = updated
        self.room.set_meta("context", context)
        if complete:
            self._work_enabled.clear()
        elif self.started and not self.preparing_stop and not self.stopping.is_set():
            self._work_enabled.set()

    async def start(self, context: dict) -> dict:
        async with self._start_lock:
            if self.started:
                return {"started": True, "pid": os.getpid(), "snapshot": self.room.snapshot()}
            if context.get("attempt_id", self.attempt_id) != self.attempt_id:
                raise ValueError("context attempt does not match runtime")
            contract = {"team_engine": "veria", "upstream_commit": VERIA_UPSTREAM_COMMIT,
                        "models": self.models, "python": self.settings.get("veria_python"),
                        "tool_image": self.settings.get("veria_tool_image")}
            if self.team_profile != "legacy6" or self.providers:
                contract.update(team_profile=self.team_profile, solver_ids=list(self.solver_ids), providers=self.providers)
            if self.settings.get("veria_environment", "docker") != "docker":
                contract["environment"] = self.settings["veria_environment"]
            saved = self.room.meta("veria_contract")
            if saved and saved != contract:
                legacy_fields = {"team_profile", "solver_ids", "providers"}
                legacy_provider = self.settings.get("veria_provider")
                if not legacy_provider:
                    names = tuple(saved.get("models", {}).values())
                    legacy_provider = ("glm" if names and all(str(name).startswith("glm-") for name in names) else
                                       "deepseek" if names and all(str(name).startswith("deepseek-") for name in names) else None)
                legacy_match = ("solver_ids" not in saved and self.solver_ids == SOLVERS and
                    {k: v for k, v in contract.items() if k not in legacy_fields} == saved and
                    (not self.providers or (legacy_provider is not None and set(self.providers.values()) == {legacy_provider})))
                if not legacy_match:
                    raise ValueError("cannot change the Veria engine, roster, providers, models or environment within an attempt")
            self.room.set_meta("veria_contract", contract)
            previous = self.room.meta("context", {})
            if previous.get("deadline") is not None and context.get("deadline") != previous["deadline"]:
                raise ValueError("resume cannot change the persisted competition deadline")
            incoming_platform = context.get("platform", previous.get("platform", {}))
            context = {**previous, **context, "attempt_id": self.attempt_id,
                       "challenge_code": self.code, "team_engine": "veria"}
            context["rotation_policy"] = self._rotation_policy()
            context["platform"] = previous.get("platform", {})
            self.room.set_meta("context", context)
            self._update_platform(incoming_platform)
            context = self.room.meta("context", {})
            atomic_write(self.directory / "inputs" / "challenge.json", json.dumps({
                "challenge": context.get("challenge", {}), "addresses": context.get("addresses", []),
                "platform": context.get("platform", {}), "attempt_id": self.attempt_id},
                ensure_ascii=False, indent=2) + "\n")
            self.room.set_meta("runtime_pid", os.getpid())
            self.room.recover_deliveries()
            self.room.recover_bumps()
            for agent_id in (*self.solver_ids, "coordinator", "reporter"):
                role = "solver" if agent_id in self.solver_ids else agent_id
                self.room.register_member(agent_id, role, self.models[agent_id],
                    workspace=str(self.directory / "agents" / agent_id),
                    run_id=str(context.get("run_id") or context.get("nonce") or self.run_dir.name))
            if self.environment_factory is None:
                if self.settings.get("veria_environment", "docker") == "hosted":
                    from .veria.hosted_environment import HostedProcessEnvironment
                    self.environment_factory = HostedProcessEnvironment
                else:
                    from .veria.environment import SharedDockerEnvironment
                    self.environment_factory = SharedDockerEnvironment
            self.environment = self.environment_factory(self.directory,
                str(context.get("run_id") or context.get("nonce") or self.run_dir.name),
                self.code, self.attempt_id, self.settings)
            environment = await self.environment.start()
            previous_environment = self.room.meta("environment", {})
            if (previous_environment.get("image_id") and
                    previous_environment["image_id"] != environment.get("image_id")):
                raise ValueError("tool image identity changed within the same attempt")
            self.room.set_meta("environment", environment)
            self.room.event("environment_ready", environment)
            if environment.get("volatile_state_lost"):
                self.room.notice("runtime", "工具容器已重建，易失现场丢失。重新核对后台服务、权限与前提；持久文件仍在。")
            # The supervisor reconciles platform receipts before role work resumes.
            for recovered in context.get("recovery", {}).get("submissions", []):
                request_id = recovered.get("request_id")
                if request_id and recovered.get("receipt"):
                    if not self.room.candidate(request_id) and recovered.get("flag"):
                        self.room.save_candidate(str(recovered.get("sender") or "recovery"),
                            recovered["flag"], request_id=request_id)
                    await self._receive_receipt({"request_id": request_id,
                        "receipt": recovered["receipt"], "platform": context.get("platform", {})})
            self.started = True
            self.room.recover_acquisitions()
            self._schedule_acquisitions()
            self.preparing_stop = False
            if not self._completed():
                self._work_enabled.set()
            for candidate in self.room.pending_candidates():
                await self._emit_candidate(candidate)
            for agent_id in (*self.solver_ids, "coordinator", "reporter"):
                self._launch(agent_id, "solver" if agent_id in self.solver_ids else agent_id)
            self.slots["coordinator"].wake.set()
            review = self.room.meta("review_request")
            if review and (self.room.review(review["review_id"]) or {}).get("status") == "running":
                self._launch_review(review)
            self._background.append(asyncio.create_task(self._heartbeat(), name="veria-heartbeat"))
            self.room.event("runtime_started", {"pid": os.getpid(), "team_engine": "veria"})
            return {"started": True, "pid": os.getpid(), "snapshot": self.room.publish_snapshot()}

    def _launch(self, agent_id: str, role: str, *, review_id: str | None = None) -> VeriaSlot:
        slot = self.slots.get(agent_id)
        if slot and slot.task and not slot.task.done():
            return slot
        slot = VeriaSlot(agent_id, role, review_id=review_id)
        self.slots[agent_id] = slot
        loop = self._reporter_loop if role == "reporter" else self._role_loop
        slot.task = asyncio.create_task(loop(slot), name=f"veria-{agent_id}")
        return slot

    async def _new_driver(self, slot: VeriaSlot):
        if self.driver_factory is None:
            from .veria.driver import VeriaRoleDriver
            self.driver_factory = VeriaRoleDriver
        record = self.room.agent(slot.agent_id) or {}
        workspace = self._workspace(slot)
        workspace.mkdir(parents=True, exist_ok=True)

        async def on_tool(name, arguments):
            return await self.tool(slot, name, arguments)

        async def on_session(session_id):
            self.room.set_session(slot.agent_id, session_id, generation=slot.generation,
                                  session_path=str(workspace), reason="sdk_observed")

        async def on_event(kind, data):
            if kind in {"tool_execution_registered", "tool_execution_result", "assistant_text", "visible_text"}:
                slot.last_activity = time.monotonic()
            if kind == "tool_execution_result" and data.get("success"):
                slot.tool_executions += 1
            metadata = {"generation": slot.generation, "turn_id": slot.turn_id,
                        "review_id": slot.review_id, "model_id": self.models[slot.agent_id],
                        "provider": self.providers.get(slot.agent_id),
                        "name": data.get("name", data.get("tool_name", ""))}
            metadata["reportable"] = kind in {
                "assistant_text", "visible_text", "tool_call", "tool_result",
                "tool_execution_result", "sdk_error", "tool_routing_error", "assistant_error"}
            if kind == "tool_execution_result":
                metadata.update(success=bool(data.get("success")), tool_use_id=data.get("tool_use_id"))
            if slot.role == "coordinator" and kind in {"tool_call", "tool_use", "tool_result"}:
                metadata["reportable"] = False
            return self.room.record_evidence(slot.agent_id, kind, data, metadata=metadata)

        context = {**self.room.meta("context", {}), "environment": self.room.meta("environment", {}),
                   "agent_id": slot.agent_id, "solver_ids": self.solver_ids,
                   "provider": self.providers.get(slot.agent_id), "role_providers": self.providers,
                   "rotation_policy": self._rotation_policy(),
                   "team_profile": self.team_profile, "review": self._review_context() if slot.role == "reviewer" else None,
                   "distfile_names": sorted(str(path.relative_to(self.directory / "inputs"))
                       for path in (self.directory / "inputs").rglob("*") if path.is_file())}
        slot.driver = self.driver_factory(slot.agent_id, slot.role, self.models[slot.agent_id],
            workspace, context, self.environment, on_tool, on_session, on_event,
            session_id=record.get("session_id"))
        await slot.driver.start()
        self.room.update_agent(slot.agent_id, status="idle")
        return slot.driver

    def _messages(self, slot: VeriaSlot) -> list[dict]:
        messages = self.room.mailbox(slot.agent_id, limit=30)
        self.room.mark_injected(slot.agent_id, [item["id"] for item in messages], slot.turn_id)
        return messages

    def _rotation_policy(self) -> dict:
        # Settings are supplied by the validated supervisor configuration. Do
        # not inherit a stale or challenge-supplied instruction from context.
        timeout = float(self.settings.get("no_flag_timeout", 0))
        return {"mode": "no_flag_timeout" if timeout > 0 else "review",
                "timeout_seconds": timeout}

    def _prompt(self, slot: VeriaSlot) -> str:
        slot.turn_id = uuid.uuid4().hex
        slot.published_in_turn = False
        payload = {"agent_id": slot.agent_id, "turn_id": slot.turn_id,
                   "platform": self.room.meta("context", {}).get("platform", {}),
                   "rotation_policy": self._rotation_policy(),
                   "messages": self._messages(slot), "report": self.room.latest_report()}
        if slot.role == "solver":
            bumps = self.room.pending_bumps(slot.agent_id)
            self.room.inject_bumps(slot.agent_id, [item["id"] for item in bumps], slot.turn_id)
            payload["guidance"] = bumps
            confirmed = self.room.confirmed_submissions()
            payload["instruction"] = continuation_prompt(
                has_confirmed=bool(confirmed), insights=bumps,
                first=not slot.continuation and not int((self.room.agent(slot.agent_id) or {}).get("turns", 0)),
                rotation_policy=payload["rotation_policy"])
            payload["confirmed_candidates"] = confirmed
        elif slot.role == "coordinator":
            payload["instruction"] = "Read notices and solver traces. Give technical guidance, selectively broadcast reusable findings, and queue targeted bumps for subsequent turns. No report draft is required."
            payload["members"] = self.room.agents()
        elif slot.role == "reporter":
            payload["batch"] = slot.batch
            payload["reporter_backlog"] = self.room.meta("reporter_backlog", {})
            payload["instruction"] = "Summarize only this fixed batch. Preserve prior verified facts and distinguish hypotheses, negatives, blockers and unchecked areas. Publish with publish_report or return the complete report JSON. The report needs overview, recent_changes, short_history, next_steps, evidence_refs. No coordinator draft is needed."
        else:
            self._advance_review_phase(slot)
            payload["review_context"] = self._review_context()
            payload["review_context"].update(self._review_schedule(slot))
            payload["review_context"]["phase"] = (self.room.review(slot.review_id) or {}).get("phase")
            payload["instruction"] = "First actually reproduce EVERY locally known flag acquisition path using new tool executions; persist per-flag outcomes with record_reproductions (even an empty list when no flags are known) before broader review. Complete with matching reproductions, audit_evidence_refs from after the checkpoint, and sections for lateral_opportunities, coverage, missed_routes, prior_rejections, member_health; mark unchecked or blocked items with reasons. Old logs and resubmitting an old value are not reproduction. Then check lateral opportunities, coverage, missed vulnerabilities, untried routes, previous rejections, and member health. Before resetting services, changing shared state, or replaying a one-shot acquisition, notify_coordinator and ask it to broadcast the impact and coordinate affected members. Read the persistent coordination messages and wait for a concrete plan and affected members to be ready; a broadcast alone does not authorize disruption. If coordination cannot be completed within this bounded review, record that reproduction as blocked and continue the remaining checks. Complete the review with evidence for each reproduced/failed/blocked/unchecked item and concrete corrections."
        if slot.role != "solver":
            payload["instruction"] += rotation_policy_appendix(payload["rotation_policy"])
        return json.dumps(payload, ensure_ascii=False, default=str)

    async def _turn(self, slot: VeriaSlot) -> dict:
        record = self.room.agent(slot.agent_id) or {}
        if int(record.get("restarts", 0)) > int(self.settings.get("max_retries", 2)):
            raise RuntimeError("role recovery budget exhausted")
        if slot.role == "reviewer":
            self._advance_review_phase(slot)
        driver = slot.driver or await self._new_driver(slot)
        slot.running = True
        self.room.update_agent(slot.agent_id, status="running")
        try:
            # Bound inactivity, not useful long-running SDK turns. A hard
            # 15-minute cutoff otherwise exhausts healthy solvers' retries.
            slot.last_activity = time.monotonic()
            turn_started = slot.last_activity
            turn = asyncio.create_task(driver.turn(self._prompt(slot)))
            timeout = float(self.settings.get("role_turn_timeout", 900))
            try:
                while not turn.done():
                    # Review/report work remains bounded even with continuous
                    # activity; long solver/coordinator turns use inactivity.
                    last_progress = slot.last_activity if slot.role in {"solver", "coordinator"} else turn_started
                    remaining = max(.001, timeout - (time.monotonic() - last_progress))
                    if slot.role == "reviewer":
                        phase_remaining = self._review_next_boundary(slot) - time.time()
                        remaining = min(remaining, max(.001, phase_remaining))
                    done, _ = await asyncio.wait({turn}, timeout=remaining)
                    if slot.role == "reviewer" and not done:
                        self._advance_review_phase(slot, interrupt_on_change=True)
                    last_progress = slot.last_activity if slot.role in {"solver", "coordinator"} else turn_started
                    if not done and time.monotonic() - last_progress >= timeout:
                        reason = "inactive" if slot.role in {"solver", "coordinator"} else "bounded turn limit reached"
                        raise TimeoutError(f"role {reason} after {timeout:g} seconds")
                result = turn.result()
            finally:
                if not turn.done():
                    turn.cancel()
                    await asyncio.gather(turn, return_exceptions=True)
            if result.get("is_error"):
                raise RuntimeError(str(result.get("error") or result.get("result") or "SDK turn failed"))
            if result.get("session_id"):
                self.room.set_session(slot.agent_id, result["session_id"], generation=slot.generation,
                                      session_path=str(self._workspace(slot)), reason="turn_boundary")
            if result.get("result"):
                self.room.record_evidence(slot.agent_id, "visible_text", result["result"],
                    metadata={"turn_id": slot.turn_id, "generation": slot.generation, "review_id": slot.review_id})
            self.room.complete_turn(slot.agent_id, slot.turn_id)
            self.room.update_agent(slot.agent_id, status="idle", turns=int(record.get("turns", 0)) + 1,
                                   last_error=None)
            slot.continuation += 1
            return result
        finally:
            slot.running = False

    async def _role_loop(self, slot: VeriaSlot) -> None:
        try:
            while not self.stopping.is_set():
                await self._work_enabled.wait()
                if slot.role != "solver":
                    await slot.wake.wait()
                    slot.wake.clear()
                    await self._work_enabled.wait()
                if self.stopping.is_set():
                    break
                if slot.role == "reviewer" and (self.room.review(slot.review_id) or {}).get("status") != "running":
                    return
                try:
                    result = await self._turn(slot)
                    await self._submit_final_candidates(slot, result)
                    if slot.role == "reviewer":
                        if not slot.published_in_turn:
                            await self._complete_review(slot, parse_object(result.get("structured_output") or result.get("result")))
                        self.room.update_agent(slot.agent_id, restarts=0)
                        return
                    self.room.update_agent(slot.agent_id, restarts=0)
                    if slot.role == "solver":
                        # Upstream's growing continuation delay; ordinary lack of
                        # results does not consume the technical recovery budget.
                        delay = min(slot.continuation * 30, 300)
                        await self._delay(float(self.settings.get("veria_continuation_delay", delay)))
                    elif self.room.mailbox(slot.agent_id):
                        # Injected delivery is not semantic acknowledgement. Only
                        # newly queued mail causes another normal coordinator turn.
                        if any(m.get("state") in {"queued", "pending", "uncertain"} for m in self.room.mailbox(slot.agent_id)):
                            slot.wake.set()
                except ReviewPhaseChanged:
                    await self._close_driver(slot)
                    slot.wake.set()
                except ReviewDeadlineReached as error:
                    self.room.finish_review(slot.review_id, "timed_out", str(error))
                    self.room.update_agent(slot.agent_id, status="timed_out", last_error=str(error))
                    await self.emit("review_terminated", source="reviewer", review_id=slot.review_id,
                                    status="timed_out", reason=str(error))
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if not await self._failure(slot, error):
                        return
                    slot.wake.set()
        finally:
            if slot.role == "reviewer" and self.stopping.is_set() and not self._preserve_environment:
                self.room.finish_review(slot.review_id, "partial", "runtime stopped before review completed")
            await self._close_driver(slot)

    async def _reporter_loop(self, slot: VeriaSlot) -> None:
        interval = float(self.settings.get("veria_report_interval", 60))
        try:
            while not self.stopping.is_set():
                await self._delay(interval)
                if self.stopping.is_set():
                    break
                checkpoint = self.room.evidence_watermark
                # Drain the fixed checkpoint in bounded consecutive batches.
                # A busy team must not wait another minute per batch.
                for _ in range(8):
                    try:
                        batch = self.room.reporter_batch(through=checkpoint)
                        if not batch or self.stopping.is_set():
                            break
                        slot.batch = batch
                        result = await self._turn(slot)
                        if not slot.published_in_turn:
                            await self._publish_report(slot, parse_object(result.get("structured_output") or result.get("result")))
                        self.room.update_agent(slot.agent_id, restarts=0)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        if not await self._failure(slot, error):
                            return
                        break
                    finally:
                        slot.batch = None
                covered = self.room.latest_report()["through_event"]
                self.room.set_meta("reporter_backlog", {"checkpoint": checkpoint,
                    "covered_through": covered, "unprocessed_at_checkpoint": covered < checkpoint,
                    "evidence_index": str(self.directory / "room.sqlite3")})
        finally:
            await self._close_driver(slot)

    async def _delay(self, delay: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.stopping.wait(), timeout=max(.01, delay))

    async def _close_driver(self, slot: VeriaSlot) -> None:
        if slot.driver:
            try:
                await asyncio.wait_for(slot.driver.close(), timeout=float(self.settings.get("shutdown_timeout", 20)))
            except (Exception, asyncio.CancelledError) as error:
                self.room.event("driver_cleanup_incomplete", {"agent_id": slot.agent_id, "error": safe_error(error)})
            slot.driver = None
        slot.running = False

    async def _failure(self, slot: VeriaSlot, error: BaseException) -> bool:
        description = safe_error(error)
        record = self.room.agent(slot.agent_id) or {}
        failures = int(record.get("restarts", 0)) + 1
        lowered = description.lower()
        category = ("balance" if any(token in lowered for token in ("402", "insufficient balance", "insufficientbalance", "余额")) else
                    "authentication" if any(token in lowered for token in ("authentication", "unauthorized", "invalid api key", "401", "403")) else
                    "rate_limit" if any(token in lowered for token in ("429", "rate limit", "ratelimit")) else
                    "configuration" if any(token in lowered for token in ("configuration", "unsupported model")) else
                    "process_exit" if any(token in lowered for token in ("exit code", "exited", "-9", "sigkill", "disconnected")) else
                    "timeout" if isinstance(error, TimeoutError) else "runtime_error")
        terminal = category in {"balance", "authentication", "configuration"}
        retry = not terminal and failures <= int(self.settings.get("max_retries", 2))
        self.room.set_meta("role_failure:" + slot.agent_id, {"category": category, "error": description,
            "provider": self.providers.get(slot.agent_id), "failed_at": time.time(), "retrying": retry})
        if slot.role == "reviewer":
            self.room.recover_review(slot.review_id)
            if not retry:
                self.room.finish_review(slot.review_id, "failed", description)
        self.room.update_agent(slot.agent_id, restarts=failures,
                               status="recovering" if retry else "failed", last_error=description)
        await self.emit("role_failed", sender=slot.agent_id, source=slot.agent_id,
                        error=description, category=category, provider=self.providers.get(slot.agent_id),
                        retrying=retry, failures=failures)
        self.room.event("role_failed", {"agent_id": slot.agent_id, "error": description,
            "category": category, "provider": self.providers.get(slot.agent_id), "retrying": retry})
        tool_member = getattr(slot.driver, "tool_member", slot.agent_id)
        await self._close_driver(slot)
        self.room.recover_role(slot.agent_id)
        if self.environment and slot.role != "reporter":
            try:
                await asyncio.wait_for(self.environment.stop_member(tool_member),
                    timeout=float(self.settings.get("shutdown_timeout", 20)))
            except Exception as cleanup_error:
                self.room.event("member_cleanup_incomplete", {
                    "agent_id": slot.agent_id, "error": safe_error(cleanup_error)})
        if retry:
            slot.generation = uuid.uuid4().hex
            await self._delay(min(failures, 5))
        self.room.publish_snapshot()
        return retry

    async def _emit_candidate(self, candidate: dict) -> None:
        request_id = candidate["request_id"]
        if request_id in self._sent_candidates:
            return
        self._sent_candidates.add(request_id)
        sources = candidate.get("sources") or []
        sender = candidate.get("sender") or (sources[0] if sources else "runtime")
        if isinstance(sender, dict):
            sender = sender.get("sender", sender.get("agent_id", "runtime"))
        await self.emit("candidate_flag", flag=candidate["flag"], request_id=request_id,
                        sender=sender, source=sender, event_seq=candidate.get("event_seq"))

    async def _submit(self, slot: VeriaSlot, arguments: dict) -> dict:
        flag = str(arguments.get("flag", "")).strip()
        if not 1 <= len(flag) <= 4096:
            raise ValueError("flag length must be between 1 and 4096")
        existing = self.room.candidate_for_flag(flag)
        cooldown = self.room.meta("submission_cooldowns", {}).get(slot.agent_id, {})
        seconds = (0, 30, 120, 300, 600)[min(int(cooldown.get("wrong_count", 0)), 4)]
        remaining = seconds - (time.time() - float(cooldown.get("last_incorrect", 0)))
        if not existing and remaining > 0:
            return {"status": "cooldown", "retry_after": round(remaining, 1), "queued": False}
        candidate = self.room.save_candidate(slot.agent_id, flag, evidence_refs=arguments.get("evidence_refs"))
        request_id = candidate["request_id"]
        if candidate["status"] in {"correct", "duplicate", "incorrect", "error"}:
            return candidate
        waiter = self._candidate_waiters.setdefault(request_id, asyncio.Event())
        await self._emit_candidate(candidate)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(waiter.wait(), timeout=float(self.settings.get("operation_timeout", 30)))
        result = self.room.candidate(request_id) or candidate
        if result["status"] in {"pending", "unknown"}:
            result = {**result, "message": "已排队，结果未知；保留原 request_id，后续回执将持久保存并通知。"}
        return result

    async def _submit_final_candidates(self, slot: VeriaSlot, result: dict) -> None:
        if slot.role not in {"solver", "reviewer", "coordinator"}:
            return
        flags = []
        structured = result.get("structured_output")
        if isinstance(structured, dict) and isinstance(structured.get("flag"), str):
            flags.append(structured["flag"])
        flags += re.findall(r"(?m)^FLAG:\s*([^\r\n]+)$", str(result.get("result", "")))
        for flag in dict.fromkeys(flags):
            if 1 <= len(flag.strip()) <= 4096:
                await self._submit(slot, {"flag": flag})

    async def _receive_receipt(self, payload: dict) -> dict:
        request_id = str(payload["request_id"])
        receipt = payload.get("receipt", {})
        if not isinstance(receipt, dict):
            raise ValueError("receipt must be an object")
        self._update_platform(payload.get("platform"))
        previous_status = (self.room.candidate(request_id) or {}).get("status")
        changed = self.room.apply_receipt(request_id, receipt)
        candidate = self.room.candidate(request_id)
        if changed:
            cooldowns = self.room.meta("submission_cooldowns", {})
            senders = {source if isinstance(source, str) else source.get("sender", source.get("agent_id"))
                       for source in (candidate or {}).get("sources", [])}
            for sender in senders:
                if not sender:
                    continue
                if (candidate or {}).get("status") == "incorrect" and previous_status != "incorrect":
                    old = cooldowns.get(sender, {})
                    cooldowns[sender] = {"wrong_count": old.get("wrong_count", 0) + 1,
                                         "last_incorrect": time.time()}
                elif (candidate or {}).get("status") in {"correct", "duplicate"}:
                    cooldowns.pop(sender, None)
            self.room.set_meta("submission_cooldowns", cooldowns)
            self.room.broadcast("platform", json.dumps({"request_id": request_id, "receipt": receipt,
                "whole_challenge_complete": self._completed()}, ensure_ascii=False),
                recipients=[*self.solver_ids, "coordinator"], message_id="receipt:" + request_id + ":" +
                    hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest()[:16])
            if "coordinator" in self.slots:
                self.slots["coordinator"].wake.set()
        if request_id in self._candidate_waiters:
            self._candidate_waiters[request_id].set()
        self._schedule_acquisitions()
        return {"recorded": True, "duplicate": not changed, "status": (candidate or {}).get("status", "unknown")}

    def _schedule_acquisitions(self) -> None:
        if self.stopping.is_set() or not self.room.acquisition_jobs():
            return
        if self._acquisition_task and not self._acquisition_task.done():
            return
        self._acquisition_task = asyncio.create_task(self._acquisition_loop(), name="veria-acquisition-material")

    async def _freeze_acquisition_file(self, path: Path, destination: Path, *, expected_sha256: str | None = None) -> dict:
        """Copy saved bytes only. No command execution, path following or model summary."""
        from .evidence_access import is_private_evidence_path, open_evidence_file
        relative = path.relative_to(self.directory)
        if (not relative.parts or relative.parts[0] not in {"evidence", "inputs", "members", "shared"}
                or is_private_evidence_path(relative)):
            raise ValueError("only public persisted challenge material can be frozen")
        destination_relative = destination.relative_to(self.directory)
        # Published artifacts live in the shared tool mount. Anchor every
        # destination component too: a symlink there must not redirect copying.
        # Unpublished temporary bytes are kept outside that shared mount.
        with contextlib.ExitStack() as stack:
            parent = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, parent)
            for component in destination_relative.parts:
                try:
                    os.mkdir(component, dir_fd=parent)
                except FileExistsError:
                    pass
                parent = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                stack.callback(os.close, parent)
            return await self._copy_acquisition_file(path, relative, destination, parent,
                "copy-" + uuid.uuid4().hex + ".tmp", expected_sha256)

    def _acquisition_tool_path(self, path: Path) -> str:
        relative = path.relative_to(self.directory)
        if relative.parts[0] not in {"inputs", "members", "shared", "control"}:
            raise ValueError("artifact is not available through the member tool mount")
        return str(path) if self.settings.get("veria_environment") == "hosted" else str(Path("/challenge") / relative)

    async def _copy_acquisition_file(self, path, relative, destination, parent, temporary, expected_sha256):
        from .evidence_access import open_evidence_file
        def identity(info):
            return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

        async def verify(handle, name, sha256, size, *, expected_inode=None):
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError("frozen artifact must be a regular file")
            if expected_inode is not None and (before.st_dev, before.st_ino) != expected_inode:
                raise ValueError("frozen artifact was exchanged during publication")
            actual, actual_size = hashlib.sha256(), 0
            while chunk := handle.read(64 * 1024):
                actual.update(chunk)
                actual_size += len(chunk)
                await asyncio.sleep(0)
            after = os.fstat(handle.fileno())
            named = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if (not stat.S_ISREG(named.st_mode) or identity(before) != identity(after)
                    or identity(after) != identity(named) or actual_size != size
                    or actual.hexdigest() != sha256):
                raise ValueError("an existing frozen artifact has been modified or exchanged")

        with contextlib.ExitStack() as stack:
            private = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, private)
            for component in ("acquisitions", ".building"):
                try:
                    os.mkdir(component, mode=0o700, dir_fd=private)
                except FileExistsError:
                    pass
                private = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=private)
                stack.callback(os.close, private)
            os.fchmod(private, 0o700)
            fd = os.open(temporary, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=private)
            created = False
            name = None
            try:
                with os.fdopen(fd, "w+b") as output:
                    digest, size = hashlib.sha256(), 0
                    with open_evidence_file(self.directory, relative) as source:
                        before = os.fstat(source.fileno())
                        while chunk := source.read(64 * 1024):
                            output.write(chunk)
                            digest.update(chunk)
                            size += len(chunk)
                            await asyncio.sleep(0)
                        after = os.fstat(source.fileno())
                        if identity(before) != identity(after) or size != after.st_size:
                            raise ValueError("source changed while acquisition material was being copied")
                    output.flush()
                    # Apply permissions to the actual open inode, never a
                    # shared pathname that another member can turn into a link.
                    os.fchmod(output.fileno(), 0o444)
                    os.fsync(output.fileno())
                    sha256 = digest.hexdigest()
                    if expected_sha256 and expected_sha256 != sha256:
                        raise ValueError("saved evidence SHA-256 no longer matches its journal")
                    name = sha256 + "-" + re.sub(r"[^A-Za-z0-9_.-]", "_", path.name)[:100]
                    frozen = destination / name
                    try:
                        existing_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                    except FileNotFoundError:
                        original = os.fstat(output.fileno())
                        staged = os.stat(temporary, dir_fd=private, follow_symlinks=False)
                        if (not stat.S_ISREG(staged.st_mode)
                                or (staged.st_dev, staged.st_ino) != (original.st_dev, original.st_ino)):
                            raise ValueError("private artifact staging file was exchanged")
                        os.replace(temporary, name, src_dir_fd=private, dst_dir_fd=parent)
                        created = True
                        published_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                        with os.fdopen(published_fd, "rb") as published:
                            await verify(published, name, sha256, size,
                                         expected_inode=(original.st_dev, original.st_ino))
                    else:
                        with os.fdopen(existing_fd, "rb") as existing:
                            await verify(existing, name, sha256, size)
            except BaseException:
                if created and name is not None:
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(name, dir_fd=parent)
                raise
            finally:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(temporary, dir_fd=private)
        result = {"source_path": str(path), "frozen_path": str(frozen), "sha256": sha256,
                  "tool_path": self._acquisition_tool_path(frozen), "bytes": size, "verified_reproduction": False}
        if relative.parts[0] in {"inputs", "members", "shared", "control"}:
            result["source_tool_path"] = self._acquisition_tool_path(path)
        return result

    def _acquisition_script_path(self, value: str, member: str) -> Path:
        path = Path(value)
        if path.is_absolute() and path.is_relative_to("/challenge"):
            path = self.directory / path.relative_to("/challenge")
        elif not path.is_absolute():
            # Every member Bash starts here; do not guess shell `cd` state.
            path = self.directory / "members" / member / "work" / path
        if not path.is_relative_to(self.directory):
            raise ValueError("script is outside persisted challenge material; copy it into member work/shared first")
        return path

    @staticmethod
    def _observed_script(command: str) -> str | None:
        try:
            words = shlex.split(command)
        except ValueError:
            return None
        if not words or any(word in {";", "&&", "||", "|", ">", ">>", "<"} for word in words):
            return None
        executable = Path(words[0]).name
        if executable in {"python", "python3", "sage", "ruby", "node", "bash", "sh", "perl"}:
            arguments = [word for word in words[1:] if not word.startswith("-")]
            if arguments and Path(arguments[0]).suffix.lower() in {".py", ".sage", ".rb", ".js", ".sh", ".pl"}:
                return arguments[0]
        elif words[0].startswith("./") and Path(words[0]).suffix.lower() in {".py", ".sage", ".rb", ".js", ".sh", ".pl"}:
            return words[0]
        return None

    async def _build_acquisition(self, material: dict) -> dict:
        candidate, job = material["candidate"], material["job"]
        draft = (material["draft"] or {}).get("payload", {})
        # Content-addressed blobs are shared by all receipt versions, avoiding
        # another full copy of the same historical evidence for each new flag.
        destination = self.directory / "shared" / "acquisitions" / "blobs"
        evidence, observed, scripts, unavailable = [], [], [], []
        inferred_paths = []
        for record in material["records"]:
            data = record["data"]
            item = {"event_seq": record["seq"], "sender": record["sender"], "kind": data.get("kind"),
                    "id": data.get("id"), "source_sha256": data.get("sha256")}
            try:
                frozen = await self._freeze_acquisition_file(Path(data["path"]), destination,
                    expected_sha256=data.get("sha256"))
                item.update(frozen)
                try:
                    # Whole evidence remains in its frozen file. Parse only
                    # bounded JSON metadata; large stdout never enters memory
                    # just to infer an optional replay command.
                    if frozen["bytes"] > 2 * 1024 * 1024:
                        raw = {}
                        item["command_metadata_unparsed"] = "Evidence exceeds the 2MiB metadata parser guard; inspect the complete frozen file with paged evidence tools."
                    else:
                        from .evidence_access import open_evidence_file
                        with open_evidence_file(self.directory, Path(frozen["frozen_path"]).relative_to(self.directory)) as handle:
                            raw_bytes = handle.read(2 * 1024 * 1024 + 1)
                        if len(raw_bytes) > 2 * 1024 * 1024:
                            raise ValueError("frozen evidence grew beyond the metadata parser guard")
                        if hashlib.sha256(raw_bytes).hexdigest() != frozen["sha256"]:
                            raise ValueError("frozen evidence changed before metadata parsing")
                        raw = json.loads(raw_bytes)
                except (json.JSONDecodeError, UnicodeError):
                    raw = {}
                command = raw.get("command") if isinstance(raw, dict) else None
                if isinstance(command, str):
                    observed.append({"command": command, "event_seq": record["seq"],
                        "sender": record["sender"], "success": data.get("metadata", {}).get("success"),
                        "evidence_ref": str(data["path"]), "tool_member": raw.get("tool_member", record["sender"]),
                        "verified_reproduction": False})
                    script = self._observed_script(command)
                    if script:
                        inferred_paths.append((script, str(raw.get("tool_member") or record["sender"]), "observed_command"))
            except (OSError, ValueError, KeyError, TypeError) as error:
                item["unavailable_reason"] = safe_error(error)
                unavailable.append({"field": "evidence_refs", "reference": data.get("path"), "reason": safe_error(error)})
            evidence.append(item)
        paths = [(value, (material["draft"] or {}).get("field_senders", {}).get("script_paths", candidate["sender"]), "member_supplied")
                 for value in draft.get("script_paths", [])] + inferred_paths
        seen = set()
        for value, member, origin in paths:
            try:
                path = self._acquisition_script_path(value, member)
                if str(path) in seen:
                    continue
                seen.add(str(path))
                item = {"requested_path": value, "member": member, "origin": origin,
                        **await self._freeze_acquisition_file(path, destination)}
                scripts.append(item)
            except (OSError, ValueError) as error:
                unavailable.append({"field": "script_paths", "reference": value, "reason": safe_error(error)})
        pending = [key for key in ("entrypoints", "prerequisites", "replay_commands") if key not in draft]
        if not draft.get("replay_commands"):
            pending.append("replay_commands")
        if "script_paths" not in draft and not scripts:
            pending.append("script_paths")
        if not any(item.get("kind") == "tool_execution_result" and "frozen_path" in item for item in evidence):
            pending.append("tool_evidence")
        if material["unresolved_evidence_refs"]:
            pending.append("evidence_refs")
        pending += [item["field"] for item in unavailable]
        return {"schema_version": 1, "record_type": "acquisition_material", "request_id": candidate["request_id"],
            "attempt_id": self.attempt_id, "challenge_code": self.code, "sender": candidate["sender"],
            "sources": candidate["sources"], "flag_sha256": hashlib.sha256(candidate["flag"].encode()).hexdigest(),
            "platform_receipt": candidate["receipt"], "source_cutoff_event": job["cutoff_event"],
            "draft_revision": job["draft_revision"], "draft_sender": (material["draft"] or {}).get("sender"),
            "field_senders": (material["draft"] or {}).get("field_senders", {}),
            "entrypoints": draft.get("entrypoints"), "prerequisites": draft.get("prerequisites"),
            "replay_commands": draft.get("replay_commands"), "notes": draft.get("notes", ""),
            "supplied_challenge_entrypoints": {key: material["context"].get(key) for key in ("addresses", "challenge")},
            "observed_commands": observed, "scripts": scripts, "tool_evidence": evidence,
            "unresolved_evidence_refs": material["unresolved_evidence_refs"], "unavailable": unavailable,
            "pending_fields": sorted(set(pending)), "material_complete": not pending,
            "fresh_reproduction": False, "replay_executed": False,
            "limitation": "Historical acquisition material only. Commands and scripts are unverified replay suggestions; reviewer must obtain this review's fresh acquisition evidence."}

    async def _acquisition_loop(self) -> None:
        while not self.stopping.is_set():
            jobs = self.room.acquisition_jobs()
            if not jobs:
                return
            for job in jobs:
                try:
                    material = self.room.acquisition_material(job)
                    manifest = self.room.save_acquisition(job, await self._build_acquisition(material))
                    self.room.notice("runtime", json.dumps({"type": "acquisition_saved", "request_id": job["request_id"],
                        "version": manifest["version"], "manifest_path": manifest["manifest_path"],
                        "pending_fields": manifest["pending_fields"], "fresh_reproduction": False,
                        "instruction": "Ask the finding member to fill missing acquisition details; selectively broadcast reusable findings."},
                        ensure_ascii=False), evidence_refs=[manifest["manifest_path"]],
                        message_id=f"acquisition:{job['request_id']}:{manifest['version']}")
                    if "coordinator" in self.slots:
                        self.slots["coordinator"].wake.set()
                except StaleAcquisitionJob:
                    # A supplement arrived while an older copy was running.
                    # Leave the newer durable job pending and re-read its input.
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.room.fail_acquisition(job, safe_error(error))
                await asyncio.sleep(0)

    async def _publish_report(self, slot: VeriaSlot, arguments: dict) -> dict:
        if slot.role != "reporter" or not slot.batch:
            raise ValueError("only the active reporter batch can publish")
        if slot.published_in_turn:
            return {"published": True, "duplicate": True, **self.room.latest_report()}
        report = self.room.publish_report(arguments, seen_through=slot.batch["through_event"],
                                          batch_id=slot.batch["batch_id"])
        slot.published_in_turn = True
        await self.emit("report_published", source="reporter", report_version=report["report_version"],
                        through_event=report["through_event"], report_path=str(self.directory / "report.md"),
                        report_mode="reporter_snapshot", evidence_watermark=self.room.evidence_watermark)
        return {"published": True, **report}

    def _new_review_schedule(self, started_at: float) -> dict:
        total = float(self.settings.get("veria_review_budget_seconds", 1800))
        reproduction = min(float(self.settings.get("veria_review_reproduction_seconds", 600)), total)
        reserve = min(float(self.settings.get("veria_review_final_seconds", 120)), total)
        return {"deadline": started_at + total,
                "reproduction_deadline": started_at + min(reproduction, max(0, total - reserve)),
                "coverage_deadline": started_at + max(0, total - reserve)}

    def _review_schedule(self, slot: VeriaSlot) -> dict:
        review = self.room.review(slot.review_id) or {}
        request = review.get("request", {})
        # Older runs have started_at but no phase times: derive once from their
        # original start, never from the time of a recovery or the next turn.
        schedule = self.room.meta("review_schedule:" + str(slot.review_id))
        if schedule is None:
            schedule = {**self._new_review_schedule(float(request.get("started_at", review.get("created_at", time.time())))),
                        **{key: request[key] for key in ("deadline", "reproduction_deadline", "coverage_deadline") if key in request}}
            competition = self.room.meta("context", {}).get("deadline")
            if competition is not None:
                schedule["deadline"] = min(schedule["deadline"], float(competition))
                schedule["reproduction_deadline"] = min(schedule["reproduction_deadline"], schedule["deadline"])
                schedule["coverage_deadline"] = min(schedule["coverage_deadline"], schedule["deadline"])
            self.room.set_meta("review_schedule:" + str(slot.review_id), schedule)
        return schedule

    def _review_next_boundary(self, slot: VeriaSlot) -> float:
        schedule = self._review_schedule(slot)
        review = self.room.review(slot.review_id) or {}
        if review.get("status") != "running":
            return time.time() + 3600
        phase = review.get("phase", "reproduction")
        return schedule[{"reproduction": "reproduction_deadline", "coverage": "coverage_deadline"}.get(phase, "deadline")]

    def _save_reproduction_checkpoint(self, slot: VeriaSlot, reproductions: list, *, automatic: bool = False) -> dict:
        key = "review_reproductions:" + slot.review_id
        old = self.room.meta(key)
        if old and old["reproductions"] == reproductions:
            return old
        if old:
            raise ValueError("a reproduction checkpoint is immutable; start a new review for changed paths")
        checkpoint = {"review_id": slot.review_id, "reproductions": reproductions,
            "status": "not_applicable" if not reproductions else "recorded", "automatic": automatic}
        checkpoint["event_seq"] = self.room.event("review_reproductions_recorded", checkpoint, "reviewer")
        self.room.set_meta(key, checkpoint)
        self.room.review_progress(slot.review_id, phase="coverage")
        return checkpoint

    def _advance_review_phase(self, slot: VeriaSlot, *, interrupt_on_change: bool = False) -> None:
        review = self.room.review(slot.review_id) or {}
        if review.get("status") != "running":
            return
        schedule = self._review_schedule(slot)
        now = time.time()
        if now >= schedule["deadline"]:
            raise ReviewDeadlineReached("review total deadline reached; retries do not extend the original 30-minute budget")
        checkpoint = review.get("reproduction_checkpoint")
        changed = False
        known = review.get("request", {}).get("confirmed_submissions", self.room.confirmed_submissions())
        if not checkpoint and (not known or now >= schedule["reproduction_deadline"]):
            partial = self.room.meta("review_partial_reproductions:" + slot.review_id, [])
            by_id = {item["request_id"]: item for item in partial}
            for candidate in known:
                by_id.setdefault(candidate["request_id"], {"request_id": candidate["request_id"],
                    "status": "unchecked", "reason": "reproduction phase deadline reached before this path was verified", "evidence_refs": []})
            self._save_reproduction_checkpoint(slot, list(by_id.values()), automatic=True)
            changed = bool(known)
        current = self.room.review(slot.review_id) or {}
        if current.get("reproduction_checkpoint") and now >= schedule["coverage_deadline"] and current.get("phase") != "finalizing":
            self.room.review_progress(slot.review_id, phase="finalizing")
            changed = True
        elif not current.get("phase"):
            self.room.review_progress(slot.review_id, phase="reproduction")
        if changed and interrupt_on_change:
            raise ReviewPhaseChanged("review phase deadline reached; resume the next bounded phase")

    async def _record_review_progress(self, slot: VeriaSlot, arguments: dict) -> dict:
        if slot.role != "reviewer" or not slot.review_id:
            raise ValueError("only the active reviewer records phase evidence")
        self._advance_review_phase(slot)
        if "reproductions" in arguments:
            partial = self._validated_reproductions(slot, arguments["reproductions"], require_all=False)
            previous = self.room.meta("review_partial_reproductions:" + slot.review_id, [])
            merged = {item["request_id"]: item for item in [*previous, *partial]}
            self.room.set_meta("review_partial_reproductions:" + slot.review_id, list(merged.values()))
        sections = arguments.get("sections", {})
        references = arguments.get("audit_evidence_refs", [])
        if not isinstance(sections, dict) or not set(sections) <= REVIEW_SECTIONS:
            raise ValueError("review progress sections must use the five named coverage sections")
        checkpoint = self.room.meta("review_reproductions:" + slot.review_id)
        if sections or references:
            if not checkpoint:
                raise ValueError("record the reproduction checkpoint before coverage phase evidence")
            if len(self._review_tool_records(slot, references, after=checkpoint["event_seq"])) != len(references):
                raise ValueError("phase evidence must be this review's executions after the reproduction checkpoint")
            if any(not value for value in sections.values()):
                raise ValueError("each persisted review section requires findings or an explicit limitation")
        phase = (self.room.review(slot.review_id) or {}).get("phase", "reproduction")
        return self.room.review_progress(slot.review_id, phase=phase, sections=sections, evidence_refs=references)

    def _review_context(self) -> dict:
        request = self.room.meta("review_request", {})
        return {**request, "report": self.room.latest_report(),
                "platform": self.room.meta("context", {}).get("platform", {}),
                "confirmed_submissions": request.get("confirmed_submissions", self.room.confirmed_submissions()),
                "latest_confirmed_submissions": self.room.confirmed_submissions(),
                "acquisition_records": self.room.acquisition_summaries(),
                "members": self.room.agents(), "previous_review": self.room.meta("last_review"),
                "reproduction_checkpoint": self.room.meta("review_reproductions:" + str(request.get("review_id", ""))),
                "phase_progress": self.room.meta("review_progress:" + str(request.get("review_id", "")), {}),
                "schedule": self.room.meta("review_schedule:" + str(request.get("review_id", "")), {})}

    def _launch_review(self, request: dict) -> VeriaSlot:
        review_id = request["review_id"]
        self.room.register_member("reviewer", "reviewer", self.models["reviewer"],
            workspace=str(self.directory / "agents" / "reviewer"),
            run_id=str(self.room.meta("context", {}).get("run_id") or
                       self.room.meta("context", {}).get("nonce") or self.run_dir.name))
        slot = self._launch("reviewer", "reviewer", review_id=review_id)
        try:
            self._advance_review_phase(slot)
        except ReviewDeadlineReached as error:
            self.room.finish_review(review_id, "timed_out", str(error))
        slot.wake.set()
        return slot

    def _review_tool_records(self, slot: VeriaSlot, references: list, *, after: int = 0) -> list[dict]:
        if not isinstance(references, list):
            raise ValueError("evidence_refs must be a list")
        records = [self.room.evidence_record(reference) for reference in references]
        return [record for record in records if record and record["sender"] == "reviewer" and
            record["seq"] > after and record["data"].get("kind") == "tool_execution_result" and
            record["data"].get("metadata", {}).get("review_id") == slot.review_id]

    def _validated_reproductions(self, slot: VeriaSlot, reproductions, *, require_all: bool = True) -> list[dict]:
        if not isinstance(reproductions, list) or any(not isinstance(item, dict) for item in reproductions):
            raise ValueError("review reproductions must be a list of objects")
        review = self.room.review(slot.review_id) or {}
        known_items = review.get("request", {}).get("confirmed_submissions", self.room.confirmed_submissions())
        known = {item["request_id"]: item for item in known_items}
        recorded = [item.get("request_id") for item in reproductions]
        if len(recorded) != len(set(recorded)):
            raise ValueError("duplicate reproduction request_id")
        if require_all and not known.keys() <= set(recorded):
            raise ValueError("each known flag needs a reproduced/failed/blocked/unchecked record")
        for item in reproductions:
            status = item.get("status")
            if status not in {"reproduced", "failed", "blocked", "unchecked", "not_applicable"}:
                raise ValueError("invalid reproduction status")
            if item.get("request_id") not in known and not (status == "not_applicable" and not known):
                raise ValueError("reproduction request_id must identify one of this review's frozen known flags")
            if item.get("request_id") in known and status == "not_applicable":
                raise ValueError("known flags must be reproduced, failed, blocked or unchecked")
            records = self._review_tool_records(slot, item.get("evidence_refs", []))
            if status == "reproduced":
                successful = [record for record in records if record["data"].get("metadata", {}).get("success") is True]
                if not successful:
                    raise ValueError("reproduction requires this review's successful tool result evidence")
                candidate = known.get(item.get("request_id"))
                if not candidate or not str(item.get("acquisition_steps", "")).strip():
                    raise ValueError("reproduction requires a known request_id and acquisition_steps")
                acquired = False
                for record in successful:
                    evidence = record["data"]
                    raw = Path(evidence["path"]).read_bytes()
                    if hashlib.sha256(raw).hexdigest() != evidence["sha256"]:
                        raise ValueError("reproduction evidence content changed after recording")
                    try:
                        output = json.loads(raw)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    # Inspect execution output, not the command string; an echo
                    # of the old value or unrelated success is not reproduction.
                    if isinstance(output, dict) and candidate["flag"] not in str(output.get("command", "")):
                        acquired |= candidate["flag"] in json.dumps(output.get("result"), ensure_ascii=False)
                if not acquired:
                    raise ValueError("reproduction must show the known flag in fresh acquisition output, not a copied command value")
            elif status in {"failed", "blocked", "unchecked"}:
                if not str(item.get("reason", "")).strip():
                    raise ValueError("failed/blocked/unchecked reproduction requires a concrete reason")
                if status == "failed" and not records:
                    raise ValueError("failed reproduction requires this review's actual attempted tool evidence")
        return reproductions

    async def _record_reproductions(self, slot: VeriaSlot, arguments: dict) -> dict:
        if slot.role != "reviewer" or not slot.review_id:
            raise ValueError("only the active reviewer records reproductions")
        reproductions = self._validated_reproductions(slot, arguments.get("reproductions", []))
        return self._save_reproduction_checkpoint(slot, reproductions)

    async def _complete_review(self, slot: VeriaSlot, arguments: dict) -> dict:
        if slot.role != "reviewer" or not slot.review_id:
            raise ValueError("only the active reviewer can complete this review")
        if slot.published_in_turn:
            return {"completed": True, "duplicate": True, "review_id": slot.review_id}
        reproductions = self._validated_reproductions(slot, arguments.get("reproductions", []))
        checkpoint = self.room.meta("review_reproductions:" + slot.review_id)
        if not checkpoint or checkpoint["reproductions"] != reproductions:
            raise ValueError("call record_reproductions before broader review and complete_review")
        sections = arguments.get("sections", {})
        required = REVIEW_SECTIONS
        if not isinstance(sections, dict) or any(not sections.get(name) for name in required):
            raise ValueError("review sections must cover lateral_opportunities, coverage, missed_routes, prior_rejections, member_health; record unchecked/blocked reasons explicitly")
        references = arguments.get("audit_evidence_refs", [])
        if len(self._review_tool_records(slot, references, after=checkpoint["event_seq"])) != len(references):
            raise ValueError("audit evidence must be this review's tool executions after the reproduction checkpoint")
        if not references:
            def explicit_limitation(value):
                if isinstance(value, dict):
                    return value.get("status") in {"blocked", "unchecked", "not_applicable"} and bool(str(value.get("reason", "")).strip())
                return isinstance(value, str) and bool(re.match(r"^(blocked|unchecked|not_applicable)\s*:\s*\S", value, re.IGNORECASE))
            if not all(explicit_limitation(sections[name]) for name in required):
                raise ValueError("without audit evidence all sections must state blocked/unchecked/not_applicable and a concrete reason")
        await self._record_review_progress(slot, {"sections": sections, "audit_evidence_refs": references})
        result = {**arguments, "review_id": slot.review_id, "completed_at": time.time(),
                  "new_tool_executions": slot.tool_executions}
        evidence = self.room.record_evidence("reviewer", "review_result", result,
                                             metadata={"review_id": slot.review_id})
        result["evidence"] = evidence
        self.room.complete_review(slot.review_id, result)
        self.room.notice("reviewer", json.dumps(result, ensure_ascii=False),
                         message_id="review-complete:" + slot.review_id)
        if "coordinator" in self.slots:
            self.slots["coordinator"].wake.set()
        slot.published_in_turn = True
        await self.emit("review_completed", source="reviewer", review_id=slot.review_id,
                        summary=str(arguments.get("summary", "")), evidence_refs=[evidence])
        return {"completed": True, "review_id": slot.review_id, "evidence": evidence}

    def _read_evidence(self, arguments: dict) -> dict:
        return self._evidence_access(arguments, search=False)

    def _search_evidence(self, arguments: dict) -> dict:
        return self._evidence_access(arguments, search=True)

    def _evidence_access(self, arguments: dict, *, search: bool) -> dict:
        allowed = {"path", "reference", "offset", "max_chars", "expected_sha256"}
        if search:
            allowed.add("query")
        if not isinstance(arguments, dict) or set(arguments) - allowed:
            raise ValueError("unexpected evidence access parameters")
        reference = arguments.get("path") or arguments.get("reference")
        if not isinstance(reference, str) or not reference.strip():
            raise ValueError("evidence path must be a nonempty string")
        path = Path(reference)
        try:
            relative = path.relative_to(self.directory) if path.is_absolute() else path
        except ValueError:
            raise ValueError("evidence must be inside this challenge directory") from None
        if not relative.parts or ".." in relative.parts or is_private_evidence_path(relative):
            raise ValueError("only public challenge evidence files may be read")
        options = {"offset": arguments.get("offset", 0), "max_chars": arguments.get("max_chars", 12000),
                   "expected_sha256": arguments.get("expected_sha256")}
        if search:
            result = search_evidence_page(self.directory, relative, query=arguments.get("query"), **options)
        else:
            result = read_evidence_page(self.directory, relative, **options)
        return {**result, "path": str(self.directory / relative)}

    @staticmethod
    def _acquisition_response(value: dict) -> dict:
        """Keep large manifests on disk, explicitly reachable through paging."""
        manifest = value.get("manifest")
        if not manifest or len(json.dumps(manifest, ensure_ascii=False).encode()) <= 64 * 1024:
            return value
        summary = {key: manifest[key] for key in ("request_id", "version", "manifest_path", "fingerprint",
            "pending_fields", "material_complete", "fresh_reproduction", "replay_executed") if key in manifest}
        return {**value, "manifest": summary, "manifest_inline": False,
                "note": "Complete manifest retained at manifest_path. Use read_evidence/search_evidence to inspect it; this index is not the full material."}

    async def tool(self, slot: VeriaSlot, name: str, arguments: dict) -> dict:
        if name in {"check_messages", "get_messages"}:
            messages = self._messages(slot)
            return {"messages": messages, "additionalContext": json.dumps(messages, ensure_ascii=False)}
        if name in {"ack_messages", "ack_message"}:
            ids = arguments.get("message_ids") or [arguments.get("message_id")]
            return {"acknowledged": self.room.acknowledge_messages(slot.agent_id,
                [str(item) for item in ids if item], evidence=str(arguments.get("evidence", "")))}
        if name == "submit_flag":
            if slot.role == "reporter":
                raise ValueError("reporter does not submit flags")
            return await self._submit(slot, arguments)
        if name == "record_acquisition":
            if slot.role not in {"solver", "coordinator", "reviewer"}:
                raise ValueError("only solver, coordinator or reviewer may record acquisition details")
            request_id = arguments.get("request_id")
            if not isinstance(request_id, str) or not request_id.strip():
                raise ValueError("request_id must be a nonempty string")
            fields = {key: value for key, value in arguments.items() if key != "request_id"}
            # Review workspaces are generation-specific, whereas the durable
            # sender remains the logical reviewer. Freeze relative paths against
            # this invocation's actual tool directory before it is retired.
            if slot.role == "reviewer" and slot.review_id and isinstance(fields.get("script_paths"), list):
                tool_member = "reviewer-" + hashlib.sha256(str(slot.review_id).encode()).hexdigest()[:16]
                fields["script_paths"] = [str(self._acquisition_script_path(value, tool_member))
                    if isinstance(value, str) and not Path(value).is_absolute() else value
                    for value in fields["script_paths"]]
            result = self.room.record_acquisition(slot.agent_id, request_id, fields)
            self._schedule_acquisitions()
            return self._acquisition_response(result)
        if name == "read_acquisition":
            if slot.role not in {"coordinator", "reporter", "reviewer"}:
                raise ValueError("only support roles read other acquisition records")
            if set(arguments) - {"request_id", "version"}:
                raise ValueError("read_acquisition accepts only request_id and version")
            if "version" in arguments and (isinstance(arguments["version"], bool)
                    or not isinstance(arguments["version"], int) or arguments["version"] < 1):
                raise ValueError("version must be a positive integer")
            if "request_id" in arguments:
                if not isinstance(arguments["request_id"], str) or not arguments["request_id"].strip():
                    raise ValueError("request_id must be a nonempty string")
                return self._acquisition_response(self.room.acquisition_status(arguments["request_id"], version=arguments.get("version")))
            if "version" in arguments:
                raise ValueError("version requires request_id")
            return {"acquisition_records": self.room.acquisition_summaries()}
        if name == "notify_coordinator":
            message = self.room.notice(slot.agent_id, str(arguments.get("message", arguments.get("body", ""))),
                evidence_refs=arguments.get("evidence_refs"), message_id=arguments.get("message_id"))
            self.slots["coordinator"].wake.set()
            return {"message_id": message["id"], "state": "saved"}
        if name == "broadcast":
            if slot.role != "coordinator":
                raise ValueError("only coordinator may broadcast technical guidance")
            recipients = list(self.solver_ids)
            reviewer = self.slots.get("reviewer")
            if reviewer and reviewer.task and not reviewer.task.done():
                recipients.append("reviewer")
            return self.room.broadcast(slot.agent_id, str(arguments.get("message", arguments.get("body", ""))),
                recipients=recipients, evidence_refs=arguments.get("evidence_refs"),
                message_id=arguments.get("message_id"), reply_to=arguments.get("reply_to"),
                source_message_ids=arguments.get("source_message_ids"))
        if name == "bump_agent":
            if slot.role != "coordinator":
                raise ValueError("only coordinator may queue targeted guidance")
            target = str(arguments.get("solver_id", ""))
            if target not in self.solver_ids:
                raise ValueError("bump target must be one of this team's configured solver IDs")
            return self.room.queue_bump(target, str(arguments.get("insights", "")),
                                        bump_id=arguments.get("bump_id"))
        if name in {"get_solve_status", "check_swarm_status"}:
            return {"snapshot": self.room.snapshot(), "pending_bumps": {
                agent: self.room.pending_bumps(agent) for agent in self.solver_ids}}
        if name == "read_solver_trace":
            target = str(arguments.get("solver_id", ""))
            if target not in self.solver_ids:
                raise ValueError("trace target must be a managed solver ID")
            events = self.room.events(after=max(0, int(arguments.get("after", 0))), limit=100)
            return {"solver_id": target, "events": [e for e in events if e["sender"] == target],
                    "scanned_through": events[-1]["seq"] if events else arguments.get("after", 0)}
        if name == "read_evidence":
            return await asyncio.to_thread(self._read_evidence, arguments)
        if name == "search_evidence":
            return await asyncio.to_thread(self._search_evidence, arguments)
        if name == "get_report":
            return {"report": self.room.latest_report(), "platform": self.room.meta("context", {}).get("platform", {}),
                    "acquisition_records": self.room.acquisition_summaries(),
                    "batch": slot.batch, "review_context": self._review_context() if slot.role == "reviewer" else None}
        if name == "publish_report":
            return await self._publish_report(slot, arguments)
        if name == "record_reproductions":
            return await self._record_reproductions(slot, arguments)
        if name == "record_review_progress":
            return await self._record_review_progress(slot, arguments)
        if name == "complete_review":
            return await self._complete_review(slot, arguments)
        if name == "significant_evidence":
            if slot.role not in {"coordinator", "reporter", "reviewer"}:
                raise ValueError("only coordinator, reporter or reviewer escalates significant evidence")
            source = str(arguments.get("source_event_id") or arguments.get("evidence_id") or "")
            if not source:
                raise ValueError("significant evidence requires an existing source identity")
            source_record = self.room.evidence_record(source)
            from .veria.state import REPORTABLE_EVENTS
            if (not source_record or source_record["sender"] == "reporter" or
                    source_record["type"] not in REPORTABLE_EVENTS or
                    source_record["data"].get("reportable") is False):
                raise ValueError("significant evidence must reference existing original reportable material")
            source = str(source_record["seq"])
            seen = self.room.meta("significant_evidence_ids", [])
            if source in seen:
                return {"recorded": True, "duplicate": True}
            self.room.set_meta("significant_evidence_ids", [*seen, source])
            await self.emit("significant_evidence", source=slot.agent_id, evidence_id=source,
                            summary=str(arguments.get("summary", "")), evidence_refs=arguments.get("evidence_refs", []))
            return {"recorded": True}
        raise ValueError(f"unknown or forbidden Veria tool: {name}")

    async def _heartbeat(self) -> None:
        while not self.stopping.is_set():
            try:
                environment = await asyncio.wait_for(self.environment.health(), timeout=3)
            except Exception as error:
                environment = {"healthy": False, "error": safe_error(error)}
            agents, failures = self._role_health()
            report = self.room.latest_report()
            env_ok = environment.get("healthy", environment.get("running", True))
            self._environment_failures = 0 if env_ok else self._environment_failures + 1
            await self.emit("heartbeat", source="runtime", pid=os.getpid(), team_engine="veria",
                preparing_stop=self.preparing_stop, roles=agents, environment=environment,
                models=self.models, providers=self.providers, solver_ids=list(self.solver_ids),
                healthy=env_ok and not failures, degraded_roles=failures,
                degraded_reason="tool environment unavailable" if not env_ok else ",".join(failures),
                report_mode="reporter_snapshot", report_version=report["report_version"],
                through_event=report["through_event"], evidence_watermark=self.room.evidence_watermark)
            if self._environment_failures == 2:
                await self.emit("environment_fault", source="runtime", needs_restart=True,
                                reason="tool environment unavailable on consecutive health checks")
            self.room.publish_snapshot()
            await self._delay(min(5, float(self.settings.get("heartbeat_timeout", 30)) / 3))

    async def command(self, command: str, payload: dict) -> dict:
        if payload.get("attempt_id", self.attempt_id) != self.attempt_id:
            raise ValueError("stale attempt command")
        if command == "start":
            return await self.start(payload)
        if command == "stop":
            self._preserve_environment = bool(payload.get("preserve_environment", False))
            self.stopping.set()
            self._work_enabled.set()
            return {"stopping": True}
        if not self.started:
            raise RuntimeError("start the Veria team before issuing commands")
        if payload.get("attempt_id", self.attempt_id) != self.attempt_id:
            raise ValueError("stale attempt command")
        if command in {"snapshot", "audit_snapshot", "request_report"}:
            self._update_platform(payload.get("platform"))
            if command == "request_report":
                self.room.request_audit_report(str(payload["request_id"]), int(payload["round_id"]))
                return self.room.audit_report_status(str(payload["request_id"]), allow_delta=payload.get("allow_delta", True))
            if command == "audit_snapshot":
                snapshot = self.room.audit_snapshot(payload.get("request_id"), allow_delta=payload.get("allow_delta", True))
                if payload.get("request_id"):
                    snapshot["report_request"] = self.room.audit_report_status(str(payload["request_id"]), allow_delta=payload.get("allow_delta", True))
                return snapshot
            return self.room.publish_snapshot()
        if command == "report_status":
            return self.room.audit_report_status(str(payload["request_id"]), allow_delta=payload.get("allow_delta", True))
        if command == "submission_result":
            return await self._receive_receipt(payload)
        if command == "message":
            recipient = str(payload.get("recipient", "coordinator"))
            recipient = "coordinator" if recipient == "leader" else recipient
            if recipient not in self.models:
                raise ValueError("unknown recipient")
            if recipient == "coordinator":
                message = self.room.notice("host", str(payload.get("body", "")),
                    message_id=payload.get("message_id"), evidence_refs=payload.get("evidence_refs"))
            else:
                message = self.room.broadcast("host", str(payload.get("body", "")), recipients=[recipient],
                    message_id=payload.get("message_id"), evidence_refs=payload.get("evidence_refs"))
            if recipient in self.slots and recipient != "reporter":
                self.slots[recipient].wake.set()
            return {"message_id": message["id"], "state": "saved", "recipient": recipient}
        if command == "review":
            self._update_platform(payload.get("platform"))
            slot = self.slots.get("reviewer")
            if slot and slot.task and not slot.task.done():
                return {"started": False, "already_running": True, "review_id": slot.review_id}
            review_id = str(payload.get("review_id") or "review-" + uuid.uuid4().hex)
            previous = self.room.meta("review_request") or {}
            if previous.get("review_id") == review_id and self.room.meta("last_review", {}).get("review_id") == review_id:
                return {"started": False, "completed": True, "review_id": review_id}
            saved_review = self.room.review(review_id)
            if saved_review and saved_review["status"] != "running":
                return {"started": False, "review_id": review_id, "status": saved_review["status"],
                        "completed": saved_review["status"] == "completed"}
            request = saved_review["request"] if saved_review else {
                "review_id": review_id, "reason": payload.get("reason", "30分钟有效作答无新flag"),
                "started_at": time.time(), "previous_review": self.room.meta("last_review"),
                "confirmed_submissions": self.room.confirmed_submissions()}
            if not saved_review:
                request.update(self._new_review_schedule(request["started_at"]))
            if previous.get("review_id") != review_id and self.room.agent("reviewer"):
                self.room.reset_session("reviewer", reason="new_review")
            self.room.start_review(review_id, request)
            self.room.set_meta("review_request", request)
            self._launch_review(request)
            return {"started": True, **request}
        if command == "cancel_review":
            slot = self.slots.get("reviewer")
            if not slot or not slot.review_id:
                return {"cancelled": False, "reason": "no reviewer is active"}
            review = self.room.finish_review(slot.review_id, "cancelled", str(payload.get("reason") or "supervisor ended this partial review"))
            if slot.task and not slot.task.done():
                slot.task.cancel()
                await asyncio.gather(slot.task, return_exceptions=True)
            self.room.update_agent("reviewer", status="cancelled")
            return {"cancelled": review["status"] == "cancelled", "review": review}
        if command == "recover_members":
            reason = str(payload.get("reason", "")).strip()
            if not reason:
                raise ValueError("controlled member recovery requires a reason")
            request_id = str(payload.get("request_id") or "recovery-" + uuid.uuid4().hex)
            key = "operator_recovery:" + request_id
            old = self.room.meta(key)
            signature = {"provider": payload.get("provider"), "members": payload.get("members"), "reason": reason}
            if old:
                if old["request"] != signature:
                    raise ValueError("recovery request id reused with different payload")
                return old["receipt"]
            selected = payload.get("members") or list(self.models)
            if not isinstance(selected, list) or any(member not in self.models for member in selected):
                raise ValueError("recovery members must be configured member IDs")
            provider = payload.get("provider")
            if provider and provider not in set(self.providers.values()):
                raise ValueError("unknown recovery provider")
            recovered, refused = [], {}
            if self.stopping.is_set() or self.preparing_stop or self._completed():
                raise ValueError("cannot recover members while stopping or after confirmed completion")
            deadline = self.room.meta("context", {}).get("deadline")
            if deadline is not None and time.time() >= deadline:
                raise ValueError("cannot recover members after the original competition deadline")
            for member in selected:
                if provider and self.providers.get(member) != provider:
                    continue
                slot = self.slots.get(member)
                record = self.room.agent(member)
                if not record or record.get("status") != "failed":
                    refused[member] = "only terminal failed members may be explicitly recovered"
                    continue
                if slot and slot.task and not slot.task.done():
                    refused[member] = "role loop has not finished cleanup"
                    continue
                if member == "reviewer":
                    refused[member] = "failed bounded reviews are terminal; request a new review instead"
                    continue
                recovered.append(member)
            receipt = self.room.authorize_recovery(request_id, signature, recovered, refused, deadline)
            for member in recovered:
                restarted = self._launch(member, "solver" if member in self.solver_ids else member)
                restarted.wake.set()
            self.room.publish_snapshot()
            return receipt
        if command == "health":
            environment = await self.environment.health()
            agents, failures = self._role_health()
            env_ok = environment.get("healthy", environment.get("running", True))
            return {"healthy": env_ok and not failures and not self.stopping.is_set(),
                    "responsive": True, "runtime_pid": os.getpid(), "agents": agents, "roles": agents,
                    "core_failures": [] if env_ok else ["tool_environment"],
                    "degraded_roles": failures, "environment": environment,
                    "needs_restart": not env_ok, "recoverable": bool(failures) and env_ok,
                    "team_engine": "veria"}
        if command == "prepare_stop":
            self.preparing_stop = True
            self._work_enabled.clear()
            self.environment.quiesce(True)
            self.room.event("prepare_stop", payload)
            deadline = time.monotonic() + float(self.settings.get("shutdown_timeout", 20))
            while any(slot.running for slot in self.slots.values()) and time.monotonic() < deadline:
                await asyncio.sleep(.05)
            return {"preparing_stop": True, "flushed": not any(s.running for s in self.slots.values()),
                    "in_flight": [s.agent_id for s in self.slots.values() if s.running],
                    "pending_submissions": self.room.pending_candidates(),
                    "snapshot": self.room.publish_snapshot(), "checkpoint": str(self.directory / "snapshot.json")}
        if command == "cancel_stop":
            self.preparing_stop = False
            self.environment.quiesce(False)
            if not self._completed():
                self._work_enabled.set()
            self.room.event("stop_cancelled", payload)
            return {"preparing_stop": False, "snapshot": self.room.publish_snapshot()}
        raise ValueError(f"unknown host command: {command}")

    async def shutdown(self) -> None:
        if self._shutdown_done:
            return
        self._shutdown_done = True
        self.stopping.set()
        self._work_enabled.set()
        tasks = [*self._request_tasks, *self._background,
                 *([self._acquisition_task] if self._acquisition_task else []),
                 *(slot.task for slot in self.slots.values() if slot.task)]
        current = asyncio.current_task()
        tasks = [task for task in tasks if task is not current]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.environment:
            try:
                if self._preserve_environment:
                    results = await asyncio.wait_for(asyncio.gather(*(
                        self.environment.stop_member(agent_id) for agent_id in self.slots),
                        return_exceptions=True),
                        timeout=veria_environment_cleanup_timeout(preserve_environment=True))
                    errors = [safe_error(result) for result in results if isinstance(result, BaseException)]
                    if errors:
                        raise RuntimeError("Member cleanup incomplete: " + "; ".join(errors))
                    self.room.event("environment_retained_for_recovery", {})
                else:
                    await asyncio.wait_for(self.environment.stop(),
                        timeout=veria_environment_cleanup_timeout(preserve_environment=False))
            except Exception as error:
                self.room.event("environment_cleanup_incomplete", {"error": safe_error(error)})
        for record in self.room.agents():
            if record["status"] != "failed":
                self.room.update_agent(record["agent_id"], status="stopped", pid=None)
        self.room.event("runtime_stopped", {"pid": os.getpid(), "team_engine": "veria"})
        self.room.publish_snapshot()
        if self._input_transport:
            self._input_transport.close()
        self.room.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--backend", choices=["claude"], default="claude")
    parser.add_argument("--model")
    parser.add_argument("--settings-json", default="{}")
    args = parser.parse_args()
    runtime = VeriaRuntime(args.run_dir, args.code, args.attempt_id, args.backend, args.model,
                           json.loads(args.settings_json))
    asyncio.run(runtime.serve())


if __name__ == "__main__":
    main()
