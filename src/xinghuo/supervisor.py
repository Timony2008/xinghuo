"""Small orchestration loop. No model is awaited on the supervision path."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import time
import uuid
from dataclasses import replace
from pathlib import Path

from .config import Config, VERIA_STOP_RPC_TIMEOUT
from .core import SubmissionReceipt, completion_confirmed
from .store import Store, RunLock, atomic_json
from .strategy import (DemoStrategy, SDKStrategy, ReviewDecision, ReviewPacketBuilder,
                       ReviewPacketTooLarge, SwitchConfirmation, choose_candidate)


RESERVED = {"starting", "active", "draining", "closing", "unknown"}
PLATFORM_ACTIVE = {"pending", "available", "stop_pending"}


class Supervisor:
    def __init__(self, platform, store: Store, config: Config, *, strategy=None,
                 team_factory=None, clock=time.time, on_lifecycle=None):
        self.platform, self.store, self.config = platform, store, config.validate()
        saved_config = store.get_meta("config")
        if saved_config:
            saved = Config(**saved_config).validate()
            immutable = ("team_engine", "backend", "no_flag_timeout")
            if saved.team_engine == "veria" or config.team_engine == "veria":
                immutable += ("veria_python", "veria_tool_image", "veria_models", "veria_upstream_commit",
                              "veria_team_profile", "veria_solver_ids", "veria_providers", "veria_provider",
                              "veria_tool_cpu_limit", "veria_tool_memory_limit", "veria_tool_network", "veria_environment")
            legacy_provider_compat = ("veria_provider" not in saved_config
                                      and saved.veria_team_profile == "legacy6"
                                      and len(set(saved.veria_providers.values())) == 1)
            def immutable_value(cfg, key):
                value = getattr(cfg, key)
                # Pre-provider legacy saves used a single unambiguous service.
                # Match the now-explicit service without changing any model or
                # member binding. Mixed/ambiguous profiles retain strict checks.
                if (key == "veria_provider" and value is None and legacy_provider_compat
                        and cfg.veria_team_profile == "legacy6"):
                    providers = set(cfg.veria_providers.values())
                    if len(providers) == 1:
                        return next(iter(providers))
                return value
            changed = [key for key in immutable
                       if immutable_value(saved, key) != immutable_value(config, key)]
            if changed:
                raise ValueError("saved run identity/configuration cannot change on resume: " + ", ".join(changed))
        self.clock = clock
        self.on_lifecycle = on_lifecycle
        self.strategy = strategy or (DemoStrategy() if config.backend == "demo" else
            SDKStrategy(store.run_dir / "strategy", timeout=config.strategy_timeout, model=config.model))
        if team_factory is None:
            from .runtime import TeamProcess
            team_factory = TeamProcess
        self.team_factory = team_factory
        self.teams = {}
        self._starting_teams: set[str] = set()
        self._runtime_start_tasks: dict[str, asyncio.Task] = {}
        self._recovering_teams: set[str] = set()
        self.jobs: dict[str, asyncio.Task] = {}
        self.last_heartbeat = {}
        self.stop_requested = False
        self.lock = None
        self.rng = random.Random(config.seed)
        saved = store.get_meta("rng_state")
        if saved:
            self.rng.setstate(self._tuples(saved))
        self.deadline = store.get_meta("deadline")
        self.started_at = store.get_meta("started_at")
        self.work_deadline = store.get_meta("work_deadline")
        self.exit_reason = store.get_meta("exit_reason")
        self.round_id = store.get_meta("round_id", 0)
        self.started = False
        self.shutting_down = False
        self._submit_locks: dict[str, asyncio.Lock] = {}
        self._finish_locks: dict[str, asyncio.Lock] = {}
        self._audit_locks: dict[str, asyncio.Lock] = {}
        self._fill_lock = asyncio.Lock()

    @staticmethod
    def _tuples(value):
        return tuple(Supervisor._tuples(v) for v in value) if isinstance(value, list) else value

    def _job(self, key, coro):
        if self.shutting_down:
            coro.close()
            return
        if key in self.jobs and not self.jobs[key].done():
            coro.close()
            return
        self.jobs[key] = asyncio.create_task(coro, name=key)

    def _reap(self):
        for key, task in list(self.jobs.items()):
            if task.done():
                del self.jobs[key]
                if not task.cancelled() and task.exception():
                    exc = task.exception()
                    self.store.event("job_failed", job=key, error=f"{type(exc).__name__}: {exc}")

    async def _call(self, awaitable):
        async with asyncio.timeout(self.config.operation_timeout):
            return await awaitable

    async def initialize(self):
        self.lock = RunLock(self.store.run_dir / "supervisor.lock")
        now = self.clock()
        resumed = self.deadline is not None
        if self.deadline is None:
            self.started_at = now
            self.deadline = now + self.config.budget
            self.store.set_meta("deadline", self.deadline)
            self.store.set_meta("started_at", now)
            self.store.set_meta("run_id", uuid.uuid4().hex)
            self.store.set_meta("config", self.config.to_dict())
        if self.started_at is None:
            raise ValueError("saved run has a deadline but no started_at")
        if self.work_deadline is None:
            # Older runs only saved the final deadline. Use their original
            # cleanup policy when migrating, not the new resume parameters.
            saved_config = self.store.get_meta("config", {})
            reserve = min(saved_config.get("cleanup_reserve", self.config.cleanup_reserve),
                          max(0., (self.deadline - self.started_at) * .1))
            self.work_deadline = self.deadline - reserve
            self.store.set_meta("work_deadline", self.work_deadline)
        if self.store.get_meta("next_audit") is None:
            self.store.set_meta("next_audit", self.store.get_meta("started_at", now) + self.config.audit_interval)
        self.store.set_meta("status", "running")
        self.exit_reason = None
        self.store.set_meta("exit_reason", None)
        self._lifecycle("initialized", resumed=resumed, status="running")
        await self.reconcile()
        # Downtime is not active solving time. Preserve the accumulated amount.
        for row in self.store.challenges():
            row["last_tick"] = now
            self.store.save_challenge(row)
        self.started = True

    def timing_status(self):
        """Actual persisted schedule; a fresh, uninitialized run has no times."""
        timing = {key: None for key in ("started_at", "deadline", "work_deadline", "budget_seconds",
                  "cleanup_reserve_seconds", "elapsed_seconds", "remaining_seconds",
                  "deadline_remaining_seconds", "work_remaining_seconds")}
        if self.started_at is None or self.deadline is None:
            return timing
        now = self.clock()
        timing.update(started_at=self.started_at, deadline=self.deadline,
                      work_deadline=self.work_deadline,
                      budget_seconds=self.deadline - self.started_at,
                      elapsed_seconds=max(0., now - self.started_at),
                      remaining_seconds=max(0., self.deadline - now),
                      deadline_remaining_seconds=max(0., self.deadline - now))
        if self.work_deadline is not None:
            timing.update(cleanup_reserve_seconds=self.deadline - self.work_deadline,
                          work_remaining_seconds=max(0., self.work_deadline - now))
        return timing

    def _lifecycle(self, event, **fields):
        payload = {"event": event, "run_id": self.store.get_meta("run_id"),
                   **self.timing_status(), **fields}
        self.store.event("supervisor_" + event, **payload)
        if self.on_lifecycle is not None:
            try:
                self.on_lifecycle(payload)
            except Exception as exc:
                # Observability must not change the scheduler's outcome or
                # replace the runtime exception that is being reported.
                self.store.event("lifecycle_callback_failed", lifecycle_event=event,
                                 error=type(exc).__name__)

    def _stopping(self, status, reason):
        self.store.set_meta("status", status)
        self.exit_reason = reason
        self.store.set_meta("exit_reason", reason)
        self._lifecycle("stopping", status=status, exit_reason=reason)

    def _work_allowed(self):
        return (not self.shutting_down and not self.stop_requested
                and self.work_deadline is not None and self.clock() < self.work_deadline)

    async def reconcile(self):
        rows = await self._call(self.platform.list_challenges())
        for challenge in rows:
            incoming = challenge.to_dict()
            code = challenge.code
            row = self.store.challenge(code)
            if row is None:
                row = dict(code=code, challenge=incoming, status="queued", attempt_id=None,
                           revision=0, progress_revision=0, score_revision=0, score=None,
                           active_seconds=0., last_flag_work=0., last_review_work=0.,
                           last_tick=self.clock(), confirmation=None, runtime_restarts=0,
                           last_audit_score_revision=0, last_audit_flag_count=challenge.correct_flag_count)
            old_flags = row["challenge"].get("correct_flag_count", 0)
            # A stale list response must not undo a newer submission receipt.
            incoming["correct_flag_count"] = max(old_flags, challenge.correct_flag_count)
            reported_complete = incoming["completed"]
            incoming["completed"] = bool(reported_complete or completion_confirmed(row["challenge"]))
            incoming["completed"] = completion_confirmed(incoming)
            if reported_complete and not incoming["completed"]:
                self.store.event("completion_inconsistent", code,
                    correct_flag_count=incoming["correct_flag_count"], flag_count=incoming["flag_count"])
            row["challenge"] = incoming
            if (row["status"] == "draining" and code not in self.teams
                    and code not in self._starting_teams and code not in self._recovering_teams
                    and not row.get("close_intent")
                    and not (code in self._finish_locks and self._finish_locks[code].locked())):
                # A process can die after journaling the checkpoint phase.
                # Resume the same attempt/window; the next tick reapplies the
                # hard timeout instead of reserving a dead slot forever.
                row["status"] = "active"
                self.store.event("checkpoint_resumed", code, attempt_id=row["attempt_id"])
            if row["status"] == "completed" and not incoming["completed"]:
                # A corrected total (or invalid legacy state) must not leave an
                # unfinished challenge permanently excluded from selection.
                row["status"] = "paused" if challenge.container_status == "stopped" else "active"
                row["close_intent"] = None
                self.store.event("completion_reopened", code)
            if incoming["correct_flag_count"] > old_flags:
                self._new_flag(row)
            if challenge.container_status in PLATFORM_ACTIVE:
                if row["status"] in {"queued", "paused"}:
                    # A platform may return every scene as already available.
                    # Those scenes do not each receive a Python/SDK team: only
                    # admitted attempts consume a local slot, and the rest
                    # remain selectable without destroying their scenes.
                    if self.reserve_count() < self.config.parallel:
                        row["status"] = "active" if challenge.container_status == "available" else "unknown"
                        row["start_uncertain"] = challenge.container_status != "available"
                        if not row.get("attempt_id"):
                            row["attempt_id"] = uuid.uuid4().hex
                            self._begin_work_window(row)
                        row["last_tick"] = self.clock()
                elif row["status"] in {"starting", "unknown"} and row.get("start_uncertain"):
                    if challenge.container_status == "available":
                        row.update(status="active", start_uncertain=False)
                        self.store.event("start_reconciled", code)
            elif challenge.container_status == "stopped":
                if row["status"] in {"closing", "unknown"} and row.get("close_intent"):
                    row["status"] = "completed" if incoming["completed"] else "paused"
                    row["close_confirmed"] = True
                    self.store.event("closure_confirmed", code, reason=row.get("close_intent"))
                elif row["status"] == "unknown" and row.get("start_uncertain"):
                    # Reconciled not started: a later bounded retry can start this same attempt.
                    row["status"] = "queued"
                    row["start_uncertain"] = False
            self.store.save_challenge(row)
            consistent_complete = completion_confirmed(incoming)
            if consistent_complete and row["status"] in {"queued", "paused"}:
                row["status"] = "completed"
                self.store.save_challenge(row)
            elif consistent_complete and row["status"] in {"active", "starting"}:
                self._job(f"finish:{code}", self.finish(code, "completed"))
            elif (row["status"] == "active" and challenge.container_status == "available" and code not in self.teams
                  and code not in self._starting_teams and code not in self._recovering_teams
                  and row.get("runtime_restarts", 0) <= self.config.max_retries and self._work_allowed()):
                self._job(f"runtime:{code}", self.start_runtime(code))

    def _new_flag(self, row):
        if (self.config.no_flag_timeout and self.started and row["status"] == "active"
                and row.get("work_clock_started")):
            # Receipts and list responses can arrive between ticks. Anchor the
            # fresh window to confirmation time, not the previous tick's work.
            now = self.clock()
            row["active_seconds"] += max(0., now - row["last_tick"])
            row["last_tick"] = now
        row["progress_revision"] += 1
        row["revision"] += 1
        row["last_flag_work"] = row["active_seconds"]
        row["last_review_work"] = row["active_seconds"]
        row["confirmation"] = None
        self.store.event("new_flag_confirmed", row["code"],
                         count=row["challenge"]["correct_flag_count"])

    def eligible(self, excluding=None):
        return [r for r in self.store.challenges()
                if r["code"] != excluding and r["status"] in {"queued", "paused"}
                and not r["challenge"]["completed"]
                and r["challenge"]["container_status"] in {"stopped", "available"}
                and r.get("start_failures", 0) <= self.config.max_retries]

    def reserve_count(self):
        return sum(r["status"] in RESERVED for r in self.store.challenges())

    def _begin_work_window(self, row):
        """A newly admitted attempt gets its own no-flag window and queue turn."""
        order = self.store.get_meta("attempt_order", 0) + 1
        self.store.set_meta("attempt_order", order)
        row["last_started_order"] = order
        row["work_clock_started"] = False
        if self.config.no_flag_timeout:
            row["last_flag_work"] = row["active_seconds"]

    def no_flag_expired(self, row):
        return bool(self.config.no_flag_timeout > 0 and row["status"] == "active"
                    and row.get("work_clock_started")
                    and row["active_seconds"] - row["last_flag_work"] >= self.config.no_flag_timeout
                    and not completion_confirmed(row["challenge"]))

    def check_no_flag_timeouts(self, now=None):
        if not self.config.no_flag_timeout:
            return
        now = self.clock() if now is None else now
        for row in self.store.challenges():
            if row["status"] != "active" or not row.get("work_clock_started"):
                continue
            # This attempt owns a solving slot after its first runtime start.
            # Keep counting recovery gaps, so an exhausted runtime cannot
            # evade the hard timeout. initialize() excludes supervisor downtime.
            row["active_seconds"] += max(0., now - row["last_tick"])
            row["last_tick"] = now
            self.store.save_challenge(row)
            if self.no_flag_expired(row):
                self._job("finish:" + row["code"], self.finish(row["code"], "no_flag_timeout"))

    async def choose(self, operation_id, recommended=None, excluding=None):
        existing = self.store.operation(operation_id)
        if existing:
            return existing["choice"]["selected"]
        candidates = self.eligible(excluding)
        if not candidates:
            return None
        if self.config.no_flag_timeout:
            # The hard timeout is a queue policy, not a model recommendation.
            # Visit untouched questions first, then revisit in rotation order.
            # No strategy latency, epsilon draw or reviewer can delay a slot.
            candidates.sort(key=lambda r: (r.get("attempt_id") is not None,
                                           r.get("last_started_order", 0), r["code"]))
            codes = [r["code"] for r in candidates]
            choice = {"selected": codes[0], "recommended": codes[0], "explored": False,
                      "epsilon": 0., "candidates": codes, "policy": "no_flag_timeout_queue"}
            self.store.save_selection(operation_id, choice["selected"], choice, self.rng.getstate())
            self.store.event("candidate_selected", choice["selected"], operation_id=operation_id, **choice)
            return choice["selected"]
        codes = [r["code"] for r in candidates]
        if recommended not in codes:
            try:
                packet = ReviewPacketBuilder().build({"mode": "select", "challenge": {}}, [], [],
                    [r["challenge"] for r in candidates],
                    {"remaining_seconds": max(0, self.deadline - self.clock())})
                decision = await self.strategy.decide(packet)
                recommended = decision.replacement
                if recommended not in codes:
                    raise ValueError("strategy did not recommend an eligible candidate")
            except Exception as exc:
                recommended = codes[0]
                self.store.event("selection_fallback", error=type(exc).__name__, selected=recommended)
        # No await between random draw, journaled choice and saved RNG state.
        choice = choose_candidate(codes, recommended, epsilon=self.config.epsilon, rng=self.rng)
        self.store.save_selection(operation_id, choice["selected"], choice, self.rng.getstate())
        self.store.event("candidate_selected", choice["selected"], operation_id=operation_id, **choice)
        return choice["selected"]

    async def fill_one(self):
        async with self._fill_lock:
            await self._fill_one()

    async def _fill_one(self):
        if not self._work_allowed() or self.reserve_count() >= self.config.parallel:
            return
        # Reuse an outstanding selection after an interrupted start.
        eligible_codes = {r["code"] for r in self.eligible()}
        selected = next((o for o in self.store.operations("selection") if o["status"] == "chosen"
                         and o["code"] in eligible_codes), None)
        for old in self.store.operations("selection"):
            if old["status"] == "chosen" and old["code"] not in eligible_codes:
                self.store.save_operation(old["id"], "selection", old["code"], "invalidated", choice=old["choice"])
        operation_id = selected["id"] if selected else "fill:" + uuid.uuid4().hex
        code = await self.choose(operation_id)
        if not code or self.reserve_count() >= self.config.parallel:
            return
        await self.start_challenge(code, operation_id)

    async def start_challenge(self, code, selection_id):
        if not self._work_allowed() or self.reserve_count() >= self.config.parallel:
            return
        row = self.store.challenge(code)
        if row["status"] not in {"queued", "paused"}:
            return
        row.update(status="starting", attempt_id=uuid.uuid4().hex, confirmation=None,
                   close_intent=None, close_confirmed=False, start_uncertain=True,
                   runtime_restarts=0, last_tick=self.clock(), significant_evidence_ids=[],
                   pending_review_request=None, review_request_attempts=0, review_request_error=None)
        self._begin_work_window(row)
        row["revision"] += 1
        self.store.save_challenge(row)
        opid = "start:" + row["attempt_id"]
        self.store.save_operation(opid, "start", code, "pending", attempt_id=row["attempt_id"])
        try:
            if row["challenge"]["container_status"] == "available":
                addresses = row["challenge"]["container_addr"]
                self.store.event("existing_scene_admitted", code, attempt_id=row["attempt_id"])
            else:
                addresses = await self._call(self.platform.start(code))
            row = self.store.challenge(code)
            row.update(status="active", start_uncertain=False)
            row["challenge"].update(container_status="available", container_addr=list(addresses))
            self.store.save_challenge(row)
            self.store.save_operation(opid, "start", code, "confirmed", addresses=list(addresses))
            selected = self.store.operation(selection_id)
            if selected:
                self.store.save_operation(selection_id, "selection", code, "used", choice=selected["choice"])
        except Exception as exc:
            row = self.store.challenge(code)
            # Unknown keeps its slot until list_challenges reconciles it.
            row["status"] = "unknown"
            row["start_failures"] = row.get("start_failures", 0) + 1
            self.store.save_challenge(row)
            self.store.save_operation(opid, "start", code, "unknown", error=type(exc).__name__)
            self.store.event("start_uncertain", code, error=type(exc).__name__)
            return
        await self.start_runtime(code)

    async def start_runtime(self, code):
        if not self._work_allowed():
            return
        row = self.store.challenge(code)
        if (code in self.teams or code in self._starting_teams or code in self._recovering_teams
                or row["status"] != "active"):
            return
        if len(set(self.teams) | self._starting_teams | self._recovering_teams) >= self.config.parallel:
            self.store.event("runtime_admission_deferred", code, parallel=self.config.parallel)
            return
        team = self.team_factory(self.store.run_dir, code, row["attempt_id"],
                                 backend=self.config.backend, model=self.config.model,
                                 settings=self.config.to_dict())
        self.teams[code] = team
        self._starting_teams.add(code)
        self._runtime_start_tasks[code] = asyncio.current_task()
        try:
            await team.start({"challenge": row["challenge"], "addresses": row["challenge"]["container_addr"],
                              "platform": self.platform_card(row), "mode": "solve",
                              "rotation_policy": {"mode": "no_flag_timeout" if self.config.no_flag_timeout else "review",
                                                  "timeout_seconds": self.config.no_flag_timeout},
                              "deadline": self.deadline, "nonce": self.store.get_meta("run_id"),
                              "run_id": self.store.get_meta("run_id"), "challenge_code": code,
                              "attempt_id": row["attempt_id"], "team_engine": self.config.team_engine,
                              "recovery": {**{key: row.get(key) for key in
                                  ("runtime_restarts", "active_seconds", "last_flag_work", "last_review_work",
                                   "pending_review_request")},
                                  "submissions": [op for op in self.store.operations("submit")
                                      if op["code"] == code and op.get("attempt_id", row["attempt_id"]) == row["attempt_id"]]}})
            current = self.store.challenge(code)
            if (self.config.no_flag_timeout and self.started and current["status"] == "active"
                    and current.get("work_clock_started")):
                current["active_seconds"] += max(0., self.clock() - current["last_tick"])
            current.update(work_clock_started=True, last_tick=self.clock())
            self.store.save_challenge(current)
            self.last_heartbeat[code] = self.clock()
            self.store.event("runtime_started", code, pid=team.pid, attempt_id=row["attempt_id"],
                             team_engine=self.config.team_engine,
                             team_profile=self.config.veria_team_profile if self.config.team_engine == "veria" else None,
                             solver_ids=self.config.veria_solver_ids if self.config.team_engine == "veria" else [],
                             role_providers=self.config.veria_providers if self.config.team_engine == "veria" else {})
        except BaseException:
            if self.teams.get(code) is team:
                self.teams.pop(code, None)
            await team.stop()
            current = self.store.challenge(code)
            current["runtime_restarts"] += 1
            current["health_error"] = "runtime_start_failed"
            if (self.config.no_flag_timeout and current["status"] == "active"
                    and not current.get("work_clock_started")):
                # Initial startup failures must not reserve a slot forever.
                # Exclude the startup wait itself, then count this failed
                # attempt's bounded recovery/idle window from the first failure.
                current.update(work_clock_started=True, last_tick=self.clock())
            self.store.save_challenge(current)
            raise
        finally:
            self._starting_teams.discard(code)
            self._runtime_start_tasks.pop(code, None)

    @staticmethod
    def platform_card(row):
        return dict(row["challenge"], completed=completion_confirmed(row["challenge"]),
                    cumulative_score=row.get("score"),
                    score_revision=row["score_revision"], progress_revision=row["progress_revision"])

    async def submit(self, code, flag, request_id=None, sender=None, attempt_id=None):
        async with self._submit_locks.setdefault(code, asyncio.Lock()):
            await self._submit(code, flag, request_id, sender, attempt_id)

    async def _submit(self, code, flag, request_id=None, sender=None, attempt_id=None):
        row = self.store.challenge(code)
        if row["challenge"]["completed"] and not completion_confirmed(row["challenge"]):
            row["challenge"]["completed"] = False
            self.store.save_challenge(row)
        attempt_id = attempt_id or row["attempt_id"]
        if attempt_id != row["attempt_id"]:
            self.store.event("stale_submission_rejected", code, attempt_id=attempt_id, request_id=request_id)
            return
        if not isinstance(flag, str) or not flag.strip():
            raise ValueError("candidate flag must be nonempty text")
        if self.config.team_engine == "veria":
            flag = flag.strip()
        identity = (code + "\0" + attempt_id + "\0" + flag if self.config.team_engine == "veria"
                    else code + "\0" + flag)
        opid = "submit:" + hashlib.sha256(identity.encode()).hexdigest()
        existing = self.store.operation(opid)
        attempts = (existing or {}).get("attempts", 0)
        sources = list((existing or {}).get("sources", []))
        source = {"request_id": request_id, "sender": sender}
        if source not in sources:
            sources.append(source)
        canonical_request = (existing or {}).get("request_id") or request_id
        canonical_sender = (existing or {}).get("sender") or sender
        journal = {"flag": flag, "request_id": canonical_request, "sender": canonical_sender,
                   "attempt_id": attempt_id, "sources": sources}
        if existing and existing["status"] in {"confirmed", "received"}:
            receipt = SubmissionReceipt(**existing["receipt"])
        else:
            attempts += 1
            self.store.save_operation(opid, "submit", code, "pending", **journal, attempts=attempts)
            try:
                receipt = await self._call(self.platform.submit(code, flag))
                if not isinstance(receipt, SubmissionReceipt):
                    raise TypeError("platform submit must return SubmissionReceipt")
            except Exception as exc:
                unknown = SubmissionReceipt(unknown=True)
                self.store.save_operation(opid, "submit", code, "unknown", **journal,
                    attempts=attempts, error=type(exc).__name__, receipt=unknown.to_dict())
                self.store.event("submission_uncertain", code, operation_id=opid, error=type(exc).__name__)
                if self.config.team_engine == "veria" and self.store.challenge(code)["attempt_id"] == attempt_id:
                    await self._deliver_receipt(code, request_id, sender, attempt_id, opid, sources, unknown)
                return
            # Journal the exact receipt before changing derived progress. A crash
            # in between is replayed from this receipt, not a duplicate response.
            self.store.save_operation(opid, "submit", code, "received", **journal, attempts=attempts,
                                      receipt=receipt.to_dict())
        if self.store.challenge(code)["attempt_id"] != attempt_id:
            self.store.event("stale_receipt_retained", code, operation_id=opid, attempt_id=attempt_id)
            return
        if not receipt.unknown:
            self._apply_receipt(code, opid, receipt)
        self.store.save_operation(opid, "submit", code, "unknown" if receipt.unknown else "confirmed",
                                  **journal, attempts=attempts,
                                  receipt=receipt.to_dict())
        await self._deliver_receipt(code, request_id, sender, attempt_id, opid, sources, receipt)
        if completion_confirmed(self.store.challenge(code)["challenge"]):
            self._job(f"finish:{code}", self.finish(code, "completed"))

    async def _deliver_receipt(self, code, request_id, sender, attempt_id, opid, sources, receipt):
        if code in self.teams and self.store.challenge(code)["attempt_id"] == attempt_id:
            try:
                await self.teams[code].request("submission_result", {"request_id": request_id,
                    "sender": sender, "attempt_id": attempt_id, "operation_id": opid,
                    "sources": sources, "receipt": receipt.to_dict(),
                    "platform": self.platform_card(self.store.challenge(code))}, timeout=self.config.operation_timeout)
            except Exception as exc:
                self.store.event("receipt_delivery_pending", code, operation_id=opid, error=type(exc).__name__)

    def _apply_receipt(self, code, opid, receipt):
        row = self.store.challenge(code)
        applied = row.setdefault("applied_receipts", [])
        if opid in applied:
            return
        previous_count = row["challenge"]["correct_flag_count"]
        if receipt.correct_flag_count is not None:
            row["challenge"]["correct_flag_count"] = max(previous_count, receipt.correct_flag_count)
        if receipt.total_flag_count is not None:
            row["challenge"]["flag_count"] = receipt.total_flag_count
        if row["challenge"]["correct_flag_count"] > previous_count:
            self._new_flag(row)
        old_score = row.get("score")
        gained = not receipt.duplicate and (receipt.awarded > 0 or
            receipt.cumulative_score is not None and old_score is not None and receipt.cumulative_score > old_score)
        if receipt.cumulative_score is not None:
            row["score"] = max(old_score or 0, receipt.cumulative_score)
        if gained:
            row["score_revision"] += 1
            row["confirmation"] = None
            row["revision"] += 1
            self.store.event("score_gained", code, awarded=receipt.awarded, cumulative_score=row["score"])
        count, total = row["challenge"]["correct_flag_count"], row["challenge"]["flag_count"]
        if self.config.team_engine == "native" and total > 0 and count >= total:
            row["challenge"]["completed"] = True
        # A new total can invalidate an earlier completion bit. Do not let a
        # later (even incorrect) submission consume inconsistent saved state.
        row["challenge"]["completed"] = completion_confirmed(row["challenge"])
        applied.append(opid)
        self.store.save_challenge(row)
        self.store.event("submission_confirmed", code, operation_id=opid, receipt=receipt.to_dict())

    @staticmethod
    def _audit_operation_id(attempt, round_id):
        return f"audit:{attempt}:{round_id}"

    def _save_audit_request(self, operation, status=None, **updates):
        body = {key: value for key, value in operation.items()
                if key not in {"id", "kind", "code", "status"}}
        body.update(updates)
        self.store.save_operation(operation["id"], "audit_request", operation["code"],
                                  status or operation["status"], **body)
        return self.store.operation(operation["id"])

    async def _request_audit_report(self, code, round_id, attempt, team, *, wait=True):
        request_id = f"{attempt}:{round_id}"
        operation_id = self._audit_operation_id(attempt, round_id)
        operation = self.store.operation(operation_id)
        old_status = operation.get("report_status", {}) if operation else {}
        allow_delta = not (operation or {}).get("prefer_reporter", False)
        status = {**old_status, "request_id": request_id, "ready": False}
        if team is None:
            return {**status, "error": "runtime_missing"}
        # request_report is idempotent in both runtimes: repeated calls retain
        # the original required_through_event, never take a fresh cutoff.
        try:
            async with asyncio.timeout(self.config.audit_report_timeout if wait else self.config.operation_timeout):
                status = await team.request("request_report", {"request_id": request_id, "round_id": round_id,
                    "report_mode": "reporter_snapshot" if self.config.team_engine == "veria" else "leader_report",
                    "platform": self.platform_card(self.store.challenge(code)), "allow_delta": allow_delta},
                    timeout=self.config.operation_timeout)
                status = {**status, "request_id": request_id}
                if operation:
                    self._save_audit_request(operation, report_status=status)
                while wait and not status.get("ready"):
                    if status.get("error"):
                        raise RuntimeError(status["error"])
                    await asyncio.sleep(.1)
                    status = await team.request("report_status", {"request_id": request_id, "allow_delta": allow_delta},
                                                timeout=self.config.operation_timeout)
                    status = {**status, "request_id": request_id}
                    if operation:
                        self._save_audit_request(operation, report_status=status)
                if status.get("ready"):
                    self.store.event("audit_report_ready", code, **{**status, "round_id": round_id})
                return status
        except Exception as exc:
            status = {**status, "request_id": request_id, "ready": False,
                      "error": type(exc).__name__, "soft_timeout": isinstance(exc, TimeoutError)}
            if operation:
                self._save_audit_request(operation, report_status=status)
            self.store.event("audit_report_unavailable", code, **{**status, "round_id": round_id})
            return status

    async def resume_pending_audits(self, code=None):
        """Resume original requests and committed-but-unexecuted switches."""
        if self.config.no_flag_timeout:
            return
        requests = sorted(self.store.operations("audit_request"), key=lambda item: item["round_id"])
        for operation in requests:
            if code is not None and operation["code"] != code:
                continue
            row = self.store.challenge(operation["code"])
            if (not row or row["status"] != "active"
                    or row["attempt_id"] != operation["attempt_id"]):
                if operation["status"] == "pending":
                    self._save_audit_request(operation, "superseded", reason="attempt_inactive")
                continue
            confirmation = row.get("confirmation") or {}
            committed_switch = (operation["status"] == "applied" and confirmation.get("execute")
                                and confirmation.get("round_id") == operation["round_id"]
                                and row["revision"] == operation["revision"])
            if operation["status"] == "pending" or committed_switch:
                # _audit_one's applied branch only resumes guarded execution;
                # no model call or round count is repeated after a crash.
                await self.audit_one(operation["code"], operation["round_id"], wait_for_report=False)

    def _defer_audit(self, code, operation, row, report_status, *, reason, break_confirmation=True,
                     material_error=None):
        updates = {"report_status": report_status, "deferred_at": self.clock()}
        if material_error:
            updates.update(prefer_reporter=True, last_material_error=material_error)
        operation = self._save_audit_request(operation, **updates)
        if break_confirmation:
            if "confirmation_before_wait" not in operation:
                operation = self._save_audit_request(operation, confirmation_before_wait=row.get("confirmation"))
            row["confirmation"] = SwitchConfirmation.update(row.get("confirmation"),
                ReviewDecision("need_info", "本轮固定截点材料待补齐；等待为软超时，迟到后继续原轮次。"),
                round_id=operation["round_id"], attempt_id=operation["attempt_id"],
                score_revision=row["progress_revision"])
            row["confirmation"]["provisional"] = True
            self.store.save_challenge(row)
        self.store.event("audit_deferred", code, round_id=operation["round_id"],
                         request_id=operation["request_id"], reason=reason,
                         material_error=material_error)

    async def _audit_wait_followup(self, code, team, operation_id, snapshot, material_error=None):
        if not team or not team.alive:
            return
        operation = self.store.operation(operation_id)
        signature = {key: snapshot.get(key) for key in
                     ("report_mode", "report_version", "required_through_event", "through_event", "snapshot_error")}
        signature["material_error"] = material_error
        fingerprint = hashlib.sha256(json.dumps(signature, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        if operation.get("wait_followup_fingerprint") == fingerprint:
            return
        body = (f"审核轮次{operation['round_id']}仍等待原固定截点的完整材料；"
                "补齐后会接回原轮次，不另起截点。" +
                ("完整增量超过材料保护，请继续自主发布覆盖该截点的完整reporter报告。"
                 if material_error else "请核对报告版本、固定截点及完整增量覆盖。"))
        try:
            await team.request("message", {"recipient": "reporter" if self.config.team_engine == "veria" else "leader",
                                           "body": body}, timeout=self.config.operation_timeout)
            self._save_audit_request(self.store.operation(operation_id), wait_followup_fingerprint=fingerprint)
        except Exception as exc:
            self.store.event("audit_followup_pending", code, error=type(exc).__name__)

    async def _audit_health(self, code, team):
        now = self.clock()
        heartbeat = self.last_heartbeat.get(code)
        health = {"supervisor_pid": os.getpid(), "checked_at": now,
                  "runtime_alive": bool(team and team.alive),
                  "heartbeat_healthy": heartbeat is not None and now - heartbeat <= self.config.heartbeat_timeout}
        try:
            if not health["runtime_alive"]:
                raise RuntimeError("runtime not running")
            probe = await team.request("health", {}, timeout=self.config.operation_timeout)
            if probe.get("error"):
                raise RuntimeError(probe["error"])
            health["runtime"] = probe
            health["healthy"] = probe.get("healthy") is True and health["heartbeat_healthy"]
        except Exception as exc:
            health.update(healthy=False, probe_error=type(exc).__name__)
        current = self.store.challenge(code)
        if self.config.team_engine == "veria":
            health_path = self.store.run_dir / "health" / code / str(current["attempt_id"]) / (uuid.uuid4().hex + ".json")
            atomic_json(health_path, health)
            health["evidence_refs"] = [str(health_path)]
        current["last_audit_health"] = health
        self.store.save_challenge(current)
        self.store.event("audit_health_checked", code, health=health)
        if (not health["runtime_alive"] or health.get("probe_error") or not health["heartbeat_healthy"]
                or health.get("runtime", {}).get("needs_restart") is True):
            if current["status"] == "active" and current.get("runtime_restarts", 0) <= self.config.max_retries:
                self._job("recover:" + code, self.recover_runtime(code))
        return health

    def _health_material(self, health):
        if self.config.team_engine != "veria" or not isinstance(health.get("runtime"), dict):
            return health
        runtime = health["runtime"]
        probe = {key: runtime[key] for key in ("healthy", "responsive", "runtime_pid", "needs_restart",
                 "recoverable", "team_engine", "core_failures", "degraded_roles", "error") if key in runtime}
        roles = runtime.get("roles", runtime.get("agents", []))
        probe["roles"] = [{key: role[key] for key in ("agent_id", "role", "model_id", "provider", "status", "running",
            "client_initialized", "client_alive", "restarts", "degraded_reason", "last_error") if key in role}
            for role in roles if isinstance(role, dict)]
        if isinstance(runtime.get("environment"), dict):
            probe["environment"] = {key: runtime["environment"][key] for key in ("healthy", "running",
                "container_id", "image_id", "architecture", "network_mode", "volatile_state_lost",
                "recovered", "reason", "error") if key in runtime["environment"]}
        return {**health, "runtime": probe,
                "detail_note": "角色健康采用紧凑卡片；完整成员和环境探测见本次不可变健康证据引用。"}

    async def audit_one(self, code, round_id, *, wait_for_report=True):
        if self.config.no_flag_timeout:
            return
        async with self._audit_locks.setdefault(code, asyncio.Lock()):
            await self._audit_one(code, round_id, wait_for_report=wait_for_report)

    async def _audit_one(self, code, round_id, *, wait_for_report=True):
        row = self.store.challenge(code)
        if row["status"] != "active":
            return
        attempt = row["attempt_id"]
        operation_id = self._audit_operation_id(attempt, round_id)
        operation = self.store.operation(operation_id)
        if operation and operation["status"] == "applied":
            # A crash may have happened after committing the decision but before
            # journaled switch/selection execution. Replaying never calls a model
            # or counts the round again.
            confirmation = row.get("confirmation") or {}
            if (confirmation.get("execute") and confirmation.get("round_id") == round_id
                    and row["revision"] == operation["revision"]):
                await self.switch(code, None, expected_revision=operation["revision"])
            return
        if operation and operation["status"] == "superseded":
            return
        if operation is None:
            self.store.save_operation(operation_id, "audit_request", code, "pending",
                attempt_id=attempt, round_id=round_id, revision=row["revision"],
                score_at_start=row["score_revision"], created_at=self.clock(),
                request_id=f"{attempt}:{round_id}")
            operation = self.store.operation(operation_id)
            self.store.event("audit_report_requested", code, round_id=round_id,
                             request_id=operation["request_id"])
        revision, score_at_start = operation["revision"], operation["score_at_start"]
        team = self.teams.get(code)
        report_request = await self._request_audit_report(code, round_id, attempt, team,
                                                        wait=wait_for_report)
        # Technical health remains checked even when the reporter soft wait
        # expires. Incomplete reports are deferred, not final audit decisions.
        health = await self._audit_health(code, team)
        row = self.store.challenge(code)
        if row["attempt_id"] != attempt or row["status"] != "active":
            self._save_audit_request(self.store.operation(operation_id), "superseded", reason="attempt_inactive")
            return
        confirmation = row.get("confirmation") or {}
        if confirmation.get("round_id", -1) > round_id:
            self._save_audit_request(self.store.operation(operation_id), "superseded", reason="newer_round_applied")
            return
        gained = row["score_revision"] > row.get("last_audit_score_revision", 0)
        snapshot = {"challenge": self.platform_card(row)}
        if team and team.alive:
            try:
                snapshot.update(await team.request("audit_snapshot", {"round_id": round_id,
                    "request_id": report_request["request_id"],
                    "allow_delta": not operation.get("prefer_reporter", False),
                    "platform": self.platform_card(row)}, timeout=self.config.operation_timeout))
            except Exception as exc:
                snapshot.update(report_unknown=True, snapshot_error=type(exc).__name__)
        if not report_request.get("ready"):
            snapshot["report_unknown"] = True
        if self.config.team_engine == "veria":
            required = report_request.get("required_through_event")
            mode = snapshot.get("report_mode")
            if (mode not in {"reporter_snapshot", "report_plus_delta"}
                    or report_request.get("report_mode") != mode
                    or isinstance(required, bool) or not isinstance(required, int) or required < 0
                    or snapshot.get("required_through_event") != required
                    or not report_request.get("report_version")
                    or snapshot.get("report_version") != report_request.get("report_version")
                    or snapshot.get("through_event", -1) < required
                    or mode == "report_plus_delta" and snapshot.get("coverage_complete") is not True):
                snapshot["report_unknown"] = True
        elif not snapshot.get("report_version") or snapshot.get("through_event", 0) < snapshot.get("evidence_watermark", 0):
            snapshot["report_unknown"] = True

        # Do not apply a newer round ahead of a still-pending older cutoff.
        # It remains persisted and will be consumed in round order when ready.
        earlier_pending = any(item["attempt_id"] == attempt and item["round_id"] < round_id
                              and item["status"] == "pending"
                              for item in self.store.operations("audit_request"))
        delta_error = report_request.get("delta_error")
        delta_material_error = (delta_error if isinstance(delta_error, dict)
                                and delta_error.get("error_type") == "material_too_large" else None)
        if delta_material_error:
            self.store.event("audit_material_error", code, round_id=round_id,
                             error_type="material_too_large", detail=delta_material_error)
        if (snapshot.get("report_unknown") or earlier_pending) and health["healthy"] and not gained:
            self._defer_audit(code, self.store.operation(operation_id), row, report_request,
                reason="material_too_large" if delta_material_error else
                       "earlier_round_pending" if earlier_pending else "report_coverage_incomplete",
                break_confirmation=not earlier_pending, material_error=delta_material_error)
            if not earlier_pending:
                await self._audit_wait_followup(code, team, operation_id, snapshot, delta_material_error)
            return

        def compact(value):
            report = value.get("report") or {k: value.get(k) for k in
                ("overview", "recent_changes", "short_history", "next_steps", "evidence_refs")}
            return {"report": report, **{k: value[k] for k in
                ("captured_at", "window_id", "report_version", "through_event", "last_event",
                 "evidence_watermark", "pending_reporter_messages", "platform", "report_mode",
                 "required_through_event", "post_cutoff_events", "pending_after_cutoff",
                 "coverage_complete", "delta_events", "delta_evidence_refs", "evidence_refs",
                 "active_review") if k in value}}
        mode = snapshot.get("report_mode", "reporter_snapshot" if self.config.team_engine == "veria" else "leader_report")
        material = {"challenge": self.platform_card(row), **compact(snapshot),
                    "report_mode": mode, "harness_health": self._health_material(health)}
        material["reporter_report_request" if self.config.team_engine == "veria" else "leader_report_request"] = report_request
        history = [{"round_id": r["round_id"], "decision": r["decision"],
                    "streak": r.get("confirmation", {}).get("streak", 0)}
                   for r in self.store.reviews(attempt)]
        material_error = None
        try:
            packet = ReviewPacketBuilder().build(material, [compact(w) for w in snapshot.get("windows", [])[-2:]],
                history, [r["challenge"] for r in self.eligible(code)],
                {"remaining_seconds": max(0, self.deadline - self.clock()), "score_gain_since_review": gained,
                 "active_summaries": [self.platform_card(r) for r in self.store.challenges() if r["status"] == "active"]})
        except ValueError as exc:
            material_error = "material_too_large" if isinstance(exc, ReviewPacketTooLarge) else "material_invalid"
            packet = {"challenge": self.platform_card(row), "material_error": str(exc),
                      "material_error_type": material_error, "mode": "review"}
            self.store.event("audit_material_error", code, round_id=round_id,
                             error_type=material_error, detail=str(exc))
        if material_error and mode == "report_plus_delta" and health["healthy"] and not gained:
            # A complete but huge raw delta can be replaced by the reporter's
            # later complete publication for this SAME cutoff. It has not been
            # accepted/applied, so explicitly request a reporter-only upgrade.
            self._defer_audit(code, self.store.operation(operation_id), row, report_request,
                reason=material_error, material_error={"error_type": material_error,
                                                      "detail": packet["material_error"]})
            await self._audit_wait_followup(code, team, operation_id, snapshot, material_error)
            return
        if gained:
            decision = ReviewDecision("continue", "本轮有平台确认加分，直接通过审核。")
        elif not health["healthy"]:
            decision = ReviewDecision("need_info", "harness健康检查异常，先恢复或核查执行链路，本轮不累计换题建议。")
        elif material_error:
            decision = ReviewDecision("need_info", f"审核材料错误（{material_error}）；完整正文未截断，请检查材料构造或显式大小保护。")
        elif snapshot.get("report_unknown"):
            decision = ReviewDecision("need_info", "本轮固定截点报告及完整增量覆盖尚未确认；本轮不累计换题建议。")
        else:
            try:
                decision = await self.strategy.decide(packet)
                # Continue/switch auditing and target selection are separate
                # calls. A review response cannot bypass a fresh selection.
                decision = replace(decision, replacement=None)
            except Exception as exc:
                decision = ReviewDecision("need_info", f"审核调用失败：{type(exc).__name__}")
        review = snapshot.get("active_review")
        if (decision.action == "switch" and isinstance(review, dict)
                and review.get("status") in {"running", "pending"}
                and not (review.get("phase_evidence_complete") is True
                         and review.get("reproduction_checkpoint")
                         and review.get("investigation_evidence_refs"))):
            decision = ReviewDecision("need_info", "reviewer尚未保存复现checkpoint及其后的实际遗漏检查证据，暂不累计换题建议。")
        fresh = self.store.challenge(code)
        if fresh["attempt_id"] != attempt or fresh["status"] != "active":
            self._save_audit_request(self.store.operation(operation_id), "superseded", reason="attempt_inactive")
            return
        new_gain = fresh["score_revision"] > score_at_start
        if new_gain:
            decision = ReviewDecision("continue", "审核期间出现平台加分，撤销旧换题建议。")
        stale = fresh["revision"] != revision
        if stale and decision.action == "switch":
            decision = ReviewDecision("need_info", "本轮固定截点后出现新flag或关键证据，旧换题判断已失效，等待下一轮。")
            self.store.event("stop_decision_stale", code, expected=revision, actual=fresh["revision"])
        previous = fresh.get("confirmation")
        if previous and previous.get("provisional") and previous.get("round_id") == round_id:
            previous = self.store.operation(operation_id).get("confirmation_before_wait")
        confirmation = SwitchConfirmation.update(previous, decision,
            round_id=round_id, attempt_id=attempt, score_revision=fresh["progress_revision"],
            has_score_gain=gained or new_gain)
        fresh["confirmation"] = confirmation
        fresh["last_audit_score_revision"] = fresh["score_revision"]
        body = {"round_id": round_id, "decision": decision.to_dict(), "confirmation": confirmation,
                "packet": packet, "revision": revision, "request_id": report_request["request_id"]}
        operation = self.store.operation(operation_id)
        operation_body = {key: value for key, value in operation.items()
                          if key not in {"id", "kind", "code", "status"}}
        operation_body.update(applied_at=self.clock(), report_status=report_request)
        # One SQLite commit protects the round's at-most-once application across
        # process death between challenge, review and request bookkeeping.
        with self.store.db:
            self.store.db.execute("INSERT OR REPLACE INTO challenges VALUES(?,?)", (code, self.store.encode(fresh)))
            self.store.db.execute("INSERT OR REPLACE INTO reviews VALUES(?,?,?)", (attempt, round_id, self.store.encode(body)))
            self.store.db.execute("INSERT OR REPLACE INTO operations VALUES(?,?,?,?,?)",
                (operation_id, "audit_request", code, "applied", self.store.encode(operation_body)))
        self.store.event("audit_decision", code, round_id=round_id, decision=decision.to_dict(),
                         streak=confirmation["streak"])
        if decision.action == "need_info" and team and team.alive:
            try:
                recipient = ("reporter" if snapshot.get("report_unknown") else "coordinator") if self.config.team_engine == "veria" else "leader"
                await team.request("message", {"recipient": recipient, "body": decision.reason},
                                   timeout=self.config.operation_timeout)
            except Exception as exc:
                self.store.event("audit_followup_pending", code, error=type(exc).__name__)
        elif confirmation["execute"]:
            await self.switch(code, None, expected_revision=revision)

    async def audit_round(self, round_id):
        tasks = [self.audit_one(r["code"], round_id) for r in self.store.challenges() if r["status"] == "active"]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                self.store.event("audit_failed", round_id=round_id, error=type(result).__name__)

    async def switch(self, code, recommended, expected_revision):
        if self.config.no_flag_timeout:
            return
        row = self.store.challenge(code)
        if row["status"] != "active" or row["revision"] != expected_revision:
            return
        if any(o["code"] == code and o["kind"] == "submit" and o["status"] != "confirmed"
               and o.get("attempt_id", row["attempt_id"]) == row["attempt_id"]
               for o in self.store.operations()):
            self.store.event("switch_waiting_submission", code)
            return
        choice_id = "replace:" + row["attempt_id"] + ":" + str((row.get("confirmation") or {}).get("round_id", self.round_id))
        selected = await self.choose(choice_id, recommended, excluding=code)
        if not selected:
            return
        if selected not in {r["code"] for r in self.eligible(code)}:
            self.store.event("replacement_unavailable", code, selected=selected)
            return
        await self.finish(code, "switch", expected_revision=expected_revision)
        fresh = self.store.challenge(code)
        target = self.store.challenge(selected)
        if fresh.get("close_confirmed") and target["status"] in {"queued", "paused"} and not target["challenge"]["completed"]:
            if self.reserve_count() < self.config.parallel:
                await self.start_challenge(selected, choice_id)

    async def finish(self, code, reason, expected_revision=None):
        async with self._finish_locks.setdefault(code, asyncio.Lock()):
            await self._finish(code, reason, expected_revision)

    async def _idle_submission_checkpoint(self, code, team):
        """Drain final runtime flags and give their existing submit jobs a bound.

        A lost platform reply is kept in the durable outbox, never silently
        treated as an incorrect flag. It cannot postpone a hard timeout
        indefinitely after the bounded grace and final platform reconciliation.
        """
        if team:
            for event in team.poll_events():
                self.handle_team_event(code, event)
        # Candidate events schedule jobs; let them acquire the submission lock
        # and write the pending journal before selecting the tasks to wait for.
        await asyncio.sleep(0)
        pending = []
        for key, task in self.jobs.items():
            if task is asyncio.current_task() or task.done():
                continue
            retry = self.store.operation(key[len("retry:"):]) if key.startswith("retry:") else None
            if key.startswith("flag:" + code + ":") or retry and retry["code"] == code:
                pending.append(task)
        try:
            async with asyncio.timeout(self.config.operation_timeout):
                if pending:
                    await asyncio.gather(*(asyncio.shield(task) for task in pending), return_exceptions=True)
                # Direct callers of submit() do not necessarily have a job key.
                async with self._submit_locks.setdefault(code, asyncio.Lock()):
                    pass
        except TimeoutError:
            self.store.event("idle_submission_grace_expired", code,
                             grace_seconds=self.config.operation_timeout)

    async def _cancel_idle_stop(self, code, team, *, reason):
        row = self.store.challenge(code)
        row.update(status="active", last_tick=self.clock())
        self.store.save_challenge(row)
        self.store.event("no_flag_timeout_cancelled", code, reason=reason,
                         correct_flag_count=row["challenge"]["correct_flag_count"])
        if team:
            try:
                await team.request("cancel_stop", {}, timeout=self.config.operation_timeout)
            except Exception as exc:
                self.store.event("cancel_stop_failed", code, error=type(exc).__name__)
                self._job("recover:" + code, self.recover_runtime(code))

    async def _finish(self, code, reason, expected_revision=None):
        row = self.store.challenge(code)
        if row["status"] in {"closing", "paused", "completed"}:
            return
        idle_progress = None
        idle_attempt = None
        if reason == "no_flag_timeout":
            if not self.no_flag_expired(row):
                return
            idle_progress, idle_attempt = row["progress_revision"], row["attempt_id"]
            # Stop recovery/reconciliation from launching another SDK runtime
            # while this attempt is being checkpointed.
            row["status"] = "draining"
            self.store.save_challenge(row)
            self.store.event("no_flag_timeout_reached", code,
                no_flag_seconds=row["active_seconds"] - row["last_flag_work"],
                timeout_seconds=self.config.no_flag_timeout, attempt_id=idle_attempt)
            startup = self._runtime_start_tasks.get(code)
            if startup and startup is not asyncio.current_task():
                # A recovering interpreter may not have launched its process
                # yet. Cancel and join that owner before stopping/closing, so
                # it cannot launch an orphan after this scene is gone.
                startup.cancel()
                await asyncio.gather(startup, return_exceptions=True)
        if reason == "completed" and not completion_confirmed(row["challenge"]):
            row["challenge"]["completed"] = False
            self.store.save_challenge(row)
            self.store.event("completion_unconfirmed", code, phase="before_checkpoint")
            return
        team = self.teams.get(code)
        if team:
            try:
                # The runtime may use its entire shutdown budget to drain a
                # turn. Leave transport time after that bounded inner wait.
                rpc_margin = VERIA_STOP_RPC_TIMEOUT if self.config.team_engine == "veria" else 0
                checkpoint = await team.request("prepare_stop", {"reason": reason},
                    timeout=self.config.shutdown_timeout + rpc_margin)
                self.store.event("checkpoint_ready", code,
                    flushed=checkpoint.get("flushed"), in_flight=checkpoint.get("in_flight", []))
            except Exception as exc:
                self.store.event("checkpoint_incomplete", code, error=type(exc).__name__)
        if reason == "no_flag_timeout":
            await self._idle_submission_checkpoint(code, team)
            # A final list can confirm a flag whose submit response was lost.
            # Failure to fetch the list is recorded, but cannot make a hard
            # timeout wait forever for the provider/platform to recover.
            try:
                await self.reconcile()
            except Exception as exc:
                self.store.event("idle_reconcile_unavailable", code, error=type(exc).__name__)
            fresh = self.store.challenge(code)
            if fresh["attempt_id"] != idle_attempt:
                return
            if completion_confirmed(fresh["challenge"]):
                reason = "completed"
            elif (fresh["progress_revision"] != idle_progress
                  or fresh["active_seconds"] - fresh["last_flag_work"] < self.config.no_flag_timeout):
                await self._cancel_idle_stop(code, team, reason="new_flag_confirmed")
                return
            unresolved = [op["id"] for op in self.store.operations("submit")
                          if op["code"] == code and op["status"] != "confirmed"
                          and op.get("attempt_id", idle_attempt) == idle_attempt]
            if unresolved:
                self.store.event("idle_submissions_retained", code, operation_ids=unresolved,
                                 attempt_id=idle_attempt)
        row = self.store.challenge(code)
        if reason == "completed" and not completion_confirmed(row["challenge"]):
            row["challenge"]["completed"] = False
            self.store.save_challenge(row)
            self.store.event("completion_unconfirmed", code, phase="after_checkpoint")
            if team:
                await team.request("cancel_stop", {})
            return
        if expected_revision is not None and row["revision"] != expected_revision:
            self.store.event("stop_decision_stale", code, expected=expected_revision, actual=row["revision"])
            if team:
                await team.request("cancel_stop", {})
            return
        if reason == "switch":
            # Platform and journal are checked again after checkpoint, before destroying the scene.
            await self.reconcile()
            row = self.store.challenge(code)
            if row["revision"] != expected_revision or row["challenge"]["completed"]:
                self.store.event("stop_decision_stale", code)
                if team:
                    await team.request("cancel_stop", {})
                return
            if any(op["status"] != "confirmed" for op in self.store.operations("submit")
                   if op["code"] == code and op.get("attempt_id", row["attempt_id"]) == row["attempt_id"]):
                self.store.event("switch_waiting_submission", code, phase="after_checkpoint")
                if team:
                    await team.request("cancel_stop", {})
                return
        row.update(status="closing", close_intent=reason, close_confirmed=False)
        self.store.save_challenge(row)
        if team:
            await team.stop()
            self._record_runtime_cleanup(code, team)
            self.teams.pop(code, None)
        if reason == "no_flag_timeout":
            # stop() may take time. A submission already in flight can confirm
            # during that wait; preserve the scene and original attempt then
            # restore a new runtime rather than closing after a fresh flag.
            try:
                await self.reconcile()
            except Exception as exc:
                self.store.event("idle_reconcile_unavailable", code, error=type(exc).__name__)
            row = self.store.challenge(code)
            if (row["attempt_id"] == idle_attempt and not completion_confirmed(row["challenge"])
                    and row["progress_revision"] != idle_progress):
                row.update(status="active", close_intent=None, close_confirmed=False, last_tick=self.clock())
                self.store.save_challenge(row)
                self.store.event("no_flag_timeout_cancelled", code, reason="new_flag_during_runtime_stop",
                                 correct_flag_count=row["challenge"]["correct_flag_count"])
                await self.start_runtime(code)
                return
        await self.close_instance(code)

    async def close_instance(self, code):
        row = self.store.challenge(code)
        opid = "close:" + row["attempt_id"]
        attempts = (self.store.operation(opid) or {}).get("attempts", 0) + 1
        self.store.save_operation(opid, "close", code, "pending", attempts=attempts)
        try:
            await self._call(self.platform.close(code))
        except Exception as exc:
            self.store.save_operation(opid, "close", code, "unknown", attempts=attempts, error=type(exc).__name__)
            self.store.event("closure_uncertain", code, error=type(exc).__name__)
            return
        # A successful close response is confirmation; asynchronous stop_pending is checked by reconciliation too.
        states = await self._call(self.platform.list_challenges())
        state = next((c for c in states if c.code == code), None)
        if state is not None and state.container_status == "stopped":
            row = self.store.challenge(code)
            row.update(status="completed" if row["challenge"]["completed"] else "paused", close_confirmed=True)
            row["challenge"].update(container_status="stopped", container_addr=[])
            self.store.save_challenge(row)
            self.store.save_operation(opid, "close", code, "confirmed", attempts=attempts)
            self.store.event("closure_confirmed", code, reason=row.get("close_intent"))

    async def request_review(self, code):
        row = self.store.challenge(code)
        team = self.teams.get(code)
        if not team or row["status"] != "active":
            return
        pending = row.get("pending_review_request")
        if pending and pending.get("attempt_id") != row["attempt_id"]:
            pending = None
        if pending and (self.clock() < row.get("review_retry_at", 0)
                        or row.get("review_request_attempts", 0) > self.config.max_retries):
            return
        if not pending:
            pending = {"review_id": f"{row['attempt_id']}:review:{uuid.uuid4().hex}",
                       "attempt_id": row["attempt_id"], "requested_work": row["active_seconds"],
                       "reason": "累计作答30分钟无新flag，独立检查健康、复现与覆盖。"}
            row["review_request_attempts"] = 0
        row["pending_review_request"] = pending
        row["review_request_attempts"] = row.get("review_request_attempts", 0) + 1
        row["review_retry_at"] = self.clock() + self.config.poll_interval
        self.store.save_challenge(row)
        try:
            response = await team.request("review", {**pending, "platform": self.platform_card(row)},
                                          timeout=self.config.operation_timeout)
            if response.get("error"):
                raise RuntimeError(response["error"])
        except Exception as exc:
            fresh = self.store.challenge(code)
            if fresh["attempt_id"] == pending["attempt_id"]:
                fresh["review_request_error"] = type(exc).__name__
                self.store.save_challenge(fresh)
            self.store.event("review_request_pending", code, review_id=pending["review_id"], error=type(exc).__name__)
            return
        fresh = self.store.challenge(code)
        if fresh["attempt_id"] != pending["attempt_id"]:
            return
        already_running = response.get("already_running") is True
        response_id = response.get("review_id") or pending["review_id"]
        newly_acknowledged = (not already_running or response_id == pending["review_id"]
                              and fresh.get("review_id") != response_id)
        if newly_acknowledged:
            # A lost first response may be retried against the same running ID.
            # Acknowledge its original work position once, never the retry time.
            fresh["last_review_work"] = pending.get("requested_work", fresh["active_seconds"])
        fresh["review_id"] = response_id
        fresh["pending_review_request"] = None
        fresh["review_request_error"] = None
        self.store.save_challenge(fresh)
        self.store.event("reviewer_requested" if newly_acknowledged else "reviewer_already_running",
                         code, review_id=fresh["review_id"])

    async def recover_runtime(self, code):
        # TeamProcess.start performs asynchronous interpreter checks before a
        # subprocess exists. That interval is startup, not a dead runtime.
        if code in self._starting_teams or code in self._recovering_teams:
            return
        self._recovering_teams.add(code)
        try:
            team = self.teams.pop(code, None)
            if team:
                if self.config.team_engine == "veria":
                    await team.stop(preserve_environment=True)
                else:
                    await team.stop()
                self._record_runtime_cleanup(code, team)
            row = self.store.challenge(code)
            row["runtime_restarts"] += 1
            if row.get("pending_review_request"):
                row["review_request_attempts"] = 0
                row["review_retry_at"] = self.clock()
            self.store.save_challenge(row)
        finally:
            self._recovering_teams.discard(code)
        if row["runtime_restarts"] <= self.config.max_retries and row["status"] == "active":
            await self.start_runtime(code)
        else:
            row["health_error"] = "runtime_recovery_exhausted"
            self.store.save_challenge(row)
            self.store.event("runtime_recovery_exhausted", code)

    async def process_recovery_requests(self):
        requests_dir = self.store.run_dir / "control" / "recovery-requests"
        if not requests_dir.is_dir():
            return
        for path in sorted(requests_dir.glob("*.json")):
            now = self.clock()
            receipt = {"request_id": path.stem, "processed_at": now,
                       "run_id": self.store.get_meta("run_id"), "deadline": self.deadline}
            operation_id = "operator-recovery:" + path.stem
            operation = self.store.operation(operation_id)
            request = operation.get("request") if operation else None
            attempts = (operation or {}).get("attempts", 0)
            terminal = {"confirmed", "partial", "refused", "needs_attention"}
            if operation and operation["status"] in terminal:
                receipt = operation["receipt"]
            elif operation and operation.get("next_retry_at", 0) > now:
                continue
            else:
                code = None
                try:
                    if path.is_symlink() or path.stat().st_size > 16384:
                        raise ValueError("invalid recovery control file")
                    incoming = json.loads(path.read_text(encoding="utf-8"))
                    if (not isinstance(incoming, dict) or incoming.get("request_id") != path.stem
                            or incoming.get("run_id") != self.store.get_meta("run_id")
                            or incoming.get("provider") not in {None, "deepseek", "glm"}):
                        raise ValueError("recovery identity/provider mismatch")
                    if request is not None and incoming != request:
                        raise ValueError("persisted recovery payload cannot change on retry")
                    request = incoming
                    code = request.get("code")
                    row = self.store.challenge(code)
                    if (row is None or row["status"] != "active"
                            or request.get("attempt_id") != row["attempt_id"]
                            or not self._work_allowed()):
                        raise ValueError("recovery requires this active saved attempt within its original budget")
                    if self.config.team_engine != "veria":
                        raise ValueError("controlled member recovery requires Veria")
                    receipt.update(code=code, attempt_id=row["attempt_id"], provider=request.get("provider"),
                                   attempts=attempts)
                    team = self.teams.get(code)
                    if attempts >= 3:
                        receipt.update(status="unknown", needs_attention=True,
                                       reason="recovery reply remains unknown after three journaled attempts")
                    elif not team or not team.alive:
                        # No command has been sent: leave the original request
                        # queued while normal runtime recovery restores IPC.
                        receipt.update(status="unknown" if attempts else "pending",
                                       reason="waiting_runtime", next_retry_at=now + self.config.poll_interval)
                    else:
                        attempts += 1
                        next_retry_at = now + self.config.poll_interval * (2 ** (attempts - 1))
                        # Persist the exact request and attempt before sending.
                        # If the supervisor dies mid-RPC, replay uses the same
                        # runtime-durable request_id and cannot rearm twice.
                        self.store.save_operation(operation_id, "operator_recovery", code, "pending",
                            request=request, attempts=attempts, next_retry_at=next_retry_at)
                        try:
                            result = await team.request("recover_members", {
                                "request_id": request["request_id"], "provider": request.get("provider"),
                                "reason": request.get("reason", "operator requested recovery")},
                                timeout=self.config.operation_timeout)
                            if not isinstance(result, dict):
                                raise RuntimeError("invalid recovery RPC response")
                            status = ("refused" if result.get("error") else
                                      "partial" if result.get("refused") and result.get("recovered") else
                                      "refused" if result.get("refused") else "confirmed")
                            receipt.update(status=status, result=result, attempts=attempts)
                        except (OSError, RuntimeError, TimeoutError) as exc:
                            # Losing the response says nothing about whether the
                            # runtime already persisted/performed the recovery.
                            receipt.update(status="unknown", error=type(exc).__name__, reason=str(exc),
                                           attempts=attempts, next_retry_at=next_retry_at,
                                           needs_attention=attempts >= 3)
                except (OSError, ValueError, TypeError, RuntimeError) as exc:
                    receipt.update(code=code, status="unknown" if attempts else "refused",
                                   error=type(exc).__name__, reason=str(exc), attempts=attempts,
                                   needs_attention=bool(attempts))
                operation_status = "needs_attention" if receipt.get("needs_attention") else receipt["status"]
                self.store.save_operation(operation_id, "operator_recovery", code, operation_status,
                    request=request, attempts=attempts, next_retry_at=receipt.get("next_retry_at", 0), receipt=receipt)
                self.store.event("operator_recovery_" + operation_status, code, receipt=receipt)
            # Persist observations before consuming/parking the queue entry.
            # Unknown outcomes remain queued for a finite same-ID retry; after
            # three lost replies keep the request as evidence for an operator.
            atomic_json(self.store.run_dir / "control" / "recovery-receipts" / path.name, receipt)
            if receipt.get("needs_attention"):
                parked = self.store.run_dir / "control" / "recovery-parked" / path.name
                parked.parent.mkdir(parents=True, exist_ok=True)
                path.replace(parked)
            elif receipt["status"] in {"confirmed", "partial", "refused"}:
                path.unlink(missing_ok=True)

    def _record_runtime_cleanup(self, code, team):
        result = getattr(team, "cleanup_result", None)
        if self.config.team_engine != "veria" or not result:
            return
        row = self.store.challenge(code)
        if row["attempt_id"] == result.get("attempt_id"):
            row["runtime_cleanup"] = result
            self.store.save_challenge(row)
        if result.get("cleanup_status") == "incomplete":
            self.store.event("runtime_cleanup_incomplete", code, cleanup=result)

    def status(self):
        return {"run_id": self.store.get_meta("run_id"), "status": self.store.get_meta("status"),
                "team_engine": self.config.team_engine,
                "no_flag_timeout_seconds": self.config.no_flag_timeout,
                "role_models": self.config.veria_models if self.config.team_engine == "veria" else {"default": self.config.model},
                "team_profile": self.config.veria_team_profile if self.config.team_engine == "veria" else None,
                "solver_ids": self.config.veria_solver_ids if self.config.team_engine == "veria" else [],
                "role_providers": self.config.veria_providers if self.config.team_engine == "veria" else {},
                "veria_upstream_commit": self.config.veria_upstream_commit if self.config.team_engine == "veria" else None,
                **self.timing_status(), "exit_reason": self.exit_reason,
                "round_id": self.round_id, "reserved": self.reserve_count(),
                "challenges": self.store.challenges(), "demo_peak": self.store.get_meta("demo_peak")}

    def handle_team_event(self, code, event, now=None):
        """Reject obsolete identities before scheduling any platform side effect."""
        now = self.clock() if now is None else now
        row = self.store.challenge(code)
        kind = event.get("type", "runtime_event")
        attempt = event.get("attempt_id")
        reported_code = event.get("code", event.get("challenge_code"))
        strict = self.config.team_engine == "veria"
        if (row is None or reported_code not in {None, code}
                or attempt is not None and attempt != row["attempt_id"]
                or strict and (reported_code != code or attempt != row["attempt_id"])):
            self.store.event("stale_runtime_event_rejected", code, received_kind=kind,
                             received_attempt=attempt, received_code=reported_code)
            return False
        if kind == "heartbeat":
            self.last_heartbeat[code] = now
            if strict:
                row["runtime_status"] = {key: event[key] for key in
                    ("team_engine", "generation", "roles", "models", "report_version", "through_event",
                     "evidence_watermark", "degraded_reason", "environment", "healthy") if key in event}
                self.store.save_challenge(row)
        elif kind == "candidate_flag":
            flag = event.get("flag")
            if not isinstance(flag, str) or not flag.strip():
                self.store.event("invalid_candidate_rejected", code)
                return False
            if strict:
                flag = flag.strip()
            key = "flag:" + code + ":" + hashlib.sha256(flag.encode()).hexdigest()
            if strict:
                # Serialize at the submission lock, preserving each discoverer's
                # provenance even when equal candidates arrive in the same tick.
                source_identity = str(event.get("request_id") or event.get("sender") or event.get("source"))
                key += ":" + hashlib.sha256(source_identity.encode()).hexdigest()
            self._job(key, self.submit(code, flag, event.get("request_id"),
                                      event.get("sender", event.get("source")), row["attempt_id"]))
        elif kind == "significant_evidence":
            evidence_id = event.get("evidence_id", event.get("source_event_id"))
            if strict and (not isinstance(evidence_id, (str, int)) or isinstance(evidence_id, bool) or not str(evidence_id)):
                self.store.event("unidentified_evidence_rejected", code)
                return False
            known = row.setdefault("significant_evidence_ids", [])
            if evidence_id is not None and evidence_id in known:
                return False
            if evidence_id is not None:
                known.append(evidence_id)
            row["revision"] += 1
            row["confirmation"] = None
            self.store.save_challenge(row)
        elif kind == "report_published":
            row["report_status"] = {key: event[key] for key in
                ("report_mode", "report_version", "through_event", "evidence_watermark", "report_path") if key in event}
            if "report_path" not in row["report_status"] and event.get("path"):
                row["report_status"]["report_path"] = event["path"]
            self.store.save_challenge(row)
            self._job("pending-audit:" + code, self.resume_pending_audits(code))
        elif kind in {"runtime_fault", "health_degraded", "environment_fault", "role_failed"}:
            row["health_error"] = event.get("reason", event.get("error", kind))
            self.store.save_challenge(row)
            if event.get("needs_restart") is True and row["status"] == "active":
                self._job("recover:" + code, self.recover_runtime(code))
        if kind != "heartbeat":
            self.store.event("runtime_" + kind, code, payload=event)
        return True

    async def run(self):
        primary_error = None
        try:
            await self.initialize()
            next_poll = self.clock() + self.config.poll_interval
            next_audit = self.store.get_meta("next_audit", self.clock() + self.config.audit_interval)
            while self._work_allowed():
                now = self.clock()
                self._reap()
                for code, team in list(self.teams.items()):
                    for event in team.poll_events():
                        self.handle_team_event(code, event, now)
                    row = self.store.challenge(code)
                    if row["status"] == "active":
                        if not self.config.no_flag_timeout and code not in self._starting_teams:
                            row["active_seconds"] += max(0, now - row["last_tick"])
                            row["last_tick"] = now
                            self.store.save_challenge(row)
                        review_due = row["active_seconds"] - max(row["last_flag_work"], row["last_review_work"]) >= self.config.review_interval
                        review_retry = (row.get("pending_review_request") and now >= row.get("review_retry_at", 0)
                                        and row.get("review_request_attempts", 0) <= self.config.max_retries)
                        if review_retry or review_due and not row.get("pending_review_request"):
                            self._job("review:" + code, self.request_review(code))
                        if code not in self._starting_teams and (not team.alive or now - self.last_heartbeat.get(code, now) > self.config.heartbeat_timeout):
                            self._job("recover:" + code, self.recover_runtime(code))
                self.check_no_flag_timeouts(now)
                if now >= next_poll:
                    next_poll = now + self.config.poll_interval
                    self._job("reconcile", self.reconcile())
                    self._job("pending-audits", self.resume_pending_audits())
                    self._job("operator-recovery", self.process_recovery_requests())
                    for op in self.store.operations("submit"):
                        if op["status"] != "confirmed" and op.get("attempts", 0) <= self.config.max_retries:
                            self._job("retry:" + op["id"], self.submit(op["code"], op["flag"],
                                op.get("request_id"), op.get("sender"), op.get("attempt_id")))
                    for row in self.store.challenges():
                        op = self.store.operation("close:" + str(row["attempt_id"]))
                        if row["status"] == "closing" and (op or {}).get("attempts", 0) <= self.config.max_retries:
                            self._job("close:" + row["code"], self.close_instance(row["code"]))
                if not self.config.no_flag_timeout and now >= next_audit:
                    skipped = max(0, int((now - next_audit) // self.config.audit_interval))
                    self.round_id += skipped + 1
                    self.store.set_meta("round_id", self.round_id)
                    next_audit += (skipped + 1) * self.config.audit_interval
                    self.store.set_meta("next_audit", next_audit)
                    # A slow prior round never creates overlapping model calls or extra confirmations.
                    if "audit" not in self.jobs:
                        self._job("audit", self.audit_round(self.round_id))
                if self.reserve_count() < self.config.parallel and self.eligible() and "fill" not in self.jobs:
                    self._job("fill", self.fill_one())
                rows = self.store.challenges()
                if rows and all(r["status"] == "completed" for r in rows):
                    self._stopping("completed", "all_challenges_completed")
                    break
                atomic_json(self.store.run_dir / "status.json", self.status())
                await asyncio.sleep(self.config.tick)
            if self.store.get_meta("status") != "completed":
                self._stopping("stopped" if self.stop_requested else "deadline",
                               "stop_requested" if self.stop_requested else "work_deadline_reached")
        except BaseException as error:
            primary_error = error
            # Failing to acquire the run lock must never mutate or clean up a
            # different live supervisor's run.
            if self.lock is not None:
                self._stopping("error", "runtime_error")
            raise
        finally:
            if self.lock is not None:
                try:
                    await self.shutdown()
                except BaseException as error:
                    self.store.event("supervisor_cleanup_failed", error_type=type(error).__name__,
                                     primary_error_type=type(primary_error).__name__ if primary_error else None)
                    if primary_error is None:
                        self._stopping("error", "runtime_error")
                        raise
                finally:
                    if self.lock is not None:
                        self.lock.close()
                        self.lock = None
                    atomic_json(self.store.run_dir / "status.json", self.status())
        atomic_json(self.store.run_dir / "status.json", self.status())
        return self.status()

    async def shutdown(self):
        self.shutting_down = True
        for task in self.jobs.values():
            task.cancel()
        await asyncio.gather(*self.jobs.values(), return_exceptions=True)
        self.jobs.clear()
        async def cleanup(row):
            try:
                if row["status"] == "closing":
                    # An expired resume never enters the normal retry loop.
                    # Complete a previously journaled close during cleanup.
                    await self.close_instance(row["code"])
                else:
                    await self.finish(row["code"], "shutdown")
            except Exception as exc:
                self.store.event("cleanup_unconfirmed", row["code"], error=type(exc).__name__)
        try:
            from .config import veria_supervisor_cleanup_timeout
            cleanup_timeout = (veria_supervisor_cleanup_timeout(self.config.shutdown_timeout, self.config.operation_timeout)
                               if self.config.team_engine == "veria" else self.config.shutdown_timeout * 2)
            async with asyncio.timeout(cleanup_timeout):
                await asyncio.gather(*(cleanup(r) for r in self.store.challenges() if r["status"] in RESERVED))
        except TimeoutError:
            self.store.event("cleanup_deadline")
        for team in list(self.teams.values()):
            await team.stop()
            self._record_runtime_cleanup(team.code, team)
        self.teams.clear()
        if self.lock:
            self.lock.close()
            self.lock = None
