#!/usr/bin/env python3
"""Read-only archived-material replay through Supervisor and SDKStrategy.

No SDK/network process is created. A fake process records whether the real
strategy adapter receives a complete packet; archived files are opened read-only.
Only a compact result JSON is written to the requested output destination.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

from xinghuo.config import Config
from xinghuo.store import Store, atomic_json
from xinghuo.strategy import ReviewPacketBuilder, SDKStrategy
from xinghuo.supervisor import Supervisor
from xinghuo.veria.state import VeriaRoom


def readonly(path):
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


class Clock:
    def __init__(self, value):
        self.now = value

    def __call__(self):
        return self.now


class CapturedProcess:
    def __init__(self, captures, **kwargs):
        self.captures = captures

    async def start(self):
        pass

    async def turn(self, prompt):
        self.captures.append({"prompt_utf8_bytes": len(prompt.encode("utf-8")),
                              "has_material": "审核材料" in prompt})
        return {"is_error": False, "structured_output": {
            "action": "continue", "reason": "离线回放，仅验证审核路径实际调用，不模拟真实解题结论。"}}

    async def close(self):
        pass


class ReplayTeam:
    alive = True
    pid = 0

    def __init__(self, status, snapshot):
        self.status, self.snapshot = status, snapshot
        self.released = False

    async def request(self, kind, payload, **kwargs):
        if kind in {"request_report", "report_status"}:
            return {**self.status, "ready": self.released and self.status["ready"]}
        if kind == "health":
            return {"healthy": True, "responsive": True, "core_failures": [], "roles": []}
        if kind == "audit_snapshot":
            if not self.released or not self.status["ready"]:
                raise ValueError("archived covering material has not arrived")
            return copy.deepcopy(self.snapshot)
        return {"ok": True}


def report_material(room, request):
    """Rebase relocated archived references and validate their original hashes."""
    version = request["report_version"]
    if version is None:
        report_row = room.execute("SELECT * FROM reports WHERE through_event<=? ORDER BY version DESC LIMIT 1",
                                  (request["required_through_event"],)).fetchone()
    else:
        report_row = room.execute("SELECT * FROM reports WHERE version=?", (version,)).fetchone()
    status = {**dict(request), "report_mode": "reporter_snapshot", "ready": report_row is not None,
              "coverage_complete": report_row is not None}
    if report_row is None:
        return status, {}, 0
    report = {**json.loads(report_row["data"]), "report_version": report_row["version"],
              "through_event": report_row["through_event"]}
    status.update(report_version=report_row["version"], through_event=report_row["through_event"])
    snapshot = {**status, "report": report, "windows": []}
    if version is not None:
        return status, snapshot, report_row["created_at"]
    where, values = VeriaRoom._reportable_where()
    events = [{**dict(row), "data": json.loads(row["data"])} for row in room.execute(
        "SELECT * FROM events WHERE " + where + " AND seq>? AND seq<=? ORDER BY seq",
        (*values, report_row["through_event"], request["required_through_event"]))]
    directory = Path(room.execute("PRAGMA database_list").fetchone()["file"]).parent
    references, errors = [], []
    for event in events:
        if event["type"] != "evidence":
            continue
        data = event["data"]
        path = directory / "evidence" / Path(data["path"]).name
        try:
            content = path.read_bytes()
            if len(content) != data["bytes"] or hashlib.sha256(content).hexdigest() != data["sha256"]:
                raise ValueError("relocated archive hash mismatch")
            references.append({"event_seq": event["seq"], "path": str(path), "sha256": data["sha256"],
                               "bytes": len(content), "content": content.decode("utf-8")})
        except (OSError, UnicodeError, ValueError) as exc:
            errors.append(type(exc).__name__)
    snapshot.update(report_mode="report_plus_delta", through_event=request["required_through_event"],
                    delta_events=events, delta_evidence_refs=references, coverage_complete=not errors)
    size = len(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    status.update(report_mode="report_plus_delta", through_event=request["required_through_event"])
    if errors or size > ReviewPacketBuilder.DEFAULT_MAX_BYTES:
        status.update(ready=False, coverage_complete=False, delta_error={
            "error_type": "material_too_large" if not errors else "evidence_unavailable",
            "bytes": size, "errors": errors})
        snapshot = {}
    else:
        status.update(ready=True, coverage_complete=True)
    return status, snapshot, request["created_at"] + 181


async def replay(archive):
    state_path = archive / "state.sqlite3"
    state = readonly(state_path)
    original_hash = hashlib.sha256(state_path.read_bytes()).hexdigest()
    rows = [json.loads(item["body"]) for item in state.execute("SELECT body FROM challenges")]
    reviews = state.execute("SELECT attempt,round,body FROM reviews ORDER BY round,attempt").fetchall()
    results = []
    for saved in reviews:
        old = json.loads(saved["body"])
        code, round_id = old["packet"]["challenge"]["code"], saved["round"]
        room = readonly(archive / "teams" / code / "room.sqlite3")
        request = room.execute("SELECT * FROM veria_report_requests WHERE round_id=?", (round_id,)).fetchone()
        status, snapshot, arrival = report_material(room, request)
        clock = Clock(request["created_at"])
        captures = []
        with tempfile.TemporaryDirectory(prefix="xinghuo-strategy-replay-") as temporary:
            store = Store(Path(temporary))
            store.set_meta("run_id", "offline-archive-replay")
            for original in rows:
                row = copy.deepcopy(original)
                if row["code"] == code:
                    row.update(challenge=copy.deepcopy(old["packet"]["challenge"]), status="active",
                               attempt_id=saved["attempt"], revision=old["revision"], score_revision=0,
                               progress_revision=0, last_audit_score_revision=0, confirmation=None)
                else:
                    row["status"] = "completed" if row["challenge"]["completed"] else "paused"
                    row["challenge"].update(container_status="stopped", container_addr=[])
                store.save_challenge(row)
            config = Config(team_engine="veria", backend="claude", veria_python=sys.executable,
                            veria_tool_image="offline-not-executed", veria_tool_cpu_limit=8,
                            veria_tool_memory_limit="16g", budget=28800, operation_timeout=.1,
                            audit_report_timeout=.002)
            strategy = SDKStrategy(Path(temporary) / "strategy", timeout=1,
                process_factory=lambda **kwargs: CapturedProcess(captures, **kwargs))
            supervisor = Supervisor(None, store, config, strategy=strategy, clock=clock)
            supervisor.deadline, supervisor.work_deadline = clock.now + 28800, clock.now + 28620
            team = ReplayTeam(status, snapshot)
            supervisor.teams[code], supervisor.last_heartbeat[code] = team, clock.now
            await supervisor.audit_one(code, round_id)
            calls_before = len(captures)
            clock.now = max(arrival, clock.now + 181)
            supervisor.last_heartbeat[code] = clock.now
            team.released = True
            await supervisor.resume_pending_audits(code)
            calls_after = len(captures)
            await supervisor.resume_pending_audits(code)
            await supervisor.audit_one(code, round_id, wait_for_report=False)
            operation = store.operation(supervisor._audit_operation_id(saved["attempt"], round_id))
            final_reviews = store.reviews(saved["attempt"])
            results.append({"code": code, "round_id": round_id, "mode": status["report_mode"],
                "original_material_error": old["packet"].get("material_error"),
                "calls_before_release": calls_before, "calls_after_release": calls_after,
                "calls_after_repeat": len(captures), "idempotent": len(captures) == calls_after,
                "request_state": operation["status"], "preserved_request_id": operation["request_id"] == request["request_id"],
                "formal_review_count": len(final_reviews), "material_error": operation.get("last_material_error"),
                "packet_bytes": final_reviews[-1]["packet"].get("material_budget", {}).get("material_bytes")
                                if final_reviews else None})
            store.close()
        room.close()
    state.close()
    unchanged = hashlib.sha256(state_path.read_bytes()).hexdigest() == original_hash
    return {"scope": "offline archived material replay; no real SDK/network/model/platform calls",
            "candidate_catalog": "final archived eligible cards, not a reconstruction of each historical catalog",
            "review_count": len(results), "actual_strategy_process_calls": sum(item["calls_after_release"] for item in results),
            "idempotent": all(item["idempotent"] for item in results),
            "preserved_request_ids": all(item["preserved_request_id"] for item in results),
            "archive_state_unchanged": unchanged, "archive_state_sha256": original_hash,
            "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True, help="read-only archived state directory")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(replay(args.archive.resolve()))
    atomic_json(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key != "results"}, ensure_ascii=False))
    if not result["idempotent"] or not result["preserved_request_ids"] or not result["archive_state_unchanged"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
