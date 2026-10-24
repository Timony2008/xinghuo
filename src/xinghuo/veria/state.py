"""Single-writer durable state for the Veria team engine.

The runtime owns this Room and its inherited process lock. SDK roles never open
the database. Additional tables deliberately do not use native worker tasks.
Delivery means hand-off to an SDK, never proof that a model understood a message.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

from ..room import Room, _json, _load, atomic_write


SOLVER_IDS = ("pro-1", "pro-2", "pro-3", "flash-1", "flash-2", "flash-3")
SCHEMA_VERSION = 2
AUDIT_MATERIAL_MAX_BYTES = 768 * 1024
REPORTABLE_EVENTS = frozenset({
    "progress", "candidate_flag", "submission_result", "coordinator_notice",
    "broadcast", "message_queued", "evidence", "tool_called", "tool_result",
    "tool_call", "tool_observation", "agent_text", "agent_output", "sdk_text",
    "sdk_tool_call", "sdk_tool_result", "review_completed", "role_failed",
    "worker_failed", "environment_lost", "platform_progress", "bump_queued",
    "acquisition_saved", "acquisition_updated", "acquisition_failed",
})
_TABLES = (
    "veria_members", "veria_sessions", "veria_messages", "veria_deliveries",
    "veria_cursors", "veria_bumps", "veria_candidates", "veria_receipts",
    "veria_report_batches", "veria_report_requests", "veria_reviews",
    "veria_acquisition_drafts", "veria_acquisition_jobs", "veria_acquisition_versions",
)


class StaleAcquisitionJob(ValueError):
    """A material job was superseded by a newer cutoff or member supplement."""


class VeriaRoom(Room):
    def __init__(self, directory: Path, code: str, attempt_id: str, *, solver_ids: tuple[str, ...] | list[str] | None = None):
        super().__init__(directory, code, attempt_id)
        try:
            engine = self.meta("team_engine")
            if engine not in (None, "veria"):
                raise ValueError("cannot resume a different engine in the same attempt")
            if engine is None and self.agents():
                raise ValueError("cannot resume native members as Veria within the same attempt")
            self._migrate()
            saved_roster = self.meta("veria_solver_ids")
            requested_roster = tuple(solver_ids or saved_roster or SOLVER_IDS)
            if requested_roster not in (SOLVER_IDS, ("solver-1", "solver-2", "solver-3", "solver-4")):
                raise ValueError("unsupported Veria solver roster")
            if saved_roster and tuple(saved_roster) != requested_roster:
                raise ValueError("solver roster cannot change within an attempt")
            if not saved_roster and self.agents() and requested_roster != SOLVER_IDS:
                raise ValueError("legacy members cannot resume as a different solver roster")
            self.solver_ids = requested_roster
            self.set_meta("veria_solver_ids", list(self.solver_ids))
            self.set_meta("team_engine", "veria")
            self.recover_deliveries()
            self.recover_bumps()
        except BaseException:
            self.close()
            raise

    def _archive_attempt(self, previous_attempt: str) -> str:
        # Room's backup captures ALL tables, including our extension tables.
        archive = super()._archive_attempt(previous_attempt)
        existing = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        with self.db:
            for table in _TABLES:
                if table in existing:
                    self.db.execute(f"DELETE FROM {table}")
        for name in ("evidence", "acquisitions", "veria", "environment.json", "command-history.jsonl", "members", "inputs", "control", "shared"):
            path = self.directory / name
            if path.exists():
                shutil.move(str(path), Path(archive) / name)
        return archive

    def _migrate(self) -> None:
        self.db.execute("CREATE TABLE IF NOT EXISTS veria_schema(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
        version = int(self.db.execute("SELECT COALESCE(MAX(version),0) FROM veria_schema").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise ValueError("Veria database schema is newer than this runtime")
        if version == 0:
            self.db.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE veria_members (
                    agent_id TEXT PRIMARY KEY, role TEXT NOT NULL, model_id TEXT NOT NULL,
                    run_id TEXT NOT NULL, workspace TEXT NOT NULL, generation TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE veria_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL,
                    session_id TEXT NOT NULL, parent_session_id TEXT, generation TEXT NOT NULL,
                    reason TEXT NOT NULL, session_path TEXT, created_at REAL NOT NULL,
                    UNIQUE(agent_id,session_id)
                );
                CREATE TABLE veria_messages (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, sender TEXT NOT NULL,
                    body TEXT NOT NULL, evidence_refs TEXT NOT NULL, reply_to TEXT,
                    event_seq INTEGER NOT NULL, created_at REAL NOT NULL
                );
                CREATE TABLE veria_deliveries (
                    message_id TEXT NOT NULL, recipient TEXT NOT NULL, state TEXT NOT NULL,
                    injected_turn TEXT, injections INTEGER NOT NULL DEFAULT 0,
                    evidence TEXT, acknowledged_at REAL, PRIMARY KEY(message_id,recipient)
                );
                CREATE TABLE veria_cursors (recipient TEXT PRIMARY KEY, through_event INTEGER NOT NULL);
                CREATE TABLE veria_bumps (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
                    solver_id TEXT NOT NULL, insights TEXT NOT NULL, source_key TEXT,
                    state TEXT NOT NULL, injected_turn TEXT, injections INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, consumed_at REAL,
                    UNIQUE(solver_id,source_key)
                );
                CREATE TABLE veria_candidates (
                    request_id TEXT PRIMARY KEY, flag TEXT NOT NULL UNIQUE, sender TEXT NOT NULL,
                    sources TEXT NOT NULL, evidence_refs TEXT NOT NULL, status TEXT NOT NULL,
                    receipt TEXT, event_seq INTEGER NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE veria_receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, receipt TEXT NOT NULL, created_at REAL NOT NULL,
                    UNIQUE(request_id,fingerprint)
                );
                CREATE TABLE veria_report_batches (
                    batch_id TEXT PRIMARY KEY, after_event INTEGER NOT NULL,
                    through_event INTEGER NOT NULL, input_upper_event INTEGER NOT NULL,
                    state TEXT NOT NULL, report_version INTEGER, created_at REAL NOT NULL
                );
                CREATE TABLE veria_report_requests (
                    request_id TEXT PRIMARY KEY, round_id INTEGER NOT NULL,
                    required_through_event INTEGER NOT NULL, report_version INTEGER,
                    window_id INTEGER, created_at REAL NOT NULL
                );
                CREATE TABLE veria_reviews (
                    review_id TEXT PRIMARY KEY, request TEXT NOT NULL, status TEXT NOT NULL,
                    result TEXT, recoveries INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, completed_at REAL
                );
                CREATE INDEX veria_delivery_pending ON veria_deliveries(recipient,state);
                CREATE INDEX veria_bump_pending ON veria_bumps(solver_id,state,seq);
            """)
            self.db.execute("INSERT INTO veria_schema VALUES(?,?)", (1, time.time()))
            version = 1
        if version < 2:
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS veria_acquisition_drafts (
                    request_id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                    payload TEXT NOT NULL, sender TEXT NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS veria_acquisition_jobs (
                    request_id TEXT PRIMARY KEY, state TEXT NOT NULL,
                    cutoff_event INTEGER NOT NULL, draft_revision INTEGER NOT NULL,
                    error TEXT, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS veria_acquisition_versions (
                    request_id TEXT NOT NULL, version INTEGER NOT NULL,
                    fingerprint TEXT NOT NULL, manifest TEXT NOT NULL, created_at REAL NOT NULL,
                    PRIMARY KEY(request_id,version), UNIQUE(request_id,fingerprint)
                );
            """)
            self.db.execute("INSERT INTO veria_schema VALUES(?,?)", (2, time.time()))
        self.db.commit()

    def _event(self, kind: str, data: dict, sender: str = "runtime") -> int:
        """Insert inside the caller's transaction; unlike Room.event, no commit."""
        cursor = self.db.execute("INSERT INTO events(type,sender,data,created_at) VALUES(?,?,?,?)",
                                 (kind, sender, _json(data), time.time()))
        return int(cursor.lastrowid)

    def register_member(self, agent_id: str, role: str, model_id: str, *,
                        workspace: str | Path | None = None, run_id: str = "") -> dict:
        if agent_id not in (*self.solver_ids, "coordinator", "reporter", "reviewer"):
            raise ValueError("unknown fixed Veria member")
        expected_role = "solver" if agent_id in self.solver_ids else agent_id
        if role != expected_role or not model_id:
            raise ValueError("invalid member role or model")
        workspace = str(workspace or self.directory / "agents" / agent_id)
        old = self.member(agent_id)
        if old:
            if (old["role"], old["model_id"], old["workspace"]) != (role, model_id, workspace):
                raise ValueError("member identity/model/workspace cannot change within an attempt")
            if run_id and old["run_id"] and run_id != old["run_id"]:
                raise ValueError("run identity cannot change within an attempt")
            return old
        with self.db:
            self.db.execute("INSERT INTO agents(agent_id,role,status,updated_at) VALUES(?,?,'queued',?)",
                            (agent_id, role, time.time()))
            self.db.execute("INSERT INTO veria_members(agent_id,role,model_id,run_id,workspace) VALUES(?,?,?,?,?)",
                            (agent_id, role, model_id, run_id, workspace))
            self._event("agent_created", {"agent_id": agent_id, "role": role, "model_id": model_id})
        return self.member(agent_id) or {}

    def member(self, agent_id: str) -> dict | None:
        row = self.db.execute("SELECT a.*,m.model_id,m.run_id,m.workspace,m.generation FROM agents a "
                              "JOIN veria_members m USING(agent_id) WHERE a.agent_id=?", (agent_id,)).fetchone()
        return {**dict(row), "attempt_id": self.attempt_id, "challenge_code": self.code} if row else None

    def members(self) -> list[dict]:
        return [self.member(row[0]) for row in self.db.execute("SELECT agent_id FROM veria_members ORDER BY agent_id")]

    def set_session(self, agent_id: str, session_id: str, *, generation: str = "",
                    reason: str = "resume", session_path: str | None = None) -> dict:
        member = self.member(agent_id)
        if not member or not session_id:
            raise ValueError("registered member and nonempty session id are required")
        old = self.db.execute("SELECT * FROM veria_sessions WHERE agent_id=? AND session_id=?",
                              (agent_id, session_id)).fetchone()
        reset = self.meta("veria_session_reset:" + agent_id, {})
        parent_id = member["session_id"] or reset.get("parent_session_id")
        if reason == "resume" and reset:
            reason = reset.get("reason", reason)
        with self.db:
            if old:
                self.db.execute("UPDATE veria_sessions SET session_path=COALESCE(?,session_path) WHERE id=?",
                                (session_path, old["id"]))
            else:
                self.db.execute("INSERT INTO veria_sessions(agent_id,session_id,parent_session_id,generation,reason,session_path,created_at) "
                                "VALUES(?,?,?,?,?,?,?)", (agent_id, session_id, parent_id, generation,
                                                        reason, session_path, time.time()))
            self.db.execute("UPDATE agents SET session_id=?,updated_at=? WHERE agent_id=?", (session_id, time.time(), agent_id))
            self.db.execute("UPDATE veria_members SET generation=? WHERE agent_id=?", (generation, agent_id))
            self.db.execute("DELETE FROM meta WHERE key=?", ("veria_session_reset:" + agent_id,))
        return self.member(agent_id) or {}

    def reset_session(self, agent_id: str, reason: str) -> dict:
        member = self.member(agent_id)
        if not member or not reason:
            raise ValueError("registered member and reset reason are required")
        previous = self.meta("veria_session_reset:" + agent_id, {})
        reset = {"parent_session_id": member["session_id"] or previous.get("parent_session_id"),
                 "reason": reason, "created_at": time.time()}
        with self.db:
            self.db.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            ("veria_session_reset:" + agent_id, _json(reset)))
            self.db.execute("UPDATE agents SET session_id=NULL,updated_at=? WHERE agent_id=?", (time.time(), agent_id))
            self._event("session_reset", {"agent_id": agent_id, **reset})
        self.recover_role(agent_id)
        return self.member(agent_id) or {}

    def recover_role(self, agent_id: str) -> dict:
        if not self.member(agent_id):
            raise ValueError("unknown member")
        with self.db:
            messages = self.db.execute("UPDATE veria_deliveries SET state='uncertain' WHERE recipient=? AND state='injected'", (agent_id,)).rowcount
            bumps = self.db.execute("UPDATE veria_bumps SET state='uncertain' WHERE solver_id=? AND state='injected'", (agent_id,)).rowcount
        return {"agent_id": agent_id, "messages": messages, "bumps": bumps}

    def authorize_recovery(self, request_id: str, signature: dict, members: list[str],
                           refused: dict, deadline: float | None) -> dict:
        """Atomically rearm exactly the authorized failed roles and save receipt.

        The runtime launches roles only after commit. A crash on either side of
        commit resumes queued roles with the same sessions and retry budget.
        """
        key = "operator_recovery:" + request_id
        old = self.meta(key)
        if old:
            if old["request"] != signature:
                raise ValueError("recovery request id reused with different payload")
            return old["receipt"]
        receipt = {"request_id": request_id, "recovered": members, "refused": refused,
            "deadline": deadline, "attempt_id": self.attempt_id, "at": time.time()}
        with self.db:
            for member in members:
                row = self.db.execute("SELECT status FROM agents WHERE agent_id=?", (member,)).fetchone()
                if not row or row["status"] != "failed":
                    raise ValueError("only terminal failed members can be rearmed")
                self.db.execute("UPDATE agents SET restarts=0,status='queued',last_error=NULL,updated_at=? WHERE agent_id=?",
                    (receipt["at"], member))
                self.db.execute("UPDATE veria_deliveries SET state='uncertain' WHERE recipient=? AND state='injected'", (member,))
                self.db.execute("UPDATE veria_bumps SET state='uncertain' WHERE solver_id=? AND state='injected'", (member,))
            self.db.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, _json({"request": signature, "receipt": receipt})))
            self._event("operator_recovery", {**receipt, **signature})
        return receipt

    def session_lineage(self, agent_id: str) -> list[dict]:
        return [dict(row) for row in self.db.execute("SELECT * FROM veria_sessions WHERE agent_id=? ORDER BY id", (agent_id,))]

    def _send(self, kind: str, sender: str, recipients: list[str], body: str, *,
              message_id: str | None = None, evidence_refs: list | None = None,
              reply_to: str | None = None, source_message_ids: list[str] | None = None) -> dict:
        recipients = sorted(set("coordinator" if recipient == "leader" else recipient for recipient in recipients))
        if not body.strip() or not recipients:
            raise ValueError("message body and recipients are required")
        if any(not self.member(recipient) for recipient in recipients):
            raise ValueError("unknown message recipient")
        message_id = message_id or "msg-" + uuid.uuid4().hex
        refs = evidence_refs or []
        if source_message_ids is not None and (not isinstance(source_message_ids, list) or
                any(not isinstance(item, str) or not item.strip() for item in source_message_ids)):
            raise ValueError("source_message_ids must be a list of existing message IDs")
        sources = list(dict.fromkeys(source_message_ids or []))
        if any(self.veria_message(identifier) is None for identifier in sources):
            raise ValueError("source_message_ids must reference existing messages")
        old = self.veria_message(message_id)
        if old:
            if (old["kind"], old["sender"], old["body"], old["evidence_refs"], old["reply_to"], old["recipients"], old["source_message_ids"]) != (
                    kind, sender, body, refs, reply_to, recipients, sources):
                raise ValueError("message id reused with different content")
            return old
        with self.db:
            seq = self._event(kind, {"id": message_id, "body": body, "recipients": recipients,
                                    "evidence_refs": refs, "reply_to": reply_to,
                                    "source_message_ids": sources}, sender)
            self.db.execute("INSERT INTO veria_messages VALUES(?,?,?,?,?,?,?,?)",
                            (message_id, kind, sender, body, _json(refs), reply_to, seq, time.time()))
            self.db.executemany("INSERT INTO veria_deliveries(message_id,recipient,state) VALUES(?,?,'queued')",
                                [(message_id, recipient) for recipient in recipients])
        return self.veria_message(message_id) or {}

    def notice(self, sender: str, body: str, *, message_id: str | None = None,
               evidence_refs: list | None = None) -> dict:
        return self._send("coordinator_notice", sender, ["coordinator"], body,
                          message_id=message_id, evidence_refs=evidence_refs)

    def broadcast(self, sender: str, body: str, *, recipients: list[str] | None = None,
                  message_id: str | None = None, evidence_refs: list | None = None,
                  reply_to: str | None = None, source_message_ids: list[str] | None = None) -> dict:
        if sender not in ("coordinator", "platform", "runtime", "host"):
            raise ValueError("technical broadcast belongs to coordinator")
        return self._send("broadcast", sender, list(self.solver_ids) if recipients is None else recipients, body,
                          message_id=message_id, evidence_refs=evidence_refs, reply_to=reply_to,
                          source_message_ids=source_message_ids)

    def send(self, sender: str, recipient: str, body: str, *, evidence_refs: list | None = None,
             reply_to: str | None = None, message_id: str | None = None) -> dict:
        return self._send("message_queued", sender, [recipient], body, message_id=message_id,
                          evidence_refs=evidence_refs, reply_to=reply_to)

    def veria_message(self, message_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM veria_messages WHERE id=?", (message_id,)).fetchone()
        if row is None:
            return None
        deliveries = [dict(item) for item in self.db.execute("SELECT * FROM veria_deliveries WHERE message_id=? ORDER BY recipient", (message_id,))]
        source_event = self.db.execute("SELECT data FROM events WHERE seq=?", (row["event_seq"],)).fetchone()
        sources = _load(source_event[0], {}).get("source_message_ids", []) if source_event else []
        return {**dict(row), "evidence_refs": _load(row["evidence_refs"], []), "deliveries": deliveries,
                "recipients": [item["recipient"] for item in deliveries], "source_message_ids": sources}

    def mailbox(self, recipient: str, limit: int = 100, *, include_injected: bool = False) -> list[dict]:
        recipient = "coordinator" if recipient == "leader" else recipient
        states = "('queued','uncertain','injected')" if include_injected else "('queued','uncertain')"
        rows = self.db.execute("SELECT m.*,d.recipient,d.state,d.injected_turn,d.injections,d.evidence,e.data AS provenance_data "
                               "FROM veria_messages m JOIN veria_deliveries d ON m.id=d.message_id JOIN events e ON e.seq=m.event_seq "
                               f"WHERE d.recipient=? AND d.state IN {states} ORDER BY m.event_seq LIMIT ?",
                               (recipient, max(1, min(limit, 10000))))
        messages = []
        for row in rows:
            message = dict(row)
            provenance = _load(message.pop("provenance_data"), {})
            messages.append({**message, "evidence_refs": _load(row["evidence_refs"], []),
                             "source_message_ids": provenance.get("source_message_ids", [])})
        return messages

    def mark_injected(self, recipient: str, ids: list[str], turn_id: str) -> None:
        if not turn_id:
            raise ValueError("turn id is required")
        recipient = "coordinator" if recipient == "leader" else recipient
        with self.db:
            for message_id in ids:
                row = self.db.execute("SELECT m.event_seq,d.state FROM veria_messages m JOIN veria_deliveries d "
                                      "ON m.id=d.message_id WHERE m.id=? AND d.recipient=?", (message_id, recipient)).fetchone()
                if not row:
                    raise ValueError("message does not belong to recipient")
                if row["state"] == "acknowledged":
                    continue
                self.db.execute("UPDATE veria_deliveries SET state='injected',injected_turn=?,injections=injections+1 "
                                "WHERE message_id=? AND recipient=?", (turn_id, message_id, recipient))
                self.db.execute("INSERT INTO veria_cursors VALUES(?,?) ON CONFLICT(recipient) "
                                "DO UPDATE SET through_event=MAX(through_event,excluded.through_event)", (recipient, row["event_seq"]))

    inject = mark_injected

    def acknowledge_messages(self, recipient: str, ids: list[str], evidence: str = "") -> list[str]:
        recipient = "coordinator" if recipient == "leader" else recipient
        changed = []
        with self.db:
            for message_id in ids:
                row = self.db.execute("SELECT state FROM veria_deliveries WHERE message_id=? AND recipient=?", (message_id, recipient)).fetchone()
                if not row or row[0] == "queued":
                    raise ValueError("can only acknowledge an injected message addressed to this member")
                if row[0] != "acknowledged":
                    self.db.execute("UPDATE veria_deliveries SET state='acknowledged',evidence=?,acknowledged_at=? "
                                    "WHERE message_id=? AND recipient=?", (evidence, time.time(), message_id, recipient))
                    changed.append(message_id)
        return changed

    acknowledge = acknowledge_messages

    def recover_deliveries(self) -> int:
        with self.db:
            cursor = self.db.execute("UPDATE veria_deliveries SET state='uncertain' WHERE state='injected'")
        return cursor.rowcount

    def queue_bump(self, solver_id: str, insights: str, *, bump_id: str | None = None,
                   source_key: str | None = None) -> dict:
        if solver_id not in self.solver_ids or not self.member(solver_id) or not insights.strip():
            raise ValueError("registered solver and nonempty guidance are required")
        bump_id = bump_id or "bump-" + uuid.uuid4().hex
        old = self.db.execute("SELECT * FROM veria_bumps WHERE id=?", (bump_id,)).fetchone()
        if old:
            if (old["solver_id"], old["insights"], old["source_key"]) != (solver_id, insights, source_key):
                raise ValueError("bump id reused with different guidance")
            return dict(old)
        if source_key:
            old = self.db.execute("SELECT * FROM veria_bumps WHERE solver_id=? AND source_key=?", (solver_id, source_key)).fetchone()
            if old:
                return dict(old)
        with self.db:
            self.db.execute("INSERT INTO veria_bumps(id,solver_id,insights,source_key,state,created_at) VALUES(?,?,?,?,'queued',?)",
                            (bump_id, solver_id, insights, source_key, time.time()))
            self._event("bump_queued", {"id": bump_id, "solver_id": solver_id, "insights": insights}, "coordinator")
        return dict(self.db.execute("SELECT * FROM veria_bumps WHERE id=?", (bump_id,)).fetchone())

    def pending_bumps(self, solver_id: str, limit: int = 20, max_chars: int = 16000) -> list[dict]:
        result, size = [], 0
        for row in self.db.execute("SELECT * FROM veria_bumps WHERE solver_id=? AND state IN ('queued','uncertain') ORDER BY seq LIMIT ?", (solver_id, limit)):
            # One oversized item is returned whole, never silently truncated.
            if result and size + len(row["insights"]) > max_chars:
                break
            result.append(dict(row))
            size += len(row["insights"])
        return result

    def inject_bumps(self, solver_id: str, ids: list[str], turn_id: str) -> None:
        if not turn_id:
            raise ValueError("turn id is required")
        expected = [row["id"] for row in self.pending_bumps(solver_id, limit=len(ids), max_chars=2**63 - 1)]
        if ids != expected:
            raise ValueError("bumps must be injected in FIFO order")
        with self.db:
            for bump_id in ids:
                self.db.execute("UPDATE veria_bumps SET state='injected',injected_turn=?,injections=injections+1 WHERE id=? AND solver_id=?",
                                (turn_id, bump_id, solver_id))

    def complete_turn(self, agent_id: str, turn_id: str) -> None:
        with self.db:
            self.db.execute("UPDATE veria_bumps SET state='consumed',consumed_at=? "
                            "WHERE solver_id=? AND state='injected' AND injected_turn=?", (time.time(), agent_id, turn_id))

    def recover_bumps(self) -> int:
        with self.db:
            cursor = self.db.execute("UPDATE veria_bumps SET state='uncertain' WHERE state='injected'")
        return cursor.rowcount

    @staticmethod
    def _candidate(row: Any) -> dict | None:
        if row is None:
            return None
        return {**dict(row), "sources": _load(row["sources"], []),
                "evidence_refs": _load(row["evidence_refs"], []), "receipt": _load(row["receipt"])}

    def candidate(self, request_id: str) -> dict | None:
        return self._candidate(self.db.execute("SELECT * FROM veria_candidates WHERE request_id=?", (request_id,)).fetchone())

    def candidate_for_flag(self, flag: str) -> dict | None:
        return self._candidate(self.db.execute("SELECT * FROM veria_candidates WHERE flag=?", (flag.strip(),)).fetchone())

    def save_candidate(self, sender: str, flag: str, *, request_id: str | None = None,
                       evidence_refs: list | None = None) -> dict:
        flag = flag.strip()
        if not flag:
            raise ValueError("candidate flag is empty")
        if request_id:
            old = self.candidate(request_id)
            if old and old["flag"] != flag:
                raise ValueError("submission request id reused with another flag")
        old = self.candidate_for_flag(flag)
        refs = evidence_refs or []
        source = {"sender": sender, "evidence_refs": refs}
        if old:
            sources = old["sources"]
            if source not in sources:
                sources.append(source)
            merged_refs = list(old["evidence_refs"])
            for ref in refs:
                if ref not in merged_refs:
                    merged_refs.append(ref)
            with self.db:
                self.db.execute("UPDATE veria_candidates SET sources=?,evidence_refs=?,updated_at=? WHERE request_id=?",
                                (_json(sources), _json(merged_refs), time.time(), old["request_id"]))
            return self.candidate(old["request_id"]) or {}
        request_id = request_id or "candidate-" + uuid.uuid4().hex
        now = time.time()
        with self.db:
            seq = self._event("candidate_flag", {"request_id": request_id, "flag": flag,
                                               "sender": sender, "evidence_refs": refs}, sender)
            self.db.execute("INSERT INTO veria_candidates VALUES(?,?,?,?,?,'pending',NULL,?,?,?)",
                            (request_id, flag, sender, _json([source]), _json(refs), seq, now, now))
        return self.candidate(request_id) or {}

    @staticmethod
    def receipt_status(receipt: dict) -> str:
        if receipt.get("duplicate") is True:
            return "duplicate"
        if receipt.get("correct") is True:
            return "correct"
        if receipt.get("unknown") is True:
            return "unknown"
        if receipt.get("error"):
            return "error"
        if receipt.get("pending"):
            return "pending"
        if receipt.get("correct") is False:
            return "incorrect"
        return "unknown"

    def apply_receipt(self, request_id: str, receipt: dict) -> bool:
        if not isinstance(receipt, dict):
            raise ValueError("receipt must be an object")
        if any(key in receipt and not isinstance(receipt[key], bool) for key in ("correct", "duplicate", "unknown")):
            raise ValueError("receipt correctness markers must be boolean")
        candidate = self.candidate(request_id)
        if candidate is None:
            raise ValueError("receipt does not identify a persisted candidate")
        encoded = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(encoded.encode()).hexdigest()
        if self.db.execute("SELECT 1 FROM veria_receipts WHERE request_id=? AND fingerprint=?", (request_id, fingerprint)).fetchone():
            return False
        status = self.receipt_status(receipt)
        # A delayed timeout/error must not erase a later authoritative result.
        terminal = {"correct", "duplicate", "incorrect"}
        apply = not (candidate["status"] in terminal and status not in terminal)
        now = time.time()
        with self.db:
            self.db.execute("INSERT INTO veria_receipts(request_id,fingerprint,receipt,created_at) VALUES(?,?,?,?)",
                            (request_id, fingerprint, encoded, now))
            if apply:
                self.db.execute("UPDATE veria_candidates SET status=?,receipt=?,updated_at=? WHERE request_id=?",
                                (status, encoded, now, request_id))
                self.db.execute("INSERT INTO submissions VALUES(?,?,?) ON CONFLICT(request_id) DO UPDATE SET data=excluded.data",
                                (request_id, encoded, now))
                self._event("submission_result", {"request_id": request_id, "receipt": receipt})
                if status == "correct":
                    # This durable job is committed with the authoritative receipt.
                    # No filesystem scan or copy happens on the submission path.
                    self._queue_acquisition(request_id)
        return apply

    submission = apply_receipt

    def pending_candidates(self) -> list[dict]:
        return [self._candidate(row) for row in self.db.execute(
            "SELECT * FROM veria_candidates WHERE status IN ('pending','unknown','error') ORDER BY created_at")]

    def confirmed_submissions(self) -> list[dict]:
        return [self._candidate(row) for row in self.db.execute(
            "SELECT * FROM veria_candidates WHERE status IN ('correct','duplicate') ORDER BY created_at")]

    def receipt_history(self, request_id: str) -> list[dict]:
        return [{**dict(row), "receipt": _load(row["receipt"], {})} for row in self.db.execute(
            "SELECT * FROM veria_receipts WHERE request_id=? ORDER BY id", (request_id,))]

    def _acquisition_confirmed(self, request_id: str) -> bool:
        candidate = self.candidate(request_id)
        return bool(candidate and candidate["status"] in {"correct", "duplicate"} and any(
            self.receipt_status(item["receipt"]) == "correct" for item in self.receipt_history(request_id)))

    def _queue_acquisition(self, request_id: str, *, force: bool = False) -> bool:
        if not self._acquisition_confirmed(request_id):
            return False
        draft = self.acquisition_draft(request_id)
        old = self.db.execute("SELECT * FROM veria_acquisition_jobs WHERE request_id=?", (request_id,)).fetchone()
        revision = draft["revision"] if draft else 0
        if old and not force and old["draft_revision"] == revision:
            return False
        self.db.execute("INSERT INTO veria_acquisition_jobs VALUES(?,'pending',?,?,NULL,?) "
                        "ON CONFLICT(request_id) DO UPDATE SET state='pending',cutoff_event=excluded.cutoff_event,"
                        "draft_revision=excluded.draft_revision,error=NULL,updated_at=excluded.updated_at",
                        (request_id, self.last_event, revision, time.time()))
        return True

    def recover_acquisitions(self) -> int:
        """Backfill real old CORRECT receipts; duplicate-only values are not proof."""
        count = 0
        with self.db:
            for candidate in self.confirmed_submissions():
                job = self.db.execute("SELECT state FROM veria_acquisition_jobs WHERE request_id=?",
                                      (candidate["request_id"],)).fetchone()
                count += self._queue_acquisition(candidate["request_id"], force=bool(job and job["state"] == "error"))
        return count

    def acquisition_draft(self, request_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM veria_acquisition_drafts WHERE request_id=?", (request_id,)).fetchone()
        if row is None:
            return None
        payload = _load(row["payload"], {})
        events = self.db.execute("SELECT sender,data FROM events WHERE type='acquisition_updated' "
            "AND json_extract(data,'$.request_id')=? AND json_extract(data,'$.draft_revision')<=? "
            "ORDER BY seq", (request_id, row["revision"])).fetchall()
        updates = [{"sender": event["sender"], "data": _load(event["data"], {})} for event in events]
        # New updates persist the complete field provenance in their journal
        # record. Older drafts retain the same schema: reconstruct their actual
        # field changes from the before-images already present in that journal.
        senders = {key: row["sender"] for key in payload}
        for index, update in enumerate(updates):
            data = update["data"]
            explicit = data.get("field_senders")
            if isinstance(explicit, dict):
                senders.update({key: value for key, value in explicit.items()
                                if key in payload and isinstance(value, str) and value})
                continue
            after = (updates[index + 1]["data"].get("previous_fields")
                     if index + 1 < len(updates) else payload)
            before = data.get("previous_fields", {})
            for key in data.get("fields", []):
                if key in payload and (not isinstance(after, dict) or key not in before
                                       or before[key] != after.get(key)):
                    senders[key] = update["sender"]
        return {**dict(row), "payload": payload, "field_senders": senders}

    def record_acquisition(self, sender: str, request_id: str, fields: dict) -> dict:
        if not self.candidate(request_id):
            raise ValueError("acquisition requires a persisted candidate request_id")
        allowed = {"entrypoints", "prerequisites", "replay_commands", "script_paths", "evidence_refs", "notes"}
        if fields.keys() - allowed:
            raise ValueError("unsupported acquisition fields")
        if len(_json(fields).encode()) > 128 * 1024:
            raise ValueError("acquisition instructions exceed 128KiB; keep full scripts/output in referenced files instead")
        for key, value in fields.items():
            if key == "notes":
                if not isinstance(value, str):
                    raise ValueError("notes must be a string")
            elif not isinstance(value, list) or any(
                    not isinstance(item, (str, int) if key == "evidence_refs" else str)
                    or isinstance(item, bool) or isinstance(item, str) and not item.strip() for item in value):
                raise ValueError(f"{key} must be a list of nonempty {'references' if key == 'evidence_refs' else 'strings'}")
        old = self.acquisition_draft(request_id)
        merged = {**(old or {}).get("payload", {}), **fields}
        if fields and (not old or merged != old["payload"]):
            revision = old["revision"] + 1 if old else 1
            changed_fields = {key for key, value in fields.items()
                              if key not in (old or {}).get("payload", {})
                              or old["payload"][key] != value}
            field_senders = {**(old or {}).get("field_senders", {}),
                             **{key: sender for key in changed_fields}}
            with self.db:
                self.db.execute("INSERT INTO veria_acquisition_drafts VALUES(?,?,?,?,?) ON CONFLICT(request_id) "
                                "DO UPDATE SET revision=excluded.revision,payload=excluded.payload,"
                                "sender=excluded.sender,updated_at=excluded.updated_at",
                                (request_id, revision, _json(merged), sender, time.time()))
                self._event("acquisition_updated", {"request_id": request_id, "draft_revision": revision,
                    "fields": sorted(fields), "changed_fields": sorted(changed_fields),
                    "field_senders": field_senders, "previous_fields": (old or {}).get("payload", {})}, sender)
                self._queue_acquisition(request_id, force=True)
        return self.acquisition_status(request_id)

    def acquisition_jobs(self) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM veria_acquisition_jobs WHERE state='pending' ORDER BY updated_at,request_id")]

    def _validate_acquisition_job(self, job: dict) -> None:
        current = self.db.execute("SELECT * FROM veria_acquisition_jobs WHERE request_id=?",
                                  (job["request_id"],)).fetchone()
        draft = self.db.execute("SELECT revision FROM veria_acquisition_drafts WHERE request_id=?",
                                (job["request_id"],)).fetchone()
        revision = draft["revision"] if draft else 0
        if (not current or current["state"] != "pending"
                or any(current[key] != job[key] for key in ("cutoff_event", "draft_revision"))
                or revision != job["draft_revision"]):
            raise StaleAcquisitionJob("acquisition job was superseded; reload the pending job")

    def acquisition_material(self, job: dict) -> dict:
        self._validate_acquisition_job(job)
        candidate = self.candidate(job["request_id"])
        if not self._acquisition_confirmed(job["request_id"]):
            raise ValueError("acquisition has no authoritative local CORRECT receipt")
        senders = {candidate["sender"], *(source if isinstance(source, str) else source.get("sender")
                     for source in candidate["sources"])} - {None}
        draft = self.acquisition_draft(job["request_id"])
        placeholders = ",".join("?" for _ in senders)
        events = [dict(row) for row in self.db.execute("SELECT * FROM events WHERE type='evidence' "
            f"AND seq<=? AND sender IN ({placeholders}) AND json_extract(data,'$.kind') IN "
            "('tool_execution_registered','tool_execution_result','tool_call','tool_result') ORDER BY seq",
            (job["cutoff_event"], *sorted(senders)))]
        records = [{**event, "data": _load(event["data"], {})} for event in events]
        references = [*candidate["evidence_refs"], *(draft or {}).get("payload", {}).get("evidence_refs", [])]
        unresolved = []
        for reference in references:
            record = self.evidence_record(reference)
            if record and record["type"] == "evidence" and record["seq"] <= job["cutoff_event"]:
                if record["seq"] not in {event["seq"] for event in records}:
                    records.append(record)
            else:
                unresolved.append(reference)
        return {"candidate": candidate, "draft": draft, "job": job, "records": records,
                "unresolved_evidence_refs": unresolved, "context": self.meta("context", {})}

    def save_acquisition(self, job: dict, manifest: dict) -> dict:
        request_id = job["request_id"]
        fingerprint = hashlib.sha256(_json(manifest).encode()).hexdigest()
        old = self.db.execute("SELECT manifest FROM veria_acquisition_versions WHERE request_id=? AND fingerprint=?",
                              (request_id, fingerprint)).fetchone()
        if old is None:
            self._validate_acquisition_job(job)
        with self.db:
            if old:
                saved = _load(old["manifest"], {})
            else:
                version = int(self.db.execute("SELECT COALESCE(MAX(version),0)+1 FROM veria_acquisition_versions "
                    "WHERE request_id=?", (request_id,)).fetchone()[0])
                path = self.directory / "acquisitions" / hashlib.sha256(request_id.encode()).hexdigest() / f"manifest-v{version}.json"
                saved = {**manifest, "version": version, "fingerprint": fingerprint,
                         "manifest_path": str(path), "created_at": time.time()}
                atomic_write(path, json.dumps(saved, ensure_ascii=False, indent=2) + "\n")
                self.db.execute("INSERT INTO veria_acquisition_versions VALUES(?,?,?,?,?)",
                    (request_id, version, fingerprint, _json(saved), saved["created_at"]))
                self._event("acquisition_saved", {"request_id": request_id, "version": version,
                    "manifest_path": str(path), "pending_fields": saved["pending_fields"],
                    "fresh_reproduction": False})
            self.db.execute("UPDATE veria_acquisition_jobs SET state='ready',error=NULL,updated_at=? "
                            "WHERE request_id=? AND cutoff_event=? AND draft_revision=?",
                            (time.time(), request_id, job["cutoff_event"], job["draft_revision"]))
        return saved

    def fail_acquisition(self, job: dict, error: str) -> bool:
        with self.db:
            cursor = self.db.execute("UPDATE veria_acquisition_jobs SET state='error',error=?,updated_at=? "
                            "WHERE request_id=? AND cutoff_event=? AND draft_revision=? AND state='pending'",
                            (error, time.time(), job["request_id"], job["cutoff_event"], job["draft_revision"]))
            if cursor.rowcount:
                self._event("acquisition_failed", {"request_id": job["request_id"], "error": error})
        return bool(cursor.rowcount)

    def acquisition_status(self, request_id: str, *, version: int | None = None) -> dict:
        candidate = self.candidate(request_id)
        if not candidate:
            raise ValueError("unknown candidate request_id")
        row = self.db.execute("SELECT manifest FROM veria_acquisition_versions WHERE request_id=? " +
            ("ORDER BY version DESC LIMIT 1" if version is None else "AND version=?"),
            (request_id,) if version is None else (request_id, version)).fetchone()
        if version is not None and not row:
            raise ValueError("unknown acquisition version")
        job = self.db.execute("SELECT * FROM veria_acquisition_jobs WHERE request_id=?", (request_id,)).fetchone()
        return {"request_id": request_id, "candidate_status": candidate["status"],
                "job": dict(job) if job else None, "draft": self.acquisition_draft(request_id),
                "manifest": _load(row["manifest"], {}) if row else None}

    def acquisition_summaries(self) -> list[dict]:
        result = []
        for candidate in self.confirmed_submissions():
            item = self.acquisition_status(candidate["request_id"])
            manifest = item["manifest"] or {}
            result.append({"request_id": candidate["request_id"], "candidate_status": candidate["status"],
                "state": (item["job"] or {}).get("state", "unavailable"),
                "version": manifest.get("version"), "manifest_path": manifest.get("manifest_path"),
                "pending_fields": manifest.get("pending_fields", ["acquisition_manifest"]),
                "fresh_reproduction": False})
        return result

    def record_evidence(self, sender: str, kind: str, content: Any, *, metadata: dict | None = None) -> dict:
        """Keep complete SDK-visible evidence on disk and bounded event references."""
        metadata = dict(metadata or {})
        text = content if isinstance(content, str) else _json(content)
        raw = text.encode("utf-8")
        evidence_id = "ev-" + uuid.uuid4().hex
        path = self.directory / "evidence" / (evidence_id + ".txt")
        atomic_write(path, text)
        tool_name = str(metadata.get("name", metadata.get("tool", ""))).lower()
        bookkeeping = any(marker in tool_name for marker in (
            "ack", "publish_report", "read_report", "get_report", "read_trace", "get_status",
            "get_state", "get_team_status", "get_challenge_status", "report_status", "health",
            "check_messages", "get_messages",
        ))
        reportable = sender != "reporter" and kind != "sdk_event" and not bookkeeping
        if "reportable" in metadata:
            reportable = sender != "reporter" and bool(metadata["reportable"])
        record = {"id": evidence_id, "path": str(path), "evidence_ref": str(path), "kind": kind,
                  "bytes": len(raw), "length": len(text), "sha256": hashlib.sha256(raw).hexdigest(),
                  "preview": text[:2000], "truncated": len(text) > 2000,
                  "reportable": reportable, "metadata": metadata}
        seq = self.event("evidence", record, sender)
        return {**record, "event_seq": seq}

    def evidence_record(self, reference: int | str) -> dict | None:
        """Resolve a saved event sequence, evidence ID or exact evidence path.

        Resolution is confined to the current attempt's journal. This establishes
        provenance, not truth or reproduction: callers still inspect sender,
        kind, review_id, actual tool result and the referenced full evidence.
        """
        if isinstance(reference, bool) or not isinstance(reference, (int, str)):
            return None
        if isinstance(reference, int) or reference.isdecimal():
            try:
                seq = int(reference)
                if not 0 < seq < 2**63:
                    return None
            except ValueError:
                return None
            row = self.db.execute("SELECT * FROM events WHERE seq=?", (seq,)).fetchone()
        else:
            row = self.db.execute("SELECT * FROM events WHERE type='evidence' AND "
                                  "(json_extract(data,'$.id')=? OR json_extract(data,'$.path')=? OR "
                                  "json_extract(data,'$.evidence_ref')=?) ORDER BY seq DESC LIMIT 1",
                                  (reference, reference, reference)).fetchone()
        return {**dict(row), "data": _load(row["data"], {})} if row else None

    @staticmethod
    def _reportable_where() -> tuple[str, tuple]:
        names = tuple(sorted(REPORTABLE_EVENTS))
        placeholders = ",".join("?" for _ in names)
        where = (f"sender!='reporter' AND type IN ({placeholders}) AND "
                 "(type!='evidence' OR COALESCE(json_extract(data,'$.reportable'),1)=1)")
        return where, names

    @property
    def evidence_watermark(self) -> int:
        where, values = self._reportable_where()
        return int(self.db.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE " + where, values).fetchone()[0])

    def reportable_events(self, after: int = 0, *, through: int | None = None, limit: int = 100) -> list[dict]:
        where, values = self._reportable_where()
        rows = self.db.execute("SELECT * FROM events WHERE " + where + " AND seq>? AND seq<=? ORDER BY seq LIMIT ?",
                               (*values, after, self.evidence_watermark if through is None else through, limit))
        return [{**dict(row), "data": _load(row["data"], {})} for row in rows]

    def _reporter_event(self, event: dict) -> dict:
        """Compact transport previews without deleting journal or file evidence."""
        data = dict(event["data"])
        if event["type"] == "evidence":
            preview = str(data.get("preview", ""))
            original_length = int(data.get("length", len(preview)))
            data = {key: data[key] for key in ("id", "kind", "bytes", "metadata", "reportable") if key in data}
            data.update(reference=event["data"].get("evidence_ref", event["data"].get("path")),
                        preview=preview[:300], original_length=original_length,
                        preview_truncated=original_length > min(len(preview), 300))
        else:
            body = data.get("body")
            full_text = _json(event)
            if isinstance(body, str) and len(body) > 300 or len(full_text) > 2000:
                path = self.directory / "evidence" / f"report-event-{event['seq']}.json"
                if not path.exists():
                    atomic_write(path, full_text + "\n")
                if isinstance(body, str) and len(body) > 300:
                    data.update(body=body[:300], reference=str(path), original_length=len(body),
                                preview_truncated=True)
                if len(_json(data)) > 2000:
                    original = event["data"]
                    data = {key: original[key] for key in ("id", "request_id", "reply_to")
                            if key in original and len(str(original[key])) < 250}
                    data.update(reference=str(path), preview=_json(original)[:300],
                                original_length=len(_json(original)), preview_truncated=True)
        return {**event, "data": data}

    def reporter_batch(self, limit: int = 100, max_chars: int = 60000, *, through: int | None = None) -> dict | None:
        if limit < 1 or max_chars < 1:
            raise ValueError("positive reporter batch limits are required")
        if through is not None and (not isinstance(through, int) or isinstance(through, bool) or through < 0):
            raise ValueError("reporter checkpoint must be a nonnegative integer")
        old = self.db.execute("SELECT * FROM veria_report_batches WHERE state='pending' ORDER BY created_at LIMIT 1").fetchone()
        if old:
            batch = dict(old)
            batch["events"] = [self._reporter_event(event) for event in self.reportable_events(
                batch["after_event"], through=batch["through_event"], limit=1000000)]
            return batch
        after = int(self.meta("veria_reporter_cursor", 0))
        upper = self.evidence_watermark if through is None else min(through, self.evidence_watermark)
        selected, size = [], 0
        for event in self.reportable_events(after, through=upper, limit=limit):
            event = self._reporter_event(event)
            event_size = len(_json(event))
            if selected and size + event_size > max_chars:
                break
            selected.append(event)
            size += event_size
        if not selected:
            return None
        batch = {"batch_id": "batch-" + uuid.uuid4().hex, "after_event": after,
                 "through_event": selected[-1]["seq"], "input_upper_event": upper,
                 "state": "pending", "report_version": None, "created_at": time.time()}
        with self.db:
            self.db.execute("INSERT INTO veria_report_batches VALUES(?,?,?,?,?,?,?)", tuple(batch.values()))
        return {**batch, "events": selected}

    def publish_report(self, data: dict, *, seen_through: int, batch_id: str | None = None) -> dict:
        requested = data.get("through_event", seen_through)
        if (not isinstance(requested, int) or isinstance(requested, bool) or requested < 0
                or requested > seen_through or requested > self.last_event):
            raise ValueError("report exceeds the received event watermark")
        batch = None
        if batch_id:
            batch = self.db.execute("SELECT * FROM veria_report_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch:
                raise ValueError("unknown reporter batch")
            if batch["state"] == "published":
                return self.report_version(batch["report_version"])
            if requested != batch["through_event"] or seen_through != batch["through_event"]:
                raise ValueError("report must cover exactly its fixed input batch")
        prior = self.latest_report()
        if requested < prior["through_event"]:
            raise ValueError("report watermark cannot move backwards")
        content = dict(data)
        for name, default in (("overview", ""), ("recent_changes", []), ("short_history", []),
                              ("next_steps", []), ("evidence_refs", [])):
            content.setdefault(name, prior.get(name, default))
            if name == "overview" and not isinstance(content[name], str):
                raise ValueError("report overview must be a string")
            if name != "overview" and not isinstance(content[name], list):
                raise ValueError(f"report {name} must be a list")
        for key in ("report_version", "through_event", "published_at"):
            content.pop(key, None)
        content["platform"] = self.meta("context", {}).get("platform", {})
        content["report_mode"] = "reporter_snapshot"
        if len(_json(content).encode()) > 200000:
            raise ValueError("report exceeds 200 KB; link full evidence files")
        now = time.time()
        with self.db:
            cursor = self.db.execute("INSERT INTO reports(through_event,data,created_at) VALUES(?,?,?)", (requested, _json(content), now))
            version = int(cursor.lastrowid)
            self.db.execute("INSERT INTO meta VALUES('veria_reporter_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (_json(requested),))
            if batch:
                self.db.execute("UPDATE veria_report_batches SET state='published',report_version=? WHERE batch_id=?", (version, batch_id))
            self._event("report_published", {"report_version": version, "through_event": requested}, "reporter")
            self._bind_reports()
        self.publish_snapshot()
        return self.report_version(version)

    def report_version(self, version: int) -> dict:
        row = self.db.execute("SELECT * FROM reports WHERE version=?", (version,)).fetchone()
        if row is None:
            raise ValueError("unknown report version")
        return {**_load(row["data"], {}), "report_version": row["version"],
                "through_event": row["through_event"], "published_at": row["created_at"]}

    def _bind_reports(self) -> None:
        self.db.execute("UPDATE veria_report_requests SET report_version=(SELECT MIN(version) FROM reports "
                        "WHERE through_event>=veria_report_requests.required_through_event) WHERE report_version IS NULL")

    def request_audit_report(self, request_id: str, round_id: int) -> dict:
        if not request_id or len(request_id) > 200:
            raise ValueError("invalid audit report request id")
        old = self.db.execute("SELECT * FROM veria_report_requests WHERE request_id=?", (request_id,)).fetchone()
        if old and old["round_id"] != round_id:
            raise ValueError("audit request id reused for another round")
        with self.db:
            if not old:
                self.db.execute("INSERT INTO veria_report_requests(request_id,round_id,required_through_event,created_at) VALUES(?,?,?,?)",
                                (request_id, round_id, self.evidence_watermark, time.time()))
            self._bind_reports()
        self.set_meta("veria_active_report_request", request_id)
        return self.audit_report_status(request_id)

    def audit_report_status(self, request_id: str, *, allow_delta: bool = False) -> dict:
        request = self.db.execute("SELECT * FROM veria_report_requests WHERE request_id=?", (request_id,)).fetchone()
        if not request:
            raise ValueError("unknown audit report request")
        frozen = None
        if request["window_id"] is not None:
            row = self.db.execute("SELECT data FROM audit_windows WHERE id=?", (request["window_id"],)).fetchone()
            if row:
                frozen = _load(row[0], {})
        if frozen is None:
            frozen = self.meta("audit_fallback:" + request_id)
        report = self.report_version(request["report_version"]) if request["report_version"] is not None else self.latest_report()
        base = {**dict(request), "report_mode": "reporter_snapshot", "ready": request["report_version"] is not None,
                "coverage_complete": request["report_version"] is not None,
                "report_version": request["report_version"] or 0, "through_event": report["through_event"],
                "evidence_watermark": self.evidence_watermark,
                "pending_after_cutoff": self._pending_after(request["required_through_event"])}
        if frozen and (allow_delta or frozen.get("report_mode") != "report_plus_delta"):
            return {**base, **{key: frozen[key] for key in
                ("report_mode", "ready", "coverage_complete", "report_version", "through_event") if key in frozen}}
        if frozen and not allow_delta and request["report_version"] is None:
            # The supervisor explicitly rejected this unapplied fallback as too
            # large and awaits a real covering report for the same fixed round.
            return {**base, "ready": False, "coverage_complete": False,
                "delta_error": {"error_type": "material_too_large", "awaiting_reporter": True}}
        if not base["ready"] and allow_delta:
            delta = self._audit_delta(request["required_through_event"])
            if delta:
                base.update(report_mode="report_plus_delta", ready=delta["coverage_complete"],
                    coverage_complete=delta["coverage_complete"], report_version=delta["report"]["report_version"],
                    through_event=request["required_through_event"], delta_error=delta.get("delta_error"))
                if base["ready"]:
                    # Freeze the ready version at the status RPC, before a
                    # reporter publication can race the later snapshot RPC.
                    self.set_meta("audit_fallback:" + request_id, {**base, **delta})
        return base

    def _audit_delta(self, cutoff: int) -> dict | None:
        # Version zero is not a reporter publication and cannot become a report.
        row = self.db.execute("SELECT version FROM reports WHERE through_event<=? ORDER BY version DESC LIMIT 1", (cutoff,)).fetchone()
        if row is None:
            return None
        report = self.report_version(row[0])
        where, values = self._reportable_where()
        rows = self.db.execute("SELECT * FROM events WHERE " + where + " AND seq>? AND seq<=? ORDER BY seq",
            (*values, report["through_event"], cutoff))
        events, references, errors = [], [], []
        size = len(_json(report).encode("utf-8"))
        def oversized() -> dict:
            return {"report": report, "delta_events": [], "delta_evidence_refs": [], "coverage_complete": False,
                "delta_error": {"error_type": "material_too_large", "bytes": size, "max_bytes": AUDIT_MATERIAL_MAX_BYTES}}
        for row in rows:
            size += len(row["data"].encode("utf-8")) + 160
            if size > AUDIT_MATERIAL_MAX_BYTES:
                return oversized()
            event = {**dict(row), "data": _load(row["data"], {})}
            events.append(event)
            if event["type"] != "evidence":
                continue
            data = event["data"]
            path = Path(data.get("path", ""))
            try:
                path.resolve().relative_to((self.directory / "evidence").resolve())
                declared_bytes = int(data["bytes"])
                size += declared_bytes
                if declared_bytes < 0 or size > AUDIT_MATERIAL_MAX_BYTES:
                    return oversized()
                # Bound reads before allocation, including corrupted files that
                # have grown since their immutable evidence record was written.
                with path.open("rb") as handle:
                    raw = handle.read(declared_bytes + 1)
                if len(raw) != declared_bytes or hashlib.sha256(raw).hexdigest() != data["sha256"]:
                    raise ValueError("evidence checksum mismatch")
                references.append({"event_seq": event["seq"], "path": str(path),
                    "sha256": data["sha256"], "bytes": len(raw), "content": raw.decode("utf-8")})
            except (OSError, ValueError, KeyError) as error:
                errors.append({"event_seq": event["seq"], "reason": type(error).__name__})
        return {"report": report, "delta_events": events, "delta_evidence_refs": references,
                "coverage_complete": not errors, "delta_error": errors or None}

    def _pending_after(self, cutoff: int) -> int:
        where, values = self._reportable_where()
        return int(self.db.execute("SELECT COUNT(*) FROM events WHERE " + where + " AND seq>?", (*values, cutoff)).fetchone()[0])

    def audit_snapshot(self, request_id: str | None = None, *, allow_delta: bool = False) -> dict:
        request_id = request_id or self.meta("veria_active_report_request")
        if not request_id:
            raise ValueError("an audit report request is required")
        status = self.audit_report_status(request_id, allow_delta=allow_delta)
        if not status["ready"]:
            raise ValueError("audit report does not cover the fixed request watermark")
        report = self.report_version(status["report_version"])
        snapshot = {**self.snapshot(), **report, **status, "report": report, "report_request": status}
        if status["report_mode"] == "report_plus_delta":
            if status["window_id"] is not None:
                frozen = self.db.execute("SELECT data FROM audit_windows WHERE id=?", (status["window_id"],)).fetchone()
                material = _load(frozen[0], {}) if frozen else {}
            else:
                material = self.meta("audit_fallback:" + request_id) or self._audit_delta(status["required_through_event"]) or {}
            snapshot.update({key: material[key] for key in ("delta_events", "delta_evidence_refs") if key in material})
        # The bound report is historical; the surrounding snapshot still exposes
        # the program's current authoritative platform state separately.
        snapshot["platform"] = self.meta("context", {}).get("platform", {})
        promote = False
        if status["window_id"] is not None and not allow_delta and status["report_mode"] == "reporter_snapshot":
            old_window = self.db.execute("SELECT data FROM audit_windows WHERE id=?", (status["window_id"],)).fetchone()
            promote = bool(old_window and _load(old_window[0], {}).get("report_mode") == "report_plus_delta")
        if status["window_id"] is None or promote:
            window = {key: value for key, value in snapshot.items()
                      if key not in {"windows", "review_windows", "messages", "tasks"}}
            with self.db:
                if promote:
                    self.db.execute("UPDATE audit_windows SET data=? WHERE id=?", (_json(window), status["window_id"]))
                    self._event("audit_material_promoted", {"request_id": request_id, "window_id": status["window_id"],
                        "report_version": status["report_version"], "reason": "unapplied fallback rejected by supervisor material guard"})
                else:
                    cursor = self.db.execute("INSERT INTO audit_windows(data,created_at) VALUES(?,?)", (_json(window), time.time()))
                    self.db.execute("UPDATE veria_report_requests SET window_id=? WHERE request_id=?", (cursor.lastrowid, request_id))
            status = self.audit_report_status(request_id, allow_delta=allow_delta)
            snapshot.update(status, report_request=status)
        windows = self.audit_windows()
        for index, window in enumerate(windows, start=1):
            atomic_write(self.directory / f"review-window-{index}.json", _json(window) + "\n")
        self.publish_snapshot()
        return {**snapshot, "windows": windows, "review_windows": windows,
                "post_cutoff_events": status["pending_after_cutoff"]}

    def review(self, review_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM veria_reviews WHERE review_id=?", (review_id,)).fetchone()
        if row is None:
            return None
        progress = self.meta("review_progress:" + review_id, {})
        checkpoint = self.meta("review_reproductions:" + review_id)
        return {**dict(row), "request": _load(row["request"], {}), "result": _load(row["result"]),
                **progress, "reproduction_checkpoint": checkpoint,
                "phase_evidence_complete": bool(checkpoint and progress.get("sections") and progress.get("investigation_evidence_refs"))}

    def start_review(self, review_id: str, request: dict) -> dict:
        old = self.review(review_id)
        if old:
            if old["request"] != request:
                raise ValueError("review id reused with different request")
            return old
        if self.db.execute("SELECT 1 FROM veria_reviews WHERE status='running'").fetchone():
            raise ValueError("another review is already running")
        with self.db:
            self.db.execute("INSERT INTO veria_reviews(review_id,request,status,created_at) VALUES(?,?,'running',?)",
                            (review_id, _json(request), time.time()))
            # A new bounded review owns a fresh retry budget. Keep this in the
            # insertion transaction: a crash cannot leave a new request tied to
            # the previous terminal review's exhausted member retry counter.
            # The existing-id branch above deliberately does not rearm retries.
            self.db.execute("UPDATE agents SET restarts=0,status='queued',last_error=NULL,updated_at=? WHERE agent_id='reviewer'",
                            (time.time(),))
            self.db.execute("INSERT INTO meta VALUES('review_request',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (_json({**request, "review_id": review_id}),))
        return self.review(review_id) or {}

    def recover_review(self, review_id: str) -> dict:
        if not self.review(review_id):
            raise ValueError("unknown review")
        with self.db:
            self.db.execute("UPDATE veria_reviews SET recoveries=recoveries+1 WHERE review_id=? AND status='running'", (review_id,))
        return self.review(review_id) or {}

    def review_progress(self, review_id: str, *, phase: str, sections: dict | None = None,
                        evidence_refs: list | None = None) -> dict:
        review = self.review(review_id)
        if not review or review["status"] != "running":
            raise ValueError("review progress requires a running review")
        previous = self.meta("review_progress:" + review_id, {})
        progress = {**previous, "phase": phase, "updated_at": time.time(),
            "sections": {**previous.get("sections", {}), **(sections or {})},
            "investigation_evidence_refs": list(dict.fromkeys([
                *previous.get("investigation_evidence_refs", []), *(evidence_refs or [])]))}
        self.set_meta("review_progress:" + review_id, progress)
        self.event("review_progress", {"review_id": review_id, **progress}, "reviewer")
        return self.review(review_id) or {}

    def finish_review(self, review_id: str, status: str, reason: str) -> dict:
        if status not in {"partial", "cancelled", "timed_out", "failed"} or not reason.strip():
            raise ValueError("a terminal partial-review status and reason are required")
        review = self.review(review_id)
        if not review:
            raise ValueError("unknown review")
        if review["status"] != "running":
            return review
        result = {"review_id": review_id, "status": status, "partial": True,
            "reason": reason, "reproduction_checkpoint": review.get("reproduction_checkpoint"),
            "sections": review.get("sections", {}),
            "investigation_evidence_refs": review.get("investigation_evidence_refs", []),
            "completed_at": time.time()}
        with self.db:
            self.db.execute("UPDATE veria_reviews SET status=?,result=?,completed_at=? WHERE review_id=? AND status='running'",
                (status, _json(result), result["completed_at"], review_id))
            self._event("review_terminated", result, "reviewer")
        return self.review(review_id) or {}

    def complete_review(self, review_id: str, result: dict) -> dict:
        review = self.review(review_id)
        if review is None:
            raise ValueError("unknown review")
        if review["status"] == "completed":
            if review["result"] != result:
                raise ValueError("completed review result is immutable")
            return review
        if review["status"] != "running":
            raise ValueError("a terminated partial review cannot be marked complete")
        now = time.time()
        with self.db:
            seq = self._event("review_completed", {"review_id": review_id, "result": result}, "reviewer")
            self.db.execute("UPDATE veria_reviews SET status='completed',result=?,completed_at=? WHERE review_id=?",
                            (_json(result), now, review_id))
            self.db.execute("INSERT INTO meta VALUES('last_review',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (_json({**result, "review_id": review_id, "event_seq": seq, "completed_at": now}),))
        return self.review(review_id) or {}

    def snapshot(self) -> dict:
        snapshot = super().snapshot()
        where, values = self._reportable_where()
        through = snapshot["through_event"]
        pending = int(self.db.execute("SELECT COUNT(*) FROM events WHERE " + where + " AND seq>?", (*values, through)).fetchone()[0])
        return {**snapshot, "team_engine": "veria", "report_mode": "reporter_snapshot",
                "schema_version": SCHEMA_VERSION, "solver_ids": list(self.solver_ids), "agents": self.members(),
                "reporter_backlog": self.meta("reporter_backlog", {}),
                "messages": [self.veria_message(row[0]) for row in self.db.execute("SELECT id FROM veria_messages ORDER BY event_seq DESC LIMIT 100")],
                "pending_reporter_messages": pending, "pending_reporter_events": pending,
                "pending_candidates": self.pending_candidates(), "tasks": [],
                "acquisition_records": self.acquisition_summaries(),
                "message_cursors": [dict(row) for row in self.db.execute("SELECT * FROM veria_cursors ORDER BY recipient")],
                "message_delivery_counts": [dict(row) for row in self.db.execute("SELECT recipient,state,COUNT(*) AS count FROM veria_deliveries GROUP BY recipient,state")],
                "pending_bumps": [dict(row) for row in self.db.execute("SELECT * FROM veria_bumps WHERE state!='consumed' ORDER BY seq")],
                "active_review": next((self.review(row[0]) for row in self.db.execute("SELECT review_id FROM veria_reviews WHERE status='running'")), None)}
