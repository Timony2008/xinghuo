"""Durable, single-writer state for one challenge team.

Only the team runtime writes this database. Other processes consume atomically
published JSON/Markdown snapshots or use the runtime's command channel.
"""
from __future__ import annotations

import json
from contextlib import closing
import os
import sqlite3
import shutil
import time
import uuid
import fcntl
from pathlib import Path
from typing import Any


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _load(value: str | None, default: Any = None) -> Any:
    return json.loads(value) if value is not None else default


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class Room:
    def __init__(self, directory: Path, code: str, attempt_id: str):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.code, self.attempt_id = code, attempt_id
        self._lock = (self.directory / ".runtime.lock").open("a+")
        try:
            fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock.close()
            raise RuntimeError("another runtime already owns this team room") from None
        self.db = sqlite3.connect(self.directory / "room.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS agents (
                agent_id TEXT PRIMARY KEY, role TEXT NOT NULL, session_id TEXT,
                status TEXT NOT NULL, pid INTEGER, restarts INTEGER NOT NULL DEFAULT 0,
                turns INTEGER NOT NULL DEFAULT 0, task_id TEXT, last_error TEXT,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, body TEXT NOT NULL,
                status TEXT NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL,
                sender TEXT NOT NULL, data TEXT NOT NULL, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY, sender TEXT NOT NULL, recipient TEXT NOT NULL,
                body TEXT NOT NULL, evidence_refs TEXT NOT NULL, reply_to TEXT,
                state TEXT NOT NULL, event_seq INTEGER NOT NULL,
                injected_turn TEXT, created_at REAL NOT NULL, acknowledged_at REAL
            );
            CREATE TABLE IF NOT EXISTS reports (
                version INTEGER PRIMARY KEY AUTOINCREMENT, through_event INTEGER NOT NULL,
                data TEXT NOT NULL, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit_windows (
                id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS submissions (
                request_id TEXT PRIMARY KEY, data TEXT NOT NULL, created_at REAL NOT NULL
            );
        """)
        self.db.commit()
        self.stale_agents: list[dict] = []
        previous_attempt = self.meta("attempt_id")
        self.archived_attempt: str | None = None
        if previous_attempt and previous_attempt != attempt_id:
            self.stale_agents = self.agents()
            self.archived_attempt = self._archive_attempt(str(previous_attempt))
        self.set_meta("code", code)
        self.set_meta("attempt_id", attempt_id)

    def _archive_attempt(self, previous_attempt: str) -> str:
        safe_name = "".join(character if character.isalnum() or character in "-_" else "_" for character in previous_attempt)
        archive = self.directory / "attempts" / f"{safe_name or 'attempt'}-{time.time_ns()}"
        archive.mkdir(parents=True)
        with closing(sqlite3.connect(archive / "room.sqlite3")) as destination:
            self.db.backup(destination)
        for name in ("agents", "report.md", "snapshot.json", "review-window-1.json", "review-window-2.json"):
            path = self.directory / name
            if path.exists():
                shutil.move(str(path), archive / name)
        with self.db:
            for table in ("meta", "agents", "tasks", "events", "messages", "reports", "audit_windows", "submissions"):
                self.db.execute(f"DELETE FROM {table}")
            self.db.execute("DELETE FROM sqlite_sequence")
        return str(archive)

    def close(self) -> None:
        self.db.close()
        fcntl.flock(self._lock.fileno(), fcntl.LOCK_UN)
        self._lock.close()

    def set_meta(self, key: str, value: Any) -> None:
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (key, _json(value)))
        self.db.commit()

    def meta(self, key: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return _load(row[0], default) if row else default

    def event(self, kind: str, data: dict, sender: str = "runtime") -> int:
        cur = self.db.execute("INSERT INTO events(type,sender,data,created_at) VALUES(?,?,?,?)",
                              (kind, sender, _json(data), time.time()))
        self.db.commit()
        return int(cur.lastrowid)

    @property
    def last_event(self) -> int:
        return int(self.db.execute("SELECT COALESCE(MAX(seq),0) FROM events").fetchone()[0])

    @property
    def evidence_watermark(self) -> int:
        return int(self.db.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE type IN ('progress','candidate_flag','submission_result','message_queued')").fetchone()[0])

    def events(self, after: int = 0, limit: int = 100) -> list[dict]:
        rows = self.db.execute("SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?", (after, limit))
        return [{**dict(row), "data": _load(row["data"], {})} for row in rows]

    def agent(self, agent_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM agents WHERE agent_id=?", (agent_id,)).fetchone()
        return dict(row) if row else None

    def agents(self) -> list[dict]:
        return [dict(row) for row in self.db.execute("SELECT * FROM agents ORDER BY agent_id")]

    def add_agent(self, agent_id: str, role: str, task: str = "") -> dict:
        now = time.time()
        task_id = f"task-{uuid.uuid4().hex[:12]}" if task else None
        self.db.execute("INSERT INTO agents(agent_id,role,status,task_id,updated_at) VALUES(?,?,'queued',?,?)",
                        (agent_id, role, task_id, now))
        if task_id:
            self.db.execute("INSERT INTO tasks VALUES(?,?,?,'queued',?,?)", (task_id, agent_id, task, now, now))
        self.db.commit()
        self.event("agent_created", {"agent_id": agent_id, "role": role, "task_id": task_id})
        return self.agent(agent_id) or {}

    def update_agent(self, agent_id: str, **fields: Any) -> None:
        allowed = {"session_id", "status", "pid", "restarts", "turns", "last_error"}
        if not fields or not set(fields) <= allowed:
            raise ValueError("invalid agent fields")
        fields["updated_at"] = time.time()
        clauses = ",".join(f"{name}=?" for name in fields)
        self.db.execute(f"UPDATE agents SET {clauses} WHERE agent_id=?", (*fields.values(), agent_id))
        self.db.commit()

    def task(self, agent_id: str) -> dict | None:
        row = self.db.execute("SELECT tasks.* FROM tasks JOIN agents ON tasks.task_id=agents.task_id WHERE agents.agent_id=?", (agent_id,)).fetchone()
        return dict(row) if row else None

    def assign_task(self, agent_id: str, body: str) -> dict:
        agent = self.agent(agent_id)
        if not agent or agent["role"] != "worker" or agent["status"] != "idle":
            raise ValueError("new tasks can only be assigned to an idle managed worker")
        now = time.time()
        task_id = "task-" + uuid.uuid4().hex[:12]
        with self.db:
            self.db.execute("INSERT INTO tasks VALUES(?,?,?,'queued',?,?)", (task_id, agent_id, body, now, now))
            self.db.execute("UPDATE agents SET task_id=?,status='queued',updated_at=? WHERE agent_id=?", (task_id, now, agent_id))
        self.event("task_assigned", {"task_id": task_id, "agent_id": agent_id})
        return self.task(agent_id) or {}

    def task_status(self, agent_id: str, status: str) -> None:
        self.db.execute("UPDATE tasks SET status=?,updated_at=? WHERE task_id=(SELECT task_id FROM agents WHERE agent_id=?)",
                        (status, time.time(), agent_id))
        self.db.commit()

    def complete_worker_task(self, agent_id: str, result: str) -> dict | None:
        task = self.task(agent_id)
        if not task or task["status"] == "completed":
            return None
        now = time.time()
        data = {"task_id": task["task_id"], "agent_id": agent_id, "result": result[:4000]}
        body = "任务完成，请决定下一项具体实验或新任务：" + _json(data)
        message_id = "task-complete:" + task["task_id"]
        # Completion and its notification commit together. A crash cannot leave
        # a completed task with no durable leader wakeup waiting in the mailbox.
        with self.db:
            self.db.execute("UPDATE tasks SET status='completed',updated_at=? WHERE task_id=?", (now, task["task_id"]))
            self.db.execute("INSERT INTO events(type,sender,data,created_at) VALUES('task_completed',?,?,?)", (agent_id, _json(data), now))
            cursor = self.db.execute("INSERT INTO events(type,sender,data,created_at) VALUES('message_queued',?,?,?)",
                (agent_id, _json({"id": message_id, "recipient": "leader"}), now))
            self.db.execute("INSERT INTO messages VALUES(?,?,?,?,?,?,'queued',?,NULL,?,NULL)",
                (message_id, agent_id, "leader", body, "[]", None, int(cursor.lastrowid), now))
        return self.message(message_id)

    def send(self, sender: str, recipient: str, body: str, *, evidence_refs: list | None = None,
             reply_to: str | None = None, message_id: str | None = None) -> dict:
        if not self.agent(recipient):
            raise ValueError(f"unknown recipient: {recipient}")
        if not str(body).strip():
            raise ValueError("message body is required")
        if len(body) > 32000:
            raise ValueError("message is too long; store full evidence in a file and send a concise summary with its reference")
        message_id = message_id or f"msg-{uuid.uuid4().hex}"
        old = self.db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
        if old:
            if old["sender"] != sender or old["recipient"] != recipient or old["body"] != body:
                raise ValueError("message id reused with different content")
            return self._message(old)
        now = time.time()
        seq = self.event("message_queued", {"id": message_id, "recipient": recipient}, sender)
        self.db.execute("INSERT INTO messages VALUES(?,?,?,?,?,?,'queued',?,NULL,?,NULL)",
                        (message_id, sender, recipient, body, _json(evidence_refs or []), reply_to, seq, now))
        self.db.commit()
        return self.message(message_id) or {}

    @staticmethod
    def _message(row: sqlite3.Row) -> dict:
        return {**dict(row), "evidence_refs": _load(row["evidence_refs"], [])}

    def message(self, message_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
        return self._message(row) if row else None

    def messages(self, recipient: str | None = None, *, pending: bool = False, limit: int = 100) -> list[dict]:
        predicates, values = [], []
        if recipient:
            predicates.append("recipient=?")
            values.append(recipient)
        if pending:
            predicates.append("state!='acknowledged'")
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        order = "ASC" if pending else "DESC"
        rows = self.db.execute("SELECT * FROM messages" + where + f" ORDER BY event_seq {order} LIMIT ?", (*values, limit)).fetchall()
        return [self._message(row) for row in (rows if pending else reversed(rows))]

    def inject(self, recipient: str, message_ids: list[str], turn_id: str) -> None:
        for message_id in message_ids:
            self.db.execute("UPDATE messages SET state='injected',injected_turn=? WHERE id=? AND recipient=? AND state!='acknowledged'",
                            (turn_id, message_id, recipient))
        self.db.commit()

    def request_audit_report(self, request_id: str, round_id: int) -> dict:
        if not request_id or len(request_id) > 200:
            raise ValueError("invalid audit report request id")
        key = "audit_report:" + request_id
        request = self.meta(key)
        if request is None:
            request = {"request_id": request_id, "round_id": round_id,
                       "message_id": "audit-report:" + request_id,
                       "baseline_version": self.latest_report()["report_version"]}
            # Save the baseline first. Retrying after a crash cannot accept an
            # unrelated report or choose a newer baseline for this same round.
            self.set_meta(key, request)
        elif request["round_id"] != round_id:
            raise ValueError("audit report request id reused for another round")
        body = _json({"type": "audit_report_request", "round_id": round_id,
            "instruction": "请先撰写本轮负责人报告：关键进展、上轮实验结果、下一窗口具体突破依据、重复尝试及主要阻塞。"
            "读取当前证据与reporter报告；用send_message发给reporter，reply_to必须为本请求message_id，"
            "要求reporter处理并发布新版。随后ack本请求。保持团队继续工作，不等待全局审核。",
            "message_id": request["message_id"]})
        self.send("host", "leader", body, message_id=request["message_id"])
        return self.audit_report_status(request_id)

    def audit_report_status(self, request_id: str) -> dict:
        request = self.meta("audit_report:" + request_id)
        if request is None:
            raise ValueError("unknown audit report request")
        leader_message = self.message(request["message_id"])
        replies = [self._message(row) for row in self.db.execute(
            "SELECT * FROM messages WHERE reply_to=? AND sender='leader' AND recipient='reporter' ORDER BY event_seq DESC",
            (request["message_id"],))]
        report = self.latest_report()
        published = next((reply for reply in replies if reply["state"] == "acknowledged"
            and report["through_event"] >= reply["event_seq"]), None)
        ready = bool(leader_message and leader_message["state"] == "acknowledged" and published
                     and report["report_version"] > request["baseline_version"])
        return {**request, "ready": ready,
                "leader_state": leader_message["state"] if leader_message else "not_queued",
                "draft_message_id": (published or (replies[0] if replies else {})).get("id"),
                "report_version": report["report_version"], "through_event": report["through_event"]}

    def acknowledge(self, recipient: str, message_ids: list[str]) -> list[str]:
        acknowledged = []
        for message_id in message_ids:
            row = self.message(message_id)
            if not row or row["recipient"] != recipient:
                raise ValueError("can only acknowledge own received messages")
            if row["state"] == "queued":
                raise ValueError("cannot acknowledge a message before injection")
            if row["state"] != "acknowledged":
                self.db.execute("UPDATE messages SET state='acknowledged',acknowledged_at=? WHERE id=?",
                                (time.time(), message_id))
                acknowledged.append(message_id)
        self.db.commit()
        if acknowledged:
            self.event("messages_acknowledged", {"ids": acknowledged}, recipient)
        return acknowledged

    def publish_report(self, data: dict, *, seen_through: int) -> dict:
        prior = self.latest_report()
        requested = data.get("through_event", seen_through)
        if not isinstance(requested, int) or requested < 0:
            raise ValueError("through_event must be a nonnegative integer")
        if requested > seen_through:
            raise ValueError("cannot publish beyond received event watermark")
        through = min(requested, self.last_event)
        unread = self.messages("reporter", pending=True, limit=10000)
        if unread:
            through = min(through, min(message["event_seq"] for message in unread) - 1)
        through = max(int(prior.get("through_event", 0)), through)
        content = {}
        for name, default in (("overview", ""), ("recent_changes", []), ("short_history", []),
                              ("next_steps", []), ("evidence_refs", [])):
            content[name] = data.get(name, prior.get(name, default))
            value = content[name]
            if name == "overview" and not isinstance(value, str):
                raise ValueError("report overview must be a string")
            if name != "overview" and (not isinstance(value, list) or not all(isinstance(item, str) for item in value)):
                raise ValueError(f"report {name} must be a list of strings")
        if len(_json(content).encode("utf-8")) > 200000:
            raise ValueError("report exceeds 200 KB; summarize current state and link full evidence/history files")
        now = time.time()
        cursor = self.db.execute("INSERT INTO reports(through_event,data,created_at) VALUES(?,?,?)", (through, _json(content), now))
        self.db.commit()
        version = int(cursor.lastrowid)
        content.update(report_version=version, through_event=through, published_at=now)
        self.event("report_published", {"report_version": version, "through_event": through}, "reporter")
        self.publish_snapshot()
        return content

    def latest_report(self) -> dict:
        row = self.db.execute("SELECT * FROM reports ORDER BY version DESC LIMIT 1").fetchone()
        if not row:
            return {"overview": "", "recent_changes": [], "short_history": [],
                    "next_steps": [], "evidence_refs": [], "report_version": 0, "through_event": 0}
        return {**_load(row["data"], {}), "report_version": row["version"],
                "through_event": row["through_event"], "published_at": row["created_at"]}

    def submission(self, request_id: str, receipt: dict) -> bool:
        old = self.db.execute("SELECT request_id FROM submissions WHERE request_id=?", (request_id,)).fetchone()
        if old:
            return False
        self.db.execute("INSERT INTO submissions VALUES(?,?,?)", (request_id, _json(receipt), time.time()))
        self.db.commit()
        self.event("submission_result", {"request_id": request_id, "receipt": receipt})
        return True

    def confirmed_submissions(self) -> list[dict]:
        """Private reviewer input, independent of the bounded event cursor.

        Receipts prove past acceptance, never a new reproduction. Preserve
        duplicate markers so reviewers can deduplicate by flag identity.
        """
        candidates = self.meta("candidates", {})
        confirmed = []
        for row in self.db.execute("SELECT request_id,data FROM submissions ORDER BY created_at,request_id"):
            receipt = _load(row["data"], {})
            if receipt.get("correct") or receipt.get("duplicate"):
                candidate = candidates.get(row["request_id"], {})
                confirmed.append({"request_id": row["request_id"], "flag": candidate.get("flag"),
                                  "receipt": receipt})
        return confirmed

    def snapshot(self) -> dict:
        report = self.latest_report()
        context = self.meta("context", {})
        windows = self.audit_windows()
        return {**report, "report": report, "code": self.code, "attempt_id": self.attempt_id,
                "platform": context.get("platform", {}), "last_event": self.last_event,
                "evidence_watermark": self.evidence_watermark,
                "agents": self.agents(), "messages": self.messages(limit=100),
                "tasks": [dict(row) for row in self.db.execute("SELECT * FROM tasks ORDER BY created_at")],
                "pending_reporter_messages": len(self.messages("reporter", pending=True, limit=10000)),
                "review_windows": windows, "windows": windows, "runtime_pid": os.getpid(),
                "last_review": self.meta("last_review"), "tool_health": self.meta("tool_patterns", {})}

    def publish_snapshot(self) -> dict:
        snapshot = self.snapshot()
        atomic_write(self.directory / "snapshot.json", _json(snapshot) + "\n")
        if snapshot["report_version"]:
            self._write_markdown(snapshot["report"])
        return snapshot

    def audit_snapshot(self) -> dict:
        snapshot = self.snapshot()
        window = {key: value for key, value in snapshot.items() if key not in {"windows", "review_windows", "messages", "tasks"}}
        self.db.execute("INSERT INTO audit_windows(data,created_at) VALUES(?,?)", (_json(window), time.time()))
        self.db.commit()
        windows = self.audit_windows()
        for index, window in enumerate(windows, start=1):
            atomic_write(self.directory / f"review-window-{index}.json", _json(window) + "\n")
        self.publish_snapshot()
        return {**snapshot, "review_windows": windows, "windows": windows}

    def audit_windows(self) -> list[dict]:
        rows = self.db.execute("SELECT id,data,created_at FROM audit_windows ORDER BY id DESC LIMIT 2").fetchall()
        return [{**_load(row["data"], {}), "window_id": row["id"], "captured_at": row["created_at"]} for row in reversed(rows)]

    def _write_markdown(self, report: dict) -> None:
        context = self.meta("context", {})
        lines = [f"# {self.code} 进度报告", "", f"版本：{report['report_version']}；已覆盖事件：{report['through_event']}；最新事件：{self.last_event}",
                 "", "## 平台状态（程序注入）", "", "```json", _json(context.get("platform", {})), "```", "", "## 当前概览", "", str(report.get("overview", ""))]
        for title, key in (("最近变化", "recent_changes"), ("简短历史", "short_history"), ("下一步", "next_steps"), ("证据索引", "evidence_refs")):
            value = report.get(key, [])
            lines += ["", f"## {title}", ""]
            lines += [f"- {item if isinstance(item, str) else _json(item)}" for item in value] if isinstance(value, list) else [str(value)]
        lines += ["", "## 成员", ""]
        lines += [f"- {agent['agent_id']} ({agent['role']}): {agent['status']}；session={agent['session_id'] or '尚未建立'}" for agent in self.agents()]
        atomic_write(self.directory / "report.md", "\n".join(lines) + "\n")
