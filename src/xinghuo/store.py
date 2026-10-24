"""Supervisor-owned journal. Teams each own a separate room database."""
from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
from pathlib import Path


def atomic_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class RunLock:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.file = path.open("a+")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.file.close()
            raise RuntimeError("another supervisor owns this run directory") from None

    def close(self):
        self.file.close()


class Store:
    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.run_dir / "state.sqlite3")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS challenges(code TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL, kind TEXT NOT NULL, code TEXT, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                code TEXT NOT NULL, status TEXT NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS reviews(attempt TEXT NOT NULL, round INTEGER NOT NULL,
                body TEXT NOT NULL, PRIMARY KEY(attempt,round));
        """)
        self.db.commit()

    @staticmethod
    def encode(obj):
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    def get_meta(self, key, default=None):
        row = self.db.execute("SELECT body FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, self.encode(value)))

    def challenge(self, code):
        row = self.db.execute("SELECT body FROM challenges WHERE code=?", (code,)).fetchone()
        return json.loads(row[0]) if row else None

    def challenges(self):
        return [json.loads(r[0]) for r in self.db.execute("SELECT body FROM challenges ORDER BY code")]

    def save_challenge(self, row):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO challenges VALUES(?,?)", (row["code"], self.encode(row)))

    def event(self, kind, code=None, **body):
        with self.db:
            cur = self.db.execute("INSERT INTO events(ts,kind,code,body) VALUES(?,?,?,?)",
                                  (time.time(), kind, code, self.encode(body)))
        return cur.lastrowid

    def operation(self, operation_id):
        row = self.db.execute("SELECT kind,code,status,body FROM operations WHERE id=?", (operation_id,)).fetchone()
        return dict(id=operation_id, kind=row[0], code=row[1], status=row[2], **json.loads(row[3])) if row else None

    def save_operation(self, operation_id, kind, code, status, **body):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO operations VALUES(?,?,?,?,?)",
                            (operation_id, kind, code, status, self.encode(body)))

    def save_selection(self, operation_id, code, choice, rng_state):
        # The chosen target and the RNG position form one recovery checkpoint.
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO operations VALUES(?,?,?,?,?)",
                            (operation_id, "selection", code, "chosen", self.encode({"choice": choice})))
            self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", ("rng_state", self.encode(rng_state)))

    def operations(self, kind=None):
        rows = self.db.execute("SELECT id,kind,code,status,body FROM operations" +
                               (" WHERE kind=?" if kind else ""), (kind,) if kind else ())
        return [dict(id=r[0], kind=r[1], code=r[2], status=r[3], **json.loads(r[4])) for r in rows]

    def save_review(self, attempt, round_id, body):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO reviews VALUES(?,?,?)",
                            (attempt, round_id, self.encode(body)))

    def reviews(self, attempt, limit=3):
        rows = self.db.execute("SELECT body FROM reviews WHERE attempt=? ORDER BY round DESC LIMIT ?",
                               (attempt, limit)).fetchall()
        return [json.loads(r[0]) for r in reversed(rows)]

    def close(self):
        self.db.close()
