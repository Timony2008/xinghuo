import argparse
import asyncio
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import time
import unittest

from xinghuo.cli import execute
from xinghuo.store import Store


class RecoveryCommandTests(unittest.TestCase):
    def test_command_queues_identity_without_changing_deadline_or_room(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            store = Store(directory)
            deadline = time.time() + 300
            store.set_meta("run_id", "test-run")
            store.set_meta("status", "running")
            store.set_meta("deadline", deadline)
            store.save_challenge({"code": "test", "status": "active", "attempt_id": "same-attempt"})
            before = (directory / "state.sqlite3").read_bytes()
            args = argparse.Namespace(command="recover", run_dir=directory, code="test", provider="glm", reason="resolved auth")
            with redirect_stdout(io.StringIO()):
                self.assertEqual(asyncio.run(execute(args)), 0)
            request, = (directory / "control/recovery-requests").glob("*.json")
            payload = json.loads(request.read_text())
            self.assertEqual(payload["run_id"], "test-run")
            self.assertEqual(payload["attempt_id"], "same-attempt")
            self.assertEqual(payload["provider"], "glm")
            self.assertEqual(request.stat().st_mode & 0o777, 0o600)
            self.assertEqual(store.get_meta("deadline"), deadline)
            self.assertEqual((directory / "state.sqlite3").read_bytes(), before)
            store.close()

    def test_stopped_run_cannot_be_restarted_by_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            store = Store(directory)
            store.set_meta("status", "stop_requested")
            store.save_challenge({"code": "test", "status": "active", "attempt_id": "same-attempt"})
            with self.assertRaisesRegex(ValueError, "running run"):
                asyncio.run(execute(argparse.Namespace(command="recover", run_dir=directory, code="test", provider=None, reason="retry")))
            self.assertFalse((directory / "control").exists())
            store.close()


if __name__ == "__main__":
    unittest.main()
