import asyncio
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from xinghuo.config import Config
from xinghuo.core import TeamPool
from xinghuo.store import Store, RunLock


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_bad_worker_does_not_cancel_sibling(self):
        completed = []
        async def bad():
            raise ValueError("synthetic worker failure")
        async def good():
            await asyncio.sleep(.01)
            completed.append(True)
        pool = TeamPool(2)
        await pool.run([bad, good])
        self.assertEqual(completed, [True])
        self.assertEqual(len(pool.failures), 1)


class EntrypointTests(unittest.TestCase):
    def test_config_rejects_invalid_values(self):
        for values in ({"epsilon": 2}, {"parallel": 4}, {"audit_interval": 0}):
            with self.assertRaises(ValueError):
                Config(**values).validate()

    def test_single_supervisor_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = RunLock(Path(directory) / "run.lock")
            try:
                with self.assertRaises(RuntimeError):
                    RunLock(Path(directory) / "run.lock")
            finally:
                lock.close()

    def test_demo_real_processes_and_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            process = subprocess.run([sys.executable, "-m", "xinghuo", "demo", "--run-dir", directory,
                                      "--duration", "20"], capture_output=True, text=True, timeout=50)
            self.assertEqual(process.returncode, 0, process.stderr[-4000:])
            result = json.loads(process.stdout.strip().splitlines()[-1])
            self.assertEqual(result["status"], "completed", result)
            self.assertEqual(result["completed"], 5)
            self.assertEqual(result["demo_peak"], 3)
            reports = list(Path(directory).glob("teams/*/report.md"))
            self.assertEqual(len(reports), 5)
            self.assertTrue(all("worker-1" in p.read_text() and "worker-2" in p.read_text() for p in reports))


if __name__ == "__main__":
    unittest.main()
