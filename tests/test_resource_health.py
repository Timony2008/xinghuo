import json
from pathlib import Path
import tempfile
import unittest

from xinghuo.resource_health import ResourceMonitor, cgroup_memory, process_memory


class ResourceHealthTests(unittest.TestCase):
    def test_counters_have_baseline_and_only_new_ooms_survive_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, value in {"memory.current": "1024", "memory.peak": "4096",
                                "memory.max": str(16 * 1024**3),
                                "memory.events": "oom 5\noom_kill 3\nhigh 8\n"}.items():
                (root / name).write_text(value)
            monitor = ResourceMonitor(root, root / "saved.json")
            baseline = monitor.snapshot()
            self.assertIsNone(baseline["oom_kill_delta"])
            self.assertEqual(monitor.snapshot()["oom_kill_delta"], 0)
            (root / "memory.events").write_text("oom 7\noom_kill 4\nhigh 9\n")
            restored = ResourceMonitor(root, root / "saved.json").snapshot()
            self.assertEqual(restored["oom_kill_delta"], 1)
            self.assertEqual(restored["limit_bytes"], 16 * 1024**3)
            self.assertNotIn("victim", restored)

    def test_missing_and_unlimited_are_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertFalse(cgroup_memory(root)["available"])
            (root / "memory.current").write_text("10")
            (root / "memory.max").write_text("max")
            value = cgroup_memory(root)
            self.assertIsNone(value["limit_bytes"])
            self.assertIsNone(value["peak_bytes"])

    def test_numeric_process_identity_and_rss_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root / "123").mkdir()
            (root / "123/status").write_text("Name:\tcli\nVmRSS:\t64 kB\n")
            self.assertEqual(process_memory(123, root),
                             {"pid": 123, "exists": True, "rss_bytes": 65536})
            self.assertFalse(process_memory(999, root)["exists"])


if __name__ == "__main__":
    unittest.main()
