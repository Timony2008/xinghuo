"""Role-facing evidence and acquisition wiring, entirely offline."""
import asyncio
import json
from pathlib import Path
import shlex
import tempfile
import unittest

from xinghuo.agent_runner import ROLE_TOOLS as NATIVE_TOOLS, TOOL_SPECS as NATIVE_SPECS
from xinghuo.veria.driver import ROLE_TOOLS, TOOL_SPECS, VeriaRoleDriver, parse_special_command, SpecialCommandError
from xinghuo.veria.prompts import build_role_document
from xinghuo.veria.upstream_baseline import REPORTER_PROMPT
from xinghuo.veria_runtime import VeriaRuntime, VeriaSlot


class EvidenceWiringTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runtime = object.__new__(VeriaRuntime)
        self.runtime.directory = Path(self.temp.name).resolve()
        self.reporter = VeriaSlot("reporter", "reporter")

    async def test_reporter_can_search_and_read_the_tail_and_reject_changed_pages(self):
        text = "开始\n" + "甲乙🙂" * 25_000 + "\nTAIL-KEY-FINDING\n"
        path = self.runtime.directory / "evidence.txt"
        path.write_text(text)
        first = await self.runtime.tool(self.reporter, "read_evidence", {"path": str(path)})
        self.assertTrue(first["truncated"])
        self.assertNotIn("TAIL-KEY-FINDING", first["text"])
        found = await self.runtime.tool(self.reporter, "search_evidence", {"path": str(path), "query": "TAIL-KEY-FINDING"})
        self.assertGreater(found["matches"][0]["offset"], 24_000)
        self.assertFalse(found["complete_file"])
        tail = await self.runtime.tool(self.reporter, "read_evidence", {
            "path": str(path), "offset": found["matches"][0]["offset"], "expected_sha256": found["sha256"]})
        self.assertTrue(tail["eof"])
        self.assertFalse(tail["complete_file"])
        self.assertEqual(tail["text"], "TAIL-KEY-FINDING\n")
        path.write_text(text + "changed")
        with self.assertRaisesRegex(ValueError, "changed"):
            await self.runtime.tool(self.reporter, "read_evidence", {"path": str(path), "offset": 100, "expected_sha256": first["sha256"]})

    async def test_runtime_rejects_private_paths_symlinks_and_invalid_page_arguments(self):
        root = self.runtime.directory
        (root / "public.txt").write_text("text")
        (root / "alias.txt").symlink_to(root / "public.txt")
        for name in (".ENV", "records.SQLITE", "data.db-wal", "sdk-session.json"):
            (root / name).write_text("private")
        for name in (".ENV", "records.SQLITE", "data.db-wal", "sdk-session.json", "../outside.txt", "alias.txt"):
            with self.subTest(name=name), self.assertRaises((ValueError, OSError)):
                await self.runtime.tool(self.reporter, "read_evidence", {"path": name})
        for fields in ({"offset": True}, {"offset": -1}, {"max_chars": True}, {"max_chars": 0}, {"offset": 1}, {"unknown": 1}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                await self.runtime.tool(self.reporter, "read_evidence", {"path": "public.txt", **fields})

    def test_sdk_roles_expose_actual_page_and_search_contracts(self):
        for role in ("reporter", "reviewer", "coordinator"):
            self.assertTrue({"read_evidence", "search_evidence", "read_acquisition"} <= set(ROLE_TOOLS[role]))
        self.assertNotIn("record_acquisition", ROLE_TOOLS["reporter"])
        self.assertEqual(ROLE_TOOLS["solver"], [])
        self.assertEqual(set(NATIVE_TOOLS["strategy"]), {"read_evidence", "search_evidence"})
        for name in ("read_evidence", "search_evidence"):
            self.assertEqual(TOOL_SPECS[name][1], NATIVE_SPECS[name][1])
            self.assertIn("offset", TOOL_SPECS[name][1]["properties"])

    def test_reporter_style_is_append_only_and_preserves_complete_baseline(self):
        doc = build_role_document("reporter", "reporter", {}, {})
        self.assertEqual(doc.baseline, REPORTER_PROMPT)
        self.assertEqual(doc.patches, [])
        self.assertTrue(doc.text.startswith(REPORTER_PROMPT))
        for requirement in ("short factual sentences", "There is no hard word limit", "exact processed watermark", "missing material", "search_evidence"):
            self.assertIn(requirement, doc.appendix)
        self.assertNotIn("record_acquisition '{", doc.text)

    def test_material_instructions_keep_submission_and_fresh_reproduction_requirements(self):
        solver = build_role_document("solver", "solver-1", {}, {}).text
        self.assertIn("Submit each candidate immediately", solver)
        self.assertIn("record_acquisition '{", solver)
        self.assertIn("notify_coordinator with acquisition steps", solver)
        reviewer = build_role_document("reviewer", "reviewer", {"review": {"confirmed_submissions": []}}, {}).text
        self.assertIn("never evidence that this review independently reproduced", reviewer)
        self.assertIn("fresh acquisition output in its own review_id", reviewer)


class AcquisitionCommandWiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_solver_records_structured_material_without_executing_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            calls = []
            async def on_tool(name, arguments):
                calls.append((name, arguments))
                return {"request_id": arguments["request_id"], "job": {"state": "pending"}}
            class Environment:
                async def prepare_command(self, *args):
                    raise AssertionError("acquisition recording must not launch a member command")
            driver = VeriaRoleDriver("solver-1", "solver", "deepseek-v4-pro", temporary, {}, Environment(), on_tool)
            async def no_process_scan():
                pass
            driver._remember_processes = no_process_scan
            payload = {"request_id": "fixture-receipt", "replay_commands": ["python acquire.py --literal '$x'"], "prerequisites": []}
            command = "record_acquisition " + shlex.quote(json.dumps(payload))
            self.assertEqual(parse_special_command(command), ("record_acquisition", json.dumps(payload)))
            result = await driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": command}}, "record-1")
            self.assertEqual(calls, [("record_acquisition", payload)])
            self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "allow")
            rewritten = shlex.split(result["hookSpecificOutput"]["updatedInput"]["command"])
            self.assertEqual(rewritten[:2], ["printf", "%s\\n"])
            self.assertEqual(json.loads(rewritten[2])["request_id"], "fixture-receipt")
            for value in ("[]", "bad JSON"):
                bad = await driver.pre_tool({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {"command": "record_acquisition " + shlex.quote(value)}}, "bad")
                self.assertEqual(bad["hookSpecificOutput"]["permissionDecision"], "deny")
            self.assertEqual(len(calls), 1)
            with self.assertRaises(SpecialCommandError):
                parse_special_command(command + "; echo bad")


if __name__ == "__main__":
    unittest.main()
