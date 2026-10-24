"""Synthetic acquisition integration: no Docker, model, network, or exploit."""
from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from xinghuo.config import VERIA_MODELS
from xinghuo.veria.driver import ROLE_TOOLS
from xinghuo.veria_runtime import VeriaRuntime, VeriaSlot

from tests.test_veria_runtime import FakeDriver, FakeEnvironment, eventually


class AcquisitionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.temp.name)
        self.settings = {"veria_models": VERIA_MODELS, "veria_tool_image": "fixture:local",
            "veria_python": "/fixture/python", "veria_report_interval": 3600,
            "veria_continuation_delay": .01, "role_turn_timeout": 60,
            "operation_timeout": .001, "shutdown_timeout": .02, "max_retries": 2}
        self.context = {"run_id": "acquisition-fixture", "deadline": 99_999_999_999,
            "challenge": {"code": "fixture", "description": "ordinary offline fixture"},
            "platform": {"flag_count": 2, "correct_flag_count": 0, "completed": False}}
        self.events = []
        self.runtime = self.make_runtime()
        await self.runtime.start(self.context)
        await eventually(lambda: self.runtime.slots["pro-1"].driver is not None)

    def make_runtime(self):
        runtime = VeriaRuntime(self.run_dir, "fixture", "fixture-attempt", settings=dict(self.settings),
            environment_factory=FakeEnvironment, driver_factory=FakeDriver)

        async def emit(kind, **data):
            self.events.append({"type": kind, **data})

        runtime.emit = emit
        return runtime

    async def asyncTearDown(self):
        await self.runtime.shutdown()
        self.temp.cleanup()

    async def candidate(self, *, member="pro-1", flag="FLAG{fixture}", evidence_refs=None):
        return await self.runtime.tool(self.runtime.slots[member], "submit_flag",
            {"flag": flag, "evidence_refs": evidence_refs or []})

    async def receipt(self, candidate, receipt=None):
        return await self.runtime.command("submission_result", {"request_id": candidate["request_id"],
            "receipt": receipt if receipt is not None else {"correct": True, "duplicate": False},
            "platform": {"completed": False, "correct_flag_count": 1, "flag_count": 2}})

    async def manifest(self, request_id, *, version=1):
        def ready():
            status = self.runtime.room.acquisition_status(request_id)
            if (status["job"] or {}).get("state") == "error":
                raise AssertionError(status["job"]["error"])
            return bool(status["manifest"] and status["manifest"]["version"] >= version
                        and status["job"]["state"] == "ready")

        await eventually(ready)
        return self.runtime.room.acquisition_status(request_id)["manifest"]

    async def draft(self, candidate, **overrides):
        fields = {"entrypoints": ["ordinary offline fixture"], "prerequisites": [],
                  "replay_commands": [], "script_paths": [], "notes": "synthetic material"}
        fields.update(overrides)
        return await self.runtime.tool(self.runtime.slots["pro-1"], "record_acquisition",
                                      {"request_id": candidate["request_id"], **fields})

    async def test_authoritative_receipt_and_submission_return_before_async_archival_finishes(self):
        self.runtime.settings["operation_timeout"] = .5
        entered, release = asyncio.Event(), asyncio.Event()
        actual_build = self.runtime._build_acquisition

        async def waiting_build(material):
            entered.set()
            await release.wait()
            return await actual_build(material)

        try:
            with patch.object(self.runtime, "_build_acquisition", side_effect=waiting_build):
                submit = asyncio.create_task(self.candidate())
                await eventually(lambda: bool(self.runtime.room.pending_candidates()))
                pending = self.runtime.room.pending_candidates()[0]
                # The blocked archive must neither block receipt processing nor
                # keep a candidate submitter waiting for its platform response.
                receipt = await asyncio.wait_for(self.receipt(pending), .5)
                result = await asyncio.wait_for(submit, .5)
                await asyncio.wait_for(entered.wait(), .5)
                self.assertEqual(receipt["status"], "correct")
                self.assertEqual(result["status"], "correct")
                status = self.runtime.room.acquisition_status(pending["request_id"])
                self.assertEqual(status["job"]["state"], "pending")
                self.assertIsNone(status["manifest"])
                release.set()
                await self.manifest(pending["request_id"])
        finally:
            release.set()

    async def test_unknown_incorrect_and_duplicate_only_never_create_successful_acquisition(self):
        receipts = ({"unknown": True}, {"correct": False}, {"correct": True, "duplicate": True})
        for index, receipt in enumerate(receipts):
            candidate = await self.candidate(member=("pro-1", "pro-2", "flash-1")[index],
                                             flag=f"FLAG{{fixture-{index}}}")
            await self.draft(candidate)
            await self.receipt(candidate, receipt)
            result = await self.runtime.tool(self.runtime.slots["reporter"], "read_acquisition",
                                            {"request_id": candidate["request_id"]})
            self.assertIsNone(result["manifest"])
            self.assertIsNone(result["job"])
        self.assertFalse(self.runtime.room.acquisition_jobs())
        self.assertFalse(list((self.runtime.directory / "acquisitions").rglob("manifest-v*.json")))

    async def test_late_details_create_a_new_immutable_version_and_repeated_updates_are_idempotent(self):
        candidate = await self.candidate()
        await self.receipt(candidate)
        first = await self.manifest(candidate["request_id"])
        first_bytes = Path(first["manifest_path"]).read_bytes()
        await self.draft(candidate, notes="late fixture details")
        second = await self.manifest(candidate["request_id"], version=2)
        self.assertEqual(second["draft_revision"], 1)
        self.assertEqual(second["notes"], "late fixture details")
        await self.draft(candidate, notes="late fixture details")
        duplicate = await self.receipt(candidate)
        self.assertTrue(duplicate["duplicate"])
        await asyncio.sleep(.01)
        latest = self.runtime.room.acquisition_status(candidate["request_id"])["manifest"]
        self.assertEqual(latest["version"], 2)
        self.assertEqual(Path(first["manifest_path"]).read_bytes(), first_bytes)
        old = await self.runtime.tool(self.runtime.slots["reporter"], "read_acquisition",
            {"request_id": candidate["request_id"], "version": 1})
        self.assertEqual(old["manifest"]["version"], 1)
        self.assertEqual(len(list(Path(first["manifest_path"]).parent.glob("manifest-v*.json"))), 2)

    async def test_shutdown_cancels_copy_but_restart_recovers_durable_job_without_new_budget(self):
        entered = asyncio.Event()

        async def interrupted_build(material):
            entered.set()
            await asyncio.Event().wait()

        candidate = await self.candidate()
        with patch.object(self.runtime, "_build_acquisition", side_effect=interrupted_build):
            await self.receipt(candidate)
            await asyncio.wait_for(entered.wait(), .5)
            task = self.runtime._acquisition_task
            await self.runtime.shutdown()
            self.assertTrue(task.cancelled())
        self.runtime = self.make_runtime()
        await self.runtime.start(self.context)
        restored = await self.manifest(candidate["request_id"])
        self.assertEqual(restored["version"], 1)
        self.assertEqual(restored["request_id"], candidate["request_id"])
        self.assertEqual(self.runtime.room.meta("context")["deadline"], self.context["deadline"])
        self.assertEqual(self.runtime.room.candidate(candidate["request_id"])["status"], "correct")

    async def test_script_and_execution_snapshots_keep_hashes_and_old_bytes_after_source_changes(self):
        script = self.runtime.directory / "members/pro-1/work/fixture.py"
        script.parent.mkdir(parents=True, exist_ok=True)
        original = b"# ordinary fixture version one\n"
        changed = b"# ordinary fixture version two\n"
        script.write_bytes(original)
        evidence = self.runtime.room.record_evidence("pro-1", "tool_execution_result",
            {"command": "python3 fixture.py", "tool_member": "pro-1", "result": {"stdout": "FLAG{fixture}"}},
            metadata={"success": True})
        candidate = await self.candidate(evidence_refs=[evidence["path"]])
        await self.draft(candidate, script_paths=["fixture.py"], replay_commands=["python3 fixture.py"])
        await self.receipt(candidate)
        first = await self.manifest(candidate["request_id"])
        first_manifest_bytes = Path(first["manifest_path"]).read_bytes()
        snapshot = first["scripts"][0]
        self.assertEqual(snapshot["sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(Path(snapshot["frozen_path"]).read_bytes(), original)
        self.assertFalse(snapshot["verified_reproduction"])
        frozen_result = next(item for item in first["tool_evidence"] if item.get("frozen_path"))
        self.assertEqual(hashlib.sha256(Path(frozen_result["frozen_path"]).read_bytes()).hexdigest(),
                         evidence["sha256"])
        script.write_bytes(changed)
        await self.draft(candidate, script_paths=["fixture.py"], replay_commands=["python3 fixture.py"],
                         notes="source changed after initial snapshot")
        second = await self.manifest(candidate["request_id"], version=2)
        self.assertEqual(second["scripts"][0]["sha256"], hashlib.sha256(changed).hexdigest())
        self.assertEqual(Path(second["scripts"][0]["frozen_path"]).read_bytes(), changed)
        self.assertEqual(Path(snapshot["frozen_path"]).read_bytes(), original)
        self.assertEqual(Path(first["manifest_path"]).read_bytes(), first_manifest_bytes)
        self.assertFalse(first["fresh_reproduction"])
        self.assertFalse(second["replay_executed"])

    async def test_cancelling_an_in_progress_real_file_freeze_cleans_its_temporary_copy(self):
        source = self.runtime.directory / "shared/ordinary-large.txt"
        source.parent.mkdir(exist_ok=True)
        contents = b"ordinary offline fixture\n" * 100_000
        source.write_bytes(contents)
        destination = self.runtime.directory / "shared/acquisitions/cancel-fixture/artifacts"
        task = asyncio.create_task(self.runtime._freeze_acquisition_file(source, destination))
        # The implementation yields after each 64KiB write, so this resumes
        # while the real copy is incomplete rather than depending on wall time.
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        staging = self.runtime.directory / "acquisitions/.building"
        temporary_copies = list(staging.glob("copy-*.tmp"))
        self.assertEqual(len(temporary_copies), 1)
        # The first cooperative yield is inside the real copy loop. Its bytes
        # may still be buffered, so observe the private file without requiring
        # that an independent reader can already see the first chunk.
        self.assertTrue(temporary_copies[0].is_file())
        self.assertFalse(list(destination.iterdir()))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assertFalse(list(staging.iterdir()))
        self.assertFalse(list(destination.iterdir()))
        self.assertEqual(source.read_bytes(), contents)

    async def test_docker_and_hosted_script_addresses_map_to_tool_reachable_shared_snapshots(self):
        for hosted in (False, True):
            self.runtime.settings["veria_environment"] = "hosted" if hosted else "docker"
            source = self.runtime.directory / "shared" / f"ordinary-{hosted}.py"
            source.parent.mkdir(exist_ok=True)
            source.write_text("# ordinary shared fixture\n", encoding="utf-8")
            requested = str(source) if hosted else "/challenge/shared/" + source.name
            candidate = await self.candidate(flag=f"FLAG{{fixture-address-{hosted}}}")
            await self.draft(candidate, script_paths=[requested])
            await self.receipt(candidate)
            manifest = await self.manifest(candidate["request_id"])
            self.assertEqual(len(manifest["scripts"]), 1, manifest["unavailable"])
            snapshot = manifest["scripts"][0]
            self.assertEqual(Path(snapshot["source_path"]), source)
            frozen = Path(snapshot["frozen_path"])
            self.assertTrue(frozen.is_relative_to(self.runtime.directory / "shared/acquisitions"))
            tool_path = Path(snapshot["tool_path"])
            mapped = tool_path if hosted else self.runtime.directory / tool_path.relative_to("/challenge")
            self.assertEqual(mapped, frozen)
            self.assertEqual(mapped.read_text(), source.read_text())
            self.assertEqual(snapshot["source_tool_path"], str(source) if hosted else requested)

    async def test_reporter_can_search_and_page_saved_material_and_read_acquisition_versions(self):
        candidate = await self.candidate()
        await self.draft(candidate, notes="前" * 25_000 + "LATE_FIXTURE_CLUE" + "后" * 100)
        await self.receipt(candidate)
        manifest = await self.manifest(candidate["request_id"])
        reporter = self.runtime.slots["reporter"]
        self.assertTrue({"read_acquisition", "read_evidence", "search_evidence"} <= set(ROLE_TOOLS["reporter"]))
        records = await self.runtime.tool(reporter, "read_acquisition", {})
        self.assertEqual(records["acquisition_records"][0]["request_id"], candidate["request_id"])
        status = await self.runtime.tool(reporter, "read_acquisition", {"request_id": candidate["request_id"]})
        path = status["manifest"]["manifest_path"]
        self.assertEqual(path, manifest["manifest_path"])
        found = await self.runtime.tool(reporter, "search_evidence", {"path": path, "query": "LATE_FIXTURE_CLUE"})
        self.assertEqual(len(found["matches"]), 1)
        self.assertGreater(found["matches"][0]["offset"], 24_000)
        page = await self.runtime.tool(reporter, "read_evidence", {"path": path,
            "offset": found["matches"][0]["offset"], "expected_sha256": found["sha256"], "max_chars": 100})
        self.assertIn("LATE_FIXTURE_CLUE", page["text"])
        self.assertFalse(page["complete_file"])
        report_context = await self.runtime.tool(reporter, "get_report", {})
        self.assertEqual(report_context["acquisition_records"][0]["version"], 1)

    async def test_reporter_cannot_supply_acquisition_details_or_submit_flags(self):
        candidate = await self.candidate()
        reporter = self.runtime.slots["reporter"]
        self.assertNotIn("record_acquisition", ROLE_TOOLS["reporter"])
        self.assertNotIn("submit_flag", ROLE_TOOLS["reporter"])
        with self.assertRaisesRegex(ValueError, "only solver"):
            await self.runtime.tool(reporter, "record_acquisition", {"request_id": candidate["request_id"],
                                                                    "notes": "cannot modify"})
        with self.assertRaisesRegex(ValueError, "reporter does not submit"):
            await self.runtime.tool(reporter, "submit_flag", {"flag": "FLAG{fixture}"})
        self.assertIsNone(self.runtime.room.acquisition_draft(candidate["request_id"]))

    async def test_manifest_cannot_be_used_as_this_reviews_fresh_reproduction_evidence(self):
        candidate = await self.candidate()
        await self.receipt(candidate)
        manifest = await self.manifest(candidate["request_id"])
        self.runtime.room.start_review("fresh-proof", {"review_id": "fresh-proof",
            "confirmed_submissions": self.runtime.room.confirmed_submissions()})
        reviewer = VeriaSlot("reviewer", "reviewer", review_id="fresh-proof")
        with self.assertRaisesRegex(ValueError, "this review.*successful tool result"):
            await self.runtime.tool(reviewer, "record_reproductions", {"reproductions": [{
                "request_id": candidate["request_id"], "status": "reproduced",
                "acquisition_steps": "historical manifest only",
                "evidence_refs": [manifest["manifest_path"]]}]})
        self.assertIsNone(self.runtime.room.meta("review_reproductions:fresh-proof"))
        context = self.runtime._review_context()
        self.assertTrue(context["acquisition_records"])
        self.assertFalse(context["acquisition_records"][0]["fresh_reproduction"])


if __name__ == "__main__":
    unittest.main()
